package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	tailRoll     = 256 << 20
	batchWait    = 2 * time.Millisecond
	batchLimit   = 1024
	moveEvery    = 100 * time.Millisecond
	moveChunk    = 4 << 20
	minFree      = 1 << 20
	statusOk     = 0
	statusFull   = 1
	statusRange  = 2
	statusIo     = 3
	opAppend     = 1
	opRead       = 2
	opStatus     = 3
	maxFrameSize = 1 << 31
)

var errFull = errors.New("cell: full")
var errRange = errors.New("cell: past the head")

type Tail struct {
	base int64
	size int64
	file *os.File
}

type AppendReq struct {
	data []byte
	done chan AppendResp
}

type AppendResp struct {
	offset int64
	err    error
}

type Snapshot struct {
	head    int64
	durable int64
	flushed int64
	tails   []Tail
}

type Cell struct {
	ssd       string
	hdd       *os.File
	capacity  int64
	appends   chan AppendReq
	snapshots chan chan Snapshot
	moved     chan int64
	head      int64
	durable   int64
	flushed   int64
	tails     []Tail
}

type CellState struct {
	Flushed int64 `json:"flushed"`
}

func runCell(listen, ssd, hdd string) {
	if listen == "" || ssd == "" || hdd == "" {
		throwFmt("cell: -listen, -ssd and -hdd are required")
	}

	c := openCell(ssd, hdd)

	if c.capacity-c.head < minFree {
		throwFmt("cell: %s is full (%d of %d bytes used), not serving", hdd, c.head, c.capacity)
	}

	ln := throw2(net.Listen("tcp", listen))

	slog.Info("cell: serving", "listen", listen, "head", c.head, "flushed", c.flushed, "capacity", c.capacity)

	go c.owner()
	go c.mover()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		os.Exit(0)
	}()

	for {
		conn := throw2(ln.Accept())

		go func() {
			try(func() {
				c.serve(conn)
			}).catch(func(exc *Exception) {
				slog.Warn("cell: connection", "peer", conn.RemoteAddr(), "err", exc.error())
			})
		}()
	}
}

func openCell(ssd, hdd string) *Cell {
	throw(os.MkdirAll(ssd, 0o755))

	dev := throw2(os.OpenFile(hdd, os.O_RDWR, 0))
	capacity := throw2(dev.Seek(0, io.SeekEnd))

	c := &Cell{
		ssd:       ssd,
		hdd:       dev,
		capacity:  capacity,
		appends:   make(chan AppendReq, batchLimit),
		snapshots: make(chan chan Snapshot),
		moved:     make(chan int64),
	}

	c.flushed = c.loadState()
	c.tails = c.openTails()
	c.head = c.flushed

	if n := len(c.tails); n > 0 {
		c.head = c.tails[n-1].base + c.tails[n-1].size
	}

	c.durable = c.head

	return c
}

func (c *Cell) statePath() string {
	return filepath.Join(c.ssd, "state")
}

func (c *Cell) loadState() int64 {
	data, err := os.ReadFile(c.statePath())

	if errors.Is(err, os.ErrNotExist) {
		return 0
	}

	throw(err)

	st := CellState{}

	throw(json.Unmarshal(data, &st))

	return st.Flushed
}

func (c *Cell) saveState(flushed int64) {
	tmp := c.statePath() + ".tmp"
	f := throw2(os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644))

	throw2(f.Write(throw2(json.Marshal(CellState{Flushed: flushed}))))
	throw(f.Sync())
	throw(f.Close())
	throw(os.Rename(tmp, c.statePath()))
}

func tailPath(ssd string, base int64) string {
	return filepath.Join(ssd, "tail-"+strconv.FormatInt(base, 10))
}

func (c *Cell) openTails() []Tail {
	var tails []Tail

	for _, entry := range throw2(os.ReadDir(c.ssd)) {
		name := entry.Name()

		if !strings.HasPrefix(name, "tail-") {
			continue
		}

		base := throw2(strconv.ParseInt(strings.TrimPrefix(name, "tail-"), 10, 64))
		path := filepath.Join(c.ssd, name)
		size := throw2(entry.Info()).Size()

		if base+size <= c.flushed {
			throw(os.Remove(path))

			continue
		}

		tails = append(tails, Tail{base: base, size: size, file: throw2(os.OpenFile(path, os.O_RDWR, 0))})
	}

	sort.Slice(tails, func(i, j int) bool { return tails[i].base < tails[j].base })

	next := c.flushed

	for _, t := range tails {
		if t.base > next {
			throwFmt("cell: gap in the log between %d and %d", next, t.base)
		}

		next = t.base + t.size
	}

	return tails
}

func (c *Cell) append(data []byte) (int64, error) {
	done := make(chan AppendResp, 1)
	c.appends <- AppendReq{data: data, done: done}
	resp := <-done

	return resp.offset, resp.err
}

func (c *Cell) snapshot() Snapshot {
	reply := make(chan Snapshot, 1)
	c.snapshots <- reply

	return <-reply
}

func (c *Cell) owner() {
	for {
		select {
		case first := <-c.appends:
			c.commit(first)
		case reply := <-c.snapshots:
			reply <- Snapshot{head: c.head, durable: c.durable, flushed: c.flushed, tails: append([]Tail(nil), c.tails...)}
		case to := <-c.moved:
			c.forget(to)
		}
	}
}

func (c *Cell) commit(first AppendReq) {
	batch := []AppendReq{first}
	deadline := time.After(batchWait)

collect:
	for len(batch) < batchLimit {
		select {
		case req := <-c.appends:
			batch = append(batch, req)
		case <-deadline:
			break collect
		}
	}

	resps := make([]AppendResp, len(batch))
	touched := map[*os.File]bool{}

	for i, req := range batch {
		resps[i].offset, resps[i].err = c.write(req.data, touched)
	}

	for f := range touched {
		throw(f.Sync())
	}

	c.durable = c.head

	for i, req := range batch {
		req.done <- resps[i]
	}
}

func (c *Cell) write(data []byte, touched map[*os.File]bool) (int64, error) {
	if c.head+int64(len(data)) > c.capacity {
		return 0, errFull
	}

	if n := len(c.tails); n == 0 || c.tails[n-1].size >= tailRoll {
		file := throw2(os.OpenFile(tailPath(c.ssd, c.head), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644))
		c.tails = append(c.tails, Tail{base: c.head, file: file})
	}

	current := &c.tails[len(c.tails)-1]

	throw2(current.file.WriteAt(data, current.size))

	offset := c.head
	current.size += int64(len(data))
	c.head += int64(len(data))
	touched[current.file] = true

	return offset, nil
}

func (c *Cell) forget(to int64) {
	c.flushed = to

	var keep []Tail

	for i, t := range c.tails {
		if t.base+t.size <= to && i < len(c.tails)-1 {
			t.file.Close()
			throw(os.Remove(tailPath(c.ssd, t.base)))
		} else {
			keep = append(keep, t)
		}
	}

	c.tails = keep
}

func (c *Cell) mover() {
	for range time.Tick(moveEvery) {
		try(func() {
			c.move()
		}).catch(func(exc *Exception) {
			slog.Error("cell: mover", "err", exc.error())
		})
	}
}

func (c *Cell) move() {
	snap := c.snapshot()

	if snap.flushed == snap.durable {
		return
	}

	buf := make([]byte, moveChunk)

	for off := snap.flushed; off < snap.durable; {
		t := tailAt(snap.tails, off)
		n := min(int64(len(buf)), t.base+t.size-off, snap.durable-off)

		throw2(t.file.ReadAt(buf[:n], off-t.base))
		throw2(c.hdd.WriteAt(buf[:n], off))

		off += n
	}

	throw(c.hdd.Sync())
	c.saveState(snap.durable)
	c.moved <- snap.durable
}

func tailAt(tails []Tail, off int64) Tail {
	for _, t := range tails {
		if off >= t.base && off < t.base+t.size {
			return t
		}
	}

	throwFmt("cell: offset %d is in no tail", off)

	return Tail{}
}

func (c *Cell) read(off int64, n int64) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		out, err := c.readFrom(c.snapshot(), off, n)

		if errors.Is(err, os.ErrClosed) && attempt == 0 {
			continue
		}

		return out, err
	}
}

func (c *Cell) readFrom(snap Snapshot, off int64, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n > snap.durable {
		return nil, errRange
	}

	out := make([]byte, n)

	for pos := off; pos < off+n; {
		end := off + n

		if pos < snap.flushed {
			end = min(end, snap.flushed)

			throw2(c.hdd.ReadAt(out[pos-off:end-off], pos))
		} else {
			t := tailAt(snap.tails, pos)
			end = min(end, t.base+t.size)

			if _, err := t.file.ReadAt(out[pos-off:end-off], pos-t.base); err != nil {
				return nil, err
			}
		}

		pos = end
	}

	return out, nil
}

func (c *Cell) status() (int64, int64) {
	snap := c.snapshot()

	return snap.durable, c.capacity - snap.head
}
