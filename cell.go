package main

import (
	"encoding/binary"
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
	"unsafe"

	"container/list"
)

var (
	errFull  = errors.New("cell: full")
	errRange = errors.New("cell: past the head")
)

const (
	blockSize    = 2 << 20
	tick         = 50 * time.Millisecond
	minFree      = 1 << 20
	maxFrameSize = 1 << 31
	ioAlign      = 4096
	writesQueue  = 1024
)

type WriteReq struct {
	id     uint64
	data   []byte
	cancel bool
	out    chan reply
}

type reqKey struct {
	out chan reply
	id  uint64
}

type ReadReq struct {
	num  int64
	done chan ReadResp
}

type ReadResp struct {
	f   *os.File
	err error
}

type Cell struct {
	load     string
	store    string
	hdd      *os.File
	capacity int64
	writes   chan WriteReq
	reads    chan ReadReq
	ready    chan struct{}
	freed    chan struct{}
	rolls    int
	num      int64
	current  *os.File
	size     int64
}

func runCell(listen, load, store, hdd string) {
	if listen == "" || load == "" || store == "" || hdd == "" {
		throwFmt("cell: -listen, -load, -store and -hdd are required")
	}

	c := openCell(load, store, hdd)

	if c.capacity-c.head() < minFree {
		throwFmt("cell: %s is full (%d of %d bytes used), not serving", hdd, c.head(), c.capacity)
	}

	ln := throw2(net.Listen("tcp", listen))

	slog.Info("cell: serving", "listen", listen, "block", c.num, "head", c.head(), "capacity", c.capacity)

	go c.writer()
	go c.flusher()
	go c.loader()

	stop := make(chan os.Signal, 1)

	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		os.Exit(0)
	}()

	for {
		conn := throw2(ln.Accept())

		go c.serve(conn)
	}
}

func openCell(load, store, hdd string) *Cell {
	for _, d := range []string{load, store} {
		throw(os.MkdirAll(d, 0o755))
	}

	dev := throw2(os.OpenFile(hdd, os.O_RDWR|syscall.O_DIRECT, 0))

	c := &Cell{
		load:     load,
		store:    store,
		hdd:      dev,
		capacity: throw2(dev.Seek(0, io.SeekEnd)),
		writes:   make(chan WriteReq, writesQueue),
		reads:    make(chan ReadReq, 4096),
		ready:    make(chan struct{}, 1),
		freed:    make(chan struct{}, 1),
	}

	c.num = c.lastBlock()
	c.openCurrent()

	if c.size == blockSize {
		c.roll()
	}

	return c
}

func (c *Cell) currentPath(num int64) string {
	return filepath.Join(c.store, strconv.FormatInt(num, 10)+".current")
}

func (c *Cell) readyPath(num int64) string {
	return filepath.Join(c.store, strconv.FormatInt(num, 10))
}

func (c *Cell) loadPath(num int64) string {
	return filepath.Join(c.load, strconv.FormatInt(num, 10))
}

func blockNumbers(dir, suffix string) []int64 {
	var nums []int64

	for _, entry := range throw2(os.ReadDir(dir)) {
		name := entry.Name()

		if !strings.HasSuffix(name, suffix) {
			continue
		}

		if n, err := strconv.ParseInt(strings.TrimSuffix(name, suffix), 10, 64); err == nil {
			nums = append(nums, n)
		}
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	return nums
}

func (c *Cell) lastBlock() int64 {
	var num int64

	for _, n := range blockNumbers(c.store, "") {
		num = max(num, n+1)
	}

	for _, n := range blockNumbers(c.store, ".current") {
		num = max(num, n)
	}

	return num
}

func (c *Cell) openCurrent() {
	c.current = throw2(os.OpenFile(c.currentPath(c.num), os.O_RDWR|os.O_CREATE, 0o644))
	c.size = throw2(c.current.Stat()).Size()

	if c.size > blockSize {
		throwFmt("cell: %s is %d bytes, longer than a block", c.currentPath(c.num), c.size)
	}
}

func syncDir(path string) {
	d := throw2(os.Open(path))

	throw(d.Sync())
	throw(d.Close())
}

func (c *Cell) roll() {
	c.rolls++

	throw(c.current.Sync())
	throw(c.current.Close())
	throw(os.Rename(c.currentPath(c.num), c.readyPath(c.num)))

	c.num++
	c.openCurrent()
	syncDir(c.store)

	select {
	case c.ready <- struct{}{}:
	default:
	}
}

func (c *Cell) head() int64 {
	var head int64

	for _, n := range blockNumbers(c.store, ".current") {
		head = n*blockSize + throw2(os.Stat(c.currentPath(n))).Size()
	}

	return head
}

func (c *Cell) writer() {
	for range time.Tick(tick) {
		var batch []WriteReq

	drain:
		for {
			select {
			case req := <-c.writes:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		if len(batch) == 0 {
			continue
		}

		cancelled := map[reqKey]bool{}

		for _, req := range batch {
			if req.cancel {
				cancelled[reqKey{req.out, req.id}] = true
			}
		}

		replies := make([]reply, len(batch))
		start := time.Now()
		bytes := 0

		c.rolls = 0

		for i, req := range batch {
			switch {
			case req.cancel:
				replies[i] = reply{op: opCancel | opReply, id: req.id, silent: true}
			case cancelled[reqKey{req.out, req.id}]:
				replies[i] = reply{op: opFail, id: req.id, body: []byte{codeCancelled}}
			default:
				offset, err := c.put(req.data)

				bytes += len(req.data)
				replies[i] = c.appendReply(req.id, offset, err)
			}
		}

		written := time.Now()

		throw(c.current.Sync())

		synced := time.Now()

		for i, req := range batch {
			req.out <- replies[i]
		}

		slog.Debug("cell: tick", "batch", len(batch), "bytes", bytes, "rolls", c.rolls,
			"write", written.Sub(start), "sync", synced.Sub(written), "total", time.Since(start))
	}
}

func (c *Cell) appendReply(id uint64, offset int64, err error) reply {
	if errors.Is(err, errFull) {
		return reply{op: opFail, id: id, body: []byte{codeFull}}
	}

	throw(err)

	return reply{op: opAppend | opReply, id: id, body: binary.BigEndian.AppendUint64(nil, uint64(offset))}
}

func (c *Cell) put(data []byte) (int64, error) {
	offset := c.num*blockSize + c.size

	if offset+int64(len(data)) > c.capacity {
		return 0, errFull
	}

	for len(data) > 0 {
		n := min(int64(len(data)), blockSize-c.size)

		if _, err := c.current.WriteAt(data[:n], c.size); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				throw(err)
			}

			slog.Warn("cell: store is full, waiting for the flusher", "block", c.num)

			<-c.freed

			continue
		}

		c.size += n
		data = data[n:]

		if c.size == blockSize {
			c.roll()
		}
	}

	return offset, nil
}

func alignedBlock() []byte {
	raw := make([]byte, blockSize+ioAlign)
	skew := int(uintptr(unsafe.Pointer(&raw[0])) % ioAlign)

	if skew != 0 {
		skew = ioAlign - skew
	}

	return raw[skew : skew+blockSize]
}

func (c *Cell) flusher() {
	buf := alignedBlock()

	for {
		c.flush(buf)
		<-c.ready
	}
}

func (c *Cell) flush(buf []byte) {
	full := blockNumbers(c.store, "")

	if len(full) == 0 {
		return
	}

	start := time.Now()

	for _, num := range full {
		f := throw2(os.Open(c.readyPath(num)))

		throw2(io.ReadFull(f, buf))
		throw(f.Close())
		throw2(c.hdd.WriteAt(buf, num*blockSize))
	}

	wrote := time.Now()

	throw(c.hdd.Sync())

	synced := time.Now()

	for _, num := range full {
		throw(os.Remove(c.readyPath(num)))
	}

	syncDir(c.store)

	select {
	case c.freed <- struct{}{}:
	default:
	}

	slog.Debug("cell: flushed", "blocks", len(full), "first", full[0], "last", full[len(full)-1],
		"write", wrote.Sub(start), "sync", synced.Sub(wrote), "remove", time.Since(synced))
}

func (c *Cell) read(off int64, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n > c.capacity {
		return nil, errRange
	}

	out := make([]byte, n)

	for pos := off; pos < off+n; {
		num := pos / blockSize
		end := min(off+n, (num+1)*blockSize)
		f := throw2(c.block(num))
		_, err := f.ReadAt(out[pos-off:end-off], pos-num*blockSize)

		throw(f.Close())

		if errors.Is(err, io.EOF) {
			return nil, errRange
		}

		throw(err)

		pos = end
	}

	return out, nil
}

func (c *Cell) block(num int64) (*os.File, error) {
	done := make(chan ReadResp, 1)

	c.reads <- ReadReq{num: num, done: done}

	resp := <-done

	return resp.f, resp.err
}

type loaded struct {
	order *list.List
	byNum map[int64]*list.Element
}

func (c *Cell) loader() {
	l := &loaded{order: list.New(), byNum: map[int64]*list.Element{}}
	buf := alignedBlock()

	c.warm(l)

	for req := range c.reads {
		var resp ReadResp

		try(func() {
			resp.f = c.open(l, buf, req.num)
		}).catch(func(exc *Exception) {
			resp.err = exc.asError()
		})

		req.done <- resp
	}
}

func (c *Cell) warm(l *loaded) {
	var copies []os.FileInfo

	for _, entry := range throw2(os.ReadDir(c.load)) {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			throw(os.Remove(filepath.Join(c.load, entry.Name())))

			continue
		}

		if info, err := entry.Info(); err == nil {
			copies = append(copies, info)
		}
	}

	sort.Slice(copies, func(i, j int) bool { return copies[i].ModTime().Before(copies[j].ModTime()) })

	for _, info := range copies {
		num := throw2(strconv.ParseInt(info.Name(), 10, 64))

		l.byNum[num] = l.order.PushFront(num)
	}
}

func (c *Cell) open(l *loaded, buf []byte, num int64) *os.File {
	if e, ok := l.byNum[num]; ok {
		if f, err := os.Open(c.loadPath(num)); err == nil {
			l.order.MoveToFront(e)

			return f
		}

		l.order.Remove(e)
		delete(l.byNum, num)
	}

	if f, err := os.Open(c.currentPath(num)); err == nil {
		return f
	}

	if f, err := os.Open(c.readyPath(num)); err == nil {
		return f
	}

	start := time.Now()

	throw2(c.hdd.ReadAt(buf, num*blockSize))

	slog.Debug("cell: load", "block", num, "read", time.Since(start))

	tmp := c.loadPath(num) + ".tmp"

	for {
		err := os.WriteFile(tmp, buf, 0o644)

		if err == nil {
			break
		}

		os.Remove(tmp)

		if !errors.Is(err, syscall.ENOSPC) || l.order.Len() == 0 {
			throw(err)
		}

		last := l.order.Back()
		old := last.Value.(int64)

		l.order.Remove(last)
		delete(l.byNum, old)
		throw(os.Remove(c.loadPath(old)))
	}

	throw(os.Rename(tmp, c.loadPath(num)))

	l.byNum[num] = l.order.PushFront(num)

	return throw2(os.Open(c.loadPath(num)))
}

func (c *Cell) status() (int64, int64) {
	head := c.head()

	return head, c.capacity - head
}
