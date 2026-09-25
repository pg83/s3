package main

import (
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
	"sync"
	"syscall"
	"time"
)

var (
	errFull  = errors.New("cell: full")
	errRange = errors.New("cell: past the head")
)

const (
	blockSize    = 2 << 20
	tick         = 100 * time.Millisecond
	lruBudget    = 1 << 30
	lruSweep     = 10 * time.Second
	minFree      = 1 << 20
	maxFrameSize = 1 << 31
)

type WriteReq struct {
	data []byte
	done chan WriteResp
}

type WriteResp struct {
	offset int64
	err    error
}

type Cell struct {
	ssd      string
	hdd      *os.File
	capacity int64
	writes   chan WriteReq
	ready    chan struct{}
	fill     sync.Mutex
	num      int64
	current  *os.File
	size     int64
}

func runCell(listen, ssd, hdd string) {
	if listen == "" || ssd == "" || hdd == "" {
		throwFmt("cell: -listen, -ssd and -hdd are required")
	}

	c := openCell(ssd, hdd)

	if c.capacity-c.head() < minFree {
		throwFmt("cell: %s is full (%d of %d bytes used), not serving", hdd, c.head(), c.capacity)
	}

	ln := throw2(net.Listen("tcp", listen))

	slog.Info("cell: serving", "listen", listen, "block", c.num, "head", c.head(), "capacity", c.capacity)

	go c.writer()
	go c.flusher()
	go c.sweeper()

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

func openCell(ssd, hdd string) *Cell {
	for _, d := range []string{ssd, filepath.Join(ssd, "ready"), filepath.Join(ssd, "lru")} {
		throw(os.MkdirAll(d, 0o755))
	}

	dev := throw2(os.OpenFile(hdd, os.O_RDWR, 0))

	c := &Cell{
		ssd:      ssd,
		hdd:      dev,
		capacity: throw2(dev.Seek(0, io.SeekEnd)),
		writes:   make(chan WriteReq, 4096),
		ready:    make(chan struct{}, 1),
	}

	c.num = c.lastBlock()
	c.openCurrent()

	if c.size == blockSize {
		c.roll()
	}

	return c
}

func (c *Cell) currentPath(num int64) string {
	return filepath.Join(c.ssd, "current."+strconv.FormatInt(num, 10))
}

func (c *Cell) readyPath(num int64) string {
	return filepath.Join(c.ssd, "ready", strconv.FormatInt(num, 10))
}

func (c *Cell) lruPath(num int64) string {
	return filepath.Join(c.ssd, "lru", strconv.FormatInt(num, 10))
}

func blockNumbers(dir, prefix string) []int64 {
	var nums []int64

	for _, entry := range throw2(os.ReadDir(dir)) {
		name := entry.Name()

		if !strings.HasPrefix(name, prefix) || strings.HasSuffix(name, ".tmp") {
			continue
		}

		nums = append(nums, throw2(strconv.ParseInt(strings.TrimPrefix(name, prefix), 10, 64)))
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	return nums
}

func (c *Cell) lastBlock() int64 {
	var num int64

	for _, n := range blockNumbers(filepath.Join(c.ssd, "ready"), "") {
		num = max(num, n+1)
	}

	for _, n := range blockNumbers(c.ssd, "current.") {
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
	throw(c.current.Sync())
	throw(c.current.Close())
	throw(os.Rename(c.currentPath(c.num), c.readyPath(c.num)))
	syncDir(filepath.Join(c.ssd, "ready"))

	c.num++
	c.openCurrent()
	syncDir(c.ssd)

	select {
	case c.ready <- struct{}{}:
	default:
	}
}

func (c *Cell) head() int64 {
	var head int64

	for _, n := range blockNumbers(c.ssd, "current.") {
		head = n*blockSize + throw2(os.Stat(c.currentPath(n))).Size()
	}

	return head
}

func (c *Cell) append(data []byte) (int64, error) {
	done := make(chan WriteResp, 1)

	c.writes <- WriteReq{data: data, done: done}

	resp := <-done

	return resp.offset, resp.err
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

		resps := make([]WriteResp, len(batch))

		for i, req := range batch {
			resps[i].offset, resps[i].err = c.put(req.data)
		}

		throw(c.current.Sync())

		for i, req := range batch {
			req.done <- resps[i]
		}
	}
}

func (c *Cell) put(data []byte) (int64, error) {
	offset := c.num*blockSize + c.size

	if offset+int64(len(data)) > c.capacity {
		return 0, errFull
	}

	for len(data) > 0 {
		n := min(int64(len(data)), blockSize-c.size)

		throw2(c.current.WriteAt(data[:n], c.size))

		c.size += n
		data = data[n:]

		if c.size == blockSize {
			c.roll()
		}
	}

	return offset, nil
}

func (c *Cell) flusher() {
	for {
		c.flush()
		<-c.ready
	}
}

func (c *Cell) flush() {
	buf := make([]byte, blockSize)

	for _, num := range blockNumbers(filepath.Join(c.ssd, "ready"), "") {
		f := throw2(os.Open(c.readyPath(num)))

		throw2(io.ReadFull(f, buf))
		throw(f.Close())
		throw2(c.hdd.WriteAt(buf, num*blockSize))
		throw(c.hdd.Sync())
		throw(os.Remove(c.readyPath(num)))
	}

	syncDir(filepath.Join(c.ssd, "ready"))
}

func (c *Cell) read(off int64, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n > c.capacity {
		return nil, errRange
	}

	out := make([]byte, n)

	for pos := off; pos < off+n; {
		num := pos / blockSize
		end := min(off+n, (num+1)*blockSize)
		f := c.open(num)
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

func (c *Cell) open(num int64) *os.File {
	if f, err := os.Open(c.lruPath(num)); err == nil {
		os.Chtimes(c.lruPath(num), time.Now(), time.Now())

		return f
	}

	if err := os.Link(c.readyPath(num), c.lruPath(num)); err == nil || errors.Is(err, os.ErrExist) {
		if f, err := os.Open(c.lruPath(num)); err == nil {
			return f
		}
	}

	if f, err := os.Open(c.currentPath(num)); err == nil {
		return f
	}

	c.fill.Lock()

	defer c.fill.Unlock()

	if f, err := os.Open(c.lruPath(num)); err == nil {
		return f
	}

	buf := make([]byte, blockSize)

	throw2(c.hdd.ReadAt(buf, num*blockSize))

	tmp := c.lruPath(num) + ".tmp"

	throw(os.WriteFile(tmp, buf, 0o644))
	throw(os.Rename(tmp, c.lruPath(num)))

	return throw2(os.Open(c.lruPath(num)))
}

func (c *Cell) sweeper() {
	for range time.Tick(lruSweep) {
		try(func() {
			c.sweep()
		}).catch(func(exc *Exception) {
			slog.Error("cell: lru", "err", exc.error())
		})
	}
}

func (c *Cell) sweep() {
	dir := filepath.Join(c.ssd, "lru")
	entries := throw2(os.ReadDir(dir))

	var total int64

	infos := make([]os.FileInfo, 0, len(entries))

	for _, entry := range entries {
		info, err := entry.Info()

		if err != nil {
			continue
		}

		total += info.Size()
		infos = append(infos, info)
	}

	sort.Slice(infos, func(i, j int) bool { return infos[i].ModTime().Before(infos[j].ModTime()) })

	for _, info := range infos {
		if total <= lruBudget {
			break
		}

		if os.Remove(filepath.Join(dir, info.Name())) == nil {
			total -= info.Size()
		}
	}
}

func (c *Cell) status() (int64, int64) {
	head := c.head()

	return head, c.capacity - head
}
