package main

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	nbdBlock         = 4 << 20
	nbdServerMagic   = 0x4e42444d41474943
	nbdOptionMagic   = 0x49484156454f5054
	nbdReplyMagic    = 0x3e889045565a9
	nbdRequestMagic  = 0x25609513
	nbdSimpleMagic   = 0x67446698
	nbdFixedNewstyle = 1
	nbdNoZeroes      = 2
	nbdOptExportName = 1
	nbdOptAbort      = 2
	nbdOptList       = 3
	nbdOptInfo       = 6
	nbdOptGo         = 7
	nbdRepAck        = 1
	nbdRepServer     = 2
	nbdRepInfo       = 3
	nbdRepErrUnsup   = 1<<31 + 1
	nbdRepErrInvalid = 1<<31 + 3
	nbdRepErrUnknown = 1<<31 + 6
	nbdInfoExport    = 0
	nbdFlagHasFlags  = 1
	nbdFlagSendFlush = 1 << 2
	nbdFlagSendFua   = 1 << 3
	nbdFlagSendTrim  = 1 << 5
	nbdFlagSendZero  = 1 << 6
	nbdCmdRead       = 0
	nbdCmdWrite      = 1
	nbdCmdDisc       = 2
	nbdCmdFlush      = 3
	nbdCmdTrim       = 4
	nbdCmdZero       = 6
	nbdEIO           = 5
	nbdEINVAL        = 22
	nbdPutWindow     = 16
	nbdMaxIO         = 32 << 20
)

type NbdOp struct {
	conn    *NbdConn
	cmd     uint16
	cookie  uint64
	offset  int64
	length  int64
	data    []byte
	pending int
	err     uint32
}

type NbdPart struct {
	op    *NbdOp
	index int64
	from  int64
	n     int64
	at    int64
}

type NbdReply struct {
	cookie uint64
	err    uint32
	data   []byte
}

type NbdConn struct {
	replies chan NbdReply
	ended   chan int64
}

type NbdBlock struct {
	data     []byte
	zero     bool
	dirty    bool
	frozen   bool
	version  uint64
	fetching bool
	waiting  []NbdPart
	elem     *list.Element
}

type NbdItem struct {
	index   int64
	data    []byte
	version uint64
	prev    uint64
}

type NbdJob struct {
	gen    uint64
	items  []NbdItem
	base   map[int64]uint64
	forget []string
	err    error
	took   time.Duration
}

type NbdFetch struct {
	index int64
	data  []byte
	err   error
}

type NbdStats struct {
	dirty    int
	resident int
	gen      uint64
}

type NbdMap struct {
	Gen    uint64           `json:"gen"`
	Size   int64            `json:"size"`
	Blocks map[int64]uint64 `json:"blocks"`
}

type NbdDisk struct {
	bucket    *BucketClient
	name      string
	size      int64
	blocks    map[int64]*NbdBlock
	committed map[int64]uint64
	dirty     map[int64]bool
	lru       *list.List
	cacheMax  int
	dirtyMax  int
	baseEvery int
	gen       uint64
	next      uint64
	baseGen   uint64
	logs      []uint64
	ops       chan *NbdOp
	fetched   chan NbdFetch
	commits   chan *NbdJob
	stats     chan chan NbdStats
	closing   chan chan struct{}
	closed    chan struct{}
	stopping  bool
	working   bool
}

func runNbd(endpoint, bucket, name string, size int64, socket string, tick time.Duration, cache, dirty int64, baseEvery int, metrics string) {
	if size <= 0 || size%nbdBlock != 0 {
		throwFmt("nbd: size %d is not a positive multiple of %d", size, nbdBlock)
	}

	d := &NbdDisk{
		bucket:    newBucketClient(endpoint, bucket),
		name:      name,
		size:      size,
		blocks:    map[int64]*NbdBlock{},
		committed: map[int64]uint64{},
		dirty:     map[int64]bool{},
		lru:       list.New(),
		cacheMax:  max(1, int(cache/nbdBlock)),
		dirtyMax:  max(1, int(dirty/nbdBlock)),
		baseEvery: max(1, baseEvery),
		ops:       make(chan *NbdOp),
		fetched:   make(chan NbdFetch),
		commits:   make(chan *NbdJob),
		stats:     make(chan chan NbdStats),
		closing:   make(chan chan struct{}),
	}

	d.load()

	beforeExit = d.shutdown

	serveMetrics(metrics, d.gauges)

	ln := d.listen(socket)

	go d.run(tick)

	slog.Info("nbd: serving", "disk", name, "size", size, "gen", d.gen, "blocks", len(d.committed), "socket", socket)

	for {
		conn, err := ln.Accept()

		if err != nil {
			slog.Warn("nbd: accept", "err", err)
			metricAdd("s3_nbd_errors_total", `op="accept"`, 1)
			time.Sleep(acceptPause)

			continue
		}

		go d.serve(conn)
	}
}

func (d *NbdDisk) listen(socket string) net.Listener {
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		throw(err)
	}

	ln := throw2(net.Listen("unix", socket))

	throw(os.Chmod(socket, 0o666))

	return sys.accepts(ln)
}

func (d *NbdDisk) blockKey(index int64, gen uint64) string {
	return fmt.Sprintf("%s/b/%08x.%016x", d.name, index, gen)
}

func (d *NbdDisk) logKey(gen uint64) string {
	return fmt.Sprintf("%s/log/%016x", d.name, gen)
}

func (d *NbdDisk) baseKey(gen uint64) string {
	return fmt.Sprintf("%s/base/%016x", d.name, gen)
}

func genOf(key string) uint64 {
	return throw2(strconv.ParseUint(key[strings.LastIndex(key, "/")+1:], 16, 64))
}

func (d *NbdDisk) readMap(key string) NbdMap {
	var m NbdMap

	throw(json.Unmarshal(throw2(d.bucket.get(key)), &m))

	if m.Size != d.size {
		throwFmt("nbd: disk %s is %d bytes, not %d", d.name, m.Size, d.size)
	}

	return m
}

func (d *NbdDisk) load() {
	bases := d.bucket.list(d.name + "/base/")

	if len(bases) > 0 {
		m := d.readMap(bases[len(bases)-1])

		d.baseGen, d.gen = m.Gen, m.Gen

		for index, gen := range m.Blocks {
			d.committed[index] = gen
		}
	}

	for _, key := range d.bucket.list(d.name + "/log/") {
		gen := genOf(key)

		if gen <= d.baseGen {
			continue
		}

		m := d.readMap(key)

		for index, g := range m.Blocks {
			if g == 0 {
				delete(d.committed, index)
			} else {
				d.committed[index] = g
			}
		}

		d.gen = gen
		d.logs = append(d.logs, gen)
	}

	d.next = d.gen + 1
}

func (d *NbdDisk) serve(conn net.Conn) {
	defer conn.Close()

	var ok bool

	try(func() {
		ok = d.handshake(conn)
	}).catch(func(exc *Exception) {
		slog.Warn("nbd: handshake", "err", exc.error())
		metricAdd("s3_nbd_errors_total", `op="handshake"`, 1)
	})

	if !ok {
		return
	}

	c := &NbdConn{replies: make(chan NbdReply, 256), ended: make(chan int64, 1)}

	go d.read(conn, c)

	d.write(conn, c)
}

func (d *NbdDisk) handshake(conn net.Conn) bool {
	var hello [18]byte

	binary.BigEndian.PutUint64(hello[0:], nbdServerMagic)
	binary.BigEndian.PutUint64(hello[8:], nbdOptionMagic)
	binary.BigEndian.PutUint16(hello[16:], nbdFixedNewstyle|nbdNoZeroes)
	throw2(conn.Write(hello[:]))

	var flags [4]byte

	throw2(io.ReadFull(conn, flags[:]))

	client := binary.BigEndian.Uint32(flags[:])

	if client&nbdFixedNewstyle == 0 {
		throwFmt("client without fixed newstyle")
	}

	for {
		var head [16]byte

		throw2(io.ReadFull(conn, head[:]))

		if binary.BigEndian.Uint64(head[0:]) != nbdOptionMagic {
			throwFmt("bad option magic")
		}

		opt := binary.BigEndian.Uint32(head[8:])
		n := binary.BigEndian.Uint32(head[12:])

		if n > 4096 {
			throwFmt("option %d of %d bytes", opt, n)
		}

		data := make([]byte, n)

		throw2(io.ReadFull(conn, data))

		switch opt {
		case nbdOptExportName:
			if !d.known(string(data)) {
				throwFmt("no export %q", data)
			}

			out := make([]byte, 10, 10+124)

			binary.BigEndian.PutUint64(out[0:], uint64(d.size))
			binary.BigEndian.PutUint16(out[8:], d.flags())

			if client&nbdNoZeroes == 0 {
				out = append(out, make([]byte, 124)...)
			}

			throw2(conn.Write(out))

			return true
		case nbdOptAbort:
			optionReply(conn, opt, nbdRepAck, nil)

			return false
		case nbdOptList:
			entry := binary.BigEndian.AppendUint32(nil, uint32(len(d.name)))

			optionReply(conn, opt, nbdRepServer, append(entry, d.name...))
			optionReply(conn, opt, nbdRepAck, nil)
		case nbdOptInfo, nbdOptGo:
			if len(data) < 6 || int(binary.BigEndian.Uint32(data))+6 > len(data) {
				optionReply(conn, opt, nbdRepErrInvalid, nil)

				continue
			}

			if !d.known(string(data[4 : 4+binary.BigEndian.Uint32(data)])) {
				optionReply(conn, opt, nbdRepErrUnknown, nil)

				continue
			}

			info := binary.BigEndian.AppendUint16(nil, nbdInfoExport)

			info = binary.BigEndian.AppendUint64(info, uint64(d.size))
			info = binary.BigEndian.AppendUint16(info, d.flags())

			optionReply(conn, opt, nbdRepInfo, info)
			optionReply(conn, opt, nbdRepAck, nil)

			if opt == nbdOptGo {
				return true
			}
		default:
			optionReply(conn, opt, nbdRepErrUnsup, nil)
		}
	}
}

func (d *NbdDisk) known(export string) bool {
	return export == "" || export == d.name
}

func (d *NbdDisk) flags() uint16 {
	return nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendFua | nbdFlagSendTrim | nbdFlagSendZero
}

func optionReply(conn net.Conn, opt, kind uint32, data []byte) {
	out := binary.BigEndian.AppendUint64(nil, nbdReplyMagic)

	out = binary.BigEndian.AppendUint32(out, opt)
	out = binary.BigEndian.AppendUint32(out, kind)
	out = binary.BigEndian.AppendUint32(out, uint32(len(data)))

	throw2(conn.Write(append(out, data...)))
}

func (d *NbdDisk) read(conn net.Conn, c *NbdConn) {
	var sent int64

	defer func() { c.ended <- sent }()

	r := bufio.NewReaderSize(conn, 1<<20)

	try(func() {
		for {
			var head [28]byte

			if _, err := io.ReadFull(r, head[:]); err != nil {
				return
			}

			if binary.BigEndian.Uint32(head[0:]) != nbdRequestMagic {
				throwFmt("bad request magic")
			}

			op := &NbdOp{
				conn:   c,
				cmd:    binary.BigEndian.Uint16(head[6:]),
				cookie: binary.BigEndian.Uint64(head[8:]),
				offset: int64(binary.BigEndian.Uint64(head[16:])),
				length: int64(binary.BigEndian.Uint32(head[24:])),
			}

			if op.cmd == nbdCmdDisc {
				return
			}

			if op.cmd == nbdCmdWrite {
				if op.length > nbdMaxIO {
					throwFmt("write of %d bytes", op.length)
				}

				op.data = make([]byte, op.length)
				throw2(io.ReadFull(r, op.data))
			}

			metricAdd("s3_nbd_requests_total", `cmd="`+nbdCmdName(op.cmd)+`"`, 1)
			sent++

			if err := d.check(op); err != 0 {
				c.replies <- NbdReply{cookie: op.cookie, err: err}

				continue
			}

			d.ops <- op
		}
	}).catch(func(exc *Exception) {
		slog.Warn("nbd: read", "err", exc.error())
		metricAdd("s3_nbd_errors_total", `op="read"`, 1)
	})
}

func (d *NbdDisk) check(op *NbdOp) uint32 {
	switch op.cmd {
	case nbdCmdRead, nbdCmdWrite, nbdCmdTrim, nbdCmdZero:
		if op.offset < 0 || op.length > nbdMaxIO || op.offset+op.length > d.size {
			return nbdEINVAL
		}
	case nbdCmdFlush:
	default:
		return nbdEINVAL
	}

	return 0
}

func (d *NbdDisk) write(conn net.Conn, c *NbdConn) {
	w := bufio.NewWriterSize(conn, 1<<20)
	broken := false
	end := int64(-1)

	for done := int64(0); end < 0 || done < end; {
		select {
		case r := <-c.replies:
			done++

			if broken {
				continue
			}

			var head [16]byte

			binary.BigEndian.PutUint32(head[0:], nbdSimpleMagic)
			binary.BigEndian.PutUint32(head[4:], r.err)
			binary.BigEndian.PutUint64(head[8:], r.cookie)

			_, err := w.Write(head[:])

			if err == nil && r.err == 0 {
				_, err = w.Write(r.data)
			}

			if err == nil && len(c.replies) == 0 {
				err = w.Flush()
			}

			if err != nil {
				broken = true
			}
		case end = <-c.ended:
		}
	}

	if !broken {
		w.Flush()
	}
}

func nbdCmdName(cmd uint16) string {
	switch cmd {
	case nbdCmdRead:
		return "read"
	case nbdCmdWrite:
		return "write"
	case nbdCmdFlush:
		return "flush"
	case nbdCmdTrim:
		return "trim"
	case nbdCmdZero:
		return "zero"
	}

	return "other"
}

func (d *NbdDisk) run(tick time.Duration) {
	ticker := time.NewTicker(tick)

	try(func() {
		for {
			ops := d.ops

			if d.stopping || len(d.dirty) >= d.dirtyMax {
				ops = nil
			}

			select {
			case op := <-ops:
				d.submit(op)
			case f := <-d.fetched:
				d.install(f)
			case job := <-d.commits:
				d.settle(job)
			case <-ticker.C:
				d.commit()
			case out := <-d.stats:
				out <- NbdStats{dirty: len(d.dirty), resident: d.lru.Len() + len(d.dirty), gen: d.gen}
			case done := <-d.closing:
				d.stopping = true
				d.closed = done
				d.commit()
				d.finish()
			}
		}
	}).catch(func(exc *Exception) {
		slog.Error("nbd: disk", "err", exc.error())
		os.Exit(1)
	})
}

func (d *NbdDisk) shutdown() {
	done := make(chan struct{})

	d.closing <- done
	<-done
}

func (d *NbdDisk) finish() {
	if d.stopping && !d.working && len(d.dirty) == 0 && d.closed != nil {
		close(d.closed)
		d.closed = nil
	}
}

func (d *NbdDisk) gauges() []gauge {
	out := make(chan NbdStats, 1)

	d.stats <- out

	s := <-out

	return []gauge{
		{name: "s3_nbd_dirty_blocks", value: float64(s.dirty)},
		{name: "s3_nbd_resident_blocks", value: float64(s.resident)},
		{name: "s3_nbd_generation", value: float64(s.gen)},
	}
}

func (d *NbdDisk) block(index int64) *NbdBlock {
	b, ok := d.blocks[index]

	if !ok {
		b = &NbdBlock{}
		d.blocks[index] = b
	}

	return b
}

func (d *NbdDisk) submit(op *NbdOp) {
	if op.cmd == nbdCmdFlush || op.length == 0 {
		op.conn.replies <- NbdReply{cookie: op.cookie}

		return
	}

	if op.cmd == nbdCmdRead {
		op.data = make([]byte, op.length)
	}

	var parts []NbdPart

	for at := int64(0); at < op.length; {
		pos := op.offset + at
		n := min(op.length-at, nbdBlock-pos%nbdBlock)

		parts = append(parts, NbdPart{op: op, index: pos / nbdBlock, from: pos % nbdBlock, n: n, at: at})
		at += n
	}

	op.pending = len(parts)

	for _, p := range parts {
		d.part(p)
	}
}

func (p NbdPart) whole() bool {
	return p.from == 0 && p.n == nbdBlock
}

func (d *NbdDisk) part(p NbdPart) {
	b := d.block(p.index)

	if b.fetching {
		b.waiting = append(b.waiting, p)

		return
	}

	_, stored := d.committed[p.index]
	needs := b.data == nil && !b.zero && stored

	switch p.op.cmd {
	case nbdCmdWrite, nbdCmdZero:
		needs = needs && !p.whole()
	case nbdCmdTrim:
		needs = false
	}

	if needs {
		b.fetching = true
		b.waiting = append(b.waiting, p)

		go d.fetch(p.index, d.committed[p.index])

		return
	}

	d.apply(p, b)
}

func (d *NbdDisk) fetch(index int64, gen uint64) {
	start := time.Now()
	data, err := d.bucket.get(d.blockKey(index, gen))

	if err == nil && len(data) != nbdBlock {
		err = fmt.Errorf("block %d of %d bytes", index, len(data))
	}

	metricTime("s3_nbd_fetch_seconds", "", time.Since(start))
	d.fetched <- NbdFetch{index: index, data: data, err: err}
}

func (d *NbdDisk) install(f NbdFetch) {
	b := d.blocks[f.index]
	waiting := b.waiting

	b.fetching = false
	b.waiting = nil

	if f.err != nil {
		slog.Warn("nbd: fetch", "block", f.index, "err", f.err)
		metricAdd("s3_nbd_errors_total", `op="fetch"`, 1)

		for _, p := range waiting {
			p.op.err = nbdEIO
			d.done(p.op)
		}

		d.forgetIdle(f.index, b)

		return
	}

	b.data = f.data
	d.clean(f.index, b)

	for _, p := range waiting {
		d.part(p)
	}
}

func (d *NbdDisk) apply(p NbdPart, b *NbdBlock) {
	switch p.op.cmd {
	case nbdCmdRead:
		if b.data != nil {
			copy(p.op.data[p.at:p.at+p.n], b.data[p.from:p.from+p.n])
		}

		d.touch(b)
	case nbdCmdWrite:
		d.writable(p.index, b)
		copy(b.data[p.from:p.from+p.n], p.op.data[p.at:p.at+p.n])
		d.dirtied(p.index, b)
	case nbdCmdTrim, nbdCmdZero:
		_, stored := d.committed[p.index]

		if p.whole() && (stored || b.dirty) {
			d.uncache(b)
			b.data = nil
			b.frozen = false
			b.zero = true
			d.dirtied(p.index, b)
		} else if !p.whole() && p.op.cmd == nbdCmdZero && b.data != nil {
			d.writable(p.index, b)
			clear(b.data[p.from : p.from+p.n])
			d.dirtied(p.index, b)
		}
	}

	d.forgetIdle(p.index, b)
	d.done(p.op)
}

func (d *NbdDisk) done(op *NbdOp) {
	op.pending--

	if op.pending > 0 {
		return
	}

	r := NbdReply{cookie: op.cookie, err: op.err}

	if op.cmd == nbdCmdRead && op.err == 0 {
		r.data = op.data
	}

	op.conn.replies <- r
}

func (d *NbdDisk) writable(index int64, b *NbdBlock) {
	if b.data == nil {
		b.data = make([]byte, nbdBlock)
		b.zero = false
	} else if b.frozen {
		b.data = bytes.Clone(b.data)
		b.frozen = false
	}
}

func (d *NbdDisk) dirtied(index int64, b *NbdBlock) {
	b.version++

	if !b.dirty {
		b.dirty = true
		d.dirty[index] = true
		d.uncache(b)
	}
}

func (d *NbdDisk) touch(b *NbdBlock) {
	if b.elem != nil {
		d.lru.MoveToFront(b.elem)
	}
}

func (d *NbdDisk) uncache(b *NbdBlock) {
	if b.elem != nil {
		d.lru.Remove(b.elem)
		b.elem = nil
	}
}

func (d *NbdDisk) clean(index int64, b *NbdBlock) {
	b.elem = d.lru.PushFront(index)

	for d.lru.Len() > d.cacheMax {
		old := d.lru.Remove(d.lru.Back()).(int64)
		ob := d.blocks[old]

		ob.elem = nil
		ob.data = nil
		d.forgetIdle(old, ob)
	}
}

func (d *NbdDisk) forgetIdle(index int64, b *NbdBlock) {
	if b.data == nil && !b.zero && !b.dirty && !b.fetching && len(b.waiting) == 0 {
		delete(d.blocks, index)
	}
}

func (d *NbdDisk) commit() {
	if d.working || len(d.dirty) == 0 {
		return
	}

	job := &NbdJob{gen: d.next}

	d.next++

	for index := range d.dirty {
		b := d.blocks[index]
		item := NbdItem{index: index, version: b.version, prev: d.committed[index]}

		if !b.zero {
			item.data = b.data
			b.frozen = true
		}

		job.items = append(job.items, item)
	}

	sort.Slice(job.items, func(i, j int) bool { return job.items[i].index < job.items[j].index })

	if len(d.logs)+1 >= d.baseEvery {
		job.base = map[int64]uint64{}

		for index, gen := range d.committed {
			job.base[index] = gen
		}

		for _, item := range job.items {
			if item.data == nil {
				delete(job.base, item.index)
			} else {
				job.base[item.index] = job.gen
			}
		}

		for _, gen := range d.logs {
			job.forget = append(job.forget, d.logKey(gen))
		}

		job.forget = append(job.forget, d.logKey(job.gen))

		if d.baseGen != 0 {
			job.forget = append(job.forget, d.baseKey(d.baseGen))
		}
	}

	d.working = true

	go d.upload(job)
}

func (d *NbdDisk) upload(job *NbdJob) {
	start := time.Now()
	window := make(chan struct{}, nbdPutWindow)
	errs := make(chan error, len(job.items))

	for _, item := range job.items {
		if item.data == nil {
			errs <- nil

			continue
		}

		window <- struct{}{}

		go func() {
			errs <- d.bucket.put(d.blockKey(item.index, job.gen), item.data)
			<-window
		}()
	}

	for range job.items {
		if err := <-errs; err != nil && job.err == nil {
			job.err = err
		}
	}

	delta := NbdMap{Gen: job.gen, Size: d.size, Blocks: map[int64]uint64{}}

	for _, item := range job.items {
		if item.data == nil {
			delta.Blocks[item.index] = 0
		} else {
			delta.Blocks[item.index] = job.gen
		}
	}

	if job.err == nil {
		job.err = d.bucket.put(d.logKey(job.gen), throw2(json.Marshal(delta)))
	}

	if job.err == nil && job.base != nil {
		base := NbdMap{Gen: job.gen, Size: d.size, Blocks: job.base}

		if err := d.bucket.put(d.baseKey(job.gen), throw2(json.Marshal(base))); err != nil {
			slog.Warn("nbd: base", "gen", job.gen, "err", err)
			job.base = nil
		}
	}

	job.took = time.Since(start)
	d.commits <- job
}

func (d *NbdDisk) settle(job *NbdJob) {
	d.working = false

	if job.err != nil {
		slog.Warn("nbd: commit", "gen", job.gen, "blocks", len(job.items), "err", job.err)
		metricAdd("s3_nbd_commits_total", `result="failed"`, 1)

		for _, item := range job.items {
			if b := d.blocks[item.index]; b != nil && item.data != nil && b.frozen {
				b.frozen = false
			}
		}

		d.finish()

		return
	}

	var stale []string

	for _, item := range job.items {
		if item.prev != 0 {
			stale = append(stale, d.blockKey(item.index, item.prev))
		}

		if item.data == nil {
			delete(d.committed, item.index)
		} else {
			d.committed[item.index] = job.gen
		}

		b := d.blocks[item.index]

		if b.version != item.version {
			continue
		}

		b.dirty = false
		b.frozen = false
		delete(d.dirty, item.index)

		if b.zero {
			b.zero = false
			d.forgetIdle(item.index, b)
		} else {
			d.clean(item.index, b)
		}
	}

	d.gen = job.gen

	if job.base != nil {
		d.baseGen = job.gen
		d.logs = nil

		stale = append(stale, job.forget...)
	} else {
		d.logs = append(d.logs, job.gen)
	}

	metricAdd("s3_nbd_commits_total", `result="ok"`, 1)
	metricAdd("s3_nbd_committed_blocks_total", "", float64(len(job.items)))
	metricTime("s3_nbd_commit_seconds", "", job.took)
	slog.Debug("nbd: commit", "gen", job.gen, "blocks", len(job.items), "base", job.base != nil, "took", job.took)

	go d.bucket.deleteAll(stale)

	d.finish()
}

type BucketClient struct {
	base   string
	bucket string
	http   *http.Client
}

func newBucketClient(endpoint, bucket string) *BucketClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.MaxIdleConnsPerHost = nbdPutWindow * 2

	return &BucketClient{base: strings.TrimRight(endpoint, "/") + "/" + bucket, bucket: bucket, http: &http.Client{Transport: transport}}
}

func (b *BucketClient) do(method, path string, body []byte) ([]byte, error) {
	var rd io.Reader

	if body != nil {
		rd = bytes.NewReader(body)
	}

	resp, err := b.http.Do(throw2(http.NewRequest(method, b.base+path, rd)))

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}

	return data, nil
}

func (b *BucketClient) get(key string) ([]byte, error) {
	return b.do(http.MethodGet, "/"+key, nil)
}

func (b *BucketClient) put(key string, data []byte) error {
	_, err := b.do(http.MethodPut, "/"+key, data)

	return err
}

func (b *BucketClient) deleteAll(keys []string) {
	for _, key := range keys {
		if _, err := b.do(http.MethodDelete, "/"+key, nil); err != nil {
			slog.Warn("nbd: delete", "key", key, "err", err)
			metricAdd("s3_nbd_errors_total", `op="delete"`, 1)
		}
	}
}

func (b *BucketClient) list(prefix string) []string {
	var keys []string

	token := ""

	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}

		if token != "" {
			q.Set("continuation-token", token)
		}

		var page ListBucketResult

		throw(xml.Unmarshal(throw2(b.do(http.MethodGet, "?"+q.Encode(), nil)), &page))

		for _, c := range page.Contents {
			keys = append(keys, c.Key)
		}

		if !page.IsTruncated {
			sort.Strings(keys)

			return keys
		}

		token = page.NextContinuationToken
	}
}

func parseSize(text string) (int64, error) {
	units := map[byte]int64{'K': 1 << 10, 'M': 1 << 20, 'G': 1 << 30, 'T': 1 << 40}
	mult := int64(1)

	if n := len(text); n > 0 && units[text[n-1]] != 0 {
		mult = units[text[n-1]]
		text = text[:n-1]
	}

	v, err := strconv.ParseInt(text, 10, 64)

	return v * mult, err
}
