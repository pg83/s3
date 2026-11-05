package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	markerName     = "rebuild"
	markerQueued   = "queued"
	markerRunning  = "running"
	windowBlocks   = 16
	readGap        = 2 << 20
	rebuildScanTop = 1000
)

func rebuildKey(id int) string {
	return "rebuild/" + strconv.Itoa(id)
}

func (c *Cell) markerPath() string {
	return filepath.Join(c.store, markerName)
}

func (c *Cell) marked() string {
	data, err := os.ReadFile(c.markerPath())

	if errors.Is(err, os.ErrNotExist) {
		return ""
	}

	throw(err)

	return string(data)
}

func (c *Cell) mark(state string) {
	tmp := c.markerPath() + ".tmp"
	f := throw2(os.Create(tmp))

	throw2(f.WriteString(state))
	throw(f.Sync())
	throw(f.Close())
	throw(os.Rename(tmp, c.markerPath()))
	syncDir(c.store)
}

func (c *Cell) unmark() {
	throw(os.Remove(c.markerPath()))
	syncDir(c.store)
}

func (c *Cell) watchRebuild(e *Etcd, id int) {
	for range e.watch(rebuildKey(id)) {
		try(func() {
			c.leaveIfQueued(e, id)
		}).catch(func(exc *Exception) {
			slog.Warn("cell: rebuild key", "err", exc.error())
		})
	}
}

func (c *Cell) leaveIfQueued(e *Etcd, id int) {
	if _, queued := e.get(rebuildKey(id)); queued {
		c.mark(markerQueued)
		slog.Warn("cell: queued for a rebuild, leaving", "cell", id)
		os.Exit(0)
	}
}

func (c *Cell) reset() {
	buf := alignedBlock()

	c.flush(buf)

	for _, num := range blockNumbers(c.store, ".current") {
		clear(buf)

		f := throw2(os.Open(c.currentPath(num)))
		_, err := f.ReadAt(buf, 0)

		throw(f.Close())

		if err != nil && !errors.Is(err, io.EOF) {
			throw(err)
		}

		if n := min(blockSize, c.capacity-num*blockSize) &^ (ioAlign - 1); n > 0 {
			throw2(c.hdd.WriteAt(buf[:n], num*blockSize))
		}
	}

	throw(c.hdd.Sync())
	throw(c.current.Close())

	for _, num := range blockNumbers(c.store, ".current") {
		throw(os.Remove(c.currentPath(num)))
	}

	for _, entry := range throw2(os.ReadDir(c.load)) {
		throw(os.Remove(filepath.Join(c.load, entry.Name())))
	}

	syncDir(c.store)
	syncDir(c.load)
	c.mark(markerRunning)

	c.num = 0
	c.openCurrent()
	syncDir(c.store)
}

type rebuildObj struct {
	key   string
	raw   []byte
	rev   int64
	edits []pieceEdit

	changed bool
}

type pieceEdit struct {
	chunk  int
	piece  int
	from   int64
	to     int64
	xxh    string
	sealed [3]string
}

type rebuildPiece struct {
	obj    int
	chunk  int
	piece  int
	offset int64
	n      int64
	xxh    string
}

type rebuilder struct {
	counts  map[string]int
	c       *Cell
	store   *Store
	id      int
	host    string
	batch   int64
	pace    *pacer
	objs    []*rebuildObj
	work    []rebuildPiece
	left    atomic.Int64
	old     *window
	scratch []byte
	touched map[int]bool
}

func (c *Cell) rebuild(cfg *Config, id int, state string, rate, batch int64, metrics string) {
	spec, _ := cellSpec(cfg, id)
	start := time.Now()

	if state == markerQueued {
		slog.Info("cell: rebuild starts, the old log goes to the HDD", "cell", id, "head", c.head())
		c.reset()
	}

	go c.flusher(max(1, int(batch/blockSize)))

	r := &rebuilder{
		c:       c,
		store:   newStore(cfg),
		id:      id,
		host:    spec.Host,
		batch:   batch,
		pace:    &pacer{rate: rate, prefix: "s3_cell_rebuild"},
		old:     newWindow(c.hdd, c.capacity),
		scratch: alignedBlock(),
		touched: map[int]bool{},
		counts:  map[string]int{},
	}

	serveMetrics(metrics, func() []gauge {
		return append(append(c.gauges(), r.store.gauges()...), gauge{"s3_cell_rebuild_pieces_left", "", float64(r.left.Load())})
	})

	r.plan()
	r.run()

	r.store.etcd.del(rebuildKey(id))
	c.unmark()

	slog.Info("cell: rebuilt", "cell", id, "head", c.num*blockSize+c.size, "self", r.counts["self"], "neighbours", r.counts["neighbours"],
		"kept", r.counts["kept"], "homeless", r.counts["homeless"], "owed", r.counts["owed"], "moved", r.counts["moved"],
		"raced", r.counts["raced"], "gone", r.counts["gone"], "skipped", r.counts["skipped"], "read", r.old.read, "took", time.Since(start))
}

func cellSpec(cfg *Config, id int) (CellSpec, bool) {
	for _, c := range cfg.Cells {
		if c.Id == id {
			return c, true
		}
	}

	return CellSpec{}, false
}

func (r *rebuilder) plan() {
	from := ""
	bytes := int64(0)

	for {
		found, more := r.store.etcd.scan("obj/", from, rebuildScanTop, false)

		for _, entry := range found {
			from = entry.key + "\x00"

			m := Manifest{}

			if json.Unmarshal(entry.value, &m) != nil {
				continue
			}

			idx := len(r.objs)

			for i := range m.Chunks {
				for _, p := range m.Chunks[i].Pieces {
					if p.Cell == r.id {
						n := pieceLen(m.chunkLen(i))

						r.work = append(r.work, rebuildPiece{obj: idx, chunk: i, piece: p.Piece, offset: p.Offset, n: n, xxh: p.Xxh})
						bytes += n
					}
				}
			}

			if len(r.work) > 0 && r.work[len(r.work)-1].obj == idx {
				r.objs = append(r.objs, &rebuildObj{key: entry.key, raw: entry.value, rev: entry.rev})
			}
		}

		if !more {
			break
		}
	}

	sort.SliceStable(r.work, func(i, j int) bool { return r.work[i].offset < r.work[j].offset })
	r.left.Store(int64(len(r.work)))

	slog.Info("cell: rebuild planned", "cell", r.id, "objects", len(r.objs), "pieces", len(r.work), "bytes", bytes)
}

func (r *rebuilder) run() {
	head := r.c.num*blockSize + r.c.size
	since := int64(0)

	var homeless []rebuildPiece

	for i, w := range r.work {
		r.left.Add(-1)

		if w.offset < head {
			if w.xxh != "" && xxhHex(r.c.readBack(w.offset, w.n, r.scratch)) == w.xxh {
				r.rebuilt("kept", 0)

				continue
			}

			homeless = append(homeless, w)
			r.counts["homeless"]++

			continue
		}

		since += r.place(w, r.old.bytes(w.offset, w.n, r.reach(i)))
		head = r.c.num*blockSize + r.c.size

		if since >= r.batch {
			r.commit()
			since = 0
		}
	}

	for _, w := range homeless {
		r.place(w, nil)
	}

	r.commit()
}

func (r *rebuilder) place(w rebuildPiece, self []byte) int64 {
	if r.objs[w.obj].changed && !r.holds(w) {
		r.rebuilt("skipped", 0)

		return 0
	}

	data, sealed := self, [3]string{}

	if w.xxh == "" || self == nil || xxhHex(self) != w.xxh {
		data, sealed = r.neighbours(w, self)

		if data == nil {
			obj := r.objs[w.obj]
			bucket, key, _ := strings.Cut(strings.TrimPrefix(obj.key, "obj/"), "/")

			slog.Warn("cell: rebuild could not get a piece, the repair owes it", "key", obj.key, "chunk", w.chunk, "piece", w.piece)
			r.store.owe(r.host, bucket, key)
			r.rebuilt("owed", 0)

			return 0
		}

		r.rebuilt("neighbours", len(data))
	} else {
		r.rebuilt("self", len(data))
	}

	at := throw2(r.c.put(data))

	if at != w.offset || w.xxh == "" {
		r.objs[w.obj].edits = append(r.objs[w.obj].edits, pieceEdit{chunk: w.chunk, piece: w.piece, from: w.offset, to: at, xxh: xxhHex(data), sealed: sealed})
		r.touched[w.obj] = true
	}

	return int64(len(data))
}

func (r *rebuilder) holds(w rebuildPiece) bool {
	m := Manifest{}

	if raw := r.objs[w.obj].raw; raw == nil || json.Unmarshal(raw, &m) != nil || w.chunk >= len(m.Chunks) {
		return false
	}

	for _, p := range m.Chunks[w.chunk].Pieces {
		if p.Piece == w.piece && p.Cell == r.id && p.Offset == w.offset {
			return true
		}
	}

	return false
}

func (r *rebuilder) neighbours(w rebuildPiece, self []byte) ([]byte, [3]string) {
	m := Manifest{}

	throw(json.Unmarshal(r.objs[w.obj].raw, &m))

	c := m.chunk(w.chunk)

	var others []int

	for i := range 3 {
		if i != w.piece {
			others = append(others, i)
		}
	}

	have, _ := r.store.fetch(nil, c.reads(w.chunk, others...))
	remote := 0

	for _, i := range others {
		c.have[i] = have[w.chunk*3+i]
		remote += len(c.have[i])
	}

	r.pace.pace(remote)

	c.have[w.piece] = self

	data, _ := c.assemble()

	if data == nil {
		return nil, [3]string{}
	}

	pieces := c.pieces(data)
	sealed := [3]string{}

	for i := range 3 {
		sealed[i] = xxhHex(pieces[i])
	}

	return pieces[w.piece], sealed
}

func (r *rebuilder) commit() {
	throw(r.c.current.Sync())

	for idx := range r.touched {
		r.commitObj(r.objs[idx])
	}

	slog.Debug("cell: rebuild committed", "keys", len(r.touched), "head", r.c.num*blockSize+r.c.size, "left", r.left.Load())
	clear(r.touched)
}

func (r *rebuilder) commitObj(obj *rebuildObj) {
	for {
		m := Manifest{}

		throw(json.Unmarshal(obj.raw, &m))

		if !r.apply(&m, obj.edits) {
			slog.Debug("cell: rebuild left a key alone, it moved on", "key", obj.key)
			r.rebuiltKey("gone")
			obj.edits = nil

			return
		}

		raw := throw2(json.Marshal(m))

		if rev, ok := r.store.etcd.putIfRevision(obj.key, raw, obj.rev); ok {
			obj.raw, obj.rev, obj.edits = raw, rev, nil
			r.rebuiltKey("moved")

			return
		}

		r.rebuiltKey("raced")
		slog.Debug("cell: rebuild raced a key, reading it again", "key", obj.key)

		entry, found := r.store.etcd.get(obj.key)

		if !found {
			slog.Debug("cell: rebuild left a key alone, it is gone", "key", obj.key)
			r.rebuiltKey("gone")
			obj.raw, obj.changed, obj.edits = nil, true, nil

			return
		}

		obj.raw, obj.rev, obj.changed = entry.value, entry.rev, true
	}
}

func (r *rebuilder) apply(m *Manifest, edits []pieceEdit) bool {
	changed := false

	for _, e := range edits {
		if e.chunk >= len(m.Chunks) {
			continue
		}

		pieces := m.Chunks[e.chunk].Pieces

		for i := range pieces {
			p := &pieces[i]

			if p.Piece != e.piece || p.Cell != r.id || p.Offset != e.from {
				continue
			}

			if p.Offset != e.to || p.Xxh != e.xxh {
				p.Offset, p.Xxh = e.to, e.xxh
				changed = true
			}

			for j := range pieces {
				if q := &pieces[j]; q.Xxh == "" && e.sealed[q.Piece] != "" {
					q.Xxh = e.sealed[q.Piece]
					changed = true
				}
			}
		}
	}

	return changed
}

func (r *rebuilder) rebuilt(source string, n int) {
	r.counts[source]++
	metricAdd("s3_cell_rebuild_pieces_total", `source="`+source+`"`, 1)
	metricAdd("s3_cell_rebuild_bytes_total", `source="`+source+`"`, float64(n))
}

func (r *rebuilder) rebuiltKey(result string) {
	r.counts[result]++
	metricAdd("s3_cell_rebuild_keys_total", `result="`+result+`"`, 1)
}

func (c *Cell) readBack(off, n int64, buf []byte) []byte {
	out := make([]byte, n)

	for pos := off; pos < off+n; {
		num := pos / blockSize
		end := min(off+n, (num+1)*blockSize)
		f, err := os.Open(c.currentPath(num))

		if err != nil {
			f, err = os.Open(c.readyPath(num))
		}

		if err == nil {
			f.ReadAt(out[pos-off:end-off], pos-num*blockSize)
			throw(f.Close())
		} else {
			readHdd(c.hdd, c.capacity, buf, num*blockSize)
			copy(out[pos-off:end-off], buf[pos-num*blockSize:])
		}

		pos = end
	}

	return out
}

func readHdd(hdd *os.File, capacity int64, buf []byte, at int64) {
	clear(buf)

	n := min(int64(len(buf)), max(0, capacity-at)) &^ (ioAlign - 1)
	_, err := hdd.ReadAt(buf[:n], at)

	if err != nil && !errors.Is(err, io.EOF) {
		throw(err)
	}
}

type window struct {
	hdd      *os.File
	capacity int64
	buf      []byte
	start    int64
	n        int64
	read     int64
}

func newWindow(hdd *os.File, capacity int64) *window {
	return &window{hdd: hdd, capacity: capacity, buf: aligned(windowBlocks * blockSize)}
}

func (w *window) bytes(off, n, until int64) []byte {
	out := make([]byte, n)

	for pos := off; pos < off+n; {
		if pos < w.start || pos >= w.start+w.n {
			w.start = pos &^ (ioAlign - 1)
			w.n = min(int64(len(w.buf)), (max(until, off+n)-w.start+ioAlign-1)&^(ioAlign-1))
			readHdd(w.hdd, w.capacity, w.buf[:w.n], w.start)
			w.read += w.n
			metricAdd("s3_cell_rebuild_hdd_read_bytes_total", "", float64(w.n))
		}

		pos += int64(copy(out[pos-off:], w.buf[pos-w.start:w.n]))
	}

	return out
}

func (r *rebuilder) reach(i int) int64 {
	w := r.work[i]
	end := w.offset + w.n

	for _, next := range r.work[i+1:] {
		if next.offset-end > readGap || next.offset+next.n-w.offset > int64(len(r.old.buf)) {
			break
		}

		end = max(end, next.offset+next.n)
	}

	return end
}

type pacer struct {
	rate   int64
	prefix string
	paid   time.Time
}

func (p *pacer) pace(n int) {
	metricAdd(p.prefix+"_network_bytes_total", "", float64(n))

	if p.rate <= 0 || n == 0 {
		return
	}

	if now := time.Now(); p.paid.Before(now) {
		p.paid = now
	}

	p.paid = p.paid.Add(time.Duration(float64(n) / float64(p.rate) * float64(time.Second)))

	wait := time.Until(p.paid)

	metricAdd(p.prefix+"_paused_seconds_total", "", wait.Seconds())
	time.Sleep(wait)
}
