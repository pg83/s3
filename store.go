package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
)

var (
	errTooFewCells = errors.New("store: fewer than two cells took a chunk")
	errUnreadable  = errors.New("store: a chunk has no readable pair of pieces")
	errNoSuchKey   = errors.New("store: no such key")
	errClientGone  = errors.New("store: the client left")
)

const chunkSize = 8 << 20

type Piece struct {
	Piece  int    `json:"piece"`
	Cell   int    `json:"cell"`
	Offset int64  `json:"offset"`
	Xxh    string `json:"xxh,omitempty"`
}

type Chunk struct {
	Pieces []Piece `json:"pieces"`
}

type Manifest struct {
	Size        int64     `json:"size"`
	Md5         string    `json:"md5"`
	Mtime       time.Time `json:"mtime"`
	ContentType string    `json:"type,omitempty"`
	Chunk       int64     `json:"chunk,omitempty"`
	Chunks      []Chunk   `json:"chunks,omitempty"`
}

func (m *Manifest) UnmarshalJSON(raw []byte) error {
	type plain Manifest

	aux := struct {
		*plain
		Pieces []Piece `json:"pieces"`
	}{plain: (*plain)(m)}

	if err := json.Unmarshal(raw, &aux); err != nil {
		return err
	}

	if m.Size > 0 && len(m.Chunks) == 0 {
		m.Chunk = m.Size
		m.Chunks = []Chunk{{Pieces: aux.Pieces}}
	}

	return nil
}

func (m *Manifest) chunkLen(i int) int64 {
	return min(int64(i+1)*m.Chunk, m.Size) - int64(i)*m.Chunk
}

func (m *Manifest) placed() int {
	n := 0

	for _, c := range m.Chunks {
		n += len(c.Pieces)
	}

	return n
}

func chunkKey(key string, i int) string {
	if i == 0 {
		return key
	}

	return key + "#" + strconv.Itoa(i)
}

type Store struct {
	etcd    *Etcd
	hosts   []string
	byHost  map[string][]CellSpec
	byId    map[int]CellSpec
	links   map[int]*Link
	up      chan struct{}
	buckets []string
	known   map[string]bool
}

func newStore(cfg *Config) *Store {
	s := &Store{etcd: newEtcd(cfg.Etcd), byHost: map[string][]CellSpec{}, byId: map[int]CellSpec{}, links: map[int]*Link{}, up: make(chan struct{}, 1), known: map[string]bool{}}

	for _, b := range cfg.Buckets {
		s.buckets = append(s.buckets, b)
		s.known[b] = true
	}

	sort.Strings(s.buckets)

	for _, c := range cfg.Cells {
		if _, seen := s.byHost[c.Host]; !seen {
			s.hosts = append(s.hosts, c.Host)
		}

		s.byHost[c.Host] = append(s.byHost[c.Host], c)
		s.byId[c.Id] = c
		s.links[c.Id] = newLink(c.Addr, s.up)
	}

	sort.Strings(s.hosts)

	if len(s.hosts) < 3 {
		throwFmt("store: %d hosts in the config, three are needed", len(s.hosts))
	}

	return s
}

func objKey(bucket, key string) string {
	return "obj/" + bucket + "/" + key
}

func repairKey(host, bucket, key string) string {
	return "repair/" + host + "/" + bucket + "/" + key
}

func pieceLen(size int64) int64 {
	return (size + 1) / 2
}

func xor(a, b []byte) []byte {
	out := make([]byte, len(a))

	for i := range a {
		out[i] = a[i] ^ b[i]
	}

	return out
}

func halves(data []byte) ([]byte, []byte) {
	n := pieceLen(int64(len(data)))
	d1 := make([]byte, n)

	copy(d1, data[n:])

	return data[:n], d1
}

func assemble(d0, d1 []byte, size int64) []byte {
	out := make([]byte, 0, size)

	out = append(out, d0...)

	return append(out, d1[:size-int64(len(d0))]...)
}

func md5hex(data []byte) string {
	sum := md5.Sum(data)

	return hex.EncodeToString(sum[:])
}

func xxhHex(data []byte) string {
	return fmt.Sprintf("%016x", xxhash.Sum64(data))
}

func (s *Store) hostOrder(key string) []string {
	h := fnv.New32a()

	h.Write([]byte(key))

	start := int(h.Sum32() % uint32(len(s.hosts)))

	var order []string

	for i := range s.hosts {
		order = append(order, s.hosts[(start+i)%len(s.hosts)])
	}

	return order
}

func (s *Store) chunkHosts(key string, i int) map[int]string {
	hosts := map[int]string{}

	for j, host := range s.hostOrder(chunkKey(key, i)) {
		hosts[j] = host
	}

	return hosts
}

type placing struct {
	host  string
	cells []CellSpec
	cell  int
}

type placer struct {
	store   *Store
	pieces  map[int][]byte
	plan    map[int]*placing
	placed  map[int]Piece
	reply   chan outcome
	pending int
}

func (s *Store) newPlacer(hosts map[int]string) *placer {
	p := &placer{store: s, pieces: map[int][]byte{}, plan: map[int]*placing{}, placed: map[int]Piece{}, reply: make(chan outcome, len(hosts)*len(s.byId))}

	for i, host := range hosts {
		cells := append([]CellSpec(nil), s.byHost[host]...)

		rand.Shuffle(len(cells), func(a, b int) { cells[a], cells[b] = cells[b], cells[a] })

		p.plan[i] = &placing{host: host, cells: cells}
	}

	return p
}

func (p *placer) add(i int, data []byte) {
	p.pieces[i] = data

	if p.offer(i) {
		p.pending++
	}
}

func (p *placer) offer(i int) bool {
	pl := p.plan[i]

	if len(pl.cells) == 0 {
		return false
	}

	pl.cell, pl.cells = pl.cells[0].Id, pl.cells[1:]

	p.store.links[pl.cell].send(i, opAppend, p.pieces[i], p.reply)

	return true
}

func (p *placer) take(o outcome) {
	p.pending--

	offset, err := appended(o)

	if err == nil {
		p.placed[o.tag] = Piece{Piece: o.tag, Cell: p.plan[o.tag].cell, Offset: offset, Xxh: xxhHex(p.pieces[o.tag])}

		return
	}

	slog.Warn("store: append", "cell", p.plan[o.tag].cell, "err", err)

	if errors.Is(err, errFull) && p.offer(o.tag) {
		p.pending++
	}
}

func (p *placer) cancel() {
	for _, pl := range p.plan {
		p.store.links[pl.cell].cancel(p.reply)
	}
}

func (p *placer) wait(gone <-chan struct{}, need int) bool {
	for len(p.placed) < need && p.pending > 0 {
		select {
		case o := <-p.reply:
			p.take(o)
		case <-gone:
			p.cancel()

			return false
		}
	}

	return true
}

func (p *placer) pieces3() []Piece {
	var out []Piece

	for i := range 3 {
		if piece, ok := p.placed[i]; ok {
			out = append(out, piece)
		}
	}

	return out
}

func (s *Store) put(gone <-chan struct{}, bucket, key string, data []byte, contentType string) (Manifest, error) {
	m := Manifest{Size: int64(len(data)), Mtime: time.Now().UTC(), ContentType: contentType}

	if len(data) == 0 {
		m.Md5 = md5hex(data)

		s.etcd.put(objKey(bucket, key), throw2(json.Marshal(m)))

		return m, nil
	}

	m.Chunk = chunkSize

	start := time.Now()
	placers := make([]*placer, (m.Size+m.Chunk-1)/m.Chunk)

	for i := range placers {
		at := int64(i) * m.Chunk
		p := s.newPlacer(s.chunkHosts(key, i))
		d0, d1 := halves(data[at : at+m.chunkLen(i)])

		p.add(0, d0)
		p.add(1, d1)
		p.add(2, xor(d0, d1))

		placers[i] = p
	}

	m.Md5 = md5hex(data)

	hashed := time.Now()

	for _, p := range placers {
		if !p.wait(gone, 2) {
			for _, q := range placers {
				q.cancel()
			}

			return m, errClientGone
		}

		if len(p.placed) < 2 {
			return m, errTooFewCells
		}
	}

	acked := time.Now()
	pending := 0

	m.Chunks = make([]Chunk, len(placers))

	for i, p := range placers {
		m.Chunks[i].Pieces = p.pieces3()

		if p.pending > 0 {
			pending++
		}
	}

	rev := s.etcd.put(objKey(bucket, key), throw2(json.Marshal(m)))

	slog.Debug("store: put", "key", key, "size", m.Size, "chunks", len(placers), "hash", hashed.Sub(start), "two", acked.Sub(hashed),
		"etcd", time.Since(acked), "pending", pending)

	if pending > 0 {
		go s.settle(placers, bucket, key, m, rev)
	} else {
		s.settle(placers, bucket, key, m, rev)
	}

	return m, nil
}

func (s *Store) settle(placers []*placer, bucket, key string, m Manifest, rev int64) {
	try(func() {
		changed := false
		owed := map[string]bool{}

		for i, p := range placers {
			p.wait(nil, 3)

			if len(p.placed) > len(m.Chunks[i].Pieces) {
				m.Chunks[i].Pieces = p.pieces3()
				changed = true
			}

			for j := range 3 {
				if _, ok := p.placed[j]; !ok {
					owed[p.plan[j].host] = true
				}
			}
		}

		if changed && !s.etcd.putIfRevision(objKey(bucket, key), throw2(json.Marshal(m)), rev) {
			slog.Warn("store: key changed before its third pieces landed", "bucket", bucket, "key", key)
		}

		for host := range owed {
			s.owe(host, bucket, key)
		}
	}).catch(func(exc *Exception) {
		slog.Error("store: settle", "bucket", bucket, "key", key, "err", exc.error())
	})
}

func (s *Store) owe(host, bucket, key string) {
	s.etcd.put(repairKey(host, bucket, key), nil)
}

func (s *Store) manifest(bucket, key string) (Manifest, int64, error) {
	entry, found := s.etcd.get(objKey(bucket, key))

	if !found {
		return Manifest{}, 0, errNoSuchKey
	}

	m := Manifest{}

	throw(json.Unmarshal(entry.value, &m))

	return m, entry.rev, nil
}

type readReq struct {
	cell   int
	offset int64
	n      int64
	tag    int
}

func (s *Store) fetch(gone <-chan struct{}, reqs []readReq) (map[int][]byte, bool) {
	reply := make(chan outcome, len(reqs))
	have := map[int][]byte{}
	sent := 0

	for _, r := range reqs {
		if _, known := s.byId[r.cell]; !known {
			continue
		}

		body := binary.BigEndian.AppendUint64(nil, uint64(r.offset))

		s.links[r.cell].send(r.tag, opRead, binary.BigEndian.AppendUint32(body, uint32(r.n)), reply)

		sent++
	}

	for range sent {
		var o outcome

		select {
		case o = <-reply:
		case <-gone:
			for _, r := range reqs {
				if l, known := s.links[r.cell]; known {
					l.cancel(reply)
				}
			}

			return have, false
		}

		data, err := readOut(o)

		if err != nil {
			slog.Warn("store: read", "chunk", o.tag/3, "piece", o.tag%3, "err", err)

			continue
		}

		have[o.tag] = data
	}

	return have, true
}

type chunkState struct {
	m    *Manifest
	by   map[int]Piece
	have [3][]byte
	base int64
	n    int64
}

func (m *Manifest) chunk(i int) *chunkState {
	c := &chunkState{m: m, by: map[int]Piece{}, base: int64(i) * m.Chunk, n: m.chunkLen(i)}

	for _, p := range m.Chunks[i].Pieces {
		c.by[p.Piece] = p
	}

	return c
}

func (c *chunkState) reads(i int, pieces ...int) []readReq {
	var out []readReq

	for _, piece := range pieces {
		if p, ok := c.by[piece]; ok {
			out = append(out, readReq{cell: p.Cell, offset: p.Offset, n: pieceLen(c.n), tag: i*3 + piece})
		}
	}

	return out
}

func (c *chunkState) sealed() bool {
	for _, p := range c.by {
		if p.Xxh == "" {
			return false
		}
	}

	return len(c.by) > 0
}

func (c *chunkState) sound(i int) bool {
	p, ok := c.by[i]

	return ok && c.have[i] != nil && xxhHex(c.have[i]) == p.Xxh
}

func (c *chunkState) matches(i int, data []byte) bool {
	p, ok := c.by[i]

	return !ok || xxhHex(data) == p.Xxh
}

func (c *chunkState) assemble() ([]byte, []Piece) {
	if c.sealed() {
		var bad []Piece

		for i := range 3 {
			if _, ok := c.by[i]; ok && c.have[i] != nil && !c.sound(i) {
				bad = append(bad, c.by[i])
			}
		}

		switch {
		case c.sound(0) && c.sound(1):
			return assemble(c.have[0], c.have[1], c.n), bad
		case c.sound(0) && c.sound(2):
			if d1 := xor(c.have[0], c.have[2]); c.matches(1, d1) {
				return assemble(c.have[0], d1, c.n), bad
			}
		case c.sound(1) && c.sound(2):
			if d0 := xor(c.have[1], c.have[2]); c.matches(0, d0) {
				return assemble(d0, c.have[1], c.n), bad
			}
		}

		return nil, bad
	}

	if c.have[0] != nil && c.have[1] != nil {
		if data := assemble(c.have[0], c.have[1], c.n); md5hex(data) == c.m.Md5 {
			return data, nil
		}
	}

	if c.have[2] != nil {
		if c.have[0] != nil {
			if data := assemble(c.have[0], xor(c.have[0], c.have[2]), c.n); md5hex(data) == c.m.Md5 {
				return data, c.present(1)
			}
		}

		if c.have[1] != nil {
			if data := assemble(xor(c.have[1], c.have[2]), c.have[1], c.n); md5hex(data) == c.m.Md5 {
				return data, c.present(0)
			}
		}
	}

	return nil, nil
}

func (c *chunkState) present(i int) []Piece {
	if c.have[i] == nil {
		return nil
	}

	return []Piece{c.by[i]}
}

func (c *chunkState) pieces(data []byte) [3][]byte {
	d0, d1 := halves(data)

	return [3][]byte{d0, d1, xor(d0, d1)}
}

func (s *Store) get(gone <-chan struct{}, bucket, key string, m Manifest, start, end int64) ([]byte, error) {
	if m.Size == 0 || start >= end {
		return nil, nil
	}

	first, last := int(start/m.Chunk), int((end-1)/m.Chunk)
	chunks := make([]*chunkState, 0, last-first+1)

	var reqs []readReq

	for i := first; i <= last; i++ {
		c := m.chunk(i)

		chunks = append(chunks, c)
		reqs = append(reqs, c.reads(i, 0, 1)...)
	}

	begun := time.Now()
	have, stayed := s.fetch(gone, reqs)

	if !stayed {
		return nil, errClientGone
	}

	var parity []readReq

	for j, c := range chunks {
		c.have[0], c.have[1] = have[(first+j)*3], have[(first+j)*3+1]

		if data, _ := c.assemble(); data == nil {
			parity = append(parity, c.reads(first+j, 2)...)
		}
	}

	if len(parity) > 0 {
		have, stayed := s.fetch(gone, parity)

		if !stayed {
			return nil, errClientGone
		}

		for tag, data := range have {
			chunks[tag/3-first].have[2] = data
		}
	}

	fetched := time.Now()
	out := make([]byte, 0, end-start)

	for _, c := range chunks {
		data, bad := c.assemble()

		for _, p := range bad {
			s.suspect(bucket, key, p)
		}

		if data == nil {
			return nil, errUnreadable
		}

		out = append(out, data[max(start-c.base, 0):min(end-c.base, c.n)]...)
	}

	slog.Debug("store: get", "key", key, "size", m.Size, "range", end-start, "chunks", len(chunks), "fetch", fetched.Sub(begun), "assemble", time.Since(fetched))

	return out, nil
}

func (s *Store) suspect(bucket, key string, p Piece) {
	s.owe(s.byId[p.Cell].Host, bucket, key)
}
