package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"time"
)

var (
	errTooFewCells = errors.New("store: fewer than two cells took the object")
	errUnreadable  = errors.New("store: no combination of pieces matches the md5")
	errNoSuchKey   = errors.New("store: no such key")
	errClientGone  = errors.New("store: the client left")
)

type Piece struct {
	Cell   int   `json:"cell"`
	Offset int64 `json:"offset"`
	Piece  int   `json:"piece"`
}

type Manifest struct {
	Size        int64     `json:"size"`
	Md5         string    `json:"md5"`
	Mtime       time.Time `json:"mtime"`
	ContentType string    `json:"type,omitempty"`
	Pieces      []Piece   `json:"pieces"`
}

type Store struct {
	etcd   *Etcd
	hosts  []string
	byHost map[string][]CellSpec
	byId   map[int]CellSpec
	links  map[int]*Link
}

func newStore(cfg *Config) *Store {
	s := &Store{etcd: newEtcd(cfg.Etcd), byHost: map[string][]CellSpec{}, byId: map[int]CellSpec{}, links: map[int]*Link{}}

	for _, c := range cfg.Cells {
		if _, seen := s.byHost[c.Host]; !seen {
			s.hosts = append(s.hosts, c.Host)
		}

		s.byHost[c.Host] = append(s.byHost[c.Host], c)
		s.byId[c.Id] = c
		s.links[c.Id] = newLink(c.Addr)
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

func bucketKey(bucket string) string {
	return "bkt/" + bucket
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

func (s *Store) placer(pieces map[int][]byte, hosts map[int]string) *placer {
	p := s.newPlacer(hosts)

	for i, data := range pieces {
		p.add(i, data)
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
		p.placed[o.tag] = Piece{Cell: p.plan[o.tag].cell, Offset: offset, Piece: o.tag}

		return
	}

	slog.Warn("store: append", "cell", p.plan[o.tag].cell, "err", err)

	if errors.Is(err, errFull) && p.offer(o.tag) {
		p.pending++
	}
}

func (p *placer) wait(gone <-chan struct{}, need int) bool {
	for len(p.placed) < need && p.pending > 0 {
		select {
		case o := <-p.reply:
			p.take(o)
		case <-gone:
			for _, pl := range p.plan {
				p.store.links[pl.cell].cancel(p.reply)
			}

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

	hosts := map[int]string{}

	for i, host := range s.hostOrder(key) {
		hosts[i] = host
	}

	start := time.Now()
	p := s.newPlacer(hosts)
	d0, d1 := halves(data)

	p.add(0, d0)
	p.add(1, d1)
	p.add(2, xor(d0, d1))

	m.Md5 = md5hex(data)

	hashed := time.Now()

	if !p.wait(gone, 2) {
		return m, errClientGone
	}

	if len(p.placed) < 2 {
		return m, errTooFewCells
	}

	acked := time.Now()

	m.Pieces = p.pieces3()

	rev := s.etcd.putRev(objKey(bucket, key), throw2(json.Marshal(m)))

	slog.Debug("store: put", "key", key, "size", m.Size, "hash", hashed.Sub(start), "two", acked.Sub(hashed),
		"etcd", time.Since(acked), "pending", p.pending)

	if p.pending > 0 {
		go s.settle(p, bucket, key, m, rev)
	} else {
		s.settle(p, bucket, key, m, rev)
	}

	return m, nil
}

func (s *Store) settle(p *placer, bucket, key string, m Manifest, rev int64) {
	try(func() {
		p.wait(nil, 3)

		if len(p.placed) > len(m.Pieces) {
			m.Pieces = p.pieces3()

			if !s.etcd.putIfRevision(objKey(bucket, key), throw2(json.Marshal(m)), rev) {
				slog.Warn("store: key changed before its third piece landed", "bucket", bucket, "key", key)
			}
		}

		for i := range 3 {
			if _, ok := p.placed[i]; !ok {
				s.owe(p.plan[i].host, bucket, key)
			}
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

func (s *Store) fetchAll(gone <-chan struct{}, pieces []Piece, n int64) ([3][]byte, bool) {
	reply := make(chan outcome, len(pieces))
	have := [3][]byte{}
	sent := 0

	for _, p := range pieces {
		if _, known := s.byId[p.Cell]; !known {
			continue
		}

		body := binary.BigEndian.AppendUint64(nil, uint64(p.Offset))

		s.links[p.Cell].send(p.Piece, opRead, binary.BigEndian.AppendUint32(body, uint32(n)), reply)

		sent++
	}

	for range sent {
		var o outcome

		select {
		case o = <-reply:
		case <-gone:
			for _, p := range pieces {
				if l, known := s.links[p.Cell]; known {
					l.cancel(reply)
				}
			}

			return have, false
		}

		data, err := readOut(o)

		if err != nil {
			slog.Warn("store: read", "piece", o.tag, "err", err)

			continue
		}

		have[o.tag] = data
	}

	return have, true
}

func (s *Store) get(gone <-chan struct{}, bucket, key string, m Manifest) ([]byte, error) {
	if m.Size == 0 {
		return nil, nil
	}

	n := pieceLen(m.Size)
	byIndex := map[int]Piece{}

	for _, p := range m.Pieces {
		byIndex[p.Piece] = p
	}

	var halves []Piece

	for i := range 2 {
		if p, ok := byIndex[i]; ok {
			halves = append(halves, p)
		}
	}

	start := time.Now()
	have, stayed := s.fetchAll(gone, halves, n)

	if !stayed {
		return nil, errClientGone
	}

	fetched := time.Now()
	both := have[0] != nil && have[1] != nil

	if both {
		if data := assemble(have[0], have[1], m.Size); md5hex(data) == m.Md5 {
			slog.Debug("store: get", "key", key, "size", m.Size, "fetch", fetched.Sub(start), "assemble", time.Since(fetched))

			return data, nil
		}
	}

	if p, ok := byIndex[2]; ok {
		parity, stayed := s.fetchAll(gone, []Piece{p}, n)

		if !stayed {
			return nil, errClientGone
		}

		have[2] = parity[2]
	}

	if have[2] != nil {
		if have[0] != nil {
			if data := assemble(have[0], xor(have[0], have[2]), m.Size); md5hex(data) == m.Md5 {
				if both {
					s.suspect(bucket, key, byIndex[1])
				}

				return data, nil
			}
		}

		if have[1] != nil {
			if data := assemble(xor(have[1], have[2]), have[1], m.Size); md5hex(data) == m.Md5 {
				if both {
					s.suspect(bucket, key, byIndex[0])
				}

				return data, nil
			}
		}
	}

	return nil, errUnreadable
}

func (s *Store) suspect(bucket, key string, p Piece) {
	s.owe(s.byId[p.Cell].Host, bucket, key)
}
