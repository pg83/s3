package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"time"
)

var errTooFewCells = errors.New("store: fewer than two cells took the object")
var errUnreadable = errors.New("store: no combination of pieces matches the md5")
var errNoSuchKey = errors.New("store: no such key")

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

type Repair struct {
	Bad int `json:"bad"`
}

type Store struct {
	etcd   *Etcd
	hosts  []string
	byHost map[string][]CellSpec
	byId   map[int]CellSpec
}

func newStore(cfg *Config) *Store {
	s := &Store{etcd: newEtcd(cfg.Etcd), byHost: map[string][]CellSpec{}, byId: map[int]CellSpec{}}

	for _, c := range cfg.Cells {
		if _, seen := s.byHost[c.Host]; !seen {
			s.hosts = append(s.hosts, c.Host)
		}

		s.byHost[c.Host] = append(s.byHost[c.Host], c)
		s.byId[c.Id] = c
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

func repairKey(bucket, key string) string {
	return "repair/" + bucket + "/" + key
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

func split(data []byte) [3][]byte {
	n := pieceLen(int64(len(data)))
	d0 := data[:n]
	d1 := make([]byte, n)

	copy(d1, data[n:])

	return [3][]byte{d0, d1, xor(d0, d1)}
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

func (s *Store) appendTo(host string, data []byte) (Piece, bool) {
	cells := append([]CellSpec(nil), s.byHost[host]...)
	rand.Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })

	for _, c := range cells {
		var offset int64
		var err error

		exc := try(func() {
			cl := dialCell(c.Addr)

			defer cl.close()

			offset, err = cl.append(data)
		})

		if exc != nil {
			slog.Warn("store: append", "cell", c.Id, "addr", c.Addr, "err", exc.error())

			continue
		}

		if err != nil {
			slog.Warn("store: append", "cell", c.Id, "err", err)

			continue
		}

		return Piece{Cell: c.Id, Offset: offset}, true
	}

	return Piece{}, false
}

func (s *Store) put(bucket, key string, data []byte, contentType string) (Manifest, error) {
	m := Manifest{Size: int64(len(data)), Md5: md5hex(data), Mtime: time.Now().UTC(), ContentType: contentType}

	if len(data) > 0 {
		pieces := split(data)

		for i, host := range s.hostOrder(key) {
			if p, ok := s.appendTo(host, pieces[i]); ok {
				p.Piece = i
				m.Pieces = append(m.Pieces, p)
			}
		}

		if len(m.Pieces) < 2 {
			return m, errTooFewCells
		}
	}

	s.etcd.put(objKey(bucket, key), throw2(json.Marshal(m)))

	if len(data) > 0 && len(m.Pieces) < 3 {
		s.etcd.put(repairKey(bucket, key), throw2(json.Marshal(Repair{Bad: -1})))
	} else {
		s.etcd.del(repairKey(bucket, key))
	}

	return m, nil
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

func (s *Store) fetch(p Piece, n int64) ([]byte, bool) {
	c, known := s.byId[p.Cell]

	if !known {
		return nil, false
	}

	var data []byte
	var err error

	exc := try(func() {
		cl := dialCell(c.Addr)

		defer cl.close()

		data, err = cl.read(p.Offset, n)
	})

	if exc != nil || err != nil {
		slog.Warn("store: read", "cell", p.Cell, "offset", p.Offset, "exc", exc.asError(), "err", err)

		return nil, false
	}

	return data, true
}

func (s *Store) get(bucket, key string, m Manifest) ([]byte, error) {
	if m.Size == 0 {
		return nil, nil
	}

	n := pieceLen(m.Size)
	have := map[int][]byte{}
	byIndex := map[int]Piece{}

	for _, p := range m.Pieces {
		byIndex[p.Piece] = p
	}

	load := func(i int) []byte {
		if data, ok := have[i]; ok {
			return data
		}

		if p, ok := byIndex[i]; ok {
			if data, ok := s.fetch(p, n); ok {
				have[i] = data
			}
		}

		return have[i]
	}

	d0, d1 := load(0), load(1)
	both := d0 != nil && d1 != nil

	if both {
		if data := assemble(d0, d1, m.Size); md5hex(data) == m.Md5 {
			return data, nil
		}
	}

	if p := load(2); p != nil {
		if d0 != nil {
			if data := assemble(d0, xor(d0, p), m.Size); md5hex(data) == m.Md5 {
				if both {
					s.suspect(bucket, key, 1)
				}

				return data, nil
			}
		}

		if d1 != nil {
			if data := assemble(xor(d1, p), d1, m.Size); md5hex(data) == m.Md5 {
				if both {
					s.suspect(bucket, key, 0)
				}

				return data, nil
			}
		}
	}

	return nil, errUnreadable
}

func (s *Store) suspect(bucket, key string, piece int) {
	s.etcd.put(repairKey(bucket, key), throw2(json.Marshal(Repair{Bad: piece})))
}

func (s *Store) repair(bucket, key string, bad int) {
	m, rev, err := s.manifest(bucket, key)

	if errors.Is(err, errNoSuchKey) {
		s.etcd.del(repairKey(bucket, key))

		return
	}

	throw(err)

	byIndex := map[int]Piece{}
	hosts := map[string]bool{}

	for _, p := range m.Pieces {
		byIndex[p.Piece] = p
		hosts[s.byId[p.Cell].Host] = true
	}

	var missing []int

	for i := range 3 {
		if _, ok := byIndex[i]; !ok || i == bad {
			missing = append(missing, i)
		}
	}

	if len(missing) == 0 || m.Size == 0 {
		s.etcd.del(repairKey(bucket, key))

		return
	}

	if len(missing) > 1 {
		throwFmt("store: %s/%s has %d pieces to rebuild, can rebuild one", bucket, key, len(missing))
	}

	n := pieceLen(m.Size)
	have := map[int][]byte{}

	for i := range 3 {
		if i == missing[0] {
			continue
		}

		data, ok := s.fetch(byIndex[i], n)

		if !ok {
			throwFmt("store: %s/%s: piece %d is unreadable", bucket, key, i)
		}

		have[i] = data
	}

	var rebuilt []byte

	switch missing[0] {
	case 0:
		rebuilt = xor(have[1], have[2])
	case 1:
		rebuilt = xor(have[0], have[2])
	default:
		rebuilt = xor(have[0], have[1])
	}

	if data := assemble(orElse(have[0], rebuilt, missing[0] == 0), orElse(have[1], rebuilt, missing[0] == 1), m.Size); md5hex(data) != m.Md5 {
		throwFmt("store: %s/%s: the two pieces at hand do not rebuild the md5", bucket, key)
	}

	target := ""

	if p, ok := byIndex[bad]; ok {
		target = s.byId[p.Cell].Host
	} else {
		for _, h := range s.hosts {
			if !hosts[h] {
				target = h
			}
		}
	}

	if target == "" {
		throwFmt("store: %s/%s: no host to put piece %d on", bucket, key, missing[0])
	}

	p, ok := s.appendTo(target, rebuilt)

	if !ok {
		throwFmt("store: %s/%s: host %s took nothing", bucket, key, target)
	}

	p.Piece = missing[0]
	pieces := []Piece{p}

	for _, old := range m.Pieces {
		if old.Piece != missing[0] {
			pieces = append(pieces, old)
		}
	}

	sort.Slice(pieces, func(i, j int) bool { return pieces[i].Piece < pieces[j].Piece })
	m.Pieces = pieces

	if s.etcd.putIfRevision(objKey(bucket, key), throw2(json.Marshal(m)), rev) {
		s.etcd.del(repairKey(bucket, key))
	}
}

func orElse(have, rebuilt []byte, useRebuilt bool) []byte {
	if useRebuilt {
		return rebuilt
	}

	return have
}
