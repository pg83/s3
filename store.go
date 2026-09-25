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

func (s *Store) appendTo(cells []CellSpec, data []byte) (Piece, bool) {
	cells = append([]CellSpec(nil), cells...)

	rand.Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })

	for _, c := range cells {
		reply := make(chan outcome, 1)

		s.links[c.Id].send(c.Id, opAppend, data, reply)

		offset, err := appended(<-reply)

		if err == nil {
			return Piece{Cell: c.Id, Offset: offset}, true
		}

		slog.Warn("store: append", "cell", c.Id, "err", err)

		if !errors.Is(err, errFull) {
			return Piece{}, false
		}
	}

	return Piece{}, false
}

func (s *Store) put(bucket, key string, data []byte, contentType string) (Manifest, error) {
	m := Manifest{Size: int64(len(data)), Md5: md5hex(data), Mtime: time.Now().UTC(), ContentType: contentType}

	var owing []string

	if len(data) > 0 {
		pieces := split(data)
		order := s.hostOrder(key)
		placed := make(chan Piece, len(order))

		for i, host := range order {
			go func() {
				p, ok := s.appendTo(s.byHost[host], pieces[i])

				if !ok {
					p.Cell = -1
				}

				p.Piece = i
				placed <- p
			}()
		}

		for range order {
			p := <-placed

			if p.Cell < 0 {
				owing = append(owing, order[p.Piece])

				continue
			}

			m.Pieces = append(m.Pieces, p)
		}

		sort.Slice(m.Pieces, func(i, j int) bool { return m.Pieces[i].Piece < m.Pieces[j].Piece })

		if len(m.Pieces) < 2 {
			return m, errTooFewCells
		}
	}

	s.etcd.put(objKey(bucket, key), throw2(json.Marshal(m)))

	for _, host := range owing {
		s.owe(host, bucket, key)
	}

	return m, nil
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

func (s *Store) fetch(p Piece, n int64) ([]byte, bool) {
	if _, known := s.byId[p.Cell]; !known {
		return nil, false
	}

	body := binary.BigEndian.AppendUint64(nil, uint64(p.Offset))
	reply := make(chan outcome, 1)

	s.links[p.Cell].send(p.Cell, opRead, binary.BigEndian.AppendUint32(body, uint32(n)), reply)

	data, err := readOut(<-reply)

	if err != nil {
		slog.Warn("store: read", "cell", p.Cell, "offset", p.Offset, "err", err)

		return nil, false
	}

	return data, true
}

func (s *Store) fetchAll(pieces []Piece, n int64) [3][]byte {
	got := make(chan Piece, len(pieces))
	have := [3][]byte{}
	data := make([][]byte, 3)

	for _, p := range pieces {
		go func() {
			data[p.Piece], _ = s.fetch(p, n)
			got <- p
		}()
	}

	for range pieces {
		p := <-got

		have[p.Piece] = data[p.Piece]
	}

	return have
}

func (s *Store) get(bucket, key string, m Manifest) ([]byte, error) {
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

	have := s.fetchAll(halves, n)
	both := have[0] != nil && have[1] != nil

	if both {
		if data := assemble(have[0], have[1], m.Size); md5hex(data) == m.Md5 {
			return data, nil
		}
	}

	if p, ok := byIndex[2]; ok {
		have[2], _ = s.fetch(p, n)
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
