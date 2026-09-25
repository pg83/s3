package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sort"
	"strings"
	"time"
)

type Repairer struct {
	store *Store
	host  string
}

func runRepair(cfg *Config, host string) {
	if host == "" {
		throwFmt("repair: -host is required")
	}

	s := newStore(cfg)
	local := s.byHost[host]

	if len(local) == 0 {
		throwFmt("repair: the config has no cells of host %s", host)
	}

	for _, c := range local {
		if h, _, err := net.SplitHostPort(c.Addr); err != nil || !net.ParseIP(h).IsLoopback() {
			throwFmt("repair: cell %d of %s is at %s, own cells are reached over loopback only", c.Id, host, c.Addr)
		}
	}

	r := &Repairer{store: s, host: host}

	slog.Info("repair: watching the queue", "host", host, "cells", len(local))

	for {
		try(func() {
			r.pass()
		}).catch(func(exc *Exception) {
			slog.Error("repair", "err", exc.error())
		})

		time.Sleep(5 * time.Second)
	}
}

func (r *Repairer) pass() {
	prefix := "repair/" + r.host + "/"
	from := ""

	for {
		found, more := r.store.etcd.scan(prefix, from, 100)

		for _, entry := range found {
			from = entry.key + "\x00"

			bucket, key, ok := strings.Cut(strings.TrimPrefix(entry.key, prefix), "/")

			if !ok {
				r.store.etcd.del(entry.key)

				continue
			}

			try(func() {
				r.fix(bucket, key)
			}).catch(func(exc *Exception) {
				slog.Warn("repair", "bucket", bucket, "key", key, "err", exc.error())
			})
		}

		if !more {
			return
		}
	}
}

func (r *Repairer) fix(bucket, key string) {
	m, rev, err := r.store.manifest(bucket, key)

	if errors.Is(err, errNoSuchKey) || (err == nil && m.Size == 0) {
		r.drop(bucket, key)

		return
	}

	throw(err)

	mine := r.pieceOf(bucket, key, m)
	have, _ := r.store.fetchAll(nil, m.Pieces, pieceLen(m.Size))

	if sound(have, m) {
		r.drop(bucket, key)

		return
	}

	have[mine] = rebuild(mine, have)

	if have[mine] == nil {
		throwFmt("repair: %s/%s: the other two pieces are not both readable", bucket, key)
	}

	if !sound(have, m) {
		throwFmt("repair: %s/%s: the other two pieces do not rebuild the md5", bucket, key)
	}

	pl := r.store.placer(map[int][]byte{mine: have[mine]}, map[int]string{mine: r.host})

	pl.wait(nil, 1)

	p, ok := pl.placed[mine]

	if !ok {
		throwFmt("repair: %s/%s: no cell of %s took the piece", bucket, key, r.host)
	}

	pieces := []Piece{p}

	for _, old := range m.Pieces {
		if old.Piece != mine {
			pieces = append(pieces, old)
		}
	}

	sort.Slice(pieces, func(i, j int) bool { return pieces[i].Piece < pieces[j].Piece })
	m.Pieces = pieces

	if !r.store.etcd.putIfRevision(objKey(bucket, key), throw2(json.Marshal(m)), rev) {
		slog.Warn("repair: key changed underneath, left in the queue", "bucket", bucket, "key", key)

		return
	}

	r.drop(bucket, key)
	slog.Info("repair: rebuilt", "bucket", bucket, "key", key, "piece", mine, "cell", p.Cell)
}

func (r *Repairer) pieceOf(bucket, key string, m Manifest) int {
	present := [3]bool{}
	mine := -1

	for _, p := range m.Pieces {
		present[p.Piece] = true

		if r.store.byId[p.Cell].Host == r.host {
			mine = p.Piece
		}
	}

	for i := range 3 {
		if mine < 0 && !present[i] {
			mine = i
		}
	}

	if mine < 0 {
		throwFmt("repair: %s/%s: no piece of it belongs to %s", bucket, key, r.host)
	}

	return mine
}

func (r *Repairer) drop(bucket, key string) {
	r.store.etcd.del(repairKey(r.host, bucket, key))
}

func sound(have [3][]byte, m Manifest) bool {
	if have[0] == nil || have[1] == nil || have[2] == nil {
		return false
	}

	return md5hex(assemble(have[0], have[1], m.Size)) == m.Md5 && bytes.Equal(xor(have[0], have[1]), have[2])
}

func rebuild(piece int, have [3][]byte) []byte {
	a, b := (piece+1)%3, (piece+2)%3

	if have[a] == nil || have[b] == nil {
		return nil
	}

	return xor(have[a], have[b])
}
