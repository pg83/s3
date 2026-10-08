package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"
)

type Repairer struct {
	store *Store
	host  string
}

func runRepair(cfg *Config, host, metrics string) {
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
	prefix := "repair/" + host + "/"

	serveMetrics(metrics, r.gauges)

	slog.Info("repair: watching the queue", "host", host, "cells", len(local))

	wake := s.etcd.watch(prefix)

	for {
		select {
		case <-wake:
		case <-s.up:
		}

		start := time.Now()

		try(func() {
			r.pass(prefix)
		}).catch(func(exc *Exception) {
			slog.Error("repair", "err", exc.error())
			metricAdd("s3_repair_pass_errors_total", "", 1)
		})

		metricTime("s3_repair_pass_seconds", "", time.Since(start))
	}
}

func (r *Repairer) pass(prefix string) {
	from := ""

	for {
		found, more := r.store.etcd.scan(prefix, from, 100, true)

		for _, entry := range found {
			from = entry.key + "\x00"

			bucket, key, ok := strings.Cut(strings.TrimPrefix(entry.key, prefix), "/")

			if !ok {
				r.store.etcd.delIfRevision(entry.key, entry.rev)
				fixed("malformed")

				continue
			}

			try(func() {
				r.fix(bucket, key, entry.rev)
			}).catch(func(exc *Exception) {
				slog.Warn("repair", "bucket", bucket, "key", key, "err", exc.error())
				fixed("error")
			})
		}

		if !more {
			return
		}
	}
}

func (r *Repairer) fix(bucket, key string, entry int64) {
	for {
		m, rev, err := r.store.manifest(bucket, key)

		if errors.Is(err, errNoSuchKey) || (err == nil && m.Size == 0) {
			r.drop(bucket, key, entry)
			fixed("gone")

			return
		}

		throw(err)

		chunks := make([]*chunkState, len(m.Chunks))
		mine := make([]int, len(m.Chunks))

		var reqs []readReq

		for i := range m.Chunks {
			chunks[i] = m.chunk(i)
			mine[i] = r.pieceOf(key, i, chunks[i].by)

			if mine[i] >= 0 {
				reqs = append(reqs, chunks[i].reads(i, 0, 1, 2)...)
			}
		}

		have, _ := r.store.fetch(nil, reqs)

		for i, c := range chunks {
			c.have = [3][]byte{have[i*3], have[i*3+1], have[i*3+2]}
		}

		changed := false

		for i, c := range chunks {
			if mine[i] >= 0 && r.mend(bucket, key, c, mine[i], &m.Chunks[i]) {
				changed = true
			}
		}

		if !changed {
			r.drop(bucket, key, entry)
			fixed("sound")

			return
		}

		if r.store.etcd.putIfRevision(objKey(bucket, key), throw2(json.Marshal(m)), rev) {
			r.drop(bucket, key, entry)
			fixed("mended")
			slog.Info("repair: mended", "bucket", bucket, "key", key, "chunks", len(m.Chunks))

			return
		}

		fixed("raced")
		slog.Info("repair: key changed underneath, again", "bucket", bucket, "key", key)
	}
}

func (r *Repairer) mend(bucket, key string, c *chunkState, mine int, chunk *Chunk) bool {
	if c.sealed() && c.sound(mine) {
		return false
	}

	data, _ := c.assemble()

	if data == nil {
		throwFmt("repair: %s/%s: the other pieces of a chunk do not agree", bucket, key)
	}

	pieces := c.pieces(data)
	changed := false

	for i := range 3 {
		if p, ok := c.by[i]; ok && p.Xxh == "" && i != mine {
			p.Xxh = xxhHex(pieces[i])
			c.by[i] = p
			changed = true
		}
	}

	if !bytes.Equal(c.have[mine], pieces[mine]) {
		pl := r.store.newPlacer(map[int]string{mine: r.host})

		pl.add(mine, pieces[mine])
		pl.wait(nil, 1)

		p, ok := pl.placed[mine]

		if !ok {
			throwFmt("repair: %s/%s: no cell of %s took the piece", bucket, key, r.host)
		}

		c.by[mine] = p
		changed = true
	} else if p := c.by[mine]; p.Xxh == "" {
		p.Xxh = xxhHex(pieces[mine])
		c.by[mine] = p
		changed = true
	}

	if changed {
		chunk.Pieces = nil

		for i := range 3 {
			if p, ok := c.by[i]; ok {
				chunk.Pieces = append(chunk.Pieces, p)
			}
		}
	}

	return changed
}

func (r *Repairer) pieceOf(key string, i int, by map[int]Piece) int {
	for idx, p := range by {
		if r.store.byId[p.Cell].Host == r.host {
			return idx
		}
	}

	hosts := r.store.chunkHosts(key, i)

	for idx := range 3 {
		if _, ok := by[idx]; !ok && hosts[idx] == r.host {
			return idx
		}
	}

	return -1
}

func fixed(result string) {
	metricAdd("s3_repair_fixes_total", `result="`+result+`"`, 1)
}

func (r *Repairer) gauges() []gauge {
	return append(r.store.gauges(),
		gauge{"s3_repair_queue_entries", "", float64(r.store.etcd.count("repair/" + r.host + "/"))},
		gauge{"s3_repair_inprogress_entries", "", float64(r.store.etcd.count("inprogress/" + r.host + "/"))},
	)
}

func (r *Repairer) drop(bucket, key string, entry int64) {
	if !r.store.etcd.delIfRevision(repairKey(r.host, bucket, key), entry) {
		slog.Info("repair: the entry was written again meanwhile, kept for another pass", "bucket", bucket, "key", key)
	}
}
