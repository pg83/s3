package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	keeperLock = "lock/keeper"
	keeperTTL  = 10
)

type cellStatus struct {
	head int64
	free int64
	ok   bool
}

func runKeeper(cfg *Config, every time.Duration, full float64) {
	if every <= 0 {
		throwFmt("keeper: -every must be positive")
	}

	if full <= 0 || full > 1 {
		throwFmt("keeper: -full is a share of a disk, above 0 and at most 1")
	}

	s := newStore(cfg)
	session := throw2(concurrency.NewSession(s.etcd.c, concurrency.WithTTL(keeperTTL)))
	lock := concurrency.NewMutex(session, keeperLock)

	slog.Info("keeper: waiting for the lock")
	throw(lock.Lock(context.Background()))
	slog.Info("keeper: leading", "every", every, "full", full)

	go func() {
		<-session.Done()
		slog.Error("keeper: lost the lock")
		os.Exit(1)
	}()

	for {
		try(func() {
			keep(s, cfg, full)
		}).catch(func(exc *Exception) {
			slog.Warn("keeper", "err", exc.error())
		})

		time.Sleep(every)
	}
}

func keep(s *Store, cfg *Config, full float64) {
	if queued := rebuilding(s.etcd); len(queued) > 0 {
		slog.Info("keeper: a rebuild is under way", "cell", queued[0].key)

		return
	}

	status := s.status()
	ids := make([]int, 0, len(status))

	for id := range status {
		ids = append(ids, id)
	}

	sort.Ints(ids)

	best, most := -1, full

	for _, id := range ids {
		st := status[id]

		if !st.ok {
			slog.Info("keeper: a cell is down, nothing is queued", "cell", cellName(cfg, id))

			return
		}

		if total := st.head + st.free; total > 0 && float64(st.head)/float64(total) >= most {
			best, most = id, float64(st.head)/float64(total)
		}
	}

	if best < 0 {
		slog.Debug("keeper: no cell is full enough to compact")

		return
	}

	queue(s.etcd, best, "keeper")
	slog.Info("keeper: queued a compaction", "cell", cellName(cfg, best), "fill", most)
}

func (s *Store) status() map[int]cellStatus {
	reply := make(chan outcome, len(s.links))

	for id, l := range s.links {
		l.send(id, opStatus, nil, reply)
	}

	out := map[int]cellStatus{}

	for range s.links {
		o := <-reply
		st := cellStatus{}

		if o.op == opStatus|opReply && len(o.body) == 16 {
			st = cellStatus{head: int64(binary.BigEndian.Uint64(o.body)), free: int64(binary.BigEndian.Uint64(o.body[8:])), ok: true}
		}

		out[o.tag] = st
	}

	return out
}

func rebuilding(e *Etcd) []Entry {
	found, _ := e.scan("rebuild/", "", 1000, false)

	return found
}

func queue(e *Etcd, id int, by string) {
	e.put(rebuildKey(id), []byte(by+" "+time.Now().UTC().Format(time.RFC3339)))
}

func cellArg(cfg *Config, arg string) int {
	if host, index, ok := strings.Cut(arg, "/"); ok {
		i, err := strconv.Atoi(index)
		n := 0

		for _, c := range cfg.Cells {
			if c.Host != host {
				continue
			}

			if err == nil && n == i {
				return c.Id
			}

			n++
		}

		throwFmt("compact: no cell %s in the config", arg)
	}

	id, err := strconv.Atoi(arg)

	if _, ok := cellSpec(cfg, id); err != nil || !ok {
		throwFmt("compact: no cell %s in the config", arg)
	}

	return id
}

func cellName(cfg *Config, id int) string {
	n := 0
	spec, _ := cellSpec(cfg, id)

	for _, c := range cfg.Cells {
		if c.Id == id {
			break
		}

		if c.Host == spec.Host {
			n++
		}
	}

	return fmt.Sprintf("%d (%s/%d)", id, spec.Host, n)
}

func runCompact(cfg *Config, args []string) {
	e := newEtcd(cfg.Etcd)
	queued := rebuilding(e)

	if len(args) == 0 {
		if len(queued) == 0 {
			fmt.Println("no cell is being rebuilt")
		}

		for _, q := range queued {
			id, _ := strconv.Atoi(strings.TrimPrefix(q.key, "rebuild/"))

			fmt.Printf("cell %s: %s\n", cellName(cfg, id), q.value)
		}

		return
	}

	if len(args) > 1 {
		throwFmt("compact: one cell at a time")
	}

	id := cellArg(cfg, args[0])

	if len(queued) > 0 {
		throwFmt("compact: %s is being rebuilt already, one at a time", queued[0].key)
	}

	queue(e, id, "hand")
	fmt.Printf("cell %s queued\n", cellName(cfg, id))
}
