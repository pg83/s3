package main

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

func runBackground(cfg *Config) {
	s := newStore(cfg)

	slog.Info("background: watching the repair queue")

	for {
		try(func() {
			s.repairAll()
		}).catch(func(exc *Exception) {
			slog.Error("background", "err", exc.error())
		})

		time.Sleep(5 * time.Second)
	}
}

func (s *Store) repairAll() {
	from := ""

	for {
		found, more := s.etcd.scan("repair/", from, 100)

		for _, entry := range found {
			from = entry.key + "\x00"

			rest := strings.TrimPrefix(entry.key, "repair/")
			bucket, key, ok := strings.Cut(rest, "/")

			if !ok {
				s.etcd.del(entry.key)

				continue
			}

			r := Repair{Bad: -1}

			if len(entry.value) > 0 {
				throw(json.Unmarshal(entry.value, &r))
			}

			try(func() {
				s.repair(bucket, key, r.Bad)
				slog.Info("background: repaired", "bucket", bucket, "key", key, "bad", r.Bad)
			}).catch(func(exc *Exception) {
				slog.Warn("background: repair", "bucket", bucket, "key", key, "err", exc.error())
			})
		}

		if !more {
			return
		}
	}
}
