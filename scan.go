package main

import (
	"log/slog"
	"strings"
	"time"
)

func runScan(cfg *Config, host string, age time.Duration) {
	if host == "" {
		throwFmt("scan: -host is required")
	}

	if age <= 0 {
		throwFmt("scan: -age must be positive")
	}

	known := false

	for _, c := range cfg.Cells {
		known = known || c.Host == host
	}

	if !known {
		throwFmt("scan: the config has no cells of host %s", host)
	}

	etcd := newEtcd(cfg.Etcd)
	prefix := "inprogress/" + host + "/"
	now := time.Now()
	from := ""
	total, young, moved := 0, 0, 0

	for {
		found, more := etcd.scan(prefix, from, listLimit, false)

		for _, entry := range found {
			from = entry.key + "\x00"
			total++

			if stamp, err := time.Parse(time.RFC3339Nano, string(entry.value)); err == nil && now.Sub(stamp) < age {
				young++

				continue
			}

			if etcd.move(entry.key, "repair/"+host+"/"+strings.TrimPrefix(entry.key, prefix), entry.rev) {
				moved++
			}
		}

		if !more {
			break
		}
	}

	slog.Info("scan: done", "host", host, "in progress", total, "young", young, "to repair", moved)
}
