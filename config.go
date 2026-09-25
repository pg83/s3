package main

import (
	"encoding/json"
	"os"
)

type Cell struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Addr string `json:"addr"`
}

type Config struct {
	Etcd  []string `json:"etcd"`
	Cells []Cell   `json:"cells"`
}

func loadConfig(path string) *Config {
	if path == "" {
		throwFmt("config: -c is required")
	}

	cfg := &Config{}

	throw(json.Unmarshal(throw2(os.ReadFile(path)), cfg))

	if len(cfg.Etcd) == 0 {
		throwFmt("config: etcd endpoints are required")
	}

	seen := map[int]bool{}

	for _, c := range cfg.Cells {
		if seen[c.Id] {
			throwFmt("config: cell %d listed twice", c.Id)
		}

		if c.Host == "" || c.Addr == "" {
			throwFmt("config: cell %d needs host and addr", c.Id)
		}

		seen[c.Id] = true
	}

	return cfg
}
