package main

import (
	"encoding/json"
	"strings"
)

type Listed struct {
	Key string
	Manifest
}

type Listing struct {
	Dirs    []string
	Objects []Listed
	Next    string
}

func (s *Store) list(bucket, prefix, delimiter, after string, maxKeys int) (Listing, bool) {
	root := "obj/" + bucket + "/"
	from := root + prefix

	if after != "" && root+after > from {
		from = root + after + "\x00"

		if delimiter != "" && strings.HasSuffix(after, delimiter) {
			from = prefixEnd(root + after)
		}
	}

	out := Listing{}
	seen := map[string]bool{}

	var keys []string

	last := ""
	skip := ""
	count := 0
	truncated := false

	for count < maxKeys && !truncated {
		found, more, exists := s.etcd.scanIn(bucketKey(bucket), root+prefix, from, min(listLimit, maxKeys-count+1), true)

		if !exists {
			return out, false
		}

		for _, entry := range found {
			rel := strings.TrimPrefix(entry.key, root)

			if skip != "" && strings.HasPrefix(rel, skip) {
				continue
			}

			if count == maxKeys {
				truncated = true

				break
			}

			from = entry.key + "\x00"

			if delimiter != "" {
				if i := strings.Index(rel[len(prefix):], delimiter); i >= 0 {
					cp := rel[:len(prefix)+i+len(delimiter)]

					if !seen[cp] {
						seen[cp] = true
						out.Dirs = append(out.Dirs, cp)
						count++
						last = cp
					}

					skip = cp
					from = prefixEnd(root + cp)

					continue
				}
			}

			keys = append(keys, entry.key)
			count++
			last = rel
		}

		if !more || len(found) == 0 {
			break
		}
	}

	if truncated || (count == maxKeys && s.beyond(bucket, root+prefix, from)) {
		out.Next = last
	}

	values := s.etcd.fetch(keys)

	for _, key := range keys {
		m := Manifest{}

		throw(json.Unmarshal(values[key], &m))
		out.Objects = append(out.Objects, Listed{Key: strings.TrimPrefix(key, root), Manifest: m})
	}

	return out, true
}

func (s *Store) beyond(bucket, prefix, from string) bool {
	found, _ := s.etcd.scan(prefix, from, 1, true)

	return len(found) > 0
}
