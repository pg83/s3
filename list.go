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

func (s *Store) list(bucket, prefix, delimiter, after string, maxKeys int) Listing {
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
	last := ""
	skip := ""
	count := 0

	for count < maxKeys {
		found, more := s.etcd.scan(root+prefix, from, listLimit)

		for _, entry := range found {
			rel := strings.TrimPrefix(entry.key, root)

			if skip != "" && strings.HasPrefix(rel, skip) {
				continue
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

					if count >= maxKeys {
						break
					}

					continue
				}
			}

			m := Manifest{}

			throw(json.Unmarshal(entry.value, &m))
			out.Objects = append(out.Objects, Listed{Key: rel, Manifest: m})
			count++
			last = rel

			if count >= maxKeys {
				break
			}
		}

		if !more || len(found) == 0 {
			break
		}
	}

	if count >= maxKeys {
		if found, _ := s.etcd.scan(root+prefix, from, 1); len(found) > 0 {
			out.Next = last
		}
	}

	return out
}
