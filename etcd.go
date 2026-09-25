package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
)

type Etcd struct {
	base string
	http *http.Client
}

type KV struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	ModRevision string `json:"mod_revision"`
}

type Entry struct {
	key   string
	value []byte
	rev   int64
}

func newEtcd(endpoints []string) *Etcd {
	return &Etcd{base: endpoints[0], http: &http.Client{Timeout: 30 * time.Second}}
}

func b64(s []byte) string {
	return base64.StdEncoding.EncodeToString(s)
}

func unb64(s string) []byte {
	return throw2(base64.StdEncoding.DecodeString(s))
}

func (e *Etcd) call(path string, req any) map[string]json.RawMessage {
	resp := throw2(e.http.Post(e.base+"/v3/kv/"+path, "application/json", bytes.NewReader(throw2(json.Marshal(req)))))

	defer resp.Body.Close()

	body := throw2(io.ReadAll(resp.Body))

	if resp.StatusCode != http.StatusOK {
		throwFmt("etcd %s: HTTP %d: %s", path, resp.StatusCode, body)
	}

	out := map[string]json.RawMessage{}

	throw(json.Unmarshal(body, &out))

	return out
}

func entries(raw json.RawMessage) []Entry {
	var kvs []KV

	if raw != nil {
		throw(json.Unmarshal(raw, &kvs))
	}

	out := make([]Entry, 0, len(kvs))

	for _, kv := range kvs {
		rev, _ := strconv.ParseInt(kv.ModRevision, 10, 64)

		out = append(out, Entry{key: string(unb64(kv.Key)), value: unb64(kv.Value), rev: rev})
	}

	return out
}

func (e *Etcd) get(key string) (Entry, bool) {
	found := entries(e.call("range", map[string]any{"key": b64([]byte(key))})["kvs"])

	if len(found) == 0 {
		return Entry{}, false
	}

	return found[0], true
}

func prefixEnd(prefix string) string {
	end := []byte(prefix)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++

			return string(end[:i+1])
		}
	}

	return "\x00"
}

func (e *Etcd) scan(prefix, from string, limit int) ([]Entry, bool) {
	start := prefix
	end := prefixEnd(prefix)

	if from > start {
		start = from
	}

	if end != "\x00" && start >= end {
		return nil, false
	}

	out := e.call("range", map[string]any{
		"key":       b64([]byte(start)),
		"range_end": b64([]byte(end)),
		"limit":     strconv.Itoa(limit),
	})

	var more bool

	if raw, ok := out["more"]; ok {
		throw(json.Unmarshal(raw, &more))
	}

	return entries(out["kvs"]), more
}

func (e *Etcd) put(key string, value []byte) {
	e.call("put", map[string]any{"key": b64([]byte(key)), "value": b64(value)})
}

func (e *Etcd) del(key string) {
	e.call("deleterange", map[string]any{"key": b64([]byte(key))})
}

func (e *Etcd) delPrefix(prefix string) {
	e.call("deleterange", map[string]any{"key": b64([]byte(prefix)), "range_end": b64([]byte(prefixEnd(prefix)))})
}

func (e *Etcd) putIfRevision(key string, value []byte, rev int64) bool {
	out := e.call("txn", map[string]any{
		"compare": []map[string]any{{
			"key":          b64([]byte(key)),
			"target":       "MOD",
			"result":       "EQUAL",
			"mod_revision": strconv.FormatInt(rev, 10),
		}},
		"success": []map[string]any{{
			"request_put": map[string]any{"key": b64([]byte(key)), "value": b64(value)},
		}},
	})

	var ok bool

	if raw, found := out["succeeded"]; found {
		throw(json.Unmarshal(raw, &ok))
	}

	return ok
}
