package main

import (
	"context"
	"log/slog"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const txnOps = 128

type Etcd struct {
	c *clientv3.Client
}

type Entry struct {
	key   string
	value []byte
	rev   int64
}

func newEtcd(endpoints []string) *Etcd {
	return &Etcd{c: throw2(clientv3.New(clientv3.Config{Endpoints: endpoints}))}
}

func entries(kvs []*mvccpb.KeyValue) []Entry {
	out := make([]Entry, 0, len(kvs))

	for _, kv := range kvs {
		out = append(out, Entry{key: string(kv.Key), value: kv.Value, rev: kv.ModRevision})
	}

	return out
}

func (e *Etcd) get(key string) (Entry, bool) {
	resp := throw2(e.c.Get(context.Background(), key))

	if len(resp.Kvs) == 0 {
		return Entry{}, false
	}

	return entries(resp.Kvs)[0], true
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

func rangeOpts(prefix, from string, limit int, keysOnly bool) (string, []clientv3.OpOption, bool) {
	start := prefix
	end := prefixEnd(prefix)

	if from > start {
		start = from
	}

	if end != "\x00" && start >= end {
		return "", nil, false
	}

	opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(int64(limit))}

	if keysOnly {
		opts = append(opts, clientv3.WithKeysOnly())
	}

	return start, opts, true
}

func (e *Etcd) scan(prefix, from string, limit int, keysOnly bool) ([]Entry, bool) {
	start, opts, ok := rangeOpts(prefix, from, limit, keysOnly)

	if !ok {
		return nil, false
	}

	resp := throw2(e.c.Get(context.Background(), start, opts...))

	return entries(resp.Kvs), resp.More
}

func (e *Etcd) fetch(keys []string) map[string][]byte {
	out := map[string][]byte{}

	for i := 0; i < len(keys); i += txnOps {
		chunk := keys[i:min(i+txnOps, len(keys))]
		ops := make([]clientv3.Op, 0, len(chunk))

		for _, k := range chunk {
			ops = append(ops, clientv3.OpGet(k))
		}

		resp := throw2(e.c.Txn(context.Background()).Then(ops...).Commit())

		for _, r := range resp.Responses {
			for _, kv := range r.GetResponseRange().Kvs {
				out[string(kv.Key)] = kv.Value
			}
		}
	}

	return out
}

func (e *Etcd) put(key string, value []byte) int64 {
	return throw2(e.c.Put(context.Background(), key, string(value))).Header.Revision
}

func (e *Etcd) putIfRevision(key string, value []byte, rev int64) bool {
	resp := throw2(e.c.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpPut(key, string(value))).
		Commit())

	return resp.Succeeded
}

func (e *Etcd) del(key string) {
	throw2(e.c.Delete(context.Background(), key))
}

func (e *Etcd) delAll(keys []string) {
	for i := 0; i < len(keys); i += txnOps {
		chunk := keys[i:min(i+txnOps, len(keys))]
		ops := make([]clientv3.Op, 0, len(chunk))

		for _, k := range chunk {
			ops = append(ops, clientv3.OpDelete(k))
		}

		throw2(e.c.Txn(context.Background()).Then(ops...).Commit())
	}
}

func (e *Etcd) watch(prefix string) chan struct{} {
	wake := make(chan struct{}, 1)

	go func() {
		for {
			ch := e.c.Watch(context.Background(), prefix, clientv3.WithPrefix())

			select {
			case wake <- struct{}{}:
			default:
			}

			for resp := range ch {
				if err := resp.Err(); err != nil {
					slog.Warn("etcd: watch", "prefix", prefix, "err", err)

					break
				}

				if len(resp.Events) > 0 {
					select {
					case wake <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	return wake
}
