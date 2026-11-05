# s3

[![CI](https://github.com/pg83/s3/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/pg83/s3/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/pg83/s3/branch/master/graph/badge.svg)](https://app.codecov.io/gh/pg83/s3/tree/master)
[![Go version](https://img.shields.io/github/go-mod/go-version/pg83/s3)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An S3 store for a small cluster of cheap disks: three hosts, a few
spinning drives each, an SSD in front of every drive. It survives the
loss of any one host at 1.5x the space, answers a write once the data
is on disk on two hosts, and speaks enough S3 for the usual clients
and SDKs. One static binary, four commands, one JSON config per host,
an etcd for the names.

- **Cheap disks, used the way they like:** every drive is written
  sequentially in whole blocks through the SSD in front of it, which
  also serves the reads. Drive-managed SMR disks are fine.
- **One host may be lost:** every object is two halves and their
  parity on three hosts, and any two rebuild it. Reads go on while a
  host is down; writes go on too, and the host makes up what it missed
  when it is back.
- **Every piece checked:** a corrupt piece is caught by its own hash,
  rebuilt from the other two on the spot, and rewritten by its host.
- **Nothing to rebalance:** an object's record says where its pieces
  are. Disks hold bytes and know nothing else.
- **S3 for the usual clients:** put, get, head, delete, ranges,
  listings, multi-object delete, the SDKs' chunked uploads. The ETag
  is the md5.
- **A browser:** `s3 web` shows the cluster's disks, SSD partitions,
  capacity, activity and health, and walks the buckets and objects.

## Building

Linux, Go 1.25 or newer.

```
./build              # .build/bin/s3, published as ./s3
go build -o s3 .     # the same, without the build tool
```

## Running

A cluster is at least three hosts, each with some disks and an SSD,
an etcd every host reaches, and a config file on every host.

```
s3 cell -listen addr [-listen addr] -load dir -store dir -hdd device [-c config.json -id n] [-rate 50M] [-batch 64M] [-metrics addr]
s3 front -c config.json -listen addr [-listen addr] [-metrics addr]
s3 repair -c config.json -host name [-rate 50M] [-metrics addr]
s3 scan -c config.json -host name [-age 10m]
s3 keeper -c config.json [-full 0.75]
s3 compact -c config.json [cell]
s3 web -c config.json -listen addr
```

Every command takes `-debug` for the timing of every write, flush and
read, and stops on SIGTERM.

### Cells

One `s3 cell` per disk. `-hdd` is the raw block device: the cell owns
it whole, there is no filesystem on it. `-store` and `-load` are two
directories on the SSD, one for what is being written and one for
what is being read. The store needs room for a few blocks of 2 MiB;
the load is a cache, give it what you can spare, since that is what
reads are served from. A cell serves on every `-listen` it is given:
loopback, where the repair of its own host reaches it, and the address
the other hosts reach it at.

A disk is an append-only log. Deleting or overwriting an object frees
no space on it until the cell is rebuilt, see below. A cell whose disk
fills up refuses further writes and keeps serving reads until it is
restarted; after that it does not start at all, unless it is queued
for a rebuild, which it looks for once at every start.

`-c` and `-id` tie a cell to the cluster: the config every host has,
and the cell's id in it. Without them the cell is just a log and is
never rebuilt.

### Rebuild

A cell is rebuilt when the key `rebuild/<id>` appears in etcd; the
keeper puts it there, or `s3 compact` by hand. The cell leaves at once,
without finishing what it was doing: what it acknowledged is on the
SSD, and what it did not is a cell gone down to whoever sent it. It
marks its store first, so the next start is a rebuild whatever etcd
says by then, and a rebuild once begun is always finished.

The rebuild serves nothing; to the rest of the cluster the cell is
down. It sends its old log to the HDD whole, forgets the copies in the
load, and writes the log again from the start with only the pieces the
keys point at, in the order of their offsets, having read every key
once before it starts. A piece never lands past where it was read from,
so the old bytes are always ahead of the new head: a piece that
matches its hash there comes from the cell itself, and one that does
not, or has no hash yet, is made from the other two pieces of its
chunk, at `-rate` from the other hosts. That makes one pass for a log
full of garbage and for a disk that came back empty. Every `-batch`
written reaches the SSD before the keys of what it holds are moved to
it, and goes on to the HDD under a single flush, as an SMR disk takes
a fifth of a second for a flush however little it holds; a key changed
meanwhile is read again, and a piece it no longer points at is left
behind as garbage. A chunk with no two good pieces
left is owed to the repair of the host. Killed in the middle, the cell
picks up where its SSD says the new log ends, keeps what the keys
already point at below that, and makes again what it had moved but
not yet put in a key. When done it removes its key and its mark,
exits, and serves at the new head from its next start.

While a cell is rebuilt its pieces live on two hosts, so one cell is
rebuilt at a time, and none while another cell is down. A key written
after the rebuild read the keys and still pointing at the cell's old
log is caught by the hash on its next read and mended by the repair.

### Keeper

`s3 keeper` runs once and exits; run it from cron every ten minutes or
so, from one place at a time, with a timeout: a cell that holds its
connection and says nothing keeps it waiting. It asks every cell how
full it is and queues the fullest past `-full` of its disk, unless a
rebuild is under way or a cell is down.

### Compact

`s3 compact -c config.json` lists the cells being rebuilt; with a cell,
by its id or as `host/index` (the index counts the cells of the host
in the order of the config), it queues that cell, one at a time, as
the keeper does. A new disk is put in place the same way: an empty
log and an empty SSD, queued, rebuilt from the other hosts.

### Front

`s3 front` serves S3 over plain HTTP on every `-listen` it is given,
on one host or on all of them. No signature is checked: put it behind
whatever authenticates your clients. An object passes through the
front's memory whole, thirty two at a time; the rest wait.

### Repair

One `s3 repair` per host, `-host` being the host's name in the
config. It watches the host's queue in etcd and writes every piece the
host owes into the host's own cells, which it reaches over loopback
only; it refuses a config that puts them anywhere else. A put gives
up on a host whose cell it drew is down and leaves the piece owed; the
repair tries the host's other cells before it gives up. A host that
stays down keeps its queue until it returns; nobody repairs on its
behalf. `-rate` caps the bytes a second it reads from the cells of
other hosts: after reading the pieces of an object it sleeps until
what it fetched is paid for at that rate. Without it the repair reads
as fast as the network goes.

### Scan

`s3 scan` runs once and exits; run it on every host from cron, every
quarter of an hour or so, with the host's `-host`. A PUT answered
before every piece was on disk leaves the key marked for the host
still owed a piece until the front has seen that piece land, or
queued the debt itself. A front that dies in between leaves the mark
behind, and the scan hands every mark of its host older than `-age`
to that host's repair.

### Browser

`s3 web` is read-only: `/` shows the cluster matrix, `/buckets` lists
the buckets, `/b/<bucket>?prefix=` walks a bucket folder by folder, 500 entries a page, and
`/o/<bucket>/<key>` fetches an object the way the front does. An
object with a piece still owed is marked until its host has paid.
The cluster view refreshes every 30 seconds. Bucket listings are snapshots.

The optional `dashboard` configuration supplies a Prometheus URL and
the S3 metadata etcd job (default `etcd_2`):

```json
"dashboard": {"prometheus": "http://127.0.0.1:8030", "etcd_job": "etcd_2"}
```

Prometheus must retain the `host` label and cell jobs named `s3_cell_0`,
`s3_cell_1`, etc., in cell-ID order on each host. The dashboard also reads
`smart_device_*`, `smart_ata_attribute_raw_value`, etcd metrics, and
`s3_volume_*` from the lab's volume textfile collector. Volume labels
identify `role` (`hdd`, `store`, `load`, `etcd`), `cell_index` (`-1` for
etcd), `device`, parent `disk` and `mount`. The collector publishes
filesystem capacity, used/free bytes, collection time, filesystem scrape
success and per-partition read/write counters.

Partition usage comes from the mounted filesystem. SSD history shows
cached or queued payload; the etcd history shows database size. A full
LOAD partition is a normal LRU cache, not a low-space warning. SMART
describes the physical disk. Missing metrics are shown as unavailable;
if Prometheus fails, the last successful snapshot is marked stale.
The API at `/api/cluster` serves the same cached data to all browsers.

### Metrics

A cell, a front and a repair given `-metrics addr` serve Prometheus
metrics at `/metrics` there; put it on loopback, it has no auth.
Counters and latency histograms keep what the process has done since it
started; gauges are computed when the page is asked for.

- cell: `s3_cell_write_seconds` (an append from arrival to its reply,
  fsync included), `s3_cell_writes_total{result}`,
  `s3_cell_read_seconds`, `s3_cell_reads_total{result}`, the bytes of
  both, `s3_cell_tick_seconds{phase}`, `s3_cell_flush_seconds{phase}`,
  `s3_cell_hdd_read_seconds`, `s3_cell_blocks_total{source}` (lru, store
  or hdd), `s3_cell_lru_evictions_total`, `s3_cell_errors_total{op}`;
  gauges: the write and read queues, the full blocks waiting for the
  HDD, the LRU in blocks and bytes, the head, the HDD pointer (how far
  the log is on the disk) and the capacity.
- front: `s3_front_requests_total{method,target,code}` (499 when the
  client left before an answer), `s3_front_request_seconds`, the bytes
  received and sent, and the bodies in flight.
- a cell being rebuilt: `s3_cell_rebuild_pieces_total{source}` and
  `s3_cell_rebuild_bytes_total{source}` (self, neighbours, kept, owed,
  skipped), `s3_cell_rebuild_keys_total{result}` (moved, raced, gone),
  `s3_cell_rebuild_hdd_read_bytes_total` (the old log read back),
  `s3_cell_rebuild_network_bytes_total`,
  `s3_cell_rebuild_paused_seconds_total` (slept to hold `-rate`); the
  gauge `s3_cell_rebuild_pieces_left` beside the cell's own.
- front, repair and a cell being rebuilt: `s3_link_seconds{cell,op}`
  (a request to a cell to its reply),
  `s3_link_failures_total{cell,op,reason}`,
  `s3_link_disconnects_total{cell}`, `s3_link_queue_messages{cell}`.
- repair: `s3_repair_fixes_total{result}`, `s3_repair_pass_seconds`,
  `s3_repair_pass_errors_total`, `s3_repair_network_bytes_total` (read
  from the cells of other hosts), `s3_repair_paused_seconds_total`
  (slept to hold `-rate`); gauges: the entries of its repair queue
  and the unsettled puts marked for its host.

### Configuration

```json
{
  "etcd": ["http://127.0.0.1:2379"],
  "buckets": ["photos", "backups"],
  "cells": [
    {"id": 0, "host": "lab1", "addr": "127.0.0.1:9100"},
    {"id": 1, "host": "lab1", "addr": "127.0.0.1:9101"},
    {"id": 2, "host": "lab2", "addr": "192.168.103.17:9100"},
    {"id": 3, "host": "lab2", "addr": "192.168.103.17:9101"},
    {"id": 4, "host": "lab3", "addr": "192.168.103.18:9100"},
    {"id": 5, "host": "lab3", "addr": "192.168.103.18:9101"}
  ]
}
```

Every process on a host reads that host's file: the cells of the host
itself at loopback, the cells of the other hosts at the addresses
they serve on the storage network. `id` numbers a disk across the
cluster and must be the same in every file, since the objects' records
refer to it. `buckets` is the whole list of buckets: a bucket not in
the list does not exist, and the API neither creates nor deletes one.
`etcd` lists the endpoints; everything about names lives there, so
back it up the way you back up any etcd.

### What to expect

A PUT is answered, with the md5 as its ETag, once two of the three
pieces of every part of the object are on disk on two hosts. The
third follows; if its host is down, the host owes it and pays when it
is back. With one host down, PUTs succeed and GETs are served through
the parity. With two hosts down, a GET of an object with pieces there
fails, and a PUT is turned away with `503 SlowDown` for the client to
retry; no key is written for it, and what did land is garbage on its
disks. A disk that fills up sends its
share to the next disk of the same host, and a host with no room left
owes its pieces like a host that is down.

Cells and fronts talk over the storage network without
authentication or encryption. Keep it private.

## Limits

No multipart uploads, no server-side copy, no signatures, no CORS, no
bucket policies, ACLs or versioning, no buckets made over the API. No
limit on the size of an object beyond the front's memory. Space comes
back only by rebuilding a cell whole, and there is no tool to empty a
dead disk into the other disks of its host.

## License

MIT. See [LICENSE](LICENSE).
