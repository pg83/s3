# s3

[![CI](https://github.com/pg83/s3/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/pg83/s3/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/pg83/s3/branch/master/graph/badge.svg)](https://app.codecov.io/gh/pg83/s3/tree/master)
[![Go version](https://img.shields.io/github/go-mod/go-version/pg83/s3)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An S3 store for a small cluster of cheap disks: three hosts, a few
spinning drives each, an SSD in front of every drive. Objects are cut
into erasure-coded pieces that live in append-only logs on the disks;
the namespace and every pointer live in etcd. One static binary, four
commands, one JSON config.

The shape follows the hardware. Spinning drives, drive-managed SMR
among them, take sequential writes well and seeks badly, so every disk
is an append-only log fed through the SSD in front of it, written in
whole blocks and flushed once per batch. Two data halves and their XOR
are all the arithmetic there is: any two of the three rebuild the
chunk, one host may be lost, and the overhead is 1.5x. The manifest of
an object carries where its pieces went, so a cell knows nothing about
objects and no table maps keys to disks. And nothing is retried below
the operation: the outcome of every call is known, and the S3 client
above retries the whole request.

- **One disk, one cell, three calls:** append, read and status over a
  log of 2 MiB blocks; the SSD takes the writes and serves the reads,
  the HDD is written in whole blocks with one flush per batch and read
  with `O_DIRECT`.
- **2+1 erasure coding that is just XOR:** every 8 MiB chunk is two
  halves and their parity on three hosts, each piece with its own xxh64.
- **Writes answer on two pieces:** the third is waited for after the
  answer, and a host that took less than its share is left owing it.
- **Reads check every piece and rebuild from parity:** a piece missing
  or corrupt is rebuilt from the two that hold, and its host is sent to
  repair; a range costs the chunks it touches, not the object.
- **Repair per host, on a watch:** each host mends only its own pieces,
  into its own cells, woken by etcd and by its links coming back, never
  by a timer.
- **Static cluster, one connection per cell:** every process holds every
  link from the start; requests are multiplexed, a link that dies is
  redialled and its table sent again, a host that drops packets is as
  gone as one that refuses them.
- **S3 the way clients speak it:** PUT, GET, HEAD and DELETE, ranges,
  listings in both versions, multi-object delete, aws-chunked bodies;
  the ETag is the md5.
- **A browser:** `s3 web` walks the buckets folder by folder and shows
  every object's size, mtime, md5 and how many of its pieces are placed.
- **Tested through failures:** the suite drives the real binary, under
  the race detector, instrumented for coverage, and against a build the
  kernel refuses at every turn it is meant to survive.

## Usage

```
s3 cell -listen addr [-listen addr] -load dir -store dir -hdd device
s3 front -c config.json -listen addr [-listen addr]
s3 repair -c config.json -host name
s3 web -c config.json -listen addr
```

One `cell` per disk: `-hdd` is the raw device, `-store` and `-load` are
two directories on the SSD in front of it, one for what is being
written and one for what is being read. A cell serves the same log on
every address it is given: loopback, where the repair of its own host
reaches it, and the cluster address for every other process. A cell
that finds its disk full starts, says so and exits.

One `repair` per host, named as in the config; it reaches its own cells
over loopback only and refuses to run otherwise. A `front` wherever S3
is to be served, on every address it is given. A `web` wherever a
browser is wanted. Every command takes `-debug` to log the timing of
every tick, flush, put and get, and stops on SIGTERM.

### Configuration

Every front, repair and web reads the same file:

```json
{
  "etcd": ["http://127.0.0.1:2379"],
  "buckets": ["photos", "backups"],
  "cells": [
    {"id": 0, "host": "lab1", "addr": "192.168.103.16:9100"},
    {"id": 1, "host": "lab1", "addr": "192.168.103.16:9101"}
  ]
}
```

`etcd` lists the endpoints. `buckets` is the whole list of buckets:
nothing about them lives in etcd, a bucket that is not in the list
answers NoSuchBucket before etcd is asked, and the API neither creates
nor deletes one. `cells` numbers every disk of the cluster, names its
host and gives the address the fronts and the other hosts' repairs
reach it at; three hosts at least, and the three pieces of a chunk
always go to three different ones.

### The API

The front speaks S3 over plain HTTP: PUT, GET and HEAD of an object,
DELETE, Range requests, `ListObjects` in both versions with prefix,
delimiter, marker and continuation token, `ListBuckets`, `POST
?delete` for up to a thousand keys, and bodies sent aws-chunked the
way the SDKs send them. The ETag is the object's md5. `?location` and
`?versioning` answer with an empty configuration, `?policy` with
NoSuchBucketPolicy, and every other bucket or object subresource with
NotImplemented. No signature is checked. What is left out on purpose
is listed under [Not in the MVP](#not-in-the-mvp).

### The browser

`s3 web` is the browser: `/` lists the buckets, `/b/<bucket>?prefix=`
walks a bucket folder by folder (the delimiter is `/`, 500 entries a
page, `after=` continues), and `/o/<bucket>/<key>` fetches an object
through the same reader the front uses. Each file shows its size, mtime,
md5 and how many of its three pieces are placed; a file on two pieces is
marked until the repair of the host that owes it finishes. The page is
a snapshot, it does not poll.

## Building

```
./build                       # .build/bin/s3, published as ./s3
./build test                  # end-to-end scenarios in tst/
./build -Drace test           # the same under the race detector (needs cgo)
./build -Dcoverage coverage   # instrumented run, profile in .build/coverage.out
./build chaos                 # every scenario against a binary the kernel refuses
./lint.sh                     # house style gate, needs a sibling ay checkout
```

Go 1.25 or newer and Python 3. The build tool is `build.py` on top of
the shared graph runner: outputs are cached by content and `./build -j
N` sets parallelism. Dependencies are pinned in `go.mod`; there is no
vendor directory.

The tests are end-to-end only. Every scenario in `tst/` is a Python
script driving the real binary as local processes, the cells over
sparse files as their disks. The scenarios over the front need an etcd
binary, `S3_TEST_ETCD=/path/to/etcd` or `etcd` on PATH, and skip
without one; the eviction scenario mounts a small tmpfs and needs
unprivileged user namespaces. The lab in `tst/lib.py` writes every
process log to a file, copies them to `S3_TEST_ARTIFACTS` when a
scenario fails, and stops what it started with SIGTERM when the
scenario ends.

With `-Dcoverage` the binary is instrumented and every process writes
its counters to a directory of its own under `GOCOVERDIR`;
`dev/coverage.py` refuses a run that lost any of them, adds them up
and holds the floor. A process leaves through `os.Exit` on SIGTERM so
that the counters reach the disk; one killed with SIGKILL loses them.

`./build chaos` runs every scenario again against `s3-chaos`, the same
binary built behind the `s3chaos` tag, whose `Syscalls` refuses some
calls the way the kernel is entitled to: an accept, a dial, a read or a
write on a link or on a cell's connection. `S3_CHAOS` names the points
and how often each fails, one call in so many, `all` arms every point,
`-name` disarms one, and `S3_CHAOS_SEED` makes the choice repeatable.
The suite arms only the points every scenario survives unchanged;
`tst/test_chaos.py` arms the rest and checks that every object still
goes in and comes back whole. Every call into the operating system that
can fail belongs in `syscalls.go`; `dev/chaos_points.py` refuses a
point that is declared but never asked about.

CI runs the plain suite, the race detector, the instrumented suite and
the instrumented chaos suite on every push, adds the two profiles up
with `dev/merge_coverage.py` under a floor, and uploads one report to
Codecov.

`dev/stand.py` drives a live stand through a few hundred objects of
mixed sizes: put, list, the flush to the HDD, reads from the HDD and
then from the load, ranges, an overwrite, deletes one by one and in
bulk, with timings; `--hosts` adds a look inside every cell over ssh,
`--bucket` names a bucket of the config. Run it on a host against the
local front to measure the stand rather than the way in.

See [STYLE.md](STYLE.md) for the code style, [CLAUDE.md](CLAUDE.md)
for the working conventions and [CONTRIBUTING.md](CONTRIBUTING.md)
before sending a change.

## Design

### Cells

A cell is one disk. Its whole API is three calls:

```
append(data)        -> offset      durable when it returns
read(offset, len)   -> data
status()            -> head, free
```

Inside, a cell is an append-only log cut into 2 MiB blocks, and the
offset is the position in that log. The SSD is two places so that
reads and writes do not fight over one: `-store` takes the writes,
`-load` serves the reads. New data goes into `<n>.current` in the
store: a writer wakes every 50 ms, writes everything that arrived
since the last tick, fsyncs once and only then answers with the offsets.
A block that is full is renamed to plain `<n>`; a second goroutine
copies every full block it finds to the raw HDD at `n * 2 MiB`, then
flushes the disk once for the whole batch and removes the files: a
flush costs a drive-managed SMR disk a hundred milliseconds, so it is
paid per batch, not per block, and a batch interrupted before its flush
is simply written again at the same offsets on the next start.
A store with no room left is the HDD falling behind: the writer waits
for the flusher to free a block and writes again, and everything above
it waits in turn. The HDD is opened with `O_DIRECT`, so neither the copies
out nor the copies back pass through the page cache; every block is
2 MiB at a 2 MiB offset from a page-aligned buffer, one per goroutine. A reader asks the load's one owner goroutine for its
block and gets an open file back: a copy in the load, else
`<n>.current`, else `<n>`, in that order because a block only ever
moves forward through them, else a copy the owner makes from the HDD.
The owner keeps the copies in the order they were last read; a copy
that does not fit, which the load reports as an error on the write,
evicts the least recently read copies until it does. A block on the
HDD never changes, so a copy is good until it is evicted: a restart
keeps them, the owner relearns what is there and takes the copy time
as the order, since that is all it can know. No index, no checksum;
the bytes are the front's to interpret.

A cell that runs out of space starts, does not open its port and exits.
Refilling an empty cell from the other two hosts is the compaction.

Nine cells, three per host, numbered 0 to 8.

### Objects

An object is cut into chunks of 8 MiB; the last one is shorter, and
an object up to 8 MiB is one chunk. Each chunk is split in half: `d0`
is the first half, `d1` the second (padded to the same length), `p`
is their XOR. For two data pieces and one parity piece Reed-Solomon
is XOR, so that is all the arithmetic. Any two pieces rebuild the
chunk. Overhead is 1.5x and one host may be lost.

The three pieces of a chunk go to three cells on three different
hosts, the order of hosts drawn from the key and the chunk's number,
so a big object spreads over every cell. The manifest of an object
records where each piece went and its xxh64:

```
{"size", "md5", "mtime", "type", "chunk": 8388608,
 "chunks": [{"pieces": [{"piece", "cell", "offset", "xxh"}, ...]}, ...]}
```

`piece` is 0 for `d0`, 1 for `d1`, 2 for `p`. The manifest carries the
placement, so nothing else has to: a cell knows nothing about objects,
and no table maps keys to disks. The md5 is the object's and is its
ETag; the xxh64 is each piece's own, so a bad piece is known by itself
and never by elimination.

Records written before chunks have their pieces at the top and no
xxh64; a reader turns one into a single chunk the size of the object
and takes the same path, checking the assembled chunk against the md5
because that is the only hash it has. The first repair that touches
such a record writes it back in the new shape, hashes and all.

### Write

The front reads the whole body into memory, a bounded number of bodies
at a time so that clients past that wait in their own sockets,
sends every chunk's two halves and parity the moment it has them, all
chunks at once, and computes the md5 for the key while the cells
think, so neither the hash nor the XOR sits in front of the sends.
Each piece goes into a cell of its host picked at random; a cell that
is full sends the piece to the next cell of the same host, a host
whose cell is down is left owing the piece. Nothing is retried: the
outcome of every append is known, and the client above retries the
whole request if too little of it landed.

Every chunk on two appends: write the key with those sources and
answer the client; it does not wait for the thirds. The thirds are
waited for after the answer: the key is written again once with all
that landed, under a compare-and-swap on the revision the answer was
given at, and a `repair/<host>/` entry is left for every host that
took less than its share. A chunk on fewer than two: fail; nothing is
rolled back, the orphan pieces are garbage in their cells.

The key in etcd is written only after the pieces are durable, so a
reader never sees a pointer to bytes that are not there.

### Read

A read touches only the chunks its range covers: it fetches their
`d0` and `d1` at once and checks each piece against its xxh64. A
piece missing or failing its hash: fetch `p` for that chunk as well
and rebuild the piece from the two that hold; the rebuilt one is
checked against its own hash too. Every piece found corrupt sends the
key to the repair queue of the host holding it. A piece whose cell is
down is simply absent for this read. A chunk with no two sound pieces:
the read fails and the client decides.

Range requests cost the chunks they touch, not the object.

### Repair

Every host runs `s3 repair -host <name>` over its own queue,
`repair/<name>/`: an entry there means this host owes pieces of that
key, either because it took nothing at write time or because a read
found a piece of its corrupt. One key may sit in the queues of several
hosts at once, each owing pieces of different chunks, and each repair
mends only its own: for every chunk it knows which piece is its, the
one on its cells or, when none is, the one the host order assigns it.
It reads all pieces of the chunks that concern it at once, over the
network for the other hosts', and where its piece is sound by its
hash there is nothing to do; otherwise it rebuilds the piece from the
two that hold, appends it to one of its own cells, which it reaches
over loopback only and refuses to run otherwise, and rewrites the key
once under a compare-and-swap on the etcd revision it read. When the
swap fails because another host's repair got there first, it reads
the key again and repeats its part; when nothing was left to mend it
drops the entry. A key that was overwritten or deleted meanwhile is
skipped. A host that stays down keeps its queue, and its keys short of
its pieces, until it returns; nobody repairs on its behalf.

The handler does not poll. It watches its prefix in etcd and walks the
queue when the watch opens and on every change under it; a fix that
failed because a cell was down stays in the queue, and the queue is
walked again when any of the process's links reconnects, which is the
moment such a fix can succeed.

### Metadata

```
obj/<bucket>/<key>      -> manifest: size, md5 (the ETag), mtime, chunks
repair/<host>/<bucket>/<key>   -> this host owes pieces
```

The buckets are the config's, a static list every front, repair and
web shares: nothing about them lives in etcd, a bucket that is not in
the list answers NoSuchBucket before etcd is asked, and the API neither
creates nor deletes one (PUT of a listed bucket says it is already
owned, PUT of any other and DELETE of any say AccessDenied). Credentials
will come the same way when they are needed.

etcd is spoken to over its gRPC client, one round trip per operation.
Listing is a range scan over `obj/<bucket>/<prefix>` that asks
for keys only, one key past the page so truncation is known without a
second scan, and then fetches the manifests of the page in one
transaction; a delimiter is a seek past each common prefix. Deleting
an object deletes its key and touches no cell; `POST /<bucket>?delete`
does that for up to a thousand keys in transactions of 128. A bucket
has no settings: `?location` and
`?versioning` answer with an empty configuration, `?policy` with
NoSuchBucketPolicy, and every other bucket or object subresource with
NotImplemented rather than with a listing that happens to share the URL.

### Transport

TCP, one connection per process-cell pair. The cluster is static, so
every process knows every connection it will ever have and holds all
of them from the start: a goroutine per cell, a channel in front of
it, and a loop that connects until it is connected, then talks until
the socket dies, then connects again. Requests are multiplexed on the
connection and answered in whatever order the cell finishes them. A
frame is a length, an op and a body whose layout the op decides; every
op so far starts its body with a request id that the reply carries
back:

```
frame:   len u32 | op u8 | body            len counts op and body

1 append     id u64 | data                 -> 0x81  id | offset u64
2 read       id u64 | offset u64 | n u32   -> 0x82  id | data
3 status     id u64                        -> 0x83  id | head u64, free u64
4 cancel     id u64 | target u64           -> nothing
                                           -> 0xff  id | code u8

code: 1 full, 2 past the head, 3 io error, 4 cancelled
```

The goroutine owns the connection and the table of messages it has
taken from its channel and not yet answered. Every message gets
exactly one outcome: the reply with its id, or a synthetic `link down`.
A socket that dies produces neither; the goroutine reconnects and
sends the table again, and a connect that fails, refused or not
answered within five seconds, means the cell is not there: then
everything in the table gets `link down` and the senders decide what
that means for their operation. While the link is down a message
arriving on the channel prompts a connect right away, and everything
that arrives during that attempt is answered by its outcome, so the
answer is as fresh as the last attempt, never a timer, and never a
queue of attempts. Nothing is shared, nothing is locked, nothing is
retried below the operation.

On the cell an accept the kernel refuses is logged and tried again a
moment later; the connection waits in the backlog meanwhile. The
connection's reader puts every append straight into
the writer's queue and blocks when the queue is full, so it stops
reading the socket, the window closes and the front waits; reads and
status get a goroutine each. The writer answers into the connection's
channel of replies and a goroutine per connection serializes them to
the socket, draining what is left for a dead peer so the writer never
stalls on one. A cancel names the id of an earlier message and rides
the same queue behind it: the writer drops the append if it is still
in the tick's batch, and a front sends one for everything an operation
was waiting for when its client walks away, so nothing is written for
somebody who is no longer there.

Cells and fronts talk over the storage network, which is private; there
is no authentication and no encryption on this link. The code speaks
`net.Conn`, so the transport can be swapped without touching the
protocol.

### Not in the MVP

No compaction: a full cell is emptied and refilled. No scrub. No
rebuild walk over etcd for a lost cell. No limits on object size or
concurrent uploads. No multipart uploads, no server-side copy, no
signatures checked, no CORS, no bucket policies, ACLs or versioning,
no buckets made over the API.

## License

MIT. See [LICENSE](LICENSE).
