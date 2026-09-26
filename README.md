# s3

An S3 store for a small cluster of cheap disks: three hosts, a few
spinning drives each, an SSD in front of every drive. Objects are cut
into erasure-coded pieces that live in append-only logs; the namespace
and every pointer live in etcd.

## Cells

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

## Objects

An object is split in half: `d0` is the first half, `d1` the second
(padded to the same length), `p` is their XOR. For two data pieces and
one parity piece Reed-Solomon is XOR, so that is all the arithmetic. Any
two pieces rebuild the object. Overhead is 1.5x and one host may be
lost.

The three pieces go to three cells on three different hosts. The id of
an object records where each piece went:

```
id = [(cell, offset, piece)...], len        piece: 0 = d0, 1 = d1, 2 = p
```

The id carries the placement, so nothing else has to: a cell knows
nothing about objects, and no table maps ids to disks.

## Write

The front reads the whole body into memory, a bounded number of bodies
at a time so that clients past that wait in their own sockets,
sends the two halves to their hosts the moment it has them, then the
parity, and computes the md5 for the key while the cells think, so
neither the hash nor the XOR sits in front of the sends. Each piece
goes into a cell of its host picked at random; a cell that is full
sends the piece to the next cell of the same host, a host whose cell is
down is left owing the piece. Nothing is retried: the outcome of every append
is known, and the client above retries the whole request if too little
of it landed.

Two appends landed: write the key with those two sources and answer
the client; it does not wait for the third. The third is waited for
after the answer: landed, the key is written again with three sources
under a compare-and-swap on the revision the answer was given at;
failed, a `repair/<host>/` entry is left for the host that took
nothing. Fewer than two: fail; nothing is rolled back, the orphan
pieces are garbage in their cells.

The key in etcd is written only after the pieces are durable, so a
reader never sees a pointer to bytes that are not there.

## Read

Fetch `d0` and `d1` at once, assemble, check the md5 kept in the key.
On mismatch or a half missing fetch `p` as well and try the remaining
combinations; the piece the winning combination left out is the
corrupt one, and the key goes to the repair queue of the host holding
it. A piece whose cell is down is simply absent for this read. No
combination matches: the read fails and the client decides.

Range requests assemble the whole object and return the slice.

## Repair

Every host runs `s3 repair -host <name>` over its own queue,
`repair/<name>/`: an entry there means this host owes a piece of that
key, either because it took nothing at write time or because a read
found its piece corrupt. The handler reads what the key has, all
pieces at once, over the network for the other hosts'; if all three are
there and agree
with the md5 and with each other the entry is stale and is dropped.
Otherwise it rebuilds its own piece from the other two, appends it to
one of its own cells, which it reaches over loopback only and refuses
to run otherwise, rewrites the key under a compare-and-swap on the etcd
revision and drops the entry. A key that was overwritten or deleted
meanwhile is skipped. A host that stays down keeps its queue, and its
keys at two sources, until it returns; nobody repairs on its behalf.

The handler does not poll. It watches its prefix in etcd and walks the
queue when the watch opens and on every change under it; a fix that
failed because a cell was down stays in the queue, and the queue is
walked again when any of the process's links reconnects, which is the
moment such a fix can succeed.

## Metadata

```
obj/<bucket>/<key>      -> id, size, md5 (the ETag), mtime
repair/<host>/<bucket>/<key>   -> this host owes a piece
bkt/<bucket>            -> bucket settings
```

etcd is spoken to over its gRPC client, and every operation on a bucket
is one round trip that carries the bucket's existence as a transaction
guard: a put, get, delete or listing under a bucket that is not there
fails inside etcd and comes back as NoSuchBucket without a lookup of
its own. Listing is a range scan over `obj/<bucket>/<prefix>` that asks
for keys only, one key past the page so truncation is known without a
second scan, and then fetches the manifests of the page in one
transaction; a delimiter is a seek past each common prefix. Deleting
an object deletes its key and touches no cell; `POST /<bucket>?delete`
does that for up to a thousand keys in transactions of 128. A bucket
has no settings yet: `?location` and
`?versioning` answer with an empty configuration, `?policy` with
NoSuchBucketPolicy, and every other bucket or object subresource with
NotImplemented rather than with a listing that happens to share the URL.

## Transport

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
sends the table again, and only a connect that comes back refused
means the cell is not there: then everything in the table gets
`link down` and the senders decide what that means for their
operation. While the link is down a message arriving on the channel
prompts a connect right away, so the answer is as fresh as the last
attempt, never a timer. Nothing is shared, nothing is locked, nothing
is retried below the operation.

On the cell the connection's reader puts every append straight into
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

## Handlers

```
s3 cell -listen addr -load dir -store dir -hdd device
s3 front -c config.json -listen addr
s3 repair -c config.json -host name
s3 web -c config.json -listen addr
```

`s3 web` is the browser: `/` lists the buckets, `/b/<bucket>?prefix=`
walks a bucket folder by folder (the delimiter is `/`, 500 entries a
page, `after=` continues), and `/o/<bucket>/<key>` fetches an object
through the same reader the front uses. Each file shows its size, mtime,
md5 and how many of its three pieces are placed; a file on two pieces is
marked until the repair of the host that owes it finishes. The page is
a snapshot, it does not poll.

Config is JSON:

```json
{
  "etcd": ["http://127.0.0.1:2379"],
  "cells": [
    {"id": 0, "host": "lab1", "addr": "192.168.103.16:9100"},
    {"id": 1, "host": "lab1", "addr": "192.168.103.16:9101"}
  ]
}
```

## Not in the MVP

No compaction: a full cell is emptied and refilled. No scrub. No
rebuild walk over etcd for a lost cell. No limits on object size or
concurrent uploads. No range reads served without assembling the whole
object. No multipart uploads, no server-side copy, no signatures
checked, no CORS, no bucket policies, ACLs or versioning.

## Build and test

`./build` builds `.build/bin/s3` and publishes `./s3`. `./build test`
runs the end-to-end suite in `tst/`; the S3 scenario needs an etcd
binary, `S3_TEST_ETCD=/path/to/etcd` or `etcd` on PATH. `dev/stand.py`
drives a live stand through a few hundred objects of mixed sizes: put,
list, the flush to the HDD, reads from the HDD and then from the load,
ranges, an overwrite, deletes one by one and in bulk, with timings;
`--hosts` adds a look inside every cell over ssh. Run it on a host
against the local front to measure the stand rather than the way in. See `STYLE.md` for the code style
and `CLAUDE.md` for the working conventions.
