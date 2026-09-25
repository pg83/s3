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
`-load` serves the reads. New data goes into `current.<n>` in the
store: a writer wakes every 100 ms, writes everything that arrived
since the last tick, fsyncs once and only then answers with the offsets.
A block that is full is renamed into `ready/` in the store; a second
goroutine copies ready blocks to the raw HDD at `n * 2 MiB`, fsyncs and
removes them. Readers ask nobody: a block is opened from the load, else
from `current.<n>`, else from `ready/`, in that order because a block
only ever moves forward through them, else copied from the HDD into the
load under the one lock the cell has. The load is swept by age past its
budget. No index, no checksum; the bytes are the front's to interpret.

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

The front reads the whole body into memory, computing its md5 on the
way, then appends the three pieces to their three hosts at once, each
into a cell of its host picked at random; a cell that is full sends the
piece to the next cell of the same host, a host whose cell is down is
left owing the piece. Nothing is retried: the outcome of every append
is known, and the client above retries the whole request if too little
of it landed.

Three appends landed: write the key and return. Two landed: write the
key with two sources, then a `repair/<host>/` entry for the host that
took nothing, and return. Fewer: fail; nothing is rolled back, the
orphan pieces are garbage in their cells.

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

## Metadata

```
obj/<bucket>/<key>      -> id, size, md5 (the ETag), mtime
repair/<host>/<bucket>/<key>   -> this host owes a piece
bkt/<bucket>            -> bucket settings
```

Listing is a range scan over `obj/<bucket>/<prefix>`; a delimiter is a
seek past each common prefix. Deleting an object deletes its key and
touches no cell; `POST /<bucket>?delete` does that for up to a thousand
keys at once. A bucket has no settings yet: `?location` and
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
                                           -> 0xff  id | code u8

code: 1 full, 2 past the head, 3 io error
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
is retried below the operation. On the cell every frame is handled in
its own goroutine, so appends from one connection land in one tick and
one fsync, and a writer goroutine serializes the replies.

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
binary, `S3_TEST_ETCD=/path/to/etcd` or `etcd` on PATH. See `STYLE.md` for the code style
and `CLAUDE.md` for the working conventions.
