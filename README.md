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
offset is the position in that log. New data goes into `current.<n>` on
the SSD: a writer wakes every 100 ms, writes everything that arrived
since the last tick, fsyncs once and only then answers with the offsets.
A block that is full is renamed into `ready/`; a second goroutine copies
ready blocks to the raw HDD at `n * 2 MiB`, fsyncs and removes them.
Readers ask nobody: a block is opened from `lru/`, else hard-linked there
from `ready/`, else read from `current.<n>`, else copied from the HDD
into `lru/` under the one lock the cell has. `lru/` is swept by age
past its budget. No index, no checksum; the bytes are the front's to
interpret.

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
way, then appends the pieces to the three hosts in turn: `d0`, `d1`,
then `p`, each into the emptiest cell of its host. If the third append
fails, the two halves of data are still there and reads need no XOR.

Three appends landed: write the key and return. Two landed: write the
key with two sources plus a `repair/` entry, and return. Fewer: fail;
nothing is rolled back, the orphan pieces are garbage in their cells.

The key in etcd is written only after the pieces are durable, so a
reader never sees a pointer to bytes that are not there.

## Read

Fetch `d0` and `d1`, assemble, check the md5 kept in the key. On
mismatch fetch `p` as well and try the three combinations until one
matches; the piece the winning combination left out is the corrupt one,
and the key goes to the repair queue. No combination matches: the
object is lost.

Range requests assemble the whole object and return the slice.

## Repair

`s3 background` walks `repair/`: reads the two pieces, rebuilds the
third, appends it to the host that has none, rewrites the key with three
sources under a compare-and-swap on the etcd revision, drops the queue
entry. A key that was overwritten or deleted meanwhile is skipped. A
host that stays down keeps its keys at two sources until it returns.

## Metadata

```
obj/<bucket>/<key>      -> id, size, md5 (the ETag), mtime
repair/<bucket>/<key>   -> waiting for a third piece
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

TCP, one persistent connection per front-cell pair, requests and
responses strictly in turn, no multiplexing:

```
request:  op u8 | len u32 | payload
response: status u8 | len u32 | payload

op 1 append   payload = data                 -> offset u64
op 2 read     payload = offset u64, len u32  -> data
op 3 status                                  -> head u64, free u64

status: 0 ok, 1 full, 2 past the head, 3 io error
```

Cells and fronts talk over the storage network, which is private; there
is no authentication and no encryption on this link. The code speaks
`net.Conn`, so the transport can be swapped without touching the
protocol.

## Handlers

```
s3 cell -listen addr -ssd dir -hdd device
s3 front -c config.json -listen addr
s3 background -c config.json
s3 web -c config.json -listen addr
```

`s3 web` is the browser: `/` lists the buckets, `/b/<bucket>?prefix=`
walks a bucket folder by folder (the delimiter is `/`, 500 entries a
page, `after=` continues), and `/o/<bucket>/<key>` fetches an object
through the same reader the front uses. Each file shows its size, mtime,
md5 and how many of its three pieces are placed; a file on two pieces is
marked until the background finishes it. The page is a snapshot, it does
not poll.

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
