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
- **A browser:** `s3 web` walks the buckets and shows every object's
  size, time, md5 and whether all of its pieces are in place.

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
s3 cell -listen addr [-listen addr] -load dir -store dir -hdd device
s3 front -c config.json -listen addr [-listen addr]
s3 repair -c config.json -host name
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
no space on it. A cell whose disk fills up refuses further writes and
keeps serving reads until it is restarted; after that it does not
start at all, and there is no tool yet to move its pieces elsewhere.
Size the disks for everything that will ever be written to them.

### Front

`s3 front` serves S3 over plain HTTP on every `-listen` it is given,
on one host or on all of them. No signature is checked: put it behind
whatever authenticates your clients. An object passes through the
front's memory whole, thirty two at a time; the rest wait.

### Repair

One `s3 repair` per host, `-host` being the host's name in the
config. It watches the host's queue in etcd and writes every piece the
host owes into the host's own cells, which it reaches over loopback
only; it refuses a config that puts them anywhere else. A host that
stays down keeps its queue until it returns; nobody repairs on its
behalf.

### Browser

`s3 web` is read-only: `/` lists the buckets, `/b/<bucket>?prefix=`
walks a bucket folder by folder, 500 entries a page, and
`/o/<bucket>/<key>` fetches an object the way the front does. An
object with a piece still owed is marked until its host has paid.
The page is a snapshot; it does not refresh.

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
is back. The debt is recorded together with the key, before the
answer, so a front that dies before the third piece lands leaves
nothing unpaid. With one host down, PUTs succeed and GETs are served through
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
limit on the size of an object beyond the front's memory. No space
reclaimed on a disk, ever, and no tool to empty a full or dead one
into the others yet.

## License

MIT. See [LICENSE](LICENSE).
