"""A cell that is full says so and the piece goes to the next cell of its
host; a put that cannot land on two hosts is turned away for the client to
retry and writes no key, nothing is rolled back; everything that landed
before is whole."""

import os

import lib

MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
# four megabytes a cell, two cells a host: sixteen objects of a megabyte fit, their halves being half a megabyte each
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=4 * MB, buckets=["full"]).start()
s3 = cluster.s3()

blobs = {}
refused = None

for i in range(40):
    key = f"o{i:02d}"
    data = os.urandom(MB)
    status, _, out = s3.request("PUT", "/full/" + key, data)

    if status == 503:
        if b"SlowDown" not in out:
            lib.fail(f"put into full cells: {status} {out[:200]}")

        refused = key

        break

    if status != 200:
        lib.fail(f"put {key}: {status} {out[:200]}")

    blobs[key] = data

if refused is None:
    lib.fail("forty objects of a megabyte went into twenty four megabytes of cells")

if not 10 <= len(blobs) <= 16:
    lib.fail(f"{len(blobs)} objects landed before the cells were full, sixteen fit")

if etcd.has("obj/full/" + refused):
    lib.fail("a put that was turned away wrote its key")

if 'err="cell: full"' not in cluster.front.log():
    lib.fail("no cell ever said it was full")

used = {}

for key in blobs:
    for p in lib.pieces(etcd.manifest("full", key)):
        used.setdefault(cluster.host_of(p["cell"]), set()).add(p["cell"])

if any(len(cells) != 2 for cells in used.values()) or len(used) != 3:
    lib.fail(f"the pieces did not move on to the next cell of the host: {used}")

for key, data in blobs.items():
    status, headers, body = s3.request("GET", "/full/" + key)

    if status != 200 or body != data or headers.get("etag") != '"' + lib.md5(data) + '"':
        lib.fail(f"get {key} from full cells: {status} {len(body)}")

status, _, _ = s3.request("GET", "/full?list-type=2")
if status != 200:
    lib.fail(f"list over full cells: {status}")

print("ok")
