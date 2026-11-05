"""A cell queued by hand writes its log again from the start with only
the pieces the keys point at: the garbage of deleted and overwritten
objects is gone, the head is exactly what is live, every piece reads back
matching its hash and every object whole. Puts and gets go on while the
cell is away, and it takes writes at its new head when it is back. The
queue holds one cell at a time and says what it holds."""

import json
import os
import random
import time

import lib

MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=200 * MB, buckets=["c"]).start()
cluster.repair()
s3 = cluster.s3()
rng = random.Random(7)


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if cond():
            return

        time.sleep(0.05)

    lib.fail(f"timed out waiting for {what}")


def put(key, data):
    status, _, out = s3.request("PUT", "/c/" + key, data)

    if status != 200:
        lib.fail(f"put {key}: {status} {out[:200]}")


def check(blobs):
    for key, data in blobs.items():
        status, _, body = s3.request("GET", "/c/" + key)

        if status != 200 or body != data:
            lib.fail(f"get {key}: {status}, {len(body)} bytes of {len(data)}")


def settled():
    return not etcd.scan("inprogress/") and not etcd.scan("repair/")


sizes = [0, 1, 1000, 300 * 1000, 2 * MB, 9 * MB, 20 * MB]
blobs = {}

for i in range(40):
    key = f"o{i:02d}"
    blobs[key] = os.urandom(rng.choice(sizes))
    put(key, blobs[key])

# garbage: a third overwritten, a third deleted
for i in range(0, 40, 3):
    key = f"o{i:02d}"
    blobs[key] = os.urandom(rng.choice(sizes))
    put(key, blobs[key])

for i in range(1, 40, 3):
    key = f"o{i:02d}"
    status, _, _ = s3.request("DELETE", "/c/" + key)

    if status != 204:
        lib.fail(f"delete {key}: {status}")

    del blobs[key]

wait("the third pieces", settled)

# a key of the old format, one chunk of 70 MB: its piece is longer than
# what the rebuild reads of the old log at once, and has no hash to be
# checked by, so it comes from the other two pieces
giant = os.urandom(70 * MB)
half = (len(giant) + 1) // 2
d0, d1 = giant[:half], giant[half:] + b"\0" * (2 * half - len(giant))
parity = (int.from_bytes(d0, "big") ^ int.from_bytes(d1, "big")).to_bytes(half, "big")
pieces = []

for piece, (cell_id, data) in enumerate(((0, d0), (2, d1), (4, parity))):
    c = cluster.cell(cell_id).client()
    pieces.append({"piece": piece, "cell": cell_id, "offset": c.append(data)})
    c.close()

etcd.put("obj/c/giant", json.dumps({"size": len(giant), "md5": lib.md5(giant), "mtime": "2026-09-26T12:00:00Z", "pieces": pieces}).encode())
blobs["giant"] = giant

target = cluster.cell(0)
head, _ = target.client().status()
live = cluster.live(0)

if live >= head:
    lib.fail(f"no garbage to compact: {live} live of {head}")

# the queue, by hand
r = cluster.run("compact")

if r.returncode != 0 or "no cell is being rebuilt" not in r.stdout:
    lib.fail(f"empty queue: rc={r.returncode} {r.stdout!r} {r.stderr!r}")

for args, want in ((("h9/0",), "no cell h9/0"), (("h0/7",), "no cell h0/7"), (("x",), "no cell x"), (("17",), "no cell 17"),
                   (("0", "1"), "one cell at a time")):
    r = cluster.run("compact", *args)

    if r.returncode != 1 or want not in r.stderr:
        lib.fail(f"compact {args}: rc={r.returncode} {r.stderr!r}")

target.extra = ["-batch", "1M"]
target.hold.set()
target.supervise()

r = cluster.run("compact", "h0/0")

if r.returncode != 0 or "cell 0 (h0/0) queued" not in r.stdout:
    lib.fail(f"compact h0/0: rc={r.returncode} {r.stdout!r} {r.stderr!r}")

r = cluster.run("compact", "3")

if r.returncode != 1 or "rebuild/0 is being rebuilt already" not in r.stderr:
    lib.fail(f"a second cell queued: rc={r.returncode} {r.stderr!r}")

r = cluster.run("compact")

if r.returncode != 0 or "cell 0 (h0/0): hand " not in r.stdout:
    lib.fail(f"queue listing: {r.stdout!r}")

# the cell leaves at once and comes back to rebuild; held right there,
# the cluster works without it
if not target.held.wait(30):
    lib.fail("the cell never left for its rebuild")

if not os.path.exists(os.path.join(target.store, "rebuild")):
    lib.fail("the cell left without marking its store")

check(blobs)

for i in range(10):
    key = f"during{i}"
    blobs[key] = os.urandom(rng.choice(sizes[1:5]))
    put(key, blobs[key])

target.resume()
wait("the rebuild", lambda: not etcd.has("rebuild/0"), timeout=120)
lib.wait_port(target.port, timeout=60)
wait("the third pieces", settled)

done = [line for text in lib.cell_logs(0) for line in text.splitlines() if "cell: rebuilt" in line]

if len(done) != 1:
    lib.fail(f"rebuild summary: {done}")

summary = dict(kv.split("=", 1) for kv in done[0].split() if "=" in kv)

if summary["neighbours"] != "1" or summary["owed"] != "0" or int(summary["head"]) != live:
    lib.fail(f"rebuild summary: {done[0]}, {live} live before it")

if os.path.exists(os.path.join(target.store, "rebuild")):
    lib.fail("the mark outlived the rebuild")

# what the repair owed the host meanwhile may have gone here since
after, _ = target.client().status()

if after != cluster.live(0):
    lib.fail(f"head {after}, {cluster.live(0)} live")



def pieces_there(cell_id, head):
    """Every piece the keys put on the cell lies below its head and holds
    what it should."""

    c = cluster.cell(cell_id).client()

    for key, data in blobs.items():
        m = etcd.manifest("c", key)

        for i, ch in enumerate(lib.chunks(m)):
            n = (lib.chunk_len(m, i) + 1) // 2

            for p in ch["pieces"]:
                if p["cell"] != cell_id:
                    continue

                if p["offset"] + n > head:
                    lib.fail(f"{key} chunk {i} ends at {p['offset'] + n}, past the head {head}")

                if not p.get("xxh") or c.read(p["offset"], n) != lib.piece_bytes(data, m, i, p["piece"]):
                    lib.fail(f"{key} chunk {i} piece {p['piece']} on cell {cell_id} is not what it should be")

    c.close()


pieces_there(0, after)

check(blobs)

# new writes land at the new head
for i in range(20):
    key = f"after{i}"
    blobs[key] = os.urandom(MB)
    put(key, blobs[key])

wait("the third pieces", settled)

grown, _ = target.client().status()

if grown <= after:
    lib.fail(f"the cell took no writes after its rebuild: head {grown}")

if cluster.live(0) != grown:
    lib.fail(f"head {grown} after new writes, {cluster.live(0)} live")

check(blobs)

r = cluster.run("compact")

if "no cell is being rebuilt" not in r.stdout:
    lib.fail(f"queue after the rebuild: {r.stdout!r}")

print("ok")
