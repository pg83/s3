"""A cell whose disk and SSD come back empty is queued by its id and
rebuilds every piece from the other two of its chunk, nothing on its own
disk matching a hash. Keys of the old format get their hashes on the
way; a chunk with no two good pieces left is owed to the repair of the
host and does not hold the rebuild up. A key deleted or written again
while the rebuild runs keeps what it is now, the copy is garbage. What
the cell reads from other hosts it reads at its -rate."""

import json
import os
import shutil
import time

import lib

MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=100 * MB, buckets=["r"]).start()
cluster.repair()
s3 = cluster.s3()


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if cond():
            return

        time.sleep(0.05)

    lib.fail(f"timed out waiting for {what}")


sizes = [1, 1000, 100 * 1000, MB, 9 * MB]
blobs = {}

for i in range(30):
    key = f"o{i:02d}"
    blobs[key] = os.urandom(sizes[i % len(sizes)])
    status, _, _ = s3.request("PUT", "/r/" + key, blobs[key])

    if status != 200:
        lib.fail(f"put {key}: {status}")

wait("the third pieces", lambda: not etcd.scan("inprogress/") and not etcd.scan("repair/"))


def on(key, cell_id):
    return any(p["cell"] == cell_id for p in lib.pieces(etcd.manifest("r", key)))


# keys of the old format: one chunk, the pieces on top, no hashes
legacy = [k for k in sorted(blobs) if 0 < len(blobs[k]) <= 8 * MB and on(k, 0)][:4]

for key in legacy:
    m = etcd.manifest("r", key)
    old = {"size": m["size"], "md5": m["md5"], "mtime": m["mtime"],
           "pieces": [{"piece": p["piece"], "cell": p["cell"], "offset": p["offset"]} for p in m["chunks"][0]["pieces"]]}

    if "type" in m:
        old["type"] = m["type"]

    etcd.put(f"obj/r/{key}", json.dumps(old).encode())

# a chunk with its piece here and its other two corrupt: lost for good.
# The first piece of the log, so that every piece after it moves back
# and every key after it is written again.
first = []

for key in sorted(blobs):
    if key not in legacy and len(blobs[key]) <= 8 * MB:
        first += [(p["offset"], key) for p in lib.pieces(etcd.manifest("r", key)) if p["cell"] == 0]

lost = min(first)[1]
m = etcd.manifest("r", lost)
n = (m["size"] + 1) // 2
lost_bytes = n

for p in m["chunks"][0]["pieces"]:
    if p["cell"] != 0:
        cluster.corrupt(p["cell"], p["offset"], n)

# the last keys in the log of cell 0 change under the rebuild: one of
# two chunks with both their pieces here, deleted, and the one before,
# written again
for attempt in range(40):
    blobs["multi"] = os.urandom(9 * MB)
    status, _, _ = s3.request("PUT", "/r/multi", blobs["multi"])

    if status != 200:
        lib.fail(f"put multi: {status}")

    wait("the third pieces", lambda: not etcd.scan("inprogress/") and not etcd.scan("repair/"))

    if sum(p["cell"] == 0 for p in lib.pieces(etcd.manifest("r", "multi"))) == 2:
        break
else:
    lib.fail("no two chunks of one key on cell 0")

last = []

for key in sorted(blobs):
    if key not in legacy and key not in (lost, "multi"):
        last += [(p["offset"], key) for p in lib.pieces(etcd.manifest("r", key)) if p["cell"] == 0]

raced = ["multi", max(last)[1]]
live = cluster.live(0)

# the second piece of the deleted key is not written at all, the key
# being known gone by then
m = etcd.manifest("r", "multi")
skipped = (lib.chunk_len(m, max((p["offset"], i) for i, c in enumerate(m["chunks"]) for p in c["pieces"] if p["cell"] == 0)[1]) + 1) // 2

# a key no one can read is passed over
etcd.put("obj/r/zz-broken", b"{")

# the disk and the SSD of cell 0 are replaced
target = cluster.cell(0)
target.stop()

with open(target.hdd, "r+b") as f:
    f.truncate(0)
    f.truncate(100 * MB)

shutil.rmtree(target.store)
shutil.rmtree(target.load)

r = cluster.run("compact", "0")

if r.returncode != 0 or "cell 0 (h0/0) queued" not in r.stdout:
    lib.fail(f"compact 0: rc={r.returncode} {r.stdout!r} {r.stderr!r}")

target.extra = ["-batch", "1M", "-rate", "8M"]
target.supervise()

wait("the rebuild to plan", lambda: "rebuild planned" in target.proc.text(), timeout=60)
target.pause()

status, _, _ = s3.request("DELETE", "/r/" + raced[0])

if status != 204:
    lib.fail(f"delete {raced[0]}: {status}")

del blobs[raced[0]]
blobs[raced[1]] = os.urandom(1000)
status, _, _ = s3.request("PUT", "/r/" + raced[1], blobs[raced[1]])

if status != 200:
    lib.fail(f"put {raced[1]}: {status}")

target.resume()
samples = target.scrape()

if not lib.has_metric(samples, "s3_cell_rebuild_pieces_left") or lib.metric(samples, "s3_cell_capacity_bytes") != 100 * MB:
    lib.fail("no gauges while rebuilding")

wait("the rebuild", lambda: not etcd.has("rebuild/0"), timeout=120)
lib.wait_port(target.port, timeout=60)

done = [line for text in lib.cell_logs(0) for line in text.splitlines() if "cell: rebuilt" in line]

if len(done) != 1:
    lib.fail(f"rebuild summary: {done}")

summary = dict(kv.split("=", 1) for kv in done[0].split() if "=" in kv)

if (summary["self"] != "0" or summary["owed"] != "1" or summary["neighbours"] == "0" or summary["raced"] != "2" or summary["gone"] != "2"
        or summary["skipped"] != "1"):
    lib.fail(f"rebuild summary: {done[0]}")

head, _ = target.client().status()

if head != live - lost_bytes - skipped:
    lib.fail(f"head {head} after the rebuild, {live} live before it, {lost_bytes} of them lost, {skipped} skipped")

if not etcd.has(f"repair/h0/r/{lost}"):
    lib.fail("the lost piece is not owed to the repair")

# the old keys have their hashes now, and every piece of the cell is back
for key in legacy:
    m = etcd.manifest("r", key)

    if "chunks" not in m or any(not p.get("xxh") for p in lib.pieces(m)):
        lib.fail(f"{key} still lacks hashes: {m}")

c = target.client()

for key, data in blobs.items():
    if key == lost:
        continue

    m = etcd.manifest("r", key)

    for i, ch in enumerate(lib.chunks(m)):
        for p in ch["pieces"]:
            if p["cell"] == 0 and c.read(p["offset"], (lib.chunk_len(m, i) + 1) // 2) != lib.piece_bytes(data, m, i, p["piece"]):
                lib.fail(f"{key} chunk {i} is not back on cell 0")

    status, _, body = s3.request("GET", "/r/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

status, _, _ = s3.request("GET", "/r/" + lost)

if status == 200:
    lib.fail("an object with one good piece of a chunk read back")

status, _, _ = s3.request("GET", "/r/" + raced[0])

if status != 404:
    lib.fail(f"a key deleted under the rebuild: {status}")

etcd.delete("obj/r/zz-broken")

print("ok")
