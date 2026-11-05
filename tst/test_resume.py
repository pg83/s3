"""A rebuild killed in the middle, twice, picks up from where its SSD
says the new log ends: a piece the keys already point at below that
head is kept as it is, a piece moved but not yet in its key has no place
left ahead of the head and goes to the end, from the other two pieces of
its chunk. Nothing is lost and the head ends up exactly what is live."""

import json
import os
import time

import lib

MB = 1 << 20
BLOCK = 2 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=200 * MB, buckets=["s"]).start()
cluster.repair()
s3 = cluster.s3()


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if cond():
            return

        time.sleep(0.02)

    lib.fail(f"timed out waiting for {what}")


def put(key, data):
    status, _, _ = s3.request("PUT", "/s/" + key, data)

    if status != 200:
        lib.fail(f"put {key}: {status}")


# a little garbage at the very start of the log, live pieces after it:
# every piece moves back by a little, so one moved and not yet committed
# ends below the head the kill leaves behind
put("first", os.urandom(100 * 1000))
blobs = {}

for i in range(40):
    key = f"o{i:02d}"
    blobs[key] = os.urandom(MB)
    put(key, blobs[key])

status, _, _ = s3.request("DELETE", "/s/first")

if status != 204:
    lib.fail(f"delete: {status}")

wait("the third pieces", lambda: not etcd.scan("inprogress/") and not etcd.scan("repair/"))

# half of them in the old format: no hashes, so they come from the other
# hosts, at a rate slow enough to kill the rebuild between two commits
for key in sorted(blobs)[::2]:
    m = etcd.manifest("s", key)
    old = {"size": m["size"], "md5": m["md5"], "mtime": m["mtime"],
           "pieces": [{"piece": p["piece"], "cell": p["cell"], "offset": p["offset"]} for p in m["chunks"][0]["pieces"]]}

    etcd.put(f"obj/s/{key}", json.dumps(old).encode())

live = cluster.live(0)
target = cluster.cell(0)
target.extra = ["-batch", "1M", "-rate", "4M"]
target.supervise()


def store_head():
    nums = [int(n[:-8]) for n in os.listdir(target.store) if n.endswith(".current")]

    if not nums:
        return 0

    last = max(nums)

    return last * BLOCK + os.path.getsize(os.path.join(target.store, f"{last}.current"))


def committed(text):
    heads = [int(line.split(" head=")[1].split()[0]) for line in text.splitlines() if "rebuild committed" in line]

    return heads


r = cluster.run("compact", "h0/0")

if r.returncode != 0:
    lib.fail(f"compact: {r.stderr!r}")

# what was appended past the last commit when killed: a copy the keys
# never pointed at, garbage in the new log
wasted = 0

for kill in range(2):
    wait("a rebuild", lambda: "rebuild planned" in target.proc.text(), timeout=60)
    start = target.starts
    wait("three commits", lambda: len(committed(target.proc.text())) >= 3, timeout=60)

    # held still, killed only once something is appended past the last commit
    while True:
        target.pause()
        head = store_head()

        if head > committed(target.proc.text())[-1]:
            break

        target.resume()
        time.sleep(0.01)

    wasted += head - committed(target.proc.text())[-1]
    target.kill()
    wait("the start after the kill", lambda: target.starts > start, timeout=30)

wait("the rebuild", lambda: not etcd.has("rebuild/0"), timeout=180)
lib.wait_port(target.port, timeout=60)

done = [line for text in lib.cell_logs(0) for line in text.splitlines() if "cell: rebuilt" in line]

if len(done) != 1:
    lib.fail(f"rebuild summary: {done}")

summary = dict(kv.split("=", 1) for kv in done[0].split() if "=" in kv)

if int(summary["kept"]) == 0 or int(summary["homeless"]) == 0 or summary["owed"] != "0":
    lib.fail(f"the last start kept nothing or found nothing homeless: {done[0]}")

head, _ = target.client().status()

if head != live + wasted or cluster.live(0) != live:
    lib.fail(f"head {head}, {cluster.live(0)} live now, {live} before, {wasted} wasted by the kills")

c = target.client()

for key, data in blobs.items():
    m = etcd.manifest("s", key)

    for p in lib.pieces(m):
        if not p.get("xxh"):
            lib.fail(f"{key} has a piece without a hash: {m}")

        if p["cell"] == 0 and c.read(p["offset"], (m["size"] + 1) // 2) != lib.piece_bytes(data, m, 0, p["piece"]):
            lib.fail(f"{key} is not right on cell 0")

    status, _, body = s3.request("GET", "/s/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

print("ok")
