"""The keeper, run again and again as cron would, queues the compaction
of every cell filled past -full, the fullest first, one cell at a time
and never two keys at once; it leaves the cells alone while a rebuild
runs and while a cell is down, and says so."""

import os
import threading
import time

import lib

MB = 1 << 20
HDD = 16 * MB

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=HDD, buckets=["k"]).start()
cluster.repair()
s3 = cluster.s3()
cells = [cluster.cell(i) for i in range(6)]


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if cond():
            return

        time.sleep(0.05)

    lib.fail(f"timed out waiting for {what}")


def heads():
    """The head of every cell; None for one that is away, being rebuilt."""

    out = []

    for c in cells:
        try:
            client = c.client()
        except OSError:
            out.append(None)

            continue

        out.append(client.status()[0])
        client.close()

    return out


def below(share, skip=()):
    hs = [h for i, h in enumerate(heads()) if i not in skip]

    return None not in hs and max(hs) < share * HDD


def put(key, data):
    status, _, _ = s3.request("PUT", "/k/" + key, data)

    if status != 200:
        lib.fail(f"put {key}: {status}")


def settled():
    return not etcd.scan("inprogress/") and not etcd.scan("repair/")


def keeper():
    r = cluster.run("keeper", "-full", "0.5")

    if r.returncode != 0:
        lib.fail(f"keeper: rc={r.returncode} {r.stderr!r}")

    return r.stderr


# every cell past half its disk, most of it garbage
blobs = {}
n = 0

while min(h or 0 for h in heads()) < 0.55 * HDD:
    key = f"o{n:03d}"
    blobs[key] = os.urandom(MB)
    put(key, blobs[key])
    n += 1
    wait("the third pieces", settled)

    if n > 120:
        lib.fail(f"cells never filled: {heads()}")

for key in sorted(blobs)[4:]:
    status, _, _ = s3.request("DELETE", "/k/" + key)

    if status != 204:
        lib.fail(f"delete {key}: {status}")

    del blobs[key]

for c in cells:
    c.supervise()

# no key ever has company
seen, most, watching = [], [0], [True]


def watch():
    while watching[0]:
        try:
            keys = etcd.scan("rebuild/")
        except OSError:
            return

        most[0] = max(most[0], len(keys))

        for k in keys:
            if k not in seen:
                seen.append(k)

        time.sleep(0.01)


threading.Thread(target=watch, daemon=True).start()

# the first round queues the fullest cell, one of them on a tie, the
# next one leaves it be
now = heads()
fullest = [i for i in range(6) if now[i] == max(now)]
out = keeper()

if "queued a compaction" not in out or not any(etcd.has(f"rebuild/{i}") for i in fullest):
    lib.fail(f"the first round queued {sorted(etcd.scan('rebuild/'))}, not one of cells {fullest}: {out!r}")

if "a rebuild is under way" not in keeper():
    lib.fail("a round during a rebuild did not leave it alone")

deadline = time.time() + 300

while not (below(0.5) and not etcd.scan("rebuild/")):
    if time.time() > deadline:
        lib.fail(f"cells never compacted: {heads()}, queued {seen}")

    keeper()
    time.sleep(0.2)

if most[0] != 1 or sorted(seen) != [f"rebuild/{i}" for i in range(6)]:
    lib.fail(f"rebuilds: {seen}, at most {most[0]} at once")

if "no cell is full enough" not in keeper():
    lib.fail("a round with every cell compacted did something")

for key, data in blobs.items():
    status, _, body = s3.request("GET", "/k/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

# a cell down: the keeper leaves the cells alone, however full
down = cells[5]
down.stop()
seen.clear()

while below(0.6, skip=(5,)):
    key = f"p{n:03d}"
    blobs[key] = os.urandom(MB)
    put(key, blobs[key])
    n += 1

    if n > 400:
        lib.fail(f"cells never filled again: {heads()}")

for _ in range(3):
    if "a cell is down" not in keeper():
        lib.fail("a round with a cell down did not say so")

if seen:
    lib.fail(f"queued with a cell down: {seen}")

down.supervise()
lib.wait_port(down.port, timeout=30)

if "queued a compaction" not in keeper():
    lib.fail("nothing queued once the cell was back")

wait("the rebuild", lambda: seen and not etcd.scan("rebuild/"), timeout=120)
watching[0] = False

for key, data in blobs.items():
    status, _, body = s3.request("GET", "/k/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

print("ok")
