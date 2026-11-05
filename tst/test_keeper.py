"""Of two keepers one holds the lock and the other waits for it. The one
holding it queues the compaction of every cell filled past -full, one
cell at a time, never two keys at once; killed, it hands the lock over
and the other goes on. While a cell is down nothing is queued."""

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
seen, most, watching = set(), [0], [True]


def watch():
    while watching[0]:
        keys = etcd.scan("rebuild/")
        most[0] = max(most[0], len(keys))
        seen.update(keys)
        time.sleep(0.01)


threading.Thread(target=watch, daemon=True).start()

first = cluster.keeper("-every", "300ms", "-full", "0.5")
wait("a leader", lambda: "keeper: leading" in first.text())
second = cluster.keeper("-every", "300ms", "-full", "0.5")
wait("the second keeper", lambda: "keeper: waiting for the lock" in second.text())

wait("the first rebuild", lambda: len(seen) >= 1 and not etcd.scan("rebuild/"), timeout=120)

if "keeper: leading" in second.text():
    lib.fail("two keepers lead at once")

first.stop()
wait("the second keeper to lead", lambda: "keeper: leading" in second.text(), timeout=60)
wait("every cell compacted", lambda: not etcd.scan("rebuild/") and below(0.5), timeout=300)
wait("a round with nothing to do", lambda: "no cell is full enough" in second.text(), timeout=30)

if most[0] != 1 or seen != {f"rebuild/{i}" for i in range(6)}:
    lib.fail(f"rebuilds: {sorted(seen)}, at most {most[0]} at once")

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

wait("the keeper to see the cell down", lambda: second.text().count("a cell is down") >= 3)

if seen:
    lib.fail(f"queued with a cell down: {sorted(seen)}")

down.supervise()
wait("a rebuild after the cell is back", lambda: len(seen) >= 1, timeout=60)
wait("the rebuild", lambda: not etcd.scan("rebuild/"), timeout=120)

watching[0] = False

for key, data in blobs.items():
    status, _, body = s3.request("GET", "/k/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

print("ok")
