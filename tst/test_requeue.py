"""A repair drops a queue entry only if it is still the entry it took:
one written again while the repair was mending the key, by a settle, a
scan or a read that found a bad piece, is kept for another pass instead
of being lost with the old one."""

import os
import time

import lib

KB = 1 << 10
MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=32 * MB, buckets=["requeue"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    lib.fail(f"timed out waiting for {what}")


def hosts_of(m):
    return sorted(cluster.host_of(p["cell"]) for p in lib.pieces(m))


# an object on two pieces, owed to host 2
data = os.urandom(12 * KB)
cluster.host(2).stop()
status, _, _ = s3.request("PUT", "/requeue/x", data)
if status != 200:
    lib.fail(f"put with host 2 down: {status}")

wait("the debt of host 2", lambda: etcd.has("repair/h2/requeue/x"))
if hosts_of(etcd.manifest("requeue", "x")) != [0, 1]:
    lib.fail(f"the object is not on hosts 0 and 1: {etcd.manifest('requeue', 'x')}")
cluster.host(2).start()

# the repair of host 2 takes the entry and stalls reading the piece on a host that answers nothing
stalled = cluster.host(0).cells[0]
stalled.pause()

try:
    repairs = cluster.repair()
    repair = next(p for p in repairs if p.name.startswith("repair-h2"))
    wait("the repair of host 2 to start", lambda: "watching the queue" in repair.text())
    time.sleep(2)

    if hosts_of(etcd.manifest("requeue", "x")) != [0, 1] or not etcd.has("repair/h2/requeue/x"):
        lib.fail("the repair got past the stalled host")

    # the entry is written again while the repair is mending the key
    etcd.put("repair/h2/requeue/x", b"")
finally:
    stalled.resume()

wait("the object on three pieces and the queue empty", lambda: hosts_of(etcd.manifest("requeue", "x")) == [0, 1, 2] and not etcd.has("repair/h2/requeue/x"))

if "written again meanwhile, kept" not in repair.text():
    lib.fail("the entry written during the mend was not kept for another pass")

status, _, body = s3.request("GET", "/requeue/x")
if status != 200 or body != data:
    lib.fail(f"get after the repair: {status} {len(body)}")

print("ok")
