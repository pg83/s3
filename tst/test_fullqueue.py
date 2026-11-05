"""A cell whose disk is full does not start; queued by hand, it starts
to be rebuilt instead, and serves again with the room its garbage took."""

import os
import time

import lib

MB = 1 << 20
HDD = 8 * MB

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=HDD, buckets=["f"]).start()
cluster.repair()
s3 = cluster.s3()
target = cluster.cell(0)


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if cond():
            return

        time.sleep(0.05)

    lib.fail(f"timed out waiting for {what}")


def settled():
    return not etcd.scan("inprogress/") and not etcd.scan("repair/")


# written and deleted until cell 0 has no room left: garbage all the way
blobs = {}
n = 0

while True:
    c = target.client()
    head, free = c.status()
    c.close()

    if free < MB:
        break

    key = f"o{n:03d}"
    data = os.urandom(MB)
    status, _, _ = s3.request("PUT", "/f/" + key, data)

    if status != 200:
        lib.fail(f"put {key}: {status}")

    wait("the third pieces", settled)

    if n % 4 == 0:
        blobs[key] = data
    else:
        s3.request("DELETE", "/f/" + key)

    n += 1

    if n > 200:
        lib.fail(f"cell 0 never filled: head {head}")

target.stop()
target.supervise()
wait("a start refused", lambda: "is full" in target.proc.text(), timeout=30)

r = cluster.run("compact", "h0/0")

if r.returncode != 0 or "queued" not in r.stdout:
    lib.fail(f"compact: rc={r.returncode} {r.stdout!r} {r.stderr!r}")

wait("the rebuild", lambda: not etcd.has("rebuild/0"), timeout=120)
lib.wait_port(target.port, timeout=60)

c = target.client()
head, free = c.status()
c.close()

if head != cluster.live(0) or free < HDD // 2:
    lib.fail(f"head {head}, {free} free after the rebuild, {cluster.live(0)} live")

for key, data in blobs.items():
    status, _, body = s3.request("GET", "/f/" + key)

    if status != 200 or body != data:
        lib.fail(f"get {key}: {status}")

print("ok")
