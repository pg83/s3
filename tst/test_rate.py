"""A repair under -rate reads the pieces of other hosts no faster than the
rate: it counts the bytes it fetched from cells of other hosts and sleeps
until they are paid for, and what it mends is the same as without it."""

import os
import time

import lib

MB = 1 << 20
CHUNK = 8 * MB

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=200 * MB, buckets=["rate"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    lib.fail(f"timed out waiting for {what}")


# two objects of three chunks put while h2 is down: h2 owes a piece of each chunk
cluster.host(2).stop()
blobs = {key: os.urandom(3 * CHUNK) for key in ("a", "b")}
for key, data in blobs.items():
    status, _, _ = s3.request("PUT", "/rate/" + key, data)
    if status != 200:
        lib.fail(f"put {key}: {status}")
for key in blobs:
    wait(f"h2 owing {key}", lambda: etcd.has(f"repair/h2/rate/{key}"))
cluster.host(2).start()

# each chunk takes its two pieces from h0 and h1: 2 x 4 MiB, 48 MiB in all at 8 MiB/s
port = lib.free_port()
start = time.time()
lib.Proc("repair-h2", ["repair", "-debug", "-c", cluster.config, "-host", "h2", "-rate", "8M", *lib.metrics_args(port)]).start()
wait("both mended", lambda: not etcd.has("repair/h2/rate/a") and not etcd.has("repair/h2/rate/b"))
took = time.time() - start

if took < 5.5:
    lib.fail(f"48 MiB at 8 MiB/s took {took:.2f}s")

samples = lib.scrape(port)
moved = lib.metric(samples, "s3_repair_network_bytes_total")
paused = lib.metric(samples, "s3_repair_paused_seconds_total")
if moved != 48 * MB:
    lib.fail(f"bytes read from other hosts: {moved}, not {48 * MB}")
if paused < 5:
    lib.fail(f"paused {paused:.2f}s for 48 MiB at 8 MiB/s")

for key, data in blobs.items():
    m = etcd.manifest("rate", key)
    if [sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) for c in m["chunks"]] != [[0, 1, 2]] * 3:
        lib.fail(f"{key} is not on three hosts after the repair: {m}")
    status, _, body = s3.request("GET", "/rate/" + key)
    if status != 200 or body != data:
        lib.fail(f"get {key} after the repair: {status}")

print("ok")
