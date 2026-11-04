"""A repair passes its piece past a dead cell of its own host: a put that
drew a cell which refuses leaves the piece owed, the repair puts it in
the next cell of the host and mends the object in one go."""

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
cluster = lib.Cluster(etcd, hosts=3, cells=3, hdd_bytes=200 * MB, buckets=["dead"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    lib.fail(f"timed out waiting for {what}")


# put while h2 is down: every chunk owes h2 its piece
cluster.host(2).stop()
blobs = {key: os.urandom(4 * CHUNK) for key in ("a", "b", "c")}
for key, data in blobs.items():
    status, _, _ = s3.request("PUT", "/dead/" + key, data)
    if status != 200:
        lib.fail(f"put {key}: {status}")
for key in blobs:
    wait(f"h2 owing {key}", lambda: etcd.has(f"repair/h2/dead/{key}"))

# h2 comes back with its first cell dead: twelve pieces to place, each
# drawing the dead cell first a third of the time
for c in cluster.host(2).cells[1:]:
    c.start()
dead = 6

port = lib.free_port()
lib.Proc("repair-h2", ["repair", "-debug", "-c", cluster.config, "-host", "h2", *lib.metrics_args(port)]).start()
wait("all mended", lambda: not any(etcd.has(f"repair/h2/dead/{key}") for key in blobs))

samples = lib.scrape(port)
if lib.metric(samples, "s3_repair_fixes_total", result="mended") != len(blobs):
    lib.fail(f"mended: {lib.metric(samples, 's3_repair_fixes_total', result='mended')}, not {len(blobs)}")
if lib.metric(samples, "s3_repair_fixes_total", result="error") != 0:
    lib.fail(f"failed fixes: {lib.metric(samples, 's3_repair_fixes_total', result='error')}")

for key, data in blobs.items():
    m = etcd.manifest("dead", key)
    for c in m["chunks"]:
        if sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) != [0, 1, 2]:
            lib.fail(f"{key} is not on three hosts after the repair: {m}")
        if any(p["cell"] == dead for p in c["pieces"]):
            lib.fail(f"{key} has a piece on the dead cell: {m}")
    status, _, body = s3.request("GET", "/dead/" + key)
    if status != 200 or body != data:
        lib.fail(f"get {key} after the repair: {status}")

print("ok")
