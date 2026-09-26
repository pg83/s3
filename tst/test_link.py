"""A host that drops packets is as gone as one that refuses them: with the
cells of a host behind a black hole, a put answers on the two hosts that
are there and leaves the debt for the third, and a read of an object with
a data piece behind the hole is served through the parity, neither
waiting for the client to give up."""

import json
import os
import socket
import subprocess
import time

import lib

MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=16 * MB, buckets=["hole"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    lib.fail(f"timed out waiting for {what}")


def black_hole():
    """A listener whose one backlog slot is taken: every further SYN is dropped."""

    ln = socket.socket()
    ln.bind(("127.0.0.1", 0))
    ln.listen(0)
    filler = socket.create_connection(ln.getsockname())

    return ln, filler


# objects placed while every host answers; one of them keeps a data piece on host 2
blobs = {f"before{i}": os.urandom(12345) for i in range(6)}

for key, data in blobs.items():
    status, _, _ = s3.request("PUT", "/hole/" + key, data)
    if status != 200:
        lib.fail(f"put {key}: {status}")

for key in blobs:
    wait(f"three pieces of {key}", lambda: len(lib.pieces(etcd.manifest("hole", key))) == 3)

victim = next((key for key in blobs
               if any(cluster.host_of(p["cell"]) == 2 and p["piece"] < 2 for p in lib.pieces(etcd.manifest("hole", key)))), None)
if victim is None:
    lib.fail("no object keeps a data piece on host 2")

# a second front sees the cells of host 2 behind black holes
with open(cluster.config) as f:
    spec = json.load(f)

holes = []

for c in spec["cells"]:
    if c["host"] == "h2":
        holes.append(black_hole())
        c["addr"] = "127.0.0.1:%d" % holes[-1][0].getsockname()[1]

config = cluster.config + ".hole"

with open(config, "w") as f:
    json.dump(spec, f)

port = lib.free_port()
front = subprocess.Popen(
    [lib.BINARY, "front", "-c", config, "-listen", f"127.0.0.1:{port}"],
    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
)
lib.wait_port(port)
holed = lib.S3(port)

# a put answers on the two hosts that are there and leaves the debt for the third
data = os.urandom(12345)
started = time.time()
status, _, _ = holed.request("PUT", "/hole/after", data)
took = time.time() - started

if status != 200:
    lib.fail(f"put with a host behind a hole: {status}")

if took > 3:
    lib.fail(f"put waited {took:.1f}s for the host behind the hole")

wait("the debt of host 2", lambda: etcd.has("repair/h2/hole/after"))

m = etcd.manifest("hole", "after")
if sorted(cluster.host_of(p["cell"]) for p in lib.pieces(m)) != [0, 1]:
    lib.fail(f"put behind a hole: {m}")

for h in ("h0", "h1"):
    if etcd.has(f"repair/{h}/hole/after"):
        lib.fail(f"repair queued on {h}, which took its piece")

status, _, body = holed.request("GET", "/hole/after")
if status != 200 or body != data:
    lib.fail(f"get after: {status} {len(body)}")

# a read of a data piece behind the hole is served through the parity, not held for the client
started = time.time()
status, _, body = holed.request("GET", "/hole/" + victim)
took = time.time() - started

if status != 200 or body != blobs[victim]:
    lib.fail(f"get {victim} through the hole: {status} {len(body)}")

if took > 30:
    lib.fail(f"get {victim} through the hole took {took:.1f}s")

front.terminate()
front.wait(timeout=10)

if "link: down" not in front.stderr.read():
    lib.fail("the front never declared the link down")

print("ok")
