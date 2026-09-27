"""A put records the debt of every host still short of a piece together
with the key, before it answers: a front that dies while a third piece
is on its way leaves the debt behind, and the host pays it once it is
back. With every host up the debts written at the answer race the
thirds that land a moment later; whoever loses, every object ends on
three pieces and the queues end empty."""

import base64
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
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=32 * MB, buckets=["settle"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    lib.fail(f"timed out waiting for {what}")


def keys_under(prefix):
    start = base64.b64encode(prefix.encode()).decode()
    end = base64.b64encode((prefix[:-1] + chr(ord(prefix[-1]) + 1)).encode()).decode()
    out = etcd.call("range", {"key": start, "range_end": end, "keys_only": True})

    return [base64.b64decode(kv["key"]).decode() for kv in out.get("kvs", [])]


def hosts_of(m):
    return sorted(cluster.host_of(p["cell"]) for p in lib.pieces(m))


# the front dies while the third piece waits on a host that answers nothing
stalled = cluster.host(2).cells[0]
data = os.urandom(12 * KB)
stalled.pause()

try:
    status, headers, _ = s3.request("PUT", "/settle/crash", data)
    if status != 200 or headers.get("etag") != '"' + lib.md5(data) + '"':
        lib.fail(f"put with a stalled host: {status} {headers.get('etag')}")

    m = etcd.manifest("settle", "crash")
    if hosts_of(m) != [0, 1]:
        lib.fail(f"the answer came before two pieces were on hosts 0 and 1: {m}")
    if keys_under("repair/") != ["repair/h2/settle/crash"]:
        lib.fail(f"the debt was not written with the key: {keys_under('repair/')}")

    cluster.front.stop()
    cluster.front.start()

    if hosts_of(etcd.manifest("settle", "crash")) != [0, 1] or keys_under("repair/") != ["repair/h2/settle/crash"]:
        lib.fail(f"the debt did not outlive the front: {keys_under('repair/')}")
finally:
    stalled.resume()

cluster.repair()

wait("host 2 to pay", lambda: not etcd.has("repair/h2/settle/crash") and hosts_of(etcd.manifest("settle", "crash")) == [0, 1, 2])

status, _, body = s3.request("GET", "/settle/crash")
if status != 200 or body != data:
    lib.fail(f"get after the debt was paid: {status} {len(body)}")

# every host up, repairs running: the debts race the thirds and lose or win cleanly
blobs = {f"k{i:02d}": os.urandom(8 * KB + i) for i in range(24)}
blobs["big"] = os.urandom(3 * 8 * MB // 2)

for key, data in blobs.items():
    status, _, _ = s3.request("PUT", "/settle/" + key, data)
    if status != 200:
        lib.fail(f"put {key}: {status}")

wait("every object on three pieces and the queues empty",
     lambda: not keys_under("repair/") and all(len(lib.pieces(etcd.manifest("settle", k))) == 3 * len(etcd.manifest("settle", k)["chunks"]) for k in blobs), 60)

for key, data in blobs.items():
    m = etcd.manifest("settle", key)
    for c in m["chunks"]:
        if sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) != [0, 1, 2]:
            lib.fail(f"{key}: a chunk is not on three hosts: {c}")

    status, _, body = s3.request("GET", "/settle/" + key)
    if status != 200 or body != data:
        lib.fail(f"get {key}: {status} {len(body)}")

print("ok")
