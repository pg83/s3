"""A put that answers while a host's piece is still on its way marks the
key under inprogress/<host>/ in the same write; the settle clears the
mark when the piece lands, or turns it into that host's debt when it
does not. A front that dies before its settle leaves the mark behind,
and `s3 scan` on that host hands marks older than -age to its repair,
which then pays. Nothing else ever reaches a repair queue."""

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
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=32 * MB, buckets=["scan"]).start()
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


def scan(host, age):
    r = lib.run("scan", "-c", cluster.config, "-host", host, "-age", age)
    if r.returncode != 0 or "scan: done" not in r.stderr:
        lib.fail(f"scan {host}: rc={r.returncode} stderr={r.stderr!r}")

    return r.stderr


# every host up: the marks written with the answers are cleared by the settles, nothing is owed
blobs = {f"k{i:02d}": os.urandom(8 * KB + i) for i in range(16)}
blobs["big"] = os.urandom(3 * 8 * MB // 2)

for key, data in blobs.items():
    status, _, _ = s3.request("PUT", "/scan/" + key, data)
    if status != 200:
        lib.fail(f"put {key}: {status}")

wait("every object on three pieces", lambda: all(len(lib.pieces(etcd.manifest("scan", k))) == 3 * len(etcd.manifest("scan", k)["chunks"]) for k in blobs))
wait("the marks to clear", lambda: not keys_under("inprogress/"))
if keys_under("repair/"):
    lib.fail(f"a healthy put reached a repair queue: {keys_under('repair/')}")

# a host down at write time: its settle turns the mark into the debt at once
cluster.host(2).stop()
status, _, _ = s3.request("PUT", "/scan/down", os.urandom(12 * KB))
if status != 200:
    lib.fail(f"put with a host down: {status}")

wait("the debt of the host that is down", lambda: keys_under("repair/") == ["repair/h2/scan/down"] and not keys_under("inprogress/"))
cluster.host(2).start()
etcd.delete("repair/h2/scan/down")

# the front dies while a third piece waits on a host that answers nothing: the mark stays
stalled = cluster.host(2).cells[0]
data = os.urandom(12 * KB)
stalled.pause()

try:
    status, headers, _ = s3.request("PUT", "/scan/crash", data)
    if status != 200 or headers.get("etag") != '"' + lib.md5(data) + '"':
        lib.fail(f"put with a stalled host: {status} {headers.get('etag')}")

    if hosts_of(etcd.manifest("scan", "crash")) != [0, 1] or keys_under("inprogress/") != ["inprogress/h2/scan/crash"]:
        lib.fail(f"the answer came without its mark: {keys_under('inprogress/')}")

    cluster.front.stop()
    cluster.front.start()

    if keys_under("inprogress/") != ["inprogress/h2/scan/crash"] or keys_under("repair/"):
        lib.fail(f"the mark did not outlive the front: {keys_under('inprogress/')} {keys_under('repair/')}")

    # a scan of another host and a scan with a long age leave the mark alone
    out1, out2 = scan("h1", "1h"), scan("h2", "1h")
    if "young=0" not in out1 or "young=1" not in out2:
        lib.fail(f"a scan counted marks it should not: {out1!r} {out2!r}")
    if keys_under("inprogress/") != ["inprogress/h2/scan/crash"] or keys_under("repair/"):
        lib.fail(f"a young mark was moved: {keys_under('inprogress/')} {keys_under('repair/')}")

    etcd.put("inprogress/h2/junk", b"not a time")
    time.sleep(1.1)

    out = scan("h2", "1s")
    if '"to repair"=2' not in out:
        lib.fail(f"scan of old marks: {out!r}")
    if keys_under("inprogress/") or sorted(keys_under("repair/")) != ["repair/h2/junk", "repair/h2/scan/crash"]:
        lib.fail(f"old marks not handed to the repair: {keys_under('inprogress/')} {keys_under('repair/')}")
finally:
    stalled.resume()

cluster.repair()

wait("host 2 to pay", lambda: not keys_under("repair/") and hosts_of(etcd.manifest("scan", "crash")) == [0, 1, 2])

status, _, body = s3.request("GET", "/scan/crash")
if status != 200 or body != data:
    lib.fail(f"get after the debt was paid: {status} {len(body)}")

for cmd in (["-host", "h2", "-age", "0s"], ["-host", "h9"]):
    r = lib.run("scan", "-c", cluster.config, *cmd)
    if r.returncode != 1:
        lib.fail(f"scan {cmd} was not refused: rc={r.returncode} stderr={r.stderr!r}")

print("ok")
