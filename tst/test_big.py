"""Objects bigger than a chunk are cut into chunks of 8 MiB, each placed
like a small object on its own three hosts, every piece carrying its
xxh64: a range reads only the chunks it touches, a corrupt piece is
seen by its own hash and only it is rebuilt, one key may owe pieces to
several hosts and each repair mends its own, a host down at write time
leaves every chunk on two pieces and its repair fills them all, and a
record written before chunks reads through the same path and is
upgraded by the first repair that touches it."""

import json
import os
import time
import xml.etree.ElementTree as ET

import lib

MB = 1 << 20
CHUNK = 8 * MB
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=3, hdd_bytes=160 * MB, buckets=["big"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    lib.fail(f"timed out waiting for {what}")


def get(key, headers=None):
    status, h, body = s3.request("GET", "/big/" + key, headers=headers)
    return status, h, body


# a three-chunk object
data = os.urandom(2 * CHUNK + 12345)
status, headers, _ = s3.request("PUT", "/big/three", data, {"Content-Type": "application/octet-stream"})
if status != 200 or headers.get("etag") != '"' + lib.md5(data) + '"':
    lib.fail(f"put three: {status} {headers.get('etag')}")

wait("nine pieces", lambda: len(lib.pieces(etcd.manifest("big", "three"))) == 9)
m = etcd.manifest("big", "three")
if m["chunk"] != CHUNK or len(m["chunks"]) != 3 or [len(c["pieces"]) for c in m["chunks"]] != [3, 3, 3]:
    lib.fail(f"manifest shape: {m}")
for p in lib.pieces(m):
    if len(p["xxh"]) != 16 or int(p["xxh"], 16) < 0:
        lib.fail(f"piece without xxh64: {p}")
if "pieces" in m:
    lib.fail("new record still carries top-level pieces")
for i, c in enumerate(m["chunks"]):
    if sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) != [0, 1, 2]:
        lib.fail(f"chunk {i} is not on three hosts: {c}")

status, headers, body = get("three")
if status != 200 or body != data or headers.get("etag") != '"' + lib.md5(data) + '"':
    lib.fail(f"get three: {status} {len(body)} {headers.get('etag')}")

status, headers, _ = s3.request("HEAD", "/big/three")
if status != 200 or int(headers["content-length"]) != len(data):
    lib.fail(f"head three: {status} {headers.get('content-length')}")

# ranges touch only their chunks
for spec, lo, hi in (
    ("bytes=0-9", 0, 10),
    ("bytes=100-199", 100, 200),
    (f"bytes={CHUNK - 100}-{CHUNK + 99}", CHUNK - 100, CHUNK + 100),
    (f"bytes={2 * CHUNK}-{2 * CHUNK}", 2 * CHUNK, 2 * CHUNK + 1),
    (f"bytes={2 * CHUNK + 1}-", 2 * CHUNK + 1, len(data)),
    ("bytes=-5", len(data) - 5, len(data)),
    (f"bytes=0-{3 * CHUNK}", 0, len(data)),
):
    status, headers, body = get("three", {"Range": spec})
    if status != 206 or body != data[lo:hi] or headers.get("content-range") != f"bytes {lo}-{hi - 1}/{len(data)}":
        lib.fail(f"range {spec}: {status} {len(body)} {headers.get('content-range')}")

# a corrupt piece in the middle chunk: read through its hash, only it rebuilt
cluster.repair()
p = next(p for p in m["chunks"][1]["pieces"] if p["piece"] == 1)
owner = f"h{cluster.host_of(p['cell'])}"
cluster.corrupt(p["cell"], p["offset"], 16)

status, _, body = get("three")
if status != 200 or body != data:
    lib.fail(f"get with a corrupt piece: {status}")

wait("the corrupt piece to be mended", lambda: not etcd.has(f"repair/{owner}/big/three") and etcd.manifest("big", "three") != m)
m2 = etcd.manifest("big", "three")
p2 = next(q for q in m2["chunks"][1]["pieces"] if q["piece"] == 1)
if p2["offset"] == p["offset"] or p2["xxh"] != p["xxh"] or cluster.host_of(p2["cell"]) != cluster.host_of(p["cell"]):
    lib.fail(f"the corrupt piece was not rewritten in place of the old one: {p} -> {p2}")
if [q for q in lib.pieces(m2) if q != p2] != [q for q in lib.pieces(m) if q != p]:
    lib.fail(f"repair touched sound pieces: {m} -> {m2}")

cluster.clear_lru()
status, _, body = get("three")
if status != 200 or body != data:
    lib.fail(f"get after the mend: {status}")

# one key owed to two hosts: each repair mends its own pieces
a = next(p for p in m2["chunks"][0]["pieces"] if p["piece"] == 0)
b = next(p for p in m2["chunks"][2]["pieces"] if cluster.host_of(p["cell"]) != cluster.host_of(a["cell"]))
cluster.corrupt(a["cell"], a["offset"], 16)
cluster.corrupt(b["cell"], b["offset"], 16)

status, _, body = get("three")
if status != 200 or body != data:
    lib.fail(f"get with two corrupt pieces: {status}")

wait("both hosts to mend", lambda: not etcd.has(f"repair/h{cluster.host_of(a['cell'])}/big/three")
     and not etcd.has(f"repair/h{cluster.host_of(b['cell'])}/big/three")
     and len([q for q in lib.pieces(etcd.manifest("big", "three")) if q not in lib.pieces(m2)]) == 2)
m3 = etcd.manifest("big", "three")
new = [q for q in lib.pieces(m3) if q not in lib.pieces(m2)]
if sorted((q["piece"], cluster.host_of(q["cell"])) for q in new) != sorted([(a["piece"], cluster.host_of(a["cell"])), (b["piece"], cluster.host_of(b["cell"]))]):
    lib.fail(f"the wrong pieces were rewritten: {new}")
if {q["xxh"] for q in new} != {a["xxh"], b["xxh"]}:
    lib.fail(f"rewritten pieces changed their hashes: {new}")

cluster.clear_lru()
status, _, body = get("three")
if status != 200 or body != data:
    lib.fail(f"get after two mends: {status}")

# a host down at write time: every chunk on two pieces, its repair fills them all
cluster.host(2).stop()
other = os.urandom(2 * CHUNK + 777)
status, _, _ = s3.request("PUT", "/big/degraded", other)
if status != 200:
    lib.fail(f"put with a host down: {status}")

wait("six pieces and the debt", lambda: len(lib.pieces(etcd.manifest("big", "degraded"))) == 6 and etcd.has("repair/h2/big/degraded"))
md = etcd.manifest("big", "degraded")
for c in md["chunks"]:
    if sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) != [0, 1]:
        lib.fail(f"degraded chunk is not on hosts 0 and 1: {c}")
for h in ("h0", "h1"):
    if etcd.has(f"repair/{h}/big/degraded"):
        lib.fail(f"repair queued on {h}, which took its pieces")

status, _, body = get("degraded")
if status != 200 or body != other:
    lib.fail(f"get degraded: {status}")

cluster.host(2).start()

wait("host 2 to fill its pieces", lambda: not etcd.has("repair/h2/big/degraded") and len(lib.pieces(etcd.manifest("big", "degraded"))) == 9, 60)
md3 = etcd.manifest("big", "degraded")
for c in md3["chunks"]:
    if sorted(cluster.host_of(p["cell"]) for p in c["pieces"]) != [0, 1, 2]:
        lib.fail(f"chunk not on three hosts after the repair: {c}")
if [q for q in lib.pieces(md3) if cluster.host_of(q["cell"]) != 2] != lib.pieces(md):
    lib.fail(f"the repair touched pieces it does not own: {md} -> {md3}")

# a record from before chunks: read by the same path, upgraded by its first repair
old_data = os.urandom(12345)
s3.request("PUT", "/big/old", old_data)
wait("three pieces", lambda: len(lib.pieces(etcd.manifest("big", "old"))) == 3)
mo = etcd.manifest("big", "old")
legacy = {"size": mo["size"], "md5": mo["md5"], "mtime": mo["mtime"], "pieces": [{"cell": p["cell"], "offset": p["offset"], "piece": p["piece"]} for p in lib.pieces(mo)]}
etcd.put("obj/big/old", json.dumps(legacy).encode())

status, headers, body = get("old")
if status != 200 or body != old_data or headers.get("etag") != '"' + lib.md5(old_data) + '"':
    lib.fail(f"get legacy: {status} {len(body)} {headers.get('etag')}")

status, _, body = get("old", {"Range": "bytes=100-199"})
if status != 206 or body != old_data[100:200]:
    lib.fail(f"legacy range: {status} {len(body)}")

lp = next(p for p in legacy["pieces"] if p["piece"] == 1)
cluster.corrupt(lp["cell"], lp["offset"], 16)

status, _, body = get("old")
if status != 200 or body != old_data:
    lib.fail(f"get legacy with a corrupt piece: {status}")

owner = f"h{cluster.host_of(lp['cell'])}"
wait("the legacy record to be upgraded", lambda: not etcd.has(f"repair/{owner}/big/old") and "chunks" in etcd.manifest("big", "old"))
mu = etcd.manifest("big", "old")
if mu["chunk"] != 12345 or len(mu["chunks"]) != 1 or "pieces" in mu or len(mu["chunks"][0]["pieces"]) != 3:
    lib.fail(f"upgraded record: {mu}")
for p in mu["chunks"][0]["pieces"]:
    if len(p.get("xxh", "")) != 16:
        lib.fail(f"upgraded piece without xxh: {p}")
up = next(p for p in mu["chunks"][0]["pieces"] if p["piece"] == 1)
if up["offset"] == lp["offset"] or cluster.host_of(up["cell"]) != cluster.host_of(lp["cell"]):
    lib.fail(f"the legacy corrupt piece was not rewritten on its host: {lp} -> {up}")
if [(p["cell"], p["offset"]) for p in mu["chunks"][0]["pieces"] if p["piece"] != 1] != [(p["cell"], p["offset"]) for p in legacy["pieces"] if p["piece"] != 1]:
    lib.fail(f"the upgrade moved sound pieces: {legacy} -> {mu}")

cluster.clear_lru()
status, _, body = get("old")
if status != 200 or body != old_data:
    lib.fail(f"get upgraded: {status}")

# the browser counts pieces over chunks
web = cluster.web()
status, _, body = web.request("GET", "/b/big")
html = body.decode()
if status != 200 or ">9/9<" not in html:
    lib.fail(f"web pieces column: {status} {html[-1500:]}")

print("ok")
