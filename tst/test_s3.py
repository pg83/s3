"""The S3 front over nine cells on three hosts and one etcd: buckets,
objects of every size, listing with prefixes and delimiters, ranges,
deletes; a host down at write time leaves two pieces and that host's
repair adds the third into its own cells; a host down at read time and
a corrupt piece are both served through the parity."""

import http.client
import json
import os
import time
import xml.etree.ElementTree as ET

import lib

MB = 1 << 20
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=3, hdd_bytes=64 * MB).start()
s3 = cluster.s3()

# buckets
status, _, _ = s3.request("PUT", "/photos")
if status != 200:
    lib.fail(f"create bucket: {status}")

status, _, _ = s3.request("PUT", "/photos")
if status != 409:
    lib.fail(f"create bucket twice: {status}")

status, _, body = s3.request("GET", "/")
names = [b.find(NS + "Name").text for b in ET.fromstring(body).iter(NS + "Bucket")]
if status != 200 or names != ["photos"]:
    lib.fail(f"list buckets: {status} {names}")

status, _, _ = s3.request("HEAD", "/nope")
if status != 404:
    lib.fail(f"head missing bucket: {status}")

# objects of every size
blobs = {
    "empty": b"",
    "tiny": b"a",
    "odd": os.urandom(12345),
    "dir/a.txt": b"hello",
    "dir/b.txt": b"world",
    "dir/sub/c.txt": b"deep",
    "big": os.urandom(5 * MB + 17),
}

for key, data in blobs.items():
    status, headers, _ = s3.request("PUT", "/photos/" + key, data, {"Content-Type": "text/plain"})
    if status != 200 or headers.get("etag") != '"' + lib.md5(data) + '"':
        lib.fail(f"put {key}: {status} {headers.get('etag')}")

for key, data in blobs.items():
    status, headers, body = s3.request("GET", "/photos/" + key)
    if status != 200 or body != data or headers.get("etag") != '"' + lib.md5(data) + '"' or headers.get("content-type") != "text/plain":
        lib.fail(f"get {key}: {status} len={len(body)} {headers.get('etag')} {headers.get('content-type')}")

    status, headers, _ = s3.request("HEAD", "/photos/" + key)
    if status != 200 or int(headers["content-length"]) != len(data):
        lib.fail(f"head {key}: {status} {headers.get('content-length')}")

status, _, _ = s3.request("GET", "/photos/nope")
if status != 404:
    lib.fail(f"get missing: {status}")

# ranges
status, headers, body = s3.request("GET", "/photos/big", headers={"Range": "bytes=100-199"})
if status != 206 or body != blobs["big"][100:200] or headers.get("content-range") != f"bytes 100-199/{len(blobs['big'])}":
    lib.fail(f"range: {status} {len(body)} {headers.get('content-range')}")

status, _, body = s3.request("GET", "/photos/big", headers={"Range": "bytes=-5"})
if status != 206 or body != blobs["big"][-5:]:
    lib.fail(f"suffix range: {status} {body!r}")

status, _, _ = s3.request("GET", "/photos/tiny", headers={"Range": "bytes=5-9"})
if status != 416:
    lib.fail(f"bad range: {status}")

# listing
def listing(query):
    status, _, body = s3.request("GET", "/photos?" + query)
    if status != 200:
        lib.fail(f"list {query}: {status}")
    root = ET.fromstring(body)
    keys = [c.find(NS + "Key").text for c in root.iter(NS + "Contents")]
    prefixes = [c.find(NS + "Prefix").text for c in root.iter(NS + "CommonPrefixes")]
    truncated = root.find(NS + "IsTruncated").text == "true"
    token = root.find(NS + "NextContinuationToken")
    return keys, prefixes, truncated, None if token is None else token.text

keys, prefixes, truncated, _ = listing("list-type=2")
if keys != sorted(blobs) or prefixes or truncated:
    lib.fail(f"list all: {keys} {prefixes} {truncated}")

keys, prefixes, _, _ = listing("list-type=2&delimiter=/")
if keys != ["big", "empty", "odd", "tiny"] or prefixes != ["dir/"]:
    lib.fail(f"list delimited: {keys} {prefixes}")

keys, prefixes, _, _ = listing("list-type=2&delimiter=/&prefix=dir/")
if keys != ["dir/a.txt", "dir/b.txt"] or prefixes != ["dir/sub/"]:
    lib.fail(f"list prefix: {keys} {prefixes}")

keys, _, truncated, token = listing("list-type=2&max-keys=3")
if keys != ["big", "dir/a.txt", "dir/b.txt"] or not truncated or not token:
    lib.fail(f"list page 1: {keys} {truncated} {token}")

keys, _, truncated, _ = listing("list-type=2&max-keys=3&continuation-token=" + token)
if keys != ["dir/sub/c.txt", "empty", "odd"] or not truncated:
    lib.fail(f"list page 2: {keys} {truncated}")

keys, _, _, _ = listing("prefix=dir/&marker=dir/a.txt")
if keys != ["dir/b.txt", "dir/sub/c.txt"]:
    lib.fail(f"list v1 marker: {keys}")

keys, prefixes, truncated, token = listing("list-type=2&delimiter=/&max-keys=2")
if keys != ["big"] or prefixes != ["dir/"] or not truncated or token != "dir/":
    lib.fail(f"list delimited page 1: {keys} {prefixes} {truncated} {token}")

keys, prefixes, _, _ = listing("list-type=2&delimiter=/&max-keys=2&continuation-token=dir/")
if keys != ["empty", "odd"] or prefixes:
    lib.fail(f"list delimited page 2: {keys} {prefixes}")

# the browser
web = cluster.web()

def page(path):
    status, headers, body = web.request("GET", path)
    if status != 200 or not headers.get("content-type", "").startswith("text/html"):
        lib.fail(f"web {path}: {status} {headers.get('content-type')}")
    return body.decode()

html = page("/")
if 'href="/b/photos"' not in html or "no buckets" in html:
    lib.fail(f"web buckets: {html[-800:]}")

html = page("/b/photos")
for needle in ('href="/b/photos?prefix=dir%2F"', 'href="/o/photos/big"', 'href="/o/photos/tiny"', ">Folders<", ">Files<"):
    if needle not in html:
        lib.fail(f"web bucket lacks {needle}: {html[-1200:]}")
if 'href="/o/photos/dir/a.txt"' in html:
    lib.fail("web bucket shows a nested file at the top level")

html = page("/b/photos?prefix=dir/")
for needle in ('href="/b/photos?prefix=dir%2Fsub%2F"', 'href="/o/photos/dir/a.txt"', '<span class="cur">dir</span>'):
    if needle not in html:
        lib.fail(f"web folder lacks {needle}: {html[-1200:]}")

html = page("/b/nope")
if "no such bucket" not in html:
    lib.fail("web missing bucket is not reported")

status, headers, body = web.request("GET", "/o/photos/big")
if status != 200 or body != blobs["big"] or headers.get("content-type") != "text/plain":
    lib.fail(f"web object: {status} {len(body)} {headers.get('content-type')}")

status, _, _ = web.request("GET", "/o/photos/nope")
if status != 404:
    lib.fail(f"web missing object: {status}")

# delete
status, _, _ = s3.request("DELETE", "/photos/dir/a.txt")
if status != 204:
    lib.fail(f"delete: {status}")

status, _, _ = s3.request("GET", "/photos/dir/a.txt")
if status != 404:
    lib.fail(f"get deleted: {status}")

status, _, _ = s3.request("DELETE", "/photos")
if status != 409:
    lib.fail(f"delete non-empty bucket: {status}")

# a repair refuses a config that reaches its own cells over the network
with open(cluster.config) as f:
    spec = json.load(f)
for c in spec["cells"]:
    if c["host"] == "h0":
        c["addr"] = "10.255.0.1:" + c["addr"].rsplit(":", 1)[1]
remote = cluster.config + ".remote"
with open(remote, "w") as f:
    json.dump(spec, f)
r = lib.run("repair", "-c", remote, "-host", "h0")
if r.returncode != 1 or "loopback" not in r.stderr:
    lib.fail(f"repair with remote own cells: rc={r.returncode} stderr={r.stderr!r}")

# a host down at write time: two pieces, then that host's repair adds the third
cluster.host(2).stop()
status, _, _ = s3.request("PUT", "/photos/degraded", blobs["odd"])
if status != 200:
    lib.fail(f"put with a host down: {status}")

m = etcd.manifest("photos", "degraded")
if len(m["pieces"]) != 2 or not etcd.has("repair/h2/photos/degraded"):
    lib.fail(f"degraded manifest: {m} repair={etcd.has('repair/h2/photos/degraded')}")

for other in ("h0", "h1"):
    if etcd.has(f"repair/{other}/photos/degraded"):
        lib.fail(f"repair queued on {other}, which took its piece")

status, _, body = s3.request("GET", "/photos/degraded")
if status != 200 or body != blobs["odd"]:
    lib.fail(f"get degraded: {status}")

cluster.host(2).start()
cluster.repair()

deadline = time.time() + 30
while time.time() < deadline and len(etcd.manifest("photos", "degraded")["pieces"]) < 3:
    time.sleep(0.5)

m3 = etcd.manifest("photos", "degraded")
if len(m3["pieces"]) != 3 or etcd.has("repair/h2/photos/degraded"):
    lib.fail(f"repaired manifest: {m3} repair={etcd.has('repair/h2/photos/degraded')}")

added = [p for p in m3["pieces"] if p not in m["pieces"]]
if len(added) != 1 or cluster.host_of(added[0]["cell"]) != 2:
    lib.fail(f"the third piece did not land on the host that owed it: {added}")

if sorted(cluster.host_of(p["cell"]) for p in m3["pieces"]) != [0, 1, 2]:
    lib.fail(f"pieces are not on three hosts: {m3}")

# a host down at read time: the parity fills in
for key in ("big", "odd", "tiny"):
    m = etcd.manifest("photos", key)
    down = cluster.host_of(next(p["cell"] for p in m["pieces"] if p["piece"] == 0))
    cluster.host(down).stop()
    status, _, body = s3.request("GET", "/photos/" + key)
    cluster.host(down).start()
    if status != 200 or body != blobs[key]:
        lib.fail(f"get {key} with host {down} down: {status} {len(body)}")

# a corrupt piece: the parity rebuilds it, the key is queued on the host holding it
m = etcd.manifest("photos", "odd")
p = next(p for p in m["pieces"] if p["piece"] == 1)
owner = f"h{cluster.host_of(p['cell'])}"
cluster.corrupt(p["cell"], p["offset"], 16)
status, _, body = s3.request("GET", "/photos/odd")
if status != 200 or body != blobs["odd"]:
    lib.fail(f"get with a corrupt piece: {status}")

if not etcd.has(f"repair/{owner}/photos/odd"):
    lib.fail(f"corrupt piece was not queued on {owner}")

deadline = time.time() + 30
while time.time() < deadline and etcd.has(f"repair/{owner}/photos/odd"):
    time.sleep(0.5)

if etcd.has(f"repair/{owner}/photos/odd"):
    lib.fail("corrupt piece was not repaired")

m2 = etcd.manifest("photos", "odd")
p2 = next(p for p in m2["pieces"] if p["piece"] == 1)
if p2 == p or f"h{cluster.host_of(p2['cell'])}" != owner:
    lib.fail(f"corrupt piece was not rewritten on {owner}: {p} -> {p2}")
if [q for q in m2["pieces"] if q["piece"] != 1] != [q for q in m["pieces"] if q["piece"] != 1]:
    lib.fail(f"repair touched the sound pieces: {m} -> {m2}")

cluster.clear_lru()
status, _, body = s3.request("GET", "/photos/odd")
if status != 200 or body != blobs["odd"]:
    lib.fail(f"get after repair: {status}")

# bucket and object subresources answer for themselves, never with a listing
for query, want, mark in (
    ("policy", 404, b"NoSuchBucketPolicy"),
    ("location", 200, b"LocationConstraint"),
    ("versioning", 200, b"VersioningConfiguration"),
    ("acl", 501, b"NotImplemented"),
    ("versions", 501, b"NotImplemented"),
):
    status, _, body = s3.request("GET", "/photos?" + query)
    if status != want or mark not in body or b"ListBucketResult" in body:
        lib.fail(f"bucket ?{query}: {status} {body[:200]}")

for method in ("PUT", "DELETE"):
    status, _, _ = s3.request(method, "/photos?policy", b"{}")
    if status != 501:
        lib.fail(f"{method} bucket ?policy: {status}")

status, _, _ = s3.request("HEAD", "/photos")
if status != 200:
    lib.fail("a subresource request touched the bucket itself")

status, _, _ = s3.request("GET", "/nope?location")
if status != 404:
    lib.fail(f"subresource of a missing bucket: {status}")

status, _, body = s3.request("GET", "/photos/odd?tagging")
if status != 501 or b"NotImplemented" not in body:
    lib.fail(f"object ?tagging: {status} {body[:200]}")

# multi-object delete
status, _, _ = s3.request("PUT", "/multi")
for key in ("x", "y", "z"):
    s3.request("PUT", "/multi/" + key, b"1")

body = b"<Delete><Object><Key>x</Key></Object><Object><Key>nope</Key></Object></Delete>"
status, _, out = s3.request("POST", "/multi?delete", body, {"Content-Type": "application/xml"})
deleted = [d.find(NS + "Key").text for d in ET.fromstring(out).iter(NS + "Deleted")]
if status != 200 or deleted != ["x", "nope"]:
    lib.fail(f"multi delete: {status} {deleted} {out[:200]}")

body = b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>true</Quiet><Object><Key>y</Key></Object></Delete>'
status, _, out = s3.request("POST", "/multi?delete", body, {"Content-Type": "application/xml"})
if status != 200 or list(ET.fromstring(out).iter(NS + "Deleted")):
    lib.fail(f"quiet multi delete: {status} {out[:200]}")

status, _, out = s3.request("GET", "/multi?list-type=2")
left = [c.find(NS + "Key").text for c in ET.fromstring(out).iter(NS + "Contents")]
if left != ["z"]:
    lib.fail(f"after multi delete: {left}")

print("ok")
