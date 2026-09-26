"""The metadata paths: the buckets are the config's, so a bucket that
is not there is reported by every operation without asking etcd, and
none is created or deleted over the API; the front is the same front
on every address it listens on; a listing of hundreds of keys
comes back complete, with exact truncation at the last key, paged by
keys and by folders; three hundred keys go in one delete request; a
repair whose own cells were down when the key was queued finishes as
soon as they are back, without a restart or a poll."""

import base64
import json
import os
import threading
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
cluster = lib.Cluster(etcd, hosts=3, cells=3, hdd_bytes=64 * MB, buckets=["photos", "many", "life"], front_listeners=2).start()
s3 = cluster.s3()
other = cluster.s3(1)


def keys_under(prefix):
    start = base64.b64encode(prefix.encode()).decode()
    end = base64.b64encode((prefix[:-1] + chr(ord(prefix[-1]) + 1)).encode()).decode()
    out = etcd.call("range", {"key": start, "range_end": end, "keys_only": True})

    return [base64.b64decode(kv["key"]).decode() for kv in out.get("kvs", [])]


def listing(bucket, query):
    status, _, body = s3.request("GET", f"/{bucket}?" + query)
    if status != 200:
        lib.fail(f"list {bucket} {query}: {status} {body[:200]}")
    root = ET.fromstring(body)
    objects = [(c.find(NS + "Key").text, int(c.find(NS + "Size").text), c.find(NS + "ETag").text) for c in root.iter(NS + "Contents")]
    prefixes = [c.find(NS + "Prefix").text for c in root.iter(NS + "CommonPrefixes")]
    truncated = root.find(NS + "IsTruncated").text == "true"
    token = root.find(NS + "NextContinuationToken")
    marker = root.find(NS + "NextMarker")
    nxt = token if token is not None else marker

    return objects, prefixes, truncated, None if nxt is None else nxt.text


def multi_delete(bucket, keys, quiet=False):
    body = '<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    if quiet:
        body += "<Quiet>true</Quiet>"
    body += "".join(f"<Object><Key>{k}</Key></Object>" for k in keys) + "</Delete>"

    return s3.request("POST", f"/{bucket}?delete", body.encode(), {"Content-Type": "application/xml"})


# a missing bucket, reported by every operation
one_key = b"<Delete><Object><Key>k</Key></Object></Delete>"
for method, path, body in (
    ("PUT", "/nope/k", b"data"),
    ("PUT", "/nope/empty", b""),
    ("GET", "/nope/k", None),
    ("DELETE", "/nope/k", None),
    ("GET", "/nope?list-type=2", None),
    ("GET", "/nope?prefix=a&delimiter=/", None),
    ("POST", "/nope?delete", one_key),
    ("DELETE", "/nope", None),
    ("GET", "/nope?location", None),
):
    status, _, out = s3.request(method, path, body)
    if status != 404 or b"NoSuchBucket" not in out:
        lib.fail(f"{method} {path} on a missing bucket: {status} {out[:200]}")

for path in ("/nope", "/nope/k"):
    status, _, _ = s3.request("HEAD", path)
    if status != 404:
        lib.fail(f"HEAD {path} on a missing bucket: {status}")

status, _, out = s3.request("PUT", "/nope")
if status != 403 or b"AccessDenied" not in out:
    lib.fail(f"create a bucket over the API: {status} {out[:200]}")

if keys_under("obj/nope/") or keys_under("repair/") or keys_under("bkt/"):
    lib.fail(f"a missing bucket left keys behind: {keys_under('obj/nope/')} {keys_under('repair/')} {keys_under('bkt/')}")

# the buckets are the config's
status, _, body = s3.request("GET", "/")
names = [b.find(NS + "Name").text for b in ET.fromstring(body).iter(NS + "Bucket")]
if status != 200 or names != ["life", "many", "photos"]:
    lib.fail(f"list buckets: {status} {names}")

status, _, out = s3.request("PUT", "/life")
if status != 409 or b"BucketAlreadyOwnedByYou" not in out:
    lib.fail(f"create a configured bucket: {status} {out[:200]}")

status, _, _ = s3.request("HEAD", "/life")
if status != 200:
    lib.fail(f"head a configured bucket: {status}")

status, _, out = s3.request("GET", "/life/nope")
if status != 404 or b"NoSuchKey" not in out or b"NoSuchBucket" in out:
    lib.fail(f"missing key in a configured bucket: {status} {out[:200]}")

status, _, _ = s3.request("PUT", "/life/k", b"data")
if status != 200:
    lib.fail(f"put: {status}")

status, _, body = other.request("GET", "/life/k")
if status != 200 or body != b"data":
    lib.fail(f"get over the second address: {status} {body!r}")

status, _, body = other.request("GET", "/")
if status != 200 or [b.find(NS + "Name").text for b in ET.fromstring(body).iter(NS + "Bucket")] != ["life", "many", "photos"]:
    lib.fail(f"buckets over the second address: {status}")

for full in (True, False):
    status, _, out = s3.request("DELETE", "/life")
    if status != 403 or b"AccessDenied" not in out:
        lib.fail(f"delete a configured bucket, full={full}: {status} {out[:200]}")

    status, _, _ = s3.request("DELETE", "/life/k")
    if status != 204:
        lib.fail(f"delete key: {status}")

if keys_under("obj/life/") or keys_under("bkt/"):
    lib.fail(f"keys left behind: {keys_under('obj/life/')} {keys_under('bkt/')}")

# hundreds of keys
many = {f"k{i:04d}": bytes([i % 251]) * (i % 7 + 1) for i in range(300)}
many.update({f"d/{i}": b"folder" for i in range(5)})
failures = []
todo = list(many.items())


def put_some(items):
    for key, data in items:
        status, headers, _ = s3.request("PUT", "/many/" + key, data)
        if status != 200 or headers.get("etag") != '"' + lib.md5(data) + '"':
            failures.append(f"put {key}: {status}")


threads = [threading.Thread(target=put_some, args=(todo[i::16],)) for i in range(16)]
for th in threads:
    th.start()
for th in threads:
    th.join()
if failures:
    lib.fail(f"put many: {failures[:5]}")

want = [(k, len(v), '"' + lib.md5(v) + '"') for k, v in sorted(many.items())]

objects, prefixes, truncated, nxt = listing("many", "list-type=2")
if objects != want or prefixes or truncated or nxt:
    lib.fail(f"list many: {len(objects)} {objects[:3]} {prefixes} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "list-type=2&max-keys=305")
if len(objects) != 305 or truncated or nxt:
    lib.fail(f"list exactly all: {len(objects)} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "list-type=2&max-keys=304")
if len(objects) != 304 or not truncated or nxt != "k0298":
    lib.fail(f"list all but one: {len(objects)} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "list-type=2&max-keys=150&continuation-token=k0149")
if [o[0] for o in objects] != [f"k{i:04d}" for i in range(150, 300)] or truncated or nxt:
    lib.fail(f"list the second half: {len(objects)} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "list-type=2&prefix=k02&max-keys=50")
if [o[0] for o in objects] != [f"k{i:04d}" for i in range(200, 250)] or not truncated or nxt != "k0249":
    lib.fail(f"list a prefix page: {len(objects)} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "list-type=2&prefix=k02&max-keys=50&continuation-token=k0249")
if [o[0] for o in objects] != [f"k{i:04d}" for i in range(250, 300)] or truncated or nxt:
    lib.fail(f"list the last prefix page: {len(objects)} {truncated} {nxt}")

objects, prefixes, truncated, nxt = listing("many", "list-type=2&delimiter=/&max-keys=1")
if objects or prefixes != ["d/"] or not truncated or nxt != "d/":
    lib.fail(f"list one folder: {objects} {prefixes} {truncated} {nxt}")

objects, prefixes, truncated, nxt = listing("many", "list-type=2&delimiter=/&max-keys=301")
if len(objects) != 300 or prefixes != ["d/"] or truncated or nxt:
    lib.fail(f"list folder and all keys: {len(objects)} {prefixes} {truncated} {nxt}")

objects, prefixes, truncated, nxt = listing("many", "list-type=2&delimiter=/&max-keys=300")
if len(objects) != 299 or prefixes != ["d/"] or not truncated or nxt != "k0298":
    lib.fail(f"list folder and all keys but one: {len(objects)} {prefixes} {truncated} {nxt}")

objects, prefixes, truncated, nxt = listing("many", "delimiter=/&marker=k0297")
if [o[0] for o in objects] != ["k0298", "k0299"] or prefixes or truncated or nxt:
    lib.fail(f"list v1 tail: {objects} {prefixes} {truncated} {nxt}")

objects, _, truncated, nxt = listing("many", "prefix=d/&max-keys=5")
if [o[0] for o in objects] != [f"d/{i}" for i in range(5)] or truncated or nxt:
    lib.fail(f"list v1 folder: {objects} {truncated} {nxt}")

# three hundred keys in one delete request
status, _, out = multi_delete("many", sorted(k for k in many if k.startswith("k")))
deleted = [d.find(NS + "Key").text for d in ET.fromstring(out).iter(NS + "Deleted")]
if status != 200 or deleted != sorted(k for k in many if k.startswith("k")):
    lib.fail(f"delete three hundred: {status} {len(deleted)} {out[:200]}")

objects, _, truncated, _ = listing("many", "list-type=2")
if [o[0] for o in objects] != [f"d/{i}" for i in range(5)] or truncated:
    lib.fail(f"after the big delete: {objects} {truncated}")

status, _, out = multi_delete("many", [f"k{i:04d}" for i in range(1001)])
if status != 400 or b"MalformedXML" not in out:
    lib.fail(f"delete over the limit: {status} {out[:200]}")

status, _, out = multi_delete("many", [f"d/{i}" for i in range(5)] + ["nope"], quiet=True)
if status != 200 or list(ET.fromstring(out).iter(NS + "Deleted")):
    lib.fail(f"quiet delete of the rest: {status} {out[:200]}")

if keys_under("obj/many/"):
    lib.fail(f"keys left after deleting everything: {keys_under('obj/many/')}")

# a repair whose own cells are down when the key is queued
cluster.repair()
blob = os.urandom(12345)

cluster.host(1).stop()
status, _, _ = s3.request("PUT", "/photos/outage", blob)
if status != 200:
    lib.fail(f"put with a host down: {status}")

deadline = time.time() + 10
while time.time() < deadline and not etcd.has("repair/h1/photos/outage"):
    time.sleep(0.05)
m = etcd.manifest("photos", "outage")
if len(lib.pieces(m)) != 2 or not etcd.has("repair/h1/photos/outage"):
    lib.fail(f"degraded put: {m} queued={etcd.has('repair/h1/photos/outage')}")

cluster.host(1).start()

deadline = time.time() + 15
while time.time() < deadline and etcd.has("repair/h1/photos/outage"):
    time.sleep(0.05)

m3 = etcd.manifest("photos", "outage")
if etcd.has("repair/h1/photos/outage") or len(lib.pieces(m3)) != 3:
    lib.fail(f"repair after its cells came back: {m3} queued={etcd.has('repair/h1/photos/outage')}")

added = [p for p in lib.pieces(m3) if p not in lib.pieces(m)]
if len(added) != 1 or cluster.host_of(added[0]["cell"]) != 1:
    lib.fail(f"the third piece did not land on the host that owed it: {added}")

status, _, body = s3.request("GET", "/photos/outage")
if status != 200 or body != blob:
    lib.fail(f"get after repair: {status} {len(body)}")

print("ok")
