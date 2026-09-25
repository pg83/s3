#!/usr/bin/env python3
"""Exercise a live s3 stand: write a pile of objects of mixed sizes, list
them, watch the cells flush their blocks to the HDD, read everything back
from the HDD and then from the load, read ranges, delete one by one and in
bulk, and report every phase with its timing. Exits non-zero on the first
byte that does not match.

    dev/stand.py --endpoint https://s3.lab.mesh --count 400 --hosts lab1,lab2,lab3

Through the proxy the numbers measure the path to the lab, not the stand;
for the stand itself copy the script to a host and point it at the local
front, http://127.0.0.1:8093.
"""

import argparse
import concurrent.futures
import hashlib
import http.client
import os
import random
import ssl
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
SIZES = [1, 64 << 10, 1 << 20, (3 << 20) + 17]
HOSTS = {"lab1": "192.168.100.16", "lab2": "192.168.100.17", "lab3": "192.168.100.18"}


class S3:
    def __init__(self, endpoint, insecure):
        u = urllib.parse.urlsplit(endpoint)
        self.host = u.hostname
        self.port = u.port or (443 if u.scheme == "https" else 80)
        self.tls = u.scheme == "https"
        self.ctx = ssl.create_default_context()

        if insecure:
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def request(self, method, path, body=None, headers=None):
        conn = (http.client.HTTPSConnection(self.host, self.port, context=self.ctx, timeout=120)
                if self.tls else http.client.HTTPConnection(self.host, self.port, timeout=120))
        conn.request(method, path, body, headers or {})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()

        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data


def md5(data):
    return hashlib.md5(data).hexdigest()


def quote(key):
    return urllib.parse.quote(key, safe="/")


class Phase:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()
        print(f"== {self.name}", flush=True)

        return self

    def __exit__(self, *exc):
        print(f"   {self.name}: {time.time() - self.start:.1f}s", flush=True)


def parallel(threads, fn, items):
    with concurrent.futures.ThreadPoolExecutor(threads) as pool:
        return list(pool.map(fn, items))


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


def listing(s3, bucket, prefix, delimiter=""):
    keys, prefixes, token = [], [], ""

    while True:
        q = f"list-type=2&prefix={urllib.parse.quote(prefix)}"

        if delimiter:
            q += f"&delimiter={urllib.parse.quote(delimiter)}"

        if token:
            q += f"&continuation-token={urllib.parse.quote(token)}"

        status, _, body = s3.request("GET", f"/{bucket}?{q}")

        if status != 200:
            fail(f"list {q}: {status} {body[:200]}")

        root = ET.fromstring(body)
        keys += [c.find(NS + "Key").text for c in root.iter(NS + "Contents")]
        prefixes += [c.find(NS + "Prefix").text for c in root.iter(NS + "CommonPrefixes")]

        if root.find(NS + "IsTruncated").text != "true":
            return keys, prefixes

        token = root.find(NS + "NextContinuationToken").text


def cells(hosts):
    """What every cell of every host holds: blocks being written, full blocks
    waiting for the flusher, copies in the load. Needs root ssh to the hosts."""

    script = (
        'for i in 0 1 2; do pid=$(pgrep -f "^s3 cell .*s3_cell_$i/" | head -1); '
        'd=/var/run/s3_cell_$i; '
        'printf "%s %s %s\\n" "$(nsenter -m -t $pid ls $d/store | grep -c \\.current\\$)" '
        '"$(nsenter -m -t $pid ls $d/store | grep -c ^[0-9]*\\$)" '
        '"$(nsenter -m -t $pid ls $d/load | grep -c ^[0-9]*\\$)"; done'
    )
    out = {}

    for host in hosts:
        r = subprocess.run(["ssh", "-o", "ConnectTimeout=10", f"root@{HOSTS[host]}", script], capture_output=True, text=True)
        rows = [line.split() for line in r.stdout.strip().splitlines() if line.strip()]

        if len(rows) != 3:
            fail(f"{host}: cannot inspect the cells: {r.stderr.strip()[-200:]}")

        for i, (current, waiting, load) in enumerate(rows):
            out[f"{host}/{i}"] = (int(current), int(waiting), int(load))

    return out


def show(state):
    for name, (current, waiting, load) in sorted(state.items()):
        print(f"   {name}: current={current} waiting={waiting} load={load}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="https://s3.lab.mesh")
    ap.add_argument("--bucket", default="")
    ap.add_argument("--count", type=int, default=400)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--hosts", default="", help="comma-separated lab hosts to inspect over ssh")
    ap.add_argument("--insecure", action="store_true")
    ap.add_argument("--keep", action="store_true", help="leave the objects and the bucket in place")
    args = ap.parse_args()

    s3 = S3(args.endpoint, args.insecure)
    hosts = [h for h in args.hosts.split(",") if h]
    run = time.strftime("%Y%m%d-%H%M%S")
    bucket = args.bucket or f"stand-{run}"
    prefix = f"run/{run}/"
    rnd = random.Random(run)
    objects = {}

    for i in range(args.count):
        size = SIZES[i % len(SIZES)]
        objects[f"{prefix}d{i % 7}/o{i:05d}"] = rnd.randbytes(size) if size > 1 else b"x"

    total = sum(len(v) for v in objects.values())
    print(f"stand {run}: {args.count} objects, {total / (1 << 20):.1f} MiB, bucket {bucket}")

    status, _, body = s3.request("PUT", f"/{bucket}")

    if status not in (200, 409):
        fail(f"create bucket: {status} {body[:200]}")

    if hosts:
        print("== cells before")
        show(cells(hosts))

    with Phase("put") as ph:
        def put(item):
            key, data = item
            status, headers, body = s3.request("PUT", f"/{bucket}/{quote(key)}", data, {"Content-Type": "application/octet-stream"})

            if status != 200 or headers.get("etag") != f'"{md5(data)}"':
                fail(f"put {key}: {status} {headers.get('etag')} {body[:200]}")

        parallel(args.threads, put, list(objects.items()))
        print(f"   {args.count / (time.time() - ph.start):.0f} objects/s, {total / (1 << 20) / (time.time() - ph.start):.1f} MiB/s")

    with Phase("list"):
        keys, _ = listing(s3, bucket, prefix)

        if sorted(keys) != sorted(objects):
            fail(f"listing has {len(keys)} keys, wanted {len(objects)}")

        _, dirs = listing(s3, bucket, prefix, "/")

        if sorted(dirs) != sorted({k.rsplit("/", 1)[0] + "/" for k in objects}):
            fail(f"delimited listing: {dirs}")

        status, headers, _ = s3.request("HEAD", f"/{bucket}/{quote(next(iter(objects)))}")

        if status != 200 or headers.get("content-length") != str(len(next(iter(objects.values())))):
            fail(f"head: {status} {headers.get('content-length')}")

    if hosts:
        with Phase("flush"):
            deadline = time.time() + 120

            while True:
                state = cells(hosts)

                if all(waiting == 0 for _, waiting, _ in state.values()) or time.time() > deadline:
                    break

                time.sleep(2)

            show(state)

            if any(waiting for _, waiting, _ in state.values()):
                fail("blocks still waiting for the flusher after two minutes")

    def get(item):
        key, data = item
        status, headers, body = s3.request("GET", f"/{bucket}/{quote(key)}")

        if status != 200 or body != data or headers.get("etag") != f'"{md5(data)}"':
            fail(f"get {key}: {status} {len(body)} of {len(data)}")

    for name in ("get from the hdd", "get from the load"):
        with Phase(name) as ph:
            parallel(args.threads, get, list(objects.items()))
            print(f"   {args.count / (time.time() - ph.start):.0f} objects/s, {total / (1 << 20) / (time.time() - ph.start):.1f} MiB/s")

        if hosts:
            show(cells(hosts))

    with Phase("ranges"):
        for key in rnd.sample([k for k, v in objects.items() if len(v) > 1000], 20):
            data = objects[key]
            a = rnd.randrange(len(data) - 100)
            b = rnd.randrange(a, len(data))
            status, headers, body = s3.request("GET", f"/{bucket}/{quote(key)}", headers={"Range": f"bytes={a}-{b}"})

            if status != 206 or body != data[a:b + 1] or headers.get("content-range") != f"bytes {a}-{b}/{len(data)}":
                fail(f"range {a}-{b} of {key}: {status} {headers.get('content-range')}")

    with Phase("overwrite"):
        key = next(iter(objects))
        fresh = rnd.randbytes(2 << 20)
        status, _, _ = s3.request("PUT", f"/{bucket}/{quote(key)}", fresh)
        status2, _, body = s3.request("GET", f"/{bucket}/{quote(key)}")

        if status != 200 or status2 != 200 or body != fresh:
            fail(f"overwrite {key}: {status} {status2}")

        objects[key] = fresh

    if args.keep:
        print("kept")

        return

    keys = sorted(objects)
    half = keys[: len(keys) // 2]

    with Phase("delete one by one"):
        def delete(key):
            status, _, _ = s3.request("DELETE", f"/{bucket}/{quote(key)}")

            if status != 204:
                fail(f"delete {key}: {status}")

        parallel(args.threads, delete, half)

        for key in (half[0], keys[-1]):
            status, _, _ = s3.request("GET", f"/{bucket}/{quote(key)}")
            want = 404 if key in half else 200

            if status != want:
                fail(f"after deletes get {key}: {status}, wanted {want}")

    with Phase("delete in bulk"):
        rest = keys[len(keys) // 2:]

        for i in range(0, len(rest), 1000):
            chunk = rest[i:i + 1000]
            xml = "<Delete>" + "".join(f"<Object><Key>{k}</Key></Object>" for k in chunk) + "</Delete>"
            status, _, body = s3.request("POST", f"/{bucket}?delete", xml.encode(), {"Content-Type": "application/xml"})
            deleted = [d.find(NS + "Key").text for d in ET.fromstring(body).iter(NS + "Deleted")]

            if status != 200 or sorted(deleted) != sorted(chunk):
                fail(f"bulk delete: {status} {len(deleted)} of {len(chunk)}")

        left, _ = listing(s3, bucket, prefix)

        if left:
            fail(f"{len(left)} keys left after the deletes")

    if not args.bucket:
        status, _, _ = s3.request("DELETE", f"/{bucket}")

        if status != 204:
            fail(f"delete bucket: {status}")

    if hosts:
        print("== cells after")
        show(cells(hosts))

    print("ok")


if __name__ == "__main__":
    main()
