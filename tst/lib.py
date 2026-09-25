"""Test lab: the s3 binary under test, run as local processes."""

import base64
import hashlib
import http.client
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

BINARY = os.environ.get("S3_TEST_BINARY") or os.path.join(os.path.dirname(__file__), "..", "s3")

OP_APPEND, OP_READ, OP_STATUS = 1, 2, 3
OP_REPLY, OP_FAIL = 0x80, 0xff
CODE_FULL, CODE_RANGE, CODE_IO = 1, 2, 3


def run(*args, check=False):
    return subprocess.run([BINARY, *args], capture_output=True, text=True, check=check)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def md5(data):
    return hashlib.md5(data).hexdigest()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))

        return s.getsockname()[1]


def wait_port(port, timeout=10):
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)

    fail(f"port {port} never opened")


def ready_blocks(store):
    """Full blocks still on the SSD, waiting for the flusher: plain numbers."""

    return sorted(int(name) for name in os.listdir(store) if name.isdigit())


class Cell:
    """One `s3 cell` process over a temp SSD dir and a sparse file as the HDD."""

    def __init__(self, hdd_bytes, root=None):
        self.root = root or tempfile.mkdtemp(prefix="s3cell-")
        self.load = os.path.join(self.root, "load")
        self.store = os.path.join(self.root, "store")
        self.hdd = os.path.join(self.root, "hdd")
        self.port = free_port()
        self.proc = None

        if not os.path.exists(self.hdd):
            with open(self.hdd, "wb") as f:
                f.truncate(hdd_bytes)

    def start(self, wait=True):
        self.proc = subprocess.Popen(
            [BINARY, "cell", "-listen", f"127.0.0.1:{self.port}", "-load", self.load, "-store", self.store, "-hdd", self.hdd],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

        if wait:
            wait_port(self.port)

        return self

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=10)

        return self.proc.returncode

    def wait_exit(self, timeout=10):
        return self.proc.wait(timeout=timeout)

    def client(self):
        return Client(self.port)


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.next_id = 0

    def close(self):
        self.sock.close()

    def call(self, op, payload=b""):
        self.next_id += 1
        body = struct.pack(">BQ", op, self.next_id) + payload
        self.sock.sendall(struct.pack(">I", len(body)) + body)
        n, = struct.unpack(">I", self._recv(4))
        reply = self._recv(n)
        rop, rid = struct.unpack(">BQ", reply[:9])

        if rid != self.next_id:
            fail(f"reply id {rid} for request {self.next_id}")

        if rop == OP_FAIL:
            return OP_FAIL, reply[9]

        if rop != op | OP_REPLY:
            fail(f"op {op} answered with op {rop}")

        return rop, reply[9:]

    def _recv(self, n):
        buf = bytearray()

        while len(buf) < n:
            chunk = self.sock.recv(min(1 << 20, n - len(buf)))

            if not chunk:
                fail("cell closed the connection")

            buf += chunk

        return bytes(buf)

    def append(self, data):
        op, resp = self.call(OP_APPEND, data)

        if op == OP_FAIL and resp == CODE_FULL:
            return None

        if op == OP_FAIL:
            fail(f"append failed with code {resp}")

        return struct.unpack(">Q", resp)[0]

    def read(self, off, n):
        op, resp = self.call(OP_READ, struct.pack(">QI", off, n))

        if op == OP_FAIL and resp == CODE_RANGE:
            return None

        if op == OP_FAIL:
            fail(f"read failed with code {resp}")

        return resp

    def status(self):
        op, resp = self.call(OP_STATUS)

        if op == OP_FAIL:
            fail(f"status failed with code {resp}")

        return struct.unpack(">QQ", resp)


class Etcd:
    """A single-member etcd for the metadata; None when no binary is around."""

    def __new__(cls):
        binary = os.environ.get("S3_TEST_ETCD") or shutil.which("etcd")

        if not binary:
            return None

        obj = super().__new__(cls)
        obj.binary = binary

        return obj

    def start(self):
        self.root = tempfile.mkdtemp(prefix="s3etcd-")
        self.port = free_port()
        peer = free_port()
        self.proc = subprocess.Popen(
            [
                self.binary, "--name", "t", "--data-dir", self.root,
                "--listen-client-urls", f"http://127.0.0.1:{self.port}",
                "--advertise-client-urls", f"http://127.0.0.1:{self.port}",
                "--listen-peer-urls", f"http://127.0.0.1:{peer}",
                "--initial-advertise-peer-urls", f"http://127.0.0.1:{peer}",
                "--initial-cluster", f"t=http://127.0.0.1:{peer}",
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        wait_port(self.port)

        return self

    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def call(self, path, req):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("POST", "/v3/kv/" + path, json.dumps(req), {"Content-Type": "application/json"})
        resp = conn.getresponse()
        body = resp.read()
        conn.close()

        if resp.status != 200:
            fail(f"etcd {path}: {resp.status} {body!r}")

        return json.loads(body)

    def get(self, key):
        out = self.call("range", {"key": base64.b64encode(key.encode()).decode()})

        for kv in out.get("kvs", []):
            return base64.b64decode(kv.get("value", ""))

        return None

    def has(self, key):
        return self.get(key) is not None

    def manifest(self, bucket, key):
        raw = self.get(f"obj/{bucket}/{key}")

        if raw is None:
            fail(f"no manifest for {bucket}/{key}")

        return json.loads(raw)


class Host:
    def __init__(self, index, cells, hdd_bytes):
        self.index = index
        self.cells = [Cell(hdd_bytes) for _ in range(cells)]

    def start(self):
        for c in self.cells:
            c.start()

        return self

    def stop(self):
        for c in self.cells:
            c.stop()


class Cluster:
    """Three hosts of cells, one front and, on request, a repair per host."""

    def __init__(self, etcd, hosts, cells, hdd_bytes):
        self.etcd = etcd
        self.hosts = [Host(i, cells, hdd_bytes) for i in range(hosts)]
        self.front_port = free_port()
        self.config = os.path.join(tempfile.mkdtemp(prefix="s3cfg-"), "config.json")
        self.procs = []

        spec = {"etcd": [etcd.endpoint()], "cells": []}

        for h in self.hosts:
            for c in h.cells:
                spec["cells"].append({"id": len(spec["cells"]), "host": f"h{h.index}", "addr": f"127.0.0.1:{c.port}"})

        with open(self.config, "w") as f:
            json.dump(spec, f)

    def start(self):
        for h in self.hosts:
            h.start()

        self.procs.append(subprocess.Popen(
            [BINARY, "front", "-c", self.config, "-listen", f"127.0.0.1:{self.front_port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        ))
        wait_port(self.front_port)

        return self

    def repair(self):
        for h in self.hosts:
            self.procs.append(subprocess.Popen(
                [BINARY, "repair", "-c", self.config, "-host", f"h{h.index}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
            ))

    def web(self):
        port = free_port()
        self.procs.append(subprocess.Popen(
            [BINARY, "web", "-c", self.config, "-listen", f"127.0.0.1:{port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        ))
        wait_port(port)

        return S3(port)

    def host(self, index):
        return self.hosts[index]

    def cell(self, cell_id):
        per_host = len(self.hosts[0].cells)

        return self.hosts[cell_id // per_host].cells[cell_id % per_host]

    def host_of(self, cell_id):
        return cell_id // len(self.hosts[0].cells)

    def corrupt(self, cell_id, offset, n):
        """Flip bytes of a piece on the HDD; the SSD copies are dropped so the
        read really sees the damage."""

        cell = self.cell(cell_id)
        block = offset // (2 << 20)
        current = os.path.join(cell.store, f"{block}.current")

        # the piece may still sit in the current block; push it out to the HDD
        if os.path.exists(current):
            c = cell.client()

            while os.path.exists(current):
                c.append(os.urandom(2 << 20))

            c.close()

        deadline = time.time() + 10

        while time.time() < deadline and ready_blocks(cell.store):
            time.sleep(0.1)

        if ready_blocks(cell.store):
            fail("the flusher never emptied the store")

        with open(cell.hdd, "r+b") as f:
            f.seek(offset)
            data = f.read(n)
            f.seek(offset)
            f.write(bytes(b ^ 0xff for b in data))

        self.clear_lru()

    def clear_lru(self):
        for h in self.hosts:
            for c in h.cells:
                for name in os.listdir(c.load):
                    os.remove(os.path.join(c.load, name))

    def s3(self):
        return S3(self.front_port)


class S3:
    def __init__(self, port):
        self.port = port

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        conn.request(method, path, body, headers or {})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()

        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data
