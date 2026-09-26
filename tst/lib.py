"""Test lab: the s3 binary under test, run as local processes."""

import atexit
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
import uuid

BINARY = os.environ.get("S3_TEST_BINARY") or os.path.join(os.path.dirname(__file__), "..", "s3")
SCENARIO = os.path.splitext(os.path.basename(sys.argv[0]))[0]
LAB = tempfile.mkdtemp(prefix=f"s3-{SCENARIO}-")

OP_APPEND, OP_READ, OP_STATUS, OP_CANCEL = 1, 2, 3, 4
OP_REPLY, OP_FAIL = 0x80, 0xff
CODE_FULL, CODE_RANGE, CODE_IO, CODE_CANCELLED = 1, 2, 3, 4

PROCS = []


def environment(overrides):
    """The process environment with the overrides applied; a value of None
    removes the variable, which is how a scenario disarms the chaos of one
    process while the rest of the lab keeps it."""

    env = dict(os.environ)

    for key, value in (overrides or {}).items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value

    return env


def run(*args, check=False, env=None):
    return subprocess.run([BINARY, *args], capture_output=True, text=True, check=check, env=environment(env))


def fail(msg):
    print(msg, file=sys.stderr)
    keep_logs()
    sys.exit(1)


def keep_logs():
    """Every process log goes to S3_TEST_ARTIFACTS when it is set, so a failure in CI can be read."""

    root = os.environ.get("S3_TEST_ARTIFACTS")

    if not root:
        return

    into = os.path.join(root, SCENARIO)
    os.makedirs(into, exist_ok=True)

    for name in os.listdir(LAB):
        if name.endswith(".log"):
            shutil.copy(os.path.join(LAB, name), into)


def stop_all():
    for p in reversed(PROCS):
        try:
            p.stop()
        except (OSError, subprocess.TimeoutExpired):
            p.proc.kill()


atexit.register(stop_all)


def md5(data):
    return hashlib.md5(data).hexdigest()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))

        return s.getsockname()[1]


def wait_port(port, timeout=10, proc=None):
    """Until something listens on the port; a process that died before it
    did is reported with its log, not with a timeout."""

    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)

        if proc is not None and proc.proc.poll() is not None:
            fail(f"{proc.name} exited with {proc.proc.returncode} before opening port {port}:\n{proc.text()[-2000:]}")

    fail(f"port {port} never opened" + ("" if proc is None else f":\n{proc.text()[-2000:]}"))


def ready_blocks(store):
    """Full blocks still on the SSD, waiting for the flusher: plain numbers."""

    return sorted(int(name) for name in os.listdir(store) if name.isdigit())


class Proc:
    """One process of the binary under test: its log in the lab, its own
    coverage counters when the binary is instrumented, and stopped with
    SIGTERM at exit, which the binary answers with os.Exit so that those
    counters reach the disk."""

    counts = {}

    def __init__(self, kind, args, env=None):
        Proc.counts[kind] = Proc.counts.get(kind, 0) + 1
        self.name = f"{kind}-{Proc.counts[kind]}"
        self.args = args
        self.env = env
        self.log = os.path.join(LAB, self.name + ".log")
        self.proc = None

    def start(self):
        env = environment(self.env)

        if env.get("GOCOVERDIR"):
            env["GOCOVERDIR"] = os.path.join(env["GOCOVERDIR"], self.name + "-" + uuid.uuid4().hex)
            os.makedirs(env["GOCOVERDIR"])

        with open(self.log, "ab") as log:
            self.proc = subprocess.Popen([BINARY, *self.args], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=log, env=env)

        PROCS.append(self)

        return self

    def stop(self, timeout=10):
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=timeout)

        return self.proc.returncode

    def wait_exit(self, timeout=10):
        return self.proc.wait(timeout=timeout)

    def text(self):
        with open(self.log, errors="replace") as f:
            return f.read()


class Cell:
    """One `s3 cell` process over a temp SSD dir and a sparse file as the HDD."""

    def __init__(self, hdd_bytes, root=None, listeners=1, env=None):
        self.root = root or tempfile.mkdtemp(prefix="s3cell-")
        self.load = os.path.join(self.root, "load")
        self.store = os.path.join(self.root, "store")
        self.hdd = os.path.join(self.root, "hdd")
        self.ports = [free_port() for _ in range(listeners)]
        self.port = self.ports[0]
        self.env = env
        self.proc = None

        if not os.path.exists(self.hdd):
            with open(self.hdd, "wb") as f:
                f.truncate(hdd_bytes)

    def start(self, wait=True):
        listen = [arg for port in self.ports for arg in ("-listen", f"127.0.0.1:{port}")]
        self.proc = Proc("cell", ["cell", "-debug", *listen, "-load", self.load, "-store", self.store, "-hdd", self.hdd], self.env).start()

        if wait:
            for port in self.ports:
                wait_port(port, proc=self.proc)

        return self

    def stop(self):
        return self.proc.stop()

    def wait_exit(self, timeout=10):
        return self.proc.wait_exit(timeout)

    def log(self):
        return self.proc.text()

    def client(self, listener=0):
        return Client(self.ports[listener])


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.next_id = 0

    def close(self):
        self.sock.close()

    def send(self, op, payload=b""):
        self.next_id += 1
        body = struct.pack(">BQ", op, self.next_id) + payload
        self.sock.sendall(struct.pack(">I", len(body)) + body)

        return self.next_id

    def recv(self):
        n, = struct.unpack(">I", self._recv(4))
        reply = self._recv(n)
        rop, rid = struct.unpack(">BQ", reply[:9])

        return rid, rop, reply[9:]

    def call(self, op, payload=b""):
        self.send(op, payload)
        rid, rop, rest = self.recv()
        reply = struct.pack(">BQ", rop, rid) + rest

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
        PROCS.append(self)
        wait_port(self.port)

        return self

    def stop(self, timeout=10):
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=timeout)

        return self.proc.returncode

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

    def put(self, key, value):
        self.call("put", {"key": base64.b64encode(key.encode()).decode(), "value": base64.b64encode(value).decode()})

    def delete(self, key):
        self.call("deleterange", {"key": base64.b64encode(key.encode()).decode()})

    def manifest(self, bucket, key):
        raw = self.get(f"obj/{bucket}/{key}")

        if raw is None:
            fail(f"no manifest for {bucket}/{key}")

        return json.loads(raw)


def pieces(m):
    """Every placed piece of a manifest, chunked or not."""

    if "chunks" in m:
        return [p for c in m["chunks"] for p in c["pieces"]]

    return m.get("pieces") or []


class Host:
    def __init__(self, index, cells, hdd_bytes, env=None):
        self.index = index
        self.cells = [Cell(hdd_bytes, env=env) for _ in range(cells)]

    def start(self):
        for c in self.cells:
            c.start()

        return self

    def stop(self):
        for c in self.cells:
            c.stop()


class Front:
    """One `s3 front` process, serving S3 on every port it is given."""

    def __init__(self, config, listeners=1, env=None):
        self.config = config
        self.ports = [free_port() for _ in range(listeners)]
        self.port = self.ports[0]
        self.env = env
        self.proc = None

    def start(self):
        listen = [arg for port in self.ports for arg in ("-listen", f"127.0.0.1:{port}")]
        self.proc = Proc("front", ["front", "-debug", "-c", self.config, *listen], self.env).start()

        for port in self.ports:
            wait_port(port, proc=self.proc)

        return self

    def stop(self):
        return self.proc.stop()

    def log(self):
        return self.proc.text()

    def s3(self, listener=0):
        return S3(self.ports[listener])


class Cluster:
    """Three hosts of cells, one front and, on request, a repair per host;
    the buckets are the config's. The chaos of the cells and of the front
    is theirs to set, so a scenario can refuse one side and not the other."""

    def __init__(self, etcd, hosts, cells, hdd_bytes, buckets, front_listeners=1, cell_env=None, front_env=None):
        self.etcd = etcd
        self.hosts = [Host(i, cells, hdd_bytes, cell_env) for i in range(hosts)]
        self.config = os.path.join(tempfile.mkdtemp(prefix="s3cfg-"), "config.json")

        spec = {"etcd": [etcd.endpoint()], "buckets": list(buckets), "cells": []}

        for h in self.hosts:
            for c in h.cells:
                spec["cells"].append({"id": len(spec["cells"]), "host": f"h{h.index}", "addr": f"127.0.0.1:{c.port}"})

        with open(self.config, "w") as f:
            json.dump(spec, f)

        self.front = Front(self.config, listeners=front_listeners, env=front_env)
        self.front_ports = self.front.ports
        self.front_port = self.front.port

    def start(self):
        for h in self.hosts:
            h.start()

        self.front.start()

        return self

    def repair(self, env=None):
        for h in self.hosts:
            Proc(f"repair-h{h.index}", ["repair", "-debug", "-c", self.config, "-host", f"h{h.index}"], env).start()

    def web(self, env=None):
        port = free_port()

        web = Proc("web", ["web", "-debug", "-c", self.config, "-listen", f"127.0.0.1:{port}"], env).start()

        wait_port(port, proc=web)

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

    def s3(self, listener=0):
        return self.front.s3(listener)


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
