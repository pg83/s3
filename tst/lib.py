"""Test lab: the s3 binary under test, run as local processes."""

import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

BINARY = os.environ.get("S3_TEST_BINARY") or os.path.join(os.path.dirname(__file__), "..", "s3")

OP_APPEND, OP_READ, OP_STATUS = 1, 2, 3
ST_OK, ST_FULL, ST_RANGE, ST_IO = 0, 1, 2, 3


def run(*args, check=False):
    return subprocess.run([BINARY, *args], capture_output=True, text=True, check=check)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


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


class Cell:
    """One `s3 cell` process over a temp SSD dir and a sparse file as the HDD."""

    def __init__(self, hdd_bytes, root=None):
        self.root = root or tempfile.mkdtemp(prefix="s3cell-")
        self.ssd = os.path.join(self.root, "ssd")
        self.hdd = os.path.join(self.root, "hdd")
        self.port = free_port()
        self.proc = None

        if not os.path.exists(self.hdd):
            with open(self.hdd, "wb") as f:
                f.truncate(hdd_bytes)

    def start(self, wait=True):
        self.proc = subprocess.Popen(
            [BINARY, "cell", "-listen", f"127.0.0.1:{self.port}", "-ssd", self.ssd, "-hdd", self.hdd],
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

    def close(self):
        self.sock.close()

    def call(self, op, payload=b""):
        self.sock.sendall(struct.pack(">BI", op, len(payload)) + payload)
        hdr = self._recv(5)
        status, n = struct.unpack(">BI", hdr)

        return status, self._recv(n)

    def _recv(self, n):
        buf = bytearray()

        while len(buf) < n:
            chunk = self.sock.recv(min(1 << 20, n - len(buf)))

            if not chunk:
                fail("cell closed the connection")

            buf += chunk

        return bytes(buf)

    def append(self, data):
        status, resp = self.call(OP_APPEND, data)

        if status == ST_FULL:
            return None

        if status != ST_OK:
            fail(f"append status {status}")

        return struct.unpack(">Q", resp)[0]

    def read(self, off, n):
        status, resp = self.call(OP_READ, struct.pack(">QI", off, n))

        if status == ST_RANGE:
            return None

        if status != ST_OK:
            fail(f"read status {status}")

        return resp

    def status(self):
        status, resp = self.call(OP_STATUS)

        if status != ST_OK:
            fail(f"status status {status}")

        return struct.unpack(">QQ", resp)
