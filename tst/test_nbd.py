"""A block device over a bucket, served as NBD on a unix socket: the
handshake answers every option a client may send, reads of what was never
written are zeros, writes, trims and zeroes land on any byte, every tick
commits what was written as new versions of the blocks and one map of the
change, a restart reads the disk back from the bucket, every few commits
the map is written whole and the older maps go, clean blocks leave memory
and come back from the bucket, writes wait while too much is unwritten,
and a front that does not answer fails a commit that the next tick
repeats and a read that has to fetch."""

import os
import socket
import struct
import time
import xml.etree.ElementTree as ET

import lib

MB = 1 << 20
BLOCK = 4 * MB
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

CMD_READ, CMD_WRITE, CMD_DISC, CMD_FLUSH, CMD_TRIM, CMD_CACHE, CMD_ZERO = 0, 1, 2, 3, 4, 5, 6
OPT_EXPORT_NAME, OPT_ABORT, OPT_LIST, OPT_INFO, OPT_GO, OPT_STRUCTURED = 1, 2, 3, 6, 7, 8
REP_ACK, REP_SERVER, REP_INFO = 1, 2, 3
REP_ERR_UNSUP, REP_ERR_INVALID, REP_ERR_UNKNOWN = (1 << 31) + 1, (1 << 31) + 3, (1 << 31) + 6
EINVAL, EIO = 22, 5

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=400 * MB, buckets=["vm"]).start()
s3 = cluster.s3()


def wait(what, cond, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    lib.fail(f"timed out waiting for {what}")


def keys(prefix):
    status, _, body = s3.request("GET", f"/vm?list-type=2&prefix={prefix}")
    if status != 200:
        lib.fail(f"list {prefix}: {status}")
    return sorted(e.text for e in ET.fromstring(body).iter(NS + "Key"))


class Server:
    def __init__(self, disk, size="256M", tick="200ms", cache="8M", dirty="32M", base_every=3):
        self.disk = disk
        self.socket = os.path.join(lib.LAB, disk + ".sock")
        self.metrics = lib.free_port()
        self.args = ["nbd", "-debug", "-endpoint", f"http://127.0.0.1:{cluster.front_port}", "-bucket", "vm",
                     "-disk", disk, "-size", size, "-socket", self.socket, "-tick", tick, "-cache", cache,
                     "-dirty", dirty, "-base-every", str(base_every), "-metrics", f"127.0.0.1:{self.metrics}"]
        self.proc = None

    def start(self):
        if os.path.exists(self.socket):
            os.unlink(self.socket)
        self.proc = lib.Proc("nbd", self.args).start()
        wait(f"{self.disk} socket", lambda: os.path.exists(self.socket) or self.proc.proc.poll() is not None)
        if self.proc.proc.poll() is not None:
            lib.fail(f"nbd exited with {self.proc.proc.returncode}:\n{self.proc.text()[-2000:]}")
        return self

    def stop(self):
        code = self.proc.stop(timeout=60)
        if code != 0:
            lib.fail(f"nbd stopped with {code}:\n{self.proc.text()[-2000:]}")

    def gauge(self, name):
        return lib.metric(lib.scrape(self.metrics), name)


class Nbd:
    def __init__(self, path, export=None, no_zeroes=True, fixed=True):
        self.s = socket.socket(socket.AF_UNIX)
        self.s.settimeout(60)
        self.s.connect(path)
        self.cookie = 0
        hello = self.recv(18)
        magic, opts, flags = struct.unpack(">QQH", hello)
        if magic != 0x4e42444d41474943 or opts != 0x49484156454f5054 or flags != 3:
            lib.fail(f"hello: {hello.hex()}")
        self.s.sendall(struct.pack(">I", (1 if fixed else 0) | (2 if no_zeroes else 0)))
        if export is not None:
            self.go(export)

    def recv(self, n):
        out = b""
        while len(out) < n:
            chunk = self.s.recv(n - len(out))
            if not chunk:
                raise EOFError(f"closed after {len(out)} of {n} bytes")
            out += chunk
        return out

    def closed(self):
        try:
            return self.s.recv(1) == b""
        except OSError:
            return True

    def option(self, opt, data=b"", magic=0x49484156454f5054):
        self.s.sendall(struct.pack(">QII", magic, opt, len(data)) + data)

    def reply(self):
        magic, opt, kind, n = struct.unpack(">QIII", self.recv(20))
        if magic != 0x3e889045565a9:
            lib.fail(f"option reply magic {magic:x}")
        return opt, kind, self.recv(n)

    def info(self, opt, export):
        name = export.encode()
        self.option(opt, struct.pack(">I", len(name)) + name + struct.pack(">HH", 1, 3))
        replies = []
        while True:
            replies.append(self.reply())
            if replies[-1][1] != REP_INFO:
                return replies

    def go(self, export):
        replies = self.info(OPT_GO, export)
        if [r[1] for r in replies] != [REP_INFO, REP_ACK]:
            lib.fail(f"go: {replies}")
        kind, self.size, self.flags = struct.unpack(">HQH", replies[0][2])
        if kind != 0 or self.flags != 1 | 4 | 8 | 32 | 64:
            lib.fail(f"go info: {replies[0][2].hex()}")

    def send(self, cmd, offset=0, length=0, data=b"", flags=0, magic=0x25609513):
        self.cookie += 1
        self.s.sendall(struct.pack(">IHHQQI", magic, flags, cmd, self.cookie, offset, length) + data)
        return self.cookie

    def answer(self, length=0):
        magic, err, cookie = struct.unpack(">IIQ", self.recv(16))
        if magic != 0x67446698:
            lib.fail(f"reply magic {magic:x}")
        return err, cookie, (self.recv(length) if err == 0 else b"")

    def call(self, cmd, offset=0, length=0, data=b"", flags=0):
        cookie = self.send(cmd, offset, length, data, flags)
        err, got, body = self.answer(length if cmd == CMD_READ else 0)
        if got != cookie:
            lib.fail(f"cookie {got} for {cookie}")
        return err, body

    def read(self, offset, n):
        err, body = self.call(CMD_READ, offset, n)
        if err:
            lib.fail(f"read {offset}+{n}: error {err}")
        return body

    def write(self, offset, data, flags=0):
        err, _ = self.call(CMD_WRITE, offset, len(data), data, flags)
        if err:
            lib.fail(f"write {offset}+{len(data)}: error {err}")

    def ok(self, cmd, offset=0, n=0):
        err, _ = self.call(cmd, offset, n)
        if err:
            lib.fail(f"cmd {cmd} {offset}+{n}: error {err}")

    def close(self):
        self.send(CMD_DISC)
        self.s.close()


def expect(what, got, want):
    if got != want:
        lib.fail(f"{what}: got {len(got)} bytes {got[:16].hex()}..., want {len(want)} bytes {want[:16].hex()}...")


server = Server("d1").start()

# the handshake
c = Nbd(server.socket, fixed=False)
if not c.closed():
    lib.fail("a client without fixed newstyle was served")

c = Nbd(server.socket)
c.option(OPT_LIST)
if [c.reply(), c.reply()] != [(OPT_LIST, REP_SERVER, struct.pack(">I", 2) + b"d1"), (OPT_LIST, REP_ACK, b"")]:
    lib.fail("list")
c.option(OPT_STRUCTURED)
if c.reply() != (OPT_STRUCTURED, REP_ERR_UNSUP, b""):
    lib.fail("structured replies were not refused")
if [r[1] for r in c.info(OPT_INFO, "nope")] != [REP_ERR_UNKNOWN]:
    lib.fail("info of an unknown export")
c.option(OPT_INFO, b"\0\0\0\x09d1")
if c.reply()[1] != REP_ERR_INVALID:
    lib.fail("a malformed info was not refused")
if [r[1] for r in c.info(OPT_INFO, "")] != [REP_INFO, REP_ACK]:
    lib.fail("info of the default export")
c.option(OPT_ABORT)
if c.reply() != (OPT_ABORT, REP_ACK, b"") or not c.closed():
    lib.fail("abort")

c = Nbd(server.socket)
c.option(OPT_EXPORT_NAME, b"nope")
if not c.closed():
    lib.fail("an unknown export name was served")

for no_zeroes, n in ((True, 10), (False, 134)):
    c = Nbd(server.socket, no_zeroes=no_zeroes)
    c.option(OPT_EXPORT_NAME, b"d1")
    size, flags = struct.unpack(">QH", c.recv(10))
    if size != 256 * MB or flags != 1 | 4 | 8 | 32 | 64 or c.recv(n - 10) != b"\0" * (n - 10):
        lib.fail(f"export name: {size} {flags}")
    c.write(40 * BLOCK, b"x")
    c.close()

c = Nbd(server.socket)
c.option(OPT_GO, magic=1)
if not c.closed():
    lib.fail("a bad option magic was served")

c = Nbd(server.socket)
c.option(OPT_INFO, b"\0" * 5000)
if not c.closed():
    lib.fail("an option of 5000 bytes was served")

# requests
n = Nbd(server.socket, "d1")
expect("never written", n.read(0, 4096), b"\0" * 4096)
n.write(0, b"\0")

if n.call(CMD_READ, 256 * MB - 10, 20)[0] != EINVAL or n.call(CMD_CACHE, 0, 10)[0] != EINVAL:
    lib.fail("a read past the end or an unknown command was not refused")

span = os.urandom(BLOCK + 3 * 4096)
n.write(BLOCK - 4096 - 7, span, flags=1)
n.ok(CMD_FLUSH)
expect("across blocks", n.read(BLOCK - 4096 - 7, len(span)), span)
n.ok(CMD_READ, 0, 0)

whole = os.urandom(BLOCK)
n.write(3 * BLOCK, whole)
n.ok(CMD_ZERO, 3 * BLOCK + 100, 200)
expect("zeroed range", n.read(3 * BLOCK, 400), whole[:100] + b"\0" * 200 + whole[300:400])
n.ok(CMD_TRIM, 3 * BLOCK + 1000, 1000)
expect("partial trim keeps data", n.read(3 * BLOCK + 1000, 1000), whole[1000:2000])
n.ok(CMD_ZERO, 5 * BLOCK + 10, 10)
n.ok(CMD_TRIM, 6 * BLOCK, BLOCK)
expect("zeroes in what was never written", n.read(5 * BLOCK, 64), b"\0" * 64)

# pipelined, answered by cookie
cookies = [n.send(CMD_WRITE, 8 * BLOCK + i * 4096, 4096, bytes([i]) * 4096) for i in range(1, 9)]
answers = sorted(n.answer()[1] for _ in cookies)
if answers != cookies:
    lib.fail(f"pipelined answers {answers} for {cookies}")
expect("pipelined", n.read(8 * BLOCK + 4096, 8 * 4096), b"".join(bytes([i]) * 4096 for i in range(1, 9)))

# committed by the tick: blocks under a generation, the change as a map
wait("a commit", lambda: keys("d1/log/") or keys("d1/base/"))
wait("nothing dirty", lambda: server.gauge("s3_nbd_dirty_blocks") == 0)
stored = keys("d1/b/")
if not any(k.startswith("d1/b/00000000.") for k in stored) or not any(k.startswith("d1/b/00000003.") for k in stored):
    lib.fail(f"blocks in the bucket: {stored}")
if any(k.startswith("d1/b/00000006.") for k in stored):
    lib.fail("a trimmed block that was never written was stored")

n.ok(CMD_ZERO, 3 * BLOCK, BLOCK)
wait("the zeroed block gone", lambda: not any(k.startswith("d1/b/00000003.") for k in keys("d1/b/")))

# a second connection sees the first one's writes
m = Nbd(server.socket, "")
expect("second connection", m.read(BLOCK - 4096 - 7, len(span)), span)
m.close()

# a few commits more: the whole map, and the maps before it gone
for i in range(4):
    n.write(10 * BLOCK + i * 4096, os.urandom(4096))
    gen = server.gauge("s3_nbd_generation")
    wait("one more commit", lambda: server.gauge("s3_nbd_generation") > gen)
wait("a single base", lambda: len(keys("d1/base/")) == 1)
wait("old maps gone", lambda: all(int(k.rsplit("/", 1)[1], 16) > int(keys("d1/base/")[-1].rsplit("/", 1)[1], 16) for k in keys("d1/log/")))

# clean blocks leave memory and come back from the bucket
blocks = {i: os.urandom(BLOCK) for i in range(12, 16)}
for i, data in blocks.items():
    n.write(i * BLOCK, data)
wait("nothing dirty", lambda: server.gauge("s3_nbd_dirty_blocks") == 0)
if server.gauge("s3_nbd_resident_blocks") > 2:
    lib.fail(f"resident {server.gauge('s3_nbd_resident_blocks')} above the cache of two blocks")
for i, data in blocks.items():
    expect(f"block {i} back from the bucket", n.read(i * BLOCK, BLOCK), data)
n.write(12 * BLOCK + 5, b"abc")
expect("a write into a fetched block", n.read(12 * BLOCK, 16), blocks[12][:5] + b"abc" + blocks[12][8:16])

last = os.urandom(4096)
n.write(20 * BLOCK, last)
n.close()

# a restart commits what is left and reads the disk back
server.stop()
server = Server("d1").start()
n = Nbd(server.socket, "d1")
expect("after restart, across blocks", n.read(BLOCK - 4096 - 7, len(span)), span)
expect("after restart, the last write", n.read(20 * BLOCK, 4096), last)
expect("after restart, zeroed", n.read(3 * BLOCK, 64), b"\0" * 64)
n.write(13 * BLOCK + 1, b"q")
expect("a partial write into a block read from the bucket", n.read(13 * BLOCK, 4), blocks[13][:1] + b"q" + blocks[13][2:4])

# a front that does not answer: the commit fails and the next tick repeats it, a fetch fails
before = os.urandom(4096)
cluster.front.stop()
n.write(30 * BLOCK, before)
wait("a failed commit", lambda: 'level=WARN msg="nbd: commit"' in server.proc.text())
if n.call(CMD_READ, 14 * BLOCK, 10)[0] != EIO:
    lib.fail("a fetch from a front that is down did not fail")
cluster.front.start()
wait("the commit repeated", lambda: server.gauge("s3_nbd_dirty_blocks") == 0)
expect("after the front came back", n.read(14 * BLOCK, BLOCK), blocks[14])
n.close()
server.stop()

server = Server("d1").start()
n = Nbd(server.socket, "d1")
expect("written while the front was down", n.read(30 * BLOCK, 4096), before)

# two reads of a block on its way from the bucket wait for the same fetch
first, second = n.send(CMD_READ, 15 * BLOCK, 4096), n.send(CMD_READ, 15 * BLOCK + 4096, 4096)
got = dict((cookie, body) for _, cookie, body in (n.answer(4096), n.answer(4096)))
if got != {first: blocks[15][:4096], second: blocks[15][4096:8192]}:
    lib.fail("two reads of one block in flight")

# a block gone from the bucket, or of the wrong size, is an error to the reader
stored = keys("d1/b/")
gone = max(k for k in stored if k.startswith("d1/b/0000000c."))
short = max(k for k in stored if k.startswith("d1/b/0000000e."))
s3.request("DELETE", "/vm/" + gone)
s3.request("PUT", "/vm/" + short, b"short")
n.close()
server.stop()
server = Server("d1").start()
n = Nbd(server.socket, "d1")
if n.call(CMD_READ, 12 * BLOCK, 10)[0] != EIO or n.call(CMD_READ, 14 * BLOCK, 10)[0] != EIO:
    lib.fail("a block gone or short in the bucket was read without an error")
n.write(14 * BLOCK, blocks[14])

# a write while its block is on its way to the bucket goes to a copy
cluster.front.proc.proc.send_signal(lib.signal.SIGSTOP)
n.write(50 * BLOCK, b"a" * 4096)
time.sleep(1)
n.write(50 * BLOCK, b"b" * 4096)
expect("the copy", n.read(50 * BLOCK, 4096), b"b" * 4096)
cluster.front.proc.proc.send_signal(lib.signal.SIGCONT)
wait("nothing dirty", lambda: server.gauge("s3_nbd_dirty_blocks") == 0)

# a zeroed block in the commit that writes the whole map
for i in range(3):
    n.ok(CMD_ZERO, 40 * BLOCK, BLOCK)
    n.write(41 * BLOCK + i, b"z")
    gen = server.gauge("s3_nbd_generation")
    wait("one more commit", lambda: server.gauge("s3_nbd_generation") > gen)
n.close()
server.stop()
server = Server("d1").start()
n = Nbd(server.socket, "d1")
expect("zeroed in the base", n.read(40 * BLOCK, 4096), b"\0" * 4096)
expect("written in the base", n.read(41 * BLOCK, 4), b"zzz\0")

# a client that leaves without a word, or with answers still on their way
gone = Nbd(server.socket, "d1")
gone.s.close()
gone = Nbd(server.socket, "d1")
for i in range(4):
    gone.send(CMD_READ, i * BLOCK, BLOCK)
gone.s.close()
expect("served after clients left", n.read(BLOCK - 4096 - 7, len(span)), span)

# a request bigger than the server takes, or with a bad magic, ends the connection
n.send(CMD_WRITE, 0, 33 * MB)
if not n.closed():
    lib.fail("a write of 33 MiB was served")
n = Nbd(server.socket, "d1")
n.send(CMD_READ, 0, 10, magic=7)
if not n.closed():
    lib.fail("a request with a bad magic was served")
server.stop()

# writes wait while too much is unwritten
slow = Server("d2", tick="2s", dirty="8M").start()
n = Nbd(slow.socket, "d2")
start = time.time()
for i in range(3):
    n.write(i * BLOCK, os.urandom(BLOCK))
if time.time() - start < 1:
    lib.fail(f"three blocks over a limit of two took {time.time() - start:.2f}s, without waiting for a commit")
n.close()
slow.stop()

# the size is the disk's
bad = lib.Proc("nbd", ["nbd", "-endpoint", f"http://127.0.0.1:{cluster.front_port}", "-bucket", "vm", "-disk", "d1",
                       "-size", "128M", "-socket", os.path.join(lib.LAB, "bad.sock")]).start()
if bad.wait_exit() == 0 or "not 134217728" not in bad.text():
    lib.fail(f"a disk opened with another size:\n{bad.text()[-1000:]}")
bad = lib.Proc("nbd", ["nbd", "-endpoint", f"http://127.0.0.1:{cluster.front_port}", "-bucket", "vm", "-disk", "d3",
                       "-size", "5M", "-socket", os.path.join(lib.LAB, "bad.sock")]).start()
if bad.wait_exit() == 0 or "multiple" not in bad.text():
    lib.fail(f"a size off the block:\n{bad.text()[-1000:]}")

print("ok")
