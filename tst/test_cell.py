"""A cell hands out offsets only for bytes fsynced on the SSD, moves full
2 MiB blocks to the HDD, serves bytes from the LRU, the ready blocks, the
current block and the HDD alike, survives a restart, refuses to serve
once the disk is full, and is the same cell on every address it
listens on."""

import os
import socket
import struct
import time

import lib

MB = 1 << 20
BLOCK = 2 * MB

cell = lib.Cell(hdd_bytes=32 * MB, listeners=2).start()
c = cell.client()
other = cell.client(1)

head, free = c.status()

if head != 0 or free != 32 * MB:
    lib.fail(f"fresh cell: head={head} free={free}")

if c.read(0, 1) is not None:
    lib.fail("read of an empty cell must be refused")

small = b"hello, cell"
big = os.urandom(3 * MB)

o1 = c.append(small)
o2 = c.append(big)

if o1 != 0 or o2 != len(small):
    lib.fail(f"offsets {o1} {o2}")

if c.read(o1, len(small)) != small or c.read(o2, len(big)) != big:
    lib.fail("read back differs")

if other.read(o1, len(small)) != small or other.status() != c.status():
    lib.fail("the second address is not the same cell")

other.close()

if c.read(o2 + len(big) - 1, 2) is not None:
    lib.fail("read across the head must be refused")

# block 0 is full and goes to the HDD; block 1 is current
deadline = time.time() + 10

while time.time() < deadline and (lib.ready_blocks(cell.store) or not os.path.exists(os.path.join(cell.store, "1.current"))):
    time.sleep(0.1)

if lib.ready_blocks(cell.store):
    lib.fail(f"the store still holds full blocks {lib.ready_blocks(cell.store)}")

log = small + big

with open(cell.hdd, "rb") as f:
    if f.read(BLOCK) != log[:BLOCK]:
        lib.fail("HDD does not hold block 0")

for name in os.listdir(cell.load):
    os.remove(os.path.join(cell.load, name))

if c.read(o1, len(small)) != small or c.read(o2, len(big)) != big:
    lib.fail("read back from the HDD differs")

if os.listdir(cell.load) != ["0"]:
    lib.fail(f"load after an HDD read: {os.listdir(cell.load)}")

o3 = c.append(b"after the block")

if o3 != len(log):
    lib.fail(f"offset after the block {o3}")

log += b"after the block"

if c.read(BLOCK - 4, 8) != log[BLOCK - 4:BLOCK + 4]:
    lib.fail("read across the block boundary differs")

if c.read(o3 - 4, 4 + len(b"after the block")) != log[o3 - 4:]:
    lib.fail("read spanning the HDD and the current block differs")

# many small appends from several connections, over both addresses, share one tick and stay in order
clients = [cell.client(i % 2) for i in range(4)]
offsets = []

for i in range(200):
    offsets.append((clients[i % 4].append(b"x" * (i + 1)), i + 1))

offsets.sort()
expect = len(log)

for off, n in offsets:
    if off != expect:
        lib.fail(f"appends are not contiguous at {off}, expected {expect}")

    expect += n

for cl in clients:
    cl.close()

c.close()

# a cancel that reaches the writer before the tick drops the append
c = cell.client()
head, _ = c.status()
dropped = False

for attempt in range(20):
    i1 = c.send(lib.OP_APPEND, b"never mind")
    c.send(lib.OP_CANCEL, struct.pack(">Q", i1))
    rid, rop, body = c.recv()

    if rid != i1:
        lib.fail(f"reply {rid} for append {i1}")

    if rop == lib.OP_FAIL and body == bytes([lib.CODE_CANCELLED]):
        dropped = True
        break

    if rop != lib.OP_APPEND | lib.OP_REPLY:
        lib.fail(f"cancelled append answered op {rop} {body!r}")

    head += len(b"never mind")

if not dropped:
    lib.fail("twenty append+cancel pairs, none dropped")

if c.status()[0] != head:
    lib.fail(f"head {c.status()[0]} after cancels, expected {head}")

expect = head

# a restart keeps everything that was acknowledged
if cell.stop() != 0:
    lib.fail("cell did not exit cleanly on SIGTERM")

cell.start()

if os.listdir(cell.load) != ["0"]:
    lib.fail(f"restart dropped the load copies: {os.listdir(cell.load)}")
c = cell.client()
head, free = c.status()

if head != expect:
    lib.fail(f"head after restart {head}, expected {expect}")

if c.read(o1, len(small)) != small or c.read(o2, len(big)) != big:
    lib.fail("data lost across restart")

# fill it up: append is refused with FULL, and the next start refuses to serve
while c.append(os.urandom(MB)) is not None:
    pass

head, free = c.status()

if free >= MB:
    lib.fail(f"cell still has {free} bytes free after refusing an append of {MB}")

c.close()
cell.stop()

full = lib.Cell(hdd_bytes=0, root=cell.root)
full.start(wait=False)
rc = full.wait_exit()

if rc == 0 or "full" not in full.log():
    lib.fail(f"a full cell must exit without serving, rc={rc}")

# frames the cell refuses: a length of nothing, a body without an id, a read without its arguments, an op nobody knows
cell = lib.Cell(hdd_bytes=8 * MB).start()

for bad in (struct.pack(">I", 0), struct.pack(">IB", 1, lib.OP_STATUS)):
    s = socket.create_connection(("127.0.0.1", cell.port))
    s.sendall(bad)

    if s.recv(1) != b"":
        lib.fail(f"the cell kept a connection after {bad!r}")

    s.close()

c = cell.client()

if c.call(lib.OP_READ, b"\x00") != (lib.OP_FAIL, lib.CODE_IO):
    lib.fail("a read without its arguments was not refused")

if c.call(9) != (lib.OP_FAIL, lib.CODE_IO):
    lib.fail("an unknown op was not refused")

if c.read(8 * MB, 1) is not None:
    lib.fail("a read past the capacity was not refused")

# a cancel that names nothing is dropped and the connection goes on
c.send(lib.OP_CANCEL, b"")

if c.status()[0] != 0:
    lib.fail("a cancel that names nothing changed the head")

c.close()
cell.stop()

# a store found at start: two full blocks the flusher did not get to, the current block, a stray file, a copy half made
prepared = lib.Cell(hdd_bytes=16 * MB)
block0 = os.urandom(BLOCK)
block1 = os.urandom(BLOCK)
tail = os.urandom(1000)
os.makedirs(prepared.store)
os.makedirs(prepared.load)

for name, content in (("0", block0), ("1", block1), ("2.current", tail), ("junk", b"?")):
    with open(os.path.join(prepared.store, name), "wb") as f:
        f.write(content)

with open(os.path.join(prepared.load, "7.tmp"), "wb") as f:
    f.write(b"?")

prepared.start()
c = prepared.client()

if c.status()[0] != 2 * BLOCK + len(tail):
    lib.fail(f"head {c.status()[0]} over a prepared store, expected {2 * BLOCK + len(tail)}")

if c.read(BLOCK - 10, 20) != block0[-10:] + block1[:10] or c.read(2 * BLOCK - 10, 20) != block1[-10:] + tail[:10]:
    lib.fail("read across the prepared blocks differs")

deadline = time.time() + 10

while time.time() < deadline and lib.ready_blocks(prepared.store):
    time.sleep(0.1)

if lib.ready_blocks(prepared.store):
    lib.fail("the full block found at start was never flushed")

if os.path.exists(os.path.join(prepared.load, "7.tmp")):
    lib.fail("the half made copy survived the start")

with open(prepared.hdd, "rb") as f:
    if f.read(2 * BLOCK) != block0 + block1:
        lib.fail("the HDD does not hold the blocks found at start")

c.close()
prepared.stop()

# a current block that is exactly full at start is rolled before anything else
rolled = lib.Cell(hdd_bytes=16 * MB)
os.makedirs(rolled.store)

with open(os.path.join(rolled.store, "0.current"), "wb") as f:
    f.write(block0)

rolled.start()
c = rolled.client()

if c.status()[0] != BLOCK or c.append(b"next") != BLOCK:
    lib.fail("a full current block was not rolled at start")

if c.read(BLOCK - 4, 8) != block0[-4:] + b"next":
    lib.fail("read across the rolled block differs")

c.close()
rolled.stop()

# a current block longer than a block is not a cell
overlong = lib.Cell(hdd_bytes=16 * MB)
os.makedirs(overlong.store)

with open(os.path.join(overlong.store, "0.current"), "wb") as f:
    f.write(os.urandom(BLOCK + 1))

overlong.start(wait=False)

if overlong.wait_exit() == 0 or "longer than a block" not in overlong.log():
    lib.fail("a cell served over a current block longer than a block")

print("ok")
