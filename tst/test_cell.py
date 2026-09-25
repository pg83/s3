"""A cell hands out offsets only for bytes fsynced on the SSD, moves full
2 MiB blocks to the HDD, serves bytes from the LRU, the ready blocks, the
current block and the HDD alike, survives a restart, and refuses to serve
once the disk is full."""

import os
import time

import lib

MB = 1 << 20
BLOCK = 2 * MB

cell = lib.Cell(hdd_bytes=32 * MB).start()
c = cell.client()

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

# many small appends from several connections share one tick and stay in order
clients = [cell.client() for _ in range(4)]
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

if rc == 0 or "full" not in full.proc.stderr.read():
    lib.fail(f"a full cell must exit without serving, rc={rc}")

print("ok")
