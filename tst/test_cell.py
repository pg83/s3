"""A cell hands out offsets only for bytes on disk, serves them back from
the SSD tail and from the HDD alike, survives a restart, and refuses to
serve once the disk is full."""

import json
import os
import time

import lib

MB = 1 << 20

cell = lib.Cell(hdd_bytes=32 * MB).start()
c = cell.client()

head, free = c.status()

if head != 0 or free != 32 * MB:
    lib.fail(f"fresh cell: head={head} free={free}")

if c.read(0, 1) is not None:
    lib.fail("read past the head of an empty cell must be refused")

small = b"hello, cell"
big = os.urandom(3 * MB)

o1 = c.append(small)
o2 = c.append(big)

if o1 != 0 or o2 != len(small):
    lib.fail(f"offsets {o1} {o2}")

if c.read(o1, len(small)) != small or c.read(o2, len(big)) != big:
    lib.fail("read back from the tail differs")

if c.read(o2 + len(big) - 1, 2) is not None:
    lib.fail("read across the head must be refused")

# the mover copies the tail to the HDD and records how far it got
deadline = time.time() + 10

while time.time() < deadline:
    try:
        flushed = json.load(open(os.path.join(cell.ssd, "state")))["flushed"]
    except (FileNotFoundError, ValueError):
        flushed = 0

    if flushed == o2 + len(big):
        break

    time.sleep(0.1)
else:
    lib.fail(f"mover never caught up: flushed={flushed}")

with open(cell.hdd, "rb") as f:
    f.seek(o2)

    if f.read(len(big)) != big:
        lib.fail("HDD does not hold the moved bytes")

if c.read(o1, len(small)) != small or c.read(o2, len(big)) != big:
    lib.fail("read back from the HDD differs")

o3 = c.append(b"after the move")

if o3 != o2 + len(big):
    lib.fail(f"offset after the move {o3}")

if c.read(o2 + len(big) - 4, 4 + len(b"after the move")) != big[-4:] + b"after the move":
    lib.fail("read spanning HDD and tail differs")

# many small appends from several connections share fsyncs and stay ordered
clients = [cell.client() for _ in range(4)]
offsets = []

for i in range(200):
    offsets.append((clients[i % 4].append(b"x" * (i + 1)), i + 1))

offsets.sort()
expect = o3 + len(b"after the move")

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
