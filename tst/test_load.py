"""A cache rename can run out of space after its write succeeded. Evict
the least recently used copy and retry, and leave no temporary copies
behind even if the cache cannot hold the read. The chaos binary refuses
renames; the ordinary binary checks the same reads without refusals."""

import os
import struct

import lib

BLOCK = 2 << 20
blocks = [os.urandom(BLOCK) for _ in range(6)]
tail = b"still in the store"


def prepared(rate):
    cell = lib.Cell(hdd_bytes=16 * BLOCK, env={"S3_CHAOS": f"load rename:{rate}", "S3_CHAOS_SEED": "2"})
    os.makedirs(cell.store)
    os.makedirs(cell.load)

    with open(cell.hdd, "r+b") as f:
        f.write(b"".join(blocks))

    with open(os.path.join(cell.store, "6.current"), "wb") as f:
        f.write(tail)

    for i in range(3):
        path = os.path.join(cell.load, str(i))

        with open(path, "wb") as f:
            f.write(blocks[i])

        os.utime(path, (100 + i, 100 + i))

    return cell.start()


def no_temporary(cell):
    names = os.listdir(cell.load)

    if any(name.endswith(".tmp") for name in names):
        lib.fail(f"cache left temporary copies: {names}")


cell = prepared(2)
c = cell.client()

if c.read(0, 16) != blocks[0][:16]:
    lib.fail("could not touch the oldest cached block")

for i in (3, 4):
    if c.read(i * BLOCK, BLOCK) != blocks[i]:
        lib.fail(f"block {i} was not served after a refused cache rename")

    no_temporary(cell)

armed = 'at="load rename"' in cell.log()

if armed:
    if os.path.exists(os.path.join(cell.load, "1")) or not os.path.exists(os.path.join(cell.load, "0")):
        lib.fail("a refused rename did not evict the least recently read copy")

    if lib.metric(cell.scrape(), "s3_cell_lru_evictions_total") != 2:
        lib.fail("two refused renames did not evict two copies")

if lib.metric(cell.scrape(), "s3_cell_errors_total", op="read") != 0:
    lib.fail("a recovered cache rename was counted as a failed read")

for i in (0, 1, 2, 3, 4, 5):
    if c.read(i * BLOCK + 17, 100) != blocks[i][17:117]:
        lib.fail(f"block {i} differs after cache evictions")

    no_temporary(cell)

c.close()
cell.stop()

cell = prepared(1)
c = cell.client()
reply = c.call(lib.OP_READ, struct.pack(">QI", 3 * BLOCK, 100))

if armed:
    if reply != (lib.OP_FAIL, lib.CODE_IO):
        lib.fail(f"a cache with every rename refused returned {reply}")

    if os.listdir(cell.load):
        lib.fail(f"failed cache rename leaked files: {os.listdir(cell.load)}")

    if lib.metric(cell.scrape(), "s3_cell_lru_evictions_total") != 3:
        lib.fail("a full cache did not exhaust its three old copies")
elif reply != (lib.OP_READ | lib.OP_REPLY, blocks[3][:100]):
    lib.fail("ordinary binary did not serve the HDD read")

no_temporary(cell)

if c.read(6 * BLOCK, len(tail)) != tail:
    lib.fail("a failed cache rename prevented reading the current store block")

c.close()
cell.stop()
print("ok")
