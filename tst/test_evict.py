"""The load is a cache the cell fills from the HDD and empties only when a
copy does not fit: on a 7 MiB tmpfs three 2 MiB copies stay and the least
recently read one goes when a fourth is needed. Needs an unprivileged
user namespace for the tmpfs; without one the scenario skips."""

import os
import subprocess
import sys
import tempfile
import time

import lib

MB = 1 << 20
BLOCK = 2 * MB

if os.environ.get("S3_TEST_EVICT_INSIDE") != "1":
    env = dict(os.environ, S3_TEST_EVICT_INSIDE="1")
    probe = tempfile.mkdtemp(prefix="s3evict-probe-")
    r = subprocess.run(["unshare", "-rm", "sh", "-c", f"mount -t tmpfs -o size=1m tmpfs {probe}"], capture_output=True)

    if r.returncode != 0:
        print(f"skip: no user namespace with tmpfs here: {r.stderr.decode().strip()}")
        raise SystemExit(0)

    raise SystemExit(subprocess.call(["unshare", "-rm", sys.executable, __file__], env=env))

root = tempfile.mkdtemp(prefix="s3evict-")
load = os.path.join(root, "load")
os.mkdir(load)
subprocess.run(["mount", "-t", "tmpfs", "-o", "size=7m", "tmpfs", load], check=True)

cell = lib.Cell(hdd_bytes=32 * MB, root=root).start()
c = cell.client()

blocks = [os.urandom(BLOCK) for _ in range(6)]
for data in blocks:
    if c.append(data) is None:
        lib.fail("append refused")

ready = os.path.join(cell.store, "ready")
deadline = time.time() + 10
while time.time() < deadline and os.listdir(ready):
    time.sleep(0.1)
if os.listdir(ready):
    lib.fail(f"ready still holds {os.listdir(ready)}")


def cached():
    return sorted(int(name) for name in os.listdir(load) if not name.endswith(".tmp"))


for i, data in enumerate(blocks):
    if c.read(i * BLOCK, BLOCK) != data:
        lib.fail(f"block {i} read back differs")

if cached() != [3, 4, 5]:
    lib.fail(f"after six reads the load holds {cached()}, wanted the last three")

if c.read(3 * BLOCK, 16) != blocks[3][:16]:
    lib.fail("block 3 read back differs")

if c.read(0, 16) != blocks[0][:16]:
    lib.fail("block 0 read back differs")

if cached() != [0, 3, 5]:
    lib.fail(f"after touching 3 and reading 0 the load holds {cached()}, wanted 4 evicted")

for i, data in enumerate(blocks):
    if c.read(i * BLOCK + 100, 1000) != data[100:1100]:
        lib.fail(f"block {i} slice differs after evictions")

print("ok")
