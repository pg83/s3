"""The front, the repairs and the cells are refused by the operating system
at every turn they are meant to survive: an accept refused, a dial that
fails, a read or a write that breaks a link or a cell's connection. Every
object still goes in and comes back whole, and every piece a refused host
owes is placed by its repair. Against the ordinary binary nothing is armed
and this is a plain put and get."""

import os
import time

import lib

MB = 1 << 20

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

# Every point the process has no right to die on. A rate is one call in so
# many: a link breaks about every twentieth frame, and a third of the dials
# that follow are refused as well, which is what makes a put land on two
# hosts and owe the third.
LINKS = "all:5000,-cell read,-cell write,accept:3,dial:3,link read:20,link write:20"
CELLS = "accept:3,cell read:20,cell write:20"

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=2, hdd_bytes=48 * MB, buckets=["chaos"],
                      cell_env={"S3_CHAOS": CELLS, "S3_CHAOS_SEED": "11"},
                      front_env={"S3_CHAOS": LINKS, "S3_CHAOS_SEED": "7"}).start()
cluster.repair(env={"S3_CHAOS": LINKS, "S3_CHAOS_SEED": "5"})
s3 = cluster.s3()

sizes = [1, 777, 4096, 64 << 10, 300 << 10, MB + 1, (2 << 20) + 17]
blobs = {f"o{i:02d}": os.urandom(sizes[i % len(sizes)]) for i in range(21)}


def insist(what, attempt, tries=20):
    """A request that two links down at once turn away is the client's to
    repeat, so it is repeated; anything but that is a failure."""

    for _ in range(tries):
        status, headers, body = attempt()

        if status == 200:
            return headers, body

        if status not in (500, 503):
            lib.fail(f"{what}: {status} {body[:200]}")

        time.sleep(0.2)

    lib.fail(f"{what}: turned away {tries} times")


for key, data in blobs.items():
    headers, _ = insist(f"put {key}", lambda: s3.request("PUT", "/chaos/" + key, data))

    if headers.get("etag") != '"' + lib.md5(data) + '"':
        lib.fail(f"put {key}: etag {headers.get('etag')}")

for key, data in blobs.items():
    _, body = insist(f"get {key}", lambda: s3.request("GET", "/chaos/" + key))

    if body != data:
        lib.fail(f"get {key}: {len(body)} bytes of {len(data)}")


def placed():
    return sum(len(lib.pieces(etcd.manifest("chaos", key))) for key in blobs)


want = 3 * len(blobs)
deadline = time.time() + 120

while time.time() < deadline and placed() < want:
    time.sleep(0.5)

if placed() < want:
    lib.fail(f"{placed()} of {want} pieces placed after the repairs")

for key, data in blobs.items():
    _, body = insist(f"get {key} after the repairs", lambda: s3.request("GET", "/chaos/" + key))

    if body != data:
        lib.fail(f"get {key} after the repairs: {len(body)} bytes of {len(data)}")

# the browser runs the very same binary with nothing armed, which is how the ordinary one behaves
web = cluster.web(env={"S3_CHAOS": None})
status, _, body = web.request("GET", "/b/chaos")
if status != 200 or b'href="/o/chaos/o00"' not in body:
    lib.fail(f"web with nothing armed: {status} {body[-500:]}")

if os.environ.get("S3_CHAOS"):
    # the refusals happened, on both sides
    for name, log in (("front", cluster.front.log()), ("cell", cluster.cell(0).log())):
        if "chaos" not in log:
            lib.fail(f"the {name} was never refused")

    # the tool doing the refusing is strict about what it is told: a point
    # that does not exist, or a rate of nothing, would leave a run quietly
    # testing less than it says it does
    for spec, message in (("nonsense:5", "unknown chaos point"), ("dial:0", "needs a rate above zero")):
        r = lib.run("front", "-c", cluster.config, "-listen", "127.0.0.1:1", env={"S3_CHAOS": spec})

        if r.returncode != 1 or message not in r.stderr:
            lib.fail(f"chaos spec {spec}: rc={r.returncode} stderr={r.stderr!r}")

print("ok")
