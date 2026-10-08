"""Prometheus metrics: every cell, front and repair serves them on its own
-metrics address and a process without one serves nothing more; the
counters and histograms move with the work done, the gauges are computed
when asked: the queues, the LRU on the load SSD, the head and the HDD
pointer of each cell."""

import os
import socket
import struct
import time

import lib

MB = 1 << 20
BLOCK = 2 * MB
HDD = 64 * MB

etcd = lib.Etcd()

if etcd is None:
    print("skip: no etcd binary (set S3_TEST_ETCD)")
    raise SystemExit(0)

etcd.start()
cluster = lib.Cluster(etcd, hosts=3, cells=1, hdd_bytes=HDD, buckets=["b"]).start()
s3 = cluster.s3()
cells = [cluster.cell(i) for i in range(3)]


def scrape_cells():
    return [c.scrape() for c in cells]


def total(pages, name, **labels):
    return sum(lib.metric(p, name, **labels) for p in pages)


def expect(what, ok):
    if not ok:
        lib.fail(what)


big = os.urandom(5 * MB)
small = b"metrics"

for key, body in (("big", big), ("small", small)):
    status, _, _ = s3.request("PUT", f"/b/{key}", body)
    expect(f"put {key}: {status}", status == 200)

status, _, got = s3.request("GET", "/b/big")
expect("get big", status == 200 and got == big)

status, _, got = s3.request("GET", "/b/big", headers={"Range": "bytes=0-99"})
expect("range get", status == 206 and got == big[:100])

expect("head of a missing key", s3.request("HEAD", "/b/none")[0] == 404)
expect("an unknown bucket", s3.request("GET", "/nobucket/x")[0] == 404)
expect("list buckets", s3.request("GET", "/")[0] == 200)
expect("list a bucket", s3.request("GET", "/b?list-type=2")[0] == 200)
expect("an unknown method", s3.request("PATCH", "/b/small")[0] == 405)

# Every cell gets a piece of 2.5 MiB, the third one when the put settles; its
# first block reaches the HDD. Then reads come from the HDD, then from the LRU.
deadline = time.time() + 20

while time.time() < deadline:
    pages = scrape_cells()

    if all(lib.metric(p, "s3_cell_hdd_pointer_bytes") >= BLOCK for p in pages):
        break

    time.sleep(0.1)

expect("a block of every cell flushed to the HDD", all(lib.metric(p, "s3_cell_hdd_pointer_bytes") >= BLOCK for p in pages))

cluster.clear_lru()

for _ in range(2):
    status, _, got = s3.request("GET", "/b/big")
    expect("get big after the LRU was cleared", status == 200 and got == big)

# A read past the disk and a frame without an id, straight at a cell.
client = cells[0].client()
expect("read past the disk", client.read(HDD, 8) is None)
client.close()

with socket.create_connection(("127.0.0.1", cells[0].port)) as raw:
    raw.sendall(struct.pack(">I", 1) + bytes([lib.OP_APPEND]))
    raw.recv(1)

pages = scrape_cells()

expect("appends", total(pages, "s3_cell_writes_total", result="ok") >= 6)
expect("append timings", total(pages, "s3_cell_write_seconds_count") == total(pages, "s3_cell_writes_total"))
expect("appended bytes", total(pages, "s3_cell_write_bytes_total") >= len(big) * 3 // 2)
expect("tick phases", total(pages, "s3_cell_tick_seconds_count", phase="write") > 0 and total(pages, "s3_cell_tick_seconds_count", phase="sync") > 0)
expect("reads", total(pages, "s3_cell_reads_total", result="ok") > 0 and total(pages, "s3_cell_read_seconds_count") > 0)
expect("read bytes", total(pages, "s3_cell_read_bytes_total") >= len(big))
expect("a read past the disk", lib.metric(pages[0], "s3_cell_reads_total", result="range") >= 1)
expect("a frame without an id", lib.metric(pages[0], "s3_cell_errors_total", op="frame") == 1)
expect("blocks from the store", total(pages, "s3_cell_blocks_total", source="store") > 0)
expect("blocks from the HDD", total(pages, "s3_cell_blocks_total", source="hdd") > 0 and total(pages, "s3_cell_hdd_read_seconds_count") > 0)
expect("blocks from the LRU", total(pages, "s3_cell_blocks_total", source="lru") > 0)
expect("flushes", total(pages, "s3_cell_flush_seconds_count", phase="write") > 0 and total(pages, "s3_cell_flush_seconds_count", phase="sync") > 0)
expect("HDD bytes", total(pages, "s3_cell_hdd_written_bytes_total") >= 3 * BLOCK)

for c, p in zip(cells, pages):
    head = lib.metric(p, "s3_cell_head_bytes")
    lru = lib.metric(p, "s3_cell_lru_blocks")

    expect(f"head of {c.port}: {head}", head == c.client().status()[0])
    expect(f"pointers of {c.port}", BLOCK <= lib.metric(p, "s3_cell_hdd_pointer_bytes") <= head)
    expect(f"capacity of {c.port}", lib.metric(p, "s3_cell_capacity_bytes") == HDD)
    expect(f"lru of {c.port}: {lru}", lru == len([n for n in os.listdir(c.load) if n.isdigit()]) and lib.metric(p, "s3_cell_lru_bytes") == lru * BLOCK)

    for name in ("s3_cell_write_queue_requests", "s3_cell_read_queue_requests", "s3_cell_flush_queue_blocks"):
        expect(f"{name} of {c.port}", lib.has_metric(p, name))

front = cluster.front.scrape()

expect("puts", lib.metric(front, "s3_front_requests_total", method="PUT", target="object", code="200") == 2)
expect("gets", lib.metric(front, "s3_front_requests_total", method="GET", target="object", code="200") == 3)
expect("range gets", lib.metric(front, "s3_front_requests_total", method="GET", target="object", code="206") == 1)
expect("missing keys", lib.metric(front, "s3_front_requests_total", method="HEAD", target="object", code="404") == 1)
expect("listings", lib.metric(front, "s3_front_requests_total", target="service", code="200") == 1 and lib.metric(front, "s3_front_requests_total", target="bucket", code="200") == 1)
expect("other methods", lib.metric(front, "s3_front_requests_total", method="other", code="405") == 1)
expect("request timings", lib.metric(front, "s3_front_request_seconds_count", method="PUT") == 2)
expect("received bytes", lib.metric(front, "s3_front_received_bytes_total") == len(big) + len(small))
expect("sent bytes", lib.metric(front, "s3_front_sent_bytes_total") >= 3 * len(big))
expect("link appends", lib.metric(front, "s3_link_seconds_count", op="append") >= 6)
expect("link reads", lib.metric(front, "s3_link_seconds_count", op="read") > 0)
expect("link queues", all(lib.has_metric(front, "s3_link_queue_messages") for _ in cells) and lib.has_metric(front, "s3_front_bodies_in_flight"))

repairs = cluster.repair()
etcd.put("repair/h0/b/big", b"")
etcd.put("repair/h0/b/none", b"")
etcd.put("repair/h0/broken", b"")

deadline = time.time() + 20
page = {}

while time.time() < deadline:
    lib.wait_port(repairs[0].metrics, proc=repairs[0])
    page = lib.scrape(repairs[0].metrics)

    if lib.metric(page, "s3_repair_fixes_total") >= 3 and lib.metric(page, "s3_repair_queue_entries") == 0:
        break

    time.sleep(0.1)

expect(f"repair outcomes: {page}", all(lib.metric(page, "s3_repair_fixes_total", result=r) >= 1 for r in ("sound", "gone", "malformed")))
expect("repair passes", lib.metric(page, "s3_repair_pass_seconds_count") >= 1)
expect("repair queues", lib.metric(page, "s3_repair_queue_entries") == 0 and lib.has_metric(page, "s3_repair_inprogress_entries"))
expect("repair links", lib.has_metric(page, "s3_link_queue_messages"))

# A cell that goes away: its link counts the disconnect and the appends it failed.
cells[2].stop()
status, _, _ = s3.request("PUT", "/b/after", small)
expect(f"put with a cell down: {status}", status == 200)

deadline = time.time() + 20

while time.time() < deadline:
    front = cluster.front.scrape()

    if lib.metric(front, "s3_link_failures_total", cell="2", reason="down") > 0:
        break

    time.sleep(0.1)

expect("disconnects", lib.metric(front, "s3_link_disconnects_total", cell="2") >= 1)
expect("appends lost to a down cell", lib.metric(front, "s3_link_failures_total", cell="2", reason="down") > 0)

# Without -metrics a cell serves its protocol and nothing else.
quiet = lib.Cell(HDD, metrics=False).start()
client = quiet.client()
expect("a cell without metrics", client.status()[1] > 0)
client.close()

print("ok")
