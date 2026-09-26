"""The binary explains itself and refuses to run half configured."""

import lib

r = lib.run()

if r.returncode != 1 or "Usage: s3 command" not in r.stderr:
    lib.fail(f"no usage without a command: rc={r.returncode} stderr={r.stderr!r}")

for cmd in ("cell", "front", "repair", "web"):
    r = lib.run(cmd)

    if r.returncode != 1 or "required" not in r.stderr:
        lib.fail(f"{cmd} without flags: rc={r.returncode} stderr={r.stderr!r}")

import json
import os
import tempfile

cfg = os.path.join(tempfile.mkdtemp(prefix="s3usage-"), "config.json")

for spec, want in (
    ({"etcd": ["http://127.0.0.1:1"], "cells": []}, "buckets are required"),
    ({"etcd": ["http://127.0.0.1:1"], "buckets": ["a/b"], "cells": []}, "bucket name"),
    ({"etcd": ["http://127.0.0.1:1"], "buckets": ["a", "a"], "cells": []}, "listed twice"),
    ({"buckets": ["a"], "cells": []}, "etcd endpoints are required"),
):
    with open(cfg, "w") as f:
        json.dump(spec, f)

    r = lib.run("front", "-c", cfg, "-listen", "127.0.0.1:1")

    if r.returncode != 1 or want not in r.stderr:
        lib.fail(f"config {spec}: rc={r.returncode} stderr={r.stderr!r}")

three = {"etcd": ["http://127.0.0.1:1"], "buckets": ["a"],
         "cells": [{"id": i, "host": f"h{i}", "addr": f"127.0.0.1:{i + 1}"} for i in range(3)]}
one = {**three, "cells": three["cells"][:1]}

for spec, want in (
    ({**one, "cells": one["cells"] * 2}, "listed twice"),
    ({**one, "cells": [{"id": 0, "host": "", "addr": ""}]}, "needs host and addr"),
):
    with open(cfg, "w") as f:
        json.dump(spec, f)

    r = lib.run("front", "-c", cfg, "-listen", "127.0.0.1:1")

    if r.returncode != 1 or want not in r.stderr:
        lib.fail(f"config {spec}: rc={r.returncode} stderr={r.stderr!r}")

with open(cfg, "w") as f:
    json.dump(one, f)

for args, want in (
    (("front", "-c", cfg), "-listen is required"),
    (("front", "-c", cfg, "-listen", "127.0.0.1:1"), "three are needed"),
    (("repair", "-c", cfg), "-host is required"),
    (("web", "-c", cfg), "-listen is required"),
):
    r = lib.run(*args)

    if r.returncode != 1 or want not in r.stderr:
        lib.fail(f"{args}: rc={r.returncode} stderr={r.stderr!r}")

with open(cfg, "w") as f:
    json.dump(three, f)

r = lib.run("repair", "-c", cfg, "-host", "nope")

if r.returncode != 1 or "no cells of host nope" not in r.stderr:
    lib.fail(f"repair of a host that is not there: rc={r.returncode} stderr={r.stderr!r}")

r = lib.run("nonsense")

if r.returncode != 1 or "Usage: s3 command" not in r.stderr:
    lib.fail(f"unknown command: rc={r.returncode} stderr={r.stderr!r}")

print("ok")
