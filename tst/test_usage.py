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

r = lib.run("nonsense")

if r.returncode != 1 or "Usage: s3 command" not in r.stderr:
    lib.fail(f"unknown command: rc={r.returncode} stderr={r.stderr!r}")

print("ok")
