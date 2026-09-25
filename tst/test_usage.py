"""The binary explains itself and refuses to run half configured."""

import lib

r = lib.run()

if r.returncode != 1 or "Usage: s3 command" not in r.stderr:
    lib.fail(f"no usage without a command: rc={r.returncode} stderr={r.stderr!r}")

for cmd in ("cell", "front", "repair", "web"):
    r = lib.run(cmd)

    if r.returncode != 1 or "required" not in r.stderr:
        lib.fail(f"{cmd} without flags: rc={r.returncode} stderr={r.stderr!r}")

r = lib.run("nonsense")

if r.returncode != 1 or "Usage: s3 command" not in r.stderr:
    lib.fail(f"unknown command: rc={r.returncode} stderr={r.stderr!r}")

print("ok")
