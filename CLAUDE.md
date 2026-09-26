# s3

S3 store over append-only cells and etcd. `README.md` is for users only:
what it is, how to build and run it, what to expect, what it does not do.
The Limits there are deliberate; do not add any of it back without being
asked. The why of every design decision lives in the commit messages.

## Conventions

- Style: `STYLE.md`. One `package main`, all `.go` files in the repo root.
- Git author: `claude <claude@users.noreply.github.com>`. Commit messages in English.
- Config is JSON only.
- Errors go through `throw.go`; catches sit at the boundaries only.

## Build and test

- `./build` builds `.build/bin/s3` and publishes `./s3`.
- `./build test` runs the e2e suite in `tst/`: scenarios are Python
  scripts driving the real binary as local processes. Tests are e2e
  only; do not add Go unit tests.
- `./build -Drace test` runs the same suite with the race detector.
- `tst/test_s3.py` needs an etcd server binary: `S3_TEST_ETCD=/path/to/etcd`,
  or `etcd` on PATH; without one the scenario skips.
- Dependencies are pinned in `go.mod` and `go.sum`; no vendor directory.
- `./build -Dcoverage coverage` writes `.build/coverage.out` from the e2e
  suite: the binary is instrumented and every process gets a directory of
  its own under `GOCOVERDIR`. A process exits through `os.Exit` on SIGTERM
  so the counters reach the disk; SIGKILL loses them, and `dev/coverage.py`
  refuses a run that lost any. The lab stops every process it started at
  exit; a scenario must not leave one killed.
- `./build chaos` runs every scenario again against `s3-chaos`, the same
  binary built behind the `s3chaos` tag. Its `Syscalls` implementation
  refuses some calls the way the kernel is entitled to. `S3_CHAOS` names
  the points and how often each fails, `S3_CHAOS_SEED` makes the choice
  repeatable. Every call into the operating system that can fail belongs in
  `syscalls.go`; `dev/chaos_points.py` refuses a point nothing asks about.
  The points armed for the whole suite are in `build.py` and must be ones
  every scenario survives unchanged; the rest are armed in `tst/test_chaos.py`.
- Coverage is reported from every run together: the plain suite and the
  chaos suite each hand their profile to the aggregate job, which merges
  them with `dev/merge_coverage.py` and uploads one report to Codecov. The
  floor of one suite is in `build.py`, the floor of the runs added up is in
  `.github/workflows/ci.yml` and `codecov.yml`.
- `./lint.sh` before committing style-sensitive changes; it strips every
  comment that is not a compiler directive. What code does belongs in its
  name, why it exists belongs in the commit message.
