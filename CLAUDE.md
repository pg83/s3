# s3

S3 store over append-only cells and etcd. The design is in `README.md`;
what is deliberately left out of the MVP is listed there too, do not add
it back without being asked.

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
- `./lint.sh` before committing style-sensitive changes; it strips every
  comment that is not a compiler directive. What code does belongs in its
  name, why it exists belongs in the commit message and in `README.md`.
