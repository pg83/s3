#!/usr/bin/env sh

set -xue

../ay/ay dev refac lint
rm -f node_ownership_gen.go
gofmt -w *.go
./build s3
