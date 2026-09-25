#!/usr/bin/env sh

set -xue

../ay/ay dev refac lint
gofmt -w *.go
./build s3
