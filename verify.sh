#!/bin/bash

# make sure we're in the root
DIR="$(dirname "$(readlink -f "$0")")"
cd $DIR

echo -- build all packages
REDIS=$REDIS go install ./...

echo -- build and run all tests
REDIS=$REDIS go test -cover ./...

echo -- rerun all tests with race detector
REDIS=$REDIS GOMAXPROCS=4 go test -race ./...

echo -- format source code
go fmt ./...

echo -- static analysis
go vet ./...

echo -- report coding style issues
find . -type f -name '*.go' -exec golint {} \;
