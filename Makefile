GO ?= go

.PHONY: all build test vet fmt run clean

all: vet test build

build:
	$(GO) build -o bin/s3warm ./cmd/s3warm

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w -l .

run:
	$(GO) run ./cmd/s3warm

clean:
	rm -rf bin
