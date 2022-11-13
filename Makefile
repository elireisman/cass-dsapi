.DEFAULT: all

CASSANDRA_IMG := "cassandra:3.11.14"

.PHONY: all
all: build test run

.PHONY: cassandra
cassandra:
	@if ! docker info >/dev/null 2>&1; then echo "ERROR: Docker must be running locally"; exit 1; fi
	docker pull $(CASSANDRA_IMG)
	docker run --rm -p 9042:9042 $(CASSANDRA_IMG)

.PHONY: build
build:
	@mkdir -p bin
	@rm -f bin/*
	go build -o bin/demo cmd/main.go

.PHONY: test
test:
	@#go test ./...

.PHONY: run
run:
	bin/demo

