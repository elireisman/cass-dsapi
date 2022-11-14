.DEFAULT: all

CASSANDRA_IMG := "cassandra:3.11.14"
EXECUTABLE := "seed"

.PHONY: all
all: build test run

.PHONY: cassandra
cassandra:
	@if ! docker info >/dev/null 2>&1; then echo "ERROR: Docker must be running locally"; exit 1; fi
	docker pull $(CASSANDRA_IMG)
	docker run -d --rm -p 9042:9042 $(CASSANDRA_IMG)
	@N=10; while [ "$$N" -ne "0" ]; do echo "waiting... $$N"; N=$$((N - 1)) ; sleep 1; done

.PHONY: build
build:
	@mkdir -p bin
	@rm -f bin/*
	go build -o bin/$(EXECUTABLE) cmd/main.go

.PHONY: test
test:
	@#go test ./...

.PHONY: bench
bench:
	@go test ./...

.PHONY: run
run:
	bin/$(EXECUTABLE)

