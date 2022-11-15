.DEFAULT: all

CASSANDRA_IMG := "cassandra:3.11.14"
EXECUTABLE := "seed"

.PHONY: all
all: build test run

.PHONY: cassandra
cassandra:
	@if ! docker info >/dev/null 2>&1; then echo "ERROR: Docker must be running locally"; exit 1; fi
	docker pull $(CASSANDRA_IMG)
	docker run -d -p 9042:9042 $(CASSANDRA_IMG)
	@N=15; while [ "$$N" -ne "0" ]; do echo "Cassandra warming up... $$N"; N=$$((N - 1)) ; sleep 1; done

.PHONY: down
down:
	@docker rm -f $(shell docker ps | awk '/cassandra/{print $$1}')

.PHONY: cqlsh
cqlsh:
	@docker exec -it $(shell docker ps | awk '/cassandra/{print $$1}') cqlsh

.PHONY: build
build:
	@mkdir -p bin
	@rm -f bin/*
	go build -o bin/$(EXECUTABLE) cmd/main.go

.PHONY: test
test:
	@go test ./...

.PHONY: bench
bench:
	@go test -bench=. -benchtime=5s internal/benchmarks/*

.PHONY: run
run:
	bin/$(EXECUTABLE)

