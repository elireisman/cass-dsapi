# Data Modeling Tool

## Quick Start
* Spins up Cassandra and generates a snapshot: `make cassandra`
* `make` builds the generator and submits a single test snapshot. If it fails, rerun as Cassandra probably isn't ready yet (I have 16-core MacBook Pro, YMMV)
* You can then use CQL query tool to inspect the tables: `make cqlsh`

## Benchmarks
1. `make cassandra build`
3. Seed snapshots into Cassandra as desired: `bin/seed --help`
4. Run the benchmarks: `make bench`

## Cleanup
`make down`
