#!/usr/bin/env bash

set -euo pipefail

# assumes `make cassandra` was already run and the instance is ready

if [ ! -f bin/seed ]; then
  make build
fi


function seed_data {
  # seed a bunch of small snapshots with a canonical and a couple historicals
  for s in {1..100}; do
    bin/seed -c -s 5 -m 20 -d 80
  done

  # seed a bunch of medium-sized snapshots with a canonical and a few historicals
  for s in {1..100}; do
    bin/seed -c -s 3 -m 40 -d 120
  done

  # seed a few big ass snapshots with a canonical and a couple historicals
  for s in {1..10}; do
    bin/seed -c -s 3 -m 100 -d 1000
  done
}

time seed_data
