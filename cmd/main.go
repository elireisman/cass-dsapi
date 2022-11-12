package main

import (
	"context"
	"log"
	"time"

	"github.com/elireisman/internal/data"

	"github.com/gocql/gocql"
)

const (
	maxManifests    = 20
	maxDependencies = 200
)

func main() {
	ctx := context.Background()
	lgr := log.DefaultLogger()

	cfg := gocql.NewCluster("127.0.0.1")
	cfg.Keyspace = "eli_demo"
	cfg.Logger = lgr

	sesh, err := gocql.NewSession(cluster)
	check(err, "creating gocql.Session")

	err = data.CreateTables(ctx, lgr, sesh)
	check(err, "creating tables")

	var snap []Snapshot

	start := time.Now()
	for i := 0; i < numSnapshots; i++ {
		snap, err := data.GenerateSnapshot(ctx, lgr, maxManifests, maxDependencies)
		check(err, "generating snapshot")
	}
	dur := time.Since(start)
	lgr.Printf("Snapshot generated in %s", dur)

	start = time.Now()
	err = data.Load(ctx, lgr, sesh, snap)
	check(err, "ingesting snapshot into Cassandra")
	dur = time.Since(start)
	lgr.Printf("Ingested snapshot into Cassandra in %s", dur)
}

func check(err error, msg string) {
	if err != nil {
		panic(msg + ": " + err.Error())
	}
}
