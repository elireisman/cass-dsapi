package main

import (
	"context"
	"log"
	"time"

	"github.com/elireisman/internal/data"

	"github.com/gocql/gocql"
)

const (
	numSnapshots    = 1
	maxManifests    = 50
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

	start := time.Now()
	for i := 0; i < numSnapshots; i++ {
		err = data.GenerateSnapshot(ctx, lgr, sesh, numManifests, maxDependencies)
		check(err)
	}
	dur := time.Since(start)
	log.Printf("%d snapshots generated in %s", numSnapshots, dur)
}

func check(err error, msg string) {
	if err != nil {
		panic(msg + ": " + err.Error())
	}
}
