package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/elireisman/cass-dsapi/internal/data"
	//"github.com/gocql/gocql"
)

var (
	verbose         bool
	numSnapshots    int
	numManifests    int
	maxDependencies int
)

func init() {
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.IntVar(&numSnapshots, "s", 1, "number of snapshots to generate and write to Cassandra")
	flag.IntVar(&numManifests, "m", 20, "number of manifests to generate per snapshot")
	flag.IntVar(&maxDependencies, "d", 200, "max number of dependencies per manifest to generate")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	lgr := log.Default()

	start := time.Now()
	snap, err := data.GenerateSnapshot(ctx, lgr, numManifests, maxDependencies)
	dur := time.Since(start)
	check(err, "generating snapshot")
	lgr.Printf("Snapshot generated in %s", dur)

	if verbose {
		jsn, _ := json.MarshalIndent(&snap, "", "\t")
		fmt.Printf("\n%s\n", string(jsn))
	}
	/*
		cfg := gocql.NewCluster("127.0.0.1")
		cfg.Keyspace = "eli_demo"
		cfg.Logger = lgr

		sesh, err := gocql.NewSession(*cfg)
		check(err, "creating gocql.Session")

		err = data.CreateTables(ctx, lgr, *sesh)
		check(err, "creating tables")

		start = time.Now()
		err = data.Load(ctx, lgr, sesh, snap)
		check(err, "ingesting snapshot into Cassandra")
		dur = time.Since(start)
		lgr.Printf("Ingested snapshot into Cassandra in %s", dur)
	*/
}

func check(err error, msg string) {
	if err != nil {
		panic(msg + ": " + err.Error())
	}
}
