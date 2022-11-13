package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/elireisman/cass-dsapi/internal/data"
	"github.com/gocql/gocql"
)

var (
	verbose         bool
	numSnapshots    int
	numManifests    int
	maxDependencies int
)

const keyspaceName = "eli_demo"

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

	var snapshots []data.Snapshot
	start := time.Now()
	for i := 0; i < numSnapshots; i++ {
		snap, err := data.GenerateSnapshot(ctx, lgr, numManifests, maxDependencies)
		check(err, "generating snapshots")
		snapshots = append(snapshots, snap)
	}
	dur := time.Since(start)
	lgr.Printf("Generated %d snapshots in %s", len(snapshots), dur)

	cfg := gocql.NewCluster("127.0.0.1")
	//cfg.Keyspace = "eli_demo"
	cfg.Logger = lgr
	cfg.ProtoVersion = 3
	cfg.ConnectTimeout = 2 * time.Second
	cfg.Timeout = 10 * time.Second

	sesh, err := cfg.CreateSession()
	check(err, "creating gocql.Session")

	err = data.CreateKeyspace(ctx, lgr, *sesh, keyspaceName)
	check(err, "creating keyspace")

	err = data.CreateTables(ctx, lgr, *sesh, keyspaceName)
	check(err, "creating tables")

	start = time.Now()
	for _, snap := range snapshots {
		if verbose {
			jsn, _ := json.MarshalIndent(&snap, "", "\t")
			fmt.Printf("\n%s\n", string(jsn))
		}
		err = data.Load(ctx, lgr, *sesh, snap)
		check(err, "ingesting snapshot into Cassandra")
	}
	dur = time.Since(start)
	lgr.Printf("Ingested %d snapshots into Cassandra in %s", len(snapshots), dur)
}

func check(err error, msg string) {
	if err != nil {
		panic(msg + ": " + err.Error())
	}
}
