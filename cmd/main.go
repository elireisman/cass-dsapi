package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/elireisman/cass-dsapi/internal/data"
)

var (
	verbose         bool
	canonical       bool
	numSnapshots    int
	numManifests    int
	maxDependencies int
)

func init() {
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.BoolVar(&canonical, "c", false, "generate series of related snapshots (generate a canonical + historicals)")
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
		var snap data.Snapshot
		var err error
		if canonical && i > 0 {
			snap, err = data.GenerateSnapshot(ctx, lgr, &snapshots[0], numManifests, maxDependencies)
			check(err, "generating canonical snapshot series")
		} else {
			snap, err = data.GenerateSnapshot(ctx, lgr, nil, numManifests, maxDependencies)
			check(err, "generating unique snapshots")
		}
		snapshots = append(snapshots, snap)
	}
	dur := time.Since(start)
	lgr.Printf("Generated %d snapshots in %s", len(snapshots), dur)

	sesh, err := data.CreateClient(ctx, lgr)
	check(err, "creating gocql.Session")

	err = data.CreateKeyspace(ctx, lgr, sesh, data.Keyspace)
	check(err, "creating keyspace")

	err = data.CreateTables(ctx, lgr, sesh, data.Keyspace)
	check(err, "creating tables")

	start = time.Now()
	for _, snap := range snapshots {
		if verbose {
			jsn, _ := json.MarshalIndent(&snap, "", "\t")
			fmt.Printf("\n%s\n", string(jsn))
		}
		err = data.Load(ctx, lgr, sesh, snap, data.Keyspace)
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
