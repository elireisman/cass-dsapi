package data

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	Keyspace = "eli_demo"

	maxWriteConcurrency = 8
	batchSize           = 200
)

func CreateClient(ctx context.Context, lgr *log.Logger) (*gocql.Session, error) {
	cfg := gocql.NewCluster("127.0.0.1")
	cfg.Logger = lgr
	cfg.ProtoVersion = 3
	cfg.ConnectTimeout = 2 * time.Second
	cfg.Timeout = 10 * time.Second
	cfg.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	return cfg.CreateSession()
}

func CreateKeyspace(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string) error {
	lgr.Printf("Creating keyspace %q...", keyspace)
	q := fmt.Sprintf(`
	  CREATE KEYSPACE IF NOT EXISTS %s
	  WITH replication = {
	      'class' : 'SimpleStrategy',
	      'replication_factor' : 1
	  }`, keyspace)
	return client.Query(q).Exec()
}

func CreateTables(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string) error {
	for _, table := range tables {
		q := fmt.Sprintf(table, keyspace)
		lgr.Printf("\n%s", q)
		if err := client.Query(q).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func Load(ctx context.Context, lgr *log.Logger, client *gocql.Session, snapshot Snapshot, keyspace string) error {
	if err := writeSnapshot(ctx, lgr, client, keyspace, snapshot); err != nil {
		return fmt.Errorf("writing snapshot %s: %s", snapshot.ID, err)
	}
	lgr.Printf("Snapshot %s written", snapshot.ID)

	g, gctx := errgroup.WithContext(ctx)
	tickets := make(chan struct{}, maxWriteConcurrency)
	for i := 0; i < len(snapshot.Manifests); i++ {
		manifest := snapshot.Manifests[i]

		g.Go(func() error {
			// return ticket when work is done
			defer func() { <-tickets }()

			// add a ticket before doing work (can block!)
			tickets <- struct{}{}

			if err := writeManifest(gctx, lgr, client, keyspace, snapshot, manifest); err != nil {
				return fmt.Errorf("writing manifest %s: %s", manifest.ID, err)
			}
			lgr.Printf("\tManifest %s written", manifest.ID)

			total, err := batchDependencies(gctx, lgr, client, keyspace, snapshot, manifest)
			if err != nil {
				return fmt.Errorf("writing dependency batches for manifest %s: %s", manifest.ID, err)
			}
			lgr.Printf("\t\tTotal %d Dependencies written", total)

			return nil
		})
	}

	return g.Wait()
}

func batchDependencies(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string, snapshot Snapshot, manifest Manifest) (uint, error) {
	mdeps := client.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	rdeps := client.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	dcounts := client.NewBatch(gocql.CounterBatch).WithContext(ctx)

	rtProcessed := 0
	for _, dependency := range manifest.Runtime {
		if err := addDependency(ctx, client, &mdeps, &rdeps, &dcounts, keyspace, snapshot, manifest, dependency); err != nil {
			return 0, err
		}
		rtProcessed++
	}
	lgr.Printf("\t\t%d direct runtime dependencies submitted", rtProcessed)

	devProcessed := 0
	for _, dependency := range manifest.Development {
		if err := addDependency(ctx, client, &mdeps, &rdeps, &dcounts, keyspace, snapshot, manifest, dependency); err != nil {
			return 0, err
		}
		devProcessed++
	}
	lgr.Printf("\t\t%d direct development dependencies submitted", devProcessed)

	trProcessed := 0
	for _, dependency := range manifest.Transitives {
		if err := addDependency(ctx, client, &mdeps, &rdeps, &dcounts, keyspace, snapshot, manifest, dependency); err != nil {
			return 0, err
		}
		trProcessed++
	}
	lgr.Printf("\t\t%d transitive dependencies submitted", trProcessed)

	// final flush
	for _, batch := range []*gocql.Batch{mdeps, rdeps, dcounts} {
		if err := client.ExecuteBatch(batch); err != nil {
			return 0, fmt.Errorf("performing final dependencies batch flush: %s", err)
		}
	}
	return uint(rtProcessed + devProcessed + trProcessed), nil
}

func writeSnapshot(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string, sm Snapshot) error {
	query := fmt.Sprintf(`INSERT INTO %s.snapshots
	  (id, owner_id, repository_id, nwo, created_at, ref, commit_oid, blob_url, source_url)
	  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)

	return client.Query(query).Bind(
		sm.ID,
		sm.OwnerID,
		sm.RepositoryID,
		sm.RepositoryNWO,
		sm.CreatedAt,
		sm.Ref,
		sm.CommitSHA,
		sm.BlobURL,
		sm.SourceURL).Exec()
}

func writeManifest(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string, sm Snapshot, mm Manifest) error {
	query := fmt.Sprintf(`INSERT INTO %s.manifests
	  (id, snapshot_id, owner_id, repository_id, ref, commit_oid, blob_key, manifest_key,
	   package_manager, project_name, project_version, project_license)
	  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)

	return client.Query(query).Bind(
		mm.ID,
		sm.ID,
		sm.OwnerID,
		sm.RepositoryID,
		sm.Ref,
		sm.CommitSHA,
		mm.BlobKey, // tracked here since Snapshot can be composite of N submissions as cached in AzBS
		mm.FilePath,
		mm.PackageManager,
		mm.ProjectName,
		mm.ProjectVersion,
		mm.ProjectLicense).Exec()
}

func addDependency(ctx context.Context, client *gocql.Session, mdeps, drepos, dcounts **gocql.Batch,
	keyspace string, sm Snapshot, mm Manifest, dep Dependency) error {

	mdQuery := fmt.Sprintf(`INSERT INTO %s.manifest_dependencies
	  (manifest_id, package_manager, namespace, name, version, snapshot_id, license, source_url, scope, relationship, runtime, development)
	  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)

	(*mdeps).Entries = append((*mdeps).Entries, gocql.BatchEntry{
		Stmt: mdQuery,
		Args: []interface{}{
			mm.ID,
			mm.PackageManager,
			dep.Namespace,
			dep.Name,
			dep.Version,
			sm.ID,
			dep.License,
			dep.SourceURL,
			dep.Scope,
			dep.Relationship,
			dep.Runtime,
			dep.Development},
		Idempotent: false,
	})

	drQuery := fmt.Sprintf(`INSERT INTO %s.dependent_repositories
          (package_manager, namespace, name, version, owner_id, repository_id, license, source_url)
	  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)
	(*drepos).Entries = append((*drepos).Entries, gocql.BatchEntry{
		Stmt: drQuery,
		Args: []interface{}{
			mm.PackageManager,
			dep.Namespace,
			dep.Name,
			dep.Version,
			sm.OwnerID,
			sm.RepositoryID,
			dep.License,
			dep.SourceURL},
		Idempotent: true,
	})

	countsQuery := fmt.Sprintf(`UPDATE %s.dependent_repository_counts SET used_by = used_by + 1
	  WHERE package_manager = ? AND namespace = ? AND name = ? AND version = ?`, keyspace)
	(*dcounts).Entries = append((*dcounts).Entries, gocql.BatchEntry{
		Stmt: countsQuery,
		Args: []interface{}{
			mm.PackageManager,
			dep.Namespace,
			dep.Name,
			dep.Version},
		Idempotent: false,
	})

	return checkFlushBatches(ctx, client, mdeps, drepos, dcounts)
}

func checkFlushBatches(ctx context.Context, client *gocql.Session, mdeps, drepos, dcounts **gocql.Batch) error {
	if (*mdeps).Size()%batchSize == 0 {
		if err := client.ExecuteBatch(*mdeps); err != nil {
			return fmt.Errorf("flushing manifest_dependencies entries: %s", err)
		}
		*mdeps = client.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	}
	if (*drepos).Size()%batchSize == 0 {
		if err := client.ExecuteBatch(*drepos); err != nil {
			return fmt.Errorf("flushing dependent_repositories entries: %s", err)
		}
		*drepos = client.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	}
	if (*dcounts).Size()%batchSize == 0 {
		if err := client.ExecuteBatch(*dcounts); err != nil {
			return fmt.Errorf("flushing dependent_repository_counts entries: %s", err)
		}
		*dcounts = client.NewBatch(gocql.CounterBatch).WithContext(ctx)
	}

	return nil
}

var tables = []string{
	`
CREATE TABLE IF NOT EXISTS %s.snapshots (
	// unique ID for the record
	id   uuid,

	// repo metadata
	owner_id varint,
	repository_id varint,
	nwo text,
	source_url text,

	// Git metadata (ref, commit SHA)
	ref text,
	commit_oid text,

	// build time snap: “scanned_at”
	// push-time snap: “pushed_at”
	created_at timestamp,

	// AzBS target for manifests correlated to this snapshot
	blob_url text,

	// partition and clustering keys
	PRIMARY KEY ((repository_id, ref), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
`,

	`
CREATE TABLE IF NOT EXISTS %s.manifests (
	// unique ID for the record
	id   uuid,

	// parent snapshot ID
	snapshot_id uuid,

	// repo metadata
	owner_id varint,
	repository_id varint,

	// Git metadata
	ref text,
	commit_oid text,

	// single snapshot can be composed of many AzBS-cached submissions
	blob_key text,

	// manifest path+filename, or build label
	manifest_key text,

	// manifest project metadata
	package_manager text,
	project_name text,
	project_version text,
	project_license text,

	// partition and clustering keys
	PRIMARY KEY ((repository_id, ref), snapshot_id, package_manager, manifest_key)
) WITH CLUSTERING ORDER BY (snapshot_id DESC, package_manager ASC, manifest_key ASC);
`,

	`
CREATE TABLE IF NOT EXISTS %s.manifest_dependencies (
      // parent snapshot, manifest IDs
      snapshot_id uuid,
      manifest_id uuid,

      // decomposed package PURL fields
      package_manager text,
      namespace text,
      name text,
      version text,

      // package metadata
      license text,
      source_url text,
      scope text,
      relationship text,

      // PURLs for all direct “runtime” deps
      runtime set<text>,
      // PURLs of all direct “development” deps
      development set<text>,

      // partition and clustering keys
      PRIMARY KEY ((manifest_id), package_manager, namespace, name, version)
);
`,

	`
CREATE TABLE IF NOT EXISTS %s.dependent_repositories (
      // decomposed package PURL fields
      package_manager text,
      namespace text,
      name text,
      version text,

      // repo metadata
      owner_id varint,
      repository_id varint,

      // package metadata and usage counter
      license text,
      source_url text,

      // partition and clustering keys
      PRIMARY KEY ((package_manager, namespace, name), version, repository_id)
) WITH CLUSTERING ORDER BY (version DESC, repository_id ASC);
`,

	`
CREATE TABLE IF NOT EXISTS %s.dependent_repository_counts (
      // decomposed package PURL fields
      package_manager text,
      namespace text,
      name text,
      version text,

      // usage counter type
      used_by counter,

      // partition and clustering keys
      PRIMARY KEY ((package_manager, namespace, name), version)
) WITH CLUSTERING ORDER BY (version DESC);
`,
}
