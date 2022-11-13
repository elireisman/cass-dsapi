package data

import (
	"context"
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func CreateKeyspace(ctx context.Context, lgr *log.Logger, client gocql.Session, keyspace string) error {
	lgr.Printf("Creating keyspace...")
	q := fmt.Sprintf(`
	  CREATE KEYSPACE %s
	  WITH replication = {
	      'class' : 'SimpleStrategy',
	      'replication_factor' : 1
	  }`, keyspace)
	return client.Query(q).Exec()
}

func CreateTables(ctx context.Context, lgr *log.Logger, client gocql.Session, keyspace string) error {
	for _, table := range tables {
		q := fmt.Sprintf(table, keyspace)
		lgr.Printf("Creating table:\n%s\n\n", q)
		if err := client.Query(q).Exec(); err != nil {
			return err
		}
	}
	return nil
}

// TODO: MAKE THIS BENCHMARK FRIENDLY!
func Load(ctx context.Context, lgr *log.Logger, client gocql.Session, snapshot Snapshot) error {
	return nil
}

// TODO
func writeManifest(ctx context.Context, lgr *log.Logger, client gocql.Session, sm Snapshot, mm Manifest) error {
	return nil
}

// TODO: use batches of INSERTs instead? use UPDATE instead?
func writeDependency(ctx context.Context, lgr *log.Logger, client gocql.Session, sm Snapshot, mm Manifest, dep Dependency) error {
	// manifest_dependencies query
	query := fmt.Sprintf(`INSERT INTO %s.manifest_dependencies
	  (manifest_id, package_manager, namespace, name, version, snapshot_id, license, source_url, scope, relationship, runtime, development)
	  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err := client.Query(query).Bind(
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
		dep.Development).Exec(); err != nil {
		return err
	}

	// TODO: dependent_repos query
	// TODO: dependent_repo_counts query

	return nil
}

var tables = []string{
	`
CREATE TABLE %s.snapshots (
	// unique ID for the record
	id   uuid,

	// repo metadata
	owner_id int,
	repository_id int,

	// Git metadata (ref, commit SHA)
	ref text,
	commit_oid text,

	// build time snap: “scanned_at”
	// push-time snap: “pushed_at”
	created_at timestamp,

	// Azure Blob store metadata
	blob_key text,
	bucket_url text,

	// partition and clustering keys
	PRIMARY KEY ((repository_id, ref), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
`,

	`
CREATE TABLE %s.manifests (
	// unique ID for the record
	id   uuid,

	// parent snapshot ID
	snapshot_id uuid,

	// repo metadata
	owner_id int,
	repository_id int,

	// Git metadata
	ref text,
	commit_oid text,
	blob_oid text, // optional

	// manifest path+filename, or build label
	manifest_key text,

	// manifest project metadata
	package_manager text,
	project_name text,
	project_version text,
	license text,
	source_url text,

	// PURLs for all direct “runtime” deps
	runtime set<text>,
        // PURLs of all direct “development” deps
	development set<text>,

	// partition and clustering keys
	PRIMARY KEY ((repository_id, ref), snapshot_id, package_manager, manifest_key)
) WITH CLUSTERING ORDER BY (snapshot_id DESC, package_manager ASC, manifest_key ASC);
`,

	`
CREATE TABLE %s.manifest_dependencies (
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
CREATE TABLE %s.dependent_repositories (
      // unique ID for record
      id   int,

      // decomposed package PURL fields
      package_manager text,
      namespace text,
      name text,
      version text,

      // repo metadata
      owner_id int,
      repository_id int,

      // package metadata and usage counter
      license text,
      source_url text,

      // partition and clustering keys
      PRIMARY KEY ((package_manager, namespace, name), version, repository_id)
) WITH CLUSTERING ORDER BY (version DESC, repository_id ASC);
`,

	`
CREATE TABLE %s.dependent_repository_counts (
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
