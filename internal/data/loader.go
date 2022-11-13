package data

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func CreateClient(ctx context.Context, lgr *log.Logger) (*gocql.Session, error) {
	cfg := gocql.NewCluster("127.0.0.1")
	cfg.Logger = lgr
	cfg.ProtoVersion = 3
	cfg.ConnectTimeout = 2 * time.Second
	cfg.Timeout = 10 * time.Second

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

	for _, manifest := range snapshot.Manifests {
		if err := writeManifest(ctx, lgr, client, keyspace, snapshot, manifest); err != nil {
			return fmt.Errorf("writing manifest %s: %s", manifest.ID, err)
		}
		lgr.Printf("\tManifest %s written", manifest.ID)

		for _, dependency := range manifest.Runtime {
			if err := writeDependency(ctx, lgr, client, keyspace, snapshot, manifest, dependency); err != nil {
				return fmt.Errorf("writing manifest %s direct runtime dependency %s: %s",
					manifest.ID, dependency.ToPURL(manifest.PackageManager), err)
			}
		}
		lgr.Printf("\t\t%d direct runtime dependencies written", len(manifest.Runtime))

		for _, dependency := range manifest.Development {
			if err := writeDependency(ctx, lgr, client, keyspace, snapshot, manifest, dependency); err != nil {
				return fmt.Errorf("writing manifest %s direct development dependency %s: %s",
					manifest.ID, dependency.ToPURL(manifest.PackageManager), err)
			}
		}
		lgr.Printf("\t\t%d direct development dependencies written", len(manifest.Development))

		for _, dependency := range manifest.Transitives {
			if err := writeDependency(ctx, lgr, client, keyspace, snapshot, manifest, dependency); err != nil {
				return fmt.Errorf("writing manifest %s transitive dependency %s: %s",
					manifest.ID, dependency.ToPURL(manifest.PackageManager), err)
			}
		}
		lgr.Printf("\t\t%d transitive dependencies written", len(manifest.Transitives))

	}

	return nil
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

// TODO: use batches of INSERTs instead? use UPDATE instead?
func writeDependency(ctx context.Context, lgr *log.Logger, client *gocql.Session, keyspace string, sm Snapshot, mm Manifest, dep Dependency) error {
	// manifest_dependencies query
	query := fmt.Sprintf(`INSERT INTO %s.manifest_dependencies
	  (manifest_id, package_manager, namespace, name, version, snapshot_id, license, source_url, scope, relationship, runtime, development)
	  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)

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

	drQuery := fmt.Sprintf(`INSERT INTO %s.dependent_repositories
          (package_manager, namespace, name, version, owner_id, repository_id, license, source_url)
	  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, keyspace)
	if err := client.Query(drQuery).Bind(
		mm.PackageManager,
		dep.Namespace,
		dep.Name,
		dep.Version,
		sm.OwnerID,
		sm.RepositoryID,
		dep.License,
		dep.SourceURL).Exec(); err != nil {
		return err
	}

	countsQuery := fmt.Sprintf(`UPDATE %s.dependent_repository_counts SET used_by = used_by + 1
	  WHERE package_manager = ? AND namespace = ? AND name = ? AND version = ?`, keyspace)
	if err := client.Query(countsQuery).Bind(
		mm.PackageManager,
		dep.Namespace,
		dep.Name,
		dep.Version).Exec(); err != nil {
		return err
	}

	return nil
}

var tables = []string{
	`
CREATE TABLE IF NOT EXISTS %s.snapshots (
	// unique ID for the record
	id   uuid,

	// repo metadata
	owner_id int,
	repository_id int,
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
	owner_id int,
	repository_id int,

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
