package benchmarks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/elireisman/cass-dsapi/internal/data"

	"github.com/gocql/gocql"
)

var (
	ctx    context.Context
	client *gocql.Session
	r      *rand.Rand
	lgr    *log.Logger

	snapshots    []Snapshot
	manifests    []Manifest
	dependencies []Dependency
)

type Snapshot struct {
	ID           gocql.UUID
	OwnerID      uint
	RepositoryID uint
	NWO          string
	SourceURL    string
	Ref          string
	CommitOID    string
	CreatedAt    time.Time
	BlobURL      string
}

type Manifest struct {
	ID             gocql.UUID
	SnapshotID     gocql.UUID
	OwnerID        uint
	RepositoryID   uint
	Ref            string
	CommitOID      string
	BlobKey        string
	ManifestKey    string
	PackageManager string
	ProjectName    string
	ProjectVersion string
	ProjectLicense string
}

type Dependency struct {
	SnapshotID     gocql.UUID
	ManifestID     gocql.UUID
	PackageManager string
	Namespace      string
	Name           string
	Version        string
	License        string
	SourceURL      string
	Scope          string
	Relationship   string
	Runtime        []string
	Development    []string
}

func init() {
	var err error

	ctx = context.Background()
	lgr = log.Default()
	r = rand.New(rand.NewSource(time.Now().UnixNano()))

	client, err = data.CreateClient(ctx, lgr)
	if err != nil {
		panic(err.Error())
	}

	q := fmt.Sprintf(`SELECT id, owner_id, repository_id, nwo, source_url, ref, commit_oid, created_at, blob_url
	  FROM %s.snapshots LIMIT 1000`, data.Keyspace)
	scanner := client.Query(q).Iter().Scanner()
	for scanner.Next() {
		snapshot := Snapshot{}
		if err := scanner.Scan(
			&snapshot.ID,
			&snapshot.OwnerID,
			&snapshot.RepositoryID,
			&snapshot.NWO,
			&snapshot.SourceURL,
			&snapshot.Ref,
			&snapshot.CommitOID,
			&snapshot.CreatedAt,
			&snapshot.BlobURL); err != nil {
			panic("obtaining snapshot seed list: " + err.Error())
		}
		snapshots = append(snapshots, snapshot)
	}

	for len(manifests) < 5000 {
		selection := int(r.Uint32() % uint32(len(snapshots)))
		randoRepoID := snapshots[selection].RepositoryID

		q = fmt.Sprintf(`SELECT id, snapshot_id, owner_id, repository_id, ref, commit_oid, blob_key, manifest_key,
		  package_manager, project_name, project_version, project_license
		  FROM %s.manifests WHERE repository_id = ? LIMIT 100 ALLOW FILTERING`, data.Keyspace)

		scanner = client.Query(q).Bind(randoRepoID).Iter().Scanner()
		for scanner.Next() {
			manifest := Manifest{}
			if err := scanner.Scan(
				&manifest.ID,
				&manifest.SnapshotID,
				&manifest.OwnerID,
				&manifest.RepositoryID,
				&manifest.Ref,
				&manifest.CommitOID,
				&manifest.BlobKey,
				&manifest.ManifestKey,
				&manifest.PackageManager,
				&manifest.ProjectName,
				&manifest.ProjectVersion,
				&manifest.ProjectLicense); err != nil {
				panic("obtaining manifest seed list: " + err.Error())
			}
			manifests = append(manifests, manifest)
		}
	}

	for len(dependencies) < 5000 {
		selection := int(r.Uint32() % uint32(len(manifests)))
		randoManifestID := manifests[selection].ID

		q = fmt.Sprintf(`SELECT snapshot_id, manifest_id, package_manager, namespace, name,
		  version, license, source_url, scope, relationship, runtime, development
		  FROM %s.manifest_dependencies WHERE manifest_id = ? LIMIT 100 ALLOW FILTERING`, data.Keyspace)

		scanner = client.Query(q).Bind(randoManifestID).Iter().Scanner()
		for scanner.Next() {
			dependency := Dependency{}
			if err := scanner.Scan(
				&dependency.SnapshotID,
				&dependency.ManifestID,
				&dependency.PackageManager,
				&dependency.Namespace,
				&dependency.Name,
				&dependency.Version,
				&dependency.License,
				&dependency.SourceURL,
				&dependency.Scope,
				&dependency.Relationship,
				&dependency.Runtime,
				&dependency.Development); err != nil {
				panic("obtaining dependency seed list: " + err.Error())
			}
			dependencies = append(dependencies, dependency)
		}
	}
}

func BenchmarkCanonicalSnapshotQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT * FROM %s.snapshots
	  WHERE repository_id = ? AND ref = ?
	  ORDER BY created_at DESC
	  LIMIT 1`, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(snapshots)))
		repoID := snapshots[selection].RepositoryID
		ref := snapshots[selection].Ref

		if err := client.Query(q).Bind(repoID, ref).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkAllManifestsForSnapshotQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT * FROM %s.manifests
	  WHERE repository_id = ?
	    AND ref = ?
	    AND snapshot_id = ?
          `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(snapshots)))
		repoID := snapshots[selection].RepositoryID
		ref := snapshots[selection].Ref
		snapID := snapshots[selection].ID

		if err := client.Query(q).Bind(repoID, ref, snapID).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkManifestForSnapshotQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT * FROM %s.manifests
	  WHERE repository_id = ?
	    AND ref = ?
	    AND snapshot_id = ?
	    AND package_manager = ?
	    AND manifest_key = ?
          `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(manifests)))
		repoID := manifests[selection].RepositoryID
		ref := manifests[selection].Ref
		snapID := manifests[selection].SnapshotID
		pm := manifests[selection].PackageManager
		key := manifests[selection].ManifestKey

		if err := client.Query(q).Bind(repoID, ref, snapID, pm, key).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkAllDependenciesFromSnapshotManifestQuery(b *testing.B) {
	q := fmt.Sprintf(
		`SELECT * FROM %s.manifest_dependencies WHERE manifest_id = ?`,
		data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(manifests)))
		manifestID := manifests[selection].ID

		if err := client.Query(q).Bind(manifestID).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkOneDependencyAllVersionsFromSnapshotManifestQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT * FROM %s.manifest_dependencies
	  WHERE manifest_id = ?
	    AND package_manager = ?
	    AND namespace = ?
	    AND name = ?
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		manifestID := dependencies[selection].ManifestID
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name

		if err := client.Query(q).Bind(manifestID, pm, ns, name).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkPageOfRepositoriesDependingOnPackageQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT * FROM %s.dependent_repositories
	  WHERE package_manager = ?
	    AND namespace = ?
	    AND name = ?
	    AND version = ?
	  LIMIT 100
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name
		version := dependencies[selection].Version

		if err := client.Query(q).Bind(pm, ns, name, version).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkCountRepositoriesDependingOnPackageQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT COUNT(*) FROM %s.dependent_repositories
	  WHERE package_manager = ?
	    AND namespace = ?
	    AND name = ?
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name

		if err := client.Query(q).Bind(pm, ns, name).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkCountRepositoriesDependingOnPackageVersionQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT COUNT(*) FROM %s.dependent_repositories
	  WHERE package_manager = ?
	    AND namespace = ?
	    AND name = ?
	    AND version = ?
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name
		version := dependencies[selection].Version

		if err := client.Query(q).Bind(pm, ns, name, version).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkRepositoriesDependencyCountsOfPackageQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT SUM(used_by) FROM %s.dependent_repository_counts
	  WHERE package_manager = ?
	    AND namespace = ?
	    AND name = ?
	  LIMIT 1
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name

		if err := client.Query(q).Bind(pm, ns, name).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}
func BenchmarkRepositoriesDependencyCountsOfPackageVersionQuery(b *testing.B) {
	q := fmt.Sprintf(`
	  SELECT used_by FROM %s.dependent_repository_counts
	  WHERE package_manager = ?
	    AND namespace = ?
	    AND name = ?
	    AND version = ?
	  LIMIT 1
	  `, data.Keyspace)

	for n := 0; n < b.N; n++ {
		selection := int(r.Uint32() % uint32(len(dependencies)))
		pm := dependencies[selection].PackageManager
		ns := dependencies[selection].Namespace
		name := dependencies[selection].Name
		version := dependencies[selection].Version

		if err := client.Query(q).Bind(pm, ns, name, version).Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}
