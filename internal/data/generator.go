package data

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var dictionary []string

// TODO: parameterize this + use distribution for more realism
const (
	runtimeCount     = 20
	developmentCount = 10
)

func init() {
	words, err := os.ReadFile("/usr/share/dict/words")
	if err != nil {
		panic("failed to load words file: " + err.Error())
	}

	dictionary = strings.Split(string(words), "\n")
}

type Snapshot struct {
	ID            gocql.UUID
	RepositoryID  uint
	OwnerID       uint
	RepositoryNWO string
	CommitSHA     string
	Ref           string
	BlobURL       string
	CreatedAt     time.Time
	Manifests     []Manifest
}

type Manifest struct {
	ID             gocql.UUID
	PackageManager string
	FilePath       string
	ProjectName    string
	ProjectVersion string
	ProjectLicense string
	Runtime        []Dependency
	Development    []Dependency
	Transitives    []Dependency
}

type Dependency struct {
	Namespace    string
	Name         string
	Version      string
	SourceURL    string
	License      string
	Scope        string
	Relationship string
	Runtime      []string // PURLs of transitive deps
	Development  []string // PURLs of transitive deps
}

func (pm Dependency) ToPURL(pkgMgr string) string {
	ns := pm.Namespace
	if pkgMgr == "npm" {
		ns = fmt.Sprintf("@%s", ns)
	}
	ns = url.QueryEscape(ns)
	name := url.QueryEscape(pm.Name)

	return fmt.Sprintf("%s:%s/%s@%s", pkgMgr, ns, name, pm.Version)
}

func GenerateSnapshot(ctx context.Context, lgr *log.Logger, manifestCount, maxDepsPer int) (Snapshot, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pool := generatePackagePool(r, 10000)

	snapID, err := gocql.RandomUUID()
	if err != nil {
		return Snapshot{}, err
	}

	sm := Snapshot{
		ID:            snapID,
		RepositoryID:  uint(r.Uint32()),
		RepositoryNWO: generateNWO(r),
		OwnerID:       uint(r.Uint32()),
		CommitSHA:     generateCommitSHA(r),
		Ref:           "refs/heads/main",
		CreatedAt:     time.Now(),
	}
	sm.BlobURL = fmt.Sprintf("https://foobar.azure.net/%d/%d/%s", sm.OwnerID, sm.RepositoryID, sm.ID)
	lgr.Printf("Creating Snapshot %s: %+v", sm.ID, sm)

	for i := 0; i < manifestCount; i++ {
		depsCount := int(r.Uint32() % uint32(maxDepsPer))
		var runtimeCount, devCount, transitivesCount int
		if depsCount > 0 {
			transitivesCount = int(r.Uint32() % uint32(depsCount))
			directsCount := int(depsCount) - transitivesCount
			split := int(r.Uint32() % uint32(directsCount))
			runtimeCount = int(split)
			devCount = directsCount - split
		}

		manifest, err := generateManifest(ctx, lgr, r, sm, runtimeCount, devCount, transitivesCount, pool)
		if err != nil {
			return sm, err
		}

		sm.Manifests = append(sm.Manifests, manifest)
	}

	return sm, nil
}

func generateManifest(ctx context.Context, lgr *log.Logger, r *rand.Rand, sm Snapshot,
	rtDepsCount, devDepsCount, transDepsCount int, pool []Dependency) (Manifest, error) {

	pkgMgr := generatePackageManager(r)
	mfstID, err := gocql.RandomUUID()
	if err != nil {
		return Manifest{}, err
	}

	lgr.Printf("Creating Manifest %s: with dependencies: (runtime=%d, dev=%d, transitive=%d)", mfstID, rtDepsCount, devDepsCount, transDepsCount)
	mm := Manifest{
		ID:             mfstID,
		PackageManager: pkgMgr,
		FilePath:       generateFilepath(r, pkgMgr),
		ProjectName:    getWord(r),
		ProjectVersion: generateSemver(r),
		ProjectLicense: generateLicense(r),
	}
	runDirects, devDirects, transitives := selectManifestDependencies(r, pool, sm, mm, rtDepsCount, devDepsCount, transDepsCount)
	mm.Runtime = runDirects
	mm.Development = devDirects
	mm.Transitives = transitives

	return mm, nil
}

func generatePackagePool(r *rand.Rand, n int) []Dependency {
	var packages []Dependency
	for i := 0; i < n; i++ {
		pkg := Dependency{
			Namespace: getWord(r),
			Name:      getWord(r),
			Version:   generateSemver(r),
			SourceURL: generateGitHubURL(r),
			License:   generateLicense(r),
		}
		packages = append(packages, pkg)
	}

	return packages
}

func selectManifestDependencies(r *rand.Rand, pool []Dependency, sm Snapshot, mm Manifest, runCount, devCount, transCount int) ([]Dependency, []Dependency, []Dependency) {
	totalCount := runCount + devCount + transCount

	// select subset of pool for this manifest's total deps
	selectedDeps := map[string]Dependency{}
	for len(selectedDeps) < totalCount {
		selection := int(r.Uint32() % uint32(len(pool)))
		pkg := pool[selection]
		selectedDeps[pkg.ToPURL(mm.PackageManager)] = pkg
	}
	// copy this list into an instance we can use to populate transitive entries
	var manifestDeps []Dependency
	for _, dep := range selectedDeps {
		manifestDeps = append(manifestDeps, dep)
	}

	// select runtime deps without replacement from subset
	var runtimes []Dependency
	for i := 0; i < runCount; i++ {
		resolvedPkg := selectWithoutReplacement(r, selectedDeps)
		resolvedPkg.Scope = "runtime"
		resolvedPkg.Relationship = "direct"
		resolvedPkg.Runtime = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%runtimeCount))
		resolvedPkg.Development = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%developmentCount))
		runtimes = append(runtimes, resolvedPkg)
	}

	// select development deps without replacement from subset
	var developments []Dependency
	for i := 0; i < devCount; i++ {
		resolvedPkg := selectWithoutReplacement(r, selectedDeps)
		resolvedPkg.Scope = "development"
		resolvedPkg.Relationship = "direct"
		resolvedPkg.Runtime = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%runtimeCount))
		resolvedPkg.Development = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%developmentCount))
		developments = append(developments, resolvedPkg)
	}

	// the rest are transitives
	var transitives []Dependency
	for _, resolvedPkg := range selectedDeps {
		resolvedPkg.Scope = generateScope(r)
		resolvedPkg.Relationship = "indirect"
		resolvedPkg.Runtime = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%runtimeCount))
		resolvedPkg.Development = selectPURLs(r, mm.PackageManager, manifestDeps, int(r.Uint32()%developmentCount))
		transitives = append(transitives, resolvedPkg)
	}

	return runtimes, developments, transitives
}

func selectWithoutReplacement(r *rand.Rand, pm map[string]Dependency) Dependency {
	keys := []string{}
	for k := range pm {
		keys = append(keys, k)
	}

	selection := int(r.Uint32() % uint32(len(keys)))
	key := keys[selection]
	value := pm[key]
	delete(pm, key)

	return value
}

func selectPURLs(r *rand.Rand, pkgMgr string, pool []Dependency, count int) []string {
	out := []string{}
	for i := 0; i < count; i++ {
		selection := int(r.Uint32() % uint32(len(pool)))
		pkg := pool[selection]
		out = append(out, pkg.ToPURL(pkgMgr))
	}

	return out
}

func generateLicense(r *rand.Rand) string {
	selection := r.Uint32() % uint32(len(licenses))
	return licenses[selection]
}

func generateScope(r *rand.Rand) string {
	selection := r.Uint32() % uint32(len(scopes))
	return scopes[selection]
}

func generateSemver(r *rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d", r.Uint32()%10, r.Uint32()%50, r.Uint32()%100)
}

func generateGitHubURL(r *rand.Rand) string {
	return fmt.Sprintf("https://github.com/%s/%s", getWord(r), getWord(r))
}

func generateNWO(r *rand.Rand) string {
	return strings.Join([]string{getWord(r), getWord(r)}, "/")
}

func generateCommitSHA(r *rand.Rand) string {
	slug := getWord(r) + getWord(r)
	hasher := sha1.New()
	hasher.Write([]byte(slug))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func generateFilepath(r *rand.Rand, pm string) string {
	elems := []string{}
	for i := 0; i < 3; i++ {
		elems = append(elems, getWord(r))
	}
	elems = append(elems, generateFilename(pm))

	return strings.Join(elems, "/")
}

func getWord(r *rand.Rand) string {
	selection := int(r.Uint32() % uint32(len(dictionary)))
	return dictionary[selection]
}

func generateFilename(pm string) string {
	return packageManagers[pm]
}

func generatePackageManager(r *rand.Rand) string {
	var keys []string
	for pm := range packageManagers {
		keys = append(keys, pm)
	}
	selection := int(r.Uint32() % uint32(len(packageManagers)))

	return keys[selection]
}

var (
	packageManagers = map[string]string{
		"npm":   "package.json",
		"pip":   "requirements.txt",
		"cargo": "Cargo.toml",
		"pub":   "pubspec.json",
		"maven": "POM.xml",
		"gem":   "Gemfile",
	}

	scopes = []string{"runtime", "development"}

	licenses = []string{
		"Apache-2.0",
		"MIT",
		"GPL-1.0-only",
		"APL-1.0-only",
		"MS-PL",
		"NASA-1.3",
		"OSL-1.0",
		"SPL-1.0",
	}
)
