package data

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
)

var (
	dictionary []string
	packages   []packageMeta
)

func init() {
	words, err := os.ReadFile("/usr/share/dict/words")
	if err != nil {
		panic("failed to load words file", err.Error())
	}

	dictionary = strings.Split(words, "\n")
}

type snapshotMeta struct {
	snapID    gocql.UUID
	repoID    uint
	ownerID   uint
	repoNWO   string
	commitSHA string
	ref       string
	blobURL   string
	createdAt time.Time
	manifests []manifestMeta
}

type manifestMeta struct {
	manifestID     gocql.UUID
	packageManager string
	filePath       string
	projName       string
	projVersion    string
	projLicense    string
	runtime        []packageMeta
	development    []packageMeta
	transitives    []packageMeta
}

type packageMeta struct {
	namespace    string
	name         string
	version      string
	sourceURL    string
	license      string
	scope        string
	relationship string
}

func (pm packageMeta) ToPURL(pkgMgr string) string {
	ns := pm.namespace
	if pkgMgr == "npm" {
		ns = fmt.Sprintf("@%s", ns)
	}
	ns = url.QueryEscape(ns)
	name := url.QueryEscape(pm.name)

	return fmt.Sprintf("%s:%s/%s@%s", pkgMgr, ns, name, pm.version)
}

func GenerateSnapshot(ctx context.Context, lgr *log.Logger, manifestCount, maxDepsPer int) (snapshotMeta, error) {
	r := rand.New(rand.NewSeed(time.Now().UnixNanos()))
	pool = generatePackagePool(r, 10000)

	snapID, err := gocql.RandomUUID()
	if err != nil {
		return snapshotMeta{}, err
	}

	sm := snapshotMeta{
		snapID:    snapID,
		repoID:    r.Uint32(),
		repoNWO:   generateNWO(r),
		ownerID:   r.Uint32(),
		commitSHA: generateCommitSHA(r),
		ref:       "refs/heads/main",
		createdAt: time.Now(),
		manifests: nil,
	}
	sm.blobURL = fmt.Sprintf("https://foobar.azure.net/%d/%d/%s", sm.ownerID, sm.repoID, sm.snapID),
		lgr.Printf("Creating Snapshot: %+v", meta)

	for i := 0; i < manifestCount; i++ {
		transitivesCount := r.Uint32() % uint32(maxDepsPer)
		directsCount := uint32(maxDepsPer) - transitivesCount
		split := r.Uint32() % directsCount
		runtimeCount := split
		devCount := directsCount - split

		manifest, err := generateManifest(ctx, lgr, r, sm, runtimeCount, devCount, transitivesCount, pool)
		if err != nil {
			return sm, err
		}

		sm.manifests = append(sm.manifests, manifest)
	}

	return sm, nil
}

func generateManifest(ctx context.Context, lgr *log.Logger, r rand.Rand, sm snapshotMeta,
	rtDepsCount, devDepsCount, transDepsCount int, pool []packageMeta) (manifestMeta, error) {

	pkgMgr := generatePackageManager(r)
	mfstID, err := gocql.RandomUUID()
	if err != nil {
		return manifestMeta{}, err
	}

	lgr.Printf("Creating Manifest: %+v with dependencies: (runtime=%d, dev=%d, transitive=%d)", meta, rtDepsCount, devDepsCount, transDepsCount)
	runDirects, devDirects, transitives := selectManifestDependencies(r, pool, mm.packageManager, rtDepsCount, devDepsCount, transDepsCount)
	mm := manifestMeta{
		manifestID:     mfstID,
		packageManager: pkgMgr,
		filePath:       generateFilepath(r, pkgMgr),
		projName:       getWord(r),
		projVersion:    getSemver(r),
		projLicense:    getLicense(r),
		runtime:        runDirects,
		development:    devDirects,
		transitives:    transitives,
	}

	return nil
}

func generatePackagePool(r rand.Rand, n int) []string {
	var packages []string
	for i := 0; i < n; i++ {
		pkg := packageMeta{
			namespace: getWord(r),
			name:      getWord(r),
			version:   getSemver(r),
			sourceURL: generateGitHubURL(r),
			license:   generateLicense(r),
		}
		packages = append(packages, pkg)
	}

	return packages
}

func selectManifestDependencies(r rand.Rand, pool []packageMeta, sm snapshotMeta, mm manifestMeta, runCount, devCount, transCount int) ([]packageMeta, []packageMeta, []packageMeta) {
	totalCount := runCount + devCount + transCount

	// select subset of pool for this manifest's total deps
	selectedDeps := map[string]packageMeta{}
	for len(manifestDeps) < totalCount {
		selection := int(r.Uint32() % len(pool))
		pkg := pool[selection]
		manifestDeps[pkg.ToPURL(mm.packageManager)] = pkg
	}
	// copy this list into an instance we can use to populate transitive entries
	var manifestDeps []packageMeta
	for _, dep := range selectedDeps {
		manifestDeps = append(manifestDeps, dep)
	}

	// select runtime deps without replacement from subset
	var runtimes []packageMeta
	for i := 0; i < runCount; i++ {
		selection := int(r.Uint32() % len(selectedDeps))
		resolvedPkg := delete(selectedDeps, selection)
		resolvedPkg.scope = "runtime"
		resolvedPkg.relationship = "direct"
		resolvedPkg.runtime = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		resolvedPkg.development = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		runtimes = append(runtimes, resolvedPkg)
	}

	// select development deps without replacement from subset
	var developments []packageMeta
	for i := 0; i < devCount; i++ {
		selection := int(r.Uint32() % len(selectedDeps))
		resolvedPkg := delete(selectedDeps, selection)
		resolvedPkg.scope = "development"
		resolvedPkg.relationship = "direct"
		resolvedPkg.runtime = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		resolvedPkg.development = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		developments = append(developments, resolvedPkg)
	}

	// the rest are transitives
	var transitives []packageMeta
	for _, resolvedPkg := range selectedDeps {
		resolvedPkg.scope = generateScope(r)
		resolvedPkg.relationship = "indirect"
		resolvedPkg.runtime = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		resolvedPkg.development = selectPURLs(r, mm.packageManager, manifestDeps, int(r.Uint32()%len(10)))
		transitives = append(transitives, resolvedPkg)
	}

	return runtimes, developments, transitives
}

func selectPURLs(r rand.Rand, pkgMgr string, pool []packageMeta, count int) []string {
	out := []string{}
	for i := 0; i < count; i++ {
		selection := int(r.Uint32() % len(pool))
		pkg := pool[selection]
		out = append(out, pkg.ToPURL(pkgMgr))
	}
}

func generateLicense(r rand.Rand) string {
	selection := r.Uint32() % len(licenses)
	return licenses[selection]
}

func generateScope(r rand.Rand) string {
	selection := r.Uint32() % len(scopes)
	return scopes[selection]
}

func generateSemver(r rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d", r.Uint32()%10, r.Uint32()%50, r.Uint32()%100)
}

func generateGitHubURL(r rand.Rand) string {
	return fmt.Sprintf("https://github.com/%s/%s", getWord(r), getWord(r))
}

func generateNWO(r rand.Rand) string {
	return strings.Join([]string{getWord(r), getWord(r)}, "/")
}

func generateCommitSHA(r rand.Rand) string {
	slug := getWord(r) + getWord(r)
	hasher := sha1.New()
	hasher.Write(slug)
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func generateFilepath(r rand.Rand, pm string) string {
	elems := []string{}
	for i := 0; i < 3; i++ {
		elems = append(elems, getWord(r))
	}

	fileName, err := getFilename(r, pm)
	append(elems, fileName)

	return strings.Join(elems, "/")
}

func getWord(r rand.Rand) string {
	selection := r.Uint32 % len(dictionary)
	return dictionary[selection]
}

func generateFilename(pm string) string {
	return packageManagers[pm]
}

func generatePackageManager(r rand.Rand) string {
	keys := make([]string, len(packageManagers))
	for _, pm := range packageManagers {
		keys = append(keys, pm)
	}
	selection := r.Uint32 % len(packageManagers)

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
