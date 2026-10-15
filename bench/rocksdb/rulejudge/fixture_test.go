package rulejudge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The fixtures: synthetic manifests and results of both engines, as the bench
// workflow would leave them, that agree with their plan and with each other in
// everything but what a test changes.

var testSHA = strings.Repeat("ab", 20)

// cpuOf is the CPU model each architecture's builds ran on.
var cpuOf = map[string]string{DecidingArch: "Neoverse-N2", GuardArch: "AMD EPYC 7763 64-Core Processor"}

// exe is the sha256 of a binary: one for each engine on each architecture.
func exe(arch string, rocks bool) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s %v", arch, rocks))
	return hex.EncodeToString(sum[:])
}

// testStamp is a build stamp as build-rocksdb.sh writes one, for an architecture.
func testStamp(arch string) string {
	return "rocksdb_version=11.8.1\nportable=1\nmalloc_usable_size=off\nhost=Linux " + arch + "\n"
}

func setStamp(d map[string]string, text string) {
	sum := sha256.Sum256([]byte(text))
	d[StampKey], d[StampSHA256Key] = text, hex.EncodeToString(sum[:])
}

// testPlan is a plan of three queries over a stream of 1000 records.
func testPlan(t *testing.T) *runner.Plan {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	sd, err := spec.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var qs []runner.Query
	for i := range 3 {
		qs = append(qs, runner.Query{Group: "g", Rank: i, Age: runner.AgeNow, Expect: fmt.Sprintf("answer %d", i), Size: i + 1})
	}
	qd, err := runner.QueriesDigest(qs)
	if err != nil {
		t.Fatal(err)
	}
	return &runner.Plan{Spec: spec, SpecDigest: sd, Stream: runner.StreamInfo{Digest: "the stream", Records: 1000}, Queries: qs, QueriesDigest: qd, CacheBytes: 64 << 20}
}

// vals are a build's gates: G3 is the commit of every batch (its 99th percentile is
// the bucket bound above it), G2 the one retention (no batch after it is slow), G4
// the bytes per record, and the peak resident set in KiB.
type vals struct {
	g3, g2 time.Duration
	g4     float64
	rss    int64
}

// The values of the good set: RocksDB twice as fast to commit, alike on the rest.
var (
	pebbleVals = vals{g3: 2 * time.Millisecond, g2: 5 * time.Second, g4: 40, rss: 8 << 20}
	rocksVals  = vals{g3: time.Millisecond, g2: 5 * time.Second, g4: 40, rss: 8 << 20}
)

// newBuild is one build of cand on arch at the plan's window, repetition rep, with
// the read for repetition 1.
func newBuild(t *testing.T, plan *runner.Plan, arch, cand string, rep int, v vals) *Build {
	t.Helper()
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rocks := engineOf(cand) == "rocksdb"
	info := runner.BuildInfo{GoVersion: "go1.27.2", GOOS: "linux", GOARCH: goarchOf[arch], CGO: rocks, Revision: testSHA, Executable: exe(arch, rocks)}
	var h runner.Histogram
	for range 100 {
		h.Add(v.g3)
	}
	d := map[string]string{
		runner.SettleKey: "true", runner.RestKey: "false", runner.SyncKey: "false",
		runner.GoMemoryLimitKey: "none", runner.PostRetentionKey: "100",
	}
	engine := "pebble"
	if rocks {
		engine = "rocksdb"
		d[PerfLevelKey] = "disable"
		setStamp(d, testStamp(arch))
	}
	m := &runner.Manifest{
		Candidate: cand, Layout: "L", PlanDigest: digest, Stream: plan.Stream, Build: info, Describe: d,
		StatsCompacted: map[string]int64{"live_table_bytes": int64(v.g4 * float64(plan.Stream.Records))},
		Timing:         runner.Timing{Writes: h, Retains: []int64{int64(v.g2)}, PostRetention: [][]int64{{}}},
	}
	b := &Build{
		Job: Job{Job: runner.Job{
			SHA: testSHA, OS: "Linux", Arch: arch, Family: "rmae", Window: VerdictWindow, Candidate: cand, Rep: rep, PinsWindow: 2,
			Sync: "false", CPUModel: cpuOf[arch], NProc: 4, MemTotalKB: 16 << 20, RunID: "1", RunAttempt: "1", BinarySHA256: info.Executable,
		}, Engine: engine, MaxRSSKB: v.rss},
		Manifest: m,
	}
	if rep == 1 {
		r := &runner.Results{Candidate: cand, PlanDigest: digest, Build: info, Describe: maps.Clone(d)}
		for _, q := range plan.Queries {
			r.Queries = append(r.Queries, runner.QueryResult{Digest: q.Expect, Size: q.Size, Counters: map[string]int64{
				"read.neighbors.seeks": 2, "read.neighbors.steps": 7, "read.records_stepped": 9, runner.CounterBlockLoads: 3,
			}})
		}
		if rocks { // RocksDB counts its cold loads in its own unit
			for _, q := range r.Queries {
				q.Counters[runner.CounterBlockLoads] = 5
			}
		}
		b.Results = r
	}
	return b
}

// goodSet is a set the rule judges RocksDB decisively better on: L, RL and RL4 at
// k64a2l5m, 30 days, five repetitions each on both architectures.
func goodSet(t *testing.T) *Set {
	t.Helper()
	plan := testPlan(t)
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	s := &Set{Plans: map[string]*runner.Plan{digest: plan}}
	for _, arch := range []string{DecidingArch, GuardArch} {
		for _, cand := range []string{PebblePrefix + VerdictPolicy, RocksPrefix + VerdictPolicy, Rocks4Prefix + VerdictPolicy} {
			v := pebbleVals
			if engineOf(cand) == "rocksdb" {
				v = rocksVals
			}
			for rep := 1; rep <= 5; rep++ {
				s.Builds = append(s.Builds, newBuild(t, plan, arch, cand, rep, v))
			}
		}
	}
	return s
}

// buildOf is the build of cand on arch, repetition rep, in s.
func buildOf(t *testing.T, s *Set, arch, cand string, rep int) *Build {
	t.Helper()
	for _, b := range s.Builds {
		if b.Job.Arch == arch && b.Job.Candidate == cand && b.Job.Rep == rep {
			return b
		}
	}
	t.Fatalf("no repetition %d of %s on %s", rep, cand, arch)
	return nil
}

// writeSet writes a set as the bench workflow's artifacts under root: a plan, and a
// directory for each job with its job file, its manifest and, for a read, its
// results.
func writeSet(t *testing.T, s *Set, root string) {
	t.Helper()
	for _, p := range s.Plans {
		dir := filepath.Join(root, "plans")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := p.Save(filepath.Join(dir, planFile)); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range s.Builds {
		dir := filepath.Join(root, fmt.Sprintf("%s-%d-%s-%d", b.Job.Arch, b.Job.Window, strings.ReplaceAll(b.Job.Candidate, "/", "_"), b.Job.Rep))
		write := func(name string, v any) []byte {
			raw, err := jsonOf(v)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			return raw
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		write(runner.JobFile, b.Job)
		if b.Manifest == nil {
			continue
		}
		raw := write(runner.ManifestFile, b.Manifest)
		if b.Results != nil {
			r := *b.Results
			sum := sha256.Sum256(raw)
			r.ManifestDigest = hex.EncodeToString(sum[:])
			write(runner.ResultsFile, r)
		}
	}
}

func jsonOf(v any) ([]byte, error) { return json.MarshalIndent(v, "", " ") }
