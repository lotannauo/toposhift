package rulejudge

import (
	"bytes"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

const (
	rl  = RocksPrefix + VerdictPolicy
	rl4 = Rocks4Prefix + VerdictPolicy
	lk  = PebblePrefix + VerdictPolicy
)

// The good set is judged decisively better, with nothing that is not a result; and
// what it rests on is RocksDB's G3 at half Pebble's, and every other gate alike.
func TestTheGoodSetIsDecisive(t *testing.T) {
	t.Parallel()
	rep := Judge(goodSet(t))
	if rep.Verdict != VerdictDecisive || len(rep.Refused) > 0 || len(rep.NotResults) > 0 {
		t.Fatalf("verdict %q, refused %q, not results %q", rep.Verdict, rep.Refused, rep.NotResults)
	}
	if rep.Revision != testSHA {
		t.Errorf("revision %q", rep.Revision)
	}
	for _, c := range rep.Comparisons {
		want := Neither
		if c.Gate == GateG3 {
			want = Better
		}
		if c.Outcome != want {
			t.Errorf("%s on %s, %s: %s (%s), want %s", c.Candidate, c.Arch, c.Gate, c.Outcome, c.Why, want)
		}
	}
	var out bytes.Buffer
	Write(&out, rep)
	if !strings.Contains(out.String(), "VERDICT: "+string(VerdictDecisive)) {
		t.Errorf("the report says no verdict:\n%s", out.String())
	}
}

// Each of the rule's own checks of a RocksDB build (J3), broken in one build on the
// deciding architecture, makes that build void with a reason that names it, and
// the verdict not decided. (A build of another plan than its window's other builds
// is a set the rule refuses: see TestRefusals.)
func TestEveryRocksCheckVoidsTheBuild(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, want string
		rep        int
		change     func(b *Build)
	}{
		{"the plan of the read", "read from another plan", 1, func(b *Build) { b.Results.PlanDigest = "another" }},
		{"the number of results", "results for the plan's 3 queries", 1, func(b *Build) { b.Results.Queries = b.Results.Queries[:2] }},
		{"a wrong answer", "answers differ from the reference engine's", 1, func(b *Build) { b.Results.Mismatches = []string{"q"} }},
		{"a wrong answer before compaction", "before the compaction", 3, func(b *Build) { b.Manifest.UncompactedWrong = []string{"q"} }},
		{"an unstable query", "unstable", 1, func(b *Build) { b.Results.Unstable = []string{"q (cold)"} }},
		{"the stream", "another stream", 4, func(b *Build) { b.Manifest.Stream.Digest = "another" }},
		{"an untimed build", "untimed", 2, func(b *Build) { b.Manifest.Untimed = true }},
		{"an untimed read", "read by a binary a result may not come from", 1, func(b *Build) { b.Results.Untimed = true }},
		{"a build without cgo", "without cgo", 5, func(b *Build) { b.Manifest.Build.CGO = false }},
		{"another binary read", "built and read by different binaries", 1, func(b *Build) { b.Results.Build.Revision = strings.Repeat("cd", 20) }},
		{"the perf context in the build", "built with perf_level", 2, func(b *Build) { b.Manifest.Describe[PerfLevelKey] = "enable_count" }},
		{"the perf context in the read", "read with perf_level", 1, func(b *Build) { b.Results.Describe[PerfLevelKey] = "enable_count" }},
		{"an unsettled retention", "settle_tombstones", 2, func(b *Build) { b.Manifest.Describe[runner.SettleKey] = "false" }},
		{"a rest after retention", "rest_after_retention", 2, func(b *Build) { b.Manifest.Describe[runner.RestKey] = "true" }},
		{"another sync", "sync \"true\", not the Pebble builds' \"false\"", 2, func(b *Build) { b.Manifest.Describe[runner.SyncKey] = "true" }},
		{"no stamp", "no build stamp", 3, func(b *Build) { b.Manifest.Describe[StampKey], b.Manifest.Describe[StampSHA256Key] = "none", "none" }},
		{"a stamp without its sha256", "sha256 is not that of its text", 3, func(b *Build) { b.Manifest.Describe[StampSHA256Key] = strings.Repeat("0", 64) }},
		{"a stamp of a native build", "portable=\"0\"", 3, func(b *Build) {
			setStamp(b.Manifest.Describe, strings.Replace(testStamp(DecidingArch), "portable=1", "portable=0", 1))
		}},
		{"a stamp with malloc_usable_size", "malloc_usable_size=\"on\"", 3, func(b *Build) {
			setStamp(b.Manifest.Describe, strings.Replace(testStamp(DecidingArch), "malloc_usable_size=off", "malloc_usable_size=on", 1))
		}},
		{"a read with another stamp", "read with another build stamp", 1, func(b *Build) { b.Results.Describe[StampSHA256Key] = "other" }},
		{"another memory limit", "go_memory_limit \"1000000000\", not \"none\"", 4, func(b *Build) { b.Manifest.Describe[runner.GoMemoryLimitKey] = "1000000000" }},
		{"another answer than L's", "answers differ from L/k64a2l5m's", 1, func(b *Build) { b.Results.Queries[1].Digest = "another" }},
		{"another comparable counter than L's", "count differently from L/k64a2l5m's, first: query 2, read.neighbors.steps 8 against 7", 1, func(b *Build) {
			b.Results.Queries[2].Counters["read.neighbors.steps"] = 8
		}},
		{"a read only of another repetition", "not read", 1, func(b *Build) { b.Results = nil }},
	} {
		for _, cand := range []string{rl, rl4} {
			s := goodSet(t)
			c.change(buildOf(t, s, DecidingArch, cand, c.rep))
			rep := Judge(s)
			if rep.Verdict != VerdictNotDecided {
				t.Errorf("%s of %s: verdict %q", c.name, cand, rep.Verdict)
			}
			if !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.HasPrefix(s, cand) && strings.Contains(s, c.want) }) {
				t.Errorf("%s of %s: no reason naming %q: %q", c.name, cand, c.want, rep.NotResults)
			}
		}
	}
}

// The same breaks on the guarding architecture leave the verdict not decided too:
// a void build is not a value, wherever it is.
func TestAVoidBuildOnTheGuardIsNotAResult(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	buildOf(t, s, GuardArch, rl, 1).Results.Unstable = []string{"q"}
	rep := Judge(s)
	if !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, "on x86_64, repetition 1: 1 queries are unstable") }) {
		t.Errorf("not results %q", rep.NotResults)
	}
	// x86 not decided does not block.
	if rep.Verdict != VerdictDecisive {
		t.Errorf("verdict %q: %q", rep.Verdict, rep.Why)
	}
}

// The RocksDB builds of an architecture record one stamp: two libraries, each
// stamped as the rule accepts, leave the verdict not decided.
func TestTheStampIsOneOnAnArchitecture(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	setStamp(buildOf(t, s, DecidingArch, rl4, 2).Manifest.Describe, testStamp(DecidingArch)+"cxx=another compiler\n")
	rep := Judge(s)
	if rep.Verdict != VerdictNotDecided || !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, "record 2 build stamps") }) {
		t.Errorf("verdict %q, not results %q", rep.Verdict, rep.NotResults)
	}
	s = goodSet(t)
	setStamp(buildOf(t, s, GuardArch, rl, 2).Manifest.Describe, testStamp(GuardArch)+"cxx=another compiler\n")
	if rep := Judge(s); !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, "on x86_64 record 2 build stamps") }) || rep.Verdict != VerdictDecisive {
		t.Errorf("two stamps on x86_64: verdict %q, not results %q", rep.Verdict, rep.NotResults)
	}
}

// The builds of an engine on an architecture are of one binary.
func TestOneBinaryPerEngineAndArchitecture(t *testing.T) {
	t.Parallel()
	for _, cand := range []string{lk, rl} {
		s := goodSet(t)
		b := buildOf(t, s, DecidingArch, cand, 3)
		b.Manifest.Build.Executable = strings.Repeat("e", 64)
		if rep := Judge(s); rep.Verdict != VerdictNotDecided || !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, "ran 2 binaries") }) {
			t.Errorf("%s: verdict %q, not results %q", cand, rep.Verdict, rep.NotResults)
		}
	}
}

// RL's Go memory limit is Pebble's less the plan's cache and four memtables: a set
// where Pebble ran under 11 GiB needs RL under 11 GiB less 320 MiB.
func TestTheMemoryLimitIsPebblesLessTheCharge(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	for _, b := range s.Builds {
		limit := "11811160064" // 11 GiB
		if engineOf(b.Job.Candidate) == "rocksdb" {
			limit = "11475615744" // less 64 MiB of cache and four memtables of 64 MiB
		}
		b.Manifest.Describe[runner.GoMemoryLimitKey] = limit
		if b.Results != nil {
			b.Results.Describe[runner.GoMemoryLimitKey] = limit
		}
	}
	if rep := Judge(s); rep.Verdict != VerdictDecisive {
		t.Fatalf("verdict %q: %q %q", rep.Verdict, rep.Why, rep.NotResults)
	}
	buildOf(t, s, DecidingArch, rl, 2).Manifest.Describe[runner.GoMemoryLimitKey] = "11811160064"
	if rep := Judge(s); rep.Verdict != VerdictNotDecided || !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, `go_memory_limit "11811160064", not "11475615744"`) }) {
		t.Errorf("RL under Pebble's own limit: verdict %q, not results %q", rep.Verdict, rep.NotResults)
	}
}

// The cold counters are not comparable: RocksDB's block loads, in its own unit,
// differ from Pebble's in every query of the good set, and void nothing.
func TestTheColdCountersAreNotCompared(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	r := buildOf(t, s, DecidingArch, rl, 1).Results
	for _, q := range r.Queries {
		q.Counters[runner.CounterColdCacheBytes] = 1 << 20
		q.Counters[runner.CounterAllocs] = 7
		q.Counters["read.neighbors.rocks.block_read_count"] = 4
		q.Counters["read.neighbors.block_bytes"] = 4096
	}
	if rep := Judge(s); rep.Verdict != VerdictDecisive {
		t.Errorf("verdict %q: %q", rep.Verdict, rep.NotResults)
	}
}

// J2: the runner's check is given each window's Pebble set on each architecture, and
// never a RocksDB build. A reason it gives leaves that scope not decided.
func TestTheRunnerChecksThePebbleSetAlone(t *testing.T) {
	t.Parallel()
	var sets [][]string
	record := func(plan *runner.Plan, cs []*runner.Candidate, allowUntimed bool) []string {
		var names []string
		for _, c := range cs {
			names = append(names, c.Results.Candidate, c.Manifest.Candidate)
		}
		sets = append(sets, names)
		if allowUntimed {
			return []string{"untimed allowed"}
		}
		return runner.Check(plan, cs, allowUntimed)
	}
	rep := judgeWith(goodSet(t), record)
	if rep.Verdict != VerdictDecisive {
		t.Errorf("verdict %q: %q %q", rep.Verdict, rep.Why, rep.NotResults)
	}
	if len(sets) != 2 {
		t.Errorf("the runner's check was given %d sets, want one per architecture: %q", len(sets), sets)
	}
	for _, names := range sets {
		for _, n := range names {
			if engineOf(n) != "pebble" {
				t.Errorf("the runner's check was given %s: %q", n, names)
			}
		}
	}

	refuse := func(*runner.Plan, []*runner.Candidate, bool) []string { return []string{"a reason"} }
	rep = judgeWith(goodSet(t), refuse)
	if rep.Verdict != VerdictNotDecided || !slices.Contains(rep.NotResults, "the Pebble builds at 30 days on aarch64: a reason") {
		t.Errorf("a Pebble set the runner refuses: verdict %q, not results %q", rep.Verdict, rep.NotResults)
	}
}

// The Pebble builds are checked as the RocksDB ones are, where the rule says the
// same of both: a Pebble build that breaks one is void, and the verdict not decided.
func TestEveryPebbleCheckVoidsTheBuild(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, want string
		rep        int
		change     func(b *Build)
	}{
		{"a build with cgo", "built with cgo", 4, func(b *Build) { b.Manifest.Build.CGO = true }},
		{"an untimed build", "untimed", 5, func(b *Build) { b.Manifest.Untimed = true }},
		{"an unsettled retention", "settle_tombstones", 2, func(b *Build) { b.Manifest.Describe[runner.SettleKey] = "false" }},
		{"two memory limits", "ran under 2 Go memory limits", 2, func(b *Build) { b.Manifest.Describe[runner.GoMemoryLimitKey] = "11811160064" }},
		{"no read", "L/k64a2l5m has no read of repetition 1", 1, func(b *Build) { b.Results = nil }},
	} {
		s := goodSet(t)
		c.change(buildOf(t, s, DecidingArch, lk, c.rep))
		rep := Judge(s)
		if rep.Verdict != VerdictNotDecided || !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, c.want) }) {
			t.Errorf("%s: verdict %q, not results %q", c.name, rep.Verdict, rep.NotResults)
		}
	}
}

// A job that left no manifest (killed for memory, or timed out, perhaps before its
// resident set was read) is a cell with no result: on the deciding architecture the
// verdict is not decided; on the guard it does not block.
func TestAJobWithoutAManifestIsNotDecided(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	b := buildOf(t, s, DecidingArch, rl, 3)
	b.Manifest, b.Job.MaxRSSKB = nil, 0
	rep := Judge(s)
	if len(rep.Refused) > 0 || rep.Verdict != VerdictNotDecided || !slices.ContainsFunc(rep.NotResults, func(s string) bool { return strings.Contains(s, "repetition 3 left no manifest") }) {
		t.Errorf("refused %q, verdict %q, not results %q", rep.Refused, rep.Verdict, rep.NotResults)
	}
	s = goodSet(t)
	buildOf(t, s, GuardArch, rl4, 5).Manifest = nil
	if rep := Judge(s); rep.Verdict != VerdictDecisive {
		t.Errorf("a job on x86_64 without a manifest: verdict %q", rep.Verdict)
	}
}

// A job with a manifest whose job file records no peak resident set is refused,
// one or all of them: a workflow that does not write max_rss_kb is an input error,
// not a set whose every verdict is not decided.
func TestAJobWithoutItsResidentSetIsRefused(t *testing.T) {
	t.Parallel()
	s := goodSet(t)
	buildOf(t, s, DecidingArch, lk, 2).Job.MaxRSSKB = 0
	if rep := Judge(s); rep.Verdict != "" || !slices.Contains(rep.Refused, "1 jobs with a manifest record no max_rss_kb in job.json, the peak resident set the rule guards on (the first: repetition 2 of L/k64a2l5m at 30 days on aarch64)") {
		t.Errorf("one job without its resident set: verdict %q, refused %q", rep.Verdict, rep.Refused)
	}
	s = goodSet(t)
	for _, b := range s.Builds {
		b.Job.MaxRSSKB = 0
	}
	if rep := Judge(s); rep.Verdict != "" || !slices.ContainsFunc(rep.Refused, func(s string) bool { return strings.HasPrefix(s, "30 jobs with a manifest record no max_rss_kb") }) {
		t.Errorf("no job with its resident set: verdict %q, refused %q", rep.Verdict, rep.Refused)
	}
}

// A set the rule does not judge is refused, with every reason: no build, a candidate
// it does not compare, builds of two commits, a repetition twice, fewer than three
// repetitions, a plan that is not among the inputs, and a window built from two (a
// job without its resident set: TestAJobWithoutItsResidentSetIsRefused).
func TestRefusals(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, want string
		change     func(t *testing.T, s *Set)
	}{
		{"no build", "holds no build", func(_ *testing.T, s *Set) { s.Builds = nil }},
		{"another candidate", "M/crdb1 is not a candidate the rule compares", func(t *testing.T, s *Set) {
			buildOf(t, s, DecidingArch, lk, 5).Job.Candidate = "M/crdb1"
		}},
		{"no policy", "RL/ is not a candidate", func(t *testing.T, s *Set) { buildOf(t, s, DecidingArch, rl, 5).Job.Candidate = RocksPrefix }},
		{"two commits", "of 2 commits", func(t *testing.T, s *Set) { buildOf(t, s, GuardArch, rl, 5).Job.SHA = strings.Repeat("cd", 20) }},
		{"a repetition twice", "two jobs are repetition 4 of RL4/k64a2l5m at 30 days on aarch64", func(t *testing.T, s *Set) {
			buildOf(t, s, DecidingArch, rl4, 5).Job.Rep = 4
		}},
		{"two repetitions", "RL/k64a2l5m at 30 days on x86_64 has 2 repetitions, fewer than 3", func(_ *testing.T, s *Set) {
			s.Builds = slices.DeleteFunc(s.Builds, func(b *Build) bool { return b.Job.Arch == GuardArch && b.Job.Candidate == rl && b.Job.Rep > 2 })
		}},
		{"no plan", "is not among the inputs", func(_ *testing.T, s *Set) { s.Plans = nil }},
		{"two plans", "made from 2 plans", func(t *testing.T, s *Set) { buildOf(t, s, DecidingArch, lk, 5).Manifest.PlanDigest = "another" }},
		{"a RocksDB build of another plan", "made from 2 plans", func(t *testing.T, s *Set) { buildOf(t, s, GuardArch, rl, 2).Manifest.PlanDigest = "another" }},
	} {
		s := goodSet(t)
		c.change(t, s)
		rep := Judge(s)
		if rep.Verdict != "" || !slices.ContainsFunc(rep.Refused, func(s string) bool { return strings.Contains(s, c.want) }) {
			t.Errorf("%s: verdict %q, refused %q", c.name, rep.Verdict, rep.Refused)
		}
		var out bytes.Buffer
		Write(&out, rep)
		if !strings.Contains(out.String(), "REFUSED") || strings.Contains(out.String(), "VERDICT") {
			t.Errorf("%s: the report of a refused set:\n%s", c.name, out.String())
		}
	}
}

// Written out as the bench workflow's artifacts, the good set loads and is judged
// as it is in memory; a database's directory is never entered, and a manifest
// without a job file, or a job file that says other than its manifest, is refused.
func TestLoad(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeSet(t, goodSet(t), root)
	// A database under a build holds nothing the judge reads, whatever its files.
	db := filepath.Join(root, "aarch64-30-RL_k64a2l5m-1", runner.DBDir)
	if err := os.MkdirAll(db, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(db, runner.ManifestFile), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Load([]string{root, filepath.Join(root, "plans")})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Builds) != 30 || len(s.Plans) != 1 {
		t.Fatalf("%d builds and %d plans", len(s.Builds), len(s.Plans))
	}
	reads := 0
	for _, b := range s.Builds {
		if b.Results != nil {
			reads++
		}
	}
	if reads != 6 {
		t.Errorf("%d reads, want 6", reads)
	}
	if rep := Judge(s); rep.Verdict != VerdictDecisive {
		t.Errorf("verdict %q: %q %q %q", rep.Verdict, rep.Refused, rep.Why, rep.NotResults)
	}

	// A job that left no manifest is loaded as one.
	if err := os.Remove(filepath.Join(root, "x86_64-30-RL4_k64a2l5m-3", runner.ManifestFile)); err != nil {
		t.Fatal(err)
	}
	if s, err := Load([]string{root}); err != nil || slices.IndexFunc(s.Builds, func(b *Build) bool { return b.Manifest == nil }) < 0 {
		t.Errorf("a job without a manifest: %v", err)
	}

	for name, change := range map[string]func(dir string) error{
		"a manifest without a job file": func(dir string) error { return os.Remove(filepath.Join(dir, runner.JobFile)) },
		"a job of another candidate": func(dir string) error {
			return rewriteJob(dir, func(j *Job) { j.Candidate = rl4 })
		},
		"a job of another binary": func(dir string) error {
			return rewriteJob(dir, func(j *Job) { j.BinarySHA256 = strings.Repeat("e", 64) })
		},
		"a job of another engine": func(dir string) error { return rewriteJob(dir, func(j *Job) { j.Engine = "pebble" }) },
		"results of another manifest": func(dir string) error {
			return os.WriteFile(filepath.Join(dir, runner.ManifestFile), []byte(`{"Candidate":"RL/k64a2l5m"}`), 0o644)
		},
	} {
		root := t.TempDir()
		writeSet(t, goodSet(t), root)
		if err := change(filepath.Join(root, "aarch64-30-RL_k64a2l5m-1")); err != nil {
			t.Fatal(err)
		}
		if _, err := Load([]string{root}); err == nil {
			t.Errorf("%s is loaded", name)
		}
	}
	if _, err := Load([]string{filepath.Join(root, "absent")}); err == nil {
		t.Error("a root that is not there is loaded")
	}
}

func rewriteJob(dir string, change func(*Job)) error {
	b, err := loadBuild(dir)
	if err != nil {
		return err
	}
	change(&b.Job)
	raw, err := jsonOf(b.Job)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, runner.JobFile), raw, 0o644)
}

// The report lists every build, and each comparison of every gate.
func TestTheReportListsEveryBuildAndComparison(t *testing.T) {
	t.Parallel()
	rep := Judge(goodSet(t))
	if len(rep.Builds) != 30 || len(rep.Comparisons) != 2*2*len(Gates) {
		t.Errorf("%d builds and %d comparisons", len(rep.Builds), len(rep.Comparisons))
	}
	var out bytes.Buffer
	Write(&out, rep)
	for _, want := range []string{"COMPARISONS", "BUILDS:", "RL4/k64a2l5m", "Neoverse-N2", "8192 MiB"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report has no %q", want)
		}
	}
	if keys := slices.Sorted(maps.Keys(rep.Builds[0].Values)); !slices.Equal(keys, []string{GateG2, GateG3, GateG4, GateRSS}) {
		t.Errorf("a build's values: %v", keys)
	}
}
