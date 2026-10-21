package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// Nothing here is a measurement: the artifacts are written by the test, and every
// duration in them is a number it chose. The only timed read is one of a tiny store, run
// for its output and for the files it leaves, never for a figure.

// The counts of a timed read are checked before anything is read, and a timed read prints
// counts and never a duration.
func TestReadTimedFlags(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	bin := binary(t)
	out := filepath.Join(t.TempDir(), "out")
	if _, stderr, err := run(t, bin, "plan", "-preset", "tiny", "-min-non-empty", "0", "-out", out); err != nil {
		t.Fatalf("plan: %v\n%s", err, stderr)
	}
	if _, stderr, err := run(t, bin, "build", "-untimed", "-candidate", "L/off", "-out", out); err != nil {
		t.Fatalf("build: %v\n%s", err, stderr)
	}
	timings := filepath.Join(out, "L_off", runner.TimingsFile)
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-timed", "-timed-reps", "30"}, "-timed-reps 30: an odd number"},
		{[]string{"-timed", "-timed-reps", "0"}, "-timed-reps 0: an odd number"},
		{[]string{"-timed", "-timed-reps", "1"}, "-timed-reps 1: an odd number"},
		{[]string{"-timed", "-timed-reps", "-3"}, "-timed-reps -3: an odd number"},
		{[]string{"-timed", "-timed-warmup", "0"}, "-timed-warmup 0: at least 1"},
		{[]string{"-timed-reps", "31"}, "-timed-reps is for -timed"},
		{[]string{"-timed-warmup", "3", "-timed-reps", "31"}, "-timed-reps and -timed-warmup is for -timed"},
	} {
		_, stderr, err := run(t, bin, append([]string{"read", "-untimed", "-candidate", "L/off", "-out", out}, c.args...)...)
		if err == nil || !strings.Contains(stderr, c.want) {
			t.Errorf("read %v: %v\n%s", c.args, err, stderr)
		}
		if _, err := os.Stat(timings); err == nil {
			t.Errorf("read %v wrote timings", c.args)
		}
		if _, err := os.Stat(filepath.Join(out, "L_off", runner.ResultsFile)); err == nil {
			t.Errorf("read %v read the database", c.args)
		}
	}

	// An uninstrumented binary with no version stamp is refused as a read is: -timed does
	// not lift the guard.
	if _, stderr, err := run(t, bin, "read", "-candidate", "L/off", "-timed", "-out", out); err == nil || !strings.Contains(stderr, "version control stamp") {
		t.Errorf("a timed read by a binary with no stamp: %v\n%s", err, stderr)
	}

	stdout, stderr, err := run(t, bin, "read", "-untimed", "-candidate", "L/off", "-timed", "-timed-warmup", "1", "-timed-reps", "3", "-out", out)
	if err != nil {
		t.Fatalf("a timed read: %v\n%s", err, stderr)
	}
	if !regexp.MustCompile(`read L/off: \d+ queries timed, 1 warm-ups and 3 timed asks each, \d+ not warm`).MatchString(stderr) {
		t.Errorf("the read does not say what it timed:\n%s", stderr)
	}
	var tm runner.ReadTimings
	b, err := os.ReadFile(timings)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &tm); err != nil {
		t.Fatal(err)
	}
	if !tm.Untimed || tm.Reps != 3 || tm.Warmup != 1 || len(tm.Queries) == 0 || len(tm.Queries[0].Ns) != 3 {
		t.Errorf("timings %+v", tm)
	}
	// No duration reaches a log: neither a number from the file, nor a unit after a number.
	seen := stdout + stderr
	units := regexp.MustCompile(`\d(ns|µs|us|ms|s)\b`)
	for _, line := range strings.Split(seen, "\n") {
		// The progress line begins with the time of day.
		line = regexp.MustCompile(`^\d\d:\d\d:\d\d `).ReplaceAllString(line, "")
		if units.MatchString(line) {
			t.Errorf("a line with a duration in its unit: %q", line)
		}
	}
	for _, q := range tm.Queries {
		for _, ns := range q.Ns {
			if ns >= 1000 && regexp.MustCompile(`\b`+strconv.FormatInt(ns, 10)+`\b`).MatchString(seen) {
				t.Errorf("the duration %d of %s is in the output", ns, q.Name)
			}
		}
	}
}

// What the pins and the queries are does not depend on the block cache. A plan at the
// product's cache therefore asks G1's pinned queries, with the same answers, and
// only its digest, which covers the cache, is another. The pins themselves name the
// scenario they were chosen from, which has the cache in it, so pins made at one cache
// do not make a plan at the other.
func TestPinsAndQueriesDoNotDependOnTheCache(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	bin := binary(t)
	family := []string{"-preset", "tiny", "-events-per-second", "0.05", "-batch", "40", "-min-non-empty", "0"}
	made := map[string]struct {
		pins *runner.Pins
		plan *runner.Plan
	}{}
	for name, extra := range map[string][]string{"default": nil, "product": {"-cache-mb", "256"}} {
		dir := t.TempDir()
		args := append(append([]string{"pins", "-out", dir}, family...), extra...)
		args = append(args, "-window", "1")
		if _, stderr, err := run(t, bin, args...); err != nil {
			t.Fatalf("%s pins: %v\n%s", name, err, stderr)
		}
		args = append(append([]string{"plan", "-out", filepath.Join(dir, "R1")}, family...), extra...)
		args = append(args, "-window", "1", "-pins", filepath.Join(dir, "pins.json"), "-full")
		if _, stderr, err := run(t, bin, args...); err != nil {
			t.Fatalf("%s plan: %v\n%s", name, err, stderr)
		}
		var pins runner.Pins
		b, err := os.ReadFile(filepath.Join(dir, "pins.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &pins); err != nil {
			t.Fatal(err)
		}
		plan, err := runner.LoadPlan(filepath.Join(dir, "R1", "plan.json"))
		if err != nil {
			t.Fatal(err)
		}
		made[name] = struct {
			pins *runner.Pins
			plan *runner.Plan
		}{&pins, plan}
	}
	a, b := made["default"], made["product"]
	if a.plan.CacheBytes != 64<<20 || b.plan.CacheBytes != 256<<20 {
		t.Fatalf("the plans' caches are %d and %d bytes", a.plan.CacheBytes, b.plan.CacheBytes)
	}
	ja, _ := json.Marshal(a.pins.Classes)
	jb, _ := json.Marshal(b.pins.Classes)
	if !bytes.Equal(ja, jb) {
		t.Error("the pinned prefixes differ between the two caches")
	}
	if a.plan.QueriesDigest != b.plan.QueriesDigest {
		t.Errorf("the queries' digests differ: %s, %s", a.plan.QueriesDigest, b.plan.QueriesDigest)
	}
	if a.plan.Stream.Digest != b.plan.Stream.Digest {
		t.Errorf("the streams' digests differ: %s, %s", a.plan.Stream.Digest, b.plan.Stream.Digest)
	}
	qa, _ := json.Marshal(a.plan.Queries)
	qb, _ := json.Marshal(b.plan.Queries)
	if !bytes.Equal(qa, qb) {
		t.Error("the queries or their expected answers differ between the two caches")
	}
	da, _ := a.plan.Digest()
	db, _ := b.plan.Digest()
	if da == db {
		t.Error("the plans have the same digest: it does not cover the cache")
	}
	if a.pins.Source == b.pins.Source {
		t.Error("the pins of the two caches name the same scenario")
	}
}

// A copy of the pins of the default cache does not make a plan at the product's.
func TestPinsMadeAtAnotherCacheAreRefused(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	bin := binary(t)
	family := []string{"-preset", "tiny", "-events-per-second", "0.05", "-batch", "40", "-min-non-empty", "0"}
	dir := t.TempDir()
	if _, stderr, err := run(t, bin, append(append([]string{"pins", "-out", dir}, family...), "-window", "1")...); err != nil {
		t.Fatalf("pins: %v\n%s", err, stderr)
	}
	args := append(append([]string{"plan", "-out", filepath.Join(dir, "R1")}, family...), "-cache-mb", "256", "-window", "1", "-pins", filepath.Join(dir, "pins.json"), "-full")
	if _, stderr, err := run(t, bin, args...); err == nil || !strings.Contains(stderr, "another scenario") {
		t.Errorf("a plan at 256 MiB from pins made at 64 MiB: %v\n%s", err, stderr)
	}
}

// ---------------------------------------------------------------------------------
// The judge's command, on artifacts written by the test.

const (
	rtCommit = "abababababababababababababababababababab"
	rtBase   = "L/k64a2l5m"
	rtM      = "M/crdb1"
	rtGroup  = "service<- hot-records neighbors"
)

// rtJob is one job's artifact.
type rtJob struct {
	cand, run, commit string
	rep               int
	scale             int64
	cacheBytes        int64
	notWarm           bool
}

// writeTimedRun writes a plan of one group of three queries and the artifacts of the
// jobs under a new directory, and returns the plans' directory and the artifacts'.
func writeTimedRun(t *testing.T, jobs []rtJob) (plans, artifacts string) {
	t.Helper()
	root := t.TempDir()
	plans, artifacts = filepath.Join(root, "plans"), filepath.Join(root, "artifacts")
	spec := runner.DefaultSpec(workload.Tiny())
	specDigest, err := spec.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var queries []runner.Query
	for i := range 3 {
		queries = append(queries, runner.Query{Group: rtGroup, Rank: i, Op: runner.OpNeighbors, Age: runner.AgeNow, Expect: "x"})
	}
	qd, err := runner.QueriesDigest(queries)
	if err != nil {
		t.Fatal(err)
	}
	plan := &runner.Plan{Spec: spec, SpecDigest: specDigest, Stream: runner.StreamInfo{Digest: "s"}, Queries: queries, QueriesDigest: qd, CacheBytes: runner.ReadTimingCacheBytes}
	if err := os.MkdirAll(filepath.Join(plans, "R7"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := plan.Save(filepath.Join(plans, "R7", "plan.json")); err != nil {
		t.Fatal(err)
	}
	planDigest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	writeJSON := func(path string, v any) []byte {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		b, err := json.MarshalIndent(v, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return append(b, '\n')
	}
	for _, j := range jobs {
		if j.run == "" {
			j.run = "100"
		}
		if j.commit == "" {
			j.commit = rtCommit
		}
		if j.cacheBytes == 0 {
			j.cacheBytes = runner.ReadTimingCacheBytes
		}
		exec := strings.Repeat("3", 64)
		var h runner.Histogram
		for range 10 {
			h.Add(time.Millisecond)
		}
		m := &runner.Manifest{
			Candidate: j.cand, PlanDigest: planDigest, Stream: runner.StreamInfo{Digest: "s"},
			Build:    runner.BuildInfo{GoVersion: "go1.27.2", GOOS: "linux", GOARCH: "amd64", Revision: j.commit, Executable: exec},
			Describe: map[string]string{"cache_bytes": strconv.FormatInt(j.cacheBytes, 10)}, StatsCompacted: map[string]int64{}, Timing: runner.Timing{Writes: h},
		}
		dir := filepath.Join(artifacts, fmt.Sprintf("bench-rmae-R7-%s-rep%d-%s", strings.ReplaceAll(j.cand, "/", "_"), j.rep, j.run))
		mb := writeJSON(filepath.Join(dir, runner.ManifestFile), m)
		sum := sha256.Sum256(mb)
		md := hex.EncodeToString(sum[:])
		writeJSON(filepath.Join(dir, runner.JobFile), runner.Job{
			SHA: j.commit, OS: "ubuntu-24.04", Arch: "x86_64", Family: "rmae", Window: 7, Candidate: j.cand, Rep: j.rep, PinsWindow: 2, Sync: "false",
			CPUModel: "Test X86 A", NProc: 4, MemTotalKB: 16 << 20, RunID: j.run, RunAttempt: "1", BinarySHA256: exec,
		})
		writeJSON(filepath.Join(dir, runner.ResultsFile), &runner.Results{
			Candidate: j.cand, PlanDigest: planDigest, ManifestDigest: md, Describe: map[string]string{"cache_bytes": strconv.FormatInt(j.cacheBytes, 10)},
		})
		t7 := &runner.ReadTimings{
			Version: runner.ReadTimingsVersion, Candidate: j.cand, PlanDigest: planDigest, ManifestDigest: md,
			Build: runner.BuildInfo{Executable: exec, Revision: j.commit}, GOMAXPROCS: 4, NumCPU: 4, GoMemLimit: math.MaxInt64,
			CacheBytes: j.cacheBytes, Warmup: 3, Reps: 31, Readers: 1, WarmCounter: runner.PebbleWarmCounter,
		}
		for i, q := range queries {
			qt := runner.QueryTiming{Name: q.Name(), Group: q.Group, Age: q.Age, Op: q.Op, Digest: "x", Warm: !j.notWarm || i != 1}
			for k := range 31 {
				qt.Ns = append(qt.Ns, (int64(10+i)*1000+int64(j.rep)*100+int64(k))*j.scale/1000)
			}
			t7.Queries = append(t7.Queries, qt)
		}
		writeJSON(filepath.Join(dir, runner.TimingsFile), t7)
	}
	return plans, artifacts
}

func rtJobs(reps int, mod func(*rtJob)) []rtJob {
	var out []rtJob
	for _, c := range []struct {
		name  string
		scale int64
	}{{rtBase, 1000}, {rtM, 2000}} {
		for rep := 1; rep <= reps; rep++ {
			j := rtJob{cand: c.name, rep: rep, scale: c.scale}
			if mod != nil {
				mod(&j)
			}
			out = append(out, j)
		}
	}
	return out
}

func exitOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var ec exitCodeError
	if !errors.As(err, &ec) {
		t.Fatalf("not an exit status: %v", err)
	}
	return ec.code
}

func TestReadTimingCommand(t *testing.T) {
	t.Parallel()

	good := func(plans, artifacts string, more ...string) []string {
		return append(append([]string{"-commit", rtCommit, "-plans", plans, "-base", rtBase, "-jobs", "5", "-primary", rtGroup + "@now"}, more...), artifacts)
	}
	digest := runner.ReadTimingDigestLine() + "\n"

	t.Run("a report", func(t *testing.T) {
		t.Parallel()
		plans, artifacts := writeTimedRun(t, rtJobs(5, nil))
		jsonPath := filepath.Join(t.TempDir(), "r.json")
		var so, se bytes.Buffer
		err := runReadTiming(good(plans, artifacts, "-json", jsonPath), &so, &se)
		if exitOf(t, err) != 0 {
			t.Fatalf("%v\n%s\n%s", err, so.String(), se.String())
		}
		if !strings.HasPrefix(so.String(), digest) {
			t.Errorf("the output does not begin with the digest of the rules:\n%.300s", so.String())
		}
		if !strings.Contains(so.String(), "PRIMARY CELLS") || !strings.Contains(so.String(), "M/crdb1: jobs (µs) [") {
			t.Errorf("the report:\n%s", so.String())
		}
		var rep runner.ReadTimingReport
		b, err := os.ReadFile(jsonPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &rep); err != nil || rep.RulesDigest != runner.ReadTimingRulesDigest() || len(rep.Arches) != 1 {
			t.Errorf("the JSON: %v %+v", err, rep)
		}
	})

	t.Run("a cell that is not reported ends with status 2", func(t *testing.T) {
		t.Parallel()
		plans, artifacts := writeTimedRun(t, rtJobs(5, func(j *rtJob) { j.notWarm = j.cand == rtM && j.rep <= 3 }))
		var so, se bytes.Buffer
		err := runReadTiming(good(plans, artifacts), &so, &se)
		if exitOf(t, err) != 2 || !strings.Contains(so.String(), "NOT REPORTED (1):") {
			t.Errorf("exit %d: %v\n%s", exitOf(t, err), err, so.String())
		}
	})

	for _, c := range []struct {
		name string
		jobs []rtJob
		mod  func(args []string) []string
		want string
	}{
		{"a second commit", rtJobs(5, func(j *rtJob) {
			if j.cand == rtM && j.rep == 2 {
				j.commit = strings.Repeat("c", 40)
			}
		}), nil, "the jobs are of the commits"},
		{"a second run", rtJobs(5, func(j *rtJob) {
			if j.cand == rtM && j.rep == 2 {
				j.run = "101"
			}
		}), nil, "more than one run or attempt"},
		{"a cache of 64 MiB", rtJobs(5, func(j *rtJob) { j.cacheBytes = 64 << 20 }), nil, "not 256 MiB"},
		{"fewer jobs than asked", rtJobs(4, nil), nil, "fewer than the 5 asked for"},
		{"a base that is not a candidate", rtJobs(5, nil), func(a []string) []string {
			for i, v := range a {
				if v == "-base" {
					a[i+1] = "L/x"
				}
			}
			return a
		}, `-base "L/x" is not among`},
	} {
		t.Run(c.name+" ends with status 3", func(t *testing.T) {
			t.Parallel()
			plans, artifacts := writeTimedRun(t, c.jobs)
			args := good(plans, artifacts)
			if c.mod != nil {
				args = c.mod(args)
			}
			var so, se bytes.Buffer
			err := runReadTiming(args, &so, &se)
			if exitOf(t, err) != 3 || !strings.Contains(err.Error(), c.want) {
				t.Errorf("exit %d: %v", exitOf(t, err), err)
			}
			// Nothing is judged, and the digest of the rules is the only thing printed.
			if so.String() != digest {
				t.Errorf("a refusal printed %q", so.String())
			}
		})
	}

	t.Run("a mistake of the command line ends with status 3", func(t *testing.T) {
		t.Parallel()
		plans, artifacts := writeTimedRun(t, rtJobs(5, nil))
		for _, args := range [][]string{
			{"-nonsense"},
			{"-commit", rtCommit, "-plans", plans, "-base", rtBase, artifacts},    // no -jobs
			{"-commit", rtCommit, "-plans", plans, "-base", rtBase, "-jobs", "5"}, // no artifacts
			{"-commit", rtCommit, "-base", rtBase, "-jobs", "5", artifacts},       // no plans
			{"-commit", rtCommit, "-plans", plans, "-base", rtBase, "-jobs", "5", filepath.Join(t.TempDir(), "missing")},
		} {
			var so, se bytes.Buffer
			if err := runReadTiming(args, &so, &se); exitOf(t, err) != 3 {
				t.Errorf("%v: exit %d (%v)", args, exitOf(t, err), err)
			}
			// The digest of the rules is printed first all the same, a flag error included.
			if so.String() != digest {
				t.Errorf("%v: printed %q", args, so.String())
			}
		}
	})

	t.Run("a directory with no job file is not judged", func(t *testing.T) {
		t.Parallel()
		plans, artifacts := writeTimedRun(t, rtJobs(5, nil))
		var removed bool
		_ = filepath.WalkDir(artifacts, func(p string, _ os.DirEntry, _ error) error {
			if !removed && filepath.Base(p) == runner.JobFile {
				removed = os.Remove(p) == nil
			}
			return nil
		})
		var so, se bytes.Buffer
		err := runReadTiming(good(plans, artifacts), &so, &se)
		if exitOf(t, err) != 3 || !strings.Contains(err.Error(), "not an artifact of the bench workflow") {
			t.Errorf("exit %d: %v", exitOf(t, err), err)
		}
	})
}
