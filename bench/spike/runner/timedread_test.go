package runner_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// The tests here build tiny stores and read them with a clock they move themselves: no
// duration they handle is a measurement.

// stepClock is a clock that moves by 1000 ns each time it is read.
func stepClock() func() int64 {
	var now int64
	return func() int64 { now += 1000; return now }
}

func TestCaptureMuted(t *testing.T) {
	t.Parallel()

	c := runner.NewCapture()
	unmute := c.Mute()
	c.Count("a", 1)
	c.Sample("a", 2)
	if got := c.Take(); len(got) != 0 {
		t.Errorf("a muted recorder kept %v", got)
	}
	if got := c.Totals(); len(got) != 0 {
		t.Errorf("a muted recorder counted %v in its totals", got)
	}
	unmute()
	c.Count("a", 3)
	if got := c.Take(); got["a"] != 3 {
		t.Errorf("an unmuted recorder kept %v, want 3 for a", got)
	}

	// Holds add up, and releasing one twice releases it once.
	u1, u2 := c.Mute(), c.Mute()
	u1()
	u1()
	c.Count("a", 1)
	if got := c.Take(); len(got) != 0 {
		t.Errorf("a recorder with a hold left kept %v: a second call of the first release released the second hold", got)
	}
	u2()
	c.Count("a", 5)
	if got := c.Take(); got["a"] != 5 {
		t.Errorf("a recorder with no hold left kept %v", got)
	}
}

// results.json keeps the fields it has, and a read that is timed after makes the same
// results as one that is not: the answers, the sizes, the counters that count work, the
// description, the checks. The counters that depend on the cache, on time, or on the
// process (allocations) are the ones that may differ.
func TestReadWithoutTimingWritesTheSameResults(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	for _, name := range []string{"L/off", "M/crdb1"} {
		v := lookup(t, name)
		dir := runner.CandidateDir(t.TempDir(), name)
		if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		plain, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(dir, runner.TimingsFile)); err == nil {
			t.Fatalf("%s: a read that was not timed wrote %s", name, runner.TimingsFile)
		}
		keys := jsonKeys(t, filepath.Join(dir, runner.ResultsFile))
		wantKeys := []string{"Build", "Candidate", "Describe", "ManifestDigest", "Mismatches", "PlanDigest", "Queries", "StatsAfter", "StatsBefore", "Unstable", "Untimed"}
		if !slices.Equal(keys, wantKeys) {
			t.Errorf("%s: results.json has the fields %v, want %v", name, keys, wantKeys)
		}

		timed, timings, err := runner.ReadTimed(context.Background(), plan, v, dir, clean, runner.TimedReadOptions{Warmup: 1, Reps: 3, Clock: stepClock()}, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(jsonKeys(t, filepath.Join(dir, runner.ResultsFile)), wantKeys) {
			t.Errorf("%s: a timed read's results.json has other fields", name)
		}
		if timed.Candidate != plain.Candidate || timed.PlanDigest != plain.PlanDigest || timed.ManifestDigest != plain.ManifestDigest ||
			!maps.Equal(timed.Describe, plain.Describe) || !slices.Equal(timed.Mismatches, plain.Mismatches) || !slices.Equal(timed.Unstable, plain.Unstable) {
			t.Errorf("%s: the results of a timed read differ from a read's in what they say of the run", name)
		}
		if len(timed.Queries) != len(plain.Queries) {
			t.Fatalf("%s: %d queries timed, %d plain", name, len(timed.Queries), len(plain.Queries))
		}
		for i := range plain.Queries {
			a, b := plain.Queries[i], timed.Queries[i]
			if a.Digest != b.Digest || a.Size != b.Size || !maps.Equal(workCounters(a.Counters), workCounters(b.Counters)) {
				t.Errorf("%s: %s: counters or answer differ between a read and a timed read:\n%v\n%v", name, plan.Queries[i].Name(), workCounters(a.Counters), workCounters(b.Counters))
			}
		}
		// The timings are of the same plan and manifest, as the file says.
		if timings.Version != runner.ReadTimingsVersion || timings.PlanDigest != plain.PlanDigest || timings.ManifestDigest != plain.ManifestDigest ||
			timings.Candidate != name || timings.Warmup != 1 || timings.Reps != 3 || timings.Readers != 1 || timings.WarmCounter != runner.PebbleWarmCounter ||
			timings.CacheBytes != plan.CacheBytes || len(timings.Queries) != len(plan.Queries) {
			t.Errorf("%s: timings %+v", name, timings)
		}
		var onDisk runner.ReadTimings
		raw, err := os.ReadFile(filepath.Join(dir, runner.TimingsFile))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &onDisk); err != nil || len(onDisk.Queries) != len(plan.Queries) || len(onDisk.Queries[0].Ns) != 3 {
			t.Errorf("%s: %s: %v", name, runner.TimingsFile, err)
		}
	}
}

// workCounters are the counters that count what a read did, without the ones that depend
// on the cache, on time, or on the process.
func workCounters(c map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range c {
		switch {
		case strings.HasSuffix(k, ".block_bytes_cached"), strings.HasSuffix(k, ".block_read_ns"), k == "read.batch.separated_values",
			k == runner.CounterAllocs, k == runner.CounterAllocBytes:
		default:
			out[k] = v
		}
	}
	return out
}

func jsonKeys(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return slices.Sorted(maps.Keys(m))
}

func fileHash(t *testing.T, path string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	sum := sha256.Sum256(b)
	return string(sum[:]), true
}

// results.json is whole, and the database's shape checked, before the first timed ask,
// and the timed loop does not write it again; timings.json comes after.
func TestReadTimedWritesResultsBeforeItTimes(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "L/off")
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
		t.Fatal(err)
	}
	resultsPath, timingsPath := filepath.Join(dir, runner.ResultsFile), filepath.Join(dir, runner.TimingsFile)
	var first string
	var calls int
	var timingsSeen bool
	var now int64
	clock := func() int64 {
		calls++
		if calls == 1 {
			h, ok := fileHash(t, resultsPath)
			if !ok {
				t.Error("results.json does not exist at the first timed ask")
			}
			first = h
		}
		if _, ok := fileHash(t, timingsPath); ok {
			timingsSeen = true
		}
		now += 1000
		return now
	}
	if _, _, err := runner.ReadTimed(context.Background(), plan, v, dir, clean, runner.TimedReadOptions{Warmup: 1, Reps: 3, Clock: clock}, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2*3*len(plan.Queries) {
		t.Errorf("the clock was read %d times, want %d (two for each of 3 timed asks of each of %d queries)", calls, 2*3*len(plan.Queries), len(plan.Queries))
	}
	if last, _ := fileHash(t, resultsPath); first == "" || last != first {
		t.Error("results.json was written again by the timed loop")
	}
	if timingsSeen {
		t.Error("timings.json existed before the last timed ask")
	}
	if _, ok := fileHash(t, timingsPath); !ok {
		t.Error("timings.json was not written")
	}
}

// A query is not warm if its timed asks loaded blocks; with a cache too small to hold
// one, none of them is warm, and with room for them all they are.
func TestAQueryThatMissesTheCacheIsNotWarm(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	for _, name := range []string{"L/off", "M/crdb1"} {
		for _, c := range []struct {
			cache    int64
			wantWarm bool
		}{{1 << 10, false}, {64 << 20, true}} {
			spec := tinySpec()
			spec.CacheBytes = c.cache
			plan := mustPlan(t, spec)
			v := lookup(t, name)
			dir := runner.CandidateDir(t.TempDir(), v.Name)
			if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
				t.Fatal(err)
			}
			_, timings, err := runner.ReadTimed(context.Background(), plan, v, dir, clean, runner.TimedReadOptions{Warmup: 2, Reps: 5, Clock: stepClock()}, nil)
			if err != nil {
				t.Fatalf("%s at %d bytes: %v", name, c.cache, err)
			}
			warm := 0
			for _, q := range timings.Queries {
				if q.Warm != (q.WarmDelta == 0) {
					t.Errorf("%s: %s is warm %v with %d misses", name, q.Name, q.Warm, q.WarmDelta)
				}
				if q.Warm {
					warm++
				}
			}
			switch {
			case c.wantWarm && warm != len(timings.Queries):
				t.Errorf("%s with a cache of %d bytes: %d of %d queries warm, want all", name, c.cache, warm, len(timings.Queries))
			case !c.wantWarm && warm == len(timings.Queries):
				t.Errorf("%s with a cache of %d bytes: every query is warm, want some with misses", name, c.cache)
			}
		}
	}
}

// A database that does work while the timed asks are made has timings that do not mean
// what they are taken to: they are not written, and the results, written before, stay.
func TestReadTimedRefusesWorkDuringTheTimedLoop(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	started := false
	v := breaking(t, "M/crdb1 that flushes while timed", func(f *faulty) {
		f.stats = func(m map[string]int64) map[string]int64 {
			if started {
				m["flushes"]++
			}
			return m
		}
	})
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
		t.Fatal(err)
	}
	clock := func() int64 { started = true; return 0 }
	_, _, err := runner.ReadTimed(context.Background(), plan, v, dir, clean, runner.TimedReadOptions{Warmup: 1, Reps: 3, Clock: clock}, nil)
	if err == nil || !strings.Contains(err.Error(), "did work while being timed") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := fileHash(t, filepath.Join(dir, runner.TimingsFile)); ok {
		t.Error("timings.json was written for a database that flushed under the timed asks")
	}
	if _, ok := fileHash(t, filepath.Join(dir, runner.ResultsFile)); !ok {
		t.Error("results.json, written before the timed asks, is gone")
	}
}

// An answer that changes in the timed asks, after the passes agreed with the
// reference engine, is in the timings, and the results do not say it.
func TestReadTimedMarksAnAnswerThatChangesWhileTimed(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	started := false
	v := breaking(t, "M/crdb1 that errs while timed", func(f *faulty) {
		f.neighbors = func(ns []engine.Neighbor) []engine.Neighbor {
			if started && len(ns) > 0 {
				return ns[1:]
			}
			return ns
		}
	})
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
		t.Fatal(err)
	}
	res, timings, err := runner.ReadTimed(context.Background(), plan, v, dir, clean, runner.TimedReadOptions{Warmup: 1, Reps: 3, Clock: func() int64 { started = true; return 0 }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Mismatches) != 0 {
		t.Fatalf("the passes before the timed asks found %v", res.Mismatches)
	}
	wrong := 0
	for _, q := range timings.Queries {
		if q.Mismatch {
			wrong++
		}
	}
	if wrong == 0 {
		t.Error("no query is marked as answered wrongly")
	}
}
