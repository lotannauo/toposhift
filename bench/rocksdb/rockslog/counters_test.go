package rockslog_test

import (
	"context"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The guards of a build a result may come from, as the runner's own tests declare
// them, with cgo declared for RL: CheckFor admits cgo for RL, and not for layout L on
// Pebble, which is declared without it (this test binary has cgo; the declaration
// is what the guards judge, and nothing here is timed).
func guards(cgo bool) runner.Guards {
	return runner.Guards{Info: runner.BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r", CGO: cgo}}
}

// countersPlan is a tiny plan, made once.
var countersPlan = sync.OnceValues(func() (*runner.Plan, error) {
	spec := runner.DefaultSpec(workload.Tiny())
	spec.MinNonEmpty = 0
	return runner.MakePlan(context.Background(), spec, nil)
})

func plan(t *testing.T) *runner.Plan {
	t.Helper()
	p, err := countersPlan()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// build builds and reads a candidate under a fresh directory.
func build(t *testing.T, p *runner.Plan, v candidates.Variant, g runner.Guards) (string, *runner.Manifest, *runner.Results) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	m, err := runner.Build(context.Background(), p, v, dir, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := runner.Read(context.Background(), p, v, dir, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dir, m, r
}

// comparable is whether a counter is one both engines define the same way: the
// layout's own counters, and the reads, seeks and steps of each kind of read. What
// Pebble counts inside its iterator, the cold and allocation counters derived from
// it and the engine's timings are not; nor are RL's own diagnostics (rocks.*).
func comparable(name string) bool {
	switch name {
	case runner.CounterBlockLoads, runner.CounterColdBlockBytes, runner.CounterColdCacheBytes, runner.CounterAllocs, runner.CounterAllocBytes:
		return false
	}
	if strings.HasSuffix(name, "_ns") {
		return false
	}
	if rest, ok := strings.CutPrefix(name, "read."); ok {
		// read.<op>.<stat> is one kind of read's; read.<name> is the layout's own.
		if _, stat, ok := strings.Cut(rest, "."); ok {
			return stat == "reads" || stat == "seeks" || stat == "steps"
		}
	}
	return true
}

func comparableOf(m map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range m {
		if comparable(k) {
			out[k] = v
		}
	}
	return out
}

// RL gives, on every query of a plan, the answer layout L on Pebble gives, and the
// same interface counters (reads, seeks and steps of each read) and layout counters
// (records stepped, checkpoints met and used, entries decoded), from a build whose
// own counters (records written, checkpoints written, invalidated and loaded, the
// work of each retention) are the same too. A difference is a defect of the port.
func TestCountersAreLayoutLsOnPebble(t *testing.T) {
	t.Parallel()
	p := plan(t)
	const policy = "k64a2l5m"
	rl, err := rockslog.Variant(rockslog.NamePrefix+policy, false)
	if err != nil {
		t.Fatal(err)
	}
	l, err := candidates.Lookup("L/" + policy)
	if err != nil {
		t.Fatal(err)
	}
	_, rm, rr := build(t, p, rl, guards(true))
	_, lm, lr := build(t, p, l, guards(false))

	if len(rr.Mismatches) > 0 || len(rr.Unstable) > 0 || len(rm.UncompactedWrong) > 0 {
		t.Fatalf("RL: %d answers wrong, %d unstable, %d wrong before compaction (first wrong %v, first unstable %v)",
			len(rr.Mismatches), len(rr.Unstable), len(rm.UncompactedWrong), rr.Mismatches, rr.Unstable)
	}
	if len(p.Queries) == 0 || len(rr.Queries) != len(lr.Queries) {
		t.Fatalf("%d queries planned, RL answered %d and L %d", len(p.Queries), len(rr.Queries), len(lr.Queries))
	}
	if !rm.Build.CGO || rm.Candidate != rl.Name {
		t.Errorf("RL's manifest: candidate %q, cgo %v", rm.Candidate, rm.Build.CGO)
	}
	if got, want := comparableOf(rm.Counters), comparableOf(lm.Counters); !maps.Equal(got, want) {
		t.Errorf("the builds counted differently:\nRL %v\nL  %v", got, want)
	}
	var steps, rlLoads, lLoads int64
	for i, q := range p.Queries {
		rq, lq := rr.Queries[i], lr.Queries[i]
		if rq.Digest != lq.Digest || rq.Size != lq.Size {
			t.Errorf("%s: RL answers %s (%d), L %s (%d)", q.Name(), rq.Digest, rq.Size, lq.Digest, lq.Size)
		}
		got, want := comparableOf(rq.Counters), comparableOf(lq.Counters)
		if !maps.Equal(got, want) {
			t.Errorf("%s counted differently:\nRL %v\nL  %v", q.Name(), got, want)
		}
		for k, v := range got {
			if strings.HasSuffix(k, ".steps") {
				steps += v
			}
		}
		rlLoads += rq.Counters[runner.CounterBlockLoads]
		lLoads += lq.Counters[runner.CounterBlockLoads]
	}
	if steps == 0 {
		t.Error("no query stepped: the plan reaches no read")
	}
	// A diagnostic, not a comparison: the blocks each engine loaded cold, summed
	// over the plan, in each engine's own unit.
	t.Logf("%d queries, %d steps; cold block loads summed: RL %d, L %d", len(p.Queries), steps, rlLoads, lLoads)
	for _, k := range []string{"layout", "comparer", "checkpoints", "block_bytes", "cache_bytes", "sync", "settle_tombstones"} {
		if rm.Describe[k] != lm.Describe[k] {
			t.Errorf("described %s: RL %q, L %q", k, rm.Describe[k], lm.Describe[k])
		}
	}
}

// With the perf context on, every read that seeks reports the blocks RocksDB touched
// for it (from the cache or the files) on its own thread: a read whose perf context
// was taken on another thread than the one it ran on would report none.
func TestPerfContextCountsEveryRead(t *testing.T) {
	t.Parallel()
	p := plan(t)
	v, err := rockslog.Variant(rockslog.NamePrefix+"off", false)
	if err != nil {
		t.Fatal(err)
	}
	perf, err := rockslog.Variant(rockslog.NamePrefix+"off", true)
	if err != nil {
		t.Fatal(err)
	}
	dir, m, _ := build(t, p, v, guards(true))
	if m.Describe["perf_level"] != "disable" {
		t.Errorf("the build's perf level is %q", m.Describe["perf_level"])
	}
	r, err := runner.Read(context.Background(), p, perf, dir, guards(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Describe["perf_level"] != "enable_count" {
		t.Errorf("the read's perf level is %q", r.Describe["perf_level"])
	}
	read := 0
	for i, q := range p.Queries {
		c := r.Queries[i].Counters
		for k, seeks := range c {
			op, ok := strings.CutSuffix(k, ".seeks")
			if !ok || seeks == 0 {
				continue
			}
			read++
			if c[op+".rocks.block_read_count"]+c[op+".rocks.block_cache_hit_count"] == 0 {
				t.Errorf("%s: %s sought %d times and touched no block: %v", q.Name(), op, seeks, c)
			}
		}
	}
	if read == 0 {
		t.Fatal("no read sought")
	}
	if !slices.ContainsFunc(slices.Collect(maps.Keys(r.Queries[0].Counters)), func(k string) bool { return strings.Contains(k, ".rocks.") }) {
		t.Errorf("the first query has no perf counter: %v", r.Queries[0].Counters)
	}
}
