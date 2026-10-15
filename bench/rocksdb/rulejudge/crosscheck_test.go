package rulejudge

import (
	"bytes"
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// tinyPebble builds candidates of layout L on Pebble on a tiny plan, reading the
// first, as the bench workflow builds them (no rest after a retention), with guards
// declared for a clean binary without cgo: nothing here is timed.
func tinyPebble(t *testing.T, names ...string) (*runner.Plan, []*Build) {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	spec.MinNonEmpty = 0
	plan, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	info := runner.BuildInfo{GoVersion: "test", GOOS: runtime.GOOS, GOARCH: goarchOf[DecidingArch], Revision: testSHA, Executable: exe(DecidingArch, false)}
	g := runner.Guards{Info: info}
	var out []*Build
	reps := map[string]int{}
	for _, name := range names {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(t.TempDir(), "out")
		m, err := runner.BuildWith(context.Background(), plan, v, dir, g, runner.BuildOptions{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		reps[name]++
		b := &Build{Manifest: m, Job: Job{Job: runner.Job{
			SHA: testSHA, Arch: DecidingArch, Window: VerdictWindow, Candidate: name, Rep: reps[name], CPUModel: cpuOf[DecidingArch], BinarySHA256: info.Executable,
		}, Engine: "pebble", MaxRSSKB: 1 << 20}}
		if b.Job.Rep == 1 {
			if b.Results, err = runner.Read(context.Background(), plan, v, dir, g, nil); err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, b)
	}
	return plan, out
}

// stallManifest is a manifest whose G2 depends on its median commit and on how many
// batches after a retention it counts: two retentions, the batches after each, and
// commits whose median is 1 ms (its bucket's bound).
func stallManifest(name string, retains []time.Duration, after [][]time.Duration, n string) *runner.Candidate {
	var h runner.Histogram
	for range 60 {
		h.Add(time.Millisecond)
	}
	for range 40 {
		h.Add(3 * time.Millisecond)
	}
	t := runner.Timing{Writes: h}
	for i, r := range retains {
		t.Retains = append(t.Retains, int64(r))
		var post []int64
		for _, d := range after[i] {
			post = append(post, int64(d))
		}
		t.PostRetention = append(t.PostRetention, post)
	}
	return &runner.Candidate{
		Manifest: &runner.Manifest{
			Candidate: name, Timing: t, Describe: map[string]string{runner.PostRetentionKey: n},
			Stream: runner.StreamInfo{Records: 777}, StatsCompacted: map[string]int64{"live_table_bytes": 31_337},
		},
		Results: &runner.Results{Candidate: name},
	}
}

// The judge's G2, G3 and G4 are the runner's: on real builds of layout L on Pebble
// of a tiny plan, and on manifests whose G2 turns on the median commit and on the
// count of batches after a retention, the values the judge reads print as
// [runner.Gates] prints its own.
func TestTheGatesAreTheRunners(t *testing.T) {
	t.Parallel()
	_, builds := tinyPebble(t, "L/off", "L/k64a2l5m")
	var cs []*runner.Candidate
	for _, b := range builds {
		cs = append(cs, &runner.Candidate{Manifest: b.Manifest, Results: b.Results})
	}
	ms := time.Millisecond
	cs = append(cs,
		stallManifest("S/two", []time.Duration{100 * ms, 80 * ms}, [][]time.Duration{{5 * ms, 2 * ms, 30 * ms}, {40 * ms}}, "2"),
		stallManifest("S/all", []time.Duration{100 * ms, 80 * ms}, [][]time.Duration{{5 * ms, 2 * ms, 30 * ms}, {40 * ms}}, "100"),
		stallManifest("S/none", []time.Duration{100 * ms, 80 * ms}, [][]time.Duration{{5 * ms, 2 * ms, 30 * ms}, {40 * ms}}, "0"),
	)
	rows := runner.Gates(cs, runner.DefaultRules())
	compared := 0
	for _, c := range cs {
		b := &Build{Manifest: c.Manifest}
		for gate, row := range map[string]string{GateG2: "G2 ", GateG3: "G3 ", GateG4: "G4 "} {
			for _, r := range rows {
				if r.Candidate != c.Manifest.Candidate || !strings.HasPrefix(r.Gate, row) {
					continue
				}
				v, ok := valueOf(b, gate)
				if !ok || FormatValue(gate, v) != r.Value {
					t.Errorf("%s %s: the judge reads %s (%v), the runner %s", c.Manifest.Candidate, gate, FormatValue(gate, v), ok, r.Value)
				}
				t.Logf("%s %s: judge %s, runner %s", c.Manifest.Candidate, gate, FormatValue(gate, v), r.Value)
				compared++
			}
		}
	}
	// G3 and G4 of each, and G2 of each manifest with retentions.
	if compared < 2*len(cs)+3 {
		t.Errorf("%d values compared", compared)
	}
	// S/two counts two batches after each retention: the second retention's 80 ms and
	// its one batch's excess over the median commit outweigh the first's.
	m := time.Duration(cs[2].Manifest.Timing.Writes.Quantile(0.5))
	if v, _ := valueOf(&Build{Manifest: cs[2].Manifest}, GateG2); time.Duration(v) != 120*ms-m {
		t.Errorf("G2 of S/two is %v, want %v", time.Duration(v), 120*ms-m)
	}
}

// Real builds of layout L on Pebble pass the judge's checks of a Pebble build and
// the runner's of the Pebble set: on a set with no RocksDB build, nothing is void
// and the verdict is not decided for want of RL.
func TestRealPebbleBuildsAreResults(t *testing.T) {
	t.Parallel()
	plan, builds := tinyPebble(t, "L/k64a2l5m", "L/k64a2l5m", "L/k64a2l5m")
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rep := Judge(&Set{Builds: builds, Plans: map[string]*runner.Plan{digest: plan}})
	if len(rep.Refused) > 0 || len(rep.NotResults) > 0 {
		t.Errorf("refused %q, not results %q", rep.Refused, rep.NotResults)
	}
	if rep.Verdict != VerdictNotDecided || !strings.Contains(strings.Join(rep.Why, " "), "no build of RL/k64a2l5m at 30 days on aarch64") {
		t.Errorf("verdict %q: %q", rep.Verdict, rep.Why)
	}
	var out bytes.Buffer
	Write(&out, rep)
	t.Logf("the judge's report on a tiny plan built by Pebble alone:\n%s", out.String())
}
