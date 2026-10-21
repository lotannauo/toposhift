package runner_test

import (
	"context"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// A build with the phase log stores what a build without it stores, and a build
// without it makes no file and says nothing of it. Both are built with a synchronous
// retention (a function of the stream) and the canonical layout (a function of the
// data), so their sizes, breakdowns and the store's deterministic counts can be
// compared exactly. The log has a line for each batch, its wall times add up to the
// build's time in Write, and the store's phase samples it kept add up to what the
// build's recorder summed.
func TestThePhaseLogChangesNothingTheStoreStores(t *testing.T) {
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "Lroot/k4a2l1ns")
	build := func(on bool) (string, *runner.Manifest) {
		dir := runner.CandidateDir(t.TempDir(), v.Name)
		m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{
			RetentionMode: candidates.RetentionSync, CanonicalLayout: true, PhaseLog: on,
		}, nil)
		if err != nil {
			t.Fatalf("phase log %v: %v", on, err)
		}
		return dir, m
	}
	offDir, off := build(false)
	onDir, on := build(true)

	if _, err := os.Stat(filepath.Join(offDir, runner.PhaseLogFile)); !os.IsNotExist(err) {
		t.Errorf("a build without the log made %s: %v", runner.PhaseLogFile, err)
	}
	if v, ok := off.Describe[runner.PhaseLogKey]; ok {
		t.Errorf("a build without the log says %s = %q", runner.PhaseLogKey, v)
	}
	if on.Describe[runner.PhaseLogKey] != "true" {
		t.Errorf("a build with the log says %s = %q", runner.PhaseLogKey, on.Describe[runner.PhaseLogKey])
	}
	describe := maps.Clone(on.Describe)
	delete(describe, runner.PhaseLogKey)
	if !maps.Equal(describe, off.Describe) {
		t.Errorf("described differently: %v against %v", describe, off.Describe)
	}
	if off.Stream.Digest != on.Stream.Digest || off.Size != on.Size ||
		!reflect.DeepEqual(off.Breakdown, on.Breakdown) || !reflect.DeepEqual(off.SizeByLayer, on.SizeByLayer) {
		t.Errorf("stored differently: stream %s/%s, size %d/%d, breakdown %v/%v, layers %v/%v",
			off.Stream.Digest, on.Stream.Digest, off.Size, on.Size, off.Breakdown, on.Breakdown, off.SizeByLayer, on.SizeByLayer)
	}
	for _, name := range []string{"write.records", "checkpoint.written", "checkpoint.bytes_written", "checkpoint.invalidated", "checkpoint.build_records_walked", "checkpoint.loads", "checkpoint.loads_cold"} {
		if off.Counters[name] != on.Counters[name] {
			t.Errorf("%s: %d without the log, %d with it", name, off.Counters[name], on.Counters[name])
		}
	}
	if off.Counters["checkpoint.written"] == 0 {
		t.Error("the build wrote no checkpoint: the test does not reach the second commit")
	}

	recs, err := runner.LoadPhaseLog(onDir)
	if err != nil {
		t.Fatal(err)
	}
	if want := int(on.Timing.Writes.Count); len(recs) != want || want == 0 {
		t.Fatalf("%d lines in the log, %d batches written", len(recs), want)
	}
	var wall, walk, written, records int64
	sums := map[string]int64{}
	for i, r := range recs {
		if r.Index != i || r.WallNs <= 0 || r.Sampled() > r.WallNs {
			t.Errorf("batch %d: %+v", i, r)
		}
		if i > 0 && r.StartNs < recs[i-1].StartNs+recs[i-1].WallNs {
			t.Errorf("batch %d starts at %d before batch %d ends at %d", i, r.StartNs, i-1, recs[i-1].StartNs+recs[i-1].WallNs)
		}
		wall += r.WallNs
		walk += r.Walk
		written += r.CkptWritten
		records += int64(r.Records)
		sums["validate"] += r.Validate
		sums["state"] += r.State
		sums["invalidate"] += r.Invalidate
		sums["record_commit"] += r.RecordCommit
		sums["ckpt_build"] += r.CkptBuild
		sums["ckpt_commit"] += r.CkptCommit
	}
	if wall != on.Timing.Writes.TotalNs {
		t.Errorf("the log's wall times add up to %d ns, the build's time in Write is %d", wall, on.Timing.Writes.TotalNs)
	}
	for name, got := range sums {
		if want := on.Counters["write.phase_ns."+name]; got != want {
			t.Errorf("%s: the log adds up to %d ns, the recorder to %d", name, got, want)
		}
	}
	if walk != on.Counters["checkpoint.build_ns"] || written != on.Counters["checkpoint.written"] || records != int64(on.Stream.Records) {
		t.Errorf("walk %d/%d, written %d/%d, records %d/%d", walk, on.Counters["checkpoint.build_ns"], written, on.Counters["checkpoint.written"], records, on.Stream.Records)
	}
	if sums["ckpt_commit"] == 0 || sums["record_commit"] == 0 || sums["ckpt_build"] < walk {
		t.Errorf("phases not timed: %v, walks %d", sums, walk)
	}

	// The analysis of a synchronous build places each batch where the gate did.
	rep := runner.AnalyzePhases(recs, on.Gate)
	for class, want := range map[string]int64{
		runner.PhaseOutside: on.Gate.Outside.Batches, runner.PhaseSettling: on.Gate.Settling.Batches,
		runner.PhaseInside: on.Gate.Inside.Batches,
	} {
		if int64(rep.Counts[class]) != want {
			t.Errorf("%d batches %s, the gate counted %d", rep.Counts[class], class, want)
		}
	}
	var text strings.Builder
	rep.Format(&text, "")
	if !strings.Contains(text.String(), "= build+commit") || !strings.Contains(text.String(), "class outside") {
		t.Errorf("the report:\n%s", text.String())
	}
	if out := rep.Class(runner.PhaseOutside); out == nil || math.IsNaN(out.Tail[0].Share) || math.Abs(out.Tail[0].Share-1) > 1e-9 {
		t.Errorf("the wall time's share of the tail is not all of it: %+v", out)
	}
}

// The log is refused before anything is made for a candidate that is not built through
// the root store, which has no store phases to log.
func TestThePhaseLogNeedsTheRootStore(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "c")
	_, err := runner.BuildWith(context.Background(), mustPlan(t, tinySpec()), lookup(t, "L/off"), dir, clean, runner.BuildOptions{PhaseLog: true}, nil)
	if err == nil || !strings.Contains(err.Error(), "not built through the root store") {
		t.Errorf("%v", err)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Errorf("the build made %s before it refused", dir)
	}
}
