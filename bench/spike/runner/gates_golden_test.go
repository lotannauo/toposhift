package runner_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

var update = flag.Bool("update", false, "rewrite the golden files under testdata/ from the current output")

const (
	gatesGolden         = "testdata/gates.golden"
	timingReplicaGolden = "testdata/timing_replica.golden"
)

// checkGolden compares got with the file, byte for byte, or writes the file under -update.
func checkGolden(t *testing.T, path string, got []byte) {
	t.Helper()
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (create it with -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the text no longer matches %s.\n got:\n%s\nwant:\n%s", path, got, want)
	}
}

// goldenCandidate is a candidate with the numbers the gates read. A commit of zero
// leaves the histogram empty, and a negative retention or first batch leaves its list
// empty.
func goldenCandidate(name string, tableBytes int64, commit, retain, after time.Duration) *runner.Candidate {
	var h runner.Histogram
	if commit > 0 {
		for range 100 {
			h.Add(commit)
		}
	}
	t := runner.Timing{Writes: h}
	if retain >= 0 {
		t.Retains = []int64{int64(retain)}
	}
	if after >= 0 {
		t.AfterRetention = []int64{int64(after)}
	}
	return &runner.Candidate{
		Manifest: &runner.Manifest{
			Stream: runner.StreamInfo{Records: 10}, StatsCompacted: map[string]int64{"live_table_bytes": tableBytes},
			StatsBuilt: map[string]int64{"bytes_in": 1000}, Counters: map[string]int64{}, Describe: map[string]string{},
			Timing: t,
		},
		Results: &runner.Results{Candidate: name},
	}
}

// This file pins the text the report command prints for the other gates (G0, G2, G3,
// G4 and the share of checkpoints) and is regenerated only on purpose:
//
//	go test ./spike/runner -run TestGatesTextIsUnchanged -update
//
// The judgement of the timed builds shares the arithmetic of these gates, and a change
// to one word of a limit must not slip through there into this text. Regenerated for
// rules 5 (G2 counts the batches after a retention; the first batch is a diagnostic).
func TestGatesTextIsUnchanged(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	best := goldenCandidate("a best", 1000, 1000*time.Nanosecond, 5*time.Second, 100*time.Microsecond)
	near := goldenCandidate("b near", 2000, 1900*time.Nanosecond, 60*time.Second, 190*time.Microsecond)
	over := goldenCandidate("c over", 2010, 4000*time.Nanosecond, 60*time.Second+time.Millisecond, 5000*time.Microsecond)
	wrong := goldenCandidate("d wrong", 10, 10*time.Nanosecond, time.Second, time.Microsecond)
	wrong.Results.Mismatches = []string{"a query", "another"}
	late := goldenCandidate("e late", 10, 10*time.Nanosecond, time.Second, time.Microsecond)
	late.Manifest.UncompactedWrong = []string{"a query"}
	unstable := goldenCandidate("f unstable", 1500, 1500*time.Nanosecond, 3*time.Second, 150*time.Microsecond)
	unstable.Results.Unstable = []string{"a query (cold)"}
	rested := goldenCandidate("g rested", 1200, 1200*time.Nanosecond, 4*time.Second, time.Microsecond)
	rested.Manifest.Describe[runner.RestKey] = "true"
	restedNoAfter := goldenCandidate("h rested without a first batch", 1200, 1200*time.Nanosecond, 4*time.Second, -1)
	restedNoAfter.Manifest.Describe[runner.RestKey] = "true"
	noRetention := goldenCandidate("i no retention", 1100, 1100*time.Nanosecond, -1, -1)
	within := goldenCandidate("j checkpoints within", 1000, 1000*time.Nanosecond, time.Second, 100*time.Microsecond)
	within.Manifest.Counters["checkpoint.bytes_written"] = 200
	beyond := goldenCandidate("k checkpoints over", 1000, 1000*time.Nanosecond, time.Second, 100*time.Microsecond)
	beyond.Manifest.Counters["checkpoint.bytes_written"] = 201
	all := []*runner.Candidate{best, near, over, wrong, late, unstable, rested, restedNoAfter, noRetention, within, beyond}

	var buf bytes.Buffer
	section := func(name string, vs []runner.GateVerdict) {
		fmt.Fprintf(&buf, "== %s: %d verdicts\n", name, len(vs))
		runner.WriteGates(&buf, vs)
		for _, v := range vs {
			fmt.Fprintf(&buf, "%q %q %q %q pass=%v local=%v notcomparable=%v\n", v.Candidate, v.Gate, v.Value, v.Limit, v.Pass, v.Local, v.NotComparable)
		}
	}
	section("every branch", runner.Gates(all, rules))
	section("a plan of a full stream", runner.GatesOf(&runner.Plan{}, all, rules))
	section("a projection", runner.GatesOf(&runner.Plan{Spec: runner.Spec{Pins: &runner.Pins{}}}, all, rules))
	section("full stores read at pins", runner.GatesOf(&runner.Plan{Spec: runner.Spec{Pins: &runner.Pins{}, FullStore: true}}, all, rules))
	// Builds that recorded the batches after their retentions, as the runner records them
	// now: G2 is judged on them.
	recorded := func(c *runner.Candidate, batches ...time.Duration) *runner.Candidate {
		c.Manifest.Describe[runner.PostRetentionKey] = "100"
		c.Manifest.Timing.PostRetention = [][]int64{{}}
		for _, d := range batches {
			c.Manifest.Timing.PostRetention[0] = append(c.Manifest.Timing.PostRetention[0], int64(d))
		}
		return c
	}
	slow := make([]time.Duration, 100)
	for i := range slow {
		slow[i] = 3 * time.Second
	}
	settled := recorded(goldenCandidate("m settled", 1000, 1000*time.Nanosecond, 40*time.Second, 20*time.Millisecond), 20*time.Millisecond, time.Millisecond)
	settled.Manifest.Timing.RetainPhases = []runner.RetainPhase{{Work: int64(30 * time.Second), Flush: int64(time.Second), Settle: int64(9 * time.Second)}}
	atBudget := recorded(goldenCandidate("n at the budget", 1000, 1000*time.Nanosecond, 60*time.Second, time.Microsecond), time.Microsecond)
	overBudget := recorded(goldenCandidate("o over the budget", 1000, 1000*time.Nanosecond, 60*time.Second+time.Millisecond, time.Microsecond), time.Microsecond)
	stalled := recorded(goldenCandidate("p stalled", 1000, 1000*time.Nanosecond, 5*time.Second, 3*time.Second), slow...)
	deadline := recorded(goldenCandidate("q deadline reached", 1000, 1000*time.Nanosecond, 70*time.Second, 2*time.Second), 2*time.Second)
	deadline.Manifest.Timing.RetainPhases = []runner.RetainPhase{{Work: int64(60 * time.Second), Flush: int64(time.Second), Settle: int64(9 * time.Second), DeadlineHit: true}}
	section("batches after a retention recorded", runner.Gates([]*runner.Candidate{settled, atBudget, overBudget, stalled, deadline}, rules))
	section("none passes G0", runner.Gates([]*runner.Candidate{wrong, late, unstable}, rules))
	section("no batch was timed", runner.Gates([]*runner.Candidate{
		goldenCandidate("x", 1000, 0, time.Second, -1),
		goldenCandidate("y", 2000, 0, 2*time.Second, -1),
	}, rules))
	section("another rule", runner.Gates(all[:3], func() runner.Rules {
		r := runner.DefaultRules()
		r.CommitFactor, r.BytesPerRecordFactor, r.StallBudgetSeconds, r.ReopenCheckpointWrites = 3, 1.5, 6, 0.1
		return r
	}()))
	section("nothing", runner.Gates(nil, rules))

	checkGolden(t, gatesGolden, buf.Bytes())
}

// A label replaces the mark of a local timing, whatever else the verdict says.
func TestALabelReplacesTheMarkOfALocalTiming(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	runner.WriteGates(&buf, []runner.GateVerdict{
		{Candidate: "a", Gate: "G3", Value: "1ms", Limit: "2ms", Pass: true, Local: true, Label: "CI timing"},
		{Candidate: "a", Gate: "G3 again", Value: "3ms", Limit: "2ms", Local: true, Label: "not judged: a reason"},
		{Candidate: "a", Gate: "G3 local", Value: "1ms", Limit: "2ms", Pass: true, Local: true},
		{Candidate: "a", Gate: "G4", Value: "1", Limit: "2", Pass: true},
	})
	got := buf.String()
	for _, want := range []string{"ok (CI timing)\n", "OVER (not judged: a reason)\n", "ok (local timing)\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("the text does not hold %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "local timing") != 1 {
		t.Errorf("a label did not replace the mark of a local timing:\n%s", got)
	}
}
