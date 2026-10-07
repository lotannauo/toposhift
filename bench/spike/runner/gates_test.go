package runner_test

import (
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// gateCandidate is a candidate with the numbers the gates read.
func gateCandidate(name string, tableBytes, records int64, commit time.Duration, retain, after time.Duration) *runner.Candidate {
	var h runner.Histogram
	for i := 0; i < 100; i++ {
		h.Add(commit)
	}
	return &runner.Candidate{
		Manifest: &runner.Manifest{
			Stream: runner.StreamInfo{Records: uint64(records)}, StatsCompacted: map[string]int64{"live_table_bytes": tableBytes},
			StatsBuilt: map[string]int64{"bytes_in": 1000}, Counters: map[string]int64{},
			Timing: runner.Timing{Writes: h, Retains: []int64{int64(retain)}, AfterRetention: []int64{int64(after)}},
		},
		Results: &runner.Results{Candidate: name},
	}
}

func verdictOf(vs []runner.GateVerdict, candidate, gate string) (runner.GateVerdict, bool) {
	for _, v := range vs {
		if v.Candidate == candidate && strings.HasPrefix(v.Gate, gate) {
			return v, true
		}
	}
	return runner.GateVerdict{}, false
}

func TestGatesAreRatiosToTheBestCandidateThatAnswersRight(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules() // factors of 2; a stall budget of 60 s
	best := gateCandidate("best", 1000, 10, 1000*time.Nanosecond, 5*time.Second, 100*time.Microsecond)
	near := gateCandidate("near", 2000, 10, 1900*time.Nanosecond, 60*time.Second, 190*time.Microsecond)                   // twice the bytes, within the factors
	over := gateCandidate("over", 2010, 10, 4000*time.Nanosecond, 60*time.Second+time.Millisecond, 5000*time.Microsecond) // over every limit
	// A candidate that answers wrongly, and is far cheaper than any: it must not set the
	// standard the others are held to.
	wrong := gateCandidate("wrong", 10, 10, 10*time.Nanosecond, time.Second, time.Microsecond)
	wrong.Results.Mismatches = []string{"a query"}
	late := gateCandidate("late", 10, 10, 10*time.Nanosecond, time.Second, time.Microsecond) // wrong only before compaction
	late.Manifest.UncompactedWrong = []string{"a query"}
	vs := runner.Gates([]*runner.Candidate{best, near, over, wrong, late}, rules)

	for _, c := range []struct {
		candidate, gate string
		pass            bool
	}{
		{"best", "G0", true},
		{"best", "G4", true},
		{"best", "G3 batch", true},
		{"best", "G3 first", true},
		{"best", "G2", true},
		{"near", "G4", true},
		{"near", "G3 batch", true},
		{"near", "G3 first", true},
		{"near", "G2", true},
		{"over", "G4", false},
		{"over", "G3 batch", false},
		{"over", "G3 first", false},
		{"over", "G2", false},
		{"wrong", "G0", false},
		{"late", "G0", false},
	} {
		v, ok := verdictOf(vs, c.candidate, c.gate)
		if !ok || v.Pass != c.pass {
			t.Errorf("%s %s: %+v (found %v), want pass %v", c.candidate, c.gate, v, ok, c.pass)
		}
	}
	// A candidate that answers wrongly is held to G0 and no other ratio.
	for _, g := range []string{"G4", "G3"} {
		for _, name := range []string{"wrong", "late"} {
			if v, ok := verdictOf(vs, name, g); ok {
				t.Errorf("a candidate that answers wrongly was judged on %s: %+v", g, v)
			}
		}
	}
	// Timings are marked as local, bytes and answers are not.
	if v, _ := verdictOf(vs, "best", "G3 batch"); !v.Local {
		t.Error("a commit timing is not marked local")
	}
	if v, _ := verdictOf(vs, "best", "G4"); v.Local {
		t.Error("bytes are marked as a local timing")
	}
}

// The share of the bytes written that went to checkpoints is held to the share the
// design is reopened above.
func TestGatesCheckpointWritesAgainstTheReopenShare(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules() // 20%
	a := gateCandidate("a", 1000, 10, time.Microsecond, time.Second, time.Microsecond)
	b := gateCandidate("b", 1000, 10, time.Microsecond, time.Second, time.Microsecond)
	a.Manifest.Counters["checkpoint.bytes_written"] = 200 // of 1000 bytes in
	b.Manifest.Counters["checkpoint.bytes_written"] = 201
	vs := runner.Gates([]*runner.Candidate{a, b}, rules)
	if v, ok := verdictOf(vs, "a", "checkpoint bytes"); !ok || !v.Pass {
		t.Errorf("exactly the share: %+v", v)
	}
	if v, ok := verdictOf(vs, "b", "checkpoint bytes"); !ok || v.Pass {
		t.Errorf("a share over it passes: %+v", v)
	}
	var out strings.Builder
	runner.WriteGates(&out, vs)
	if !strings.Contains(out.String(), "OVER") || !strings.Contains(out.String(), "the design is reopened above it") {
		t.Errorf("the text does not say what is over:\n%s", out.String())
	}
}

// A commit time is compared to the best candidate's by a factor, so it is a time and not the
// edge of a bucket that doubles: 1.1 ms and 4.1 ms are nearly four times apart and not
// in adjacent buckets of a power of two.
func TestG3ComparesTimesAndNotBucketsThatDouble(t *testing.T) {
	t.Parallel()

	best := gateCandidate("best", 1000, 10, 1100*time.Microsecond, time.Second, time.Millisecond)
	slow := gateCandidate("slow", 1000, 10, 4100*time.Microsecond, time.Second, time.Millisecond)
	vs := runner.Gates([]*runner.Candidate{best, slow}, runner.DefaultRules())
	if v, ok := verdictOf(vs, "slow", "G3 batch"); !ok || v.Pass {
		t.Errorf("a commit 3.7 times the best's passes the factor of 2: %+v", v)
	}
	ok := gateCandidate("ok", 1000, 10, 2100*time.Microsecond, time.Second, time.Millisecond) // 1.9 times
	vs = runner.Gates([]*runner.Candidate{best, ok}, runner.DefaultRules())
	if v, found := verdictOf(vs, "ok", "G3 batch"); !found || !v.Pass {
		t.Errorf("a commit 1.9 times the best's does not pass the factor of 2: %+v", v)
	}
}

// A stream of pins is a projection, and only the answers are judged on it.
func TestOnAProjectionOnlyTheAnswersAreAGate(t *testing.T) {
	t.Parallel()

	cs := []*runner.Candidate{
		gateCandidate("a", 1000, 10, time.Microsecond, time.Second, time.Millisecond),
		gateCandidate("b", 1000, 10, time.Microsecond, time.Second, time.Millisecond),
	}
	full := runner.GatesOf(&runner.Plan{}, cs, runner.DefaultRules())
	proj := runner.GatesOf(&runner.Plan{Spec: runner.Spec{Pins: &runner.Pins{}}}, cs, runner.DefaultRules())
	if len(proj) == 0 || len(proj) >= len(full) {
		t.Fatalf("%d verdicts on a projection and %d on a full stream", len(proj), len(full))
	}
	// Full stores read at pins are judged on every gate.
	if pinnedFull := runner.GatesOf(&runner.Plan{Spec: runner.Spec{Pins: &runner.Pins{}, FullStore: true}}, cs, runner.DefaultRules()); len(pinnedFull) != len(full) {
		t.Errorf("%d verdicts on full stores read at pins and %d on a full stream", len(pinnedFull), len(full))
	}
	for _, v := range proj {
		if !strings.HasPrefix(v.Gate, "G0") {
			t.Errorf("a projection is judged on %s", v.Gate)
		}
	}
}

// The first batch after a retention of a build that rested after each one does not carry
// the compactions' catch-up: its value is shown and not judged, and it sets no standard
// for a build that did not rest.
func TestAFirstBatchAfterARetentionOfARestedBuildIsNotComparable(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	plain := gateCandidate("plain", 1000, 10, time.Microsecond, time.Second, 10*time.Second)
	fast := gateCandidate("rested", 1000, 10, time.Microsecond, time.Second, time.Microsecond) // far quicker than the other, which it must not hold to a standard
	fast.Manifest.Describe = map[string]string{runner.RestKey: "true"}
	vs := runner.Gates([]*runner.Candidate{plain, fast}, rules)

	if v, ok := verdictOf(vs, "rested", "G3 first"); !ok || !v.NotComparable || !v.Pass || !strings.Contains(v.Limit, "not comparable") {
		t.Errorf("the rested build's first batch after a retention: %+v", v)
	}
	if v, ok := verdictOf(vs, "plain", "G3 first"); !ok || v.NotComparable || !v.Pass {
		t.Errorf("a build that did not rest was held to a rested build's value: %+v", v)
	}
	var out strings.Builder
	runner.WriteGates(&out, vs)
	if !strings.Contains(out.String(), "not judged") {
		t.Errorf("the report does not say the value is not judged:\n%s", out.String())
	}
}
