package runner

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// The class the analysis gives a batch is the class the gate gave it while the build
// ran, from the windows the manifest holds: over random windows and batches, in an
// unpaced build and in a paced one (whose unpaced and excluded batches are the
// gate's), the counts of each class equal the gate's own.
func TestThePhaseReportClassifiesABatchAsTheGateDoes(t *testing.T) {
	t.Parallel()

	for _, paced := range []bool{false, true} {
		rng := rand.New(rand.NewPCG(1, 2))
		g := newGateTracker(epoch)
		var ends int64
		var starts []int64
		for range 12 {
			start := ends + 1 + rng.Int64N(500)
			rewrite := start + 1 + rng.Int64N(400)
			settle := rewrite + rng.Int64N(300)
			g.publish(at(start))
			g.rewriteEnded(at(rewrite))
			g.settleEnded(at(settle))
			g.returned(at(settle))
			ends = settle
			starts = append(starts, start)
		}
		if paced {
			g.setPaced()
			g.startPacing(at(starts[4] + 10)) // some windows start before the pacing
		}
		var batches [][2]int64
		for range 400 {
			s, d := rng.Int64N(ends+600), rng.Int64N(120)
			batches = append(batches, [2]int64{s, d})
			g.batch(at(s), time.Duration(d), 1)
		}
		tr := g.trace(at(ends+1000), "background", time.Hour, nil)
		var recs []PhaseRecord
		for i, b := range batches {
			recs = append(recs, PhaseRecord{Index: i, StartNs: b[0], WallNs: b[1], Records: 1})
		}
		rep := AnalyzePhases(recs, tr)
		for _, c := range []struct {
			class string
			want  int64
		}{
			{PhaseOutside, tr.Outside.Batches},
			{PhaseSettling, tr.Settling.Batches},
			{PhaseInside, tr.Inside.Batches},
			{PhaseUnpaced, tr.Unpaced.Batches},
			{PhaseExcluded, tr.Excluded.Batches},
		} {
			if got := int64(rep.Counts[c.class]); got != c.want {
				t.Errorf("paced %v: %d batches %s, the gate counted %d", paced, got, c.class, c.want)
			}
		}
		if paced && (tr.Excluded.Batches == 0 || tr.Unpaced.Batches == 0) {
			t.Errorf("the paced case does not exercise the unpaced and excluded batches: %d, %d", tr.Unpaced.Batches, tr.Excluded.Batches)
		}
		if !paced && (tr.Inside.Batches == 0 || tr.Settling.Batches == 0 || tr.Outside.Batches == 0) {
			t.Errorf("the case does not exercise every class: %d %d %d", tr.Inside.Batches, tr.Settling.Batches, tr.Outside.Batches)
		}
	}
}

// The tail is the batches at or above the p99 of the wall time; a column's share is
// its time in the tail over the tail's wall time, its excess share is the part of the
// tail's excess over the other batches it accounts for, and the rows that add up to
// the wall time add up to all of both.
func TestThePhaseReportAttributesTheTailToTheColumns(t *testing.T) {
	t.Parallel()

	var recs []PhaseRecord
	// 98 ordinary batches of 10 ms: 8 ms of record commit, 1 ms of state, 1 ms of
	// other; and 2 slow ones of 50 ms: 8 ms of record commit, 30 ms of build (20 of
	// it the walks), 2 ms of the second commit, 10 ms of other.
	for i := range 100 {
		r := PhaseRecord{Index: i, StartNs: int64(i) * 1e9, Records: 1000}
		if i == 40 || i == 90 {
			r.WallNs, r.RecordCommit, r.CkptBuild, r.Walk, r.CkptCommit = 50e6, 8e6, 30e6, 20e6, 2e6
		} else {
			r.WallNs, r.RecordCommit, r.State = 10e6, 8e6, 1e6
		}
		recs = append(recs, r)
	}
	rep := AnalyzePhases(recs, &GateTrace{})
	out := rep.Class(PhaseOutside)
	if out == nil || out.Batches != 100 || rep.Class(PhaseInside) != nil {
		t.Fatalf("classes %+v", rep.Counts)
	}
	if out.TailFrom != 50e6 || out.TailBatches != 2 {
		t.Fatalf("the tail is %d batches from %d, want 2 from 50 ms", out.TailBatches, out.TailFrom)
	}
	near := func(what string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}
	col := func(name string) PhaseTailRow { return out.Tail[phaseColumnIndex(name)] }
	near("build share", col("ckpt_build").Share, 30.0/50)
	near("build+commit share", col("= build+commit").Share, 32.0/50)
	near("walk+commit share", col("= walk+commit").Share, 22.0/50)
	near("record_commit share", col("record_commit").Share, 8.0/50)
	// The excess of the tail over the rest is 40 ms: the build accounts for 30, the
	// second commit 2, the other 9, and the state is 1 less than the rest.
	near("build median share", col("ckpt_build").MedianShare, 30.0/50)
	near("other median share", col("other").MedianShare, 10.0/50)
	near("build excess", col("ckpt_build").ExcessShare, 30.0/40)
	near("commit excess", col("ckpt_commit").ExcessShare, 2.0/40)
	near("state excess", col("state").ExcessShare, -1.0/40)
	near("other excess", col("other").ExcessShare, 9.0/40)
	near("record_commit excess", col("record_commit").ExcessShare, 0)
	var sum float64
	for _, name := range []string{"validate", "state", "invalidate", "record_commit", "ckpt_build", "ckpt_commit", "other"} {
		sum += col(name).ExcessShare
	}
	near("the excess shares add up to", sum, 1)
	if col("ckpt_build").Dominant != 2 {
		t.Errorf("the build led %d tail batches, want 2", col("ckpt_build").Dominant)
	}
	if got := out.Spread[phaseColumnIndex("record_commit")]; got.P50 != 8e6 || got.P99 != 8e6 || got.Max != 8e6 {
		t.Errorf("record_commit spread %+v", got)
	}
	// Each half of the class has one slow batch, the largest of its 50: a tail of one each.
	if out.HalfTail != [2]int{1, 1} {
		t.Errorf("half tails %v, want one batch each", out.HalfTail)
	}
	near("first half build+commit", out.Halves[0][0], 32.0/50)
	near("second half build+commit", out.Halves[1][0], 32.0/50)
}

// Sampled and Other split a batch's wall time, and Other is never negative.
func TestTheUnsampledPartOfABatchIsWhatTheSamplesLeave(t *testing.T) {
	t.Parallel()

	r := PhaseRecord{WallNs: 100, Validate: 1, State: 2, Invalidate: 3, RecordCommit: 40, CkptBuild: 20, CkptCommit: 4}
	if r.Sampled() != 70 || r.Other() != 30 {
		t.Errorf("sampled %d, other %d", r.Sampled(), r.Other())
	}
	r.WallNs = 60 // clock skew between two readings
	if r.Other() != 0 {
		t.Errorf("other %d", r.Other())
	}
}
