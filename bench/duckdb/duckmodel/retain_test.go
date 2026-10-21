package duckmodel

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func seqsOf(rs []engine.Record) []uint64 {
	out := make([]uint64, len(rs))
	for i, r := range rs {
		out[i] = r.Seq
	}
	slices.Sort(out)
	return out
}

// keptByFilter is the Seqs of the records of a stream that a filter keeps.
func keptByFilter(f *Filter, written []engine.Record) []uint64 {
	for _, r := range written {
		f.Add(r)
	}
	var out []uint64
	for _, r := range written {
		if f.Keep(r) {
			out = append(out, r.Seq)
		}
	}
	slices.Sort(out)
	return out
}

var retentions = [][]float64{{0.5}, {0.3, 0.7}}

// TestFilterKeepsWhatARetainingModelHolds is the exactness the file builder relies on:
// the final horizon applied once to the whole stream keeps the records a model holds
// after being retained at each horizon in turn, as the stream went by, late records
// and all.
func TestFilterKeepsWhatARetainingModelHolds(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	for i, cfg := range conformance.Configs() {
		for _, retain := range retentions {
			m := New(ArmRecords)
			s := stream(t, cfg, 50, retain, []engine.Engine{m})
			got := seqsOf(m.Records())
			want := keptByFilter(NewFilter(s.horizon), s.written)
			if !slices.Equal(got, want) {
				t.Errorf("config %d retain %v: the model holds %d records, the filter keeps %d", i, retain, len(got), len(want))
			}
			// A retention that dropped something and left baselines: the case is not vacuous.
			before := 0
			for _, r := range s.written {
				if r.EventTime.Before(s.horizon) {
					before++
				}
			}
			if len(want) >= len(s.written) || len(want) <= len(s.written)-before {
				t.Errorf("config %d retain %v: %d of %d records are kept, %d are before the horizon: no baseline was dropped or kept", i, retain, len(want), len(s.written), before)
			}
		}
	}
}

// TestBaselineMutantsChangeWhatIsKept: a baseline per subject, or chosen by Seq, keeps
// other records than the honest rule, in the filter and in the model alike.
func TestBaselineMutantsChangeWhatIsKept(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	retain := []float64{0.3, 0.7}
	for name, mut := range map[string]mutant{"one baseline per subject": mutBaselinePerSubject, "a baseline chosen by Seq": mutBaselineBySeq} {
		differs := 0
		for _, cfg := range conformance.Configs() {
			honest, bad := New(ArmRecords), newModel(ArmRecords, mut)
			s := stream(t, cfg, 50, retain, []engine.Engine{honest, bad})
			if !slices.Equal(seqsOf(honest.Records()), keptByFilter(NewFilter(s.horizon), s.written)) {
				t.Fatalf("%s: the honest model and filter differ", name)
			}
			if !slices.Equal(seqsOf(bad.Records()), keptByFilter(NewFilter(s.horizon), s.written)) {
				differs++
			}
			if !slices.Equal(keptByFilter(newFilter(s.horizon, mut), s.written), keptByFilter(NewFilter(s.horizon), s.written)) {
				differs++
			}
		}
		if differs < 2 {
			t.Errorf("%s: changed what was kept in %d of the checks only", name, differs)
		}
	}
}

// sameReads asks two engines every read at and after the horizon, for every entity, in
// every layer, as of the tokens a store guarantees (the newest, the one at the last
// retention and one between), and returns a description of the first answer that
// differs.
func sameReads(t testingT, a, b engine.Engine, s streamed) string {
	t.Helper()
	var times []time.Time
	for i, r := range s.written {
		if i%53 != 0 || r.EventTime.Before(s.horizon) {
			continue
		}
		last := r.EventTime
		if r.Through.After(last) {
			last = r.Through
		}
		times = append(times, r.EventTime.Add(-time.Nanosecond), r.EventTime, r.Through, last.Add(r.TTL-time.Nanosecond), last.Add(r.TTL))
	}
	times = append(times, s.horizon, s.written[len(s.written)-1].EventTime.Add(time.Hour), engine.MaxEventTime)
	times = slices.DeleteFunc(times, func(x time.Time) bool { return x.Before(s.horizon) || x.IsZero() })
	tokens := []uint64{engine.Latest, s.floor, s.floor + (a.LastSeq()-s.floor)/2}
	for _, ent := range s.entities {
		for _, layer := range layers {
			for _, tok := range tokens {
				sc := engine.Scope{Layer: layer, AsOf: tok}
				for _, x := range times {
					ga, err1 := a.Alive(ent, x, sc)
					gb, err2 := b.Alive(ent, x, sc)
					if err1 != nil || err2 != nil || ga != gb {
						return "Alive " + ent.String() + " " + x.Format(time.RFC3339Nano)
					}
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						na, err1 := a.Neighbors(ent, dir, x, sc)
						nb, err2 := b.Neighbors(ent, dir, x, sc)
						if err1 != nil || err2 != nil || !slices.Equal(na, nb) {
							return "Neighbors " + dir.String() + " " + ent.String() + " " + x.Format(time.RFC3339Nano)
						}
					}
				}
			}
		}
	}
	return ""
}

// retainedReadsDiffer streams a workload into a model that is retained at the horizons
// and into one that is not, and reports where their reads at and after the last horizon
// differ.
func retainedReadsDiffer(t testingT, arm Arm, mut mutant, cfg workload.Config, retain []float64) string {
	t.Helper()
	retained, full := newModel(arm, mut), New(arm)
	// The unretained model is never retained: it is given the same batches, and the
	// horizon only decides which records the stream drops.
	s := stream(t, cfg, 50, retain, []engine.Engine{retained}, full)
	return sameReads(t, retained, full, s)
}

// TestARetentionLeavesReadsAtAndAfterTheHorizonUnchanged: for any workload and any
// horizons, a model retained at them answers every read at or after the last horizon, as
// of a token at or above the one at that retention, as a model that kept everything does.
func TestARetentionLeavesReadsAtAndAfterTheHorizonUnchanged(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	cfgs := conformance.Configs()
	rapid.Check(t, func(rt *rapid.T) {
		cfg := cfgs[rapid.IntRange(0, len(cfgs)-1).Draw(rt, "config")]
		retain := rapid.SampledFrom([][]float64{{0.25}, {0.5}, {0.8}, {0.3, 0.7}, {0.2, 0.4, 0.9}}).Draw(rt, "retain")
		arm := rapid.SampledFrom(arms).Draw(rt, "arm")
		if d := retainedReadsDiffer(rt, arm, mutNone, cfg, retain); d != "" {
			rt.Fatalf("%s, retain %v: reads differ at %s", arm, retain, d)
		}
	})
}

// TestRetentionMutantsAreCaughtByTheReads: on the fixed workloads, a baseline per subject
// or chosen by Seq changes some read at or after the horizon, and the honest rule none.
func TestRetentionMutantsAreCaughtByTheReads(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	retain := []float64{0.3, 0.7}
	for _, arm := range arms {
		for i, cfg := range conformance.Configs() {
			if d := retainedReadsDiffer(t, arm, mutNone, cfg, retain); d != "" {
				t.Fatalf("%s, config %d: the honest model's reads differ at %s", arm, i, d)
			}
		}
		for name, mut := range map[string]mutant{"one baseline per subject": mutBaselinePerSubject, "a baseline chosen by Seq": mutBaselineBySeq} {
			caught := false
			for _, cfg := range conformance.Configs() {
				if retainedReadsDiffer(t, arm, mut, cfg, retain) != "" {
					caught = true
					break
				}
			}
			if !caught {
				t.Errorf("%s: %s was not caught by a read", arm, name)
			}
		}
	}
}
