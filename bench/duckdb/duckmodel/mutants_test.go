package duckmodel

import (
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// detectors are the checks a mutant of the read rule is held to: the conformance suite's
// scripted checks, and its fixed workloads with two retentions. None draws at random, so
// what they catch is the same on every run.
var detectors = map[string]func(engine.Engine) error{
	"instant":   conformance.CheckInstant,
	"producers": conformance.CheckProducers,
	"relations": conformance.CheckRelations,
	"extremes":  conformance.CheckExtremes,
	"workload with retentions": func(e engine.Engine) error {
		return conformance.Check(e, conformance.Configs()[0], conformance.Options{RetainAt: []float64{0.3, 0.7}})
	},
	"workload with runs and retentions": func(e engine.Engine) error {
		return conformance.Check(e, conformance.Configs()[5], conformance.Options{RetainAt: []float64{0.3, 0.7}})
	},
}

// TestConformanceCatchesTheMutantsOfTheReadRule runs the checks the guide names against
// a model with one deliberate fault each: a check named for a fault must fail on it, on
// both arms, and pass on the model without it.
func TestConformanceCatchesTheMutantsOfTheReadRule(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	cases := []struct {
		name string
		mut  mutant
		by   []string
	}{
		{"a tie at one instant broken by event time alone", mutTieByEventOnly, []string{"instant"}},
		{"a deadline from the event time, not the through time", mutDeadlineFromEvent, []string{"instant", "workload with runs and retentions"}},
		{"a delete treated as holding", mutDeleteHolds, []string{"producers", "instant"}},
		{"a TTL of zero treated as lapsed", mutTTLZeroExpires, []string{"producers", "instant"}},
		{"the snapshot token not applied", mutSeqIgnored, []string{"producers", "instant"}},
		{"Window leaving out an overwritten record", mutWindowDedups, []string{"instant"}},
		{"a batch dropping an entity with no neighbor", mutBatchDropsEmpty, []string{"producers", "instant"}},
		{"a batch answering in sorted order", mutBatchSorted, []string{"producers", "instant"}},
		{"an instant converted without clamping", mutNoClamp, []string{"extremes"}},
		{"a window whose upper bound is inclusive", mutWindowInclusive, []string{"instant"}},
		{"one baseline per subject", mutBaselinePerSubject, []string{"workload with retentions"}},
		{"a baseline chosen by Seq", mutBaselineBySeq, []string{"workload with retentions"}},
	}
	for _, arm := range arms {
		for name, d := range detectors {
			if err := d(New(arm)); err != nil {
				t.Fatalf("%s: the check %q fails on the honest model: %v", arm, name, err)
			}
		}
		for _, c := range cases {
			for _, name := range c.by {
				err := detectors[name](newModel(arm, c.mut))
				if err == nil {
					t.Errorf("%s: %s: the check %q did not fail", arm, c.name, name)
				}
			}
		}
	}
}

// TestAFaultInTheFoldIsAFaultOfTheIntervalsArm: a fault in the fold of intervals is a fault of
// the intervals arm only, and conformance sees it there, as far as answers go. What it
// cannot see, an end source or an end bound that only the last instant reaches, the rows
// tests do.
func TestAFaultInTheFoldIsAFaultOfTheIntervalsArm(t *testing.T) {
	if err := detectors["producers"](newModel(ArmIntervals, mutIntervalsSplit)); err == nil {
		t.Error("intervals left split by producer were not caught on the intervals arm")
	}
	if err := detectors["producers"](newModel(ArmRecords, mutIntervalsSplit)); err != nil {
		t.Errorf("the records arm does not fold, and a fault in the fold changed it: %v", err)
	}
}
