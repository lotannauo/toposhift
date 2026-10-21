package duckengine

import (
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// detectors are the checks a fault in a statement is held to: the conformance suite's
// scripted checks and its fixed workloads with retentions. None draws at random, so what
// they catch is the same on every run.
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

// TestSQLAnswersAsTheModelDoes holds every statement of every arm to the engine contract:
// reads of files that were built from the records an engine was given, after the
// retention, answer as the reference engine does, on the scripted checks and on fixed
// workloads with mid-stream retentions.
func TestSQLAnswersAsTheModelDoes(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	for _, arm := range Arms {
		for name, d := range detectors {
			t.Run(string(arm)+"/"+name, func(t *testing.T) {
				t.Parallel()
				if err := d(newFileEngine(t, arm, sqlHonest)); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestTheChecksCatchTheFaultsOfTheStatements runs, for each deliberate fault of a
// statement, the check the guide names for it against a file engine whose statements
// have the fault: the check must fail. The honest statements pass them above.
//
// A fault the guide lists that is not here is no fault: a through time is never before
// the event time (the lifecycle specification refuses it), so counting a deadline from
// the through time alone, or from the larger of the two, or with the coalesce left out
// where greatest already skips a null, answers every read alike.
func TestTheChecksCatchTheFaultsOfTheStatements(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	cases := []struct {
		name string
		mut  sqlMutant
		arms []Arm
		by   string
	}{
		{"the newest record of an edge, not of each producer's reference", sqlNewestPerEdge, []Arm{ArmA2, ArmI}, "producers"},
		{"a tie at one instant broken by nothing, not the higher Seq", sqlNewestByEventOnly, []Arm{ArmA2}, "instant"},
		{"a deadline reached still holds", sqlDeadlineInclusive, []Arm{ArmA2}, "instant"},
		{"a deadline from the event time, not the through time", sqlThroughIgnored, []Arm{ArmA2}, "instant"},
		{"a reverse read of the record file by its source", sqlReverseBySource, []Arm{ArmA1, ArmA2, ArmI}, "instant"},
		{"the snapshot token left out", sqlTokenIgnored, []Arm{ArmA2, ArmI}, "instant"},
		{"a window whose upper bound is inclusive", sqlWindowInclusive, []Arm{ArmA2, ArmI}, "instant"},
		{"a window with no order", sqlWindowUnordered, []Arm{ArmA1, ArmA2, ArmI}, "producers"},
		{"an interval that still holds when it ends", sqlIntervalEndInclusive, []Arm{ArmI}, "instant"},
		{"an interval with no end treated as ended", sqlOpenIntervalDropped, []Arm{ArmI}, "workload with retentions"},
	}
	for _, c := range cases {
		for _, arm := range c.arms {
			t.Run(c.by+"/"+string(arm)+"/"+c.name, func(t *testing.T) {
				t.Parallel()
				if err := detectors[c.by](newFileEngine(t, arm, c.mut)); err == nil {
					t.Errorf("the check %q did not fail on %s", c.by, c.name)
				}
			})
		}
	}
}
