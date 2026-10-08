package runner_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// phased is an engine that says how its last retention spent its time, as an engine
// that settles a retention's tombstones does. The figures are invented and depend on
// the number of the retention.
type phased struct {
	fullEngine
	retentions int
}

func (p *phased) Retain(h time.Time) error {
	p.retentions++
	return p.fullEngine.Retain(h)
}

func (p *phased) LastRetain() (work, flush, settle time.Duration, deadlineHit bool) {
	n := time.Duration(p.retentions)
	return n * time.Millisecond, 2 * n * time.Millisecond, 3 * n * time.Millisecond, p.retentions%2 == 0
}

// silent is an engine that reports nothing of how a retention was spent, whatever the
// engine it wraps would report: it stands for one that has no such method.
type silent struct {
	fullEngine
}

// A build records, for every retention, the phases an engine reports for it, and for an
// engine that reports none it records none.
func TestABuildRecordsHowAnEngineSpentEachRetention(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	variant := candidates.Wrap(lookup(t, "M/crdb1"), "M/phased", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) {
		return &phased{fullEngine: e.(fullEngine)}, nil
	})
	_, m, _ := run(t, plan, variant)
	retentions := len(plan.Stream.Retentions)
	if retentions == 0 || len(m.Timing.RetainPhases) != retentions {
		t.Fatalf("%d phases recorded for %d retentions", len(m.Timing.RetainPhases), retentions)
	}
	for i, p := range m.Timing.RetainPhases {
		n := int64(i + 1)
		want := runner.RetainPhase{Work: n * int64(time.Millisecond), Flush: 2 * n * int64(time.Millisecond), Settle: 3 * n * int64(time.Millisecond), DeadlineHit: n%2 == 0}
		if p != want {
			t.Errorf("retention %d: phases %+v, want %+v", i, p, want)
		}
	}

	quiet := candidates.Wrap(lookup(t, "M/crdb1"), "M/silent", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) {
		return &silent{fullEngine: e.(fullEngine)}, nil
	})
	_, plain, _ := run(t, plan, quiet)
	if len(plain.Timing.RetainPhases) != 0 {
		t.Errorf("phases were recorded for an engine that says nothing of them: %+v", plain.Timing.RetainPhases)
	}
}

// The rules say when they were last set and by which version, so that a change to what a
// gate means is not made without a dated entry in the log.
func TestRulesLogIsDated(t *testing.T) {
	t.Parallel()

	b, err := os.ReadFile("rules.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Log (2026-10-08", "RulesVersion is 5", "const RulesVersion = 5"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("rules.go does not hold %q", want)
		}
	}
}
