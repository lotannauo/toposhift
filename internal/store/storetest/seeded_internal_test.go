package storetest

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The fixed workloads, with the suite's retention schedules, reach what the
// WindowSince checks exist for: seeds with and without their payload, seeds decided
// before the horizon, windows that start at the horizon with seeds, references the
// horizon rule leaves out there, seeds whose deadline is the window's start,
// refusals, and the fold identity. The seeds are fixed, so the counts are too.
func TestTheSinceChecksReachWhatTheyAreFor(t *testing.T) {
	t.Parallel()
	var total sinceCoverage
	for _, w := range Workloads() {
		for _, retain := range [][]float64{nil, {0.5}, {0.3, 0.7}} {
			s, err := memstore.Open(memstore.Options{Policy: w.Policy})
			if err != nil {
				t.Fatal(err)
			}
			cov, err := checkSince(s, w, Options{RetainAt: retain, Entities: 6, Probes: 4})
			_ = s.Close()
			if err != nil {
				t.Fatalf("workload %q retain %v: %v", w.Name, retain, err)
			}
			total.calls += cov.calls
			total.seeds += cov.seeds
			total.payloadKnown += cov.payloadKnown
			total.unknown += cov.unknown
			total.atHorizon += cov.atHorizon
			total.deadlineAtFrom += cov.deadlineAtFrom
			total.deadlineOnHorizon += cov.deadlineOnHorizon
			total.beforeHorizon += cov.beforeHorizon
			total.refused += cov.refused
			total.foldChecks += cov.foldChecks
		}
	}
	t.Logf("coverage of the fixed workloads: %+v", total)
	for name, n := range map[string]int{
		"calls": total.calls, "seeds": total.seeds, "seeds with their payload": total.payloadKnown,
		"seeds whose payload is not known": total.unknown, "windows from the horizon with seeds": total.atHorizon,
		"seeds whose deadline is the window's start": total.deadlineAtFrom, "seeds decided before the horizon": total.beforeHorizon,
		"refusals": total.refused, "fold identity checks": total.foldChecks,
		"references the horizon rule left out": total.deadlineOnHorizon,
	} {
		if n == 0 {
			t.Errorf("the fixed workloads reached no %s", name)
		}
	}
}
