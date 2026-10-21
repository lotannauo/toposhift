package doltengine_test

import (
	"context"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

func scenarioNamed(t *testing.T, name string) scenario {
	t.Helper()
	for _, sc := range scenarios {
		if sc.name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return scenario{}
}

// TestMutantsAreCaught switches each deliberate defect of the engine on, one at a time, and
// requires the scenarios written for it to fail against the model. The scenarios are
// deterministic, so a defect either is caught or is not; a scenario listed here that stops
// catching its defect has lost its teeth. The defects of the writer's order (a sequence
// number raised before its commit, an error ending the transaction) are caught by the
// tests that need no server, flow_test.go.
func TestMutantsAreCaught(t *testing.T) {
	for _, c := range []struct {
		fault string
		by    []string
	}{
		{doltengine.FaultDropSeqFilter, []string{"settlement", "late records"}},
		{doltengine.FaultTieByEvent, []string{"ties"}},
		{doltengine.FaultStateKeepsLast, []string{"late records"}},
		{doltengine.FaultCommitAfter, []string{"settlement", "retention"}},
		{doltengine.FaultWindowDedup, []string{"ties"}},
		{doltengine.FaultBatchDropsEmpty, []string{"settlement"}},
		{doltengine.FaultRetainDropsHolding, []string{"retention"}},
		{doltengine.FaultRetainDropsOpenState, []string{"retention"}},
		{doltengine.FaultAcceptBeforeHorizon, []string{"retention"}},
		{doltengine.FaultDeadlineInclusive, []string{"deadlines"}},
		{doltengine.FaultSkipValidate, []string{"refusals"}},
		{doltengine.FaultAcceptStaleSeq, []string{"refusals"}},
		{doltengine.FaultAcceptLayerConflict, []string{"refusals"}},
		{doltengine.FaultIgnoreCapacity, []string{"refusals"}},
		{doltengine.FaultIgnoreProducerLen, []string{"refusals"}},
		// A row used for a key whose event is after the instant gives a wrong answer; the same
		// rule written with a strict comparison gives the same answers and settles fewer keys,
		// which only the counts show; a key not answered from the log, or every key answered
		// from it, is wrong or is settled differently; and the current state read for an old
		// token is wrong.
		{doltengine.FaultUseRowAfterT, []string{"settlement"}},
		{doltengine.FaultEventBefore, []string{"settlement"}},
		{doltengine.FaultNoLogFallback, []string{"settlement"}},
		{doltengine.FaultLogOnly, []string{"settlement"}},
		{doltengine.FaultHeadForOldToken, []string{"settlement"}},
		// A commit at the server's time, or by the session's user, is a commit whose date or
		// author is not the stream's: the server's own log shows it.
		{doltengine.FaultWallClockDate, []string{"settlement"}},
		{doltengine.FaultSessionAuthor, []string{"settlement"}},
	} {
		for _, name := range c.by {
			t.Run(c.fault+"/"+name, func(t *testing.T) {
				t.Parallel()
				e := openEngine(t, doltmodel.ArmCommit)
				e.Break(c.fault)
				p := newPair(t, e, doltmodel.ArmCommit)
				if err := scenarioNamed(t, name).run(context.Background(), p); err == nil {
					t.Fatalf("an engine that %s passed the %s scenario", c.fault, name)
				}
			})
		}
	}
}
