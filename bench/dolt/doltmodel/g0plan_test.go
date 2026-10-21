package doltmodel_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
)

func planOf(t *testing.T, spec runner.Spec) *runner.Plan {
	t.Helper()
	spec.MinNonEmpty = 0
	spec.BatchSize = 200
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestADefectOfTheModelIsAMismatchOnAPlan: defects inside the model, not only in what it
// returns, are found by the questions of a plan that reaches them (an hour of the small
// cluster with a retention in it).
func TestADefectOfTheModelIsAMismatchOnAPlan(t *testing.T) {
	spec := runner.DefaultSpec(workload.Small())
	spec.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	p := planOf(t, spec)
	if rep, err := g0.Run(context.Background(), p, doltmodel.New(), nil); err != nil || !rep.OK() {
		t.Fatalf("the unbroken model: %v, %+v", err, rep)
	}
	// The faults this plan reaches; the others are found by the checks of mutants_test.go.
	for _, fault := range []string{
		doltmodel.FaultTieByEvent, doltmodel.FaultUseRowAfterT, doltmodel.FaultRetainDropsHolding, doltmodel.FaultStateKeepsLast,
	} {
		m := doltmodel.New()
		m.Break(fault)
		rep, err := g0.Run(context.Background(), p, m, nil)
		if err != nil {
			t.Fatalf("%s: %v", fault, err)
		}
		if rep.OK() {
			t.Errorf("a model that %s answered every question of the plan as the reference did", fault)
		}
	}
}

// TestADiagnosticReadingTheLogIsNotWrongOnAPlanWithLateRecords is the plan-level form of the
// check on the diagnostic: with the log substituted for the commits it answers every
// question as the reference does, which is the defect its wrong-answer count exists to
// expose.
func TestADiagnosticReadingTheLogIsNotWrongOnAPlanWithLateRecords(t *testing.T) {
	spec := runner.DefaultSpec(workload.Small())
	spec.Workload.LateProbability, spec.Workload.LateMeanDelay = 0.4, 5*time.Minute
	p := planOf(t, spec)
	d := doltmodel.NewCommitTime()
	d.Break(doltmodel.FaultDiagUsesLog)
	rep, err := g0.Run(context.Background(), p, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Queries != rep.Matched {
		t.Fatalf("a diagnostic reading the log was wrong %d times", rep.Queries-rep.Matched)
	}
}

var (
	hourOnce sync.Once
	hourPlan *runner.Plan
	hourErr  error
)

// theHourPlan is an hour of the small cluster with every kind of read, the old snapshot
// included, made once.
func theHourPlan() (*runner.Plan, error) {
	hourOnce.Do(func() {
		spec := runner.DefaultSpec(workload.Small())
		spec.MinNonEmpty = 0
		spec.BatchSize = 200
		hourPlan, hourErr = runner.MakePlan(context.Background(), spec, nil)
	})
	return hourPlan, hourErr
}

// checkOldTokenSettledFromHistory is the check that the rule does not silently become the log
// path: the plan's questions about the old snapshot answer as the reference does, and settle
// most of their keys from the state as of the commit at the token, with the log asked only
// for the rest.
func checkOldTokenSettledFromHistory(m *doltmodel.Model) error {
	p, err := theHourPlan()
	if err != nil {
		return err
	}
	rep, err := g0.Run(context.Background(), p, m, nil)
	if err != nil {
		return err
	}
	if !rep.OK() {
		return fmt.Errorf("%d of %d answers are not the reference's", rep.Queries-rep.Matched, rep.Queries)
	}
	var state, log, alone, needed, noCommit uint64
	for _, g := range rep.Groups {
		for k, v := range g.Paths {
			if strings.HasSuffix(k, "/no-commit") {
				noCommit += v
			}
		}
		if g.Age != runner.AgeOldToken {
			continue
		}
		for k, v := range g.Paths {
			switch {
			case strings.HasSuffix(k, "/keys-state"):
				state += v
			case strings.HasSuffix(k, "/keys-log"):
				log += v
			case strings.HasSuffix(k, "/as-of-alone"):
				alone += v
			case strings.HasSuffix(k, "/needed-log"):
				needed += v
			}
		}
	}
	if noCommit != 0 {
		return fmt.Errorf("%d reads of the plan had no commit to read: every token of a plan is a batch boundary, so none may", noCommit)
	}
	if state == 0 || state <= log || alone == 0 {
		return fmt.Errorf("the old snapshot's reads settled %d keys from the state and %d from the log, %d reads from the state alone and %d with the log", state, log, alone, needed)
	}
	return nil
}

// checkRowAtInstant: a row whose event time is at or before the instant settles its key,
// equal included, which no answer shows (the log would return the same record) but the keys
// counted do; and a key with a row after the instant goes to the log beside one that does not.
func checkRowAtInstant(m *doltmodel.Model) error {
	if err := m.Write([]engine.Record{observe(edge1, 1, "a", 100, 0)}); err != nil {
		return err
	}
	for secs, want := range map[int64]settled{99: {0, 1, false}, 100: {1, 0, true}, 101: {1, 0, true}} {
		var err error
		got := counted(m, "neighbors", func() {
			_, err = m.Neighbors(node1, engine.Reverse, at(secs), engine.Current(catalog.L2))
		})
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("at %d: settled %+v, want %+v", secs, got, want)
		}
	}
	// Two producers, one before the instant and one after: one key settled, one in the log.
	if err := m.Write([]engine.Record{observe(edge1, 2, "b", 110, 0)}); err != nil {
		return err
	}
	got := counted(m, "neighbors", func() { _, _ = m.Neighbors(node1, engine.Reverse, at(105), engine.Current(catalog.L2)) })
	if got != (settled{1, 1, false}) {
		return fmt.Errorf("two producers: settled %+v", got)
	}
	return nil
}

func TestARowAtTheInstantSettlesItsKey(t *testing.T) {
	if err := checkRowAtInstant(doltmodel.New()); err != nil {
		t.Fatal(err)
	}
}

// TestNoReadOfAPlanLacksACommit: in the arm with commits every token of a plan names a commit,
// so none of its reads is answered from the log for want of one; the arm without commits has
// none to name for its old token, and counts those reads on their own.
func TestNoReadOfAPlanLacksACommit(t *testing.T) {
	p, err := theHourPlan()
	if err != nil {
		t.Fatal(err)
	}
	for arm, want := range map[doltmodel.Arm]bool{doltmodel.ArmCommit: false, doltmodel.ArmRows: true} {
		rep, err := g0.Run(context.Background(), p, doltmodel.New(doltmodel.WithArm(arm)), nil)
		if err != nil || !rep.OK() {
			t.Fatalf("%s: %v, %d of %d", arm, err, rep.Matched, rep.Queries)
		}
		var none uint64
		for k, v := range rep.TotalPaths() {
			if strings.HasSuffix(k, "/no-commit") {
				none += v
			}
		}
		if (none != 0) != want {
			t.Errorf("%s: %d reads had no commit to read, want some: %v", arm, none, want)
		}
	}
}

func TestTheOldSnapshotsReadsAreSettledFromHistory(t *testing.T) {
	if err := checkOldTokenSettledFromHistory(doltmodel.New(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))); err != nil {
		t.Fatal(err)
	}
	// The control has no commits, so it settles none of them from history.
	if err := checkOldTokenSettledFromHistory(doltmodel.New(doltmodel.WithArm(doltmodel.ArmRows))); err == nil {
		t.Fatal("the arm with no commits settled the old snapshot's keys from history")
	}
}
