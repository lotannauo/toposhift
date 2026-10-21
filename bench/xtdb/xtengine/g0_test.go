package xtengine

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/xtdb/g0"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
)

// hourSpec is an hour of the small cluster, the spec the model's own gate uses: long
// enough for every kind of read, windows included, and an old snapshot.
func hourSpec() runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	return s
}

// retainingSpec is hourSpec with a retention at 40 minutes, so that the later reads,
// and the old snapshot, are made on what a retention left.
func retainingSpec() runner.Spec {
	s := hourSpec()
	s.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	return s
}

// tinySpec is twenty minutes of the tiny cluster: a few thousand records and about two hundred
// questions, of every kind but a window, in batches of 50. It is what the live node is
// given, because a node takes milliseconds to write a record and to answer a read, and an hour
// of the small cluster is a hundred thousand records.
func tinySpec() runner.Spec {
	s := runner.DefaultSpec(workload.Tiny())
	s.MinNonEmpty = 0
	s.BatchSize = 50
	return s
}

func tinyRetainingSpec() runner.Spec {
	s := tinySpec()
	s.Retentions = []runner.Retention{{At: 14 * time.Minute, Keep: 6 * time.Minute}}
	return s
}

func makePlan(t testing.TB, spec runner.Spec) *runner.Plan {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// checkPlan is the first gate for an engine: the stream of a plan is regenerated into it,
// and every question of the plan, at every age, in digest and in size, has the answer the
// reference engine gave. The statements it wrote are counted as the model's planner
// counts them for the same stream, and no read of the plan was answered by a client fold:
// every token of a plan is the end of a batch.
func checkPlan(t testing.TB, p *runner.Plan, e *Engine) {
	t.Helper()
	rep, err := g0.Run(context.Background(), p, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.Queries != len(p.Queries) {
		t.Fatalf("%d of %d answers equal the reference's; first mismatches %+v", rep.Matched, rep.Queries, rep.Mismatches)
	}
	for _, g := range rep.Groups {
		if g.Matched != g.Queries {
			t.Errorf("%s at %s: %d of %d", g.Group, g.Age, g.Matched, g.Queries)
		}
	}
	if rc := e.ReadCounts(); rc.ClientFold != 0 || rc.Native < uint64(len(p.Queries)) {
		t.Errorf("reads: %+v for %d questions", rc, len(p.Queries))
	}
	m := xtmodel.New()
	if _, err := g0.Run(context.Background(), p, m, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := e.Counts(), m.Counts(); !reflect.DeepEqual(got, want) {
		t.Errorf("the engine counted %+v, the model %+v", got, want)
	}
	if r := e.Refused(); r.OffMicrosecond != 0 {
		t.Errorf("%d records have a boundary off a whole microsecond", r.OffMicrosecond)
	}
}

// TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid on the fake node: the plans the
// model passes, through the engine's statements.
func TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid(t *testing.T) {
	for name, spec := range map[string]runner.Spec{"no retention": hourSpec(), "a retention": retainingSpec(), "tiny": tinySpec(), "tiny with a retention": tinyRetainingSpec()} {
		t.Run(name, func(t *testing.T) {
			checkPlan(t, makePlan(t, spec), openFake(t, newFakeNode(), 0))
		})
	}
}
