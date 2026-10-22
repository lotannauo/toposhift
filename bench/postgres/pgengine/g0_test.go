package pgengine_test

import (
	"context"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
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

func makePlan(t *testing.T, spec runner.Spec) *runner.Plan {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid is the first gate on a
// live server, for the plans the in-memory model passes: the stream of a plan is
// regenerated into an arm, and every question of the plan, at every age, in digest
// and in size, has the answer the reference engine gave. The engine's rows are also
// the model's, clipped to the horizon of the last retention.
func TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid(t *testing.T) {
	for _, c := range []struct {
		name string
		spec runner.Spec
	}{
		{"no retention", hourSpec()},
		{"a retention", retainingSpec()},
	} {
		p := makePlan(t, c.spec)
		for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition, pgschema.StreamNaive, pgschema.Bulk, pgschema.BulkB} {
			t.Run(c.name+" "+string(arm), func(t *testing.T) {
				// Each arm has an engine, and a schema, of its own.
				t.Parallel()
				e := openEngine(t, arm)
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
				// The rows are the model's for the same stream.
				model := pgmodel.New()
				if _, err := g0.Run(context.Background(), p, model, nil); err != nil {
					t.Fatal(err)
				}
				got, err := e.Tables(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var horizon int64
				if n := len(rep.Stream.Retentions); n > 0 {
					horizon = pgmodel.ToNanos(rep.Stream.Retentions[n-1].Horizon)
				}
				if g, w := got.Clipped(horizon), model.Tables().Clipped(horizon); !g.Equal(w) {
					t.Errorf("the rows differ from the model's: %s", g.FirstDifference(w))
				}
			})
		}
	}
}
