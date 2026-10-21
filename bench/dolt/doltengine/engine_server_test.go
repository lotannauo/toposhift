package doltengine_test

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var arms = []doltmodel.Arm{doltmodel.ArmCommit, doltmodel.ArmRows}

// TestConformance is the engine contract on a live server: both arms are checked against the
// reference engine on random and fixed workloads with mid-stream retentions, on the
// scripted checks, on a reopen and on concurrent reads.
func TestConformance(t *testing.T) {
	for _, arm := range arms {
		t.Run(arm.String(), func(t *testing.T) { conformance.Run(t, factory(t, arm)) })
	}
}

// TestTheEngineIsHeldToTheModel runs every scenario on a model and an engine of each arm and
// requires them to agree on every answer, every counter, every table and every commit.
func TestTheEngineIsHeldToTheModel(t *testing.T) {
	for _, arm := range arms {
		for _, sc := range scenarios {
			t.Run(arm.String()+"/"+sc.name, func(t *testing.T) {
				t.Parallel()
				p := newPair(t, openEngine(t, arm), arm)
				if err := sc.run(context.Background(), p); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// genRecord draws one record of a small universe, as the model's own property test does.
func genRecord(t *rapid.T, label string, floorSecs int64) engine.Record {
	edges := []engine.Subject{edge1, edge2, edge3}
	subject := rapid.SampledFrom(append(edges, engine.EntitySubject(pod1), engine.EntitySubject(node1))).Draw(t, label+" subject")
	producer := rapid.SampledFrom([]lifecycle.Producer{"a", "b", "c"}).Draw(t, label+" producer")
	secs := rapid.Int64Range(max(floorSecs, 0), 60).Draw(t, label+" secs")
	r := engine.Record{Layer: catalog.L2, Subject: subject, Producer: producer, EventTime: at(secs), Kind: lifecycle.Observe, Payload: []byte{1}}
	if subject.Kind == engine.SubjectEntity {
		e, _ := catalog.Default().Entity(subject.A.Type())
		r.Layer = e.Layer()
	}
	switch rapid.IntRange(0, 5).Draw(t, label+" shape") {
	case 0:
		r.Kind, r.Payload = lifecycle.Delete, nil
	case 1:
		r.TTL = time.Duration(rapid.Int64Range(1, 30).Draw(t, label+" ttl")) * time.Second
	case 2:
		r.TTL = time.Duration(rapid.Int64Range(1, 30).Draw(t, label+" ttl")) * time.Second
		r.Through = at(secs + rapid.Int64Range(0, 20).Draw(t, label+" through"))
	}
	return r
}

// TestRandomStreamsAreHeldToTheModel: random streams of the tiny universe, with retentions,
// agree with the model at the end of each.
func TestRandomStreamsAreHeldToTheModel(t *testing.T) {
	for _, arm := range arms {
		t.Run(arm.String(), func(t *testing.T) {
			dsn := serverDSN(t)
			rapid.Check(t, func(rt *rapid.T) {
				name := doltengine.DatabaseName(fmt.Sprintf("%s/%d", t.TempDir(), time.Now().UnixNano()))
				e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
				if err != nil {
					rt.Fatal(err)
				}
				defer func() {
					_ = e.Close()
					_ = doltengine.DropDatabase(context.Background(), dsn, name)
				}()
				p := newPair(t, e, arm)
				seq := uint64(rapid.SampledFrom([]uint64{0, 1<<32 - 3, 1<<63 - 3}).Draw(rt, "first seq"))
				for b := range rapid.IntRange(1, 6).Draw(rt, "batches") {
					var batch []engine.Record
					for i := range rapid.IntRange(1, 5).Draw(rt, "batch size") {
						r := genRecord(rt, fmt.Sprintf("record %d.%d", b, i), doltmodel.Nanos(p.horizon)/int64(time.Second))
						seq++
						r.Seq = seq
						batch = append(batch, r)
					}
					if err := p.write(batch...); err != nil {
						rt.Fatal(err)
					}
					if rapid.IntRange(0, 3).Draw(rt, "retain") == 0 {
						if err := p.retain(at(rapid.Int64Range(10, 60).Draw(rt, "horizon"))); err != nil {
							rt.Fatal(err)
						}
					}
				}
				if err := p.agree(context.Background()); err != nil {
					rt.Fatal(err)
				}
			})
		})
	}
}

func hourSpec(retain bool) runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	if retain {
		s.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	}
	return s
}

// TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceDid is the first gate on a live server, for
// the plans the model passes: the stream of a plan is regenerated into each arm, and every
// question of the plan, at every age, in digest and in size, has the answer the reference
// engine gave. The engine settles every key the way the model does, and its tables are the
// model's.
func TestEveryQuestionOfAPlanIsAnsweredAsTheReferenceDid(t *testing.T) {
	for _, retain := range []bool{false, true} {
		plan, err := runner.MakePlan(context.Background(), hourSpec(retain), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, arm := range arms {
			t.Run(fmt.Sprintf("retain %v/%s", retain, arm), func(t *testing.T) {
				t.Parallel()
				e := openEngine(t, arm)
				rep, err := g0.Run(context.Background(), plan, e, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !rep.OK() || rep.Queries != len(plan.Queries) {
					t.Fatalf("%d of %d answers equal the reference's; first mismatches %+v", rep.Matched, rep.Queries, rep.Mismatches)
				}
				model := doltmodel.New(doltmodel.WithArm(arm), doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
				mrep, err := g0.Run(context.Background(), plan, model, nil)
				if err != nil {
					t.Fatal(err)
				}
				if got, want := rep.TotalPaths(), mrep.TotalPaths(); !maps.Equal(got, want) {
					t.Errorf("the reads of the plan were settled differently:\n the engine %v\n the model  %v", got, want)
				}
				for i, g := range rep.Groups {
					if g.Matched != g.Queries || !maps.Equal(g.Paths, mrep.Groups[i].Paths) {
						t.Errorf("%s at %s: %d of %d; paths %v, the model's %v", g.Group, g.Age, g.Matched, g.Queries, g.Paths, mrep.Groups[i].Paths)
					}
				}
				if e.Counts() != model.Counts() {
					t.Errorf("writer counts: the engine %+v, the model %+v", e.Counts(), model.Counts())
				}
				got, err := e.Tables(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if d := got.FirstDifference(model.Tables()); d != "" {
					t.Errorf("the tables differ from the model's: %s", d)
				}
				// The old snapshot of the arm with commits is settled from history: most keys come
				// from the state as of the commit, and no read lacks a commit.
				var noCommit uint64
				for k, v := range rep.TotalPaths() {
					if strings.HasSuffix(k, "/no-commit") {
						noCommit += v
					}
				}
				if (arm == doltmodel.ArmCommit) == (noCommit != 0) {
					t.Errorf("%s: %d reads had no commit to read", arm, noCommit)
				}
			})
		}
	}
}

// TestAWorkloadIsHeldToTheModelAtScale writes a workload of a few thousand records, with
// retentions, to a model and an engine in batches, and compares the tables and the counters
// after each, with the probes at the end.
func TestAWorkloadIsHeldToTheModelAtScale(t *testing.T) {
	cfg := conformance.Configs()[1] // late records, a second producer
	for _, arm := range arms {
		t.Run(arm.String(), func(t *testing.T) {
			p := newPair(t, openEngine(t, arm), arm)
			g, err := workload.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			span := g.End().Sub(g.Start())
			next, retains := 0, []float64{0.3, 0.7}
			var horizon time.Time
			ctx := context.Background()
			for n := 0; ; n++ {
				recs := g.Batch(60)
				if len(recs) == 0 {
					break
				}
				kept := recs[:0]
				for _, r := range recs {
					if !r.EventTime.Before(horizon) {
						kept = append(kept, r)
					}
				}
				if len(kept) == 0 {
					continue
				}
				if err := p.write(kept...); err != nil {
					t.Fatal(err)
				}
				if n%6 == 0 {
					if err := p.storesAgree(ctx); err != nil {
						t.Fatalf("after batch %d: %v", n, err)
					}
				}
				if next < len(retains) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retains[next]*float64(span)))) {
					horizon = g.Start().Add(time.Duration(retains[next] * float64(span)))
					next++
					if err := p.retain(horizon); err != nil {
						t.Fatal(err)
					}
					g.SetHorizon(horizon)
					if err := p.storesAgree(ctx); err != nil {
						t.Fatalf("after the retention at %v: %v", horizon, err)
					}
				}
			}
			if err := p.storesAgree(ctx); err != nil {
				t.Fatal(err)
			}
			// Reads of the keys the workload used, as of some tokens, not every one.
			pr := p.probesFor()
			pr.tokens = pr.tokens[max(0, len(pr.tokens)-4):]
			pr.instants = pr.instants[len(pr.instants)/2:]
			pr.instants = pr.instants[:min(len(pr.instants), 8)]
			if err := p.readsAgree(pr); err != nil {
				t.Fatal(err)
			}
		})
	}
}
