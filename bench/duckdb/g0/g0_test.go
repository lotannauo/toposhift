package g0_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/duckdb/g0"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

var arms = []duckmodel.Arm{duckmodel.ArmRecords, duckmodel.ArmIntervals}

// hourSpec is an hour of the small cluster, the spec the runner's own tests use: long
// enough for every kind of read, windows included, and an old snapshot.
func hourSpec() runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	return s
}

// retainingSpec is hourSpec with a retention at 40 minutes, so that the later reads, and
// the old snapshot, are made on what a retention left. Its reads reach back no further
// than the horizon, so it has no window.
func retainingSpec() runner.Spec {
	s := hourSpec()
	s.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	return s
}

func plan(t *testing.T, spec runner.Spec) *runner.Plan {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTheModelAnswersEveryQuestionOfAPlanAsTheReferenceEngineDid(t *testing.T) {
	for name, spec := range map[string]runner.Spec{"no retention": hourSpec(), "a retention": retainingSpec()} {
		p := plan(t, spec)
		for _, arm := range arms {
			t.Run(name+" "+arm.String(), func(t *testing.T) { checkPlan(t, p, arm, name == "no retention") })
		}
	}
}

func checkPlan(t *testing.T, p *runner.Plan, arm duckmodel.Arm, windows bool) {
	t.Helper()
	rep, err := g0.Run(context.Background(), p, duckmodel.New(arm), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.Queries != len(p.Queries) || len(rep.Mismatches) != 0 {
		t.Fatalf("%d of %d answers equal the reference's; first mismatches %+v", rep.Matched, rep.Queries, rep.Mismatches)
	}
	// Every kind of read, at every age the plan has, was asked.
	ops := map[runner.Op]bool{}
	ages := map[string]bool{}
	for _, q := range p.Queries {
		ops[q.Op], ages[q.Age] = true, true
	}
	for _, op := range []runner.Op{runner.OpNeighbors, runner.OpBatch, runner.OpAlive, runner.OpWindow} {
		if !ops[op] && (windows || op != runner.OpWindow) {
			t.Errorf("the plan asks no %s", op)
		}
	}
	if !ages[runner.AgeOldToken] || !ages[runner.AgeNow] || !ages[runner.AgeDead] {
		t.Errorf("the plan's ages: %v", ages)
	}
	groups := 0
	for _, g := range rep.Groups {
		groups += g.Queries
		if g.Matched != g.Queries {
			t.Errorf("%s at %s: %d of %d", g.Group, g.Age, g.Matched, g.Queries)
		}
	}
	if groups != rep.Queries {
		t.Errorf("the groups hold %d questions, the report %d", groups, rep.Queries)
	}
}

// wrong is an engine that answers like the model except for one thing: the faults G0
// must catch.
type wrong struct {
	engine.Engine
	fault string
}

func (w wrong) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	ns, err := w.Engine.Neighbors(fp, d, t, s)
	if w.fault == "neighbors drop one" && len(ns) > 0 {
		ns = ns[1:]
	}
	if w.fault == "neighbors unordered" && len(ns) > 1 {
		ns[0], ns[len(ns)-1] = ns[len(ns)-1], ns[0]
	}
	return ns, err
}

func (w wrong) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	all, err := w.Engine.NeighborsBatch(fps, d, t, s)
	if w.fault == "batch drops one" {
		for i := range all {
			if len(all[i]) > 0 {
				all[i] = all[i][1:]
				break
			}
		}
	}
	return all, err
}

func (w wrong) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	ok, err := w.Engine.Alive(fp, t, s)
	if w.fault == "alive inverted" {
		ok = !ok
	}
	return ok, err
}

func (w wrong) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	rs, err := w.Engine.Window(fp, d, from, to, s)
	if w.fault == "window drops the last record" && len(rs) > 0 {
		rs = rs[:len(rs)-1]
	}
	return rs, err
}

func TestAnAnswerThatDiffersFromTheReferenceIsAMismatch(t *testing.T) {
	p := plan(t, hourSpec())
	for _, fault := range []string{"neighbors drop one", "neighbors unordered", "batch drops one", "alive inverted", "window drops the last record"} {
		rep, err := g0.Run(context.Background(), p, wrong{duckmodel.New(duckmodel.ArmRecords), fault}, nil)
		if err != nil {
			t.Fatalf("%s: %v", fault, err)
		}
		if rep.OK() || rep.Matched >= rep.Queries || len(rep.Mismatches) == 0 {
			t.Errorf("%s: %d of %d answers matched", fault, rep.Matched, rep.Queries)
		}
	}
}

func TestAStreamThatIsNotThePlansIsRefused(t *testing.T) {
	p := plan(t, retainingSpec())
	p.Stream.Digest = "0000"
	if _, err := g0.Run(context.Background(), p, duckmodel.New(duckmodel.ArmRecords), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another digest: %v", err)
	}
	p = plan(t, retainingSpec())
	p.Stream.Records++
	if _, err := g0.Run(context.Background(), p, duckmodel.New(duckmodel.ArmRecords), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another count: %v", err)
	}
}

// TestPlansOnDisk runs G0 on plan files the environment names, which is how the plans of
// the earlier measurements are used: TOPOSHIFT_DUCK_PLANS is a list of plan.json paths
// separated by the OS path list separator. The test is skipped without it. The model
// holds the whole stream in memory.
func TestPlansOnDisk(t *testing.T) {
	paths := os.Getenv("TOPOSHIFT_DUCK_PLANS")
	if paths == "" {
		t.Skip("TOPOSHIFT_DUCK_PLANS is not set")
	}
	for _, path := range filepath.SplitList(paths) {
		p, err := runner.LoadPlan(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, arm := range arms {
			t.Run(path+" "+arm.String(), func(t *testing.T) {
				rep, err := g0.Run(context.Background(), p, duckmodel.New(arm), func(f string, a ...any) { t.Logf(f, a...) })
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%d records, %d of %d answers equal the plan's", rep.Stream.Records, rep.Matched, rep.Queries)
				if !rep.OK() {
					t.Fatalf("first mismatches: %+v", rep.Mismatches)
				}
			})
		}
	}
}
