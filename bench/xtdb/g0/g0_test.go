package g0_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/xtdb/g0"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// hourSpec is an hour of the small cluster, the spec the runner's own tests use: long
// enough for every kind of read, windows included, and an old snapshot. With a
// retention in it the later reads are made on what a retention left.
func hourSpec() runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	return s
}

// retainingSpec is hourSpec with a retention at 40 minutes, so that the later reads,
// and the old snapshot, are made on what a retention left. Its reads reach back no
// further than the horizon, so it has no window.
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
		t.Run(name, func(t *testing.T) { checkPlan(t, plan(t, spec), name == "no retention") })
	}
}

func checkPlan(t *testing.T, p *runner.Plan, windows bool) {
	t.Helper()
	m := xtmodel.New()
	rep, err := g0.Run(context.Background(), p, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	// No question of a plan asks at a token inside a batch: the plans' tokens are batch
	// boundaries, so every read is answered by a system time, none by a client fold.
	if rc := m.ReadCounts(); rc.ClientFold != 0 || rc.Native < uint64(len(p.Queries)) {
		t.Errorf("reads: %+v for %d questions", rc, len(p.Queries))
	}
	if c := m.Counts(); c.OffMicrosecond != 0 {
		t.Errorf("%d records have a boundary off a whole microsecond", c.OffMicrosecond)
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
	*xtmodel.Model
	fault string
}

func (w wrong) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	ns, err := w.Model.Neighbors(fp, d, t, s)
	if w.fault == "neighbors drop one" && len(ns) > 0 {
		ns = ns[1:]
	}
	if w.fault == "neighbors unordered" && len(ns) > 1 {
		ns[0], ns[len(ns)-1] = ns[len(ns)-1], ns[0]
	}
	return ns, err
}

func (w wrong) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	all, err := w.Model.NeighborsBatch(fps, d, t, s)
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
	ok, err := w.Model.Alive(fp, t, s)
	if w.fault == "alive inverted" {
		ok = !ok
	}
	return ok, err
}

func (w wrong) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	rs, err := w.Model.Window(fp, d, from, to, s)
	if w.fault == "window drops the last record" && len(rs) > 0 {
		rs = rs[:len(rs)-1]
	}
	return rs, err
}

func TestAnAnswerThatDiffersFromTheReferenceIsAMismatch(t *testing.T) {
	p := plan(t, hourSpec())
	for _, fault := range []string{"neighbors drop one", "neighbors unordered", "batch drops one", "alive inverted", "window drops the last record"} {
		rep, err := g0.Run(context.Background(), p, wrong{xtmodel.New(), fault}, nil)
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
	if _, err := g0.Run(context.Background(), p, xtmodel.New(), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another digest: %v", err)
	}
	p = plan(t, retainingSpec())
	p.Stream.Records++
	if _, err := g0.Run(context.Background(), p, xtmodel.New(), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another count: %v", err)
	}
}

// TestAskHoldsALoadedEngineToThePlanWithoutRegeneratingTheStream: an engine that was
// loaded earlier is asked the plan's questions by Ask, which equals what Run found, and
// the digest of the answers is the same on every run and differs for a wrong answer.
func TestAskHoldsALoadedEngineToThePlanWithoutRegeneratingTheStream(t *testing.T) {
	p := plan(t, hourSpec())
	m := xtmodel.New()
	full, err := g0.Run(context.Background(), p, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if full.AnswersDigest == "" {
		t.Fatal("no answers digest")
	}
	again, err := g0.Ask(context.Background(), p.Queries, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !again.OK() || again.Matched != full.Matched || !reflect.DeepEqual(again.Groups, full.Groups) || again.AnswersDigest != full.AnswersDigest {
		t.Fatalf("Ask: %d of %d, digest %s; Run: %d of %d, digest %s", again.Matched, again.Queries, again.AnswersDigest, full.Matched, full.Queries, full.AnswersDigest)
	}
	bad, err := g0.Ask(context.Background(), p.Queries, wrong{m, "alive inverted"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bad.OK() || bad.AnswersDigest == full.AnswersDigest {
		t.Errorf("a wrong engine: %d of %d, digest %s", bad.Matched, bad.Queries, bad.AnswersDigest)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g0.Ask(ctx, p.Queries, m, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled context: %v", err)
	}
}

func TestCheckStreamRefusesAStreamOrAnEngineThatIsNotThePlans(t *testing.T) {
	p := plan(t, retainingSpec())
	if err := g0.CheckStream(p, p.Stream, p.Stream.LastSeq); err != nil {
		t.Fatal(err)
	}
	if err := g0.CheckStream(p, p.Stream, p.Stream.LastSeq-1); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Errorf("an engine behind the stream: %v", err)
	}
	other := p.Stream
	other.OldToken++
	if err := g0.CheckStream(p, other, p.Stream.LastSeq); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Errorf("another old token: %v", err)
	}
}

// TestPlansOnDisk runs G0 on plan files the environment names, which is how the plans
// of the earlier measurements are used: TOPOSHIFT_XT_PLANS is a list of plan.json paths
// separated by the OS path list separator. The test is skipped without it. The model
// holds the whole stream in memory.
func TestPlansOnDisk(t *testing.T) {
	paths := os.Getenv("TOPOSHIFT_XT_PLANS")
	if paths == "" {
		t.Skip("TOPOSHIFT_XT_PLANS is not set")
	}
	for _, path := range splitList(paths) {
		t.Run(path, func(t *testing.T) {
			p, err := runner.LoadPlan(path)
			if err != nil {
				t.Fatal(err)
			}
			rep, err := g0.Run(context.Background(), p, xtmodel.New(), func(f string, a ...any) { t.Logf(f, a...) })
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

func splitList(s string) []string {
	var out []string
	start := 0
	for i := range len(s) {
		if s[i] == os.PathListSeparator {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
