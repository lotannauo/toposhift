package g0_test

import (
	"context"
	"errors"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
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

// hourSpec is an hour of the small cluster, the spec the runner's own tests use: long
// enough for every kind of read, windows included, and an old snapshot.
func hourSpec() runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	return s
}

// retainingSpec is hourSpec with a retention at 40 minutes, so that the later reads, and
// the old snapshot, are made on what a retention left. Its reads reach back no further than
// the horizon, so it has no window.
func retainingSpec() runner.Spec {
	s := hourSpec()
	s.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	return s
}

var (
	plansMu sync.Mutex
	plans   = map[string]*runner.Plan{}
)

// plan makes the plan of a spec once per test binary, under a name; a caller that changes
// the plan changes its own copy (the plan's slices are shared and must be read only).
func plan(t *testing.T, name string, spec runner.Spec) *runner.Plan {
	t.Helper()
	plansMu.Lock()
	defer plansMu.Unlock()
	if p, ok := plans[name]; ok {
		cp := *p
		return &cp
	}
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	plans[name] = p
	cp := *p
	return &cp
}

// models are the two arms that must answer exactly, built as a cmd builds them.
func models() map[string]*doltmodel.Model {
	capacity := doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)
	return map[string]*doltmodel.Model{
		"commit": doltmodel.New(capacity),
		"rows":   doltmodel.New(doltmodel.WithArm(doltmodel.ArmRows), capacity),
	}
}

func TestBothArmsAnswerEveryQuestionOfAPlanAsTheReferenceEngineDid(t *testing.T) {
	for name, spec := range map[string]runner.Spec{"no retention": hourSpec(), "a retention": retainingSpec()} {
		p := plan(t, name, spec)
		for arm, m := range models() {
			t.Run(name+"/"+arm, func(t *testing.T) { checkPlan(t, p, m, name == "no retention") })
		}
	}
}

func checkPlan(t *testing.T, p *runner.Plan, m *doltmodel.Model, windows bool) {
	t.Helper()
	rep, err := g0.Run(context.Background(), p, m, nil)
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
	checkPaths(t, rep, m.Arm())
}

// checkPaths: each question was counted once, as answered from the state as of a commit alone,
// with the log for some key, or (a window) from the log; and the arm without commits never
// settled a key of an old token from a state row, and every old-token read of that arm had
// no commit to read.
func checkPaths(t *testing.T, rep *g0.Report, arm doltmodel.Arm) {
	t.Helper()
	outcome := func(paths map[string]uint64) (n uint64) {
		for k, v := range paths {
			if strings.HasSuffix(k, "/as-of-alone") || strings.HasSuffix(k, "/needed-log") || strings.HasSuffix(k, "/no-commit") || k == "window/log" {
				n += v
			}
			if strings.HasSuffix(k, "/commit-time") {
				t.Errorf("a path of the diagnostic: %s", k)
			}
		}
		return n
	}
	if got := outcome(rep.TotalPaths()); got != uint64(rep.Queries) {
		t.Errorf("%d reads counted for %d questions: %v", got, rep.Queries, rep.TotalPaths())
	}
	for _, g := range rep.Groups {
		if got := outcome(g.Paths); got != uint64(g.Queries) {
			t.Errorf("%s at %s: %d reads for %d questions: %v", g.Group, g.Age, got, g.Queries, g.Paths)
		}
		if g.Age == runner.AgeOldToken && arm == doltmodel.ArmRows {
			var none uint64
			for k, v := range g.Paths {
				if strings.HasSuffix(k, "/no-commit") {
					none += v
				}
			}
			if none != uint64(g.Queries) {
				t.Errorf("%s at %s: %d of %d reads had no commit in the arm without commits", g.Group, g.Age, none, g.Queries)
			}
			for k, v := range g.Paths {
				if strings.HasSuffix(k, "/keys-state") || strings.HasSuffix(k, "/as-of-alone") {
					t.Errorf("%s at %s: the arm with no commits settled keys from a state row (%s = %d)", g.Group, g.Age, k, v)
				}
			}
		}
	}
}

// wrong is an engine that answers like the model except for one thing: the faults G0
// must catch.
type wrong struct {
	*doltmodel.Model
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
	p := plan(t, "no retention", hourSpec())
	for _, fault := range []string{"neighbors drop one", "neighbors unordered", "batch drops one", "alive inverted", "window drops the last record"} {
		rep, err := g0.Run(context.Background(), p, wrong{doltmodel.New(), fault}, nil)
		if err != nil {
			t.Fatalf("%s: %v", fault, err)
		}
		if rep.OK() || rep.Matched >= rep.Queries || len(rep.Mismatches) == 0 {
			t.Errorf("%s: %d of %d answers matched", fault, rep.Matched, rep.Queries)
		}
	}
}

func TestAStreamThatIsNotThePlansIsRefused(t *testing.T) {
	p := plan(t, "a retention", retainingSpec())
	p.Stream.Digest = "0000"
	if _, err := g0.Run(context.Background(), p, doltmodel.New(), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another digest: %v", err)
	}
	p = plan(t, "a retention", retainingSpec())
	p.Stream.Records++
	if _, err := g0.Run(context.Background(), p, doltmodel.New(), nil); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Fatalf("a plan with another count: %v", err)
	}
}

// TestTheDiagnosticArmIsWrongOnAPlanWithLateRecordsAndTheDesignIsNot: a plan whose stream
// has late records, asked of the diagnostic, has answers that differ from the reference's,
// and the diagnostic is never reported as a pass. The count is a function of the plan.
func TestTheDiagnosticArmIsWrongOnAPlanWithLateRecordsAndTheDesignIsNot(t *testing.T) {
	spec := hourSpec()
	spec.Workload.LateProbability, spec.Workload.LateMeanDelay = 0.4, 5*time.Minute
	p := plan(t, "late records", spec)
	count := func() int {
		rep, err := g0.Run(context.Background(), p, doltmodel.NewCommitTime(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return rep.Queries - rep.Matched
	}
	wrong := count()
	if wrong == 0 {
		t.Fatal("the commit-time reading answered every question of a stream with late records as the reference did")
	}
	if again := count(); again != wrong {
		t.Fatalf("two runs count %d and %d wrong answers", wrong, again)
	}
	// The design itself answers every question of that plan.
	if rep, err := g0.Run(context.Background(), p, doltmodel.New(), nil); err != nil || !rep.OK() {
		t.Fatalf("the design on a plan with late records: %v, %d of %d", err, rep.Matched, rep.Queries)
	}
}

// TestPlansOnDisk runs G0 on plan files the environment names, which is how the plans of
// the earlier measurements are used: TOPOSHIFT_DOLT_PLANS is a list of plan.json paths
// separated by the OS path list separator. The test is skipped without it. The model holds
// the whole stream in memory.
func TestPlansOnDisk(t *testing.T) {
	paths := os.Getenv("TOPOSHIFT_DOLT_PLANS")
	if paths == "" {
		t.Skip("TOPOSHIFT_DOLT_PLANS is not set")
	}
	for _, path := range filepath.SplitList(paths) {
		t.Run(path, func(t *testing.T) {
			p, err := runner.LoadPlan(path)
			if err != nil {
				t.Fatal(err)
			}
			for arm, m := range models() {
				rep, err := g0.Run(context.Background(), p, m, func(f string, a ...any) { t.Logf(arm+": "+f, a...) })
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%s: %d records, %d of %d answers equal the plan's", arm, rep.Stream.Records, rep.Matched, rep.Queries)
				if !rep.OK() {
					t.Fatalf("%s: first mismatches: %+v", arm, rep.Mismatches)
				}
			}
		})
	}
}

// sinkOf writes a stream to an engine, as a loader does.
type sinkOf struct{ e engine.Engine }

func (s sinkOf) Write(b []engine.Record) error { return s.e.Write(b) }
func (s sinkOf) Retain(h time.Time) error      { return s.e.Retain(h) }

// TestCheckAsksAnEngineThatWasLoadedElsewhere: a gate that does not write is the gate that
// does: an engine written the stream by another route, and then asked, gives the report that
// Run gave, question for question.
func TestCheckAsksAnEngineThatWasLoadedElsewhere(t *testing.T) {
	p := plan(t, "a retention", retainingSpec())
	byRun, err := g0.Run(context.Background(), p, doltmodel.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := doltmodel.New()
	info, err := runner.Drive(context.Background(), p.Spec, sinkOf{m})
	if err != nil {
		t.Fatal(err)
	}
	if err := g0.CheckStream(info, p.Stream, m); err != nil {
		t.Fatal(err)
	}
	byCheck, err := g0.Check(context.Background(), p, m, nil, info)
	if err != nil {
		t.Fatal(err)
	}
	if !byCheck.OK() || byCheck.Matched != byRun.Matched || byCheck.Stream.Digest != byRun.Stream.Digest || len(byCheck.Groups) != len(byRun.Groups) {
		t.Fatalf("by Check %d of %d, by Run %d of %d", byCheck.Matched, byCheck.Queries, byRun.Matched, byRun.Queries)
	}
	for i, g := range byCheck.Groups {
		if want := byRun.Groups[i]; g.Group != want.Group || g.Age != want.Age || !maps.Equal(g.Paths, want.Paths) {
			t.Errorf("%s at %s: %v, by Run %v", g.Group, g.Age, g.Paths, want.Paths)
		}
	}
	// Check writes nothing: an engine that cannot be written to is asked all the same.
	if m.LastSeq() != p.Stream.LastSeq {
		t.Fatalf("seq %d", m.LastSeq())
	}
}

// TestEveryQuestionHasItsOwnResult: one result for each question, in the plan's order, with
// the paths and the work of that question alone, which sum to the groups' and the engine's.
func TestEveryQuestionHasItsOwnResult(t *testing.T) {
	p := plan(t, "no retention", hourSpec())
	m := doltmodel.New()
	rep, err := g0.Run(context.Background(), p, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Results) != len(p.Queries) {
		t.Fatalf("%d results for %d questions", len(rep.Results), len(p.Queries))
	}
	sumPaths, sumWork := map[string]uint64{}, map[string]uint64{}
	for i, r := range rep.Results {
		q := p.Queries[i]
		if r.Group != q.Group || r.Rank != q.Rank || r.Age != q.Age || r.Op != q.Op || !r.Match {
			t.Fatalf("result %d: %+v for question %s #%d at %s (%s)", i, r, q.Group, q.Rank, q.Age, q.Op)
		}
		var outcomes uint64
		for k, v := range r.Paths {
			sumPaths[k] += v
			if strings.HasSuffix(k, "/as-of-alone") || strings.HasSuffix(k, "/needed-log") || strings.HasSuffix(k, "/no-commit") || k == "window/log" {
				outcomes += v
			}
		}
		if outcomes != 1 {
			t.Fatalf("question %d was counted %d times: %v", i, outcomes, r.Paths)
		}
		for k, v := range r.Work {
			sumWork[k] += v
		}
	}
	if !maps.Equal(sumPaths, rep.TotalPaths()) || !maps.Equal(sumPaths, m.Paths()) {
		t.Errorf("the questions' paths %v, the groups' %v, the model's %v", sumPaths, rep.TotalPaths(), m.Paths())
	}
	if !maps.Equal(sumWork, m.Work()) || len(sumWork) == 0 {
		t.Errorf("the questions' work %v, the model's %v", sumWork, m.Work())
	}
	// A wrong answer is the result of its question and no other.
	bad, err := g0.Run(context.Background(), p, wrong{doltmodel.New(), "alive inverted"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var falses int
	for i, r := range bad.Results {
		if !r.Match {
			falses++
			if p.Queries[i].Op != runner.OpAlive {
				t.Errorf("a %s was wrong when only existence reads are", p.Queries[i].Op)
			}
		}
	}
	if falses != bad.Queries-bad.Matched || falses == 0 {
		t.Errorf("%d wrong results, %d wrong answers", falses, bad.Queries-bad.Matched)
	}
}

// TestCheckStreamSaysWhatDiffers: each field that decides the stream, and an engine that has
// not been written all of it.
func TestCheckStreamSaysWhatDiffers(t *testing.T) {
	p := plan(t, "a retention", retainingSpec())
	m := doltmodel.New()
	info, err := runner.Drive(context.Background(), p.Spec, sinkOf{m})
	if err != nil {
		t.Fatal(err)
	}
	if err := g0.CheckStream(info, p.Stream, m); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*runner.StreamInfo){
		"the digest":     func(s *runner.StreamInfo) { s.Digest = "0" },
		"the records":    func(s *runner.StreamInfo) { s.Records++ },
		"the last seq":   func(s *runner.StreamInfo) { s.LastSeq++ },
		"the old token":  func(s *runner.StreamInfo) { s.OldToken++ },
		"the dropped":    func(s *runner.StreamInfo) { s.Dropped++ },
		"the retentions": func(s *runner.StreamInfo) { s.Retentions = nil },
	} {
		want := p.Stream
		mutate(&want)
		if err := g0.CheckStream(info, want, m); !errors.Is(err, g0.ErrStreamDiffers) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := g0.CheckStream(info, p.Stream, doltmodel.New()); !errors.Is(err, g0.ErrStreamDiffers) {
		t.Errorf("an engine that was not written the stream: %v", err)
	}
}
