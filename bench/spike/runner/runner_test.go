package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// clean is the build a result may come from, as far as the guards can tell.
var clean = runner.Guards{Info: runner.BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r"}}

// hourSpec is an hour of the small cluster with no retention: long enough for
// every kind of read, windows included.
func hourSpec() runner.Spec {
	s := runner.DefaultSpec(workload.Small())
	s.MinNonEmpty = 0
	s.BatchSize = 200
	return s
}

func mustPlan(t *testing.T, spec runner.Spec) *runner.Plan {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func lookup(t *testing.T, name string) candidates.Variant {
	t.Helper()
	v, err := candidates.Lookup(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// run builds and reads a candidate under a fresh directory and returns the
// directory, the manifest and the results.
func run(t *testing.T, plan *runner.Plan, v candidates.Variant) (string, *runner.Manifest, *runner.Results) {
	t.Helper()
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	m, err := runner.Build(context.Background(), plan, v, dir, clean, nil)
	if err != nil {
		t.Fatalf("building %s: %v", v.Name, err)
	}
	res, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
	if err != nil {
		t.Fatalf("reading %s: %v", v.Name, err)
	}
	return dir, m, res
}

func TestPlanIsTheSameEveryTime(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := tinySpec()
	a, b := mustPlan(t, spec), mustPlan(t, spec)
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if !bytes.Equal(ja, jb) {
		t.Fatal("the same spec made two different plans")
	}
	da, _ := a.Digest()
	if spec.Workload.Seed++; true {
		c := mustPlan(t, spec)
		if dc, _ := c.Digest(); dc == da {
			t.Error("another seed made the same plan")
		}
	}
	if len(a.Queries) == 0 || a.Shadow == 0 {
		t.Fatalf("empty plan: %d queries over %d entities", len(a.Queries), a.Shadow)
	}
	for _, q := range a.Queries {
		if q.Expect == "" {
			t.Fatalf("%s has no expected answer", q.Name())
		}
		if q.Age != runner.AgeNow && q.Age != runner.AgeOldToken && q.At.Before(a.Stream.Horizon) && q.From.Before(a.Stream.Horizon) {
			t.Errorf("%s asks for a time before the horizon %s", q.Name(), a.Stream.Horizon)
		}
	}
}

func TestPlanRefusesQueriesThatAnswerNothing(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := tinySpec()
	spec.MinNonEmpty = 1 // every query must have an answer; the existence of a dead entity is none
	_, err := runner.MakePlan(context.Background(), spec, nil)
	if err == nil || !strings.Contains(err.Error(), "empty answers") {
		t.Errorf("a plan whose groups answer nothing: %v", err)
	}
}

func TestPlanFileKeepsItsWord(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := plan.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := runner.LoadPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := plan.Digest()
	if got, _ := loaded.Digest(); got != want {
		t.Errorf("a saved plan has digest %s, was %s", got, want)
	}

	// The file is edited as text: the snapshot token of a read of the newest
	// state does not survive a trip through a float.
	raw, _ := os.ReadFile(path)
	for name, edit := range map[string]func([]byte) []byte{
		"spec": func(b []byte) []byte { return bytes.Replace(b, []byte(`"BatchSize": 64`), []byte(`"BatchSize": 7`), 1) },
		"queries": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"Age": "now"`), []byte(`"Age": "yesterday"`), 1)
		},
	} {
		b := edit(slices.Clone(raw))
		if bytes.Equal(b, raw) {
			t.Fatalf("the edit of the %s changed nothing", name)
		}
		bad := filepath.Join(t.TempDir(), "plan.json")
		if err := os.WriteFile(bad, b, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := runner.LoadPlan(bad)
		if err == nil || !strings.Contains(err.Error(), "hash to") {
			t.Errorf("a plan with its %s edited: %v", name, err)
		}
	}
}

// Every candidate in the registry, built from the same plan and read, agrees
// with the reference engine on every query, costs the same on a second pass, and
// can be compared with the others.
func TestEveryCandidateAnswersAsTheReferenceDoes(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	var cs []*runner.Candidate
	for _, v := range candidates.All() {
		dir, m, res := run(t, plan, v)
		if len(res.Mismatches) != 0 || len(res.Unstable) != 0 {
			t.Errorf("%s: %d answers differ from the reference (first %v), %d unstable (first %v)",
				v.Name, len(res.Mismatches), first(res.Mismatches), len(res.Unstable), first(res.Unstable))
		}
		if len(res.Queries) != len(plan.Queries) {
			t.Fatalf("%s: %d results for %d queries", v.Name, len(res.Queries), len(plan.Queries))
		}
		if m.Stream.Digest != plan.Stream.Digest || m.Stream.LastSeq != plan.Stream.LastSeq {
			t.Errorf("%s was built from a different stream", v.Name)
		}
		if m.StatsCompacted["live_table_bytes"] == 0 || len(m.Breakdown) == 0 || len(m.SizeByLayer) == 0 || m.Describe["pebble_options"] == "" {
			t.Errorf("%s: the manifest is missing what the database holds: %+v", v.Name, m)
		}
		if res.Describe["recovered_bytes"] != "0" {
			t.Errorf("%s did work when opened: %s bytes recovered", v.Name, res.Describe["recovered_bytes"])
		}
		for i, q := range res.Queries {
			if want := plan.Queries[i]; q.Digest != want.Expect || q.Size != want.Size {
				t.Errorf("%s: %s answered %s (%d), the reference %s (%d)", v.Name, want.Name(), q.Digest, q.Size, want.Expect, want.Size)
				break
			}
			if q.Counters["read."+string(plan.Queries[i].Op)+".reads"] == 0 {
				t.Errorf("%s: %s recorded no read", v.Name, plan.Queries[i].Name())
				break
			}
			if _, ok := q.Warm["read."+string(plan.Queries[i].Op)+".block_bytes_cached"]; !ok {
				t.Errorf("%s: %s has no warm counters from the second pass: %v", v.Name, plan.Queries[i].Name(), q.Warm)
				break
			}
		}
		loaded, err := runner.LoadCandidate(dir)
		if err != nil {
			t.Fatal(err)
		}
		cs = append(cs, loaded)
	}
	if problems := runner.Check(plan, cs, false); len(problems) != 0 {
		t.Errorf("the candidates cannot be compared: %v", problems)
	}

	var short, full bytes.Buffer
	runner.Write(&short, plan, cs, false)
	runner.Write(&full, plan, cs, true)
	for _, v := range candidates.All() {
		if !strings.Contains(short.String(), v.Name) {
			t.Errorf("the report does not mention %s", v.Name)
		}
	}
	if !strings.Contains(short.String(), "every answer of every candidate is the reference engine's") || strings.Contains(short.String(), "NOT TO BE COMPARED") {
		t.Errorf("the report of good results says otherwise:\n%.600s", short.String())
	}
	hidden := false // an age the short report leaves out, such as an hour's window
	for _, q := range plan.Queries {
		hidden = hidden || q.Age == runner.AgeWindow1h
	}
	if hidden && full.Len() <= short.Len() || !hidden && full.Len() != short.Len() {
		t.Errorf("the full report is %d bytes and the short one %d, with ages hidden from the short one: %v", full.Len(), short.Len(), hidden)
	}
}

func first(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return xs[0]
}

// The hour spec has a week's worth of ages in it: windows, a day back is absent
// (the stream is an hour) and an hour back is present.
func TestEveryKindOfReadIsAskedAndAgreed(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, hourSpec())
	ops := map[runner.Op]int{}
	ages := map[string]int{}
	for _, q := range plan.Queries {
		ops[q.Op]++
		ages[q.Age]++
	}
	for _, op := range []runner.Op{runner.OpNeighbors, runner.OpBatch, runner.OpAlive, runner.OpWindow} {
		if ops[op] == 0 {
			t.Errorf("no %s query in the plan: %v", op, ops)
		}
	}
	for _, age := range []string{runner.AgeNow, runner.Age1h, runner.AgeOldToken, runner.AgeWindow1h} {
		if ages[age] == 0 {
			t.Errorf("no query at age %s in the plan: %v", age, ages)
		}
	}
	if ages[runner.Age1d] != 0 || ages[runner.AgeWindow1d] != 0 {
		t.Errorf("queries a day back in a stream of an hour: %v", ages)
	}
	for _, v := range candidates.All() {
		_, _, res := run(t, plan, v)
		if len(res.Mismatches) != 0 || len(res.Unstable) != 0 {
			t.Errorf("%s: mismatches %v, unstable %v", v.Name, first(res.Mismatches), first(res.Unstable))
		}
	}
}

// fullEngine is what a wrapper has to stand for to be built and read.
type fullEngine interface {
	engine.Engine
	engine.Quiescer
	engine.Statser
	engine.Describer
	engine.Breakdowner
	engine.LayerSizer
}

// faulty is an engine that gets one kind of read wrong, or one kind of write.
type faulty struct {
	fullEngine
	neighbors func([]engine.Neighbor) []engine.Neighbor
	batch     func([][]engine.Neighbor) [][]engine.Neighbor
	alive     func(bool) bool
	window    func([]engine.Record) []engine.Record
	write     func(f *faulty, batch []engine.Record) error
	retain    func() error
	rec       engine.Recorder
	onRead    func(f *faulty)
	// again makes every neighbors read twice, which is a slower engine.
	again bool
}

func (f *faulty) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	if f.again {
		if _, err := f.fullEngine.Neighbors(fp, d, t, s); err != nil {
			return nil, err
		}
	}
	ns, err := f.fullEngine.Neighbors(fp, d, t, s)
	if f.onRead != nil {
		f.onRead(f)
	}
	if f.neighbors != nil {
		ns = f.neighbors(ns)
	}
	return ns, err
}

func (f *faulty) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	out, err := f.fullEngine.NeighborsBatch(fps, d, t, s)
	if f.batch != nil {
		out = f.batch(out)
	}
	return out, err
}

func (f *faulty) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	ok, err := f.fullEngine.Alive(fp, t, s)
	if f.alive != nil {
		ok = f.alive(ok)
	}
	return ok, err
}

func (f *faulty) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	rs, err := f.fullEngine.Window(fp, d, from, to, s)
	if f.window != nil {
		rs = f.window(rs)
	}
	return rs, err
}

func (f *faulty) Write(batch []engine.Record) error {
	if f.write != nil {
		return f.write(f, batch)
	}
	return f.fullEngine.Write(batch)
}

func (f *faulty) Retain(h time.Time) error {
	if f.retain != nil {
		if err := f.retain(); err != nil {
			return err
		}
	}
	return f.fullEngine.Retain(h)
}

// breaking is a variant of M/crdb1 that misbehaves as the mutation says.
func breaking(t *testing.T, name string, mutate func(*faulty)) candidates.Variant {
	t.Helper()
	return candidates.Wrap(lookup(t, "M/crdb1"), name, func(e engine.Engine, o candidates.Options) (engine.Engine, error) {
		f := &faulty{fullEngine: e.(fullEngine), rec: o.Recorder}
		mutate(f)
		return f, nil
	})
}

// A candidate that gets an answer wrong is found by the digest of that answer,
// whatever kind of read it was and however it was wrong.
func TestAWrongAnswerIsCaught(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, hourSpec())
	for _, c := range []struct {
		name string
		op   runner.Op
		mut  func(*faulty)
	}{
		{"neighbor dropped", runner.OpNeighbors, func(f *faulty) {
			f.neighbors = func(ns []engine.Neighbor) []engine.Neighbor {
				if len(ns) > 0 {
					return ns[1:]
				}
				return ns
			}
		}},
		{"neighbors out of order", runner.OpNeighbors, func(f *faulty) {
			f.neighbors = func(ns []engine.Neighbor) []engine.Neighbor { slices.Reverse(ns); return ns }
		}},
		{"batch answers swapped", runner.OpBatch, func(f *faulty) {
			f.batch = func(out [][]engine.Neighbor) [][]engine.Neighbor {
				if len(out) > 1 {
					out[0], out[1] = out[1], out[0]
				}
				return out
			}
		}},
		{"alive wrong", runner.OpAlive, func(f *faulty) { f.alive = func(ok bool) bool { return !ok } }},
		{"window record dropped", runner.OpWindow, func(f *faulty) {
			f.window = func(rs []engine.Record) []engine.Record {
				if len(rs) > 0 {
					return rs[:len(rs)-1]
				}
				return rs
			}
		}},
		{"window out of order", runner.OpWindow, func(f *faulty) {
			f.window = func(rs []engine.Record) []engine.Record { slices.Reverse(rs); return rs }
		}},
		{"window payload changed", runner.OpWindow, func(f *faulty) {
			f.window = func(rs []engine.Record) []engine.Record {
				for i := range rs {
					rs[i].Payload = append(append([]byte(nil), rs[i].Payload...), 0)
				}
				return rs
			}
		}},
	} {
		dir := runner.CandidateDir(t.TempDir(), "M_crdb1")
		v := breaking(t, "M/crdb1", c.mut) // the name of the candidate it stands in for
		if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		res, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(res.Mismatches) == 0 {
			t.Errorf("%s: no answer was found wrong", c.name)
			continue
		}
		for _, name := range res.Mismatches {
			if !strings.Contains(name, string(c.op)) {
				t.Errorf("%s: a %s query is wrong too: %s", c.name, c.op, name)
				break
			}
		}
		cand, err := runner.LoadCandidate(dir)
		if err != nil {
			t.Fatal(err)
		}
		if problems := runner.Check(plan, []*runner.Candidate{cand}, false); len(problems) == 0 || !strings.Contains(strings.Join(problems, " "), "differ from the reference") {
			t.Errorf("%s: the report would compare it all the same: %v", c.name, problems)
		}
	}
}

func TestBuildStopsOnWhatIsNotAsPlanned(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	boom := errors.New("disk on fire")
	for _, c := range []struct {
		name string
		want string
		mut  func(*faulty)
	}{
		{"a write fails", "disk on fire", func(f *faulty) {
			n := 0
			f.write = func(f *faulty, b []engine.Record) error {
				if n++; n == 5 {
					return boom
				}
				return f.fullEngine.Write(b)
			}
		}},
		{"a write loses a record", "LastSeq", func(f *faulty) {
			f.write = func(f *faulty, b []engine.Record) error { return f.fullEngine.Write(b[:len(b)-1]) }
		}},
		{"a retention fails", "disk on fire", func(f *faulty) { f.retain = func() error { return boom } }},
		{"checkpoints fail", "checkpoints", func(f *faulty) {
			f.write = func(f *faulty, b []engine.Record) error {
				f.rec.Count("checkpoint.errors", 1) // counted, not returned
				return f.fullEngine.Write(b)
			}
		}},
	} {
		dir := filepath.Join(t.TempDir(), "c")
		_, err := runner.Build(context.Background(), plan, breaking(t, "M/crdb1", c.mut), dir, clean, nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, runner.ManifestFile)); statErr == nil {
			t.Errorf("%s: a manifest was written for a build that failed", c.name)
		}
	}

	// An engine that cannot report what a measurement needs is not built at all.
	bare := candidates.Wrap(lookup(t, "M/crdb1"), "M/bare", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) {
		return struct{ engine.Engine }{e}, nil
	})
	if _, err := runner.Build(context.Background(), plan, bare, filepath.Join(t.TempDir(), "c"), clean, nil); err == nil || !strings.Contains(err.Error(), "cannot report") {
		t.Errorf("an engine that reports nothing: %v", err)
	}
}

func TestBuildAndReadRefuseWhatTheyShould(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "L/off")
	root := t.TempDir()

	// A binary a result may not come from.
	for name, info := range map[string]runner.BuildInfo{
		"race": {Race: true, Revision: "r"}, "invariants": {Invariants: true, Revision: "r"}, "cgo": {CGO: true, Revision: "r"}, "dirty": {Modified: true, Revision: "r"}, "no stamp": {},
	} {
		if _, err := runner.Build(context.Background(), plan, v, filepath.Join(root, name), runner.Guards{Info: info}, nil); err == nil {
			t.Errorf("a %s build was accepted", name)
		}
		// Unless the run says it is only a validation (the other reasons are
		// leaves of the same check, covered by TestGuardsRefuseWhatAResultMayNotComeFrom).
		if name != "race" && name != "no stamp" {
			continue
		}
		if _, err := runner.Build(context.Background(), plan, v, filepath.Join(root, name+"-untimed"), runner.Guards{Info: info, Untimed: true}, nil); err != nil {
			t.Errorf("an untimed %s build was refused: %v", name, err)
		}
	}

	// A directory that is not new.
	used := filepath.Join(root, "used")
	if err := os.MkdirAll(used, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(used, "x"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Build(context.Background(), plan, v, used, clean, nil); err == nil {
		t.Error("a build into a directory with something in it")
	}

	// A directory in a git worktree.
	repo := filepath.Join(root, "repo")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Build(context.Background(), plan, v, filepath.Join(repo, "bench", "results"), clean, nil); !errors.Is(err, runner.ErrInsideWorktree) {
		t.Errorf("a build inside a worktree: %v", err)
	}
	link := filepath.Join(root, "link")
	if err := os.MkdirAll(filepath.Join(repo, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(repo, "sub"), link); err == nil {
		if err := runner.CheckOutside(filepath.Join(link, "out")); !errors.Is(err, runner.ErrInsideWorktree) {
			t.Errorf("a symlink into a worktree: %v", err)
		}
	}
	if err := runner.CheckOutside(filepath.Join(root, "not", "yet", "there")); err != nil {
		t.Errorf("a directory outside any worktree: %v", err)
	}

	// Reading something that was not built, or built for another plan, or as
	// another candidate.
	dir, _, _ := run(t, plan, v)
	if _, err := runner.Read(context.Background(), plan, v, filepath.Join(root, "missing"), clean, nil); err == nil || !strings.Contains(err.Error(), "no manifest") {
		t.Errorf("reading nothing: %v", err)
	}
	if _, err := runner.Read(context.Background(), plan, lookup(t, "M/crdb1"), dir, clean, nil); err == nil || !strings.Contains(err.Error(), "holds L/off") {
		t.Errorf("reading it as another candidate: %v", err)
	}
	other := *plan
	other.Queries = slices.Clone(plan.Queries)
	other.Queries[0].Expect = "00"
	if _, err := runner.Read(context.Background(), &other, v, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "another plan") {
		t.Errorf("reading against edited answers: %v", err)
	}
	if _, err := runner.Read(context.Background(), plan, v, dir, runner.Guards{Info: runner.BuildInfo{Race: true}}, nil); err == nil {
		t.Error("a read by a race build")
	}
}

// A database that was not closed clean does work when it is opened: its log is
// written to a table. A read refuses it, because the shape of its tables is no
// longer the one the build compacted.
func TestReadRefusesADatabaseThatWorksWhenOpened(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "L/off")
	dir, _, _ := run(t, plan, v)

	// Write one more record and close without flushing it.
	e, err := v.Open(filepath.Join(dir, runner.DBDir), candidates.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rack, err := identity.NewResolver(catalog.Default()).Resolve(catalog.Rack, []identity.Attr{{Key: catalog.RackID, Value: "late"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := engine.Record{
		Layer: catalog.L0, Subject: engine.EntitySubject(rack.Fingerprint()), Producer: "x",
		EventTime: plan.Stream.End, Seq: e.LastSeq() + 1, Kind: 1, Payload: bytes.Repeat([]byte{1}, 50),
	}
	if err := e.Write([]engine.Record{rec}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Read(context.Background(), plan, v, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "from the log") {
		t.Errorf("a database with a log to replay: %v", err)
	}
}

// A database that changes shape while it is being read is refused.
func TestReadRefusesADatabaseThatFlushesWhileBeingRead(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	dir, _, _ := run(t, plan, lookup(t, "M/crdb1"))
	rack, err := identity.NewResolver(catalog.Default()).Resolve(catalog.Rack, []identity.Attr{{Key: catalog.RackID, Value: "late"}})
	if err != nil {
		t.Fatal(err)
	}
	flushing := breaking(t, "M/crdb1", func(f *faulty) {
		done := false
		f.onRead = func(f *faulty) {
			if done {
				return
			}
			done = true
			rec := engine.Record{
				Layer: catalog.L0, Subject: engine.EntitySubject(rack.Fingerprint()), Producer: "x",
				EventTime: plan.Stream.End, Seq: f.LastSeq() + 1, Kind: 1, Payload: []byte("x"),
			}
			if err := f.fullEngine.Write([]engine.Record{rec}); err != nil {
				t.Error(err)
			}
			if err := f.Quiesce(context.Background()); err != nil { // flushes
				t.Error(err)
			}
		}
	})
	if _, err := runner.Read(context.Background(), plan, flushing, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "did work while being read") {
		t.Errorf("a database that flushed during the read: %v", err)
	}
}

func TestCheckFindsWhatMakesResultsIncomparable(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	load := func(name string) *runner.Candidate {
		dir, _, _ := run(t, plan, lookup(t, name))
		c, err := runner.LoadCandidate(dir)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := load("L/off"), load("M/crdb1")
	if problems := runner.Check(plan, []*runner.Candidate{a, b}, false); len(problems) != 0 {
		t.Fatalf("good results: %v", problems)
	}
	mutate := func(name string, want string, f func(a, b *runner.Candidate)) {
		t.Helper()
		ca, cb := *a, *b
		ma, mb, ra, rb := *a.Manifest, *b.Manifest, *a.Results, *b.Results
		ca.Manifest, cb.Manifest, ca.Results, cb.Results = &ma, &mb, &ra, &rb
		ma.Describe = cloneMap(a.Manifest.Describe)
		mb.Describe = cloneMap(b.Manifest.Describe)
		f(&ca, &cb)
		problems := strings.Join(runner.Check(plan, []*runner.Candidate{&ca, &cb}, false), "\n")
		if !strings.Contains(problems, want) {
			t.Errorf("%s: wanted %q among the problems, got:\n%s", name, want, problems)
		}
	}
	mutate("another plan", "another plan", func(_, b *runner.Candidate) { b.Results.PlanDigest = "x" })
	mutate("built from another plan", "another plan", func(_, b *runner.Candidate) { b.Manifest.PlanDigest = "x" })
	mutate("untimed", "may not come from", func(a, _ *runner.Candidate) { a.Manifest.Untimed = true })
	mutate("mismatch", "differ from the reference", func(_, b *runner.Candidate) { b.Results.Mismatches = []string{"q"} })
	mutate("unstable", "second pass", func(a, _ *runner.Candidate) { a.Results.Unstable = []string{"q"} })
	mutate("another stream", "another stream", func(a, _ *runner.Candidate) { a.Manifest.Stream.Digest = "x" })
	mutate("another toolchain", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.GoVersion = "other" })
	mutate("another revision", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.Revision = "other" })
	mutate("read by another binary", "different binaries", func(_, b *runner.Candidate) { b.Results.Build.Revision = "other" })
	mutate("built and read by different binaries", "built and read by different binaries", func(a, _ *runner.Candidate) { a.Results.Build.GOARCH = "other" })
	mutate("another platform", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.GOOS = "other" })
	mutate("another architecture", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.GOARCH = "other" })
	mutate("cgo", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.CGO = true })
	mutate("race", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.Race = true })
	mutate("invariants", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.Invariants = true })
	mutate("dirty tree", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.Modified = true })
	mutate("another executable", "different binaries", func(_, b *runner.Candidate) { b.Manifest.Build.Executable = "other" })
	// A candidate built and read by one binary, another candidate by a second one:
	// each is consistent with itself, and they still cannot be compared.
	mutate("two binaries, each consistent", "were built by different binaries", func(_, b *runner.Candidate) {
		b.Manifest.Build.Revision, b.Results.Build.Revision = "other", "other"
	})
	mutate("other options", "different Pebble options", func(_, b *runner.Candidate) {
		b.Manifest.Describe["pebble_options"] = strings.Replace(b.Manifest.Describe["pebble_options"], "bytes_per_sync=", "bytes_per_sync=1", 1)
	})
	mutate("missing results", "results for", func(_, b *runner.Candidate) { b.Results.Queries = b.Results.Queries[1:] })
}

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func TestGuardsRefuseWhatAResultMayNotComeFrom(t *testing.T) {
	t.Parallel()

	ok := runner.BuildInfo{Revision: "abc"}
	for name, c := range map[string]struct {
		info    runner.BuildInfo
		untimed bool
		want    string // empty: accepted
	}{
		"clean":              {info: ok},
		"no stamp":           {info: runner.BuildInfo{}, want: "version control stamp"},
		"unoptimized":        {info: runner.BuildInfo{Revision: "abc", Unoptimized: true}, want: "optimization or inlining off"},
		"race":               {info: runner.BuildInfo{Race: true}, want: "race detector"},
		"invariants":         {info: runner.BuildInfo{Invariants: true}, want: "invariants"},
		"cgo":                {info: runner.BuildInfo{CGO: true}, want: "CGO_ENABLED=0"},
		"dirty":              {info: runner.BuildInfo{Modified: true}, want: "uncommitted"},
		"all of them":        {info: runner.BuildInfo{Race: true, Invariants: true, CGO: true, Modified: true}, want: "race detector; built with the invariants tag; built with cgo"},
		"untimed everything": {info: runner.BuildInfo{Race: true, Invariants: true, CGO: true, Modified: true}, untimed: true},
	} {
		err := runner.Guards{Info: c.info, Untimed: c.untimed}.Check()
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v, want it to mention %q", name, err, c.want)
		}
	}
}

func TestCaptureSumsAndTakes(t *testing.T) {
	t.Parallel()

	c := runner.NewCapture()
	c.Count("a", 2)
	c.Sample("a", 3)
	c.Sample("b", 7)
	got := c.Take()
	if got["a"] != 5 || got["b"] != 7 || len(got) != 2 {
		t.Errorf("Take = %v", got)
	}
	if again := c.Take(); len(again) != 0 {
		t.Errorf("Take twice = %v", again)
	}
	c.Count("a", 1)
	if all := c.Totals(); all["a"] != 6 || all["b"] != 7 {
		t.Errorf("Totals = %v", all)
	}
}

// Each query is asked at the instant, snapshot and span its age says.
func TestQueriesAreAskedAtTheirAges(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, hourSpec())
	end, info := plan.Stream.End, plan.Stream
	seen := map[string]bool{}
	for _, q := range plan.Queries {
		switch q.Age {
		case runner.AgeNow:
			if !q.At.Equal(end) || q.AsOf != engine.Latest {
				t.Errorf("%s: asked at %s, token %d", q.Name(), q.At, q.AsOf)
			}
		case runner.Age1h:
			if !q.At.Equal(end.Add(-time.Hour)) || q.AsOf != engine.Latest {
				t.Errorf("%s: asked at %s, token %d", q.Name(), q.At, q.AsOf)
			}
		case runner.Age1d:
			if !q.At.Equal(end.Add(-24*time.Hour)) || q.AsOf != engine.Latest {
				t.Errorf("%s: asked at %s, token %d", q.Name(), q.At, q.AsOf)
			}
		case runner.AgeOldToken:
			if !q.At.Equal(info.OldAt) || q.AsOf != info.OldToken || !q.At.Before(end) {
				t.Errorf("%s: asked at %s, token %d, want %s and %d", q.Name(), q.At, q.AsOf, info.OldAt, info.OldToken)
			}
		case runner.AgeWindow1h:
			if !q.From.Equal(end.Add(-time.Hour)) || !q.To.Equal(end) || q.AsOf != engine.Latest {
				t.Errorf("%s: window %s to %s", q.Name(), q.From, q.To)
			}
		case runner.AgeWindow1d:
			if !q.From.Equal(end.Add(-24*time.Hour)) || !q.To.Equal(end) {
				t.Errorf("%s: window %s to %s", q.Name(), q.From, q.To)
			}
		default:
			t.Errorf("%s: unknown age", q.Name())
		}
		if q.Op == runner.OpBatch && (len(q.Fps) < 2 || q.Rank != 0) {
			t.Errorf("%s: a batch of %d entities, rank %d", q.Name(), len(q.Fps), q.Rank)
		}
		if q.Op != runner.OpBatch && len(q.Fps) != 1 {
			t.Errorf("%s: %d entities in a single read", q.Name(), len(q.Fps))
		}
		if (q.Op == runner.OpWindow) != (q.Age == runner.AgeWindow1h || q.Age == runner.AgeWindow1d) {
			t.Errorf("%s: a %s read at age %s", q.Name(), q.Op, q.Age)
		}
		seen[strings.SplitN(q.Group, " ", 2)[0]] = true
	}
	for _, class := range []string{"node<-", "service<-", "pod->", "host<-", "node->L1", "service<-L3", "node"} {
		if !seen[class] {
			t.Errorf("no query of class %s: %v", class, seen)
		}
	}
	// The directions follow the side of the prefix.
	for _, q := range plan.Queries {
		if strings.HasPrefix(q.Group, "node<-") && q.Dir != engine.Reverse || strings.HasPrefix(q.Group, "pod->") && q.Dir != engine.Forward {
			t.Errorf("%s: direction %s", q.Name(), q.Dir)
		}
	}
	// The cache is the share of the payload bytes asked for, in whole megabytes, at least one.
	want := max(int64(float64(info.PayloadBytes)*plan.Spec.CacheFraction)>>20<<20, 1<<20)
	if plan.CacheBytes != want {
		t.Errorf("cache %d, want %d", plan.CacheBytes, want)
	}
}

// A build whose stream is not the planned one is refused, whichever way it differs.
func TestBuildRefusesAStreamThatIsNotThePlans(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	for name, mod := range map[string]func(*runner.Plan){
		"digest":     func(p *runner.Plan) { p.Stream.Digest = "00" },
		"records":    func(p *runner.Plan) { p.Stream.Records++ },
		"dropped":    func(p *runner.Plan) { p.Stream.Dropped++ },
		"last seq":   func(p *runner.Plan) { p.Stream.LastSeq++ },
		"old token":  func(p *runner.Plan) { p.Stream.OldToken++ },
		"retentions": func(p *runner.Plan) { p.Stream.Retentions = nil },
	} {
		p := *plan
		p.Stream.Retentions = slices.Clone(plan.Stream.Retentions)
		mod(&p)
		_, err := runner.Build(context.Background(), &p, lookup(t, "L/off"), filepath.Join(t.TempDir(), "c"), clean, nil)
		if err == nil || !strings.Contains(err.Error(), "different stream") {
			t.Errorf("a plan with another %s: %v", name, err)
		}
	}
}

// A database that is not what the manifest says it was built as, or that opens
// at another snapshot than the plan's, is refused.
func TestReadRefusesADatabaseThatIsNotWhatItWasBuiltAs(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "L/k64a4")
	dir, _, _ := run(t, plan, v)

	raw, err := os.ReadFile(filepath.Join(dir, runner.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"layout", "comparer", "key_schema_in_tables", "collectors_in_tables", "time_filter_asked", "checkpoints",
		"block_bytes", "memtable_bytes", "target_file_bytes", "l_base_max_bytes", "l0_compaction_threshold", "cache_bytes", "sync",
	} {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		m["Describe"].(map[string]any)[key] = "something else"
		b, _ := json.Marshal(m)
		if err := os.WriteFile(filepath.Join(dir, runner.ManifestFile), b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := runner.Read(context.Background(), plan, v, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "not what it was built as") || !strings.Contains(err.Error(), key) {
			t.Errorf("a database whose %s is not what the manifest says: %v", key, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, runner.ManifestFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	moved := *plan
	moved.Stream.Digest = "00"
	if _, err := runner.Read(context.Background(), &moved, v, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "another plan") {
		t.Errorf("a plan of another stream: %v", err)
	}
	other := *plan
	other.Stream.LastSeq++
	if _, err := runner.Read(context.Background(), &other, v, dir, clean, nil); err == nil || !strings.Contains(err.Error(), "opens at seq") {
		t.Errorf("a plan that ends at another snapshot: %v", err)
	}
	if _, err := runner.Read(context.Background(), plan, v, dir, clean, nil); err != nil {
		t.Errorf("the manifest put back: %v", err)
	}
}

// An engine whose reads cost something different the second time is flagged, so
// a counter that is not repeatable is never mistaken for one that is.
func TestAnEngineWhoseCostsDriftIsFlaggedUnstable(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	n := int64(0)
	drifting := breaking(t, "M/crdb1", func(f *faulty) {
		f.onRead = func(f *faulty) { n++; f.rec.Count("read.neighbors.steps", n) }
	})
	dir := runner.CandidateDir(t.TempDir(), "M_crdb1")
	if _, err := runner.Build(context.Background(), plan, drifting, dir, clean, nil); err != nil {
		t.Fatal(err)
	}
	res, err := runner.Read(context.Background(), plan, drifting, dir, clean, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Unstable) == 0 {
		t.Error("counters that differ between two passes were not flagged")
	}
	for _, name := range res.Unstable {
		if !strings.Contains(name, "neighbors") {
			t.Errorf("a query that does not read neighbors is unstable: %s", name)
		}
	}
	if len(res.Mismatches) != 0 {
		t.Errorf("the answers were right, but %d are marked wrong", len(res.Mismatches))
	}
	cand, err := runner.LoadCandidate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if problems := strings.Join(runner.Check(plan, []*runner.Candidate{cand}, false), "\n"); !strings.Contains(problems, "second pass") {
		t.Errorf("the report would compare it all the same: %s", problems)
	}
}

// A validation run is marked in both files, and its results are not compared.
func TestAnUntimedRunIsMarkedAndNotCompared(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "M/crdb1")
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	g := runner.Guards{Info: runner.BuildInfo{GoVersion: "test", Invariants: true}, Untimed: true}
	m, err := runner.Build(context.Background(), plan, v, dir, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Untimed || !m.Build.Invariants {
		t.Errorf("the manifest of an untimed build: untimed %v, build %+v", m.Untimed, m.Build)
	}
	// Read by a clean binary, the results are still marked: the build is what was untimed.
	if res, err := runner.Read(context.Background(), plan, v, dir, clean, nil); err != nil || !res.Untimed {
		t.Errorf("the results of a database built untimed: %v, untimed %v", err, res != nil && res.Untimed)
	}
	// Read by the same binary, untimed too, the run is valid.
	if _, err := runner.Read(context.Background(), plan, v, dir, g, nil); err != nil {
		t.Fatal(err)
	}
	cand, err := runner.LoadCandidate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if problems := strings.Join(runner.Check(plan, []*runner.Candidate{cand}, false), "\n"); !strings.Contains(problems, "may not come from") {
		t.Errorf("an untimed run is compared: %s", problems)
	}
	if problems := runner.Check(plan, []*runner.Candidate{cand}, true); len(problems) != 0 {
		t.Errorf("a valid untimed run, asked whether it was valid: %v", problems)
	}
}

// The report shows what each candidate holds and what retention did.
func TestReportShowsWhatEachCandidateHolds(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	var cs []*runner.Candidate
	for _, name := range []string{"L/k64a4", "M/crdb1"} {
		dir, _, _ := run(t, plan, lookup(t, name))
		c, err := runner.LoadCandidate(dir)
		if err != nil {
			t.Fatal(err)
		}
		cs = append(cs, c)
	}
	var out bytes.Buffer
	runner.Write(&out, plan, cs, false)
	text := out.String()
	for _, want := range []string{
		"table bytes", "table bytes per record", "L2 table bytes", "baseline (logical)", "checkpoint (logical)", "payload reverse (logical)",
		"write amplification built (depends on when compactions ran)", "retain.records_replayed", "retain.keys_visited", "checkpoint.written", "steps (Next calls the layout made", "seeks (calls that position the iterator)", "key bytes of the points iterated", "value bytes of the points iterated", "What a read cost is all of the tables below together",
		"internal steps", "block bytes loaded", "points iterated", "node<- hot-records neighbors now", "node<- hot-records batch now",
		"records stepped over (layout L", "versions stepped over (layout M", "checkpoints used (layout L)", "checkpoint entries decoded", "points a range tombstone covered", "bytes fetched from value blocks",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the report lacks %q:\n%s", want, text)
		}
	}
	// Layout M has no checkpoints and no baseline, and the report shows zero for them.
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "checkpoint (logical)") && !strings.HasSuffix(strings.TrimSpace(line), "0 B") {
			t.Errorf("layout M is said to hold checkpoints: %q", line)
		}
	}
}

func TestReadBuildInfoSeesTheTagsItWasBuiltWith(t *testing.T) {
	t.Parallel()

	bi := runner.ReadBuildInfo()
	if bi.GoVersion == "" || bi.GOOS == "" || bi.GOARCH == "" {
		t.Errorf("no toolchain in %+v", bi)
	}
	if bi.Invariants != invariantsTag || bi.Race != raceTag {
		t.Errorf("the test binary has invariants %v and race %v, build info says %+v", invariantsTag, raceTag, bi)
	}
}

// Over more than a day the reads a day back and the windows of a day are asked,
// at the instants they say.
func TestQueriesADayBack(t *testing.T) {
	t.Parallel()

	spec := tinySpec()
	spec.Workload.Duration = 26 * time.Hour
	spec.Workload.EventsPerSecond = 0.05
	spec.Retentions = []runner.Retention{{At: 25 * time.Hour, Keep: 24 * time.Hour}}
	plan := mustPlan(t, spec)
	end := plan.Stream.End
	var day, window int
	for _, q := range plan.Queries {
		switch q.Age {
		case runner.Age1d:
			day++
			if !q.At.Equal(end.Add(-24 * time.Hour)) {
				t.Errorf("%s: asked at %s", q.Name(), q.At)
			}
		case runner.AgeWindow1d:
			window++
			if !q.From.Equal(end.Add(-24*time.Hour)) || !q.To.Equal(end) {
				t.Errorf("%s: window %s to %s", q.Name(), q.From, q.To)
			}
		}
		if q.At.Before(plan.Stream.Horizon) && q.Op != runner.OpWindow || q.Op == runner.OpWindow && q.From.Before(plan.Stream.Horizon) {
			t.Errorf("%s is before the horizon %s", q.Name(), plan.Stream.Horizon)
		}
	}
	if day == 0 || window == 0 {
		t.Errorf("%d reads a day back and %d windows of a day in %d queries", day, window, len(plan.Queries))
	}
}

// The prefixes asked about are chosen among those that exist at the end.
func TestQueriesAreChosenAmongLivePrefixes(t *testing.T) {
	t.Parallel()

	var lives []bool
	sel := selectorFunc(func(c workload.Class, hot, median int, live bool) workload.Picks {
		lives = append(lives, live)
		return workload.Picks{}
	})
	runner.BuildQueries(tinySpec(), sel, runner.StreamInfo{})
	if len(lives) == 0 {
		t.Fatal("nothing was picked")
	}
	for _, l := range lives {
		if !l {
			t.Error("prefixes were picked without regard to whether they exist at the end")
		}
	}
}

type selectorFunc func(c workload.Class, hot, median int, live bool) workload.Picks

func (f selectorFunc) Pick(c workload.Class, hot, median int, live bool) workload.Picks {
	return f(c, hot, median, live)
}

// An engine that answers right the first time and wrong the second is found: the
// second pass is checked against the first, and a counter that appears only in one
// of them is a difference too.
func TestAnEngineThatChangesItsMindIsFlagged(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	reads := 0
	for _, q := range plan.Queries {
		if q.Op == runner.OpNeighbors {
			reads++
		}
	}
	for name, mut := range map[string]func(*faulty){
		"another answer": func(f *faulty) {
			calls := 0
			f.neighbors = func(ns []engine.Neighbor) []engine.Neighbor {
				if calls++; calls > reads && len(ns) > 0 {
					return ns[1:]
				}
				return ns
			}
		},
		"another counter": func(f *faulty) {
			calls := 0
			f.onRead = func(f *faulty) {
				if calls++; calls > reads {
					f.rec.Count("read.neighbors.extra", 1)
				}
			}
		},
	} {
		v := breaking(t, "M/crdb1", mut)
		dir := runner.CandidateDir(t.TempDir(), "M_crdb1")
		if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
			t.Fatal(err)
		}
		res, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Unstable) == 0 {
			t.Errorf("%s: not flagged", name)
		}
	}
}

// A slower engine measures larger counters: what is counted is what the engine
// did, so one that does a read twice reads twice as much.
func TestACostlierEngineMeasuresLarger(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	total := func(v candidates.Variant) (steps, points, blocks int64) {
		dir := runner.CandidateDir(t.TempDir(), "M_crdb1")
		if _, err := runner.Build(context.Background(), plan, v, dir, clean, nil); err != nil {
			t.Fatal(err)
		}
		res, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Mismatches) != 0 {
			t.Fatalf("%d answers differ", len(res.Mismatches))
		}
		for i, q := range plan.Queries {
			if q.Op == runner.OpNeighbors {
				c := res.Queries[i].Counters
				steps += c["read.neighbors.steps"]
				points += c["read.neighbors.points"]
				blocks += c["read.neighbors.block_bytes"]
			}
		}
		return
	}
	s1, p1, b1 := total(lookup(t, "M/crdb1"))
	s2, p2, b2 := total(breaking(t, "M/crdb1", func(f *faulty) { f.again = true }))
	if s1 == 0 || s2 != 2*s1 || p2 != 2*p1 || b2 < b1 {
		t.Errorf("steps %d -> %d, points %d -> %d, block bytes %d -> %d: reading twice should double the first two and not lower the third", s1, s2, p1, p2, b1, b2)
	}
}

// The same candidate built twice from the same plan comes to the same bytes and
// costs the same to read: nothing in a result depends on which build it was.
func TestTwoBuildsOfACandidateAreTheSame(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	for _, name := range []string{"L/k64a4", "M/crdb1"} {
		_, m1, r1 := run(t, plan, lookup(t, name))
		_, m2, r2 := run(t, plan, lookup(t, name))
		for _, k := range []string{"live_table_bytes", "tombstones"} {
			if m1.StatsCompacted[k] != m2.StatsCompacted[k] {
				t.Errorf("%s: %s is %d, then %d", name, k, m1.StatsCompacted[k], m2.StatsCompacted[k])
			}
		}
		if !reflect.DeepEqual(m1.Breakdown, m2.Breakdown) || !reflect.DeepEqual(m1.SizeByLayer, m2.SizeByLayer) {
			t.Errorf("%s: what it holds differs between builds: %v / %v", name, m1.SizeByLayer, m2.SizeByLayer)
		}
		if !reflect.DeepEqual(m1.Counters, m2.Counters) {
			t.Errorf("%s: what building counted differs between builds", name)
		}
		stable := func(m map[string]int64) map[string]int64 {
			out := map[string]int64{}
			for k, v := range m {
				if !strings.HasSuffix(k, ".block_bytes_cached") && !strings.HasSuffix(k, ".block_read_ns") && k != "read.batch.separated_values" {
					out[k] = v
				}
			}
			return out
		}
		for i, q := range r1.Queries {
			if !reflect.DeepEqual(stable(q.Counters), stable(r2.Queries[i].Counters)) {
				t.Errorf("%s: %s costs %v, then %v", name, plan.Queries[i].Name(), stable(q.Counters), stable(r2.Queries[i].Counters))
				break
			}
		}
	}
}

// Over more than a day, the reads a day back and the windows of a day are asked
// of the candidates too, and agree with the reference.
func TestReadsADayBackAreAgreedOn(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := tinySpec()
	spec.Workload.Duration = 26 * time.Hour
	spec.Workload.EventsPerSecond = 0.05
	spec.Retentions = []runner.Retention{{At: 25 * time.Hour, Keep: 24 * time.Hour}}
	spec.MinNonEmpty = 0.25 // every age of every group, the old snapshot included, must have an answer
	plan := mustPlan(t, spec)
	ages := map[string]bool{}
	for _, g := range plan.Groups {
		ages[g.Age] = true
	}
	for _, age := range []string{runner.AgeNow, runner.Age1h, runner.Age1d, runner.AgeOldToken, runner.AgeWindow1d} {
		if !ages[age] {
			t.Fatalf("no group at age %s: %v", age, ages)
		}
	}
	for _, name := range []string{"L/k64a4", "M/crdb1"} {
		_, _, res := run(t, plan, lookup(t, name))
		if len(res.Mismatches) != 0 || len(res.Unstable) != 0 {
			t.Errorf("%s: mismatches %v, unstable %v", name, first(res.Mismatches), first(res.Unstable))
		}
	}
}

// The old snapshot is read at its own instant: as of the newest instant every
// refreshed edge has expired, and the read asks about nothing.
func TestOldSnapshotsAreReadWhereTheyWereTaken(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	for name, retentions := range map[string][]runner.Retention{
		"no retention": nil,
		"a retention":  {{At: 40 * time.Minute, Keep: 20 * time.Minute}},
	} {
		spec := hourSpec()
		spec.Retentions = retentions
		spec.MinNonEmpty = 0.25
		plan := mustPlan(t, spec)
		info := plan.Stream
		want := info.Start.Add(spec.Workload.Duration / 4 * 3) // 45 minutes; 50 after a retention at 40
		if retentions != nil {
			want = info.Start.Add(50 * time.Minute)
		}
		if !info.OldAt.Equal(want) || info.OldToken < info.TokenFloor {
			t.Errorf("%s: the old snapshot is read at %s with token %d (floor %d), want %s", name, info.OldAt, info.OldToken, info.TokenFloor, want)
		}
		n := 0
		for _, q := range plan.Queries {
			if q.Age == runner.AgeOldToken {
				n++
				if q.Size == 0 && (strings.HasPrefix(q.Group, "host") || strings.HasPrefix(q.Group, "node->")) {
					t.Errorf("%s: %s: a refreshed prefix answers nothing as of the old snapshot", name, q.Name())
				}
			}
		}
		if n == 0 {
			t.Errorf("%s: no read of an old snapshot", name)
		}
	}
}

// The results of a candidate are tied to the manifest they were read against.
func TestResultsAreTiedToTheirManifest(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	dir, _, _ := run(t, plan, lookup(t, "L/off"))
	if _, err := runner.LoadCandidate(dir); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, runner.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, runner.ManifestFile), append(b, ' '), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.LoadCandidate(dir); err == nil || !strings.Contains(err.Error(), "another manifest") {
		t.Errorf("results beside a manifest they were not read against: %v", err)
	}
}

// What the files hold is fixed: a field added, renamed or dropped is seen here,
// because the files are read by tools that were written against them.
func TestFilesKeepTheirFields(t *testing.T) {
	t.Parallel()

	fields := func(v any) string {
		typ := reflect.TypeOf(v)
		names := make([]string, typ.NumField())
		for i := range names {
			names[i] = typ.Field(i).Name
		}
		return strings.Join(names, ",")
	}
	for name, c := range map[string]struct {
		v    any
		want string
	}{
		"Plan":         {runner.Plan{}, "Spec,SpecDigest,RulesDigest,Stream,Queries,QueriesDigest,Groups,CacheBytes,Shadow"},
		"Spec":         {runner.Spec{}, "Version,Workload,BatchSize,Retentions,Hot,Median,CacheFraction,MinNonEmpty"},
		"Query":        {runner.Query{}, "Group,Rank,Op,Layer,Dir,Fps,Age,At,From,To,AsOf,Expect,Size"},
		"Stream":       {runner.StreamInfo{}, "Digest,Records,Dropped,LastSeq,PayloadBytes,Retentions,TokenFloor,OldToken,OldAt,Start,End,Horizon"},
		"Group":        {runner.GroupInfo{}, "Group,Age,Queries,NonEmpty"},
		"Manifest":     {runner.Manifest{}, "Candidate,Layout,PlanDigest,Stream,Build,Untimed,Describe,Counters,StatsBuilt,StatsCompacted,Breakdown,SizeByLayer,Size"},
		"Results":      {runner.Results{}, "Candidate,PlanDigest,ManifestDigest,Build,Untimed,Describe,Queries,Mismatches,Unstable,StatsBefore,StatsAfter"},
		"Query result": {runner.QueryResult{}, "Digest,Size,Counters,Warm"},
		"Build":        {runner.BuildInfo{}, "GoVersion,GOOS,GOARCH,CGO,Race,Invariants,Tags,Unoptimized,Revision,Modified,Executable"},
	} {
		if got := fields(c.v); got != c.want {
			t.Errorf("%s has fields\n  %s\nwant\n  %s", name, got, c.want)
		}
	}
}

func TestReadBuildInfoHashesTheExecutable(t *testing.T) {
	t.Parallel()

	if h := runner.ReadBuildInfo().Executable; len(h) != 64 {
		t.Errorf("executable hash %q", h)
	}
}

// The keys of the files, at every depth: a field added, renamed or dropped in a
// nested record is seen here, because the files are read by tools written against them.
func TestFilesKeepTheirNestedKeys(t *testing.T) {
	t.Parallel()

	var keys func(prefix string, v any, out map[string]bool)
	keys = func(prefix string, v any, out map[string]bool) {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				out[prefix+k] = true
				keys(prefix+k+".", e, out)
			}
		case []any:
			for _, e := range x {
				keys(prefix, e, out)
			}
		}
	}
	flat := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var m any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		keys("", m, out)
		var names []string
		for k := range out {
			if !strings.HasPrefix(k, "Spec.Workload.") && !strings.HasPrefix(k, "Workload.") { // the workload's own options are the workload package's
				names = append(names, k)
			}
		}
		slices.Sort(names)
		return strings.Join(names, " ")
	}
	got := flat(runner.Plan{
		Queries: []runner.Query{{Fps: nil}}, Groups: []runner.GroupInfo{{}},
		Stream: runner.StreamInfo{Retentions: []runner.AppliedRetention{{}}},
		Spec:   runner.Spec{Retentions: []runner.Retention{{}}},
	})
	const want = "CacheBytes Groups Groups.Age Groups.Group Groups.NonEmpty Groups.Queries Queries Queries.Age Queries.AsOf Queries.At Queries.Dir Queries.Expect Queries.Fps Queries.From Queries.Group Queries.Layer Queries.Op Queries.Rank Queries.Size Queries.To QueriesDigest RulesDigest Shadow Spec Spec.BatchSize Spec.CacheFraction Spec.Hot Spec.Median Spec.MinNonEmpty Spec.Retentions Spec.Retentions.At Spec.Retentions.Keep Spec.Version Spec.Workload SpecDigest Stream Stream.Digest Stream.Dropped Stream.End Stream.Horizon Stream.LastSeq Stream.OldAt Stream.OldToken Stream.PayloadBytes Stream.Records Stream.Retentions Stream.Retentions.AfterRecords Stream.Retentions.Horizon Stream.Retentions.LastSeq Stream.Start Stream.TokenFloor"
	if got != want {
		t.Errorf("the plan file has keys\n%s\nwant\n%s", got, want)
	}
	qr := flat(runner.Results{Queries: []runner.QueryResult{{}}, Build: runner.BuildInfo{}})
	const wantResults = "Build Build.CGO Build.Executable Build.GOARCH Build.GOOS Build.GoVersion Build.Invariants Build.Modified Build.Race Build.Revision Build.Tags Build.Unoptimized Candidate Describe ManifestDigest Mismatches PlanDigest Queries Queries.Counters Queries.Digest Queries.Size Queries.Warm StatsAfter StatsBefore Unstable Untimed"
	if qr != wantResults {
		t.Errorf("the results file has keys\n%s\nwant\n%s", qr, wantResults)
	}
}

// A stream with the shapes of real churn (fresh identities, a heartbeat on every
// pod, a backlog, runs that are coalesced and extended) has prefixes busy with run
// extensions, so the groups chosen by extensions exist, and every candidate agrees
// with the reference on them.
func TestAClusterShapedStreamIsAgreedOn(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := runner.DefaultSpec(conformance.Configs()[6])
	spec.BatchSize = 200
	spec.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	spec.MinNonEmpty = 0
	plan := mustPlan(t, spec)
	groups := 0
	for _, g := range plan.Groups {
		if strings.Contains(g.Group, "hot-extensions") && g.NonEmpty > 0 {
			groups++
		}
	}
	if groups == 0 {
		t.Fatalf("no group chosen by run extensions in %d groups", len(plan.Groups))
	}
	for _, v := range candidates.All() {
		_, _, res := run(t, plan, v)
		if len(res.Mismatches) != 0 || len(res.Unstable) != 0 {
			t.Errorf("%s: mismatches %v, unstable %v", v.Name, first(res.Mismatches), first(res.Unstable))
		}
	}
}

// A read that is told to stop stops between queries, with the reason.
func TestAReadStopsWhenToldTo(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	dir, _, _ := run(t, plan, lookup(t, "M/crdb1"))
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	v := breaking(t, "M/crdb1", func(f *faulty) {
		f.onRead = func(*faulty) {
			if calls++; calls == 10 {
				cancel()
			}
		}
	})
	if _, err := runner.Read(ctx, plan, v, dir, clean, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("a read cancelled in its tenth query: %v", err)
	}
	if calls > 11 {
		t.Errorf("the read went on for %d more queries after it was told to stop", calls-10)
	}
}
