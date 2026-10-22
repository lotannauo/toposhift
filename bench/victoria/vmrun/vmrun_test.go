package vmrun_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/actfixture"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vminput"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/bench/victoria/vmrun"
	"github.com/lotannauo/toposhift/internal/identity"
)

var envOpts = vmrun.EnvOptions{FlagsPath: "../scripts/flags.conf", ScriptPath: "../scripts/vm-server.sh"}

var (
	planOnce sync.Once
	planMade *runner.Plan
	planErr  error
)

// fixturePlan is a plan over the small registered cluster, made once.
func fixturePlan(t testing.TB) *runner.Plan {
	t.Helper()
	planOnce.Do(func() { planMade, planErr = runner.MakePlan(context.Background(), fixture.Spec(), nil) })
	if planErr != nil {
		t.Fatal(planErr)
	}
	return planMade
}

// planFile saves a plan and returns its path and the SHA-256 of the file.
func planFile(t testing.TB, p *runner.Plan) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	sum, err := vmrun.FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, sum
}

func planInput(t testing.TB) *vmrun.Input {
	t.Helper()
	in, err := vmrun.PlanInput(fixturePlan(t), "test")
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// activityFiles writes the every-sample file of the default stream, the file the
// product's coalescer makes of it, and the questions of the first.
func activityFiles(t testing.TB) (every, coalesced, questions, questionsSHA string) {
	t.Helper()
	dir := t.TempDir()
	rs := actfixture.Placement(actfixture.Default())
	every = actfixture.Write(t, dir, "s.parquet", rs)
	coalesced = actfixture.Write(t, dir, "c.parquet", actfixture.Coalesce(t, rs, actfixture.DefaultCoalescer()))
	q, err := vminput.BuildQuestions(context.Background(), every, nil)
	if err != nil {
		t.Fatal(err)
	}
	questions = filepath.Join(dir, "questions.json")
	if err := q.Save(questions); err != nil {
		t.Fatal(err)
	}
	if questionsSHA, err = vmrun.FileSHA256(questions); err != nil {
		t.Fatal(err)
	}
	return every, coalesced, questions, questionsSHA
}

// ---- guards that need no server ----

// An output is never written inside a git work tree, whether the directory exists or not
// and however deep it is below the work tree's root.
func TestAnOutputInsideAGitWorkTreeIsRefused(t *testing.T) {
	tree := t.TempDir()
	// In a linked work tree .git is a file, in a clone a directory: both mark one.
	if err := os.WriteFile(filepath.Join(tree, ".git"), []byte("gitdir: /elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	clone := t.TempDir()
	if err := os.Mkdir(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{tree, clone} {
		for _, d := range []string{root, filepath.Join(root, "out"), filepath.Join(root, "a", "b", "c")} {
			if err := vmrun.RefuseInWorkTree(d); !errors.Is(err, vmrun.ErrInWorkTree) {
				t.Errorf("%s: %v", d, err)
			}
			if err := vmrun.WriteJSON(d, "x.json", map[string]int{"a": 1}); !errors.Is(err, vmrun.ErrInWorkTree) {
				t.Errorf("WriteJSON into %s: %v", d, err)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(tree, "out")); err == nil {
		t.Error("a directory was made inside the work tree")
	}
	outside := filepath.Join(t.TempDir(), "run", "1")
	if err := vmrun.RefuseInWorkTree(outside); err != nil {
		t.Errorf("a directory outside any work tree: %v", err)
	}
	if err := vmrun.WriteJSON(outside, "x.json", map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	var back map[string]int
	if err := vmrun.ReadJSON(outside, "x.json", &back); err != nil || back["a"] != 1 {
		t.Errorf("%v, %v", back, err)
	}
	if err := vmrun.ReadJSON(outside, "missing.json", &back); err == nil {
		t.Error("a missing file was read")
	}
}

// A plan is used only if its file is the one named in advance.
func TestAPlanThatIsNotTheOneNamedInAdvanceIsRefused(t *testing.T) {
	path, sum := planFile(t, fixturePlan(t))
	if _, err := vmrun.LoadPlanInput(path, sum); err != nil {
		t.Fatal(err)
	}
	if _, err := vmrun.LoadPlanInput(path, strings.ToUpper(sum)); err != nil {
		t.Errorf("an upper-case digest: %v", err)
	}
	other := "0" + sum[1:]
	if sum[0] == '0' {
		other = "1" + sum[1:]
	}
	for name, expect := range map[string]string{"another digest": other, "none": ""} {
		if _, err := vmrun.LoadPlanInput(path, expect); !errors.Is(err, vmrun.ErrDigest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := vmrun.LoadPlanInput(filepath.Join(t.TempDir(), "missing.json"), sum); err == nil {
		t.Error("a missing plan was read")
	}
	// A questions file is held to the same rule.
	every, _, questions, qsum := activityFiles(t)
	if _, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{Questions: questions, ExpectQuestions: qsum}); err != nil {
		t.Fatal(err)
	}
	for name, expect := range map[string]string{"another digest": "00" + qsum[2:], "none": ""} {
		if _, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{Questions: questions, ExpectQuestions: expect}); !errors.Is(err, vmrun.ErrDigest) {
			t.Errorf("questions, %s: %v", name, err)
		}
	}
}

// Questions chosen from one file are not read against another.
func TestQuestionsOfAnotherFileAreRefused(t *testing.T) {
	_, _, questions, qsum := activityFiles(t)
	o := actfixture.Default()
	o.Pods = 41
	other := actfixture.Write(t, t.TempDir(), "other.parquet", actfixture.Placement(o))
	if _, err := vmrun.ActivityInput(other, vmrun.ActivityOptions{Questions: questions, ExpectQuestions: qsum}); err == nil {
		t.Error("the questions of one file were accepted for another")
	}
}

// The reference's stream is regenerated and must be the digest the plan records.
func TestAPlanWhoseStreamDoesNotRegenerateIsRefused(t *testing.T) {
	p := *fixturePlan(t)
	p.Stream.Digest = "00" + p.Stream.Digest[2:]
	in, err := vmrun.PlanInput(&p, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := in.Worlds(context.Background()); !errors.Is(err, vmrun.ErrPlanStream) {
		t.Errorf("%v", err)
	}
	p = *fixturePlan(t)
	p.Stream.LastSeq++
	in, _ = vmrun.PlanInput(&p, "test")
	if _, _, err := in.Worlds(context.Background()); !errors.Is(err, vmrun.ErrPlanStream) {
		t.Errorf("a different last seq: %v", err)
	}
}

func TestMonthsAndTheOneMonthRule(t *testing.T) {
	ms := func(s string) int64 {
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		return tm.UnixMilli()
	}
	cases := []struct {
		first, last string
		months      int
		ok          bool
	}{
		{"2026-01-01T00:00:00Z", "2026-01-31T23:59:59.999Z", 1, true},
		{"2026-01-31T23:59:59.999Z", "2026-02-01T00:00:00Z", 2, false},
		{"2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z", 3, false},
		{"2025-12-31T23:00:00Z", "2026-01-01T01:00:00Z", 2, false},
		{"2022-08-22T00:00:00Z", "2022-08-22T00:00:00Z", 1, true},
	}
	for _, c := range cases {
		counts := vmmodel.Counts{Samples: 5, FirstMs: ms(c.first), LastMs: ms(c.last)}
		if got := vmrun.Months(counts); got != c.months {
			t.Errorf("%s to %s: %d months, want %d", c.first, c.last, got, c.months)
		}
		err := vmrun.CheckOneMonth(counts)
		if c.ok != (err == nil) || (!c.ok && !errors.Is(err, vmrun.ErrMonths)) {
			t.Errorf("%s to %s: %v", c.first, c.last, err)
		}
	}
	if vmrun.Months(vmmodel.Counts{}) != 0 || vmrun.CheckOneMonth(vmmodel.Counts{}) != nil {
		t.Error("nothing written spans no month")
	}
}

// ---- convert ----

func TestConvertCountsAreDeterministicAndAreTheConvertersOwn(t *testing.T) {
	ctx := context.Background()
	path, sum := planFile(t, fixturePlan(t))
	in, err := vmrun.LoadPlanInput(path, sum)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := vmrun.Convert(ctx, in, 2, envOpts)
	if err != nil {
		t.Fatal(err)
	}

	// The converter run by hand over the same stream gives the same counts.
	conv, err := vmmodel.NewConverter(vmmodel.Options{Registry: in.Registry, Until: in.Until})
	if err != nil {
		t.Fatal(err)
	}
	every, pinned := vmmodel.EverySampleSpec(in.Plan.Spec)
	info, err := runner.Drive(ctx, every, vmmodel.Projection(pinned, conv))
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rep.Counts, conv.Counts()) || rep.SamplesDigest != conv.Digest() {
		t.Errorf("the report's counts are not the converter's")
	}
	if rep.Every.Digest != info.Digest || rep.Every.Records != info.Records || rep.Every.LastSeq != info.LastSeq {
		t.Errorf("stream %+v, want %+v", rep.Every, info)
	}
	if rep.Runs != 2 || rep.Label != "local, deterministic" || rep.Months != 1 {
		t.Errorf("runs %d, label %q, months %d", rep.Runs, rep.Label, rep.Months)
	}
	// The coalesced stream's digest, which the plan records, is beside the every-sample
	// stream's, and the two differ.
	if rep.Input.CoalescedStreamDigest != in.Plan.Stream.Digest || rep.Input.CoalescedStreamDigest == rep.Every.Digest {
		t.Errorf("streams: %s and %s", rep.Input.CoalescedStreamDigest, rep.Every.Digest)
	}
	if rep.Input.PlanSHA256 != sum || rep.Input.RegistrySHA256 != in.Registry.Digest() || rep.Input.Kind != vmrun.KindPlan {
		t.Errorf("input %+v", rep.Input)
	}
	// What it is run against is recorded: the flags file's digest, the registered image,
	// the machine.
	flagsSum, _ := vmrun.FileSHA256("../scripts/flags.conf")
	if rep.Env.FlagsSHA256 != flagsSum || rep.Env.FlagsChecked || !strings.HasPrefix(rep.Env.Image, "victoriametrics/victoria-metrics:v") || rep.Env.CPU == "" || rep.Env.OS == "" || rep.Env.Go == "" {
		t.Errorf("env %+v", rep.Env)
	}
	if c := rep.Counts; c.Samples == 0 || c.Markers == 0 || c.Scraped == 0 || c.Series == 0 || len(c.Churn) == 0 {
		t.Errorf("counts %+v", c)
	}

	// Two runs are the least that make a count deterministic.
	if _, err := vmrun.Convert(ctx, in, 1, envOpts); err == nil {
		t.Error("one run was accepted")
	}
	if _, err := vmrun.Convert(ctx, in, 2, vmrun.EnvOptions{}); err == nil {
		t.Error("no flags.conf was accepted")
	}
}

// Two conversions of a stream that is not the same twice are not deterministic counts.
func TestConvertRefusesAStreamThatChanges(t *testing.T) {
	in := planInput(t)
	real := planInput(t).Every // the stream itself, not the input that is changed
	calls := 0
	in.SetEvery(func(ctx context.Context, sinks ...runner.Sink) (vmrun.Stream, error) {
		calls++
		st, err := real(ctx, sinks...)
		if calls == 2 {
			st.Digest = "changed"
		}
		return st, err
	})
	if _, err := vmrun.Convert(context.Background(), in, 2, envOpts); !errors.Is(err, vmrun.ErrNotDeterministic) {
		t.Errorf("a stream whose digest changes: %v", err)
	}
	// The counts differ too when a record is missing the second time.
	calls = 0
	in2 := planInput(t)
	real2 := planInput(t).Every
	dropped := false
	in2.SetEvery(func(ctx context.Context, sinks ...runner.Sink) (vmrun.Stream, error) {
		calls++
		if calls == 2 {
			sinks = append([]runner.Sink{dropFirst{sinks[0], &dropped}}, sinks[1:]...)
		}
		return real2(ctx, sinks...)
	})
	if _, err := vmrun.Convert(context.Background(), in2, 2, envOpts); !errors.Is(err, vmrun.ErrNotDeterministic) {
		t.Errorf("a stream that loses a record: %v", err)
	}
}

// dropFirst loses the first record of the first batch it is given.
type dropFirst struct {
	runner.Sink
	dropped *bool
}

func (d dropFirst) Write(b []engine.Record) error {
	if !*d.dropped && len(b) > 1 {
		*d.dropped = true
		b = b[1:]
	}
	return d.Sink.Write(b)
}

func TestConvertAnActivityFile(t *testing.T) {
	every, _, _, _ := activityFiles(t)
	in, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := vmrun.Convert(context.Background(), in, 2, envOpts)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := vminput.Info(every)
	if rep.Every.Digest != info.Digest || rep.Every.Records != uint64(info.Records) || rep.Counts.Markers != 0 || rep.Counts.Scraped != 0 || rep.Months != 1 {
		t.Errorf("%+v", rep)
	}
	if rep.Counts.Samples != uint64(info.Records) || rep.Input.Activity != "s.parquet" || rep.Input.CoalescedStreamDigest != "" {
		t.Errorf("%+v", rep)
	}
	// A TTL that is not the file's is a registry that contradicts the stream.
	bad, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vmrun.Convert(context.Background(), bad, 2, envOpts); !errors.Is(err, vmmodel.ErrWindow) {
		t.Errorf("another TTL: %v", err)
	}
	// An input with no questions cannot be read.
	if _, err := in.Queries(); err == nil {
		t.Error("questions of an input that has none")
	}
}

// ---- the premise ----

func TestFactsOnAFullInput(t *testing.T) {
	ctx := context.Background()
	in := planInput(t)
	rep, err := vmrun.Facts(ctx, in, "", 0, false, envOpts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Facts.Subjects < 1000 || rep.Facts.Identical+rep.Facts.EndsEarlier != rep.Facts.Subjects || rep.Facts.SmallestBand == 0 {
		t.Errorf("%+v", rep.Facts)
	}
	// Pods end earlier by up to half an hour and a node's band is four minutes: the check
	// holds each subject to its own band, so this passes, and the strict reading, which
	// compares the largest early end with the smallest band, does not.
	if rep.WithinSmallestBand || rep.Facts.MaxEarly < rep.Facts.SmallestBand {
		t.Errorf("max early %s, smallest band %s", rep.Facts.MaxEarly, rep.Facts.SmallestBand)
	}
	if _, err := vmrun.Facts(ctx, in, "", 0, true, envOpts); !errors.Is(err, vmrun.ErrBand) {
		t.Errorf("the strict reading: %v", err)
	}
	if rep.Projected || rep.PinnedEntities != 0 || rep.Retentions != 0 || rep.ExtendFraction != 0.5 || rep.Label != "local, deterministic" {
		t.Errorf("%+v", rep)
	}

	// A plan with a retention: the every-sample spec carries it, and the premise holds
	// with it.
	rp, err := runner.MakePlan(ctx, fixture.RetainingSpec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := vmrun.PlanInput(rp, "test")
	if err != nil {
		t.Fatal(err)
	}
	retainedRep, err := vmrun.Facts(ctx, retained, "", 0, false, envOpts)
	if err != nil || retainedRep.Retentions != 1 {
		t.Fatalf("%+v, %v", retainedRep, err)
	}

	// An activity file, against its coalesced counterpart.
	every, coalesced, _, _ := activityFiles(t)
	ain, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	arep, err := vmrun.Facts(ctx, ain, coalesced, vminput.DefaultExtendFraction, false, envOpts)
	if err != nil {
		t.Fatal(err)
	}
	if arep.Coalesced != "c.parquet" || arep.Facts.EndsEarlier == 0 || arep.Facts.MaxEarly == 0 || arep.ExtendFraction != 0.5 {
		t.Errorf("%+v", arep)
	}
	// One producer and one window: every end that is early is below the one band, so the
	// strict reading holds too.
	if !arep.WithinSmallestBand {
		t.Errorf("max early %s, smallest band %s", arep.Facts.MaxEarly, arep.Facts.SmallestBand)
	}
	if _, err := vmrun.Facts(ctx, ain, coalesced, vminput.DefaultExtendFraction, true, envOpts); err != nil {
		t.Errorf("the strict reading on one window: %v", err)
	}
	if _, err := vmrun.Facts(ctx, ain, "", 0.5, false, envOpts); err == nil {
		t.Error("no coalesced file")
	}
	// The wrong fraction: the ends the coalescer moved are more than it says it may.
	if _, err := vmrun.Facts(ctx, ain, coalesced, 0.01, false, envOpts); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("a narrower fraction: %v", err)
	}
}

// ---- read, through the model of the store ----

// modelEngine is the model of the store written the input's every-sample stream.
func modelEngine(t testing.TB, in *vmrun.Input) *vmquery.Engine {
	t.Helper()
	st := vmmodel.NewStore()
	var conv *vmmodel.Converter
	conv, err := vmmodel.NewConverter(vmmodel.Options{
		Registry: in.Registry, Until: in.Until,
		Emit: func(ss []vmmodel.Sample) error { return st.Sink(conv)(ss) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := in.Every(context.Background(), conv); err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Seal(); err != nil {
		t.Fatal(err)
	}
	return vmquery.NewModelEngine(in.Registry, st, vmmodel.Semantics{}, in.LastSeq())
}

func TestReadASyntheticPlanThroughTheModel(t *testing.T) {
	in := planInput(t)
	rep, err := vmrun.Read(context.Background(), in, modelEngine(t, in), vmrun.Env{})
	if err != nil {
		t.Fatal(err)
	}
	tot := rep.Gate.Total()
	if rep.Invalid != "" || tot.Asked != len(in.Plan.Queries) || tot.Compared < 300 || tot.Equal != tot.Compared || len(rep.NotCompared) != 0 {
		t.Errorf("%+v\n%s", rep, rep.Gate)
	}
	// What the store cannot express is a row of the capability table, never compared.
	if rep.Gate.Rows[vmmodel.CapWindow] == 0 || rep.Gate.Rows[vmmodel.CapAsKnownAt] == 0 || len(rep.Gate.Rows) != 2 || tot.Refused == 0 {
		t.Errorf("rows %v", rep.Gate.Rows)
	}
}

func TestReadAnActivityFileThroughTheModel(t *testing.T) {
	every, _, questions, qsum := activityFiles(t)
	in, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{Questions: questions, ExpectQuestions: qsum})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := vmrun.Read(context.Background(), in, modelEngine(t, in), vmrun.Env{})
	if err != nil {
		t.Fatalf("%v\n%+v", err, rep)
	}
	tot := rep.Gate.Total()
	if tot.Asked != len(in.Questions.Queries) || tot.Compared == 0 || tot.Equal != tot.Compared {
		t.Errorf("%+v\n%s", tot, rep.Gate)
	}
	for _, f := range []string{"node<-", "pod->", "node exists"} {
		if got := rep.Gate.ByFamily[f]; got == nil || got.Compared == 0 {
			t.Errorf("family %s: %+v", f, got)
		}
	}
	if rep.Gate.Rows[vmmodel.CapWindow] == 0 || rep.Gate.Rows[vmmodel.CapAsKnownAt] == 0 {
		t.Errorf("rows %v", rep.Gate.Rows)
	}
	if rep.Input.QuestionsSHA256 != qsum || rep.Input.Activity != "s.parquet" {
		t.Errorf("%+v", rep.Input)
	}
}

// brokenEngine answers like the engine it wraps, with what is wrong with it.
type brokenEngine struct {
	engine.Engine
	dropPeer, refuseAll bool
}

func (b brokenEngine) refusal() error {
	return &vmquery.NotExpressibleError{Capability: vmmodel.CapWindow, Question: "all"}
}

func (b brokenEngine) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	if b.refuseAll {
		return nil, b.refusal()
	}
	ns, err := b.Engine.Neighbors(fp, d, t, s)
	if b.dropPeer && len(ns) > 0 {
		ns = ns[1:]
	}
	return ns, err
}

func (b brokenEngine) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	if b.refuseAll {
		return nil, b.refusal()
	}
	return b.Engine.NeighborsBatch(fps, d, t, s)
}

func (b brokenEngine) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	if b.refuseAll {
		return false, b.refusal()
	}
	return b.Engine.Alive(fp, t, s)
}

func TestAReadThatDiffersFromTheReferenceMakesTheArmInvalid(t *testing.T) {
	in := planInput(t)
	good := modelEngine(t, in)
	rep, err := vmrun.Read(context.Background(), in, brokenEngine{Engine: good, dropPeer: true}, vmrun.Env{})
	if !errors.Is(err, vmrun.ErrInvalid) || rep == nil || rep.Invalid == "" || rep.Gate.Wrong == 0 || len(rep.Gate.Mismatches) == 0 {
		t.Fatalf("a missing neighbor: %v, %+v", err, rep)
	}
	// An engine that refuses everything compares nothing: that is not a pass.
	rep, err = vmrun.Read(context.Background(), in, brokenEngine{Engine: good, refuseAll: true}, vmrun.Env{})
	if !errors.Is(err, vmrun.ErrInvalid) || rep == nil || rep.Gate.Total().Compared != 0 || !strings.Contains(rep.Invalid, "no question was compared") {
		t.Fatalf("an engine that refused everything: %v, %+v", err, rep)
	}
}

func TestReadNeedsQuestions(t *testing.T) {
	every, _, _, _ := activityFiles(t)
	in, err := vmrun.ActivityInput(every, vmrun.ActivityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vmrun.Read(context.Background(), in, nil, vmrun.Env{}); err == nil {
		t.Error("an input with no questions was read")
	}
}

// ---- merge and sizes ----

func TestWalkSize(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, n int) {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("data/small/a", 100)
	write("data/big/b", 50)
	write("indexdb/c", 7)
	write("flock.lock", 0)
	write("cache/x", 3)
	got, err := vmrun.WalkSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 160 || got.Files != 5 || got.ByEntry["data"] != 150 || got.ByEntry["indexdb"] != 7 || got.ByEntry["cache"] != 3 || got.ByEntry["flock.lock"] != 0 {
		t.Errorf("%+v", got)
	}
	if !reflect.DeepEqual(got.Entries(), []string{"cache", "data", "flock.lock", "indexdb"}) {
		t.Errorf("entries %v", got.Entries())
	}
	if _, err := vmrun.WalkSize(filepath.Join(dir, "missing")); err == nil {
		t.Error("a missing directory was walked")
	}
}
