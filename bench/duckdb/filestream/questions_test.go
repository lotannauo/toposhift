package filestream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// planOf is the plan the runner makes of the stream the file was written from.
func planOf(t testing.TB, g generated) *runner.Plan {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), g.spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func makeQuestions(t testing.TB, g generated) *QuestionSet {
	t.Helper()
	qs, err := Make(context.Background(), g.path, MakeOptions{Options: g.options(), MinNonEmpty: g.spec.MinNonEmpty}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return qs
}

// TestTheQuestionsAreTheRunnersPlan: the questions made from the file of a generated
// stream, and the reference engine's answer to each, are the plan the runner makes from
// the generator: the same questions in the same order with the same answers, and the same
// digest. Only the two ways of finding them differ, so a question set is a plan.
func TestTheQuestionsAreTheRunnersPlan(t *testing.T) {
	for _, cfg := range []func() workload.Config{
		workload.Tiny,
		func() workload.Config {
			c := workload.Tiny()
			c.CoalesceRuns, c.ExtendTTLFraction = true, 0.5
			return c
		},
		func() workload.Config {
			c := workload.Tiny()
			c.ConfirmProbability, c.ConfirmTTL = 0.5, 4*60e9
			return c
		},
	} {
		g := generate(t, cfg(), 64)
		plan, qs := planOf(t, g), makeQuestions(t, g)
		if len(qs.Queries) == 0 || len(qs.Queries) != len(plan.Queries) {
			t.Fatalf("%d questions, the plan has %d", len(qs.Queries), len(plan.Queries))
		}
		if !reflect.DeepEqual(qs.Queries, plan.Queries) {
			for i := range qs.Queries {
				if !reflect.DeepEqual(qs.Queries[i], plan.Queries[i]) {
					t.Fatalf("question %d: %+v, the plan's %+v", i, qs.Queries[i], plan.Queries[i])
				}
			}
		}
		if qs.QueriesDigest != plan.QueriesDigest || !reflect.DeepEqual(qs.Groups, plan.Groups) || qs.Shadow != plan.Shadow {
			t.Errorf("digest %s, groups %v, shadow %d; the plan's %s, %v, %d", qs.QueriesDigest, qs.Groups, qs.Shadow, plan.QueriesDigest, plan.Groups, plan.Shadow)
		}
		if !reflect.DeepEqual(qs.Stream, plan.Stream) {
			t.Errorf("stream %+v, the plan's %+v", qs.Stream, plan.Stream)
		}
		var answered int
		for _, q := range qs.Queries {
			if q.Size > 0 {
				answered++
			}
		}
		if answered == 0 {
			t.Error("every answer is empty")
		}
	}
}

// TestAFaultInTheReferenceFeedIsCaught: a reference engine fed only what entities are
// the source of answers reverse reads from half the edges, and the questions made with it
// are not the plan's.
func TestAFaultInTheReferenceFeedIsCaught(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	plan := planOf(t, g)
	fault = mutShadowSourceOnly
	defer func() { fault = mutNone }()
	qs := makeQuestions(t, g)
	if reflect.DeepEqual(qs.Queries, plan.Queries) {
		t.Error("a reference engine that did not see the edges of the targets answered as the plan did")
	}
}

// TestAHourOfFilesAsksTheAgesThatFitIt: a stream of one hour is asked about now, an hour
// back (the first instant), the old snapshot, a read past the end and the hour as a window,
// and about no age that needs more history than it has.
func TestAHourOfFilesAsksTheAgesThatFitIt(t *testing.T) {
	g := generate(t, workload.Small(), 1000)
	qs := makeQuestions(t, g)
	ages := map[string]int{}
	for _, q := range qs.Queries {
		ages[q.Age]++
	}
	for _, want := range []string{runner.AgeNow, runner.Age1h, runner.AgeOldToken, runner.AgeDead, runner.AgeWindow1h} {
		if ages[want] == 0 {
			t.Errorf("no question at age %s: %v", want, ages)
		}
	}
	for _, not := range []string{runner.Age3h, runner.Age9h, runner.Age1d, runner.AgeWindow1d} {
		if ages[not] != 0 {
			t.Errorf("%d questions at age %s, which an hour of history cannot have", ages[not], not)
		}
	}
}

// TestAQuestionSetIsTheSameTwiceAndSurvivesItsFile: making it again gives the same bytes
// and so the same name, and loading what was saved gives the set back.
func TestAQuestionSetIsTheSameTwiceAndSurvivesItsFile(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	a, b := makeQuestions(t, g), makeQuestions(t, g)
	dir := t.TempDir()
	sa, err := a.Save(filepath.Join(dir, "a.json"))
	if err != nil {
		t.Fatal(err)
	}
	sb, err := b.Save(filepath.Join(dir, "b.json"))
	if err != nil {
		t.Fatal(err)
	}
	if sa != sb {
		t.Errorf("two sets of one file have SHA-256 %s and %s", sa, sb)
	}
	back, err := Load(filepath.Join(dir, "a.json"))
	if err != nil {
		t.Fatal(err)
	}
	m1, _ := a.Marshal()
	m2, _ := back.Marshal()
	if !bytes.Equal(m1, m2) {
		t.Error("the set loaded is not the set saved")
	}
	if !reflect.DeepEqual(back.ReplayOptions(), g.options()) {
		t.Errorf("the options to replay it: %+v, not %+v", back.ReplayOptions(), g.options())
	}
	if got, err := os.ReadFile(filepath.Join(dir, "a.json")); err != nil || len(got) == 0 || strings.Contains(string(got), g.path) {
		t.Errorf("the saved set names the path of its input (%v)", err)
	}
}

func TestALoadRefusesWhatWasChanged(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	qs := makeQuestions(t, g)
	dir := t.TempDir()
	good := filepath.Join(dir, "good.json")
	if _, err := qs.Save(good); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(good)
	var n int
	edit := func(f func(map[string]any)) string {
		n++
		var c map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber() // a token above 2^53 stays what it was
		if err := dec.Decode(&c); err != nil {
			t.Fatal(err)
		}
		f(c)
		b, _ := json.Marshal(c)
		p := filepath.Join(dir, fmt.Sprintf("edited-%d.json", n))
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, p := range map[string]string{
		"another format":         edit(func(c map[string]any) { c["Format"] = 2 }),
		"another questions hash": edit(func(c map[string]any) { c["QueriesDigest"] = strings.Repeat("0", 64) }),
		"a question changed": edit(func(c map[string]any) {
			c["Queries"].([]any)[0].(map[string]any)["Rank"] = 99
		}),
		"an unknown field": edit(func(c map[string]any) { c["Extra"] = 1 }),
		"another record count": edit(func(c map[string]any) {
			c["Input"].(map[string]any)["Records"] = 1
		}),
		"a bad input digest": edit(func(c map[string]any) { c["Input"].(map[string]any)["Digest"] = "sha256:00" }),
		"no period":          edit(func(c map[string]any) { c["Period"].(map[string]any)["Duration"] = 0 }),
	} {
		if _, err := Load(p); err == nil {
			t.Errorf("%s was loaded", name)
		}
	}
	// A change to an answer is not a change to the questions: the file's name, its
	// SHA-256, is what holds the answers.
	changed := edit(func(c map[string]any) {
		c["Queries"].([]any)[0].(map[string]any)["Expect"] = strings.Repeat("0", 64)
	})
	b, _ := os.ReadFile(changed)
	sumA, _ := qs.Save(filepath.Join(dir, "again.json"))
	if _, err := Load(changed); err != nil {
		t.Fatalf("the answers are not what the questions' digest covers: %v", err)
	}
	if bytes.Equal(b, raw) || sumA == "" {
		t.Fatal("test setup")
	}
}

// TestAMostlyEmptyGroupRefusesTheSet: engines agree on an empty answer without being
// tested, so a set of which a group is mostly empty is not made.
func TestAMostlyEmptyGroupRefusesTheSet(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	_, err := Make(context.Background(), g.path, MakeOptions{Options: g.options(), MinNonEmpty: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "mostly empty") {
		t.Fatalf("every group wholly non-empty was required of a stream with empty answers: %v", err)
	}
	if _, err := Make(context.Background(), g.path, MakeOptions{Options: g.options(), MinNonEmpty: 2}, nil); err == nil {
		t.Error("a share above 1 was accepted")
	}
}

// TestAReadPastTheEndThatFindsAnEntityAliveRefusesTheSet: nodes whose heartbeats outlive the
// hour past the end are not dead there, so a read that asks whether they are is not the
// read it is meant to be, and the runner's own plan of the stream is refused as well.
func TestAReadPastTheEndThatFindsAnEntityAliveRefusesTheSet(t *testing.T) {
	cfg := workload.Tiny()
	cfg.HeartbeatTTLFactor = 100 // 100 minutes
	g := generate(t, cfg, 64)
	if _, err := runner.MakePlan(context.Background(), g.spec, nil); err == nil || !strings.Contains(err.Error(), "past the end") {
		t.Fatalf("the runner's plan of the stream: %v", err)
	}
	if _, err := Make(context.Background(), g.path, MakeOptions{Options: g.options()}, nil); err == nil || !strings.Contains(err.Error(), "past the end") {
		t.Fatalf("the question set: %v", err)
	}
}
