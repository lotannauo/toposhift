package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func shaOf(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// planFile writes a tiny plan with two retentions and returns its path.
func planFile(t *testing.T, seed uint64) string {
	t.Helper()
	cfg := workload.Tiny()
	cfg.Seed = seed
	spec := runner.DefaultSpec(cfg)
	spec.BatchSize = 64
	spec.MinNonEmpty = 0
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	return path
}

type result struct {
	out, errOut string
	err         error
}

func exec(args ...string) result {
	var out, errOut bytes.Buffer
	err := run(context.Background(), args, &out, &errOut)
	return result{out.String(), errOut.String(), err}
}

func noDuration(t *testing.T, what string, rs ...string) {
	t.Helper()
	for _, s := range rs {
		if m := duration.FindString(s); m != "" {
			t.Errorf("%s: the output has what looks like a time, %q:\n%s", what, m, s)
		}
	}
}

// TestBuildReadExplainSizesEnv runs the whole path on a tiny plan: build every arm, read
// them (the gate, then the counters, twice), audit the counting, report the sizes and the
// environment.
func TestBuildReadExplainSizesEnv(t *testing.T) {
	plan := planFile(t, 1)
	sum := shaOf(t, plan)
	root := t.TempDir()
	build := filepath.Join(root, "build")

	r := exec("build", "-plan", plan, "-expect-plan", sum, "-out", build)
	if r.err != nil {
		t.Fatalf("build: %v\n%s", r.err, r.out)
	}
	for _, want := range []string{"plan sha256 " + sum, "stream digest ", "file a1/archive.parquet", "file a2/fwd.parquet", "file i/entities.parquet"} {
		if !strings.Contains(r.out, want) {
			t.Errorf("build: no %q in\n%s", want, r.out)
		}
	}
	noDuration(t, "build", r.out, r.errOut)

	out1, out2 := filepath.Join(root, "c1"), filepath.Join(root, "c2")
	r = exec("read", "-dir", build, "-plan", plan, "-expect-plan", sum, "-out", out1)
	if r.err != nil {
		t.Fatalf("read: %v\n%s", r.err, r.out)
	}
	if strings.Count(r.out, "G0 total:") != 3 || strings.Contains(r.out, "MISMATCH") {
		t.Errorf("read:\n%s", r.out)
	}
	noDuration(t, "read", r.out, r.errOut)
	// Reading again writes the same bytes.
	if r2 := exec("read", "-dir", build, "-plan", plan, "-expect-plan", sum, "-out", out2, "-arms", "A2,I"); r2.err != nil {
		t.Fatal(r2.err)
	}
	for _, arm := range []string{"A2", "I"} {
		a, _ := os.ReadFile(filepath.Join(out1, arm+".counters.json"))
		b, _ := os.ReadFile(filepath.Join(out2, arm+".counters.json"))
		if len(a) == 0 || !bytes.Equal(a, b) {
			t.Errorf("%s: two reads wrote different counters files (%d and %d bytes)", arm, len(a), len(b))
		}
	}
	var cf duckengine.CountersFile
	b, _ := os.ReadFile(filepath.Join(out1, "A1.counters.json"))
	if err := json.Unmarshal(b, &cf); err != nil {
		t.Fatal(err)
	}
	if cf.Arm != duckengine.ArmA1 || cf.Passes != 2 || cf.Plan.SHA256 != sum || cf.Env.DuckDB != "v1.5.6" || len(cf.Queries) == 0 || cf.Plan.Queries != len(cf.Queries) {
		t.Errorf("the counters file: %+v", cf.Plan)
	}
	if strings.Contains(string(b), "Latency") || strings.Contains(string(b), "LATENCY") {
		t.Error("the counters file has a timing")
	}

	for _, arm := range []string{"A1", "A2", "I"} {
		r = exec("explain", "-dir", build, "-plan", plan, "-expect-plan", sum, "-arm", arm, "-max", "6")
		if r.err != nil || !strings.Contains(r.out, "the count agrees") || strings.Contains(r.out, "DIFFERS") {
			t.Errorf("explain %s: %v\n%s", arm, r.err, r.out)
		}
		noDuration(t, "explain "+arm, r.out, r.errOut)
	}
	r = exec("explain", "-dir", build, "-plan", plan, "-expect-plan", sum, "-arm", "A2", "-query", "0,1")
	if r.err != nil || strings.Count(r.out, "\nquestion ") != 2 {
		t.Errorf("explain -query: %v\n%s", r.err, r.out)
	}

	r = exec("sizes", "-dir", build, "-verify-hashes")
	if r.err != nil || !strings.Contains(r.out, "arm A1:") || !strings.Contains(r.out, "arm A2:") || !strings.Contains(r.out, "arm I with the record files it reads") {
		t.Errorf("sizes: %v\n%s", r.err, r.out)
	}
	noDuration(t, "sizes", r.out)

	r = exec("env")
	var env duckengine.Env
	if err := json.Unmarshal([]byte(r.out), &env); err != nil || env.DuckDB != "v1.5.6" || env.SettingsSHA256 == "" || env.ArchiveOrder == "" {
		t.Errorf("env: %v\n%s", err, r.out)
	}
}

func TestRefusals(t *testing.T) {
	plan := planFile(t, 1)
	sum := shaOf(t, plan)
	root := t.TempDir()
	build := filepath.Join(root, "build")
	if r := exec("build", "-plan", plan, "-expect-plan", sum, "-out", build, "-arms", "A2,I"); r.err != nil {
		t.Fatal(r.err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	inTree := filepath.Join(wd, "scratch-that-must-not-be-made")
	zeros := strings.Repeat("0", 64)
	otherPlan := planFile(t, 2)
	otherSum := shaOf(t, otherPlan)

	for name, args := range map[string][]string{
		"a build in a git work tree":             {"build", "-plan", plan, "-expect-plan", sum, "-out", inTree},
		"counters in a git work tree":            {"read", "-dir", build, "-plan", plan, "-expect-plan", sum, "-out", inTree, "-arms", "A2"},
		"a build of a plan with another digest":  {"build", "-plan", plan, "-expect-plan", zeros, "-out", filepath.Join(root, "x")},
		"a build with no expected digest":        {"build", "-plan", plan, "-out", filepath.Join(root, "x")},
		"a build with no output":                 {"build", "-plan", plan, "-expect-plan", sum},
		"a build with an unknown arm":            {"build", "-plan", plan, "-expect-plan", sum, "-out", filepath.Join(root, "x"), "-arms", "A3"},
		"a build into what holds a build":        {"build", "-plan", plan, "-expect-plan", sum, "-out", build},
		"a read of another digest":               {"read", "-dir", build, "-plan", plan, "-expect-plan", zeros},
		"a read with no expected digest":         {"read", "-dir", build, "-plan", plan},
		"a read of another plan's build":         {"read", "-dir", build, "-plan", otherPlan, "-expect-plan", otherSum},
		"a read of an arm that was not built":    {"read", "-dir", build, "-plan", plan, "-expect-plan", sum, "-arms", "A1"},
		"a read of no build":                     {"read", "-dir", filepath.Join(root, "none"), "-plan", plan, "-expect-plan", sum},
		"an explain of two arms":                 {"explain", "-dir", build, "-plan", plan, "-expect-plan", sum, "-arm", "A2,I"},
		"an explain of a question not in a plan": {"explain", "-dir", build, "-plan", plan, "-expect-plan", sum, "-arm", "A2", "-query", "999999"},
		"a timed read":                           {"read", "-dir", build, "-plan", plan, "-expect-plan", sum, "-timed"},
		"a timed build":                          {"build", "-plan", plan, "-expect-plan", sum, "-out", filepath.Join(root, "y"), "-timed"},
		"sizes of nothing":                       {"sizes", "-dir", filepath.Join(root, "none")},
		"a stray argument":                       {"env", "extra"},
	} {
		r := exec(args...)
		if r.err == nil {
			t.Errorf("%s was accepted:\n%s", name, r.out)
		}
		noDuration(t, name, r.out)
	}
	if _, err := os.Stat(inTree); err == nil {
		os.RemoveAll(inTree)
		t.Error("a directory was made inside the work tree")
	}
	if _, err := os.Stat(filepath.Join(root, "x")); err == nil {
		t.Error("a refused build made its directory")
	}
}

func TestNoCounterIsWrittenWhenAnAnswerIsWrong(t *testing.T) {
	ctx := context.Background()
	planA, planB := planFile(t, 1), planFile(t, 2)
	sumA := shaOf(t, planA)
	build := filepath.Join(t.TempDir(), "build")
	if r := exec("build", "-plan", planA, "-expect-plan", sumA, "-out", build, "-arms", "A2"); r.err != nil {
		t.Fatal(r.err)
	}
	pB, err := runner.LoadPlan(planB)
	if err != nil {
		t.Fatal(err)
	}
	// The files of one stream asked the questions of another: G0 fails, and no counter
	// comes of it.
	var out bytes.Buffer
	inB := &input{kind: "plan", queries: pB.Queries, queriesDigest: pB.QueriesDigest, stream: pB.Stream, plan: pB}
	cf, err := readArm(ctx, build, duckengine.ArmA2, inB, nil, &out, true)
	if err == nil || cf != nil || !strings.Contains(err.Error(), "no counter is kept") {
		t.Fatalf("the questions of another plan: %v, %v", cf, err)
	}
	if strings.Contains(out.String(), "counters (every question") {
		t.Errorf("counters were printed:\n%s", out.String())
	}
}

func TestRefuseInWorkTree(t *testing.T) {
	wd, _ := os.Getwd()
	if err := refuseInWorkTree(filepath.Join(wd, "a", "b")); err == nil {
		t.Error("a directory that does not exist yet, in the work tree")
	}
	if err := refuseInWorkTree(wd); err == nil {
		t.Error("the work tree")
	}
	root := t.TempDir()
	if err := refuseInWorkTree(filepath.Join(root, "x", "y")); err != nil {
		t.Errorf("a temporary directory: %v", err)
	}
	// A work tree of another kind of checkout, whose .git is a file.
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: /elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := refuseInWorkTree(filepath.Join(root, "x", "y")); err == nil {
		t.Error("a directory under a .git file")
	}
}
