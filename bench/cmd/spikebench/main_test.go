package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// binary builds the command with cgo off, no race detector and no version control
// stamp, so that it does not depend on whether the tree under test is clean. Such
// a binary is not one a result may come from (the program cannot tell it from one
// built from a dirty tree), so these tests run it untimed and test the refusal.
//
// Each kind of binary (by its extra build flags) is built once for all the tests.
func binary(t *testing.T, extra ...string) string {
	t.Helper()
	key := strings.Join(extra, " ")
	builtMu.Lock()
	defer builtMu.Unlock()
	if out, ok := built[key]; ok {
		return out
	}
	if builtDir == "" {
		dir, err := os.MkdirTemp("", "spikebench-test")
		if err != nil {
			t.Fatal(err)
		}
		builtDir = dir
	}
	out := filepath.Join(builtDir, "spikebench"+strconv.Itoa(len(built)))
	args := append([]string{"build", "-buildvcs=false", "-o", out}, extra...)
	cmd := exec.CommandContext(context.Background(), "go", append(args, ".")...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, b)
	}
	built[key] = out
	return out
}

var (
	builtMu  sync.Mutex
	built    = map[string]string{}
	builtDir string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if builtDir != "" {
		_ = os.RemoveAll(builtDir)
	}
	os.Exit(code)
}

func run(t *testing.T, bin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var so, se bytes.Buffer
	cmd := exec.CommandContext(t.Context(), bin, args...)
	cmd.Stdout, cmd.Stderr = &so, &se
	err = cmd.Run()
	return so.String(), se.String(), err
}

// The whole of a comparison through the real program: a plan, a build and a read
// of each candidate in a process of its own, and a report, with the guards on.
func TestRunComparesCandidatesEndToEnd(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // builds a binary and runs it; the full tier does

	bin := binary(t)
	out := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := run(t, bin, "run", "-untimed", "-preset", "tiny", "-retain", "12m/6m", "-batch", "200", "-min-non-empty", "0",
		"-candidates", "L/off,L/k64a4,M/crdb1+filter", "-out", out)
	if err != nil {
		t.Fatalf("run: %v\n%s\n%s", err, stdout, stderr)
	}
	for _, want := range []string{"L/off", "L/k64a4", "M/crdb1+filter", "table bytes", "seeks (calls that position the iterator)"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the report lacks %q:\n%.800s", want, stdout)
		}
	}
	// A validation run is valid and is not a result: the one thing the report holds
	// against it is that.
	for _, l := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "- ") && !strings.Contains(l, "validation run") {
			t.Errorf("the report holds something else against a valid validation run: %q", l)
		}
	}
	for _, f := range []string{"plan.json", "report.txt", "L_off/manifest.json", "L_off/results.json", "M_crdb1+filter/results.json"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}

	// Without a version control stamp the program cannot know the tree was clean:
	// the plan is there, so the refusal is the guard's.
	if _, stderr, err := run(t, bin, "build", "-candidate", "L/off", "-out", out); err == nil || !strings.Contains(stderr, "version control stamp") {
		t.Errorf("a build by a binary with no version control stamp: %v\n%s", err, stderr)
	}
	if _, stderr, err := run(t, bin, "read", "-candidate", "L/off", "-out", out); err == nil || !strings.Contains(stderr, "version control stamp") {
		t.Errorf("a read by a binary with no version control stamp: %v\n%s", err, stderr)
	}
	// Running again resumes: what was built and read from this plan is not built again.
	if _, stderr, err := run(t, bin, "run", "-untimed", "-candidates", "L/off,L/k64a4,M/crdb1+filter", "-out", out); err != nil || strings.Count(stderr, "not again") != 3 {
		t.Errorf("a second run: %v\n%s", err, stderr)
	}
	// Plan flags against a plan that exists are an error and not quietly dropped.
	if _, stderr, err := run(t, bin, "run", "-untimed", "-preset", "tiny", "-candidates", "L/off", "-out", out); err == nil || !strings.Contains(stderr, "plan flags") {
		t.Errorf("a run with plan flags against an existing plan: %v\n%s", err, stderr)
	}
	// A second plan into the same directory is refused: a plan is made once.
	if _, stderr, err := run(t, bin, "plan", "-preset", "tiny", "-retain", "12m/6m", "-batch", "200", "-min-non-empty", "0", "-out", out); err == nil || !strings.Contains(stderr, "a plan is made once") {
		t.Errorf("a second plan: %v\n%s", err, stderr)
	}
	// So is building into a directory that has a database in it.
	if _, stderr, err := run(t, bin, "build", "-untimed", "-candidate", "L/off", "-out", out); err == nil || !strings.Contains(stderr, "not empty") {
		t.Errorf("a second build: %v\n%s", err, stderr)
	}
	// Asked for no candidates by name, the report covers the ones that were run.
	if def, _, err := run(t, bin, "report", "-untimed", "-out", out); err != nil || !strings.Contains(def, "L/off") || strings.Contains(def, "M/default") {
		t.Errorf("the default report: %v\n%.500s", err, def)
	}
	// Asked for one that was not run, it says so.
	if _, stderr, err := run(t, bin, "report", "-untimed", "-candidates", "M/default", "-out", out); err == nil || !strings.Contains(stderr, "no manifest") {
		t.Errorf("a report of a candidate that was not run: %v\n%s", err, stderr)
	}
	// The report with every age is the file that was written.
	full, _, err := run(t, bin, "report", "-untimed", "-all", "-candidates", "L/off,L/k64a4,M/crdb1+filter", "-out", out)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "report.txt")); string(b) != full {
		t.Error("report -all is not the report.txt it wrote")
	}
}

// A binary built with Pebble's invariant checks is refused for a result, and is
// allowed for a validation run that says it is one.
func TestAnInstrumentedBinaryIsRefusedForAResult(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	good := binary(t)
	instrumented := binary(t, "-tags", "invariants")
	out := filepath.Join(t.TempDir(), "out")
	if _, stderr, err := run(t, good, "plan", "-preset", "tiny", "-min-non-empty", "0", "-out", out); err != nil {
		t.Fatalf("plan: %v\n%s", err, stderr)
	}
	_, stderr, err := run(t, instrumented, "build", "-candidate", "L/off", "-out", out)
	if err == nil || !strings.Contains(stderr, "invariants") || !strings.Contains(stderr, "-untimed") {
		t.Errorf("an invariants build for a result: %v\n%s", err, stderr)
	}
	if _, err := os.Stat(filepath.Join(out, "L_off")); err == nil {
		t.Error("a refused build left a directory behind")
	}
	if _, stderr, err := run(t, instrumented, "build", "-candidate", "L/off", "-untimed", "-out", out); err != nil {
		t.Errorf("an untimed validation build: %v\n%s", err, stderr)
	}
	if _, stderr, err := run(t, instrumented, "read", "-candidate", "L/off", "-untimed", "-out", out); err != nil {
		t.Errorf("an untimed validation read: %v\n%s", err, stderr)
	}
	// A whole run through an instrumented binary passes the flag to each step.
	all := filepath.Join(t.TempDir(), "all")
	if _, stderr, err := run(t, instrumented, "run", "-untimed", "-preset", "tiny", "-min-non-empty", "0", "-candidates", "L/off", "-out", all); err != nil {
		t.Errorf("an untimed run: %v\n%s", err, stderr)
	}
	// What an untimed run made is not a result: the report says so, and fails.
	stdout, _, err := run(t, good, "report", "-candidates", "L/off", "-out", out)
	if err == nil || !strings.Contains(stdout, "NOT TO BE COMPARED") {
		t.Errorf("a report of an untimed run: %v\n%.500s", err, stdout)
	}
	// Asked about the validity of the run, and not whether it can be compared, it passes.
	if _, stderr, err := run(t, good, "report", "-untimed", "-candidates", "L/off", "-out", out); err != nil {
		t.Errorf("the report of a valid untimed run: %v\n%s", err, stderr)
	}
}

// A candidate that answers differently from the reference engine fails the step
// that found out, the run goes on with the others, and the report names the
// disagreement and fails too.
func TestAWrongAnswerFailsTheRunButNotTheOtherCandidates(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	bin := binary(t)
	out := filepath.Join(t.TempDir(), "out")
	if _, stderr, err := run(t, bin, "plan", "-preset", "tiny", "-min-non-empty", "0", "-out", out); err != nil {
		t.Fatalf("plan: %v\n%s", err, stderr)
	}
	// Change what the reference engine is said to have answered to the first question.
	path := filepath.Join(out, "plan.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// (Edited as text: the snapshot token of a read of the newest state does not
	// survive a trip through a float.)
	changed := regexp.MustCompile(`"Expect": "[0-9a-f]+"`).ReplaceAll(raw, []byte(`"Expect": "00000000000000000000000000000000"`))
	if bytes.Equal(changed, raw) {
		t.Fatal("the plan has no expected answer to change")
	}
	raw = changed
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, err := run(t, bin, "run", "-untimed", "-candidates", "L/off,M/crdb1", "-out", out)
	if err == nil || !strings.Contains(stderr, "differently from the reference engine") {
		t.Fatalf("a run with a wrong answer: %v\n%s", err, stderr)
	}
	for _, c := range []string{"L_off", "M_crdb1"} {
		if _, err := os.Stat(filepath.Join(out, c, "results.json")); err != nil {
			t.Errorf("%s was not read after the other was found wrong: %v", c, err)
		}
	}
	if !strings.Contains(stdout, "differ from the reference engine's") || !strings.Contains(stdout, "NOT TO BE COMPARED") {
		t.Errorf("the report of wrong answers:\n%.600s", stdout)
	}
}

func TestOutputInsideARepositoryIsRefused(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	bin := binary(t)
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range [][]string{{"plan"}, {"build", "-candidate", "L/off"}, {"read", "-candidate", "L/off"}, {"report"}, {"run"}} {
		_, stderr, err := run(t, bin, append(cmd, "-out", filepath.Join(repo, "results"))...)
		if err == nil || !strings.Contains(stderr, "git worktree") {
			t.Errorf("%s inside a repository: %v\n%s", cmd[0], err, stderr)
		}
	}
	for _, c := range []struct{ args []string }{
		{[]string{"plan"}},
		{[]string{"plan", "-out", t.TempDir(), "-preset", "huge"}},
		{[]string{"build", "-out", t.TempDir()}},
		{[]string{"plan", "-out", t.TempDir(), "-preset", "tiny", "-retain", "nonsense"}},
		{[]string{"plan", "-out", t.TempDir(), "-preset", "tiny", "-days", "2"}},
	} {
		if _, _, err := run(t, bin, c.args...); err == nil {
			t.Errorf("%v was accepted", c.args)
		}
	}
	if _, _, err := run(t, bin); err == nil {
		t.Error("no command was accepted")
	}
}

// Windows of retained history through the real program: pins chosen once from the
// shortest window, a run of each candidate over each window with only the pinned
// prefixes written, and a judgement of G1 from them, with the guards on.
func TestWindowsRunEndToEnd(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // builds a binary and runs it; the full tier does

	bin := binary(t)
	out := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := run(t, bin, "windows", "-untimed", "-preset", "tiny", "-events-per-second", "0.05", "-batch", "40",
		"-windows", "1,2,3", "-candidates", "L/off,L/k64a4l1ns,M/crdb1", "-canonical-layout", "-out", out)
	if err != nil {
		t.Fatalf("windows: %v\n%.2000s\n%.2000s", err, stdout, stderr)
	}
	for _, f := range []string{"pins.json", "g1.txt", "g1.json", "R1/plan.json", "R2/plan.json", "R3/plan.json", "R3/L_off/results.json", "R1/M_crdb1/results.json", "R2/L_k64a4l1ns/manifest.json"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
	// The flag overrides the default: an untimed build that is told not to rest does not,
	// and its manifest says so.
	own := filepath.Join(t.TempDir(), "own")
	_, planErr, err := run(t, bin, "plan", "-untimed", "-preset", "tiny", "-events-per-second", "0.05", "-batch", "40", "-out", own)
	if err != nil {
		t.Errorf("plan: %v\n%s", err, planErr)
	} else if _, stderr, err := run(t, bin, "build", "-untimed", "-rest-after-retention=false", "-out", own, "-candidate", "L/off"); err != nil {
		t.Errorf("build: %v\n%s", err, stderr)
	} else if b, err := os.ReadFile(filepath.Join(own, "L_off/manifest.json")); err != nil || !strings.Contains(string(b), `"rest_after_retention": "false"`) {
		t.Errorf("the manifest of a build told not to rest does not say so: %v", err)
	} else if !strings.Contains(string(b), `"canonical_layout": "false"`) {
		t.Error("the manifest of a build not told to rewrite its tables does not say it did not")
	} else if _, stderr, err := run(t, bin, "build", "-untimed", "-canonical-layout", "-out", own, "-candidate", "M/crdb1"); err != nil {
		t.Errorf("build -canonical-layout: %v\n%s", err, stderr)
	} else if b, err := os.ReadFile(filepath.Join(own, "M_crdb1/manifest.json")); err != nil || !strings.Contains(string(b), `"canonical_layout": "true"`) {
		t.Errorf("the manifest of a build told to rewrite its tables does not say so: %v", err)
	} else if _, stderr, err := run(t, bin, "read", "-untimed", "-out", own, "-candidate", "M/crdb1"); err != nil {
		t.Errorf("read of a canonical build: %v\n%s", err, stderr)
	}
	// A plan flag is not quietly dropped when a plan exists: run refuses it (here -full
	// and -payload-pad, which a plan of another kind would have made).
	if _, statErr := os.Stat(filepath.Join(own, "plan.json")); statErr == nil { // without a plan, run would make one and build everything
		for _, flag := range [][]string{{"-full"}, {"-payload-pad", "8"}} {
			if _, stderr, err := run(t, bin, append([]string{"run", "-untimed", "-out", own}, flag...)...); err == nil || !strings.Contains(stderr, "cannot be applied") {
				t.Errorf("run with %v over an existing plan: %v\n%.300s", flag, err, stderr)
			}
		}
	}
	// An untimed build rests after each retention by default, and its manifest says so;
	// windows passes -canonical-layout to the builds of every window.
	for _, f := range []string{"R3/L_off/manifest.json", "R1/M_crdb1/manifest.json", "R2/L_k64a4l1ns/manifest.json"} {
		if b, err := os.ReadFile(filepath.Join(out, f)); err != nil || !strings.Contains(string(b), `"rest_after_retention": "true"`) || !strings.Contains(string(b), `"canonical_layout": "true"`) {
			t.Errorf("%s does not say it rested after each retention and was rewritten in the canonical layout: %v", f, err)
		}
	}
	for _, want := range []string{"G1 (rules", "L/off: ", "L/k64a4l1ns: ", "M/crdb1: ", "projected", "not decided, marked ?"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the judgement lacks %q:\n%.1500s", want, stdout)
		}
	}
	if strings.Contains(stdout, "NOT TO BE JUDGED TOGETHER") {
		t.Errorf("a valid validation run's windows are held to be incomparable:\n%.1500s", stdout)
	}
	// The verdicts for tools say what the text says.
	var report runner.G1Report
	if b, err := os.ReadFile(filepath.Join(out, "g1.json")); err != nil || json.Unmarshal(b, &report) != nil {
		t.Errorf("g1.json: %v", err)
	} else if len(report.Verdicts) != 3 || len(report.Cells) == 0 || report.RulesVersion != runner.RulesVersion || len(report.Problems) != 0 {
		t.Errorf("g1.json holds %d verdicts, %d cells, rules version %d, problems %v", len(report.Verdicts), len(report.Cells), report.RulesVersion, report.Problems)
	} else {
		for _, v := range report.Verdicts {
			if !strings.Contains(stdout, v.Candidate+": ") || v.Pass != strings.Contains(stdout, v.Candidate+": PASSES G1\n") {
				t.Errorf("g1.json says %+v, and the text does not", v)
			}
		}
	}
	// A judgement that fails leaves no report of an earlier one beside its text.
	if err := os.WriteFile(filepath.Join(out, "g1.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, bin, "g1", "-untimed", "-out", out, "-windows", "1,2,3,4"); err == nil {
		t.Error("windows of 1, 2, 3 and 4 days were judged from three")
	}
	if _, err := os.Stat(filepath.Join(out, "g1.json")); err == nil {
		t.Error("the report of an earlier judgement is still beside a judgement that failed")
	}
	// A judgement that works writes it again.
	if _, stderr, err := run(t, bin, "g1", "-untimed", "-out", out, "-windows", "1,2,3"); err != nil {
		t.Errorf("g1 again: %v\n%s", err, stderr)
	}
	if _, err := os.Stat(filepath.Join(out, "g1.json")); err != nil {
		t.Errorf("no report after a judgement that worked: %v", err)
	}
	// A family that stopped is resumed on the plans it made, if the flags are the same,
	// and refused if they are not: the pins are of the scenario they were chosen from.
	same := []string{
		"windows", "-untimed", "-preset", "tiny", "-events-per-second", "0.05", "-batch", "40",
		"-windows", "1,2,3", "-candidates", "L/off,L/k64a4l1ns,M/crdb1", "-canonical-layout", "-out", out,
	}
	if stdout, stderr, err := run(t, bin, same...); err != nil {
		t.Errorf("windows resumed with the same flags: %v\n%.1500s\n%.1500s", err, stdout, stderr)
	}
	// Without the build flag the family was built with, it is refused before anything runs.
	if _, stderr, err := run(t, bin, slices.DeleteFunc(slices.Clone(same), func(a string) bool { return a == "-canonical-layout" })...); err == nil || !strings.Contains(stderr, "a family is built all with or all without it") {
		t.Errorf("windows resumed without -canonical-layout: %v\n%.1500s", err, stderr)
	}
	// A build in the last window made otherwise is found before the first window runs,
	// not after hours of builds in the windows before it.
	last := filepath.Join(out, "R3", "L_off", "manifest.json")
	if b, err := os.ReadFile(last); err != nil {
		t.Errorf("no manifest to alter: %v", err)
	} else {
		alt := strings.Replace(string(b), `"canonical_layout": "true"`, `"canonical_layout": "false"`, 1)
		if alt == string(b) {
			t.Error("the manifest does not say canonical_layout true")
		}
		if err := os.WriteFile(last, []byte(alt), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, stderr, err := run(t, bin, same...); err == nil || !strings.Contains(stderr, "the window of 3 days") || strings.Contains(stderr, "windows: 1 days of retained history") {
			t.Errorf("windows resumed over a last window built plain: %v\n%.1500s", err, stderr)
		}
		if err := os.WriteFile(last, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	other := append(slices.Clone(same[:len(same)-2]), "-batch", "50", "-out", out)
	if _, stderr, err := run(t, bin, other...); err == nil || !strings.Contains(stderr, "another scenario") {
		t.Errorf("windows resumed with another batch size: %v\n%.1500s", err, stderr)
	}
	// The same family on full stores read at the pins: the stream of each window is the
	// whole one, and the block counters are decided.
	fullOut := filepath.Join(t.TempDir(), "full")
	stdout, stderr, err = run(t, bin, append(slices.Clone(same[:len(same)-2]), "-full", "-out", fullOut)...)
	if err != nil {
		t.Fatalf("windows -full: %v\n%.2000s\n%.2000s", err, stdout, stderr)
	}
	if strings.Contains(stdout, "not decided, marked ?") || !strings.Contains(stdout, "G1 (rules") {
		t.Errorf("full stores leave block counters undecided, or were not judged:\n%.1500s", stdout)
	}
	for _, d := range []string{"R1", "R3"} {
		pp, err1 := runner.LoadPlan(filepath.Join(out, d, "plan.json"))
		pf, err2 := runner.LoadPlan(filepath.Join(fullOut, d, "plan.json"))
		if err1 != nil || err2 != nil {
			t.Fatal(err1, err2)
		}
		if !pf.Spec.FullStore || pf.Stream.Records <= pp.Stream.Records || pf.QueriesDigest == "" || len(pf.Queries) != len(pp.Queries) {
			t.Errorf("%s: full %v, %d records against the projection's %d, %d queries against %d", d, pf.Spec.FullStore, pf.Stream.Records, pp.Stream.Records, len(pf.Queries), len(pp.Queries))
		}
	}
	// Pins are chosen once, and a window cannot be run without them.
	if _, stderr, err := run(t, bin, "pins", "-untimed", "-preset", "tiny", "-window", "1", "-out", out); err == nil || !strings.Contains(stderr, "exists") {
		t.Errorf("pins chosen twice: %v\n%s", err, stderr)
	}
}
