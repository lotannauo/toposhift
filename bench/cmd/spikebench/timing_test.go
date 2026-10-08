package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

const timingRevision = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"

func writeTimingJSON(t *testing.T, path string, v any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// timingFixture writes the artifacts of one window of the bench workflow (three
// repetitions of three candidates on both architectures, with invented timings), the
// plan beside them, and the counters runs of the same plan. The builds of aarch64 are
// under one directory of in and those of x86_64 under another.
func timingFixture(t *testing.T) (arm, x86, counters string) {
	t.Helper()
	arm, x86, counters = t.TempDir(), t.TempDir(), t.TempDir()
	spec := runner.DefaultSpec(workload.Tiny())
	specDigest, err := spec.Digest()
	if err != nil {
		t.Fatal(err)
	}
	queries, err := runner.QueriesDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := runner.DefaultRules().Digest()
	if err != nil {
		t.Fatal(err)
	}
	plan := &runner.Plan{Spec: spec, SpecDigest: specDigest, RulesDigest: rules, Stream: runner.StreamInfo{Digest: "stream", Records: 1000}, QueriesDigest: queries, CacheBytes: 1 << 20}
	if err := os.MkdirAll(filepath.Join(arm, "plans", "R7"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := plan.Save(filepath.Join(arm, "plans", "R7", "plan.json")); err != nil {
		t.Fatal(err)
	}
	planDigest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	commit := map[string]time.Duration{"L/off": 10 * time.Millisecond, "L/k64a2l1ns": 15 * time.Millisecond, "M/crdb1": 12 * time.Millisecond}
	for _, cand := range []string{"L/off", "L/k64a2l1ns", "M/crdb1"} {
		for _, a := range []struct{ arch, goarch, os, exe, dir string }{
			{"aarch64", "arm64", "ubuntu-24.04-arm", strings.Repeat("1", 64), arm},
			{"x86_64", "amd64", "ubuntu-24.04", strings.Repeat("2", 64), x86},
		} {
			for rep := 1; rep <= 3; rep++ {
				var h runner.Histogram
				for range 100 {
					h.Add(commit[cand])
				}
				m := &runner.Manifest{
					Candidate: cand, PlanDigest: planDigest, Stream: plan.Stream,
					Build:    runner.BuildInfo{GoVersion: "go1.27.1", GOOS: "linux", GOARCH: a.goarch, Revision: timingRevision, Executable: a.exe},
					Describe: map[string]string{"pebble_options": "[Options]\n", "sync": "false", "rest_after_retention": "false", "canonical_layout": "false", "metrics_every": "0s", "settle_tombstones": "true", "settle_deadline": "2m0s", "post_retention_batches": "100"},
					Counters: map[string]int64{}, StatsBuilt: map[string]int64{"bytes_in": 1000}, StatsCompacted: map[string]int64{"live_table_bytes": 40000},
					Timing: runner.Timing{
						Writes: h, Retains: []int64{int64(20 * time.Second)}, AfterRetention: []int64{int64(5 * time.Millisecond)},
						PostRetention: [][]int64{{int64(5 * time.Millisecond), int64(commit[cand])}}, RetainPhases: []runner.RetainPhase{{Work: int64(20 * time.Second), Settle: int64(time.Second)}},
					},
				}
				dir := filepath.Join(a.dir, "bench-"+strings.ReplaceAll(cand, "/", "_")+"-"+a.os+"-rep"+string(rune('0'+rep)))
				writeTimingJSON(t, filepath.Join(dir, runner.ManifestFile), m)
				writeTimingJSON(t, filepath.Join(dir, runner.JobFile), runner.Job{
					SHA: timingRevision, OS: a.os, Arch: a.arch, Family: "rmae", Window: 7, Candidate: cand, Rep: rep, PinsWindow: 2,
					Sync: "false", CPUModel: "Test CPU " + a.arch, NProc: 4, MemTotalKB: 1 << 20, RunID: "100", RunAttempt: "1", BinarySHA256: a.exe,
				})
			}
		}
		// A counters run: a manifest and the results read from it.
		cm := &runner.Manifest{Candidate: cand, PlanDigest: planDigest}
		dir := filepath.Join(counters, strings.ReplaceAll(cand, "/", "_"))
		writeTimingJSON(t, filepath.Join(dir, runner.ManifestFile), cm)
		b, err := os.ReadFile(filepath.Join(dir, runner.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		writeTimingJSON(t, filepath.Join(dir, runner.ResultsFile), &runner.Results{Candidate: cand, PlanDigest: planDigest, ManifestDigest: hex.EncodeToString(sum[:])})
	}
	return arm, x86, counters
}

// fakeGit records its calls and answers as it was made to.
type fakeGit struct {
	calls  [][3]string
	answer runner.RevisionCheck
}

func (f *fakeGit) check(_ context.Context, checkout, rev, ref string) runner.RevisionCheck {
	f.calls = append(f.calls, [3]string{checkout, rev, ref})
	return f.answer
}

func timing(t *testing.T, git *fakeGit, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err = runTiming(context.Background(), args, &out, &errOut, git.check)
	return out.String(), errOut.String(), err
}

func TestTimingJudgesTheArtifactsOfARun(t *testing.T) {
	t.Parallel()

	arm, x86, counters := timingFixture(t)
	json1 := filepath.Join(t.TempDir(), "timing.json")
	git := &fakeGit{answer: runner.RevisionCheck{Ancestor: true}}
	out, _, err := timing(t, git, "-in", arm, "-in", x86, "-counters", counters, "-git", "/some/checkout", "-json", json1)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "every precondition holds: these are CI timings") {
		t.Errorf("the text does not say the timings are judged:\n%s", out)
	}
	b, err := os.ReadFile(json1)
	if err != nil {
		t.Fatal(err)
	}
	var rep runner.TimingReport
	if err := json.Unmarshal(b, &rep); err != nil || !rep.Judged || rep.Revision != timingRevision || rep.RevisionRef != "origin/main" {
		t.Errorf("the JSON decodes to judged %v, revision %q, ref %q, error %v", rep.Judged, rep.Revision, rep.RevisionRef, err)
	}
	if want := [][3]string{{"/some/checkout", timingRevision, "origin/main"}}; len(git.calls) != 1 || git.calls[0] != want[0] {
		t.Errorf("git was asked %v, want %v", git.calls, want)
	}
	if _, err := os.Stat(json1 + ".tmp"); err == nil {
		t.Error("a temporary file was left beside the JSON")
	}

	// The roots given as arguments after the flags are roots too, and the order of the
	// roots does not matter.
	out2, _, err := timing(t, git, "-counters", counters, "-git", "/some/checkout", "-ref", "origin/main", x86, arm)
	if err != nil || out2 != out {
		t.Errorf("positional roots: error %v, same text %v", err, out2 == out)
	}
	// Without the counters runs G0 is not established and nothing that needs it is judged.
	out3, _, err := timing(t, git, "-in", arm, "-in", x86, "-git", "/some/checkout")
	if err != nil || strings.Contains(out3, "every precondition holds") || !strings.Contains(out3, "is not established: no counters run") {
		t.Errorf("without counters: error %v\n%s", err, out3)
	}
}

func TestTimingNeverAsksGitWithoutAGitCheckout(t *testing.T) {
	t.Parallel()

	arm, x86, counters := timingFixture(t)
	git := &fakeGit{answer: runner.RevisionCheck{Ancestor: true}}
	out, _, err := timing(t, git, "-in", arm, "-in", x86, "-counters", counters)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(git.calls) != 0 {
		t.Errorf("git was asked %v without -git", git.calls)
	}
	// No check was asked for, so no ref is recorded.
	json2 := filepath.Join(t.TempDir(), "timing.json")
	if _, _, err := timing(t, git, "-in", arm, "-in", x86, "-counters", counters, "-ref", "origin/other", "-json", json2); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(json2)
	if err != nil {
		t.Fatal(err)
	}
	var rep runner.TimingReport
	if err := json.Unmarshal(b, &rep); err != nil || rep.RevisionRef != "" || rep.RevisionChecked {
		t.Errorf("without -git the JSON has ref %q, checked %v, error %v", rep.RevisionRef, rep.RevisionChecked, err)
	}
	if !strings.Contains(out, "WARNING: the revision was not checked against origin/main (give -git)") || strings.Contains(out, "every precondition holds") {
		t.Errorf("the text of an unverified run:\n%s", out)
	}
}

func TestTimingRefusesARevisionThatIsNotOnMain(t *testing.T) {
	t.Parallel()

	arm, x86, counters := timingFixture(t)
	git := &fakeGit{answer: runner.RevisionCheck{Ancestor: false}}
	json1 := filepath.Join(t.TempDir(), "timing.json")
	out, _, err := timing(t, git, "-in", arm, "-in", x86, "-counters", counters, "-git", "/some/checkout", "-json", json1)
	if err == nil || !strings.Contains(err.Error(), "these timings cannot be judged together (1 reasons, listed above)") {
		t.Fatalf("error %v", err)
	}
	if !strings.Contains(out, "is not an ancestor of origin/main") || !strings.Contains(out, "THESE TIMINGS ARE NOT TO BE JUDGED TOGETHER") {
		t.Errorf("the text does not give the reason:\n%s", out)
	}
	if _, err := os.Stat(json1); err != nil {
		t.Errorf("the JSON was not written beside the refusal: %v", err)
	}
	var usage usageError
	if errors.As(err, &usage) {
		t.Error("a refusal of the inputs is a usage error")
	}
}

func TestTimingUsageErrors(t *testing.T) {
	t.Parallel()

	arm, _, _ := timingFixture(t)
	git := &fakeGit{}
	for name, args := range map[string][]string{
		"no root":         {},
		"a flag only":     {"-ref", "origin/main"},
		"a ref for git":   {"-in", arm, "-ref", "-x"},
		"an empty ref":    {"-in", arm, "-ref", ""},
		"an unknown flag": {"-in", arm, "-nope"},
	} {
		_, _, err := timing(t, git, args...)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Errorf("%s: error %v, want a usage error", name, err)
		}
	}
	if len(git.calls) != 0 {
		t.Errorf("git was asked %v", git.calls)
	}
	// -h prints the flags and succeeds.
	_, errOut, err := timing(t, git, "-h")
	if err != nil || !strings.Contains(errOut, "-counters") || !strings.Contains(errOut, "-join-from") {
		t.Errorf("-h: error %v, flags printed:\n%s", err, errOut)
	}
}

func TestTimingWritesNothingInsideARepository(t *testing.T) {
	t.Parallel()

	// The worktree this test runs in is a git worktree: a JSON file there is refused
	// before anything is written.
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("not inside a git worktree")
		}
		dir = parent
	}
	arm, x86, counters := timingFixture(t)
	target := filepath.Join(dir, "timing-must-not-exist.json")
	_, _, err = timing(t, &fakeGit{}, "-in", arm, "-in", x86, "-counters", counters, "-json", target)
	if !errors.Is(err, runner.ErrInsideWorktree) {
		t.Errorf("error %v, want %v", err, runner.ErrInsideWorktree)
	}
	if _, err := os.Stat(target); err == nil {
		os.Remove(target)
		t.Error("a file was written inside the worktree")
	}
}

func TestTimingRefusesWhatCannotBeLoaded(t *testing.T) {
	t.Parallel()

	arm, x86, _ := timingFixture(t)
	// A manifest with no job file is not an artifact of the bench workflow.
	if err := os.Remove(filepath.Join(arm, "bench-L_off-ubuntu-24.04-arm-rep1", runner.JobFile)); err != nil {
		t.Fatal(err)
	}
	out, _, err := timing(t, &fakeGit{}, "-in", arm, "-in", x86)
	if err == nil || !strings.Contains(err.Error(), "job.json") || out != "" {
		t.Errorf("error %v with output %q, want an error and nothing printed", err, out)
	}
}

func TestAncestryFromExit(t *testing.T) {
	t.Parallel()

	exit := func(code string) error {
		return exec.CommandContext(context.Background(), "sh", "-c", "exit "+code).Run()
	}
	const rev = timingRevision
	for _, c := range []struct {
		name     string
		err      error
		ancestor bool
		failure  string // words the failure must hold; "" for an answer
	}{
		{"exit 0", nil, true, ""},
		{"exit 1", exit("1"), false, ""},
		{"exit 128", exit("128"), false, "revision abcdefabcdef is not in /checkout: fetch it"},
		{"another exit", exit("2"), false, "status 2"},
		{"git could not be run", errors.New("executable file not found"), false, "git could not be run"},
	} {
		got := ancestryFromExit(rev, "/checkout", c.err)
		if got.Ancestor != c.ancestor || (c.failure == "") != (got.Err == "") || !strings.Contains(got.Err, c.failure) {
			t.Errorf("%s: %+v, want ancestor %v and a failure naming %q", c.name, got, c.ancestor, c.failure)
		}
	}
}

func TestRefCheckFromExit(t *testing.T) {
	t.Parallel()

	exit := func(code string) error {
		return exec.CommandContext(context.Background(), "sh", "-c", "exit "+code).Run()
	}
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"a commit", nil, ""},
		{"a ref that is not one", exit("1"), "origin/main is not a commit in /checkout"},
		{"a git that exits another way", exit("128"), "origin/main is not a commit in /checkout"},
		{"a git that could not be run", errors.New("executable file not found"), "git could not be run: executable file not found"},
	} {
		if got := refCheckFromExit("origin/main", "/checkout", c.err); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	// A git that was stopped is not a ref that is not a commit.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := exec.CommandContext(ctx, "sh", "-c", "exit 0").Run()
	if got := refCheckFromExit("origin/main", "/checkout", err); !strings.HasPrefix(got, "git could not be run") {
		t.Errorf("a cancelled context: %q", got)
	}
}

func TestWriteFileAtomicLeavesNothingBehindOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "report.json")
	if err := writeFileAtomic(target, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(target, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(target); err != nil || string(b) != "two" {
		t.Errorf("file %q, error %v", b, err)
	}
	// A directory cannot be replaced by a file: the rename fails and the temporary file goes.
	blocked := filepath.Join(dir, "blocked")
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "inside"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(blocked, []byte("three")); err == nil {
		t.Error("a directory was replaced by a file")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("a temporary file was left behind: %s", e.Name())
		}
	}
}

// A join set is loaded with its plans and judged beside the builds: one of another family
// is refused.
func TestTimingChecksTheJoinSetAgainstTheBuilds(t *testing.T) {
	t.Parallel()

	arm, x86, counters := timingFixture(t)
	armJoin, x86Join, _ := timingFixture(t)
	git := &fakeGit{answer: runner.RevisionCheck{Ancestor: true}}
	out, _, err := timing(t, git, "-in", arm, "-in", x86, "-counters", counters, "-git", "/some/checkout", "-join-from", armJoin, "-join-from", x86Join)
	if err != nil || !strings.Contains(out, "every precondition holds") {
		t.Fatalf("a join set of the same family: error %v\n%s", err, out)
	}
	for _, dir := range []string{armJoin, x86Join} {
		matches, err := filepath.Glob(filepath.Join(dir, "*", runner.JobFile))
		if err != nil || len(matches) == 0 {
			t.Fatal(matches, err)
		}
		for _, job := range matches {
			b, err := os.ReadFile(job)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(job, bytes.ReplaceAll(b, []byte(`"family":"rmae"`), []byte(`"family":"ph5m-rmae"`)), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	out, _, err = timing(t, git, "-in", arm, "-in", x86, "-counters", counters, "-git", "/some/checkout", "-join-from", armJoin, "-join-from", x86Join)
	if err == nil || !strings.Contains(out, "the join set: its builds are of family ph5m-rmae and the builds judged are of family rmae") {
		t.Errorf("a join set of another family: error %v\n%s", err, out)
	}
}

func TestTimingRefusesAJsonFileInAnAbsentDirectoryOfARepository(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("not inside a git worktree")
		}
		dir = parent
	}
	arm, x86, counters := timingFixture(t)
	target := filepath.Join(dir, "no-such-directory", "timing.json")
	_, _, err = timing(t, &fakeGit{}, "-in", arm, "-in", x86, "-counters", counters, "-json", target)
	if !errors.Is(err, runner.ErrInsideWorktree) {
		t.Errorf("error %v, want %v", err, runner.ErrInsideWorktree)
	}
}
