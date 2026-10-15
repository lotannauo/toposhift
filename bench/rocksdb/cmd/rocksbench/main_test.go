package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockskv"
	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/rocksdb/rulejudge"
	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// tinyPlan writes a tiny plan into a new output directory, as spikebench plan does.
func tinyPlan(t *testing.T) (string, *runner.Plan) {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	spec.MinNonEmpty = 0
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if err := p.Save(filepath.Join(out, planFile)); err != nil {
		t.Fatal(err)
	}
	return out, p
}

// exitCode is the status the program ends with for the error of a command: 1 for any
// error, and the code an exitCodeError carries.
func exitCode(err error) int {
	var ec exitCodeError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ec):
		return ec.code
	}
	return 1
}

// A tiny plan goes through build and read, which write spikebench's manifest and
// results for the RL candidate; a read refuses a plan the build was not made from,
// as one whose expected answer was changed; and a binary that may not give a result
// is refused unless the run says it is untimed.
func TestBuildAndRead(t *testing.T) {
	t.Parallel()
	out, p := tinyPlan(t)
	const name = "RL/k64a2l5m"
	args := []string{"-out", out, "-candidate", name, "-untimed"}
	ctx := context.Background()
	if err := run(ctx, "build", []string{"-out", out, "-candidate", name}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "not one a result may come from") {
		t.Fatalf("a build by a test binary without -untimed: %v", err)
	}
	if err := run(ctx, "build", args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, "read", args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	dir := runner.CandidateDir(out, name)
	c, err := runner.LoadCandidate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Manifest.Candidate != name || c.Results.Candidate != name || len(c.Results.Queries) != len(p.Queries) || len(c.Results.Mismatches) != 0 {
		t.Fatalf("manifest of %q, results of %q with %d queries (plan %d), %d mismatches",
			c.Manifest.Candidate, c.Results.Candidate, len(c.Results.Queries), len(p.Queries), len(c.Results.Mismatches))
	}
	if !c.Manifest.Build.CGO || !c.Manifest.Untimed || c.Manifest.Describe["engine"] != "rocksdb" || c.Manifest.Describe["rest_after_retention"] != "true" {
		t.Errorf("manifest: cgo %v, untimed %v, engine %q, rest %q", c.Manifest.Build.CGO, c.Manifest.Untimed, c.Manifest.Describe["engine"], c.Manifest.Describe["rest_after_retention"])
	}

	// The same plan with one expected answer changed is another plan.
	q := p.Queries[0]
	q.Expect = strings.Repeat("0", len(q.Expect))
	p.Queries[0] = q
	if err := p.Save(filepath.Join(out, planFile)); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, "read", args, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "another plan") {
		t.Fatalf("a read against a changed plan: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	t.Parallel()
	out, _ := tinyPlan(t)
	ctx := context.Background()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"no candidate":         {"-out", out},
		"a candidate of L":     {"-out", out, "-candidate", "L/off", "-untimed"},
		"an unknown candidate": {"-out", out, "-candidate", "RL/on", "-untimed"},
		"inside the worktree":  {"-out", wd, "-candidate", "RL/off", "-untimed"},
	} {
		if err := run(ctx, "build", args, io.Discard, io.Discard); err == nil {
			t.Errorf("%s: built", name)
		}
	}
	if err := run(ctx, "plan", nil, io.Discard, io.Discard); !errors.Is(err, errUsage) {
		t.Errorf("an unknown command: %v", err)
	}
}

// The names and keys the judge reads are the ones the engine and its candidates
// write.
func TestTheJudgeReadsWhatTheEngineWrites(t *testing.T) {
	t.Parallel()
	if rockskv.StampKey != rulejudge.StampKey || rockskv.StampSHA256Key != rulejudge.StampSHA256Key {
		t.Errorf("the stamp's keys: rockskv %q and %q, rulejudge %q and %q", rockskv.StampKey, rockskv.StampSHA256Key, rulejudge.StampKey, rulejudge.StampSHA256Key)
	}
	if rockslog.NamePrefix != rulejudge.RocksPrefix || rockslog.NamePrefix4 != rulejudge.Rocks4Prefix {
		t.Errorf("the names: rockslog %q and %q, rulejudge %q and %q", rockslog.NamePrefix, rockslog.NamePrefix4, rulejudge.RocksPrefix, rulejudge.Rocks4Prefix)
	}
}

// A build that may give a result refuses a library without a build stamp, and
// builds nothing; -untimed allows one; a stamped library is built from. Not
// parallel: it replaces the binary's record, so that the guards let a test binary
// through, and the library's stamp, and puts both back after.
func TestBuildRefusesALibraryWithoutAStamp(t *testing.T) {
	oldInfo, oldStamp := readBuildInfo, libraryStamp
	t.Cleanup(func() { readBuildInfo, libraryStamp = oldInfo, oldStamp })
	readBuildInfo = func() runner.BuildInfo {
		return runner.BuildInfo{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CGO: true, Revision: strings.Repeat("ab", 20), Executable: strings.Repeat("cd", 32)}
	}
	ctx := context.Background()
	for _, s := range []rockskv.Stamp{{Dir: "/opt/homebrew/lib"}, {}} {
		libraryStamp = func() (rockskv.Stamp, error) { return s, nil }
		out, _ := tinyPlan(t)
		err := run(ctx, "build", []string{"-out", out, "-candidate", "RL/off"}, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "may not give a result") {
			t.Errorf("a library without a stamp (%+v): %v", s, err)
		}
		if _, err := os.Stat(runner.CandidateDir(out, "RL/off")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the refused build made its directory: %v", err)
		}
		if err := run(ctx, "build", []string{"-out", out, "-candidate", "RL/off", "-untimed"}, io.Discard, io.Discard); err != nil {
			t.Errorf("an untimed build from a library without a stamp: %v", err)
		}
	}
	libraryStamp = func() (rockskv.Stamp, error) {
		return rockskv.Stamp{Dir: "/p/lib", Found: true, Text: "portable=1\n", SHA256: "s"}, nil
	}
	out, _ := tinyPlan(t)
	if err := run(ctx, "build", []string{"-out", out, "-candidate", "RL/off"}, io.Discard, io.Discard); err != nil {
		t.Errorf("a build from a stamped library: %v", err)
	}
	libraryStamp = func() (rockskv.Stamp, error) { return rockskv.Stamp{}, errors.New("unreadable") }
	if err := run(ctx, "build", []string{"-out", t.TempDir(), "-candidate", "RL/off"}, io.Discard, io.Discard); err == nil {
		t.Error("a stamp that cannot be read is not refused")
	}
}

// A build given the Pebble builds' memory limit runs under that limit less the plan's
// cache and four memtables, and its manifest records it; given none, it runs under
// none. Not parallel: the limit is the process's, and is put back after.
func TestPebbleMemoryLimit(t *testing.T) {
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	for _, c := range []struct{ flag, want string }{{"2147483648", ""}, {"none", "none"}} {
		out, p := tinyPlan(t)
		want := c.want
		if want == "" {
			want = strconv.FormatInt(2<<30-(p.CacheBytes+pebblekv.BenchTuning().MemTableReserve()), 10)
		}
		if err := run(context.Background(), "build", []string{"-out", out, "-candidate", "RL/off", "-untimed", "-pebble-memory-limit", c.flag}, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		m, err := runner.LoadManifest(runner.CandidateDir(out, "RL/off"))
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Describe[runner.GoMemoryLimitKey]; got != want {
			t.Errorf("-pebble-memory-limit %s: the manifest records %q, want %q", c.flag, got, want)
		}
	}
	if err := run(context.Background(), "build", []string{"-out", t.TempDir(), "-candidate", "RL/off", "-untimed", "-pebble-memory-limit", "lots"}, io.Discard, io.Discard); err == nil {
		t.Error("a limit that is not a number is accepted")
	}
}

// The judge on a tiny plan built by both engines, one repetition each of L, RL and
// RL4 at k64a2l5m, as the bench workflow's dry run builds them: it refuses the set
// for its single repetitions, with status 3, and finds every RocksDB build a result
// in every other respect (its answers and comparable counters are L's, the regime,
// the perf context, the memory limit), except for the build stamp when the library
// this test links has none. The guards are declared as counters_test.go declares
// them: nothing here is timed.
func TestJudgeATinyPlan(t *testing.T) {
	t.Parallel()
	out, p := tinyPlan(t)
	arch := map[string]string{"arm64": "aarch64", "amd64": "x86_64"}[runtime.GOARCH]
	if arch == "" {
		t.Skipf("the bench workflow runs on aarch64 and x86_64, not %s", runtime.GOARCH)
	}
	sha, exe := strings.Repeat("ab", 20), strings.Repeat("cd", 32)
	ctx := context.Background()
	for _, name := range []string{"L/k64a2l5m", "RL/k64a2l5m", "RL4/k64a2l5m"} {
		var v candidates.Variant
		var err error
		rocks := strings.HasPrefix(name, "RL")
		if rocks {
			v, err = rockslog.Variant(name, false)
		} else {
			v, err = candidates.Lookup(name)
		}
		if err != nil {
			t.Fatal(err)
		}
		g := runner.Guards{Info: runner.BuildInfo{GoVersion: "test", GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CGO: rocks, Revision: sha, Executable: exe}}
		dir := runner.CandidateDir(out, name)
		if _, err := runner.BuildWith(ctx, p, v, dir, g, runner.BuildOptions{}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := runner.Read(ctx, p, v, dir, g, nil); err != nil {
			t.Fatal(err)
		}
		engine := "pebble"
		if rocks {
			engine = "rocksdb"
		}
		job, err := json.Marshal(rulejudge.Job{Job: runner.Job{
			SHA: sha, Arch: arch, Window: rulejudge.VerdictWindow, Candidate: name, Rep: 1, CPUModel: "test CPU", BinarySHA256: exe,
		}, Engine: engine, MaxRSSKB: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, runner.JobFile), job, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var report bytes.Buffer
	err := run(ctx, "judge", []string{out}, &report, io.Discard)
	t.Logf("the judge's report on a tiny plan built by both engines:\n%s", report.String())
	if exitCode(err) != judgeErrorStatus || !strings.Contains(report.String(), "has 1 repetitions, fewer than 3") {
		t.Errorf("status %d (%v), want %d for single repetitions", exitCode(err), err, judgeErrorStatus)
	}
	set, err := rulejudge.Load([]string{out})
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := rockskv.LibraryStamp()
	if err != nil {
		t.Fatal(err)
	}
	for _, why := range rulejudge.Judge(set).NotResults {
		if !stamp.Found && strings.Contains(why, "build stamp") {
			continue
		}
		t.Errorf("not a result: %s", why)
	}
	if exitCode(run(ctx, "judge", nil, io.Discard, io.Discard)) != judgeErrorStatus {
		t.Error("judge with no directory does not end with status 3")
	}
	if exitCode(run(ctx, "judge", []string{filepath.Join(out, "absent")}, io.Discard, io.Discard)) != judgeErrorStatus {
		t.Error("judge of a directory that is not there does not end with status 3")
	}
}
