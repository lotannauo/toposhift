// Package scripts_test runs the bench workflow's shell scripts with the inputs they
// must take and those they must refuse.
package scripts_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The setup of a dispatch of the bench workflow: the script takes a well-formed dispatch
// from main or from a gate-freeze tag on main, and refuses every malformed input, each
// with a message that names the input and never repeats its text.

// repo is a git repository with two commits: onMain, which origin/main points at, and
// offMain, a commit after it that is on no branch of origin.
type repo struct {
	dir, onMain, offMain string
}

func newRepo(t *testing.T) repo {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = dir
		// No configuration of the machine's (a signing key, a hook) reaches the test.
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("commit", "-q", "--allow-empty", "--no-verify", "-m", "on main")
	on := git("rev-parse", "HEAD")
	git("update-ref", "refs/remotes/origin/main", on)
	git("commit", "-q", "--allow-empty", "--no-verify", "-m", "off main")
	return repo{dir: dir, onMain: on, offMain: git("rev-parse", "HEAD")}
}

// dispatch is the environment of a well-formed background dispatch of the 7-day rung
// 24,576 from a gate-freeze tag of r's commit on main.
func dispatch(r repo) map[string]string {
	return map[string]string{
		"REF": "refs/tags/gate-freeze-20261012", "SHA": r.onMain,
		"FAMILY": "rmae", "WINDOWS": "7", "CANDIDATES": "Lroot/off,Lroot/k64a2l1ns", "REPS": "3", "ARCHES": "both",
		"PINS_WINDOW": "2", "SYNC": "true", "METRICS_EVERY": "10s", "GO_MEM_LIMIT": "11GiB", "MAX_PARALLEL": "12",
		"RETENTION_MODE": "background", "RETAIN_FLUSH_DELS": "24576", "EXPECTED_SHA": r.onMain, "READ": "false", "READ_TIMED": "false",
		"SHAPE": "", "SERVE_OPTIONS": "false",
	}
}

type result struct {
	code            int
	stderr          string
	output, summary string
}

// run runs the script in r's directory with env and nothing else of the test's
// environment but the path.
func run(t *testing.T, r repo, env map[string]string) result {
	t.Helper()
	script, err := filepath.Abs("gate-dispatch-check.sh")
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	output, summary := filepath.Join(tmp, "output"), filepath.Join(tmp, "summary")
	cmd := exec.CommandContext(t.Context(), "bash", script)
	cmd.Dir = r.dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
		"GITHUB_OUTPUT=" + output, "GITHUB_STEP_SUMMARY=" + summary, "RUNNER_TEMP=" + tmp,
	}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	res := result{stderr: stderr.String()}
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		res.code = exit.ExitCode()
	case err != nil:
		t.Fatal(err)
	}
	if b, err := os.ReadFile(output); err == nil {
		res.output = string(b)
	}
	if b, err := os.ReadFile(summary); err == nil {
		res.summary = string(b)
	}
	return res
}

func TestADispatchIsTakenFromMainOrAFreezeTagOnMain(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	got := run(t, r, dispatch(r))
	if got.code != 0 {
		t.Fatalf("a well-formed background dispatch: exit %d\n%s", got.code, got.stderr)
	}
	for _, want := range []string{
		"retention_mode=background\n", "pace=4500\n", "retain_flush_dels=24576\n", "expected_sha=" + r.onMain + "\n",
		"sync=true\n", "metrics_every=10s\n", "max_parallel=12\n", "rocksdb_oses=[]\n",
		"go_mem_limit=11GiB\n", "pebble_memory_limit=11811160064\n", "read=false\n", "read_timed=false\n", "plan_flags=\n",
		`matrix={"include":[{"os":"ubuntu-24.04","window":7,"candidate":"Lroot/off","slug":"Lroot_off","engine":"pebble","rep":1},`,
	} {
		if !strings.Contains(got.output, want) {
			t.Errorf("the outputs lack %q:\n%s", want, got.output)
		}
	}
	if n := strings.Count(got.output, `"rep":`); n != 12 {
		t.Errorf("%d jobs in the matrix, want 2 architectures x 2 candidates x 3 repetitions", n)
	}
	for _, want := range []string{"### Planned jobs: 12", "retention mode: `background`; pace: `4500`", "retention flush threshold: `24576`", "commit: `" + r.onMain + "`"} {
		if !strings.Contains(got.summary, want) {
			t.Errorf("the summary lacks %q:\n%s", want, got.summary)
		}
	}

	// From main, a synchronous dispatch as before: no commit named, the run's own is
	// expected, the threshold is the default and the pace none.
	sync := dispatch(r)
	sync["REF"], sync["SHA"], sync["RETENTION_MODE"], sync["RETAIN_FLUSH_DELS"], sync["EXPECTED_SHA"] = "refs/heads/main", r.offMain, "sync", "default", ""
	sync["WINDOWS"], sync["CANDIDATES"], sync["SYNC"], sync["METRICS_EVERY"] = "2,7,30", "L/off,L/k64a2l1ns", "false", ""
	got = run(t, r, sync)
	if got.code != 0 {
		t.Fatalf("a synchronous dispatch from main: exit %d\n%s", got.code, got.stderr)
	}
	for _, want := range []string{"retention_mode=sync\n", "pace=0\n", "retain_flush_dels=default\n", "expected_sha=" + r.offMain + "\n"} {
		if !strings.Contains(got.output, want) {
			t.Errorf("a synchronous dispatch: the outputs lack %q:\n%s", want, got.output)
		}
	}
}

// rocksDispatch is the environment of the RocksDB rule's 30-day dispatch from main:
// layout L on Pebble and on RocksDB, both policies, the file-size variant, five
// repetitions on both architectures, repetition 1 read.
func rocksDispatch(r repo) map[string]string {
	env := dispatch(r)
	env["REF"], env["SHA"], env["EXPECTED_SHA"] = "refs/heads/main", r.offMain, r.offMain
	env["WINDOWS"], env["CANDIDATES"], env["REPS"] = "30", "L/off,L/k64a2l5m,RL/off,RL/k64a2l5m,RL4/k64a2l5m", "5"
	env["SYNC"], env["METRICS_EVERY"], env["RETENTION_MODE"], env["RETAIN_FLUSH_DELS"], env["READ"] = "false", "", "sync", "default", "true"
	return env
}

// A dispatch of layout L on both engines: every job is marked with the engine that
// builds it, RL/ and RL4/ with rocksdb and the rest with pebble, and the Go memory
// limit is also given in bytes, which rocksbench takes as the Pebble builds' limit.
func TestADispatchOfBothEnginesMarksEachJob(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	got := run(t, r, rocksDispatch(r))
	if got.code != 0 {
		t.Fatalf("the RocksDB rule's dispatch: exit %d\n%s", got.code, got.stderr)
	}
	for _, want := range []string{
		"read=true\n", "read_timed=false\n", "plan_flags=\n", "go_mem_limit=11GiB\n", "pebble_memory_limit=11811160064\n", "retention_mode=sync\n",
		`rocksdb_oses=["ubuntu-24.04","ubuntu-24.04-arm"]` + "\n",
		`{"os":"ubuntu-24.04","window":30,"candidate":"L/off","slug":"L_off","engine":"pebble","rep":1}`,
		`{"os":"ubuntu-24.04","window":30,"candidate":"RL/k64a2l5m","slug":"RL_k64a2l5m","engine":"rocksdb","rep":5}`,
		`{"os":"ubuntu-24.04-arm","window":30,"candidate":"RL4/k64a2l5m","slug":"RL4_k64a2l5m","engine":"rocksdb","rep":1}`,
	} {
		if !strings.Contains(got.output, want) {
			t.Errorf("the outputs lack %q:\n%s", want, got.output)
		}
	}
	if n, rocks := strings.Count(got.output, `"rep":`), strings.Count(got.output, `"engine":"rocksdb"`); n != 50 || rocks != 30 {
		t.Errorf("%d jobs, %d of them RocksDB; want 2 architectures x 5 candidates x 5 repetitions, 3 of the candidates RocksDB", n, rocks)
	}
	for _, want := range []string{"### Planned jobs: 50", "| ubuntu-24.04-arm | 30 | `RL4/k64a2l5m` | rocksdb | 5 |", "| ubuntu-24.04 | 30 | `L/off` | pebble | 1 |", "read repetition 1 after its build: `true`", "`-pebble-memory-limit 11811160064`"} {
		if !strings.Contains(got.summary, want) {
			t.Errorf("the summary lacks %q:\n%s", want, got.summary)
		}
	}

	// The limit in MiB, and none; and RocksDB alone (the pilot) need not read.
	for _, c := range []struct{ limit, bytes, cands, read string }{
		{"512MiB", "536870912", "RL/k64a2l5m", "false"},
		{"", "none", "RL/off,RL/k64a2l5m", "true"},
		{"999999GiB", "1073740750258176", "L/off", "false"},
	} {
		env := rocksDispatch(r)
		env["GO_MEM_LIMIT"], env["CANDIDATES"], env["READ"] = c.limit, c.cands, c.read
		got := run(t, r, env)
		if got.code != 0 {
			t.Fatalf("limit %q, candidates %s, read %s: exit %d\n%s", c.limit, c.cands, c.read, got.code, got.stderr)
		}
		if want := "pebble_memory_limit=" + c.bytes + "\n"; !strings.Contains(got.output, want) {
			t.Errorf("limit %q: the outputs lack %q:\n%s", c.limit, want, got.output)
		}
	}
}

// timedDispatch is the environment of a dispatch that times the reads of every
// repetition: layout L and M on Pebble, five repetitions on both architectures, the
// 7-day window, retention run in step with the ingest.
func timedDispatch(r repo) map[string]string {
	env := dispatch(r)
	env["REF"], env["SHA"], env["EXPECTED_SHA"] = "refs/heads/main", r.offMain, r.offMain
	env["WINDOWS"], env["CANDIDATES"], env["REPS"] = "7", "L/k64a2l5m,M/crdb1,L/off", "5"
	env["SYNC"], env["METRICS_EVERY"], env["GO_MEM_LIMIT"], env["RETENTION_MODE"], env["RETAIN_FLUSH_DELS"] = "false", "", "", "sync", "default"
	env["READ"], env["READ_TIMED"] = "true", "true"
	return env
}

// A dispatch that times the reads says so, and only it makes its plans at the product's
// block cache: the plan flags are a fixed mapping of the input and empty otherwise.
func TestATimedReadDispatchMakesItsPlansAtTheProductsCache(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	for _, wins := range []string{"7", "1", "2,7"} {
		env := timedDispatch(r)
		env["WINDOWS"] = wins
		got := run(t, r, env)
		if got.code != 0 {
			t.Fatalf("a timed read of windows %s: exit %d\n%s", wins, got.code, got.stderr)
		}
		for _, want := range []string{"read=true\n", "read_timed=true\n", "plan_flags=-cache-mb 256\n", "retention_mode=sync\n"} {
			if !strings.Contains(got.output, want) {
				t.Errorf("windows %s: the outputs lack %q:\n%s", wins, want, got.output)
			}
		}
		if want := "time the reads of every repetition: `true` (extra plan flags: `-cache-mb 256`)"; !strings.Contains(got.summary, want) {
			t.Errorf("windows %s: the summary lacks %q:\n%s", wins, want, got.summary)
		}
	}

	// The same dispatch without the input, and one that reads without timing, change
	// nothing about the plans, and so keep every registered command valid.
	for _, read := range []string{"true", "false"} {
		env := timedDispatch(r)
		env["READ"], env["READ_TIMED"], env["WINDOWS"] = read, "false", "30"
		got := run(t, r, env)
		if got.code != 0 {
			t.Fatalf("read %s, not timed: exit %d\n%s", read, got.code, got.stderr)
		}
		for _, want := range []string{"read_timed=false\n", "plan_flags=\n"} {
			if !strings.Contains(got.output, want) {
				t.Errorf("read %s, not timed: the outputs lack %q:\n%s", read, want, got.output)
			}
		}
		if strings.Contains(got.output, "-cache-mb") {
			t.Errorf("read %s, not timed: the plans' flags name a cache:\n%s", read, got.output)
		}
	}
}

// A timed read is refused, with exit 1 and an error that names the input and does not
// repeat it, when it is not a read, when a candidate is built by rocksbench, which has
// no timed read, when the retention is the store gate's, when a window is longer
// than the step limits were sized for, and when it runs under a Go memory limit.
func TestATimedReadIsRefusedWhenItsConditionsFail(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	const evil = "x::set-output name=y::z"
	background := map[string]string{
		"RETENTION_MODE": "background", "SYNC": "true", "METRICS_EVERY": "10s", "EXPECTED_SHA": r.onMain, "SHA": r.onMain,
		"CANDIDATES": "Lroot/off,Lroot/k64a2l1ns",
	}
	for _, c := range []struct {
		name string
		set  map[string]string
		want string
	}{
		{"an input as read_timed", map[string]string{"READ_TIMED": evil}, "read_timed:"},
		{"read_timed as a word", map[string]string{"READ_TIMED": "yes"}, "read_timed:"},
		{"read_timed empty", map[string]string{"READ_TIMED": ""}, "read_timed:"},
		{"no read", map[string]string{"READ": "false"}, "read_timed: a timed read is a read"},
		{"a RocksDB candidate", map[string]string{"CANDIDATES": "L/off,RL/off"}, "read_timed: rocksbench has no timed read"},
		{"a RocksDB candidate of four-way layout alone", map[string]string{"CANDIDATES": "RL4/k64a2l5m"}, "read_timed: rocksbench has no timed read"},
		{"a background retention", background, "read_timed: a background dispatch is the store gate's arm"},
		{"a Go memory limit", map[string]string{"GO_MEM_LIMIT": "11GiB"}, "read_timed: a timed read runs with no Go memory limit"},
		{"a Go memory limit in MiB", map[string]string{"GO_MEM_LIMIT": "512MiB"}, "read_timed: a timed read runs with no Go memory limit"},
		{"a window of 8 days", map[string]string{"WINDOWS": "8"}, "read_timed: windows of at most 7 days"},
		{"a window of 30 days", map[string]string{"WINDOWS": "30"}, "read_timed: windows of at most 7 days"},
		{"a long window among short ones", map[string]string{"WINDOWS": "2,7,30"}, "read_timed: windows of at most 7 days"},
	} {
		env := timedDispatch(r)
		for k, v := range c.set {
			env[k] = v
		}
		got := run(t, r, env)
		if got.code != 1 || !strings.Contains(got.stderr, "::error::"+c.want) {
			t.Errorf("%s: exit %d, %q; want 1 and an error starting %q", c.name, got.code, got.stderr, c.want)
		}
		if strings.Contains(got.stderr, "set-output") {
			t.Errorf("%s: the error repeats the input: %q", c.name, got.stderr)
		}
		if got.output != "" {
			t.Errorf("%s: outputs were written: %q", c.name, got.output)
		}
	}
}

// Every input that is not well formed is refused, with exit 1 and an error that names the
// input and does not repeat it. The tag is matched by an anchored expression, not a
// glob, and its commit must be on origin/main; the commit the dispatch names must be
// the run's.
func TestADispatchRefusesEveryMalformedInput(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	const evil = "x::set-output name=y::z"
	for _, c := range []struct {
		name string
		set  map[string]string
		want string
	}{
		{"a branch other than main", map[string]string{"REF": "refs/heads/gate"}, "the ref:"},
		{"a pull request's merge ref", map[string]string{"REF": "refs/pull/1/merge"}, "the ref:"},
		{"a tag of seven digits", map[string]string{"REF": "refs/tags/gate-freeze-2026101"}, "the ref:"},
		{"a tag of nine digits", map[string]string{"REF": "refs/tags/gate-freeze-202610120"}, "the ref:"},
		{"a tag with a suffix", map[string]string{"REF": "refs/tags/gate-freeze-20261012-x"}, "the ref:"},
		{"a tag under the name", map[string]string{"REF": "refs/tags/gate-freeze-20261012/x"}, "the ref:"},
		{"a tag of letters a glob would take", map[string]string{"REF": "refs/tags/gate-freeze-2026101x"}, "the ref:"},
		{"a tag with a prefix", map[string]string{"REF": "refs/tags/x-gate-freeze-20261012"}, "the ref:"},
		{"a branch of the tag's name", map[string]string{"REF": "refs/heads/gate-freeze-20261012"}, "the ref:"},
		{"a tag whose commit is not on main", map[string]string{"SHA": r.offMain, "EXPECTED_SHA": r.offMain}, "the ref: the tag's commit is not on origin/main"},
		{"an input as the ref", map[string]string{"REF": evil}, "the ref:"},

		{"a retention mode", map[string]string{"RETENTION_MODE": "async"}, "retention_mode:"},
		{"an input as the retention mode", map[string]string{"RETENTION_MODE": evil}, "retention_mode:"},

		{"a threshold off the ladder", map[string]string{"RETAIN_FLUSH_DELS": "20000"}, "retain_flush_dels:"},
		{"an input as the threshold", map[string]string{"RETAIN_FLUSH_DELS": evil}, "retain_flush_dels:"},
		{"a threshold with sync", map[string]string{"RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "32768"}, "retain_flush_dels: a threshold is measured only with retention_mode background"},
		{"a threshold with a candidate not through the root store", map[string]string{"CANDIDATES": "Lroot/off,L/k64a2l1ns"}, "retain_flush_dels: a threshold is for the candidates built through the root store"},

		{"no commit named, background", map[string]string{"EXPECTED_SHA": ""}, "expected_sha: a background dispatch names the commit"},
		{"a commit that is not the run's", map[string]string{"EXPECTED_SHA": strings.Repeat("a", 40)}, "expected_sha: not the commit"},
		{"a commit of uppercase digits", map[string]string{"EXPECTED_SHA": strings.ToUpper(r.onMain)}, "expected_sha: 40 lowercase"},
		{"a short commit", map[string]string{"EXPECTED_SHA": r.onMain[:12]}, "expected_sha: 40 lowercase"},
		{"an input as the commit", map[string]string{"EXPECTED_SHA": evil}, "expected_sha: 40 lowercase"},
		{"a commit not the run's, sync from main", map[string]string{"REF": "refs/heads/main", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default", "EXPECTED_SHA": strings.Repeat("b", 40)}, "expected_sha: not the commit"},

		{"two windows, background", map[string]string{"WINDOWS": "7,30"}, "windows: a background dispatch builds exactly one window"},
		{"a window of 14 days, background", map[string]string{"WINDOWS": "14"}, "windows: a background dispatch builds exactly one window"},
		{"commits not synced, background", map[string]string{"SYNC": "false"}, "sync: a background dispatch syncs every commit"},
		{"no metrics, background", map[string]string{"METRICS_EVERY": ""}, "metrics_every: a background dispatch samples its metrics"},
		{"a candidate not through the root store, background", map[string]string{"RETAIN_FLUSH_DELS": "default", "CANDIDATES": "Lroot/off,L/off"}, "candidates: a background dispatch builds the candidates through the root store"},

		{"an input as read", map[string]string{"READ": evil}, "read:"},
		{"read as a word", map[string]string{"READ": "yes"}, "read:"},

		{"RocksDB with a background retention", map[string]string{"CANDIDATES": "Lroot/off,RL/off", "RETAIN_FLUSH_DELS": "default"}, "retention_mode: the RocksDB candidates"},
		{"RocksDB with a threshold", map[string]string{"CANDIDATES": "RL4/off", "RETENTION_MODE": "sync", "SYNC": "false", "METRICS_EVERY": ""}, "retain_flush_dels: the RocksDB candidates"},
		{"RocksDB with metrics", map[string]string{"CANDIDATES": "RL/off", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default"}, "metrics_every: rocksbench samples no metrics"},
		{"both engines without a read", map[string]string{"CANDIDATES": "L/off,RL/off", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default", "METRICS_EVERY": ""}, "read: a dispatch of both engines"},
		{"both engines, RL4 first, without a read", map[string]string{"CANDIDATES": "RL4/k64a2l5m,L/k64a2l5m", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default", "METRICS_EVERY": ""}, "read: a dispatch of both engines"},

		{"a family", map[string]string{"FAMILY": evil}, "family:"},
		{"the architectures", map[string]string{"ARCHES": evil}, "arches:"},
		{"sync", map[string]string{"SYNC": evil}, "sync:"},
		{"the metrics interval", map[string]string{"METRICS_EVERY": evil}, "metrics_every:"},
		{"the memory limit", map[string]string{"GO_MEM_LIMIT": evil}, "go_mem_limit:"},
		{"the pins window", map[string]string{"PINS_WINDOW": evil}, "pins_window:"},
		{"the repetitions", map[string]string{"REPS": evil}, "reps:"},
		{"the parallel jobs", map[string]string{"MAX_PARALLEL": evil}, "max_parallel:"},
		{"the windows", map[string]string{"WINDOWS": evil}, "windows:"},
		{"the candidates", map[string]string{"CANDIDATES": evil}, "candidates:"},
	} {
		env := dispatch(r)
		for k, v := range c.set {
			env[k] = v
		}
		got := run(t, r, env)
		if got.code != 1 || !strings.Contains(got.stderr, "::error::"+c.want) {
			t.Errorf("%s: exit %d, %q; want 1 and an error starting %q", c.name, got.code, got.stderr, c.want)
		}
		if strings.Contains(got.stderr, "set-output") {
			t.Errorf("%s: the error repeats the input: %q", c.name, got.stderr)
		}
		if got.output != "" {
			t.Errorf("%s: outputs were written: %q", c.name, got.output)
		}
	}
}

// The script passes shellcheck where it is installed (the workflow's own run blocks are
// checked by actionlint, which does not see a script file). It is written for bash 3.2
// too, the bash a Mac has, on which this test also runs it.
func TestTheDispatchCheckPassesShellcheck(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("shellcheck"); err != nil {
		t.Skip("shellcheck is not installed")
	}
	out, err := exec.CommandContext(t.Context(), "shellcheck", "gate-dispatch-check.sh").CombinedOutput()
	if err != nil {
		t.Errorf("shellcheck: %v\n%s", err, out)
	}
}

// The shape and serve_options inputs: a background dispatch of the root store's
// candidates may name a table shape, as build flags each of which is one exact word, and
// serve's options. A dispatch that names neither has the outputs and the summary it had
// before the inputs existed, byte for byte, and neither input adds a job.

// A dispatch that names no shape and not serve's options comes to the same outputs and
// summary as before the inputs existed: a background dispatch of the store gate's arm
// and a synchronous one that times its reads are pinned here, byte for byte. A shape of
// spaces alone is no shape.
func TestADispatchWithoutAShapeOrServeOptionsIsUnchanged(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	for _, shape := range []string{"", "   "} {
		env := dispatch(r)
		env["ARCHES"], env["REPS"], env["SHAPE"] = "x64", "1", shape
		got := run(t, r, env)
		if got.code != 0 {
			t.Fatalf("shape %q: exit %d\n%s", shape, got.code, got.stderr)
		}
		wantOutput := `matrix={"include":[` +
			`{"os":"ubuntu-24.04","window":7,"candidate":"Lroot/off","slug":"Lroot_off","engine":"pebble","rep":1},` +
			`{"os":"ubuntu-24.04","window":7,"candidate":"Lroot/k64a2l1ns","slug":"Lroot_k64a2l1ns","engine":"pebble","rep":1}]}` + "\n" +
			"family=rmae\nfamily_flags=-run-max-age 30m\nwindows=7\npins_window=2\nsync=true\nmetrics_every=10s\n" +
			"go_mem_limit=11GiB\npebble_memory_limit=11811160064\nread=false\nread_timed=false\nplan_flags=\nmax_parallel=12\n" +
			"retention_mode=background\npace=4500\nretain_flush_dels=24576\nexpected_sha=" + r.onMain + "\nrocksdb_oses=[]\n"
		if got.output != wantOutput {
			t.Errorf("shape %q: the outputs changed:\n got %q\nwant %q", shape, got.output, wantOutput)
		}
		wantSummary := "### Planned jobs: 2\n\n" +
			"- ref: `refs/tags/gate-freeze-20261012`; commit: `" + r.onMain + "`\n" +
			"- family: `rmae` (scenario flags `-run-max-age 30m`)\n" +
			"- windows: `7` days; pins chosen from the `2`-day window\n" +
			"- sync: `true`; metrics every: `10s`\n" +
			"- retention mode: `background`; pace: `4500` records/s (0: unpaced); retention flush threshold: `24576`\n" +
			"- Go memory limit: `11GiB` (a RocksDB build's: that less the plan's cache and four memtables; `-pebble-memory-limit 11811160064`)\n" +
			"- read repetition 1 after its build: `false`\n" +
			"- time the reads of every repetition: `false` (extra plan flags: `none`)\n" +
			"- at most `12` build jobs at once\n\n" +
			"| runner | window (days) | candidate | engine | repetition |\n|---|---|---|---|---|\n" +
			"| ubuntu-24.04 | 7 | `Lroot/off` | pebble | 1 |\n" +
			"| ubuntu-24.04 | 7 | `Lroot/k64a2l1ns` | pebble | 1 |\n"
		if got.summary != wantSummary {
			t.Errorf("shape %q: the summary changed:\n got %q\nwant %q", shape, got.summary, wantSummary)
		}
	}

	env := timedDispatch(r)
	env["ARCHES"], env["REPS"], env["CANDIDATES"] = "arm64", "1", "L/k64a2l5m"
	got := run(t, r, env)
	if got.code != 0 {
		t.Fatalf("a timed read: exit %d\n%s", got.code, got.stderr)
	}
	wantOutput := `matrix={"include":[{"os":"ubuntu-24.04-arm","window":7,"candidate":"L/k64a2l5m","slug":"L_k64a2l5m","engine":"pebble","rep":1}]}` + "\n" +
		"family=rmae\nfamily_flags=-run-max-age 30m\nwindows=7\npins_window=2\nsync=false\nmetrics_every=\n" +
		"go_mem_limit=\npebble_memory_limit=none\nread=true\nread_timed=true\nplan_flags=-cache-mb 256\nmax_parallel=12\n" +
		"retention_mode=sync\npace=0\nretain_flush_dels=default\nexpected_sha=" + r.offMain + "\nrocksdb_oses=[]\n"
	if got.output != wantOutput {
		t.Errorf("a timed read: the outputs changed:\n got %q\nwant %q", got.output, wantOutput)
	}
	wantSummary := "### Planned jobs: 1\n\n" +
		"- ref: `refs/heads/main`; commit: `" + r.offMain + "`\n" +
		"- family: `rmae` (scenario flags `-run-max-age 30m`)\n" +
		"- windows: `7` days; pins chosen from the `2`-day window\n" +
		"- sync: `false`; metrics every: `off`\n" +
		"- retention mode: `sync`; pace: `0` records/s (0: unpaced); retention flush threshold: `default`\n" +
		"- Go memory limit: `none` (a RocksDB build's: that less the plan's cache and four memtables; `-pebble-memory-limit none`)\n" +
		"- read repetition 1 after its build: `true`\n" +
		"- time the reads of every repetition: `true` (extra plan flags: `-cache-mb 256`)\n" +
		"- at most `12` build jobs at once\n\n" +
		"| runner | window (days) | candidate | engine | repetition |\n|---|---|---|---|---|\n" +
		"| ubuntu-24.04-arm | 7 | `L/k64a2l5m` | pebble | 1 |\n"
	if got.summary != wantSummary {
		t.Errorf("a timed read: the summary changed:\n got %q\nwant %q", got.summary, wantSummary)
	}
}

// outputsWithout is the outputs of a dispatch less the lines of the two inputs.
func outputsWithout(output string) string {
	var kept []string
	for line := range strings.SplitSeq(output, "\n") {
		if !strings.HasPrefix(line, "shape=") && !strings.HasPrefix(line, "serve_options=") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// Every word of a shape is taken alone and with the others it may go with, in any
// order and with any number of spaces between, and is passed on as it was given, joined
// by single spaces. Serve's options are taken alone and with a shape. Each is written to
// the outputs and to the summary, and the rest of the outputs, the matrix with them,
// are the dispatch's without the inputs: they add no job.
func TestADispatchTakesAShapeAndServeOptions(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	base := run(t, r, dispatch(r))
	if base.code != 0 {
		t.Fatalf("a dispatch without the inputs: exit %d\n%s", base.code, base.stderr)
	}
	const p5 = "-target-sizes-mb=64,4,8,64,128,256,512"
	for _, c := range []struct{ shape, serve, want string }{
		{"-lbase-target-mb=4", "false", "-lbase-target-mb=4"},
		{p5, "false", p5},
		{"-disable-delete-only", "false", "-disable-delete-only"},
		{"-compaction-concurrency=1,3", "false", "-compaction-concurrency=1,3"},
		{"-compression=snappy", "false", "-compression=snappy"},
		{"-compression=zstd", "false", "-compression=zstd"},
		{"-compression=balanced", "false", "-compression=balanced"},
		{p5 + " -disable-delete-only", "false", p5 + " -disable-delete-only"},
		{p5 + " -disable-delete-only -compaction-concurrency=1,3", "true", p5 + " -disable-delete-only -compaction-concurrency=1,3"},
		{"  -compression=zstd   " + p5 + "  -compaction-concurrency=1,3 -disable-delete-only ", "true", "-compression=zstd " + p5 + " -compaction-concurrency=1,3 -disable-delete-only"},
		{"-lbase-target-mb=4 -disable-delete-only -compaction-concurrency=1,3 -compression=balanced", "false", "-lbase-target-mb=4 -disable-delete-only -compaction-concurrency=1,3 -compression=balanced"},
		{"", "true", ""},
	} {
		env := dispatch(r)
		env["SHAPE"], env["SERVE_OPTIONS"] = c.shape, c.serve
		got := run(t, r, env)
		if got.code != 0 {
			t.Errorf("shape %q, serve's options %s: exit %d\n%s", c.shape, c.serve, got.code, got.stderr)
			continue
		}
		wantShape, wantServe := "", ""
		if c.want != "" {
			wantShape = "shape=" + c.want + "\n"
		}
		if c.serve == "true" {
			wantServe = "serve_options=true\n"
		}
		if want := wantShape + wantServe; !strings.HasSuffix(got.output, "rocksdb_oses=[]\n"+want) {
			t.Errorf("shape %q, serve's options %s: the outputs do not end with %q:\n%s", c.shape, c.serve, want, got.output)
		}
		if n := strings.Count(got.output, "shape="); n > 1 {
			t.Errorf("shape %q: %d shape outputs", c.shape, n)
		}
		if rest := outputsWithout(got.output); rest != base.output {
			t.Errorf("shape %q, serve's options %s: the other outputs changed:\n got %q\nwant %q", c.shape, c.serve, rest, base.output)
		}
		shown := c.want
		if shown == "" {
			shown = "Pebble's own"
		}
		line := "- table shape: `" + shown + "`; serve's options: `" + c.serve +
			"` (passed to every build; each manifest records them as pebble_shape and serve_options)\n"
		if want := "retention flush threshold: `24576`\n" + line + "- Go memory limit:"; !strings.Contains(got.summary, want) {
			t.Errorf("shape %q, serve's options %s: the summary lacks %q:\n%s", c.shape, c.serve, want, got.summary)
		}
		if rest := strings.Replace(got.summary, line, "", 1); rest != base.summary {
			t.Errorf("shape %q, serve's options %s: the summary changed beyond its one line:\n got %q\nwant %q", c.shape, c.serve, rest, base.summary)
		}
	}
}

// A shape or serve's options is refused, with exit 1 and an error that names the input
// and does not repeat it, for any word that is not exactly one of the shape's (another
// value, an empty one, a flag without its = or with its value as the next word,
// capitals, a prefix or a suffix, a second value or a second =, two dashes, an
// abbreviation, a glob, shell syntax, another flag of the program, a word hidden in the
// value of another), for a flag given twice, for more words or characters than a shape
// can have, and for a dispatch that is not a background build of the root store's
// candidates.
func TestAShapeOrServeOptionsIsRefused(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	const (
		evil = "x::set-output name=y::z"
		p5   = "-target-sizes-mb=64,4,8,64,128,256,512"
		word = "shape: every word is"
	)
	type refusal struct {
		name string
		set  map[string]string
		want string
	}
	shape := func(s string) map[string]string { return map[string]string{"SHAPE": s} }
	cases := []refusal{
		{"an input as the shape", shape(evil), "shape: build flags separated by spaces"},
		{"another base-level target", shape("-lbase-target-mb=8"), word},
		{"another list of sizes", shape("-target-sizes-mb=64,4,8"), word},
		{"a list of sizes with one more", shape(p5 + ",1024"), word},
		{"another concurrency", shape("-compaction-concurrency=1,4"), word},
		{"another compression", shape("-compression=lz4"), word},
		{"an empty compression", shape("-compression="), word},
		{"an empty list of sizes", shape("-target-sizes-mb="), word},
		{"a compression without =", shape("-compression"), word},
		{"a base-level target without =", shape("-lbase-target-mb"), word},
		{"a compression as the next word", shape("-compression zstd"), word},
		{"a list of sizes as the next word", shape("-target-sizes-mb 64,4,8,64,128,256,512"), word},
		{"a value given to a switch", shape("-disable-delete-only=true"), word},
		{"a switch turned off", shape("-disable-delete-only=false"), word},
		{"capitals", shape("-Compression=zstd"), "shape: build flags separated by spaces"},
		{"a capital value", shape("-compression=ZSTD"), "shape: build flags separated by spaces"},
		{"a suffix on a value", shape("-compression=zstdx"), word},
		{"a prefix on a value", shape("-compression=xzstd"), word},
		{"a prefix on a flag", shape("x-compression=zstd"), word},
		{"a suffix on a switch", shape("-disable-delete-onlyx"), word},
		{"two values", shape("-compression=zstd,snappy"), word},
		{"a second =", shape("-compression=zstd=zstd"), word},
		{"two dashes", shape("--compression=zstd"), word},
		{"two dashes on a switch", shape("--disable-delete-only"), word},
		{"an abbreviation", shape("-disable-delete"), word},
		{"an abbreviated flag", shape("-target-sizes=64,4,8,64,128,256,512"), word},
		{"a glob as the value", shape("-compression=*"), "shape: build flags separated by spaces"},
		{"a glob in the value", shape("-target-sizes-mb=64,4,8,64,128,256,51?"), "shape: build flags separated by spaces"},
		{"a bracket glob", shape("-compression=[z]std"), "shape: build flags separated by spaces"},
		{"a command after a word", shape("-compression=zstd;true"), "shape: build flags separated by spaces"},
		{"a substitution", shape("-compression=$(true)"), "shape: build flags separated by spaces"},
		{"a backquote", shape("-compression=`true`"), "shape: build flags separated by spaces"},
		{"quotes", shape(`"-compression=zstd"`), "shape: build flags separated by spaces"},
		{"a single quote", shape("'-disable-delete-only'"), "shape: build flags separated by spaces"},
		{"a second line", shape("-disable-delete-only\n-serve-options"), "shape: build flags separated by spaces"},
		{"a tab", shape("-disable-delete-only\t-compression=zstd"), "shape: build flags separated by spaces"},
		{"serve's options as a word of the shape", shape("-serve-options"), word},
		{"serve's options after a word", shape("-disable-delete-only -serve-options"), word},
		{"another flag of the program", shape("-l0-stop-writes=20"), word},
		{"the retention mode", shape("-retention-mode=sync"), word},
		{"a pace", shape("-pace=0"), word},
		{"an untimed build", shape("-untimed"), word},
		{"profiles", shape("-profile-windows"), word},
		{"a read-ahead", shape("-read-ahead=8"), word},
		{"a candidate", shape("-candidate=L/off"), "shape: build flags separated by spaces"},
		{"a word inside the list of sizes", shape(p5 + ",-serve-options"), word},
		{"a word inside the concurrency", shape("-compaction-concurrency=1,3,-disable-delete-only"), word},
		{"a word inside the compression", shape("-compression=zstd,-disable-delete-only"), word},
		{"a compression given twice", shape("-compression=zstd -compression=zstd"), "shape: a flag is given twice"},
		{"two compressions", shape("-compression=zstd -compression=snappy"), "shape: a flag is given twice"},
		{"a switch given twice", shape("-disable-delete-only " + p5 + " -disable-delete-only"), "shape: a flag is given twice"},
		{"a list of sizes given twice", shape(p5 + " " + p5), "shape: a flag is given twice"},
		{"a base-level target and a list of sizes", shape("-lbase-target-mb=4 " + p5), "shape: -lbase-target-mb and -target-sizes-mb: give one"},
		{"a list of sizes and a base-level target", shape(p5 + " -disable-delete-only -lbase-target-mb=4"), "shape: -lbase-target-mb and -target-sizes-mb: give one"},
		{"five words", shape("-lbase-target-mb=4 -disable-delete-only -compaction-concurrency=1,3 -compression=zstd " + p5), "shape: at most 4 words"},
		{"201 characters", shape(strings.Repeat(" ", 201-len("-disable-delete-only")) + "-disable-delete-only"), "shape: longer than 200 characters"},
		{"a shape with retention_mode sync", map[string]string{"SHAPE": p5, "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default"}, "shape: a table shape is measured only with retention_mode background"},
		{"a shape of a candidate not through the root store", map[string]string{"SHAPE": "-disable-delete-only", "RETAIN_FLUSH_DELS": "default", "CANDIDATES": "Lroot/off,L/off"}, "shape: a table shape is for the candidates built through the root store"},
		{"a shape of a RocksDB candidate", map[string]string{"SHAPE": "-compression=zstd", "CANDIDATES": "RL/off", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default", "METRICS_EVERY": ""}, "shape: a table shape is measured only with retention_mode background"},

		{"an input as serve's options", map[string]string{"SERVE_OPTIONS": evil}, "serve_options: not true or false"},
		{"serve's options as a word", map[string]string{"SERVE_OPTIONS": "yes"}, "serve_options: not true or false"},
		{"serve's options in capitals", map[string]string{"SERVE_OPTIONS": "TRUE"}, "serve_options: not true or false"},
		{"serve's options as a number", map[string]string{"SERVE_OPTIONS": "1"}, "serve_options: not true or false"},
		{"serve's options empty", map[string]string{"SERVE_OPTIONS": ""}, "serve_options: not true or false"},
		{"serve's options with a flag", map[string]string{"SERVE_OPTIONS": "true -disable-delete-only"}, "serve_options: not true or false"},
		{"serve's options with retention_mode sync", map[string]string{"SERVE_OPTIONS": "true", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default"}, "serve_options: serve's options are measured only with retention_mode background"},
		{"serve's options of a candidate not through the root store", map[string]string{"SERVE_OPTIONS": "true", "RETAIN_FLUSH_DELS": "default", "CANDIDATES": "Lroot/off,L/k64a2l5m"}, "serve_options: serve's options are for the candidates built through the root store"},
		{"serve's options of a RocksDB candidate", map[string]string{"SERVE_OPTIONS": "true", "CANDIDATES": "RL4/k64a2l5m", "RETENTION_MODE": "sync", "RETAIN_FLUSH_DELS": "default", "METRICS_EVERY": ""}, "serve_options: serve's options are measured only with retention_mode background"},
	}
	for _, c := range cases {
		env := dispatch(r)
		for k, v := range c.set {
			env[k] = v
		}
		got := run(t, r, env)
		if got.code != 1 || !strings.Contains(got.stderr, "::error::"+c.want) {
			t.Errorf("%s: exit %d, %q; want 1 and an error starting %q", c.name, got.code, got.stderr, c.want)
		}
		if strings.Contains(got.stderr, "set-output") || strings.Contains(got.stderr, "$(true)") {
			t.Errorf("%s: the error repeats the input: %q", c.name, got.stderr)
		}
		if got.output != "" {
			t.Errorf("%s: outputs were written: %q", c.name, got.output)
		}
	}
}
