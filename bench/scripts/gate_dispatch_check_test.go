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
		"REF": "refs/tags/gate-freeze-20261012", "SHA": r.onMain, "RUN_ID": "100", "RUN_ATTEMPT": "1",
		"FAMILY": "rmae", "WINDOWS": "7", "CANDIDATES": "Lroot/off,Lroot/k64a2l1ns", "REPS": "3", "ARCHES": "both",
		"PINS_WINDOW": "2", "SYNC": "true", "METRICS_EVERY": "10s", "GO_MEM_LIMIT": "11GiB", "MAX_PARALLEL": "12",
		"RETENTION_MODE": "background", "RETAIN_FLUSH_DELS": "24576", "EXPECTED_SHA": r.onMain,
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
		"sync=true\n", "metrics_every=10s\n", "max_parallel=12\n", "plans_key=bench-plans-100-1\n",
		`matrix={"include":[{"os":"ubuntu-24.04","window":7,"candidate":"Lroot/off","slug":"Lroot_off","rep":1},`,
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
