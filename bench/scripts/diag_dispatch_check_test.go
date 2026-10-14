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

// The setup of a dispatch of the diag workflow: the script takes a well-formed dispatch
// from main, or a patch from the verify branch, and refuses every malformed input, each
// with a message that names the input and never repeats its text.

const control = `{"name":"control","build_flags":"-profile-windows","plan_flags":""}`

// diagDispatch is the environment of a well-formed dispatch from main: an unpatched
// build of one candidate in a control and a variant, twice, on the arm64 Linux runner
// and the x64 one.
func diagDispatch(r repo) map[string]string {
	return map[string]string{
		"REF": "refs/heads/main", "SHA": r.onMain, "RUN_ID": "100", "RUN_ATTEMPT": "1", "LABEL": "p5",
		"BASE_SHA": r.onMain, "HAS_PATCH_B64": "false", "GIST_ID": "", "PATCH_SHA256": "", "TREE": "",
		"CONFIGS": `[` + control + `,{"name":"cache2g","build_flags":"-profile-windows","plan_flags":"-cache-mb=2048"}]`,
		"FAMILY":  "rmae", "WINDOW": "7", "PINS_WINDOW": "2", "CANDIDATE": "Lroot/k64a2l5m", "REPS": "2",
		"RUNNERS": "ubuntu-24.04-arm,ubuntu-24.04", "MACOS_LOW_RSS": "false", "RETENTION_MODE": "background",
		"RETAIN_FLUSH_DELS": "default", "SYNC": "true", "METRICS_EVERY": "10s", "GO_MEM_LIMIT": "", "PACE": "4500",
		"MAX_PARALLEL": "8",
	}
}

func runDiag(t *testing.T, r repo, env map[string]string) result {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	script, err := filepath.Abs("diag-dispatch-check.sh")
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

func TestADiagDispatchIsTakenFromMainOrFromTheVerifyBranch(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	got := runDiag(t, r, diagDispatch(r))
	if got.code != 0 {
		t.Fatalf("a well-formed dispatch: exit %d\n%s", got.code, got.stderr)
	}
	for _, want := range []string{
		"patched=false\n", "base_sha=" + r.onMain + "\n", "candidate_slug=Lroot_k64a2l5m\n", "pace=4500\n",
		"family_flags=-run-max-age 30m\n", "window=7\n", "metrics_every=10s\n", "max_parallel=8\n",
		`plans=[{"id":"p1","flags":""},{"id":"p2","flags":"-cache-mb=2048"}]` + "\n",
		// The repetition is the outer loop, then the runner, then the configuration:
		// the control and its variants of one repetition run together.
		`matrix={"include":[{"os":"ubuntu-24.04-arm","name":"control","role":"A","build_flags":"-profile-windows","plan":"p1","rep":1},{"os":"ubuntu-24.04-arm","name":"cache2g","role":"B","build_flags":"-profile-windows","plan":"p2","rep":1},`,
	} {
		if !strings.Contains(got.output, want) {
			t.Errorf("the outputs lack %q:\n%s", want, got.output)
		}
	}
	if n := strings.Count(got.output, `"rep":`); n != 8 {
		t.Errorf("%d jobs in the matrix, want 2 runners x 2 configurations x 2 repetitions", n)
	}
	for _, want := range []string{"### Diagnostic dispatch: 8 build jobs", "**Diagnostic only.**", "patch: none", "`GOMAXPROCS` 10"} {
		if !strings.Contains(got.summary, want) {
			t.Errorf("the summary lacks %q:\n%s", want, got.summary)
		}
	}

	// A patch is built from the verify branch, with its hash and its tree.
	patched := diagDispatch(r)
	patched["REF"], patched["HAS_PATCH_B64"] = "refs/heads/verify", "true"
	patched["PATCH_SHA256"], patched["TREE"] = strings.Repeat("a", 64), strings.Repeat("b", 40)
	patched["CONFIGS"] = `[` + control + `,{"name":"lbase","build_flags":"-profile-windows -lbase-target-mb=256 -read-ahead=8","plan_flags":""},` +
		`{"name":"sizes","build_flags":"-profile-windows -target-sizes-mb=64,128,512 -disable-delete-only","plan_flags":""}]`
	patched["RUNNERS"], patched["REPS"] = "macos-15", "3"
	patched["MACOS_LOW_RSS"] = "true"
	got = runDiag(t, r, patched)
	if got.code != 0 {
		t.Fatalf("a patched dispatch from the verify branch: exit %d\n%s", got.code, got.stderr)
	}
	for _, want := range []string{
		"patched=true\n", "tree=" + strings.Repeat("b", 40) + "\n", "patch_sha256=" + strings.Repeat("a", 64) + "\n",
		`"build_flags":"-profile-windows -lbase-target-mb=256 -read-ahead=8","plan":"p1"`,
	} {
		if !strings.Contains(got.output, want) {
			t.Errorf("the patched outputs lack %q:\n%s", want, got.output)
		}
	}
	if n := strings.Count(got.output, `"rep":`); n != 9 {
		t.Errorf("%d jobs in the patched matrix, want 1 runner x 3 configurations x 3 repetitions", n)
	}

	// A build of main's own code may also come from the verify branch, and a
	// synchronous retention needs no pace.
	synchronous := diagDispatch(r)
	synchronous["REF"], synchronous["RETENTION_MODE"], synchronous["PACE"] = "refs/heads/verify", "sync", "0"
	synchronous["CONFIGS"] = `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-disable-delete-only","plan_flags":""}]`
	if got = runDiag(t, r, synchronous); got.code != 0 {
		t.Fatalf("a synchronous dispatch from the verify branch: exit %d\n%s", got.code, got.stderr)
	}
}

// Every input that is not well formed is refused, with exit 1 and an error that names the
// input and does not repeat it.
func TestADiagDispatchRefusesEveryMalformedInput(t *testing.T) {
	t.Parallel()
	r := newRepo(t)

	const evil = "x::set-output name=y::z"
	cfgs := func(second string) map[string]string {
		return map[string]string{"CONFIGS": `[` + control + `,` + second + `]`}
	}
	variant := func(flags string) map[string]string {
		return cfgs(`{"name":"b","build_flags":"` + flags + `","plan_flags":""}`)
	}
	patch := map[string]string{"REF": "refs/heads/verify", "HAS_PATCH_B64": "true", "PATCH_SHA256": strings.Repeat("a", 64), "TREE": strings.Repeat("b", 40)}
	with := func(base map[string]string, k, v string) map[string]string {
		out := map[string]string{}
		for bk, bv := range base {
			out[bk] = bv
		}
		out[k] = v
		return out
	}
	for _, c := range []struct {
		name string
		set  map[string]string
		want string
	}{
		{"a dispatched commit that is not on main", map[string]string{"REF": "refs/heads/verify", "SHA": r.offMain}, "the ref: the dispatched commit is not on origin/main"},
		{"a branch other than main or verify", map[string]string{"REF": "refs/heads/gate"}, "the ref:"},
		{"a tag", map[string]string{"REF": "refs/tags/gate-freeze-20261012"}, "the ref:"},
		{"an input as the ref", map[string]string{"REF": evil}, "the ref:"},
		{"a patch from main", with(patch, "REF", "refs/heads/main"), "the ref: a build of a patch is dispatched from the verify branch"},
		{"a label with a space", map[string]string{"LABEL": "p 5"}, "label:"},
		{"an input as the label", map[string]string{"LABEL": evil}, "label:"},
		{"a base commit that is not on main", map[string]string{"BASE_SHA": r.offMain}, "base_sha: the commit is not on origin/main"},
		{"a short base commit", map[string]string{"BASE_SHA": r.onMain[:12]}, "base_sha:"},
		{"an input as the base commit", map[string]string{"BASE_SHA": evil}, "base_sha:"},

		{"a patch with no hash", with(patch, "PATCH_SHA256", ""), "patch_sha256:"},
		{"a patch with no tree", with(patch, "TREE", ""), "tree:"},
		{"a patch with a short tree", with(patch, "TREE", strings.Repeat("b", 12)), "tree:"},
		{"a patch inline and as a gist", with(patch, "GIST_ID", strings.Repeat("c", 32)), "patch_b64 and gist_id:"},
		{"a gist id with the wrong characters", with(with(patch, "HAS_PATCH_B64", "false"), "GIST_ID", evil), "gist_id:"},
		{"a hash without a patch", map[string]string{"PATCH_SHA256": strings.Repeat("a", 64)}, "patch_sha256 and tree:"},
		{"a tree without a patch", map[string]string{"TREE": strings.Repeat("b", 40)}, "patch_sha256 and tree:"},

		{"configs that are not JSON", map[string]string{"CONFIGS": evil}, "configs:"},
		{"one configuration", map[string]string{"CONFIGS": `[` + control + `]`}, "configs:"},
		{"five configurations", map[string]string{"CONFIGS": `[` + strings.Repeat(control+",", 4) + control + `]`}, "configs:"},
		{"a configuration with a key more", cfgs(`{"name":"b","build_flags":"","plan_flags":"","x":""}`), "configs:"},
		{"a configuration with a key fewer", cfgs(`{"name":"b","build_flags":""}`), "configs:"},
		{"a configuration whose flags are not a string", cfgs(`{"name":"b","build_flags":1,"plan_flags":""}`), "configs:"},
		{"two configurations of one name", cfgs(`{"name":"control","build_flags":"-disable-delete-only","plan_flags":""}`), "configs: two configurations have one name"},
		{"a name with capitals", cfgs(`{"name":"B","build_flags":"","plan_flags":""}`), "configs: every name"},
		{"an input as a name", cfgs(`{"name":"` + evil + `","build_flags":"","plan_flags":""}`), "configs: every name"},
		{"a variant that is the control", cfgs(`{"name":"b","build_flags":"-profile-windows","plan_flags":""}`), "configs: a variant has the control's flags"},

		{"a flag that is not on the list", variant("-profile-windows -sync"), "build_flags:"},
		{"an input as a flag", variant(evil), "build_flags:"},
		{"a flag given twice", variant("-disable-delete-only -disable-delete-only"), "build_flags: a flag is given twice"},
		{"a base target above the bound", variant("-lbase-target-mb=2000"), "build_flags: -lbase-target-mb"},
		{"a base target of zero", variant("-lbase-target-mb=0"), "build_flags: -lbase-target-mb"},
		{"a target list of eight sizes", variant("-target-sizes-mb=1,2,3,4,5,6,7,8"), "build_flags: -target-sizes-mb"},
		{"a target size above the bound", variant("-target-sizes-mb=64,5000"), "build_flags: every size of -target-sizes-mb"},
		{"both table shapes", variant("-lbase-target-mb=256 -target-sizes-mb=64,128"), "build_flags: -lbase-target-mb and -target-sizes-mb"},
		{"a read-ahead above the bound", variant("-read-ahead=65"), "build_flags: -read-ahead"},
		{"a read-ahead of a word", variant("-read-ahead=x"), "build_flags: -read-ahead"},
		{"a plan flag among the build flags", variant("-cache-mb=2048"), "build_flags:"},
		{"seven words", variant("-profile-windows -disable-delete-only -read-ahead=1 -lbase-target-mb=1 -x -y -z"), "build_flags:"},
		{"a build flag among the plan flags", cfgs(`{"name":"b","build_flags":"","plan_flags":"-read-ahead=8"}`), "plan_flags:"},
		{"a cache of zero", cfgs(`{"name":"b","build_flags":"","plan_flags":"-cache-mb=0"}`), "plan_flags: -cache-mb"},
		{"an input as a plan flag", cfgs(`{"name":"b","build_flags":"","plan_flags":"` + evil + `"}`), "plan_flags:"},
		{"profiling a synchronous retention", map[string]string{"RETENTION_MODE": "sync", "PACE": "0"}, "build_flags: -profile-windows is for a background build"},
		{"profiling a candidate not through the root store", map[string]string{"CANDIDATE": "L/k64a2l5m", "RETENTION_MODE": "sync", "PACE": "0", "CONFIGS": `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-profile-windows","plan_flags":""}]`}, "build_flags: -profile-windows"},
		{"a read-ahead of a candidate not through the root store", map[string]string{"CANDIDATE": "L/off", "RETENTION_MODE": "sync", "PACE": "0", "CONFIGS": `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-read-ahead=4","plan_flags":""}]`}, "build_flags: -read-ahead"},

		{"a window of 30 days", map[string]string{"WINDOW": "30"}, "window:"},
		{"an input as the window", map[string]string{"WINDOW": evil}, "window:"},
		{"pins from a longer window", map[string]string{"WINDOW": "2", "PINS_WINDOW": "7"}, "pins_window:"},
		{"an input as the pins window", map[string]string{"PINS_WINDOW": evil}, "pins_window:"},
		{"a family", map[string]string{"FAMILY": evil}, "family:"},
		{"two candidates", map[string]string{"CANDIDATE": "Lroot/off,Lroot/k64a2l1ns"}, "candidate:"},
		{"an input as the candidate", map[string]string{"CANDIDATE": evil}, "candidate:"},
		{"a background build not through the root store", map[string]string{"CANDIDATE": "L/off", "CONFIGS": `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-disable-delete-only","plan_flags":""}]`}, "candidate: a background build"},
		{"a retention mode", map[string]string{"RETENTION_MODE": evil}, "retention_mode:"},
		{"a threshold off the ladder", map[string]string{"RETAIN_FLUSH_DELS": "20000"}, "retain_flush_dels:"},
		{"a threshold with sync", map[string]string{"RETENTION_MODE": "sync", "PACE": "0", "RETAIN_FLUSH_DELS": "16384", "CONFIGS": `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-disable-delete-only","plan_flags":""}]`}, "retain_flush_dels: a threshold needs"},
		{"sync", map[string]string{"SYNC": evil}, "sync:"},
		{"the metrics interval", map[string]string{"METRICS_EVERY": evil}, "metrics_every:"},
		{"the memory limit", map[string]string{"GO_MEM_LIMIT": evil}, "go_mem_limit:"},
		{"a pace of letters", map[string]string{"PACE": evil}, "pace:"},
		{"a pace above the bound", map[string]string{"PACE": "30000"}, "pace:"},
		{"a pace with sync retention", map[string]string{"RETENTION_MODE": "sync", "CONFIGS": `[{"name":"a","build_flags":"","plan_flags":""},{"name":"b","build_flags":"-disable-delete-only","plan_flags":""}]`}, "pace: a pace needs"},
		{"one repetition", map[string]string{"REPS": "1"}, "reps:"},
		{"four repetitions", map[string]string{"REPS": "4"}, "reps:"},
		{"an input as the repetitions", map[string]string{"REPS": evil}, "reps:"},
		{"no parallelism", map[string]string{"MAX_PARALLEL": "0"}, "max_parallel:"},
		{"an input as the parallelism", map[string]string{"MAX_PARALLEL": evil}, "max_parallel:"},

		{"macOS without the memory confirmation", map[string]string{"RUNNERS": "macos-15"}, "runners: macos-15 has 7 GB"},
		{"a runner that is not offered", map[string]string{"RUNNERS": "ubuntu-latest"}, "runners: every one"},
		{"an input as a runner", map[string]string{"RUNNERS": evil}, "runners: every one"},
		{"a runner twice", map[string]string{"RUNNERS": "ubuntu-24.04,ubuntu-24.04"}, "runners: a runner is named twice"},
		{"no runner", map[string]string{"RUNNERS": ""}, "runners:"},
		{"the macOS confirmation", map[string]string{"MACOS_LOW_RSS": evil}, "macos_low_rss:"},
	} {
		env := diagDispatch(r)
		for k, v := range c.set {
			env[k] = v
		}
		got := runDiag(t, r, env)
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

// The script passes shellcheck where it is installed.
func TestTheDiagDispatchCheckPassesShellcheck(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("shellcheck"); err != nil {
		t.Skip("shellcheck is not installed")
	}
	out, err := exec.CommandContext(t.Context(), "shellcheck", "diag-dispatch-check.sh").CombinedOutput()
	if err != nil {
		t.Errorf("shellcheck: %v\n%s", err, out)
	}
}
