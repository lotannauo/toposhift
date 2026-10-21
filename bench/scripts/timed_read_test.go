package scripts_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// A dispatch with the read_timed input reads every repetition's build and times the
// reads, with plans made at the product's block cache. These tests read the workflow
// and fail if a repetition after the first stops reading, if the build step's limit
// stops leaving room for the read, if the read is not told to time, or if the pins and
// the plans are not made at one cache. The two steps that carry the flag are also run,
// with a stub for the program, once with the input and once without.

type timedStep struct {
	Name           string         `yaml:"name"`
	If             string         `yaml:"if"`
	TimeoutMinutes any            `yaml:"timeout-minutes"`
	Env            map[string]any `yaml:"env"`
	Run            string         `yaml:"run"`
}

type timedJob struct {
	TimeoutMinutes any            `yaml:"timeout-minutes"`
	Outputs        map[string]any `yaml:"outputs"`
	Steps          []timedStep    `yaml:"steps"`
}

type timedInput struct {
	Type    string `yaml:"type"`
	Default any    `yaml:"default"`
}

type timedWorkflow struct {
	On struct {
		WorkflowDispatch struct {
			Inputs map[string]timedInput `yaml:"inputs"`
		} `yaml:"workflow_dispatch"`
	} `yaml:"on"`
	Jobs map[string]timedJob `yaml:"jobs"`
}

func loadTimedWorkflow(t *testing.T) timedWorkflow {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "bench.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf timedWorkflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("bench.yml: %v", err)
	}
	return wf
}

func (j timedJob) step(t *testing.T, name string) timedStep {
	t.Helper()
	var found []timedStep
	for _, s := range j.Steps {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d steps named %q, want 1", len(found), name)
	}
	return found[0]
}

// minutes is a timeout given as a number, or fails the test.
func minutes(t *testing.T, what string, v any) int {
	t.Helper()
	n, ok := v.(int)
	if !ok {
		t.Fatalf("%s is %v (%T), want a number of minutes", what, v, v)
	}
	return n
}

const (
	readStepName   = "Read the candidate"
	buildStepName  = "Build the candidate"
	plansStepName  = "Make the pins and the plans"
	readCondition  = "needs.setup.outputs.read == 'true' && (matrix.rep == 1 || needs.setup.outputs.read_timed == 'true')"
	buildTimeoutRE = `^\$\{\{ needs\.setup\.outputs\.read == 'true' && \(matrix\.rep == 1 \|\| needs\.setup\.outputs\.read_timed == 'true'\) && (\d+) \|\| (\d+) \}\}$`
)

func TestTheReadTimedInputReachesTheJobsThroughTheSetup(t *testing.T) {
	t.Parallel()
	wf := loadTimedWorkflow(t)

	in, ok := wf.On.WorkflowDispatch.Inputs["read_timed"]
	if !ok || in.Type != "boolean" || in.Default != false {
		t.Errorf("the input read_timed is %+v (present %v), want a boolean that defaults to false", in, ok)
	}
	setup := wf.Jobs["setup"]
	if got := setup.step(t, "Check the inputs and plan the jobs").Env["READ_TIMED"]; got != "${{ inputs.read_timed }}" {
		t.Errorf("the setup checks READ_TIMED = %v, want the input", got)
	}
	for _, out := range []string{"read_timed", "plan_flags"} {
		if want := "${{ steps.matrix.outputs." + out + " }}"; setup.Outputs[out] != want {
			t.Errorf("the setup output %s is %v, want %s", out, setup.Outputs[out], want)
		}
	}
}

// Every repetition reads when the reads are timed, and the repetitions after the first
// keep the shorter build limit the read needs. The limits leave the rest of the job its
// time: the build, the read and the steps around them fit in the job's.
func TestEveryRepetitionReadsWhenTheReadsAreTimed(t *testing.T) {
	t.Parallel()
	wf := loadTimedWorkflow(t)
	build := wf.Jobs["build"]

	read := build.step(t, readStepName)
	if read.If != readCondition {
		t.Errorf("the read step runs if %q, want %q: a repetition after the first must read when the reads are timed", read.If, readCondition)
	}
	if got := read.Env["READ_TIMED"]; got != "${{ needs.setup.outputs.read_timed }}" {
		t.Errorf("the read step's READ_TIMED = %v, want the setup's output", got)
	}

	limit, _ := build.step(t, buildStepName).TimeoutMinutes.(string)
	m := regexp.MustCompile(buildTimeoutRE).FindStringSubmatch(limit)
	if m == nil {
		t.Fatalf("the build step's limit %q does not give the short limit to every repetition that reads (repetition 1, or all when timed) and the long one to the rest", limit)
	}
	short, long := atoi(t, m[1]), atoi(t, m[2])
	jobLimit := minutes(t, "the build job's timeout", build.TimeoutMinutes)
	readLimit := minutes(t, "the read step's timeout", read.TimeoutMinutes)
	const otherSteps = 15 // the checkout, Go, the plan, the sampler and the collection
	if short+readLimit+otherSteps > jobLimit {
		t.Errorf("a build of %d minutes, a read of %d and %d for the other steps do not fit in the job's %d", short, readLimit, otherSteps, jobLimit)
	}
	if long+otherSteps > jobLimit {
		t.Errorf("a build of %d minutes and %d for the other steps do not fit in the job's %d", long, otherSteps, jobLimit)
	}
	if short >= long {
		t.Errorf("the limit of a build that reads (%d) is not shorter than the one that does not (%d)", short, long)
	}
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// stubbedStep runs the script of a step of the workflow with a program that records its
// arguments, one invocation to a line, in place of spikebench. GNU time is dropped from
// the script, since the machine this runs on may not have it, and so is the check that
// it exists; the rest is the runner's: bash -eo pipefail. It returns the arguments and
// the exit status.
func stubbedStep(t *testing.T, s timedStep, env map[string]string, binary string) (calls []string, code int) {
	t.Helper()
	dir := t.TempDir()
	runner := filepath.Join(dir, "runner")
	if err := os.MkdirAll(runner, 0o755); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, "calls")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + record + "\n"
	if err := os.WriteFile(filepath.Join(runner, binary), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	// The stub's name is the one the step runs: spikebench, or rocksbench.
	timeCmd := regexp.MustCompile(`/usr/bin/time -v -o "[^"]*" \\\n`)
	if n := len(timeCmd.FindAllString(s.Run, -1)); n == 0 {
		t.Fatalf("step %q has no /usr/bin/time -v line, so this test cannot drop it", s.Name)
	}
	script := timeCmd.ReplaceAllString(s.Run, "env \\\n")
	scriptFile := filepath.Join(dir, "step.sh")
	if err := os.WriteFile(scriptFile, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(t.Context(), "bash", "--noprofile", "--norc", "-e", "-o", "pipefail", scriptFile)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "RUNNER_TEMP=" + runner}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := os.MkdirAll(filepath.Join(runner, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	t.Logf("step %q, exit %d:\n%s", s.Name, code, out)
	if b, err := os.ReadFile(record); err == nil {
		calls = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	return calls, code
}

// The read step passes -timed exactly when the setup says the reads are timed, and a
// timed read of a RocksDB candidate (which the setup has refused already) ends the step
// before the program runs.
func TestTheReadStepTimesTheReadOnlyWhenTheReadsAreTimed(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	read := loadTimedWorkflow(t).Jobs["build"].step(t, readStepName)
	env := func(timed, engine string) map[string]string {
		return map[string]string{"WINDOW": "7", "CANDIDATE": "L/off", "ENGINE": engine, "GO_MEM_LIMIT": "", "READ_TIMED": timed}
	}

	calls, code := stubbedStep(t, read, env("true", "pebble"), "spikebench")
	if code != 0 || len(calls) != 1 || !strings.HasPrefix(calls[0], "read -out ") || !strings.HasSuffix(calls[0], " -candidate L/off -timed") {
		t.Errorf("a timed read: exit %d, calls %q, want one `read ... -candidate L/off -timed`", code, calls)
	}

	calls, code = stubbedStep(t, read, env("false", "pebble"), "spikebench")
	if code != 0 || len(calls) != 1 || !strings.HasPrefix(calls[0], "read -out ") || !strings.HasSuffix(calls[0], " -candidate L/off") {
		t.Errorf("a read that is not timed: exit %d, calls %q, want one `read ... -candidate L/off`", code, calls)
	}

	calls, code = stubbedStep(t, read, env("true", "rocksdb"), "rocksbench")
	if code == 0 || len(calls) != 0 {
		t.Errorf("a timed read of a RocksDB candidate: exit %d, calls %q, want a failure before the program runs", code, calls)
	}

	// A RocksDB read that is not timed is the read it was.
	calls, code = stubbedStep(t, read, env("false", "rocksdb"), "rocksbench")
	if code != 0 || len(calls) != 1 || strings.Contains(calls[0], "-timed") {
		t.Errorf("a RocksDB read that is not timed: exit %d, calls %q", code, calls)
	}
}

// The pins and every plan are made with the plan flags the setup gives, so that the
// pins' scenario (which names the cache) is the plans'.
func TestThePinsAndEveryPlanTakeThePlanFlags(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	plans := loadTimedWorkflow(t).Jobs["plans"].step(t, plansStepName)
	if got := plans.Env["PLAN_FLAGS"]; got != "${{ needs.setup.outputs.plan_flags }}" {
		t.Errorf("the plans step's PLAN_FLAGS = %v, want the setup's plan_flags output", got)
	}
	env := func(flags string) map[string]string {
		return map[string]string{"FAMILY_FLAGS": "-run-max-age 30m", "PLAN_FLAGS": flags, "PINS_WINDOW": "2", "WINDOWS": "2,7"}
	}

	calls, code := stubbedStep(t, plans, env("-cache-mb 256"), "spikebench")
	if code != 0 || len(calls) != 3 {
		t.Fatalf("exit %d, calls %q, want the pins and two plans", code, calls)
	}
	for i, kind := range []string{"pins", "plan", "plan"} {
		if !strings.HasPrefix(calls[i], kind+" ") || strings.Count(calls[i], " -cache-mb 256 ") != 1 || !strings.Contains(calls[i], " -run-max-age 30m ") {
			t.Errorf("call %d, %q: want %s with the family's flags and -cache-mb 256 once", i, calls[i], kind)
		}
	}

	calls, code = stubbedStep(t, plans, env(""), "spikebench")
	if code != 0 || len(calls) != 3 {
		t.Fatalf("without plan flags: exit %d, calls %q, want the pins and two plans", code, calls)
	}
	for i, call := range calls {
		if strings.Contains(call, "-cache-mb") {
			t.Errorf("call %d, %q: names a cache without plan flags", i, call)
		}
	}
}
