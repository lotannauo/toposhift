package scripts_test

import (
	"maps"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// A bench dispatch's shape and serve_options inputs reach every build through the
// setup: the setup's check takes them from the inputs, writes them as outputs only when
// the dispatch names them, and the build step passes the shape's words and
// -serve-options to spikebench. These tests read the workflow, run the build step with a
// stub for the program, and run the setup's check and the build step one after the
// other, so that what the check lets through is what the build is given.

// The inputs of a dispatch: the fifteen there were, and the two that name a shape and
// serve's options. GitHub takes at most 25 inputs in a workflow_dispatch.
var benchInputs = []string{
	"arches", "candidates", "expected_sha", "family", "go_mem_limit", "max_parallel", "metrics_every", "pins_window",
	"read", "read_timed", "reps", "retain_flush_dels", "retention_mode", "serve_options", "shape", "sync", "windows",
}

func TestTheShapeAndServeOptionsReachTheBuildThroughTheSetup(t *testing.T) {
	t.Parallel()
	wf := loadTimedWorkflow(t)

	inputs := wf.On.WorkflowDispatch.Inputs
	if got := slices.Sorted(maps.Keys(inputs)); !slices.Equal(got, benchInputs) || len(got) > 25 {
		t.Errorf("the dispatch's inputs are %q, want %q (at most 25)", got, benchInputs)
	}
	if in := inputs["shape"]; in.Type != "string" || in.Default != "" {
		t.Errorf("the input shape is %+v, want a string that defaults to empty", in)
	}
	if in := inputs["serve_options"]; in.Type != "boolean" || in.Default != false {
		t.Errorf("the input serve_options is %+v, want a boolean that defaults to false", in)
	}

	setup := wf.Jobs["setup"]
	check := setup.step(t, "Check the inputs and plan the jobs")
	build := wf.Jobs["build"].step(t, buildStepName)
	for env, out := range map[string]string{"SHAPE": "shape", "SERVE_OPTIONS": "serve_options"} {
		if want := "${{ inputs." + out + " }}"; check.Env[env] != want {
			t.Errorf("the setup checks %s = %v, want %s", env, check.Env[env], want)
		}
		if want := "${{ steps.matrix.outputs." + out + " }}"; setup.Outputs[out] != want {
			t.Errorf("the setup output %s is %v, want %s", out, setup.Outputs[out], want)
		}
		if want := "${{ needs.setup.outputs." + out + " }}"; build.Env[env] != want {
			t.Errorf("the build step's %s = %v, want %s", env, build.Env[env], want)
		}
	}
}

// buildEnv is the env of the build step for repetition 1 of Lroot/off in a background
// dispatch at the threshold 24,576, with the shape and serve's options given.
func buildEnv(engine, shape, serve string) map[string]string {
	return map[string]string{
		"WINDOW": "7", "CANDIDATE": "Lroot/off", "ENGINE": engine, "SYNC": "true", "METRICS_EVERY": "10s",
		"GO_MEM_LIMIT": "11GiB", "PEBBLE_MEMORY_LIMIT": "11811160064", "RETENTION_MODE": "background", "PACE": "4500",
		"RETAIN_FLUSH_DELS": "24576", "SHAPE": shape, "SERVE_OPTIONS": serve,
	}
}

// The build step passes the shape's words, each as one argument, and -serve-options
// when the setup wrote them, and nothing more; without them the command is the one a
// dispatch ran before the inputs existed. rocksbench is never given either (the setup
// refuses a RocksDB candidate with them).
func TestTheBuildStepPassesTheShapeAndServeOptions(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	build := loadTimedWorkflow(t).Jobs["build"].step(t, buildStepName)
	const before = " -candidate Lroot/off -retention-mode background -metrics-every 10s -pace 4500 -retain-flush-dels 24576"
	const p5 = "-target-sizes-mb=64,4,8,64,128,256,512 -disable-delete-only"

	for _, c := range []struct{ shape, serve, want string }{
		{"", "", before + " -sync"},
		{"", "false", before + " -sync"},
		{p5, "", before + " " + p5 + " -sync"},
		{"", "true", before + " -serve-options -sync"},
		{p5 + " -compaction-concurrency=1,3 -compression=zstd", "true", before + " " + p5 + " -compaction-concurrency=1,3 -compression=zstd -serve-options -sync"},
	} {
		calls, code := stubbedStep(t, build, buildEnv("pebble", c.shape, c.serve), "spikebench")
		if code != 0 || len(calls) != 1 || !strings.HasPrefix(calls[0], "build -out ") || !strings.HasSuffix(calls[0], c.want) {
			t.Errorf("shape %q, serve's options %q: exit %d, calls %q, want one `build -out ...%s`", c.shape, c.serve, code, calls, c.want)
		}
	}

	calls, code := stubbedStep(t, build, buildEnv("rocksdb", p5, "true"), "rocksbench")
	if code != 0 || len(calls) != 1 || !strings.HasSuffix(calls[0], " -candidate Lroot/off -pebble-memory-limit 11811160064 -sync") {
		t.Errorf("a RocksDB build: exit %d, calls %q, want neither the shape nor serve's options", code, calls)
	}
}

// What the setup's check writes is what the build step passes: a background dispatch
// with a shape and serve's options, its outputs handed to the build step as GitHub hands
// them (an output the check did not write is empty), gives spikebench exactly the
// shape's words, as the dispatch gave them, and -serve-options; one without them gives
// neither.
func TestTheSetupsOutputsAreWhatTheBuildPasses(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	r := newRepo(t)
	build := loadTimedWorkflow(t).Jobs["build"].step(t, buildStepName)
	const shape = "-compression=balanced  -lbase-target-mb=4 -disable-delete-only"

	for _, c := range []struct{ shape, serve, want string }{
		{shape, "true", " -compression=balanced -lbase-target-mb=4 -disable-delete-only -serve-options -sync"},
		{"", "false", " -retain-flush-dels 24576 -sync"},
	} {
		env := dispatch(r)
		env["SHAPE"], env["SERVE_OPTIONS"] = c.shape, c.serve
		got := run(t, r, env)
		if got.code != 0 {
			t.Fatalf("shape %q: exit %d\n%s", c.shape, got.code, got.stderr)
		}
		outputs := map[string]string{}
		for line := range strings.SplitSeq(strings.TrimSuffix(got.output, "\n"), "\n") {
			k, v, _ := strings.Cut(line, "=")
			outputs[k] = v
		}
		benv := buildEnv("pebble", outputs["shape"], outputs["serve_options"])
		benv["RETENTION_MODE"], benv["PACE"], benv["RETAIN_FLUSH_DELS"] = outputs["retention_mode"], outputs["pace"], outputs["retain_flush_dels"]
		calls, code := stubbedStep(t, build, benv, "spikebench")
		if code != 0 || len(calls) != 1 || !strings.HasSuffix(calls[0], c.want) {
			t.Errorf("shape %q, serve's options %s: exit %d, calls %q, want one ending %q", c.shape, c.serve, code, calls, c.want)
			continue
		}
		if strings.Count(calls[0], "-serve-options") > 1 || (c.serve != "true" && strings.Contains(calls[0], "-serve-options")) {
			t.Errorf("shape %q, serve's options %s: the call is %q", c.shape, c.serve, calls[0])
		}
	}
}
