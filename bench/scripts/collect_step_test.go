package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// The step of the bench workflow that keeps a build job's manifest, job file and logs runs
// whatever happened before it, under the shell GitHub gives a step of shell: bash, which is
// bash --noprofile --norc -eo pipefail. A job may leave any file the step reads unwritten:
// a repetition after the first has no read, a job killed before GNU time reported has no
// build report, and a job that failed before its build has no binary and no logs. The step
// must still write job.json, keep the logs the job had and write its summary. The test
// takes the step's script from the workflow and runs it as the runner does, in a
// temporary runner directory, once for each kind of job.

const collectStepName = "Collect the manifest and the logs"

type workflowStep struct {
	Name  string         `yaml:"name"`
	If    string         `yaml:"if"`
	Shell string         `yaml:"shell"`
	Env   map[string]any `yaml:"env"`
	Run   string         `yaml:"run"`
}

type workflowJob struct {
	Steps []workflowStep `yaml:"steps"`
}

type workflowRun struct {
	Shell string `yaml:"shell"`
}

type workflowDefaults struct {
	Run workflowRun `yaml:"run"`
}

type benchWorkflow struct {
	Defaults workflowDefaults       `yaml:"defaults"`
	Jobs     map[string]workflowJob `yaml:"jobs"`
}

// collectStep is the script of the collect step and the names of its env, after checking
// that the runner runs it as the test does: always, and in the workflow's default shell.
func collectStep(t *testing.T) (script string, envKeys []string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "bench.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf benchWorkflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("bench.yml: %v", err)
	}
	if wf.Defaults.Run.Shell != "bash" {
		t.Fatalf("the workflow's default shell is %q, not bash: GitHub runs a step of shell: bash as bash --noprofile --norc -eo pipefail {0}, "+
			"which is how this test runs the step, so it must be revisited for another shell", wf.Defaults.Run.Shell)
	}
	var found []workflowStep
	for _, step := range wf.Jobs["build"].Steps {
		if step.Name == collectStepName {
			found = append(found, step)
		}
	}
	if len(found) != 1 {
		t.Fatalf("the build job has %d steps named %q, want 1", len(found), collectStepName)
	}
	step := found[0]
	if step.If != "always()" {
		t.Fatalf("the step %q has the condition %q, want always(): it keeps the evidence of a job that failed", collectStepName, step.If)
	}
	if step.Shell != "" {
		t.Fatalf("the step %q sets its own shell %q, so it no longer runs as bash --noprofile --norc -eo pipefail {0}: revisit this test", collectStepName, step.Shell)
	}
	return step.Run, slices.Sorted(maps.Keys(step.Env))
}

// collectEnv is the env of the step for a Pebble job of repetition 2. The test pins its
// names against the step's, so that a variable the step gains is added here.
func collectEnv() map[string]string {
	pairs := [][2]string{
		{"ENGINE", "pebble"},
		{"ROCKSDB_PREFIX", ""},
		{"ROCKSDB_CACHE_KEY", ""},
		{"ROCKSDB_CACHE_HIT", ""},
		{"FAMILY", "rmae"},
		{"PINS_WINDOW", "2"},
		{"SYNC", "true"},
		{"METRICS_EVERY", "10s"},
		{"GO_MEM_LIMIT", "11GiB"},
		{"RETENTION_MODE", "background"},
		{"PACE", "4500"},
		{"RETAIN_FLUSH_DELS", "24576"},
		{"REF", "refs/heads/main"},
		{"EXPECTED_SHA", "0123456789abcdef0123456789abcdef01234567"},
		{"OS", "ubuntu-24.04"},
		{"WINDOW", "2"},
		{"CANDIDATE", "Lroot/off"},
		{"SLUG", "Lroot_off"},
		{"REP", "2"},
		{"SHA", "0123456789abcdef0123456789abcdef01234567"},
		{"RUN_ID", "100"},
		{"RUN_ATTEMPT", "1"},
	}
	env := map[string]string{}
	for _, kv := range pairs {
		env[kv[0]] = kv[1]
	}
	return env
}

// A collectCase is one kind of job: what it left in the runner directory and what the
// step must make of it.
type collectCase struct {
	name string

	// env overrides the step's env. {runner} stands for the runner's temp directory.
	env map[string]string

	// work is the files of the job's build directory, by name, and files the other files
	// under the runner's temp directory, by slash-separated path.
	work map[string]string

	files map[string]string

	// lscpuFails and dpkgFails make the stubs of those commands exit with status 1.
	lscpuFails bool

	dpkgFails bool

	// want is the fields of job.json that depend on the case, and summary the lines the
	// summary must contain.
	want map[string]any

	summary []string
}

func newCollectCase(name string) *collectCase {
	return &collectCase{name: name, env: map[string]string{}, work: map[string]string{}, files: map[string]string{}, want: map[string]any{}}
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

const collectManifest = "{\"candidate\":\"Lroot/off\"}\n"

const collectResults = "{\"answers\":[]}\n"

const collectMetrics = "{\"t\":1}\n{\"t\":2}\n"

const collectBuildTime = "\tCommand being timed: \"spikebench build\"\n" +
	"\tElapsed (wall clock) time (h:mm:ss or m:ss): 1:02.03\n" +
	"\tMaximum resident set size (kbytes): 123456\n"

const collectReadTime = "\tCommand being timed: \"spikebench read\"\n" +
	"\tElapsed (wall clock) time (h:mm:ss or m:ss): 0:03.00\n" +
	"\tMaximum resident set size (kbytes): 2048\n"

// Two samples of the root filesystem, as the sampler writes them: the used 1K-blocks
// grow from 1 GiB to 2 GiB.
const collectSystemLog = "2026-10-16T00:00:00Z\n== root\n" +
	"Filesystem     1024-blocks      Used Available Capacity Mounted on\n" +
	"/dev/root        100000000   1048576  90000000       2% /\n" +
	"2026-10-16T00:01:00Z\n== root\n" +
	"Filesystem     1024-blocks      Used Available Capacity Mounted on\n" +
	"/dev/root        100000000   2097152  90000000       3% /\n"

const collectPebbleBinary = "pebble binary\n"

const collectRocksBinary = "rocksdb binary\n"

const collectStamp = "rocksdb build stamp\n"

const collectRocksEntry = "rocksdb-v1-test"

const collectLibraries = "liblz4-dev:arm64=1.9.4 g++=4:13.2.0"

const collectLscpuStub = `#!/bin/sh
printf 'Architecture:          x86_64\nModel name:            Test CPU\n'
`

const collectDpkgStub = `#!/bin/sh
printf 'liblz4-dev:arm64\t1.9.4\ng++\t4:13.2.0\n'
`

const collectFailStub = "#!/bin/sh\nexit 1\n"

func collectCases() []*collectCase {
	var cases []*collectCase

	// A repetition after the first runs no read, so it has no read.time.txt: the
	// defect this test was written for.
	noRead := newCollectCase("a job with no read")
	noRead.work["manifest.json"] = collectManifest
	noRead.files["logs/time.txt"] = collectBuildTime
	noRead.files["logs/build.log"] = "built\n"
	noRead.files["logs/system.log"] = collectSystemLog
	noRead.files["spikebench"] = collectPebbleBinary
	noRead.want["max_rss_kb"] = 123456
	noRead.want["read_max_rss_kb"] = 0
	noRead.want["binary_sha256"] = sha256Hex(collectPebbleBinary)
	noRead.want["cpu_model"] = "Test CPU"
	noRead.want["rocksdb_stamp_sha256"] = ""
	noRead.want["rocksdb_libraries"] = ""
	noRead.summary = append(noRead.summary,
		"- engine: pebble",
		"- manifest written: yes; results of a read written: no",
		"- peak memory: 121 MiB; of the read: none",
		"- elapsed time (h:mm:ss or m:ss): 1:02.03",
		"(root, mnt and the work directory's filesystem): root 2.0 GiB",
		"- hardware: Test CPU, ",
		"- binary sha256: `"+sha256Hex(collectPebbleBinary)+"`",
	)
	cases = append(cases, noRead)

	read := newCollectCase("a job with a read")
	read.env["REP"] = "1"
	read.work["manifest.json"] = collectManifest
	read.work["results.json"] = collectResults
	read.work["metrics.jsonl"] = collectMetrics
	read.files["logs/time.txt"] = collectBuildTime
	read.files["logs/build.log"] = "built\n"
	read.files["logs/read.time.txt"] = collectReadTime
	read.files["logs/read.log"] = "read\n"
	read.files["logs/system.log"] = collectSystemLog
	read.files["spikebench"] = collectPebbleBinary
	read.want["max_rss_kb"] = 123456
	read.want["read_max_rss_kb"] = 2048
	read.want["binary_sha256"] = sha256Hex(collectPebbleBinary)
	read.summary = append(read.summary,
		"- manifest written: yes; results of a read written: yes",
		"- peak memory: 121 MiB; of the read: 2 MiB",
	)
	cases = append(cases, read)

	// Killed for memory, by a timeout or by a cancel: the binary ran and the sampler
	// wrote, but there is no build report and no manifest.
	killed := newCollectCase("a job killed before GNU time reported")
	killed.files["logs/build.log"] = "building\n"
	killed.files["logs/system.log"] = collectSystemLog
	killed.files["spikebench"] = collectPebbleBinary
	killed.want["max_rss_kb"] = 0
	killed.want["read_max_rss_kb"] = 0
	killed.want["binary_sha256"] = sha256Hex(collectPebbleBinary)
	killed.summary = append(killed.summary,
		"- manifest written: no; results of a read written: no",
		"- peak memory: unknown; of the read: none",
		"- elapsed time (h:mm:ss or m:ss): unknown",
		"(root, mnt and the work directory's filesystem): root 2.0 GiB",
	)
	cases = append(cases, killed)

	// Failed before the build directory, the binary or the sampler existed.
	early := newCollectCase("a job that failed before its build")
	early.want["max_rss_kb"] = 0
	early.want["read_max_rss_kb"] = 0
	early.want["binary_sha256"] = "unknown"
	early.summary = append(early.summary,
		"- manifest written: no; results of a read written: no",
		"- peak memory: unknown; of the read: none",
		"- elapsed time (h:mm:ss or m:ss): unknown",
		"(root, mnt and the work directory's filesystem): unknown",
		"- binary sha256: `unknown`",
	)
	cases = append(cases, early)

	rocks := newCollectCase("a RocksDB build")
	rocksEnv(rocks)
	rocks.env["ROCKSDB_CACHE_HIT"] = "true"
	rocks.work["manifest.json"] = collectManifest
	rocks.files["logs/time.txt"] = collectBuildTime
	rocks.files["logs/build.log"] = "built\n"
	rocks.files["logs/system.log"] = collectSystemLog
	rocks.files["rocksbench"] = collectRocksBinary
	rocks.files["rocks/lib/rocksdb-build.stamp"] = collectStamp
	rocks.want["max_rss_kb"] = 123456
	rocks.want["binary_sha256"] = sha256Hex(collectRocksBinary)
	rocks.want["rocksdb_stamp_sha256"] = sha256Hex(collectStamp)
	rocks.want["rocksdb_libraries"] = collectLibraries
	rocks.summary = append(rocks.summary,
		"- engine: rocksdb",
		"- RocksDB library: cache entry `"+collectRocksEntry+"` (restored: true), build stamp sha256 `"+sha256Hex(collectStamp)+"`",
		"- packages: "+collectLibraries,
	)
	cases = append(cases, rocks)

	// A RocksDB job whose library was not restored: it has no build stamp, no logs and no
	// build report, and dpkg-query finds a package missing.
	miss := newCollectCase("a RocksDB build without its library")
	rocksEnv(miss)
	miss.files["rocksbench"] = collectRocksBinary
	miss.dpkgFails = true
	miss.want["max_rss_kb"] = 0
	miss.want["binary_sha256"] = sha256Hex(collectRocksBinary)
	miss.want["rocksdb_stamp_sha256"] = ""
	miss.want["rocksdb_libraries"] = ""
	miss.summary = append(miss.summary,
		"- RocksDB library: cache entry `"+collectRocksEntry+"` (restored: no), build stamp sha256 `none`",
		"- packages: unknown",
	)
	cases = append(cases, miss)

	noLscpu := newCollectCase("a runner without lscpu")
	noLscpu.lscpuFails = true
	noLscpu.work["manifest.json"] = collectManifest
	noLscpu.files["logs/time.txt"] = collectBuildTime
	noLscpu.files["logs/system.log"] = collectSystemLog
	noLscpu.files["spikebench"] = collectPebbleBinary
	noLscpu.want["max_rss_kb"] = 123456
	noLscpu.want["cpu_model"] = "unknown"
	noLscpu.summary = append(noLscpu.summary, "- hardware: unknown, ")
	cases = append(cases, noLscpu)

	return cases
}

// rocksEnv makes c a job of the RocksDB candidate RL/off, whose library is under the
// runner's temp directory.
func rocksEnv(c *collectCase) {
	c.env["ENGINE"] = "rocksdb"
	c.env["CANDIDATE"] = "RL/off"
	c.env["SLUG"] = "RL_off"
	c.env["RETENTION_MODE"] = "sync"
	c.env["METRICS_EVERY"] = ""
	c.env["PACE"] = "0"
	c.env["RETAIN_FLUSH_DELS"] = "default"
	c.env["ROCKSDB_PREFIX"] = "{runner}/rocks"
	c.env["ROCKSDB_CACHE_KEY"] = collectRocksEntry
}

func TestTheCollectStepKeepsTheJobFileAndTheLogsOfEveryJob(t *testing.T) {
	t.Parallel()
	script, envKeys := collectStep(t)
	if want := slices.Sorted(maps.Keys(collectEnv())); !slices.Equal(envKeys, want) {
		t.Fatalf("the step's env is %v, but this test supplies %v: update collectEnv", envKeys, want)
	}
	for _, tool := range []string{"bash", "jq", "awk", "sha256sum", "cut", "paste", "tr"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed, so the collect step cannot be run", tool)
		}
	}
	for _, c := range collectCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			checkCollect(t, script, c)
		})
	}
}

// checkCollect runs the step for c as the runner does and checks everything it left.
func checkCollect(t *testing.T, script string, c *collectCase) {
	t.Helper()
	tmp := t.TempDir()
	runner := filepath.Join(tmp, "runner")
	stubs := filepath.Join(tmp, "stubs")
	summaryPath := filepath.Join(tmp, "summary")
	write := func(path, content string, mode os.FileMode) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
	}

	env := collectEnv()
	for k, v := range c.env {
		env[k] = strings.ReplaceAll(v, "{runner}", runner)
	}
	if err := os.MkdirAll(runner, 0o755); err != nil {
		t.Fatal(err)
	}
	workDir := filepath.Join(runner, "bench", "R"+env["WINDOW"], env["SLUG"])
	for name, content := range c.work {
		write(filepath.Join(workDir, name), content, 0o644)
	}
	for rel, content := range c.files {
		write(filepath.Join(runner, filepath.FromSlash(rel)), content, 0o644)
	}

	lscpu, dpkg := collectLscpuStub, collectDpkgStub
	if c.lscpuFails {
		lscpu = collectFailStub
	}
	if c.dpkgFails {
		dpkg = collectFailStub
	}
	write(filepath.Join(stubs, "lscpu"), lscpu, 0o755)
	write(filepath.Join(stubs, "dpkg-query"), dpkg, 0o755)
	scriptFile := filepath.Join(tmp, "collect.sh")
	write(scriptFile, script, 0o644)

	// The real HOME, never a temporary one: where jq is a version manager's shim, a
	// fresh HOME makes the shim install it.
	cmd := exec.CommandContext(t.Context(), "bash", "--noprofile", "--norc", "-e", "-o", "pipefail", scriptFile)
	cmd.Dir = tmp
	cmd.Env = []string{
		"PATH=" + stubs + string(os.PathListSeparator) + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"RUNNER_TEMP=" + runner, "GITHUB_STEP_SUMMARY=" + summaryPath,
		"ImageOS=ubuntu24", "ImageVersion=20261001.1", "RUNNER_NAME=GitHub Actions 7",
	}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the step ended with %v, and a job's evidence must be kept whatever it left:\n%s", err, out)
	}

	// The job file is written and says what the job was.
	out := filepath.Join(runner, "out")
	data, err := os.ReadFile(filepath.Join(out, "job.json"))
	if err != nil {
		t.Fatalf("no job.json: %v", err)
	}
	var job map[string]any
	if err := json.Unmarshal(data, &job); err != nil {
		t.Fatalf("job.json is not JSON: %v\n%s", err, data)
	}
	want := map[string]any{"image_os": "ubuntu24", "image_version": "20261001.1", "runner_name": "GitHub Actions 7"}
	fromEnv := [][2]string{
		{"sha", "SHA"},
		{"os", "OS"},
		{"family", "FAMILY"},
		{"window", "WINDOW"},
		{"candidate", "CANDIDATE"},
		{"rep", "REP"},
		{"pins_window", "PINS_WINDOW"},
		{"sync", "SYNC"},
		{"metrics_every", "METRICS_EVERY"},
		{"go_mem_limit", "GO_MEM_LIMIT"},
		{"retention_mode", "RETENTION_MODE"},
		{"pace", "PACE"},
		{"retain_flush_dels", "RETAIN_FLUSH_DELS"},
		{"ref", "REF"},
		{"expected_sha", "EXPECTED_SHA"},
		{"run_id", "RUN_ID"},
		{"run_attempt", "RUN_ATTEMPT"},
		{"engine", "ENGINE"},
		{"rocksdb_cache_key", "ROCKSDB_CACHE_KEY"},
		{"rocksdb_cache_hit", "ROCKSDB_CACHE_HIT"},
	}
	for _, kv := range fromEnv {
		want[kv[0]] = env[kv[1]]
	}
	maps.Copy(want, c.want)
	for _, key := range []string{"arch", "kernel", "nproc", "mem_total_kb"} {
		if _, ok := job[key]; !ok {
			t.Errorf("job.json has no %s: %s", key, data)
		}
	}
	for key, w := range want {
		got, ok := job[key]
		if !ok {
			t.Errorf("job.json has no %s: %s", key, data)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(w) {
			t.Errorf("job.json %s = %v, want %v", key, got, w)
		}
	}

	// The manifest, results and metrics the job wrote are kept as they are, and nothing
	// else of the build directory.
	for _, name := range []string{"manifest.json", "results.json", "metrics.jsonl"} {
		got, err := os.ReadFile(filepath.Join(out, name))
		if w, ok := c.work[name]; ok {
			if err != nil || string(got) != w {
				t.Errorf("%s was not kept as the job wrote it (read error: %v)", name, err)
			}
		} else if err == nil {
			t.Errorf("%s was kept, but the job wrote none", name)
		}
	}

	// The logs the job had are kept, and a job with none has no logs directory.
	hasLogs := false
	for rel, content := range c.files {
		if !strings.HasPrefix(rel, "logs/") {
			continue
		}
		hasLogs = true
		got, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(rel)))
		if err != nil || string(got) != content {
			t.Errorf("%s was not kept as the job wrote it (read error: %v)", rel, err)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "logs")); (err == nil) != hasLogs {
		t.Errorf("the logs directory is kept: %v, want %v", err == nil, hasLogs)
	}

	// The summary is written to the end.
	summary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("no summary: %v", err)
	}
	header := "### " + env["CANDIDATE"] + ", " + env["WINDOW"] + " days, " + env["OS"] + ", repetition " + env["REP"] + "\n"
	if !strings.HasPrefix(string(summary), header) {
		t.Errorf("the summary does not start with %q:\n%s", header, summary)
	}
	for _, line := range c.summary {
		if !strings.Contains(string(summary), line) {
			t.Errorf("the summary lacks %q:\n%s", line, summary)
		}
	}
}
