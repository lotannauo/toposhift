package xtengine

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	scriptPath = "../scripts/xt-server.sh"
	configPath = "../scripts/xtdb.yaml"
)

// nodeConfig reads xtdb.yaml as far as the file goes: top-level keys, with their value if
// they have one, and the keys indented under them. The file is the one source of the node's
// settings, so a key that is not meant to be there, or a value that has moved, is a change
// of what is measured.
func nodeConfig(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(configPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	out := make(map[string]string)
	top := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if trimmed := strings.TrimSpace(line); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			t.Fatalf("%s: %q is not a key", configPath, line)
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		if !strings.HasPrefix(line, " ") {
			top = key
			out[key] = value
			continue
		}
		if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") || top == "" {
			t.Fatalf("%s: %q is not indented by two spaces under a key", configPath, line)
		}
		out[top+"."+key] = value
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTheNodeConfigurationIsWhatTheBaselineRegisters(t *testing.T) {
	want := map[string]string{
		"server":                   "",
		"server.host":              "*",
		"server.port":              "5432",
		"healthz":                  "",
		"healthz.host":             "*",
		"healthz.port":             "8080",
		"log":                      "!Local",
		"log.path":                 "/var/lib/xtdb/log",
		"storage":                  "!Local",
		"storage.path":             "/var/lib/xtdb/storage",
		"memoryCache":              "",
		"memoryCache.maxSizeBytes": "2147483648",
	}
	got := nodeConfig(t)
	for k, w := range want {
		if g, ok := got[k]; !ok || g != w {
			t.Errorf("%s is %q (present %v), want %q", k, g, ok, w)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s is set, and the registered configuration does not have it", k)
		}
	}
}

func TestTheScriptPinsTheImageAndTheJVMOptions(t *testing.T) {
	b, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^image='([^']+)'$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("the script has no image line")
	}
	if got, want := string(m[1]), "ghcr.io/xtdb/xtdb:2.1.0@sha256:15b6aa9d37d1e1a4e21748788db9275a57b87c339e5eb66d86376cf1e6899816"; got != want {
		t.Errorf("the image is %s, want %s", got, want)
	}
	m = regexp.MustCompile(`(?m)^registered_jvm_options='([^']+)'$`).FindSubmatch(b)
	if m == nil || string(m[1]) != "-Xmx4g -XX:MaxDirectMemorySize=4g" {
		t.Errorf("the JVM options are %q", m)
	}
	if !bytes.Contains(b, []byte(`-p "127.0.0.1:$port:5432"`)) || !bytes.Contains(b, []byte(`-p "127.0.0.1:$health_port:8080"`)) {
		t.Error("the node is published somewhere other than loopback")
	}
}

// stubs are the commands the script runs, replaced by scripts that write down how they
// were called and answer as the environment says, so that no test starts a container: the
// script is run with a PATH of one directory, which holds the stubs and links to the few
// system tools the script runs, and nothing else.
const stubDocker = `#!/bin/sh
echo "docker $*" >> "$STUB_LOG"
case "$1 $2" in
  "image inspect") [ -z "$STUB_NO_IMAGE" ] ;;
  "container inspect")
    # The container is there if the environment says so, or once it has been run.
    if [ -z "$STUB_EXISTS" ] && [ ! -e "$STUB_LOG.ran" ]; then exit 1; fi
    case "$*" in *State.Running*) echo "${STUB_RUNNING:-true}" ;; esac ;;
  "run "*) touch "$STUB_LOG.ran" ;;
  *) exit 0 ;;
esac
`

const stubCurl = `#!/bin/sh
echo "curl $*" >> "$STUB_LOG"
[ -z "$STUB_CURL_FAILS" ]
`

const stubPsql = `#!/bin/sh
echo "psql $*" >> "$STUB_LOG"
`

// systemTools are the commands the script and the stubs run that are not stubbed. The directory the
// script is run with holds links to these and to nothing else, so that a tool the script
// looks for and the test means to be absent (psql) is absent whatever the machine has
// installed: a runner image has a psql, a laptop may not. git is optional: the script
// only asks about a work tree when it finds one.
var systemTools = []string{"dirname", "mkdir", "chmod", "sleep", "touch", "git"}

// harness is how the script is run. The zero value is the hermetic one. leak is a
// directory put on the PATH after the tools, which is what a PATH that was not built would
// do: it exists so that a test can show that the checks notice the leak.
type harness struct{ leak string }

// scriptRun is what one run of the script did.
type scriptRun struct {
	stderr string
	code   int
	calls  []string // the stubs' calls, in order
}

func (r scriptRun) called(prefix string) []string {
	var out []string
	for _, c := range r.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// missingTool is what a test does when a tool it needs is not on the machine: on a runner it
// fails, because a skip reads as a pass and the runner is where the tests are meant to
// run; elsewhere it skips.
func missingTool(t testing.TB, tool string) {
	t.Helper()
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Fatalf("no %s on the runner", tool)
	}
	t.Skipf("no %s", tool)
}

// run runs the script at path, with the stubs, in an environment, and returns what it did.
// withPsql puts a stub psql on the path.
func (h harness) run(t testing.TB, path string, env []string, withPsql bool, args ...string) scriptRun {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		missingTool(t, "bash")
	}
	dir := t.TempDir()
	for _, tool := range systemTools {
		target, err := exec.LookPath(tool)
		if err != nil {
			if tool == "git" {
				continue
			}
			missingTool(t, tool)
		}
		if err := os.Symlink(target, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	log := filepath.Join(dir, "calls")
	for name, body := range map[string]string{"docker": stubDocker, "curl": stubCurl} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if withPsql {
		if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stubPsql), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{path}, args...)...)
	// The one directory: the real docker, curl and psql are never found.
	scriptPATH := dir
	if h.leak != "" {
		scriptPATH += string(os.PathListSeparator) + h.leak
	}
	cmd.Env = append([]string{"PATH=" + scriptPATH, "HOME=" + dir, "STUB_LOG=" + log}, env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	run := scriptRun{stderr: stderr.String()}
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		run.code = ee.ExitCode()
	default:
		t.Fatal(err)
	}
	if b, err := os.ReadFile(log); err == nil {
		run.calls = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	return run
}

// scriptChecks is what the script must do, as a check that reports what it finds, so
// that it can be shown to fail on a script with one of its guards taken out.
func scriptChecks(t testing.TB, path string) {
	t.Helper()
	checkScript(t, path, harness{})
}

func checkScript(t testing.TB, path string, h harness) {
	t.Helper()
	data := t.TempDir() + "/node"
	env := []string{"TOPOSHIFT_XTDB_DATA=" + data, "TOPOSHIFT_XTDB_PORT=55999", "TOPOSHIFT_XTDB_HEALTH_PORT=55998", "TOPOSHIFT_XTDB_NAME=n"}

	// What is refused, before anything is started.
	for _, c := range []struct {
		name string
		args []string
		env  []string
		want string
	}{
		{"no command", nil, nil, "usage"},
		{"an unknown command", []string{"restart"}, nil, "usage"},
		{"start with arguments", []string{"start", "now"}, nil, "start takes no arguments"},
		{"stop with arguments", []string{"stop", "now"}, nil, "stop takes no arguments"},
		{"a relative data directory", []string{"start"}, []string{"TOPOSHIFT_XTDB_DATA=relative/dir"}, "must be an absolute path"},
		{"a data directory in a work tree", []string{"start"}, []string{"TOPOSHIFT_XTDB_DATA=" + workTree(t) + "/node"}, "inside a git work tree"},
		{"a container that is already there", []string{"start"}, []string{"STUB_EXISTS=1"}, "already exists"},
		{"a node that exits while starting", []string{"start"}, []string{"STUB_CURL_FAILS=1", "STUB_RUNNING=false"}, "exited while starting"},
	} {
		run := h.run(t, path, append(append([]string{}, env...), c.env...), false, c.args...)
		if run.code == 0 || !strings.Contains(run.stderr, c.want) {
			t.Errorf("%s: exit %d, %q; want a failure saying %q", c.name, run.code, run.stderr, c.want)
		}
		// Only a node that was started and then did not come up has had a container run.
		if started := len(run.called("docker run")) != 0; started != (c.name == "a node that exits while starting") {
			t.Errorf("%s: a container was run: %v", c.name, started)
		}
	}

	// A start: the image is there, the container is run with the pinned image, the
	// configuration and the JVM options, published on loopback, over the data directory,
	// and the script waits for the node to say it started.
	run := h.run(t, path, env, false, "start")
	if run.code != 0 {
		t.Fatalf("a start failed: %d %q", run.code, run.stderr)
	}
	runs := run.called("docker run")
	if len(runs) != 1 {
		t.Fatalf("%d containers were run: %v", len(runs), run.calls)
	}
	for _, want := range []string{
		"--name n", "-e JDK_JAVA_OPTIONS=-Xmx4g -XX:MaxDirectMemorySize=4g", "-v " + data + ":/var/lib/xtdb",
		":/config/xtdb.yaml:ro", "-p 127.0.0.1:55999:5432", "-p 127.0.0.1:55998:8080",
		"ghcr.io/xtdb/xtdb:2.1.0@sha256:15b6aa9d37d1e1a4e21748788db9275a57b87c339e5eb66d86376cf1e6899816 -f /config/xtdb.yaml",
	} {
		if !strings.Contains(runs[0], want) {
			t.Errorf("the container was run as %q, which lacks %q", runs[0], want)
		}
	}
	if len(run.called("docker pull")) != 0 {
		t.Error("an image that is there was pulled")
	}
	if got := run.called("curl"); len(got) == 0 || !strings.Contains(got[0], "http://127.0.0.1:55998/healthz/started") {
		t.Errorf("the script did not ask the node whether it started: %v", got)
	}
	if fi, err := os.Stat(data); err != nil || fi.Mode().Perm() != 0o777 {
		t.Errorf("the data directory: %v, %v", fi, err)
	}
	// A run for probes only: the JVM options and the configuration can be given, and the
	// script says that it is not the registered configuration.
	other := filepath.Join(t.TempDir(), "other.yaml")
	if err := os.WriteFile(other, []byte("log: !Local\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run = h.run(t, path, append([]string{"TOPOSHIFT_XTDB_JAVA_OPTS=-Xmx2g -XX:MaxDirectMemorySize=2g", "TOPOSHIFT_XTDB_CONFIG=" + other}, env...), false, "start")
	if got := run.called("docker run"); run.code != 0 || len(got) != 1 ||
		!strings.Contains(got[0], "-e JDK_JAVA_OPTIONS=-Xmx2g -XX:MaxDirectMemorySize=2g") || !strings.Contains(got[0], "-v "+other+":/config/xtdb.yaml:ro") {
		t.Errorf("a start with other options and configuration: exit %d, %v", run.code, run.calls)
	}
	if !strings.Contains(run.stderr, "NOT the registered configuration") {
		t.Errorf("a start with other options did not say so: %q", run.stderr)
	}
	if strings.Contains(h.run(t, path, env, false, "start").stderr, "NOT the registered") {
		t.Error("a start with the registered configuration said it was not")
	}
	// An image that is not there is pulled, by its digest.
	run = h.run(t, path, append([]string{"STUB_NO_IMAGE=1"}, env...), false, "start")
	if pulls := run.called("docker pull"); len(pulls) != 1 || !strings.HasSuffix(pulls[0], "@sha256:15b6aa9d37d1e1a4e21748788db9275a57b87c339e5eb66d86376cf1e6899816") {
		t.Errorf("pulls: %v", pulls)
	}

	// A stop: the container is stopped, with time to finish a block, and removed; with no
	// container there is nothing to do.
	run = h.run(t, path, append([]string{"STUB_EXISTS=1"}, env...), false, "stop")
	if run.code != 0 || !slices.Contains(run.calls, "docker stop --time 120 n") || !slices.Contains(run.calls, "docker rm n") {
		t.Errorf("a stop: exit %d, %v", run.code, run.calls)
	}
	run = h.run(t, path, env, false, "stop")
	if run.code != 0 || len(run.called("docker stop")) != 0 || len(run.called("docker rm")) != 0 {
		t.Errorf("a stop with no container: exit %d, %v", run.code, run.calls)
	}

	// psql is the host's, against the node on loopback, and stops at the first error.
	run = h.run(t, path, env, true, "psql", "-c", "select 1")
	if got := run.called("psql"); run.code != 0 || len(got) != 1 || got[0] != "psql --set ON_ERROR_STOP=1 host=127.0.0.1 port=55999 user=xtdb dbname=xtdb -c select 1" {
		t.Errorf("psql: exit %d, %v", run.code, run.calls)
	}
	run = h.run(t, path, env, false, "psql")
	if run.code == 0 || !strings.Contains(run.stderr, "psql is not installed") {
		t.Errorf("psql with none installed: exit %d, %q", run.code, run.stderr)
	}
}

// workTree makes a git work tree to put a data directory in, which a script that does not
// refuse it would make the directory in.
func workTree(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := exec.CommandContext(t.Context(), "git", "-C", dir, "init", "--quiet").CombinedOutput(); err != nil {
		t.Skipf("git init: %v: %s", err, out)
	}
	return dir
}

func TestTheScriptDoesWhatItSays(t *testing.T) {
	for _, tool := range []string{"git"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("no %s", tool)
		}
	}
	scriptChecks(t, scriptPath)
}
