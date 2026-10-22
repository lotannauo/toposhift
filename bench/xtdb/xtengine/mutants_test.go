package xtengine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
)

// recorder is a testing.TB that notes a failure instead of failing, so that a check
// written as a test can be run against a deliberate fault and be seen to fail. Fatal ends
// the check, as in a test.
type recorder struct {
	testing.TB
	msgs []string
}

type stopCheck struct{}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}
func (r *recorder) Error(args ...any) { r.msgs = append(r.msgs, fmt.Sprint(args...)) }
func (r *recorder) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	panic(stopCheck{})
}

func (r *recorder) Fatal(args ...any) {
	r.Error(args...)
	panic(stopCheck{})
}
func (r *recorder) Cleanup(func()) {}

// failures runs a check and returns what it reported, as one error or nil. The test t
// provides what a check needs of one besides failing: a temporary directory, and the
// skip.
func failures(t testing.TB, check func(testing.TB)) (err error) {
	r := &recorder{TB: t}
	defer func() {
		switch p := recover(); p.(type) {
		case nil:
		case stopCheck:
		default:
			panic(p)
		}
		if len(r.msgs) > 0 {
			err = fmt.Errorf("%s", strings.Join(r.msgs, "; "))
		}
	}()
	check(r)
	return nil
}

// streamCheck runs a fixed workload with mid-stream retentions through an engine over a
// fake node, against the reference engine.
func streamCheck(mut mutation) error {
	cfgs := conformance.Configs()
	e, err := fakeFactory(mut)("a directory")
	if err != nil {
		return err
	}
	defer func() { _ = e.Close() }()
	return conformance.Check(e, cfgs[len(cfgs)-1], conformance.Options{RetainAt: []float64{0.3, 0.7}})
}

func errCheck(f func(node *fakeNode, e *Engine) error, mut mutation) error {
	node := newFakeNode()
	e := openFake(nopTB{}, node, mut)
	defer func() { _ = e.Close() }()
	return f(node, e)
}

// nopTB is the testing.TB openFake needs when no test is running: it fails by panicking.
type nopTB struct{ testing.TB }

func (nopTB) Helper() {}
func (nopTB) Fatal(args ...any) {
	panic(fmt.Sprint(args...))
}

// check is a check of the engine, to be run on a correct engine and on one with a fault.
type check struct {
	name string
	run  func(t testing.TB, mut mutation) error
}

var (
	streamC = check{"the conformance stream", func(_ testing.TB, m mutation) error { return streamCheck(m) }}
	readsC  = check{"the statements of the reads by token", func(t testing.TB, m mutation) error {
		return failures(t, func(t testing.TB) { checkTokenReads(t, m) })
	}}
	floorC = check{"the instant of a read", func(t testing.TB, m mutation) error {
		return failures(t, func(t testing.TB) { checkFloor(t, m) })
	}}
	otherC = check{"the statements of the other reads", func(t testing.TB, m mutation) error {
		return failures(t, func(t testing.TB) { checkOtherReads(t, m) })
	}}
	retainC = check{"a retention as a transaction of its own", func(_ testing.TB, m mutation) error {
		return errCheck(retainCheck, m)
	}}
	readBackC = check{"a transaction the node lost", func(_ testing.TB, m mutation) error {
		return errCheck(readBackCheck, m)
	}}
	deadlineC = check{"a commit that hangs", func(_ testing.TB, m mutation) error {
		return errCheck(deadlineCheck, m)
	}}
	payloadC = check{"the payload as hex text", func(_ testing.TB, m mutation) error {
		return errCheck(payloadCheck, m)
	}}
	retryC = check{"a read whose plan is out of date", func(t testing.TB, m mutation) error {
		return failures(t, func(t testing.TB) { checkReadRetry(t, m) })
	}}
	retentionC = check{"a write before the horizon", func(_ testing.TB, m mutation) error {
		node := newFakeNode()
		e := openFake(nopTB{}, node, m)
		defer func() { _ = e.Close() }()
		return retentionCheck(e)
	}}
	systemTimeC = check{"the system times of the batches", func(_ testing.TB, m mutation) error {
		return errCheck(systemTimeCheck, m)
	}}
	offMicroC = check{"a boundary off a whole microsecond", func(_ testing.TB, m mutation) error {
		return errCheck(offMicrosecondCheck, m)
	}}
)

// TestEveryFaultIsCaught: each check passes on a correct engine and fails on an engine
// with the fault it exists for.
func TestEveryFaultIsCaught(t *testing.T) {
	for _, c := range []struct {
		fault  string
		mut    mutation
		checks []check
	}{
		{"FOR SYSTEM_TIME omitted at old tokens", mutNoSystemTime, []check{streamC, readsC}},
		{"a token inside a batch answered by system time", mutMidBatchBySystemTime, []check{streamC, readsC}},
		{"the layer filter dropped", mutNoLayerFilter, []check{streamC, readsC, otherC}},
		{"a batched read dropping an empty list", mutBatchDropsEmpty, []check{streamC, otherC}},
		{"the instant of a read rounded to the nearest microsecond", mutRoundMicros, []check{floorC}},
		{"the instant of a read rounded up to a microsecond", mutCeilMicros, []check{floorC}},
		{"a write before the horizon accepted after Retain", mutAcceptBeforeHorizon, []check{retentionC}},
		{"a refused batch taking an ordinal", mutFailedBatchTakesOrdinal, []check{systemTimeC}},
		{"the next batch at the system time of the one before", mutRepeatSystemTime, []check{systemTimeC}},
		{"LastSeq raised before the transaction commits", mutLastSeqEarly, []check{systemTimeC}},
		{"a Retain kept in memory and not written", mutRetainNoTransaction, []check{retainC}},
		{"a transaction's meta document not read back", mutNoReadBack, []check{readBackC}},
		{"a write with no deadline", mutNoDeadline, []check{deadlineC}},
		{"a payload sent as bytes", mutRawPayload, []check{payloadC}},
		{"a read whose plan is out of date not made again", mutNoReadRetry, []check{retryC}},
		{"a boundary off a microsecond written truncated", mutWriteOffMicrosecond, []check{offMicroC}},
	} {
		for _, ck := range c.checks {
			t.Run(c.fault+" / "+ck.name, func(t *testing.T) {
				t.Parallel()
				if err := ck.run(t, 0); err != nil {
					t.Fatalf("a correct engine fails: %v", err)
				}
				err := ck.run(t, c.mut)
				if err == nil {
					t.Fatal("the fault went unnoticed")
				}
				t.Logf("caught: %.200v", err)
			})
		}
	}
}

// TestEveryGuardOfTheScriptIsNeeded: the script's checks pass on the script, and each fails
// on a copy of it with one guard taken out (run with the stubs, so nothing is started).
func TestEveryGuardOfTheScriptIsNeeded(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	orig, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []struct{ guard, from, to string }{
		{"the data directory must be absolute", `die "TOPOSHIFT_XTDB_DATA must be an absolute path, not '$data'"`, `:`},
		{"the data directory must not be in a work tree", `die "TOPOSHIFT_XTDB_DATA is inside a git work tree: $data"`, `:`},
		{"a container of the name must not exist", `die "the container $name already exists; run 'xt-server.sh stop' first"`, `:`},
		{"start takes no arguments", `[ $# -eq 1 ] || die "start takes no arguments"; `, ``},
		{"stop takes no arguments", `[ $# -eq 1 ] || die "stop takes no arguments"; `, ``},
		{"an unknown command is refused", `*) die "usage: xt-server.sh start | stop | psql [psql arguments...]" ;;`, `*) : ;;`},
		{"the node is published on loopback only", `-p "127.0.0.1:$port:5432"`, `-p "$port:5432"`},
		{"the health endpoint is published on loopback only", `-p "127.0.0.1:$health_port:8080"`, `-p "$health_port:8080"`},
		{"the JVM options can be overridden", `${TOPOSHIFT_XTDB_JAVA_OPTS:-$registered_jvm_options}`, `$registered_jvm_options`},
		{"the configuration can be overridden", `${TOPOSHIFT_XTDB_CONFIG:-$here/xtdb.yaml}`, `$here/xtdb.yaml`},
		{"another configuration is announced", `echo "xt-server.sh: NOT the registered`, `: "xt-server.sh: NOT the registered`},
		{"the JVM options are given", `-e "JDK_JAVA_OPTIONS=$jvm_options"`, `-e "X=$jvm_options"`},
		{"the configuration is mounted", `-v "$config:/config/xtdb.yaml:ro"`, `-v "$config:/config/other.yaml:ro"`},
		{"the node starts with the configuration", `"$image" -f /config/xtdb.yaml`, `"$image"`},
		{"the data directory is writable by the node", `chmod 0777 "$data"`, `chmod 0755 "$data"`},
		{"the image is pulled only when it is absent", `if ! docker image inspect "$image"`, `if true`},
		{"the image is pinned by digest", `@sha256:15b6aa9d37d1e1a4e21748788db9275a57b87c339e5eb66d86376cf1e6899816'`, `'`},
		{"the node is stopped with time to finish a block", `docker stop --time 120`, `docker stop --time 10`},
		{"the node is removed", `docker rm "$name"`, `true`},
		{"psql stops at the first error", `psql --set ON_ERROR_STOP=1`, `psql`},
		{"psql must be installed", `command -v psql >/dev/null || die "psql is not installed"`, `:`},
		{"the node is waited for", `"http://127.0.0.1:$health_port/healthz/started"`, `"http://127.0.0.1:$health_port/healthz/alive"`},
	} {
		t.Run(m.guard, func(t *testing.T) {
			t.Parallel()
			if !strings.Contains(string(orig), m.from) {
				t.Fatalf("the script has no %q to take out", m.from)
			}
			dir := t.TempDir()
			path := filepath.Join(dir, "xt-server.sh")
			if err := os.WriteFile(path, []byte(strings.Replace(string(orig), m.from, m.to, 1)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "xtdb.yaml"), cfg, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := failures(t, func(t testing.TB) { scriptChecks(t, path) }); err == nil {
				t.Fatal("the script without it passes the checks")
			}
		})
	}
}

// machineWithPsql puts a psql first on the PATH of the test process, as a runner image has
// one in /usr/bin, and returns the directory it is in. The psql writes down that it was
// run, in the file next to it, and succeeds.
func machineWithPsql(t *testing.T) (dir, ran string) {
	t.Helper()
	dir = t.TempDir()
	ran = filepath.Join(dir, "ran")
	body := "#!/bin/sh\necho run >> '" + ran + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir, ran
}

// TestAPsqlOnTheMachineIsNeverTheScripts: the script's checks pass on a machine that has a
// psql, which they did not on a runner image (they expected the script to find none); the
// script never ran it.
func TestAPsqlOnTheMachineIsNeverTheScripts(t *testing.T) {
	_, ran := machineWithPsql(t)
	scriptChecks(t, scriptPath)
	if _, err := os.Stat(ran); err == nil {
		t.Error("the script ran the machine's psql")
	}
}

// TestTheChecksNoticeAPathThatLeaks: with the machine's PATH after the stubs, which is what
// the checks were before, the machine's psql is found and the checks fail.
func TestTheChecksNoticeAPathThatLeaks(t *testing.T) {
	dir, ran := machineWithPsql(t)
	err := failures(t, func(t testing.TB) { checkScript(t, scriptPath, harness{leak: dir}) })
	if err == nil || !strings.Contains(err.Error(), "psql with none installed") {
		t.Errorf("the checks on a PATH that leaks: %v", err)
	}
	if _, err := os.Stat(ran); err != nil {
		t.Error("the leaked psql was not the one the script ran")
	}
}

// probe is a testing.TB that notes a skip as well as a failure, and ends the check at
// either, so that what a check does about a missing tool can be asserted.
type probe struct {
	recorder
	skipped bool
}

func (p *probe) Skip(...any)          { p.skipped = true; panic(stopCheck{}) }
func (p *probe) Skipf(string, ...any) { p.skipped = true; panic(stopCheck{}) }

// TestAMissingToolFailsOnARunnerAndSkipsElsewhere: the checks need bash and a few system
// tools. Without one, on a runner they fail, and elsewhere they skip.
func TestAMissingToolFailsOnARunnerAndSkipsElsewhere(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	withBashOnly := t.TempDir()
	if err := os.Symlink(bash, filepath.Join(withBashOnly, "bash")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, path string }{
		{"no bash", t.TempDir()},
		{"no dirname", withBashOnly},
	} {
		for _, onRunner := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s on a runner %v", c.name, onRunner), func(t *testing.T) {
				t.Setenv("PATH", c.path)
				t.Setenv("GITHUB_ACTIONS", map[bool]string{true: "true", false: ""}[onRunner])
				p := &probe{recorder: recorder{TB: t}}
				func() {
					defer func() {
						if r := recover(); r != nil {
							if _, ok := r.(stopCheck); !ok {
								panic(r)
							}
						}
					}()
					harness{}.run(p, scriptPath, nil, false, "start")
				}()
				failed := len(p.msgs) > 0
				if failed != onRunner || p.skipped == onRunner {
					t.Errorf("failed %v, skipped %v; want failed %v, skipped %v", failed, p.skipped, onRunner, !onRunner)
				}
			})
		}
	}
}
