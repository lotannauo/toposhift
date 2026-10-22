package xtspike

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

// The tests in this file need an XTDB node, started by scripts/xt-server.sh, as the engine's
// do. They run the commands as the command line does, against it, with a settle protocol
// short enough for a test (which is not the registered one, so no -registered is given
// except to env). The node orders its transactions by system time, and engines that share it
// open with later and later bases, so each load gives -base the clock; a table prefix of its
// own keeps each load's tables apart. The environment names the node:
//
//	TOPOSHIFT_XTDB_DSN              the node's Postgres wire address (required, or the tests skip)
//	TOPOSHIFT_XTDB_HEALTH           its health port (default http://127.0.0.1:55480)
//	TOPOSHIFT_XTDB_CONTAINER        its container, to inspect (the tests of env and the others
//	                                skip the inspection without it)
//	TOPOSHIFT_XTDB_DATA             the directory it keeps its files in, for sizes
//	TOPOSHIFT_XTDB_EXPECT_REGISTERED=true  asserts the node is the registered one (CI)

type liveNodeEnv struct{ dsn, health, container, data string }

func serverEnv(t *testing.T) liveNodeEnv {
	t.Helper()
	e := liveNodeEnv{
		dsn: os.Getenv("TOPOSHIFT_XTDB_DSN"), health: os.Getenv("TOPOSHIFT_XTDB_HEALTH"),
		container: os.Getenv("TOPOSHIFT_XTDB_CONTAINER"), data: os.Getenv("TOPOSHIFT_XTDB_DATA"),
	}
	if e.dsn == "" {
		t.Skip("TOPOSHIFT_XTDB_DSN is not set: no XTDB node to test against (scripts/xt-server.sh starts one)")
	}
	if e.health == "" {
		e.health = "http://127.0.0.1:55480"
	}
	return e
}

func prefixFor(t *testing.T) string {
	sum := sha256.Sum256([]byte(t.TempDir()))
	return "xs" + hex.EncodeToString(sum[:5])
}

// live runs a command line against the node, and returns what it printed.
type live struct {
	t   *testing.T
	env liveNodeEnv
	app *App
	out *bytes.Buffer
}

func newLive(t *testing.T) *live {
	t.Helper()
	l := &live{t: t, env: serverEnv(t), out: &bytes.Buffer{}}
	l.app = &App{Stdout: l.out, Stderr: &bytes.Buffer{}}
	return l
}

func (l *live) common(prefix, out string) []string {
	args := []string{
		"-script", script, "-dsn", l.env.dsn, "-health", l.env.health, "-prefix", prefix, "-out", out,
		"-settle-quiet", "4s", "-settle-max", "3m", "-settle-poll", "1s",
	}
	if l.env.container != "" {
		args = append(args, "-container", l.env.container)
	}
	return args
}

func (l *live) run(cmd string, args ...string) error {
	l.t.Helper()
	l.out.Reset()
	return l.app.Run(context.Background(), append([]string{cmd}, args...))
}

// clockBase is a base later than every earlier test's, to the microsecond.
func clockBase() string { return time.Now().UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano) }

func TestServerEnvNamesTheContainer(t *testing.T) {
	l := newLive(t)
	if l.env.container == "" {
		t.Skip("TOPOSHIFT_XTDB_CONTAINER is not set")
	}
	out := filepath.Join(t.TempDir(), "env")
	args := []string{"-script", script, "-container", l.env.container, "-out", out}
	registered := os.Getenv("TOPOSHIFT_XTDB_EXPECT_REGISTERED") == "true"
	if registered {
		args = append(args, "-registered")
	}
	if err := l.run("env", args...); err != nil {
		t.Fatalf("%v\n%s", err, l.out)
	}
	noDuration(t, "env", l.out.String())
	var e xtreport.EnvRecord
	if err := xtreport.ReadFile(filepath.Join(out, "env.json"), &e); err != nil {
		t.Fatal(err)
	}
	c := e.Env.Container
	if c == nil || !c.Running || c.XTDBVersion != "2.1.0" || c.ConfigSHA256 == "" || c.JavaOptions == "" {
		t.Fatalf("%+v", e.Env)
	}
	if registered && !e.Env.Registered {
		t.Errorf("the node is not the registered one: %v", e.Env.NotRegistered)
	}
	if e.Env.Registered != (len(e.Env.NotRegistered) == 0) {
		t.Errorf("%+v", e.Env)
	}
}

func TestServerLoadReadAndSizesOfAPlan(t *testing.T) {
	for name, spec := range map[string]runner.Spec{"no retention": tinySpec(), "a retention": retainingSpec()} {
		t.Run(name, func(t *testing.T) {
			l := newLive(t)
			plan, sum := planFile(t, spec)
			p, err := runner.LoadPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "rec")
			input := []string{"-plan", plan, "-expect-plan", sum}
			prefix := prefixFor(t)
			var before xtnode.Dir
			if l.env.data != "" {
				before = measureOrEmpty(t, filepath.Join(l.env.data, "storage"))
			}
			base := clockBase()
			if err := l.run("load", append(append(l.common(prefix, out), input...), "-base", base)...); err != nil {
				t.Fatalf("%v\n%s", err, l.out)
			}
			noDuration(t, "load", l.out.String())
			var ld xtreport.Load
			if err := xtreport.ReadFile(filepath.Join(out, "load.json"), &ld); err != nil {
				t.Fatal(err)
			}
			if !ld.BlockFinished || !ld.Settled || ld.Writer.Records != p.Stream.Records || ld.Writer.Retentions != uint64(len(p.Stream.Retentions)) || ld.Writer.Documents == 0 {
				t.Fatalf("%+v", ld)
			}
			// The node's own count of documents is the planner's: one meta document for every
			// transaction.
			if ld.Writer.Transactions != ld.Writer.Batches+ld.Writer.Retentions {
				t.Errorf("%+v", ld.Writer)
			}

			// A second load into the tables the first made is refused by the node's own state.
			if err := l.run("load", append(append(l.common(prefix, filepath.Join(t.TempDir(), "again")), input...), "-base", base)...); err == nil || !strings.Contains(err.Error(), "already holds a stream") {
				t.Errorf("a second load into the same tables: %v", err)
			}

			// read: every answer is the reference engine's, and none by a client fold.
			if err := l.run("read", append(append(l.common(prefix, out), input...), "-load", filepath.Join(out, "load.json"))...); err != nil {
				t.Fatalf("%v\n%s", err, l.out)
			}
			noDuration(t, "read", l.out.String())
			var rd xtreport.Read
			if err := xtreport.ReadFile(filepath.Join(out, "read.json"), &rd); err != nil {
				t.Fatal(err)
			}
			if !rd.G0.OK || rd.G0.Matched != len(p.Queries) || rd.Reads.ClientFold != 0 || rd.Reads.Native < uint64(len(p.Queries)) || !rd.Settled {
				t.Fatalf("%+v %+v", rd.G0, rd.Reads)
			}

			// sizes of the node's directories.
			if l.env.data == "" {
				t.Log("TOPOSHIFT_XTDB_DATA is not set: no sizes")
				return
			}
			if err := l.run("sizes", append(append(l.common(prefix, out), input...), "-load", filepath.Join(out, "load.json"), "-data", l.env.data)...); err != nil {
				t.Fatalf("%v\n%s", err, l.out)
			}
			noDuration(t, "sizes", l.out.String())
			var sz xtreport.Sizes
			if err := xtreport.ReadFile(filepath.Join(out, "sizes.json"), &sz); err != nil {
				t.Fatal(err)
			}
			if sz.Bytes <= 0 || sz.RecordsHeld != p.Stream.Records || len(sz.Dirs) != 2 || sz.Dirs[0].Files == 0 || !sz.BlockFinished {
				t.Fatalf("%+v", sz)
			}
			// The block the load left is in storage: finishing it wrote a block file, which the
			// storage directory had not before.
			storage := sz.Dirs[1]
			if storage.Path != "storage" || storage.Bytes <= before.Bytes || storage.Files <= before.Files {
				t.Errorf("the storage directory held %d bytes in %d files before the load and %d in %d after the block was finished", before.Bytes, before.Files, storage.Bytes, storage.Files)
			}
		})
	}
}

func TestServerAnActivityFileThroughQuestionsLoadAndRead(t *testing.T) {
	l := newLive(t)
	rs := hour()
	file := writeActivity(t, rs)
	digest := contentDigest(t, file)
	qdir := filepath.Join(t.TempDir(), "q")
	fileArgs := []string{"-activity", file, "-expect-activity", digest}
	if err := l.run("questions", append([]string{"-out", qdir}, fileArgs...)...); err != nil {
		t.Fatalf("%v\n%s", err, l.out)
	}
	qfile := filepath.Join(qdir, "questions.json")
	sum, err := fileSHA(qfile)
	if err != nil {
		t.Fatal(err)
	}
	set, err := xtinput.LoadQuestions(qfile)
	if err != nil {
		t.Fatal(err)
	}
	input := []string{"-questions", qfile, "-expect-questions", sum}
	out := filepath.Join(t.TempDir(), "rec")
	prefix := prefixFor(t)
	if err := l.run("load", append(append(append(l.common(prefix, out), input...), fileArgs...), "-base", clockBase())...); err != nil {
		t.Fatalf("%v\n%s", err, l.out)
	}
	if err := l.run("read", append(append(l.common(prefix, out), input...), "-load", filepath.Join(out, "load.json"))...); err != nil {
		t.Fatalf("%v\n%s", err, l.out)
	}
	var rd xtreport.Read
	if err := xtreport.ReadFile(filepath.Join(out, "read.json"), &rd); err != nil {
		t.Fatal(err)
	}
	if !rd.G0.OK || rd.G0.Matched != len(set.Queries) || rd.Input.Kind != "activity" || rd.Reads.ClientFold != 0 {
		t.Fatalf("%+v", rd)
	}
}

func TestServerSizingOnAPlan(t *testing.T) {
	l := newLive(t)
	l.app.Getenv = func(k string) (string, bool) {
		if k == "GITHUB_ACTIONS" {
			return "true", true
		}
		return os.LookupEnv(k)
	}
	if l.env.data == "" {
		t.Skip("TOPOSHIFT_XTDB_DATA is not set")
	}
	plan, sum := planFile(t, tinySpec())
	args := append(l.common(prefixFor(t), ""), "-plan", plan, "-expect-plan", sum, "-data", l.env.data, "-base", clockBase())
	// sizing takes no -out.
	args = without(args, "-out")
	if err := l.run("sizing", args...); err != nil {
		t.Fatalf("%v\n%s", err, l.out)
	}
	want := SizingRule(RegisteredMaxLoad, RegisteredMaxDiskPct) + "\nsizing: pass\n"
	if l.out.String() != want {
		t.Errorf("output:\n%s", l.out)
	}
}

// measureOrEmpty is the size of a directory, or nothing for one that is not there yet.
func measureOrEmpty(t *testing.T, dir string) xtnode.Dir {
	t.Helper()
	if _, err := os.Stat(dir); err != nil {
		return xtnode.Dir{}
	}
	d, err := xtnode.MeasureDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestServerFinishingTheBlockIsProcessedByTheNode(t *testing.T) {
	l := newLive(t)
	p := xtnode.HTTPProber{Base: l.env.health}
	cfg := xtnode.Config{Quiet: 3 * time.Second, Max: 2 * time.Minute, Poll: time.Second}
	var before xtnode.Dir
	if l.env.data != "" {
		before = measureOrEmpty(t, filepath.Join(l.env.data, "storage"))
	}
	res, err := xtnode.FinishAndSettle(context.Background(), p, cfg)
	if err != nil || !res.BlockFinished || !res.Settled {
		t.Fatalf("%+v, %v", res, err)
	}
	if l.env.data != "" {
		// Finishing a block writes its file, even a block with nothing new in it.
		if after := measureOrEmpty(t, filepath.Join(l.env.data, "storage")); after.Files <= before.Files {
			t.Errorf("%d files before the request, %d after", before.Files, after.Files)
		}
	}
}

func TestServerSettleReadsTheNodesOwnMetrics(t *testing.T) {
	l := newLive(t)
	p := xtnode.HTTPProber{Base: l.env.health}
	s, err := p.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Lag < 0 || s.JobsAvailable < 0 {
		t.Fatalf("%+v", s)
	}
	cfg := xtnode.Config{Quiet: 3 * time.Second, Max: 2 * time.Minute, Poll: time.Second}
	res, err := xtnode.Settle(context.Background(), p, cfg)
	if err != nil || !res.Settled {
		t.Fatalf("%+v, %v", res, err)
	}
}
