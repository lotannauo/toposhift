package xtspike

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

func TestEnvRecordsTheNodeAndSaysWhetherItIsTheRegisteredOne(t *testing.T) {
	r := newRig(t)
	out := filepath.Join(t.TempDir(), "env")
	if err := r.run("env", "-script", script, "-container", "node", "-registered", "-out", out); err != nil {
		t.Fatal(err)
	}
	noDuration(t, "env", r.out.String())
	var e xtreport.EnvRecord
	if err := xtreport.ReadFile(filepath.Join(out, "env.json"), &e); err != nil {
		t.Fatal(err)
	}
	pin, err := xtnode.ReadPinned(script)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Env.Registered || e.Env.Image != pin.Image || e.Env.JavaOptions != pin.JavaOptions || e.Env.ConfigSHA256 != pin.ConfigSHA256 ||
		e.Env.QueryMode != "simple" || e.Env.Container == nil || e.Env.Container.XTDBVersion != "2.1.0" || e.Env.BinarySHA256 == "" || e.Env.NumCPU == 0 || e.Env.CPUModel == "" ||
		!e.Env.Settle.Registered || e.Env.Settle.QuietSeconds != 120 || e.Env.Settle.MaxSeconds != 1800 {
		t.Fatalf("%+v", e.Env)
	}
	if !strings.Contains(r.out.String(), "registered: yes") {
		t.Errorf("%s", r.out)
	}
	// The record is not overwritten.
	if err := r.run("env", "-script", script, "-out", out); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("a second record: %v", err)
	}
}

func TestANodeThatIsNotTheRegisteredOneIsNamedAndARegisteredRunRefusesIt(t *testing.T) {
	cases := map[string]struct {
		setup func(r *rig)
		args  []string
		why   string
	}{
		"a JVM option override":    {func(r *rig) { r.env[xtnode.EnvJavaOptions] = "-Xmx2g" }, nil, "TOPOSHIFT_XTDB_JAVA_OPTS is set"},
		"a configuration override": {func(r *rig) { r.env[xtnode.EnvConfig] = "/x.yaml" }, nil, "TOPOSHIFT_XTDB_CONFIG is set"},
		"another image":            {func(r *rig) { r.inspect = strings.Replace(r.inspect, "@sha256:", "@sha256:0", 1) }, nil, "the container's image is"},
		"other JVM options": {func(r *rig) {
			r.inspect = strings.Replace(r.inspect, "-Xmx4g", "-Xmx2g", 1)
		}, nil, "the container's JVM options are"},
		"another configuration file": {func(r *rig) {
			if err := os.WriteFile(r.cfgFile, []byte("log: x\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, nil, "the container's configuration file is not xtdb.yaml"},
		"a container that is not running": {func(r *rig) {
			r.inspect = strings.Replace(r.inspect, `"Running":true`, `"Running":false`, 1)
		}, nil, "the container is not running"},
		"a shorter settle window": {func(*rig) {}, []string{"-settle-quiet", "10s"}, "the settle protocol is not the registered one"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			c.setup(r)
			args := append([]string{"env", "-script", script, "-container", "node"}, c.args...)
			if err := r.run(append(args, "-registered")...); err == nil || !strings.Contains(err.Error(), c.why) {
				t.Fatalf("-registered: %v, want %q", err, c.why)
			}
			if err := r.run(args...); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.out.String(), "registered: NO") || !strings.Contains(r.out.String(), c.why) {
				t.Errorf("%s", r.out)
			}
		})
	}
	// With no container named the node cannot be said to be the registered one.
	r := newRig(t)
	if err := r.run("env", "-script", script, "-registered"); err == nil || !strings.Contains(err.Error(), "not inspected") {
		t.Errorf("-registered with no container: %v", err)
	}
	// Another build or node than the load's is refused at a read (see TestAReadIsHeldToTheLoad).
}

func TestEnvRefusesWhatItNeedsToBeRefused(t *testing.T) {
	r := newRig(t)
	for name, args := range map[string][]string{
		"no script":            {"env"},
		"a script with no pin": {"env", "-script", filepath.Join(t.TempDir(), "x.sh")},
		"an argument":          {"env", "-script", script, "extra"},
		"an unknown flag":      {"env", "-script", script, "-timed"},
	} {
		if err := r.run(args...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	tree := t.TempDir()
	if err := os.Mkdir(filepath.Join(tree, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.run("env", "-script", script, "-out", filepath.Join(tree, "out")); err == nil || !strings.Contains(err.Error(), "git work tree") {
		t.Errorf("an output in a work tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tree, "out")); err == nil {
		t.Error("the directory was made")
	}
}

// loaded runs a load of a plan into the rig's node and returns the arguments that name the
// plan, the output directory and the plan itself.
func (r *rig) loaded(spec runner.Spec) (input []string, out string, p *runner.Plan) {
	r.t.Helper()
	plan, sum := planFile(r.t, spec)
	p, err := runner.LoadPlan(plan)
	if err != nil {
		r.t.Fatal(err)
	}
	out = filepath.Join(r.t.TempDir(), "rec")
	input = []string{"-plan", plan, "-expect-plan", sum}
	if err := r.run(append(append([]string{"load"}, r.common(out)...), input...)...); err != nil {
		r.t.Fatalf("%v\n%s", err, r.out)
	}
	return input, out, p
}

func (r *rig) cmd(cmd, out string, input []string, extra ...string) error {
	return r.run(append(append(append([]string{cmd}, r.common(out)...), input...), extra...)...)
}

func TestAPlanThatIsNotNamedByItsDigestIsRefusedBeforeAnythingIsWritten(t *testing.T) {
	r := newRig(t)
	plan, sum := planFile(t, tinySpec())
	out := filepath.Join(t.TempDir(), "rec")
	for name, input := range map[string][]string{
		"no digest":                {"-plan", plan},
		"another digest":           {"-plan", plan, "-expect-plan", strings.Repeat("0", 64)},
		"no input":                 {},
		"a plan and questions":     {"-plan", plan, "-expect-plan", sum, "-questions", "q.json"},
		"questions with no digest": {"-questions", plan},
	} {
		if err := r.cmd("load", out, input); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if r.node.opens != 0 || r.node.tx != 0 {
			t.Fatalf("%s: the node was reached (%d opens, %d transactions)", name, r.node.opens, r.node.tx)
		}
	}
	// An edited plan is refused by its own digest check.
	b, err := os.ReadFile(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan, append(b, ' '), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.cmd("load", out, []string{"-plan", plan, "-expect-plan", sum}); err == nil {
		t.Error("an edited plan was loaded")
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("a record directory was made")
	}
}

func TestAReadIsHeldToTheLoad(t *testing.T) {
	r := newRig(t)
	input, out, _ := r.loaded(tinySpec())
	load := filepath.Join(out, "load.json")
	read := func(extra ...string) error {
		o := filepath.Join(t.TempDir(), "r")
		return r.cmd("read", o, input, append([]string{"-load", load}, extra...)...)
	}
	if err := read(); err != nil {
		t.Fatalf("%v\n%s", err, r.out)
	}

	if err := r.cmd("read", filepath.Join(t.TempDir(), "r"), input); err == nil || !strings.Contains(err.Error(), "-load is required") {
		t.Errorf("no load record: %v", err)
	}
	if err := read("-prefix", "other"); err == nil || !strings.Contains(err.Error(), "prefix") {
		t.Errorf("another prefix: %v", err)
	}
	// Another plan.
	otherPlan, otherSum := planFile(t, retainingSpec())
	o := filepath.Join(t.TempDir(), "r")
	if err := r.cmd("read", o, []string{"-plan", otherPlan, "-expect-plan", otherSum}, "-load", load); err == nil || !strings.Contains(err.Error(), "not of this input") {
		t.Errorf("another plan: %v", err)
	}
	// A record that is not a load.
	if err := r.cmd("read", filepath.Join(t.TempDir(), "r"), input, "-load", filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("a missing load record")
	}
	// Another build or node than the load's.
	r.env[xtnode.EnvConfig] = "/other.yaml"
	if err := read(); err == nil || !strings.Contains(err.Error(), "another build or node") || !strings.Contains(err.Error(), "overrides") {
		t.Errorf("an override set after the load: %v", err)
	}
	delete(r.env, xtnode.EnvConfig)
	r.inspect = strings.Replace(r.inspect, `"XTDB_VERSION=2.1.0"`, `"XTDB_VERSION=2.2.0"`, 1)
	if err := read(); err == nil || !strings.Contains(err.Error(), "container") {
		t.Errorf("another node: %v", err)
	}
	r.inspect = registeredInspect(t, r.cfgFile)
	// A node that does not hold the load's stream.
	held := r.node
	r.node = newModelNode()
	if err := read(); err == nil || !strings.Contains(err.Error(), "not the load's stream") {
		t.Errorf("an empty node: %v", err)
	}
	r.node = held
	r.node.tx++ // one transaction more than the load wrote
	if err := read(); err == nil || !strings.Contains(err.Error(), "not the load's stream") {
		t.Errorf("a node with another transaction: %v", err)
	}
	r.node.tx--
	if err := read(); err != nil {
		t.Errorf("the node as it was: %v", err)
	}
}

func TestAWrongAnswerIsRecordedAndFailsTheRead(t *testing.T) {
	r := newRig(t)
	input, out, _ := r.loaded(tinySpec())
	r.node.wrong = true
	o := filepath.Join(t.TempDir(), "r")
	err := r.cmd("read", o, input, "-load", filepath.Join(out, "load.json"))
	if err == nil || !strings.Contains(err.Error(), "answers differ") {
		t.Fatalf("%v", err)
	}
	var rd xtreport.Read
	if e := xtreport.ReadFile(filepath.Join(o, "read.json"), &rd); e != nil {
		t.Fatal(e)
	}
	if rd.G0.OK || rd.G0.Matched >= rd.G0.Queries || len(rd.G0.Mismatches) == 0 {
		t.Errorf("%+v", rd.G0)
	}
	if !strings.Contains(r.out.String(), "MISMATCH") {
		t.Errorf("%s", r.out)
	}
}

func TestANodeThatDoesNotSettleIsSaidSoAndItsSizesAreFlagged(t *testing.T) {
	r := newRig(t)
	r.probe = func() xtnode.Sample { return xtnode.Sample{Lag: 4} }
	input, out, p := r.loaded(tinySpec())
	l := r.readLoadRecord(out)
	if l.Settled || !strings.Contains(l.SettleReason, "4 log messages") {
		t.Fatalf("%+v", l)
	}
	if !strings.Contains(r.out.String(), "DID NOT SETTLE") {
		t.Errorf("%s", r.out)
	}
	// Its answers are still checked.
	o := filepath.Join(t.TempDir(), "r")
	if err := r.cmd("read", o, input, "-load", filepath.Join(out, "load.json")); err != nil {
		t.Fatal(err)
	}
	var rd xtreport.Read
	if err := xtreport.ReadFile(filepath.Join(o, "read.json"), &rd); err != nil || rd.Settled || !rd.G0.OK || rd.G0.Queries != len(p.Queries) {
		t.Fatalf("%+v, %v", rd, err)
	}
	data := t.TempDir()
	for _, d := range dataDirs {
		if err := os.MkdirAll(filepath.Join(data, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.dirOfContainer(data)
	o = filepath.Join(t.TempDir(), "s")
	if err := r.cmd("sizes", o, input, "-load", filepath.Join(out, "load.json"), "-data", data); err != nil {
		t.Fatal(err)
	}
	var sz xtreport.Sizes
	if err := xtreport.ReadFile(filepath.Join(o, "sizes.json"), &sz); err != nil || sz.Settled || !strings.Contains(sz.Note, "not the sizes of a settled node") {
		t.Fatalf("%+v, %v", sz, err)
	}
}

func TestSizesRefusesADirectoryThatIsNotTheContainers(t *testing.T) {
	r := newRig(t)
	input, out, _ := r.loaded(tinySpec())
	mk := func() string {
		d := t.TempDir()
		for _, n := range dataDirs {
			if err := os.MkdirAll(filepath.Join(d, n), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		return d
	}
	mounted, other := mk(), mk()
	r.dirOfContainer(mounted)
	load := filepath.Join(out, "load.json")
	if err := r.cmd("sizes", filepath.Join(t.TempDir(), "s"), input, "-load", load, "-data", other); err == nil || !strings.Contains(err.Error(), "keeps its files in") {
		t.Errorf("a directory the container does not mount: %v", err)
	}
	if err := r.cmd("sizes", filepath.Join(t.TempDir(), "s"), input, "-load", load, "-data", t.TempDir()); err == nil {
		t.Error("a data directory with no log and storage")
	}
	if err := r.cmd("sizes", filepath.Join(t.TempDir(), "s"), input, "-load", load); err == nil || !strings.Contains(err.Error(), "-data is required") {
		t.Errorf("no data directory: %v", err)
	}
	if err := r.cmd("sizes", filepath.Join(t.TempDir(), "s"), input, "-load", load, "-data", mounted); err != nil {
		t.Errorf("the mounted directory: %v", err)
	}
}

func TestTheNodeIsNotReachedWhenTheOutputIsInAWorkTree(t *testing.T) {
	r := newRig(t)
	plan, sum := planFile(t, tinySpec())
	tree := t.TempDir()
	if err := os.Mkdir(filepath.Join(tree, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []string{"load", "read", "sizes"} {
		err := r.cmd(cmd, filepath.Join(tree, "out"), []string{"-plan", plan, "-expect-plan", sum})
		if err == nil || !strings.Contains(err.Error(), "git work tree") {
			t.Errorf("%s: %v", cmd, err)
		}
	}
	if r.node.opens != 0 {
		t.Errorf("the node was opened %d times", r.node.opens)
	}
	if err := r.run("questions", "-out", filepath.Join(tree, "out"), "-activity", "x", "-expect-activity", "abc"); err == nil || !strings.Contains(err.Error(), "git work tree") {
		t.Errorf("questions: %v", err)
	}
}

func TestWriterRecordsWhatThePlannerWrote(t *testing.T) {
	r := newRig(t)
	_, out, p := r.loaded(retainingSpec())
	l := r.readLoadRecord(out)
	w := l.Writer
	var inserts, deletes uint64
	for _, t := range w.Tables {
		inserts += t.Inserts
		deletes += t.PortionDeletes
	}
	if w.Documents != inserts || w.Statements != inserts+deletes || w.Transactions != w.Batches+w.Retentions || w.Retentions != uint64(len(p.Stream.Retentions)) || w.Retentions == 0 {
		t.Errorf("%+v", w)
	}
	if w.InOrder+w.Late != w.Records {
		t.Errorf("%d in order and %d late of %d records", w.InOrder, w.Late, w.Records)
	}
	if w.StatementsPerRecord != float64(w.Statements)/float64(w.Records) || w.DocumentsPerRecord <= 1 {
		t.Errorf("%+v", w)
	}
	var names []string
	for _, tb := range w.Tables {
		names = append(names, tb.Name)
	}
	if strings.Join(names, ",") != "seg_f,seg_r,ent,log_f,log_r,log_e,meta" {
		t.Errorf("tables %v", names)
	}
}

func TestTheSystemTimeBaseIsRecordedAndARegisteredRunRefusesAnother(t *testing.T) {
	r := newRig(t)
	var seen []time.Time
	open := r.app.Open
	r.app.Open = func(ctx context.Context, cfg xtengine.Config) (Node, error) {
		seen = append(seen, cfg.Base)
		return open(ctx, cfg)
	}
	plan, sum := planFile(t, tinySpec())
	input := []string{"-plan", plan, "-expect-plan", sum}
	out := filepath.Join(t.TempDir(), "rec")
	if err := r.cmd("load", out, input); err != nil {
		t.Fatal(err)
	}
	if l := r.readLoadRecord(out); l.SystemBase != "2000-01-01T00:00:00Z" || !seen[0].IsZero() {
		t.Errorf("the default base: %q, opened with %v", l.SystemBase, seen)
	}
	r.node = newModelNode()
	out = filepath.Join(t.TempDir(), "rec")
	if err := r.cmd("load", out, input, "-base", "2026-10-21T12:00:00.000001Z"); err != nil {
		t.Fatal(err)
	}
	if l := r.readLoadRecord(out); l.SystemBase != "2026-10-21T12:00:00.000001Z" || !seen[1].Equal(time.Date(2026, 10, 21, 12, 0, 0, 1000, time.UTC)) {
		t.Errorf("another base: %q, opened with %v", l.SystemBase, seen)
	}
	r.node = newModelNode()
	if err := r.cmd("load", filepath.Join(t.TempDir(), "rec"), input, "-base", "2026-10-21T12:00:00Z", "-registered"); err == nil || !strings.Contains(err.Error(), "not the registered system-time base") {
		t.Errorf("a registered run with a base: %v", err)
	}
	if err := r.cmd("load", filepath.Join(t.TempDir(), "rec"), input, "-base", "yesterday"); err == nil {
		t.Error("a base that is not a time")
	}
	if r.node.opens != 0 {
		t.Errorf("the node was reached %d times by refused runs", r.node.opens)
	}
}

// TestAStreamThatIsNotThePlansFailsTheLoad: a plan whose stream is not the one the spec
// regenerates (here, a digest that is not) is loaded and then refused, so that no load
// record says the node holds a stream that is not the plan's.
func TestAStreamThatIsNotThePlansFailsTheLoad(t *testing.T) {
	r := newRig(t)
	p, err := runner.MakePlan(context.Background(), tinySpec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Stream.Digest = strings.Repeat("0", 64)
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	sum, err := fileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "rec")
	err = r.cmd("load", out, []string{"-plan", path, "-expect-plan", sum})
	if err == nil || !strings.Contains(err.Error(), "the stream is not the plan's") {
		t.Fatalf("%v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "load.json")); err == nil {
		t.Error("a load record was written for a stream that is not the plan's")
	}
}

func TestTheLastBlockIsFinishedAfterTheWritesAndBeforeAnySizeIsTaken(t *testing.T) {
	r := newRig(t)
	input, out, _ := r.loaded(tinySpec())
	if r.finishes != 1 || len(r.events) == 0 || r.events[0] != "finish" || !r.node.finishedAfterWrites {
		t.Fatalf("%d requests, events %v, finished after the writes: %v", r.finishes, r.events[:min(3, len(r.events))], r.node.finishedAfterWrites)
	}
	if l := r.readLoadRecord(out); !l.BlockFinished || !l.Settled {
		t.Errorf("%+v", l)
	}
	// A read and sizes ask nothing of the node's log.
	data := t.TempDir()
	for _, d := range dataDirs {
		if err := os.MkdirAll(filepath.Join(data, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.dirOfContainer(data)
	load := filepath.Join(out, "load.json")
	if err := r.cmd("read", filepath.Join(t.TempDir(), "r"), input, "-load", load); err != nil {
		t.Fatal(err)
	}
	so := filepath.Join(t.TempDir(), "s")
	if err := r.cmd("sizes", so, input, "-load", load, "-data", data); err != nil {
		t.Fatal(err)
	}
	if r.finishes != 1 {
		t.Errorf("%d requests", r.finishes)
	}
	var sz xtreport.Sizes
	if err := xtreport.ReadFile(filepath.Join(so, "sizes.json"), &sz); err != nil || !sz.BlockFinished {
		t.Errorf("%+v, %v", sz, err)
	}
}

func TestSizesRefuseANodeWhoseLastBlockWasNotFinished(t *testing.T) {
	r := newRig(t)
	r.stuck = true // the node never processes the request to finish its block
	input, out, _ := r.loaded(tinySpec())
	l := r.readLoadRecord(out)
	if l.BlockFinished || l.Settled || l.SettleReason == "" {
		t.Fatalf("%+v", l)
	}
	if !strings.Contains(r.out.String(), "DID NOT FINISH ITS BLOCK") {
		t.Errorf("%s", r.out)
	}
	data := t.TempDir()
	for _, d := range dataDirs {
		if err := os.MkdirAll(filepath.Join(data, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.dirOfContainer(data)
	so := filepath.Join(t.TempDir(), "s")
	err := r.cmd("sizes", so, input, "-load", filepath.Join(out, "load.json"), "-data", data)
	if err == nil || !strings.Contains(err.Error(), "last block was not finished") {
		t.Fatalf("%v", err)
	}
	if _, e := os.Stat(filepath.Join(so, "sizes.json")); e == nil {
		t.Error("sizes were recorded")
	}
	// The answers of such a node can still be checked.
	if err := r.cmd("read", filepath.Join(t.TempDir(), "r"), input, "-load", filepath.Join(out, "load.json")); err != nil {
		t.Errorf("read: %v", err)
	}
}
