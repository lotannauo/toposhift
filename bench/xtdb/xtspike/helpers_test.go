package xtspike

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
	"github.com/lotannauo/toposhift/internal/identity"
)

const script = "../scripts/xt-server.sh"

// modelNode is a node of the commands over the model of the baseline's tables, which is all
// that the commands' own logic needs: the model keeps what is written to it, so a load, a read
// and a sizes run on one shared value see one node.
type modelNode struct {
	*xtmodel.Model
	tx    uint64
	opens int
	// wrong, if set, makes every alive answer wrong.
	wrong bool
	// writes counts the batches written; finishedAfterWrites is set by the rig's request to
	// finish the block when at least one batch had been written.
	writes              int
	finishedAfterWrites bool
	// onWrite, if set, is called after each batch is written.
	onWrite func()
	// writeErr, if set, is what every write returns instead of writing.
	writeErr error
}

func newModelNode() *modelNode { return &modelNode{Model: xtmodel.New()} }

func (m *modelNode) Write(b []engine.Record) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	if err := m.Model.Write(b); err != nil {
		return err
	}
	m.tx++
	m.writes++
	if m.onWrite != nil {
		m.onWrite()
	}
	return nil
}

func (m *modelNode) Retain(h time.Time) error { m.tx++; return m.Model.Retain(h) }
func (m *modelNode) Transactions() uint64     { return m.tx }
func (m *modelNode) RefusedOffMicrosecond() uint64 {
	return 0
}

func (m *modelNode) Reads() (uint64, uint64) {
	rc := m.ReadCounts()
	return rc.Native, rc.ClientFold
}
func (m *modelNode) Close() error { return nil }

func (m *modelNode) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	ok, err := m.Model.Alive(fp, t, sc)
	if m.wrong {
		ok = !ok
	}
	return ok, err
}

// rig is the commands run over a model node, a health port that is always quiet, a fake docker
// and a clock the settle protocol moves.
type rig struct {
	t      *testing.T
	node   *modelNode
	app    *App
	out    *bytes.Buffer
	errOut *bytes.Buffer
	env    map[string]string
	// clock is the time the settle protocol and sizing see.
	clock time.Time
	// probe is what the node reports at each poll (the message ids are added).
	probe func() xtnode.Sample
	// finishes counts the requests to finish the block, and events is what was asked of the
	// node, in order. stuck makes the node never process such a request.
	finishes  int
	events    []string
	stuck     bool
	requested bool
	// inspect is what docker inspect prints.
	inspect string
	dir     string
	cfgFile string
}

// registeredInspect is a container that is the pinned image with the pinned options and
// configuration.
func registeredInspect(t *testing.T, cfgFile string) string {
	t.Helper()
	pin, err := xtnode.ReadPinned(script)
	if err != nil {
		t.Fatal(err)
	}
	return `[{"Name":"/node","Image":"sha256:x","State":{"Running":true},"Config":{"Image":"` + pin.Image + `","Env":["JDK_JAVA_OPTIONS=` + pin.JavaOptions + `","XTDB_VERSION=2.1.0","GIT_SHA=abc"]},"Mounts":[{"Source":"` + cfgFile + `","Destination":"/config/xtdb.yaml"},{"Source":"DATA","Destination":"/var/lib/xtdb"}]}]`
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{t: t, node: newModelNode(), out: &bytes.Buffer{}, errOut: &bytes.Buffer{}, env: map[string]string{}, dir: t.TempDir()}
	r.clock = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.probe = func() xtnode.Sample { return xtnode.Sample{} }
	// The configuration file the container "mounts" is a copy of xtdb.yaml.
	b, err := os.ReadFile("../scripts/xtdb.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r.cfgFile = filepath.Join(r.dir, "mounted.yaml")
	if err := os.WriteFile(r.cfgFile, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r.inspect = registeredInspect(t, r.cfgFile)
	r.app = &App{
		Stdout: r.out, Stderr: r.errOut,
		Getenv: func(k string) (string, bool) { v, ok := r.env[k]; return v, ok },
		Open: func(ctx context.Context, cfg xtengine.Config) (Node, error) {
			r.node.opens++
			return r.node, nil
		},
		Admin: func(string) xtnode.Admin { return fakeAdmin{r} },
		Docker: func(_ context.Context, args ...string) ([]byte, error) {
			return []byte(strings.ReplaceAll(r.inspect, "DATA", r.dir)), nil
		},
		Now: func() time.Time { return r.clock },
		Clock: func(c *xtnode.Config) {
			c.Now = func() time.Time { return r.clock }
			c.Sleep = func(ctx context.Context, d time.Duration) error { r.clock = r.clock.Add(d); return ctx.Err() }
		},
	}
	return r
}

// fakeAdmin is the rig's health port.
type fakeAdmin struct{ r *rig }

func (f fakeAdmin) Sample(context.Context) (xtnode.Sample, error) {
	s := f.r.probe()
	s.Submitted, s.Processed = 10, 10
	if f.r.requested {
		s.Submitted = 11
		if !f.r.stuck {
			s.Processed = 11
		}
	}
	f.r.events = append(f.r.events, "sample")
	return s, nil
}

func (f fakeAdmin) FinishBlock(context.Context) error {
	f.r.finishes++
	f.r.requested = true
	f.r.node.finishedAfterWrites = f.r.node.writes > 0
	f.r.events = append(f.r.events, "finish")
	return nil
}

// run runs a command line.
func (r *rig) run(args ...string) error {
	r.t.Helper()
	r.out.Reset()
	return r.app.Run(context.Background(), args)
}

// common is the flags every command that reaches a node takes.
func (r *rig) common(out string) []string {
	return []string{"-script", script, "-dsn", "x", "-out", out, "-container", "node"}
}

func tinySpec() runner.Spec {
	s := runner.DefaultSpec(workload.Tiny())
	s.MinNonEmpty = 0
	s.BatchSize = 50
	return s
}

func retainingSpec() runner.Spec {
	s := tinySpec()
	s.Retentions = []runner.Retention{{At: 14 * time.Minute, Keep: 6 * time.Minute}}
	return s
}

// planFile makes a plan of the spec and saves it, returning its path and SHA-256.
func planFile(t *testing.T, spec runner.Spec) (string, string) {
	t.Helper()
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return path, hex.EncodeToString(sum[:])
}

// durationLike finds a Go-style duration or a "took"/"elapsed" in output: what no command
// prints.
var durationLike = regexp.MustCompile(`\d(ns|µs|us|ms|s|m|h)\b|\btook\b|\belapsed\b|\bper second\b`)

func noDuration(t *testing.T, what, text string) {
	t.Helper()
	if m := durationLike.FindString(text); m != "" {
		t.Errorf("%s prints a duration (%q):\n%s", what, m, text)
	}
}
