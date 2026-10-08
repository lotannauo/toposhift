package pebblelog_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// checkingSink writes the stream to the engine and, after each retention,
// compares everything the engine remembers with a read of the whole database.
type checkingSink struct {
	t *testing.T
	e *pebblelog.Engine
}

func (s checkingSink) Write(batch []engine.Record) error { return s.e.Write(batch) }

func (s checkingSink) Retain(h time.Time) error {
	if err := s.e.Retain(h); err != nil {
		return err
	}
	start := time.Now()
	if !pebblelog.Complete(s.e) {
		s.t.Errorf("the map is not complete after the retention at %s", h.Format(time.RFC3339))
	}
	if err := pebblelog.CheckRememberedState(s.e); err != nil {
		s.t.Errorf("after the retention at %s: %v", h.Format(time.RFC3339), err)
	}
	s.t.Logf("after the retention at %s: %d prefixes remembered, all checked against a whole read in %s", h.Format(time.RFC3339), pebblelog.Remembered(s.e), time.Since(start).Round(time.Second))
	return nil
}

// TestTheStreamOfAPlanLeavesTheRememberedStateAWholeReadFinds drives a plan's
// whole stream through an engine, as a build does, and after every retention
// compares each prefix the engine remembers with a read of the whole prefix, and
// the map's claim to be complete with the prefixes on disk. It is a local check
// for a plan too big to keep in the repository: PEBBLELOG_PLAN names the plan's
// file (the test is skipped without it, and in a trimmed run), and
// PEBBLELOG_CANDIDATE the candidate (default L/k64a2l1ns). Every prefix is new to
// the engine, and its retentions work the state out, so it also expects it to
// have read no prefix at all.
func TestTheStreamOfAPlanLeavesTheRememberedStateAWholeReadFinds(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	path := os.Getenv("PEBBLELOG_PLAN")
	if path == "" {
		t.Skip("PEBBLELOG_PLAN does not name a plan")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("PEBBLELOG_PLAN names a plan that cannot be read: %v", err)
	}
	name := os.Getenv("PEBBLELOG_CANDIDATE")
	if name == "" {
		name = "L/k64a2l1ns"
	}
	plan, err := runner.LoadPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := candidates.Lookup(name)
	if err != nil {
		t.Fatal(err)
	}
	rec := runner.NewCapture()
	opened, err := v.Open(t.TempDir(), candidates.Options{CacheBytes: plan.CacheBytes, Recorder: rec, DisableReadCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	e, ok := opened.(*pebblelog.Engine)
	if !ok {
		_ = opened.Close()
		t.Fatalf("%s is not layout L", name)
	}
	defer func() { _ = e.Close() }()
	if _, err := runner.Drive(context.Background(), plan.Spec, checkingSink{t, e}); err != nil {
		t.Fatal(err)
	}
	totals := rec.Totals()
	if totals["checkpoint.written"] == 0 || totals["retain.state_keys"] == 0 {
		t.Fatalf("the stream wrote %d checkpoints and the retentions read %d keys for the state: it exercises nothing", totals["checkpoint.written"], totals["retain.state_keys"])
	}
	if n := totals["checkpoint.loads"]; n != 0 {
		t.Errorf("the engine read the state of %d prefixes", n)
	}
	t.Logf("counts: %v", totals)
}
