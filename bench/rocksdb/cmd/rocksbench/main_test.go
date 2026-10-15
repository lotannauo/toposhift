package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// tinyPlan writes a tiny plan into a new output directory, as spikebench plan does.
func tinyPlan(t *testing.T) (string, *runner.Plan) {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	spec.MinNonEmpty = 0
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if err := p.Save(filepath.Join(out, planFile)); err != nil {
		t.Fatal(err)
	}
	return out, p
}

// A tiny plan goes through build and read, which write spikebench's manifest and
// results for the RL candidate; a read refuses a plan the build was not made from,
// as one whose expected answer was changed; and a binary that may not give a result
// is refused unless the run says it is untimed.
func TestBuildAndRead(t *testing.T) {
	t.Parallel()
	out, p := tinyPlan(t)
	const name = "RL/k64a2l5m"
	args := []string{"-out", out, "-candidate", name, "-untimed"}
	ctx := context.Background()
	if err := run(ctx, "build", []string{"-out", out, "-candidate", name}, io.Discard); err == nil || !strings.Contains(err.Error(), "not one a result may come from") {
		t.Fatalf("a build by a test binary without -untimed: %v", err)
	}
	if err := run(ctx, "build", args, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, "read", args, io.Discard); err != nil {
		t.Fatal(err)
	}
	dir := runner.CandidateDir(out, name)
	c, err := runner.LoadCandidate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Manifest.Candidate != name || c.Results.Candidate != name || len(c.Results.Queries) != len(p.Queries) || len(c.Results.Mismatches) != 0 {
		t.Fatalf("manifest of %q, results of %q with %d queries (plan %d), %d mismatches",
			c.Manifest.Candidate, c.Results.Candidate, len(c.Results.Queries), len(p.Queries), len(c.Results.Mismatches))
	}
	if !c.Manifest.Build.CGO || !c.Manifest.Untimed || c.Manifest.Describe["engine"] != "rocksdb" || c.Manifest.Describe["rest_after_retention"] != "true" {
		t.Errorf("manifest: cgo %v, untimed %v, engine %q, rest %q", c.Manifest.Build.CGO, c.Manifest.Untimed, c.Manifest.Describe["engine"], c.Manifest.Describe["rest_after_retention"])
	}

	// The same plan with one expected answer changed is another plan.
	q := p.Queries[0]
	q.Expect = strings.Repeat("0", len(q.Expect))
	p.Queries[0] = q
	if err := p.Save(filepath.Join(out, planFile)); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, "read", args, io.Discard); err == nil || !strings.Contains(err.Error(), "another plan") {
		t.Fatalf("a read against a changed plan: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	t.Parallel()
	out, _ := tinyPlan(t)
	ctx := context.Background()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"no candidate":         {"-out", out},
		"a candidate of L":     {"-out", out, "-candidate", "L/off", "-untimed"},
		"an unknown candidate": {"-out", out, "-candidate", "RL/on", "-untimed"},
		"inside the worktree":  {"-out", wd, "-candidate", "RL/off", "-untimed"},
	} {
		if err := run(ctx, "build", args, io.Discard); err == nil {
			t.Errorf("%s: built", name)
		}
	}
	if err := run(ctx, "judge", nil, io.Discard); !errors.Is(err, errUsage) {
		t.Errorf("an unknown command: %v", err)
	}
}

// A build given the Pebble builds' memory limit runs under that limit less the plan's
// cache and four memtables, and its manifest records it; given none, it runs under
// none. Not parallel: the limit is the process's, and is put back after.
func TestPebbleMemoryLimit(t *testing.T) {
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	for _, c := range []struct{ flag, want string }{{"2147483648", ""}, {"none", "none"}} {
		out, p := tinyPlan(t)
		want := c.want
		if want == "" {
			want = strconv.FormatInt(2<<30-(p.CacheBytes+pebblekv.BenchTuning().MemTableReserve()), 10)
		}
		if err := run(context.Background(), "build", []string{"-out", out, "-candidate", "RL/off", "-untimed", "-pebble-memory-limit", c.flag}, io.Discard); err != nil {
			t.Fatal(err)
		}
		m, err := runner.LoadManifest(runner.CandidateDir(out, "RL/off"))
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Describe[runner.GoMemoryLimitKey]; got != want {
			t.Errorf("-pebble-memory-limit %s: the manifest records %q, want %q", c.flag, got, want)
		}
	}
	if err := run(context.Background(), "build", []string{"-out", t.TempDir(), "-candidate", "RL/off", "-untimed", "-pebble-memory-limit", "lots"}, io.Discard); err == nil {
		t.Error("a limit that is not a number is accepted")
	}
}
