package runner_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// A paced background build that profiles its windows writes, for each window of the
// gate, one entry of the index at the gate's instants (publication and end of rewrite,
// on the gate's clock), judged as the gate judges the window, with every file it names
// written as a gzip-compressed profile; it records the option in its manifest and puts
// the mutex profile fraction back. It touches the process's CPU profile and mutex
// profile fraction, so it does not run in parallel with other tests.
func TestABuildThatProfilesItsWindowsWritesAProfileOfEachWindow(t *testing.T) {
	plan := windowOfADayPlan(t)
	out := t.TempDir()
	v := lookup(t, "Lroot/k64a2l1ns")
	dir := runner.CandidateDir(out, v.Name)
	before := runtime.SetMutexProfileFraction(-1)
	m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{RetentionMode: candidates.RetentionBackground, Pace: 200_000, ProfileWindows: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if after := runtime.SetMutexProfileFraction(-1); after != before {
		t.Errorf("the mutex profile fraction is %d after the build, %d before", after, before)
	}
	if m.Describe[runner.ProfileWindowsKey] != "true" {
		t.Errorf("described with %s %q", runner.ProfileWindowsKey, m.Describe[runner.ProfileWindowsKey])
	}
	idx, err := runner.LoadProfileIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := m.Gate
	if len(g.Windows) == 0 || len(idx.Passes) != len(g.Windows) {
		t.Fatalf("%d passes in the index for %d windows", len(idx.Passes), len(g.Windows))
	}
	profiled := 0
	for i, ps := range idx.Passes {
		w := g.Windows[i]
		if ps.Pass != i+1 || ps.PublishedNs != w.StartNs || ps.RewriteEndNs != w.RewriteEndNs {
			t.Errorf("pass %d published at %d, rewritten by %d; window %d %+v", ps.Pass, ps.PublishedNs, ps.RewriteEndNs, i, w)
		}
		if judged := w.StartNs >= g.PaceStartNs; ps.Judged != judged {
			t.Errorf("pass %d judged %v, its window %v", ps.Pass, ps.Judged, judged)
		}
		if ps.Retention < 0 || ps.Retention >= len(m.Timing.Retains) {
			t.Errorf("pass %d of retention %d of %d", ps.Pass, ps.Retention, len(m.Timing.Retains))
		}
		if len(ps.Errors) > 0 {
			t.Errorf("pass %d: %v", ps.Pass, ps.Errors)
		}
		if ps.Skipped != "" {
			continue
		}
		profiled++
		has := map[string]bool{}
		for _, name := range ps.Files {
			has[name] = true
			b, err := os.ReadFile(filepath.Join(dir, runner.ProfilesDir, name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(b, []byte{0x1f, 0x8b}) {
				t.Errorf("%s is not a gzip-compressed profile", name)
			}
		}
		for _, kind := range []string{"_start.mutex.pprof", "_first.cpu.pprof", "_window.mutex.pprof"} {
			if name := fmt.Sprintf("pass%d%s", ps.Pass, kind); !has[name] {
				t.Errorf("pass %d has no %s: %v", ps.Pass, name, ps.Files)
			}
		}
		if ps.FirstCPUStartNs < ps.PublishedNs || ps.FirstCPUEndNs < ps.FirstCPUStartNs {
			t.Errorf("pass %d: first CPU profile from %d to %d, published at %d", ps.Pass, ps.FirstCPUStartNs, ps.FirstCPUEndNs, ps.PublishedNs)
		}
	}
	if profiled == 0 {
		t.Errorf("no window was profiled: %+v", idx.Passes)
	}
}

// The profiles are refused, before anything is written, for a build with no windows
// from the store: one whose retention is synchronous, and one not through the root
// store.
func TestProfilingTheWindowsNeedsABackgroundBuildThroughTheRootStore(t *testing.T) {
	t.Parallel()

	plan := windowOfADayPlan(t)
	for _, c := range []struct{ name, candidate, mode, want string }{
		{"synchronous", "Lroot/k64a2l1ns", candidates.RetentionSync, "only a build in the background mode has windows"},
		{"not through the root store", "L/k64a2l1ns", candidates.RetentionBackground, "not built through the root store"},
	} {
		dir := filepath.Join(t.TempDir(), "c")
		_, err := runner.BuildWith(context.Background(), plan, lookup(t, c.candidate), dir, clean, runner.BuildOptions{RetentionMode: c.mode, ProfileWindows: true}, nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s: the build made %s before it refused", c.name, dir)
		}
	}
}

// The profiles add nothing to the rules of the store gate: their text, and so their
// digest, are as they were before the option.
func TestProfilingLeavesTheStoreGateRulesAsTheyWere(t *testing.T) {
	t.Parallel()

	const pinned = "667f1857c82875128eb7524618e41003fa679b02fa3d202111db076130b1761f"
	if got := runner.StoreGateRulesDigest(); got != pinned {
		t.Errorf("the rules digest is %s, was %s before the profiles of the windows", got, pinned)
	}
}
