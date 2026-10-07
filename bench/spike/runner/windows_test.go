package runner_test

import (
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// goodWindows are windows that can be judged together: the same pins, cache and seed, the
// lengths of windows of their days, the same candidates built by the same binary.
func goodWindows() []runner.RetainedWindow {
	base := runner.DefaultSpec(workload.Tiny())
	var ws []runner.RetainedWindow
	for _, days := range []int{1, 2, 3} {
		spec := runner.WindowSpec(base, days)
		spec.Pins = &runner.Pins{Digest: "pins-1"}
		plan := &runner.Plan{Spec: spec, CacheBytes: 64 << 20}
		build := runner.BuildInfo{GoVersion: "go", GOOS: "os", GOARCH: "arch", Revision: "r", Executable: "exe"}
		var cs []*runner.Candidate
		for _, name := range []string{"A", "B"} {
			cs = append(cs, &runner.Candidate{
				Manifest: &runner.Manifest{Candidate: name, Build: build, Untimed: true},
				Results:  &runner.Results{Candidate: name, Build: build, Untimed: true},
			})
		}
		ws = append(ws, runner.RetainedWindow{Days: days, Plan: plan, Candidates: cs})
	}
	return ws
}

func reasons(ws []runner.RetainedWindow) string {
	// the plan digest, stream and queries are not what these tests are about
	var out []string
	for _, p := range runner.CheckWindows(ws, true) {
		if !strings.Contains(p, "another plan") && !strings.Contains(p, "results for") && !strings.Contains(p, "another stream") {
			out = append(out, p)
		}
	}
	return strings.Join(out, "\n")
}

func TestWindowsAreJudgedTogetherOnlyIfTheyAreOfTheSamePinsCacheSeedAndCandidates(t *testing.T) {
	t.Parallel()

	if got := reasons(goodWindows()); got != "" {
		t.Fatalf("windows that agree are held to be incomparable: %s", got)
	}
	for name, c := range map[string]struct {
		edit func(ws []runner.RetainedWindow)
		want string
	}{
		"another pins digest": {func(ws []runner.RetainedWindow) { ws[1].Plan.Spec.Pins = &runner.Pins{Digest: "pins-2"} }, "different pins"},
		"no pins":             {func(ws []runner.RetainedWindow) { ws[2].Plan.Spec.Pins = nil }, "no pins"},
		"a full store":        {func(ws []runner.RetainedWindow) { ws[1].Plan.Spec.FullStore = true }, "not both full stores or both projections"},
		"another cache":       {func(ws []runner.RetainedWindow) { ws[1].Plan.CacheBytes = 32 << 20 }, "block caches"},
		"another seed":        {func(ws []runner.RetainedWindow) { ws[2].Plan.Spec.Workload.Seed++ }, "different seeds"},
		"the wrong length":    {func(ws []runner.RetainedWindow) { ws[1].Plan.Spec.Workload.Duration += 1 }, "not the"},
		"another scenario":    {func(ws []runner.RetainedWindow) { ws[2].Plan.Spec.Workload.EventsPerSecond *= 2 }, "different scenarios"},
		"another run age":     {func(ws []runner.RetainedWindow) { ws[1].Plan.Spec.Workload.RunMaxAge = 2 * time.Hour }, "different scenarios"},
		"a rest in one build": {func(ws []runner.RetainedWindow) {
			ws[1].Candidates[0].Manifest.Describe = map[string]string{runner.RestKey: "true"}
		}, "with and without a rest"},
		"another candidate": {func(ws []runner.RetainedWindow) { ws[2].Candidates = ws[2].Candidates[:1] }, "different candidates"},
		"another binary": {func(ws []runner.RetainedWindow) {
			for _, c := range ws[1].Candidates { // consistent within the window
				c.Manifest.Build.Executable, c.Results.Build.Executable = "other", "other"
			}
		}, "windows of 1 and 2 days were built by different binaries"},
		"an answer that differs": {func(ws []runner.RetainedWindow) { ws[0].Candidates[1].Results.Mismatches = []string{"q"} }, "window of 1 days:"},
	} {
		ws := goodWindows()
		c.edit(ws)
		if got := reasons(ws); !strings.Contains(got, c.want) {
			t.Errorf("%s: the reasons %q lack %q", name, got, c.want)
		}
	}
	if got := runner.CheckWindows(nil, true); len(got) == 0 {
		t.Error("no windows were judged")
	}
}
