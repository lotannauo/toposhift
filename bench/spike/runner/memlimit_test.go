package runner_test

import (
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// A build records the Go memory limit it ran under. The limit is process-wide and the
// tests run in parallel, so this reads it and never sets it.
func TestABuildRecordsTheGoMemoryLimitItRanUnder(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	_, m, _ := run(t, mustPlan(t, tinySpec()), lookup(t, "L/off"))
	got, ok := m.Describe[runner.GoMemoryLimitKey]
	if !ok {
		t.Fatalf("the manifest does not hold %s: %v", runner.GoMemoryLimitKey, m.Describe)
	}
	// A negative argument reads the limit and leaves it as it is; the runtime's value
	// for no limit is the largest int64.
	want := "none"
	if n := debug.SetMemoryLimit(-1); n != math.MaxInt64 {
		want = strconv.FormatInt(n, 10)
	}
	if got != want {
		t.Errorf("the manifest records the limit %q, want %q", got, want)
	}
}

// A report refuses to compare builds that ran under different Go memory limits, and a
// manifest without the key is a build that ran under none.
func TestCheckRefusesBuildsUnderDifferentGoMemoryLimits(t *testing.T) {
	t.Parallel()

	plan := &runner.Plan{}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	build := func(name, limit string) *runner.Candidate {
		m := &runner.Manifest{Candidate: name, PlanDigest: digest, Describe: map[string]string{}}
		if limit != "" {
			m.Describe[runner.GoMemoryLimitKey] = limit
		}
		return &runner.Candidate{Manifest: m, Results: &runner.Results{Candidate: name, PlanDigest: digest}}
	}
	const gib11 = "11811160064"
	for _, c := range []struct {
		name         string
		a, b         string
		wantProblems bool
	}{
		{"none and a limit", "none", gib11, true},
		{"no key and a limit", "", gib11, true},
		{"two limits", gib11, "12884901888", true},
		{"one limit twice", gib11, gib11, false},
		{"none and no key", "none", "", false},
		{"none twice", "none", "none", false},
		{"no key twice", "", "", false},
	} {
		problems := strings.Join(runner.Check(plan, []*runner.Candidate{build("a", c.a), build("b", c.b)}, true), "\n")
		got := strings.Contains(problems, "different Go memory limits")
		if got != c.wantProblems {
			t.Errorf("%s: refused %v, want %v:\n%s", c.name, got, c.wantProblems, problems)
		}
	}
}
