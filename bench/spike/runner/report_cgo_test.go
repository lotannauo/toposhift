package runner_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

const cgoProblem = "built or read by a binary built with cgo"

// cgoSet is a set of one candidate, named as an external one is, that agrees with its
// plan in everything but what a test changes. The plan has no queries, so none are
// asked of the results.
func cgoSet(t *testing.T, cgoManifest, cgoResults bool) (*runner.Plan, []*runner.Candidate) {
	t.Helper()
	plan := &runner.Plan{Spec: runner.DefaultSpec(workload.Tiny())}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	build := runner.BuildInfo{GoVersion: "go", GOOS: "os", GOARCH: "arch", Revision: "r", Executable: "exe"}
	mb, rb := build, build
	mb.CGO, rb.CGO = cgoManifest, cgoResults
	return plan, []*runner.Candidate{{
		Manifest: &runner.Manifest{Candidate: "RX/off", PlanDigest: digest, Build: mb},
		Results:  &runner.Results{Candidate: "RX/off", PlanDigest: digest, Build: rb},
	}}
}

func TestCheckRefusesASetBuiltWithCgoUnlessUntimedResultsAreAllowed(t *testing.T) {
	t.Parallel()

	plan, cs := cgoSet(t, true, true)
	got := strings.Join(runner.Check(plan, cs, false), "\n")
	if !strings.Contains(got, "RX/off: "+cgoProblem) {
		t.Errorf("a set built and read with cgo, with no untimed result allowed: %q", got)
	}
	if got := strings.Join(runner.Check(plan, cs, true), "\n"); got != "" {
		t.Errorf("a set built and read with cgo, with untimed results allowed: %q", got)
	}
}

func TestCheckRaisesNoCgoProblemForASetBuiltWithoutCgo(t *testing.T) {
	t.Parallel()

	plan, cs := cgoSet(t, false, false)
	for _, allow := range []bool{false, true} {
		if got := runner.Check(plan, cs, allow); len(got) != 0 {
			t.Errorf("a set built and read without cgo, untimed allowed %v: %q", allow, got)
		}
	}
}

func TestCheckHoldsAgainstCgoOnEitherSideOfTheSet(t *testing.T) {
	t.Parallel()

	// Only the binary that read the set had cgo: both reasons are given.
	plan, cs := cgoSet(t, false, true)
	got := strings.Join(runner.Check(plan, cs, false), "\n")
	for _, want := range []string{cgoProblem, "built and read by different binaries"} {
		if !strings.Contains(got, want) {
			t.Errorf("a set read with cgo and built without: %q lacks %q", got, want)
		}
	}

	// Only the binary that built it had cgo.
	plan, cs = cgoSet(t, true, false)
	got = strings.Join(runner.Check(plan, cs, false), "\n")
	for _, want := range []string{cgoProblem, "built and read by different binaries"} {
		if !strings.Contains(got, want) {
			t.Errorf("a set built with cgo and read without: %q lacks %q", got, want)
		}
	}
}

func TestCheckWindowsNamesTheWindowOfASetBuiltWithCgo(t *testing.T) {
	t.Parallel()

	ws := goodWindows()
	for _, w := range ws {
		for _, c := range w.Candidates {
			c.Manifest.Untimed, c.Results.Untimed = false, false
			// the plan, stream and queries are not what this test is about
			digest, err := w.Plan.Digest()
			if err != nil {
				t.Fatal(err)
			}
			c.Manifest.PlanDigest, c.Results.PlanDigest = digest, digest
			c.Manifest.Candidate, c.Results.Candidate = "RX/off", "RX/off"
		}
	}
	ws[1].Candidates[0].Manifest.Build.CGO = true
	ws[1].Candidates[0].Results.Build.CGO = true

	got := strings.Join(runner.CheckWindows(ws, false), "\n")
	if want := "window of 2 days: RX/off: " + cgoProblem; !strings.Contains(got, want) {
		t.Errorf("windows of which one has a set built with cgo: %q lacks %q", got, want)
	}
	for _, other := range []string{"window of 1 days: RX/off: " + cgoProblem, "window of 3 days: RX/off: " + cgoProblem} {
		if strings.Contains(got, other) {
			t.Errorf("windows of which one has a set built with cgo: %q holds %q", got, other)
		}
	}
	if got := strings.Join(runner.CheckWindows(ws, true), "\n"); strings.Contains(got, cgoProblem) {
		t.Errorf("windows with untimed results allowed: %q", got)
	}
}

func TestWriteMarksASetBuiltWithCgoAsNotToBeCompared(t *testing.T) {
	t.Parallel()

	for _, cgo := range []bool{false, true} {
		plan, cs := cgoSet(t, cgo, cgo)
		var out bytes.Buffer
		runner.Write(&out, plan, cs, false)
		got := out.String()
		if marked := strings.Contains(got, "THESE RESULTS ARE NOT TO BE COMPARED:\n  - RX/off: "+cgoProblem); marked != cgo {
			t.Errorf("a report of a set built with cgo %v marks it as not to be compared: %v\n%s", cgo, marked, got)
		}
	}
}
