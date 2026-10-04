package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// steps records what a run asks for.
type steps struct {
	calls   []string
	failing map[string]bool // "build -candidate X"
	onCall  func(call string)
}

func (s *steps) step(cmd string, extra ...string) error {
	call := cmd + " " + strings.Join(extra, " ")
	s.calls = append(s.calls, call)
	if s.onCall != nil {
		s.onCall(call)
	}
	if s.failing[call] {
		return errors.New("failed")
	}
	return nil
}

func nothingOnDisk(string) candidateState { return candidateState{} }

func TestRunCandidatesKeepsGoingPastAFailure(t *testing.T) {
	t.Parallel()

	s := &steps{failing: map[string]bool{"build -candidate B": true}}
	// B fails to build, and has results from an earlier plan: they are not reported.
	state := func(n string) candidateState { return candidateState{hasResults: n == "B"} }
	ran, failed := runCandidates(context.Background(), []string{"A", "B", "C"}, s.step, state)
	if want := []string{"build -candidate A", "read -candidate A", "build -candidate B", "build -candidate C", "read -candidate C"}; !slices.Equal(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
	if !slices.Equal(ran, []string{"A", "C"}) || len(failed) != 1 || !strings.Contains(failed[0], "build of B") {
		t.Errorf("ran %v, failed %v", ran, failed)
	}
}

// A read that finds answers wrong fails the step and its results are reported all the same.
func TestRunCandidatesReportsAReadThatFoundWrongAnswers(t *testing.T) {
	t.Parallel()

	s := &steps{failing: map[string]bool{"read -candidate A": true, "read -candidate B": true}}
	state := func(n string) candidateState { return candidateState{hasResults: n == "A"} } // B's read crashed before writing
	ran, failed := runCandidates(context.Background(), []string{"A", "B"}, s.step, state)
	if !slices.Equal(ran, []string{"A"}) || len(failed) != 2 {
		t.Errorf("ran %v, failed %v", ran, failed)
	}
}

// Once told to stop, a run starts no further candidate and says so.
func TestRunCandidatesStopsWhenToldTo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	s := &steps{onCall: func(call string) {
		if call == "read -candidate A" {
			cancel()
		}
	}}
	ran, failed := runCandidates(ctx, []string{"A", "B", "C"}, s.step, nothingOnDisk)
	if want := []string{"build -candidate A", "read -candidate A"}; !slices.Equal(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
	if !slices.Equal(ran, []string{"A"}) || len(failed) != 1 || failed[0] != "stopped before B" {
		t.Errorf("ran %v, failed %v", ran, failed)
	}
}

// Running again resumes: what was built and read from the plan is not built again,
// and what was built and not read (an interrupted read) is only read.
func TestRunCandidatesResumes(t *testing.T) {
	t.Parallel()

	s := &steps{}
	state := func(n string) candidateState {
		switch n {
		case "A":
			return candidateState{built: true, done: true, hasResults: true}
		case "B":
			return candidateState{built: true}
		}
		return candidateState{}
	}
	ran, failed := runCandidates(context.Background(), []string{"A", "B", "C"}, s.step, state)
	if want := []string{"read -candidate B", "build -candidate C", "read -candidate C"}; !slices.Equal(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
	if !slices.Equal(ran, []string{"A", "B", "C"}) || len(failed) != 0 {
		t.Errorf("ran %v, failed %v", ran, failed)
	}
}

// What is on disk says whether a candidate is finished, built and not read, or
// neither: it counts only if it came from this plan, by this binary, in this mode.
func TestDiskStateReadsWhatARunLeft(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := runner.DefaultSpec(workload.Tiny())
	spec.BatchSize, spec.MinNonEmpty = 200, 0
	spec.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	plan, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	v, err := candidates.Lookup("L/off")
	if err != nil {
		t.Fatal(err)
	}
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	g := runner.Guards{Info: runner.BuildInfo{GoVersion: "t", Revision: "r", Executable: "exe"}}
	if got := diskState(dir, digest, "exe", false); got != (candidateState{}) {
		t.Errorf("nothing built: %+v", got)
	}
	if _, err := runner.Build(context.Background(), plan, v, dir, g, nil); err != nil {
		t.Fatal(err)
	}
	if got := diskState(dir, digest, "exe", false); got != (candidateState{built: true}) {
		t.Errorf("built, not read: %+v", got)
	}
	if _, err := runner.Read(context.Background(), plan, v, dir, g, nil); err != nil {
		t.Fatal(err)
	}
	if got := diskState(dir, digest, "exe", false); got != (candidateState{built: true, done: true, hasResults: true}) {
		t.Errorf("built and read: %+v", got)
	}
	for name, got := range map[string]candidateState{
		"another plan":   diskState(dir, "other", "exe", false),
		"another binary": diskState(dir, digest, "other", false),
		"another mode":   diskState(dir, digest, "exe", true),
	} {
		if got != (candidateState{hasResults: true}) {
			t.Errorf("%s: %+v, want only the results that are there", name, got)
		}
	}
}
