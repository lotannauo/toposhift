package main

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// -phase-log is off by default, and then what is passed on to a build step and the
// build options are as they were; on, it is passed on to the steps and to the build.
func TestPhaseLogFlag(t *testing.T) {
	t.Parallel()

	off := parseBuild(t, false)
	if off.phaseLog || off.options().PhaseLog || slices.Contains(off.args(), "-phase-log") {
		t.Errorf("off by default: %+v, passed on as %v", off, off.args())
	}
	on := parseBuild(t, false, "-phase-log")
	if !on.phaseLog || !on.options().PhaseLog || !slices.Contains(on.args(), "-phase-log") {
		t.Errorf("on: %+v, passed on as %v", on, on.args())
	}
	if again := parseBuild(t, true, on.args()...); again != on {
		t.Errorf("passed on as %v, a step makes %+v, want %+v", on.args(), again, on)
	}
}

// phases reads a build's phase log: it says what the build lacks when there is no log,
// and otherwise prints a table for each class of batch.
func TestPhasesCommand(t *testing.T) {
	conformance.SkipWhenTrimmed(t)

	spec := runner.DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	spec.MinNonEmpty = 0
	plan, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := candidates.Lookup("Lroot/k4a2l1ns")
	if err != nil {
		t.Fatal(err)
	}
	g := runner.Guards{Info: runner.BuildInfo{GoVersion: "t", Revision: "r", Executable: "exe"}}
	out := t.TempDir()
	if _, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(out, v.Name), g, runner.BuildOptions{}, nil); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runPhases([]string{"-out", out, "-candidate", v.Name}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "-phase-log") {
		t.Errorf("no log: %v", err)
	}

	out = t.TempDir()
	if _, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(out, v.Name), g, runner.BuildOptions{PhaseLog: true}, nil); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := runPhases([]string{"-out", out, "-candidate", v.Name}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"never published", "class outside", "record_commit", "ckpt_commit", "= build+commit", "steadiness"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, stdout.String())
		}
	}
	stdout.Reset()
	if err := runPhases([]string{"-out", out, "-candidate", v.Name, "-class", "inside"}, &stdout, &stderr); err != nil || strings.Contains(stdout.String(), "class outside") {
		t.Errorf("-class inside: %v\n%s", err, stdout.String())
	}
	if err := runPhases([]string{"-candidate", v.Name}, &stdout, &stderr); err == nil {
		t.Error("no -out accepted")
	}
	stdout.Reset()
	if err := runPhases([]string{"-out", out, "-candidate", v.Name, "-tail", "0.9", "-class", "outside"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), "(the p90)") {
		t.Errorf("-tail 0.9: %v\n%s", err, stdout.String())
	}
	for _, bad := range []string{"0", "1", "1.5", "-0.1"} {
		if err := runPhases([]string{"-out", out, "-candidate", v.Name, "-tail", bad}, &stdout, &stderr); err == nil {
			t.Errorf("-tail %s accepted", bad)
		}
	}
}
