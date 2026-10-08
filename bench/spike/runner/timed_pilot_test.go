package runner_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// TestTimingOnThePilotArtifacts runs the judgement on the artifacts of a real run of the
// bench workflow. It is skipped unless SPIKEBENCH_PILOT names the directory they were
// downloaded to (and SPIKEBENCH_PILOT_COUNTERS the counters runs of the same plan), so
// CI skips it. It asserts words and never numbers; the revision check is taken to find
// the revision an ancestor, so that the reasons below it show.
func TestTimingOnThePilotArtifacts(t *testing.T) {
	dir := os.Getenv("SPIKEBENCH_PILOT")
	if dir == "" {
		t.Skip("SPIKEBENCH_PILOT does not name a directory of downloaded artifacts")
	}
	var counters []string
	if c := os.Getenv("SPIKEBENCH_PILOT_COUNTERS"); c != "" {
		counters = []string{c}
	}
	in := loadInputs(t, []string{dir}, counters)
	rep := runner.JudgeTiming(in, verified())
	t.Log("\n" + timingText(rep))

	// The pilot ran under rules 4: its plan was made under other rules than this binary's,
	// and its builds did not settle a retention's tombstones nor record the batches after one.
	if len(rep.Builds) != 6 {
		t.Fatalf("%d builds", len(rep.Builds))
	}
	if !slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.Contains(p, "was made under rules") }) {
		t.Errorf("problems %q do not say the plan was made under other rules", rep.Problems)
	}
	if rep.Judged || len(rep.Verdicts) == 0 {
		t.Errorf("judged %v with %d verdicts", rep.Judged, len(rep.Verdicts))
	}
	for _, v := range rep.Verdicts {
		if v.Judged {
			t.Errorf("a verdict of the pilot is judged: %+v", v)
		}
	}
	if rep.Revision == "" || len(rep.Runs) != 1 {
		t.Errorf("revision %q from runs %v", rep.Revision, rep.Runs)
	}
	// Without the plans the rules of the plan are not in question, and the builds' own
	// settings are what stops the judgement.
	in.Plans = nil
	rep = runner.JudgeTiming(in, verified())
	if len(rep.Problems) != 0 {
		t.Fatalf("problems %q", rep.Problems)
	}
	if !slices.ContainsFunc(rep.NotJudged, func(r string) bool {
		return strings.Contains(r, "these builds did not settle a retention's tombstones")
	}) {
		t.Errorf("not judged %q do not say the builds did not settle", rep.NotJudged)
	}
	for _, v := range rep.Verdicts {
		if v.Judged && v.Gate != runner.GateG0 {
			t.Errorf("a verdict of builds that did not settle is judged: %+v", v)
		}
	}
	// What the builds show is still shown: M/crdb1 stays out of the reference set, the
	// checkpoint policy is over G3 on aarch64, the architecture with two CPU models is not
	// comparable, and the bytes are within.
	if !rep.Join.Decided || rep.Join.Joins {
		t.Errorf("M/crdb1 did not stay out of the reference set: %+v", rep.Join)
	}
	if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); v.Pass {
		t.Errorf("the checkpoint policy passes G3 on aarch64: %+v", v)
	}
	for _, cand := range []string{cOff, cPolicy, cJoiner} {
		if v := mustRow(t, rep, 7, cand, runner.GateG3Commit, "x86_64"); !v.NotComparable {
			t.Errorf("%s G3 on x86_64 is compared: %+v", cand, v)
		}
		for _, arch := range []string{"aarch64", "x86_64"} {
			if v := mustRow(t, rep, 7, cand, runner.GateG4, arch); !v.Pass {
				t.Errorf("%s G4 on %s: %+v", cand, arch, v)
			}
		}
	}
}
