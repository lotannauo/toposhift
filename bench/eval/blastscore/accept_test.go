package blastscore

import (
	"math"
	"testing"
)

// good is evidence that is Good, with room on every bound. The boundary tables
// move one number at a time.
func good() Evidence {
	return Evidence{
		Cases: 20, Recall: 0.85, RecallLower: 0.70, Lift: 2, LiftLower: 1.3,
		HasComparator: true, DiffF1: 0.05, DiffLower: 0.0, DiffUpper: 0.1,
	}
}

func TestJudgeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Evidence)
		want Verdict
	}{
		{"all room", func(*Evidence) {}, Good},
		{"recall 0.80 is good", func(e *Evidence) { e.Recall = 0.80 }, Good},
		{"recall 0.7999 is mixed", func(e *Evidence) { e.Recall = 0.7999 }, Mixed},
		{"recall 0.60 is mixed", func(e *Evidence) { e.Recall = 0.60 }, Mixed},
		{"recall 0.5999 is poor", func(e *Evidence) { e.Recall = 0.5999 }, Poor},
		{"recall lower bound 0.65 is good", func(e *Evidence) { e.RecallLower = 0.65 }, Good},
		{"recall lower bound 0.6499 is mixed", func(e *Evidence) { e.RecallLower = 0.6499 }, Mixed},
		{"lift 1.5 is good", func(e *Evidence) { e.Lift = 1.5 }, Good},
		{"lift 1.4999 is mixed", func(e *Evidence) { e.Lift = 1.4999 }, Mixed},
		{"lift 1.10 is mixed", func(e *Evidence) { e.Lift = 1.10; e.LiftLower = 1.05 }, Mixed},
		{"lift 1.0999 is poor", func(e *Evidence) { e.Lift = 1.0999; e.LiftLower = 1.05 }, Poor},
		{"a lift lower bound of 1.0 is not good", func(e *Evidence) { e.LiftLower = 1.0 }, Mixed},
		{"a lift lower bound of 1.0001 can be good", func(e *Evidence) { e.LiftLower = 1.0001 }, Good},
		{"a difference of 0 is good", func(e *Evidence) { e.DiffF1 = 0 }, Good},
		{"a difference of -0.0001 is mixed", func(e *Evidence) { e.DiffF1 = -0.0001; e.DiffLower = -0.05; e.DiffUpper = 0.05 }, Mixed},
		{"a lower bound of -0.10 passes", func(e *Evidence) { e.DiffLower = -0.10 }, Good},
		{"a lower bound of -0.1001 is mixed", func(e *Evidence) { e.DiffLower = -0.1001 }, Mixed},
		{"an upper bound of 0 is not poor", func(e *Evidence) { e.DiffF1 = -0.05; e.DiffLower = -0.1; e.DiffUpper = 0 }, Mixed},
		{"an upper bound of -0.0001 is poor", func(e *Evidence) { e.DiffF1 = -0.05; e.DiffLower = -0.1; e.DiffUpper = -0.0001 }, Poor},
		{"without a comparator it is not good", func(e *Evidence) { e.HasComparator = false }, Mixed},
		{"without a comparator it can be poor", func(e *Evidence) { e.HasComparator = false; e.Recall = 0.5 }, Poor},
		{"8 cases are judged", func(e *Evidence) { e.Cases = 8 }, Good},
		{"7 cases cannot say", func(e *Evidence) { e.Cases = 7 }, CannotSay},
		{"an undefined lift cannot say", func(e *Evidence) { e.Lift = math.NaN() }, CannotSay},
		{"an infinite lift cannot say", func(e *Evidence) { e.Lift = math.Inf(1) }, CannotSay},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := good()
			tc.edit(&e)
			if got, _ := Judge(e); got != tc.want {
				t.Errorf("Judge(%+v) = %s, want %s", e, got, tc.want)
			}
		})
	}
}

// The cut-points are the frozen numbers, with the date they were fixed on.
func TestCutPointsAreFrozen(t *testing.T) {
	if AcceptanceDate != "2026-10-15" {
		t.Errorf("date %s", AcceptanceDate)
	}
	if GoodRecall != 0.80 || GoodRecallLower != 0.65 || GoodLift != 1.5 || GoodLiftLower != 1.0 ||
		NonInferiorityMargin != 0.10 || PoorRecall != 0.60 || PoorLift != 1.10 || MinCases != 8 {
		t.Error("a cut-point moved")
	}
	if UnresolvedTruthNum*5 != UnresolvedTruthDen || LoadFailureNum*4 != LoadFailureDen {
		t.Error("a coverage rule moved")
	}
	if Resamples != 10000 || BootstrapSeed != 20261015 || Confidence != 0.95 {
		t.Error("a statistic moved")
	}
}
