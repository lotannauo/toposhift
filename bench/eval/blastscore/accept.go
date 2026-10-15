package blastscore

import "math"

// The acceptance rule, fixed on 2026-10-15 before any case was scored or any
// fact loaded. The cut-points are policy; the statistics around them (lower
// bounds, a paired bootstrap, "not resolved") are not tunable. They change only
// by a new dated comment beside this one, never by editing the date or a number
// in place.
const (
	// AcceptanceDate is the day the rule below was fixed.
	AcceptanceDate = "2026-10-15"

	// GoodRecall is the micro recall a Good stratum reaches, and GoodRecallLower
	// the least its 95 % lower bound may be.
	GoodRecall      = 0.80
	GoodRecallLower = 0.65
	// GoodLift is the lift over B_all a Good stratum reaches. Its 95 % lower bound
	// must be above GoodLiftLower.
	GoodLift      = 1.5
	GoodLiftLower = 1.0
	// NonInferiorityMargin is how far below zero the 95 % lower bound of
	// F1(W) - F1(B_1hop) may be in a Good stratum, whose difference itself must
	// reach zero.
	NonInferiorityMargin = 0.10

	// PoorRecall and PoorLift are the points below which a stratum is Poor. A
	// stratum is also Poor when the 95 % upper bound of F1(W) - F1(B_1hop) is
	// below zero: the walk is resolvably worse than one hop.
	PoorRecall = 0.60
	PoorLift   = 1.10

	// MinCases is the fewest scorable cases a stratum may have and still be
	// judged.
	MinCases = 8

	// A stratum is "cannot say" when more than UnresolvedTruthNum/UnresolvedTruthDen
	// of its truth entities are unmappable, unplaced or uninstrumented, and every
	// stratum is when more than LoadFailureNum/LoadFailureDen of the dataset's
	// cases fail the load check, uninstrumented seeds excluded first. The rules
	// are fractions of integers, compared without floating point.
	UnresolvedTruthNum, UnresolvedTruthDen = 1, 5
	LoadFailureNum, LoadFailureDen         = 1, 4
)

// The statistics, fixed with the cut-points above on 2026-10-15.
const (
	// Resamples is the number of case-bootstrap resamples.
	Resamples = 10_000
	// BootstrapSeed seeds every resample draw.
	BootstrapSeed = 20261015
	// Confidence is the level of every interval. The percentile ranks are written
	// for it in integers, so it is a label here and not a parameter.
	Confidence = 0.95
)

// Verdict is the outcome of the acceptance rule for one stratum.
type Verdict string

const (
	// Good needs every positive criterion, each on a lower bound.
	Good Verdict = "good"
	// Poor needs one negative criterion, each on a point (or an upper bound).
	Poor Verdict = "poor"
	// Mixed is everything else, reported as it is.
	Mixed Verdict = "mixed"
	// CannotSay is a stratum the data cannot judge.
	CannotSay Verdict = "cannot_say"
)

// Reason says why a stratum or a case cannot be judged. The strings are a fixed
// vocabulary, safe to print in results.
type Reason string

const (
	// ReasonFewCases is a stratum with fewer than MinCases scorable cases.
	ReasonFewCases Reason = "fewer_than_8_scorable_cases"
	// ReasonSingleNode is a co-residency stratum that reaches MinCases only
	// before the single-node rule removes its one-node cases.
	ReasonSingleNode Reason = "fewer_than_8_cases_after_single_node_rule"
	// ReasonUnresolvedTruth is a stratum more than a fifth of whose truth cannot
	// be predicted at all.
	ReasonUnresolvedTruth Reason = "over_20_percent_of_truth_unresolved"
	// ReasonLoadFailures is a dataset more than a quarter of whose cases failed
	// the load check.
	ReasonLoadFailures Reason = "over_25_percent_of_cases_failed_to_load"
	// ReasonLiftUndefined is a stratum whose base rate is zero.
	ReasonLiftUndefined Reason = "lift_undefined"
	// ReasonNoComparator is a stratum with no B_1hop to compare with, which can be
	// Poor or Mixed but not Good.
	ReasonNoComparator Reason = "no_one_hop_comparator"

	// ReasonSeedAbsent is a case whose seed has no existence in the window, and
	// ReasonQuarantinedSeed one whose seed the store quarantined. A quarantined
	// seed reaches nothing.
	ReasonSeedAbsent      Reason = "seed_absent_in_window"
	ReasonQuarantinedSeed Reason = "quarantined_seed"
	// ReasonNoDependsOn is a service-level case with no depends_on piece in the
	// window.
	ReasonNoDependsOn Reason = "no_depends_on_in_window"
	// ReasonSnapshotOutside is a case whose Kubernetes snapshot lies outside the
	// window, so the pod-level facts say nothing about it.
	ReasonSnapshotOutside Reason = "snapshot_outside_window"
	// ReasonNoSeedPods is a service seed with no pod to seed co-residency from.
	ReasonNoSeedPods Reason = "no_seed_pods"
	// ReasonSingleNodeCase is a predictor not scored on a case whose cluster has
	// one node, where co-residency equals "predict everything".
	ReasonSingleNodeCase Reason = "single_node_cluster"
)

// Evidence is what the rule reads of one stratum. Every field is a number the
// report computes; a NaN is a number that is not defined.
type Evidence struct {
	// Cases is the number of scorable cases.
	Cases int
	// Recall is the micro recall, with the lower bound of its 95 % interval.
	Recall, RecallLower float64
	// Lift is the micro precision over B_all's, with the lower bound of its
	// interval.
	Lift, LiftLower float64
	// HasComparator says B_1hop was scored on the same cases. DiffF1 is
	// F1(predictor) - F1(B_1hop) and DiffLower and DiffUpper the ends of its
	// interval, taken on the same resamples.
	HasComparator                bool
	DiffF1, DiffLower, DiffUpper float64
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// Judge applies the rule to a stratum that can be judged. It checks Poor before
// Good, but no stratum is both: the two never share a criterion's side. It
// returns the reasons that bear on the verdict.
//
// Good needs all of: recall >= 0.80 with a lower bound >= 0.65; lift >= 1.5
// with a lower bound > 1.0; and a difference from B_1hop >= 0 with a lower
// bound >= -0.10. Poor is any of: recall < 0.60; lift < 1.10; the difference's
// upper bound < 0. Mixed is everything else. Fewer than MinCases cases is
// "cannot say".
func Judge(e Evidence) (Verdict, []Reason) {
	if e.Cases < MinCases {
		return CannotSay, []Reason{ReasonFewCases}
	}
	if !finite(e.Recall) || !finite(e.Lift) {
		return CannotSay, []Reason{ReasonLiftUndefined}
	}
	if e.Recall < PoorRecall || e.Lift < PoorLift || (e.HasComparator && e.DiffUpper < 0) {
		return Poor, nil
	}
	good := e.Recall >= GoodRecall && e.RecallLower >= GoodRecallLower &&
		e.Lift >= GoodLift && e.LiftLower > GoodLiftLower &&
		e.HasComparator && e.DiffF1 >= 0 && e.DiffLower >= -NonInferiorityMargin
	if good {
		return Good, nil
	}
	if !e.HasComparator {
		return Mixed, []Reason{ReasonNoComparator}
	}
	return Mixed, nil
}
