package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RulesVersion is bumped when a rule changes its meaning.
const RulesVersion = 1

// Rules are the constants of the decision between the layouts, fixed before the
// measurements that decide (the retained-window builds and the timing on CI
// hardware) so that the numbers cannot choose their own thresholds. The values
// below were set after the untimed three-day runs made with this change, which the
// log at the end of this comment records; counters do not depend on timing, so
// those runs showed the values a timed run would. The rule itself, in words:
//
//   - G0: every candidate answers every query as the reference engine does.
//   - G1: the work of a read on the busiest prefixes must not grow with the
//     history the prefix retains. The work is each of G1Counters, and the growth
//     of a counter is the slope s = log(W_b/W_a)/log(b/a) of its value against the
//     retained days, between the first window and the third (2 and 14 days), and,
//     if that is in between, between the last two (14 and 30). The largest slope
//     among the counters above their floor is the class's: at or below
//     BoundedSlope bounded, at or above LinearSlope linear. A class still in
//     between is judged against G1Budget at TargetWindow. G1 takes slopes only from
//     stores that have always held the window, never from one that was shrunk to it:
//     shrinking folds a long run's extensions into the baseline, which flatters
//     layout L.
//   - G2: a retention must not stall ingest for longer than the budget, and the
//     block bytes right after it must stay within a factor of the settled value.
//   - G3: the time to commit a batch while building, and the first batch after
//     a retention, within a factor of the best candidate.
//   - G4: physical bytes per record within a factor of the best candidate.
//
// How counters are combined. A counter orders two candidates in a cell (a query
// group at an age) if the larger value is above the counter's floor and more than
// OrderTolerance times the smaller. A cell is ordered if some counter orders it and
// none orders it the other way; it is mixed if counters order it in opposite
// directions (the usual case: one layout seeks to every key, the other walks a run);
// it is tied on counters if none orders it. Counters decide wherever they order a
// cell. The report will list the mixed cells (it does not yet: that is for the
// change that adds the counters below), and that list is fixed before any timing is
// collected. Timing settles only the mixed cells, never reverses a counter
// verdict, and a timing that contradicts one beyond the floor is a defect to explain
// (a missing counter, allocation, collection) and not a result. A mixed cell is
// settled by warm timing on CI hardware when the sign is the same in every job of
// each architecture, the median ratio is beyond the larger of the family-wise noise
// floor and TimingMinRatio, and at least PebbleCPUShareMin of the profile is in
// Pebble for both candidates; the local run must point the same way, and where it
// does not the CI result decides and the disagreement is recorded. Otherwise the cell
// is a tie. If the slopes of the two candidates differ by SlopeDiffForTarget or
// more, the timing is taken at TargetWindow, because the verdict can change with the
// retained window.
//
// A candidate beats another only if it is better, by counters or by timing, on one
// or more cells of the first level and worse on none. A tie at a level is settled
// on the next (Levels, in order), and a tie at all of them goes to the simpler
// design (TieBreak). The bytes served from the block cache and the gross block bytes
// (every load, a cached block too, again at each seek that reloads it) are
// diagnostics and are never decided on.
//
// Some counters the rules name are not recorded yet: block_loads, cold_block_bytes
// and allocs are for the next change, and a decision cannot be read before they are.
//
// Log of what was set after the first look at results (2026-10-04, after the
// untimed three-day runs): the combination rule (counters decide, timing settles
// only mixed cells), OrderTolerance, CounterFloors, the windows 2, 7, 14 and 30 with
// TargetWindow 30, and the G1 counters including the entries decoded from
// checkpoints and the baseline. The first values of the retention and the old
// snapshot instant were also changed after the first runs, because the reads a day
// back and of the old snapshot had been aligned with the retention instant or
// emptied by it.
//
// Fields in Placeholders are values nobody has chosen yet: they are here so the
// runner and its report can be written, and the owner sets them before a result
// is read against them.
type Rules struct {
	Version int

	// Windows are the retained windows, in days, G1 is judged over; TargetWindow
	// is the last of them, the retention the design is meant to run at (the
	// design plan's candidate for the second layer).
	Windows      []int
	TargetWindow int
	// BoundedSlope and LinearSlope: a slope at or below the first is bounded, at
	// or above the second linear; in between, the slope is taken again from the
	// last two windows with the same thresholds, and if it is still in between the
	// class is judged against the absolute budget G1Budget at TargetWindow.
	BoundedSlope, LinearSlope float64
	// G1Budget is, per counter, what a read may cost at TargetWindow; nobody has
	// set it, and without it a class that stays in between is not decided.
	G1Budget map[string]int64
	// G1Counters are the counters whose slopes G1 takes the largest of, among those
	// above their floor.
	G1Counters []string
	// OrderCounters, OrderTolerance and CounterFloors say when a counter orders
	// two candidates in a cell.
	OrderCounters  []string
	OrderTolerance float64
	CounterFloors  map[string]int64
	// SlopeDiffForTarget: see above.
	SlopeDiffForTarget float64
	// The timing that settles an unordered cell: warm cache with no misses, a
	// number of warm-up and timed repetitions per query, readers (and the number
	// in the confirming run), rounds locally and jobs per architecture on CI, the
	// smallest ratio that counts, how the noise floor is derived and the smallest
	// share of CPU samples in Pebble.
	TimingRegime                      string
	TimingWarmup, TimingReps          int
	TimingReaders, TimingConfirm      int
	TimingRoundsLocal, TimingJobsCI   int
	TimingMinRatio, PebbleCPUShareMin float64
	TimingFloor                       string

	// G2: the stall a retention may cause, in seconds, and the factor its block
	// bytes may exceed the settled value by.
	StallBudgetSeconds float64
	PostRetentionBytes float64
	// G3 and G4: factors over the best candidate.
	CommitFactor, BytesPerRecordFactor float64

	// CheckpointReadGain and CheckpointExtraBytes: checkpoints are kept over none
	// only if the hot-prefix read at "now" improves by at least the first factor
	// for at most the second share of extra bytes a day.
	CheckpointReadGain, CheckpointExtraBytes float64
	// LayoutMGain: layout M over the best layout L only if at least this much
	// better on the first-level metrics, with no gate failing.
	LayoutMGain float64
	// CheckpointReplayRecords bounds the records a hot-prefix read at the 99th
	// percentile may replay when choosing checkpoint defaults; ties go to
	// DefaultKMin and DefaultAlpha.
	CheckpointReplayRecords int
	DefaultKMin             int
	DefaultAlpha            float64
	// FilterWindowCut: the time-interval filter stays only if it cuts the block
	// bytes of a day's window by at least this share.
	FilterWindowCut float64

	// ReopenCheckpointWrites and ReopenDeadlineIndex: the design is reopened if
	// checkpoint writes exceed the first share of the bytes written, or a deadline
	// index would cost more than the second share of the bytes a day.
	ReopenCheckpointWrites, ReopenDeadlineIndex float64

	// TieBreak is the order a tie is settled in: the simpler design wins. Its
	// entries are designs, not variants: layout L without checkpoints, layout M (any
	// of its variants), layout L with checkpoints (any policy).
	TieBreak []string
	// Levels are the groups of metrics a candidate must beat another on; a
	// candidate beats another only if it is better beyond the noise floor on one or
	// more of the first level and worse on none.
	Levels [4][]string
	// G1 is where the growth with retained history is judged: the instants, and
	// the prefixes read. The populations map to the plan's groups of queries by
	// selection: "churn" is the groups chosen as the busiest by records of the
	// prefixes a node or a service collects, "heartbeat" the groups chosen as the
	// busiest by run extensions, and "median" the groups chosen around the middle.
	G1Instants, G1Populations []string

	// Placeholders names the fields above that nobody has chosen.
	Placeholders []string
}

// DefaultRules are the rules as they stand.
func DefaultRules() Rules {
	return Rules{
		Version: RulesVersion,
		Windows: []int{2, 7, 14, 30}, TargetWindow: 30, BoundedSlope: 0.25, LinearSlope: 0.75,
		G1Counters:     []string{"seeks", "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded", "block_loads", "cold_block_bytes"},
		OrderCounters:  []string{"seeks", "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded", "block_loads", "cold_block_bytes", "value_bytes", "allocs"},
		OrderTolerance: 1.10,
		CounterFloors: map[string]int64{
			"seeks": 8, "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded": 256, "block_loads": 8, "cold_block_bytes": 64 << 10, "value_bytes": 16 << 10, "allocs": 64,
		},
		SlopeDiffForTarget: 0.25,
		TimingRegime:       "warm, zero cache misses in the timed repetitions", TimingWarmup: 3, TimingReps: 31,
		TimingReaders: 1, TimingConfirm: 4, TimingRoundsLocal: 8, TimingJobsCI: 5,
		TimingMinRatio: 1.10, PebbleCPUShareMin: 0.5,
		TimingFloor:        "the 95th percentile, over pairs of the same candidate, of the largest absolute log ratio over cells",
		StallBudgetSeconds: 60, PostRetentionBytes: 2,
		CommitFactor: 2, BytesPerRecordFactor: 2,
		CheckpointReadGain: 2, CheckpointExtraBytes: 0.25,
		LayoutMGain:             1.5,
		CheckpointReplayRecords: 256, DefaultKMin: 64, DefaultAlpha: 4,
		FilterWindowCut:        0.3,
		ReopenCheckpointWrites: 0.2, ReopenDeadlineIndex: 0.3,
		TieBreak: []string{"L/off", "M", "L/checkpoints"},
		Levels: [4][]string{
			{"reads at now", "reads an hour back"},
			{"bytes per record"},
			{"reads a day back", "reads of an old snapshot"},
			{"windows"},
		},
		G1Instants:    []string{AgeNow, Age1d},
		G1Populations: []string{"hottest 10 churn prefixes", "hottest 10 heartbeat prefixes", "median prefix"},
		Placeholders: []string{
			"Windows", "TargetWindow", "G1Budget", "G1Counters", "OrderCounters", "OrderTolerance", "CounterFloors", "SlopeDiffForTarget", "Timing*", "PebbleCPUShareMin", "StallBudgetSeconds", "PostRetentionBytes", "CommitFactor", "BytesPerRecordFactor",
			"CheckpointReadGain", "CheckpointExtraBytes", "LayoutMGain", "CheckpointReplayRecords", "DefaultKMin", "DefaultAlpha",
			"FilterWindowCut", "ReopenCheckpointWrites", "ReopenDeadlineIndex",
		},
	}
}

// Digest is the SHA-256 of the rules, as hex.
func (r Rules) Digest() (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
