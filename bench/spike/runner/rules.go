package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RulesVersion is bumped when a rule changes its meaning.
const RulesVersion = 3

// Rules are the constants of the decision between the layouts, fixed before the
// measurements that decide (the retained-window builds and the timing on CI
// hardware) so that the numbers cannot choose their own thresholds. The values
// below were set after the untimed three-day runs made with this change, which the
// log at the end of this comment records; counters do not depend on timing, so
// those runs showed the values a timed run would. The rule itself, in words:
//
//   - G0: every candidate answers every query as the reference engine does.
//   - G1: the work of a read on the busiest prefixes must fit a budget at the
//     retention the design runs at, and must still fit if that retention doubles.
//     For each pinned query q (a prefix at an instant of G1Instants) and counter k
//     of G1Counters, W_q(R) is k's value in the build that has always held R days
//     (Windows; never a store that was shrunk to R, which folds a long run's
//     extensions into the baseline and flatters layout L). A window with pins is a
//     projection, whose block counters (block_loads and cold_block_bytes) depend on the
//     depth of an index that is not a full store's, in level and in slope: those
//     cells are reported and not decided, whether they are over or under, until the
//     offset against a full build is recorded. The budget ratio is
//     b_q = W_q(T)/Budget_k(q) at T = TargetWindow, where
//     Budget_k(q) = Base_k*E_q + PerItem_k*D_q/ItemsPerUnit_k, E_q is the number of
//     entities the read asks about and D_q the size of the reference engine's answer
//     (G1Budget; a budget in blocks is multiplied by the block size of the runs).
//     The growth is s_q = log(W_q(T)/W_q(T'))/log(T/T') over the last two windows,
//     and is 0 when W_q(T) is at or below k's floor. The projected ratio is
//     p_q = b_q * G1Headroom^max(s_q, 0): what the read would cost if the retention
//     were multiplied by G1Headroom. A population passes if its statistic of p_q is
//     at most 1: the largest over a hot population, the median over the median one.
//     A candidate passes G1 if every population of every class passes at every
//     instant on every counter. The slope over the first and third windows (2 and
//     14 days) and the classes bounded (s at or below BoundedSlope) and linear (s at
//     or above LinearSlope) are diagnostics, and feed SlopeDiffForTarget; they do not
//     decide.
//   - G2: a retention must not stall ingest for longer than the budget, and the
//     block bytes right after it must stay within a factor of the settled value.
//   - G3: the time to commit a batch while building, and the first batch after
//     a retention, within a factor of the best candidate.
//   - G4: physical bytes per record within a factor of the best candidate.
//
// How the gates combine. G0 comes first. G1 and G2 are absolute gates on each
// candidate; G3 and G4 are ratios to the best candidate that passes G0. Only a
// candidate that passes every gate enters the levels, the tolerance and the
// tie-break below, and the specific rules between two candidates (CheckpointReadGain,
// LayoutMGain) apply only between candidates that both pass: if layout L without
// checkpoints fails G1 and layout L with checkpoints passes, the gate decides, not
// the gain rule. If no candidate passes G1 the design is reopened on the node-reverse
// failure the Stage 3 plan names, and the report lists each candidate's cells with
// the largest p_q first.
//
// How counters are combined. A counter orders two candidates in a cell (a query
// group at an age) if the larger value is above the counter's floor and more than
// OrderTolerance times the smaller, where a candidate's value in a query is the
// counter of that query. A query orders a pair of candidates one way if some counter
// orders it that way and none orders it the other way; it is mixed if counters order
// it in opposite directions (the usual case: one layout seeks to every key, the
// other walks a run). A cell is ordered one way if at least CellMajority of its
// queries are ordered that way and none the other; it is mixed if any query is mixed
// or some queries are ordered each way; and it is tied on counters
// otherwise. (A mean over the cell would hide a mixed cell whose queries cancel, and
// invent one from two prefixes that each a different counter orders oppositely.) Counters decide wherever they order a
// cell. The report lists the mixed cells for every pair of candidates, with a digest
// of the list; the list is a function of the counters alone, and it is fixed before
// any timing is collected. Timing settles only the mixed cells, never reverses a counter
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
// The counters the rules name that are not Pebble's iterator statistics are
// recorded by the read step. block_loads and cold_block_bytes are what a read has
// to bring in with the block cache emptied and the tables open: the blocks it
// misses in the cache and the compressed bytes of the index, filter and data blocks
// among them, asked twice and required to agree. allocs is the fewest allocations
// of three runs of the read after two to fill the pools, with the collector off; it
// depends on the process and is never required to repeat. It is one of the counters
// a query is ordered by like the others, so until it is made repeatable it can make a
// query ordered or mixed by noise.
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
// Log (2026-10-04, with the counters above and the list of mixed cells, before any
// build at a retained window and before any timing): the value of a candidate in a
// cell is its mean over the queries of the cell (CellValue), the unit the report
// already printed; block_loads, cold_block_bytes and allocs are defined as above;
// RulesVersion is 2. Nothing else was changed.
//
// Log (2026-10-05, before any build at a retained window and before any timing; set
// by the Stage 3 plan and confirmed by the owner): G1 is one test, a budget that must fit at TargetWindow and still fit
// when the retention is multiplied by G1Headroom (2), and the slope classes became
// diagnostics (the budget used to apply only to a class the slope left in between,
// which the Stage 3 plan contradicts); G1Budget is a struct (base per entity read, a
// share per item of the answer) with seeks 8 + 4, steps plus decoded entries 128 + 5
// (twice DefaultKMin per entity read and DefaultAlpha + 1 per item), block loads
// 8 + 1 per 64 items and cold bytes 6 blocks + 1 block per 64 items; the replay bound
// CheckpointReplayRecords (256) was removed, because the bound the checkpoint policy
// gives is about max(K_min, alpha*D) + D and 256 fails whenever D is above about 50;
// a cell is judged by the verdicts of its queries (CellMajority 2/3) and not by a
// mean, which hides and invents mixed cells; the values StallBudgetSeconds,
// PostRetentionBytes, CommitFactor, BytesPerRecordFactor, CheckpointReadGain,
// CheckpointExtraBytes, LayoutMGain, DefaultKMin, DefaultAlpha, FilterWindowCut,
// ReopenCheckpointWrites, SlopeDiffForTarget and the windows were confirmed as set,
// and the placeholders are the timing values and ReopenDeadlineIndex (whose model
// changed: a bucketed index, compared in write bytes). RulesVersion is 3.
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
	// or above the second linear: classes the report prints, which do not decide G1.
	BoundedSlope, LinearSlope float64
	// G1Budget is, per counter of G1Counters, what a read may cost at TargetWindow:
	// see [Budget]. G1Headroom is the factor the retention is multiplied by in the
	// projected ratio of G1.
	G1Budget   map[string]Budget
	G1Headroom float64
	// G1Counters are the counters G1 judges against their budgets.
	G1Counters []string
	// OrderCounters, OrderTolerance and CounterFloors say when a counter orders
	// two candidates in a cell.
	OrderCounters  []string
	OrderTolerance float64
	CounterFloors  map[string]int64
	// CellValue and CellMajority say how the verdicts of the queries of a cell make
	// the verdict of the cell.
	CellValue    string
	CellMajority float64
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
	// DefaultKMin and DefaultAlpha are the checkpoint defaults the ties of the
	// sweep go to.
	DefaultKMin  int
	DefaultAlpha float64
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
		CellValue:      "the verdict of each query, by the counters",
		CellMajority:   2.0 / 3,
		CounterFloors: map[string]int64{
			"seeks": 8, "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded": 256, "block_loads": 8, "cold_block_bytes": 64 << 10, "value_bytes": 16 << 10, "allocs": 64,
		},
		SlopeDiffForTarget: 0.25,
		TimingRegime:       "warm, zero cache misses in the timed repetitions", TimingWarmup: 3, TimingReps: 31,
		G1Headroom: 2,
		G1Budget: map[string]Budget{
			"seeks": {Base: 8, PerItem: 4, ItemsPerUnit: 1},
			// 2 x DefaultKMin per entity read (a tail and one checkpoint interval) and
			// DefaultAlpha + 1 per item (the tail grows with the answer, and so do the
			// checkpoint entries decoded).
			"internal_steps+checkpoint_entries_decoded+baseline_entries_decoded": {Base: 128, PerItem: 5, ItemsPerUnit: 1},
			"block_loads":      {Base: 8, PerItem: 1, ItemsPerUnit: 64},
			"cold_block_bytes": {Base: 6, PerItem: 1, ItemsPerUnit: 64, InBlocks: true},
		},
		TimingReaders: 1, TimingConfirm: 4, TimingRoundsLocal: 8, TimingJobsCI: 5,
		TimingMinRatio: 1.10, PebbleCPUShareMin: 0.5,
		TimingFloor:        "the 95th percentile, over pairs of the same candidate, of the largest absolute log ratio over cells",
		StallBudgetSeconds: 60, PostRetentionBytes: 2,
		CommitFactor: 2, BytesPerRecordFactor: 2,
		CheckpointReadGain: 2, CheckpointExtraBytes: 0.25,
		LayoutMGain: 1.5,
		DefaultKMin: 64, DefaultAlpha: 4,
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
		Placeholders:  []string{"Timing*", "PebbleCPUShareMin", "ReopenDeadlineIndex"},
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

// Budget is what a read of one counter may cost: Base for each entity the read asks
// about, and PerItem for each ItemsPerUnit items in the reference engine's answer.
// InBlocks says the figures are in blocks of the runs' block size (a cold read
// loads whole blocks, so the budget of its bytes scales with them).
type Budget struct {
	Base, PerItem, ItemsPerUnit int64
	InBlocks                    bool
}

// For is the budget of a read of the given number of entities with an answer of
// the given size, for runs with the given block size.
func (b Budget) For(entities, items int, blockBytes int64) float64 {
	unit := 1.0
	if b.InBlocks {
		unit = float64(blockBytes)
	}
	per := float64(b.PerItem)
	if b.ItemsPerUnit > 1 {
		per /= float64(b.ItemsPerUnit)
	}
	return unit * (float64(b.Base)*float64(entities) + per*float64(items))
}
