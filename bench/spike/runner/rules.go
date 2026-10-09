package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RulesVersion is bumped when a rule changes its meaning.
const RulesVersion = 5

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
//     offset against a full build is recorded. The budget ratio of q in the window of
//     R days is b_q(R) = W_q(R)/Budget_k(q, R), where
//     Budget_k(q, R) = Base_k*E_q + PerItem_k*D_q(R)/ItemsPerUnit_k, E_q is the number
//     of entities the read asks about and D_q(R) the size of the reference engine's
//     answer in that window (G1Budget; a budget in blocks is multiplied by the block
//     size of the runs). A population (the queries of one group at one instant) has
//     a statistic in each window: S(R) is the largest b_q(R) over a hot population and
//     the median (the lower one, for an even number) over the median one, with W_q(R)
//     counted as at least 1 so that every S(R) has a logarithm; q* is the query that
//     is S(TargetWindow), the one with more work among queries of equal ratio. The
//     growth s is the least-squares slope of ln S(R) against ln R over every window,
//     and is 0 when W_q*(TargetWindow) is at or below k's floor (G1Growth). The
//     projected ratio is p = b_q*(TargetWindow) * G1Headroom^max(s, 0): what the read
//     would cost if the retention were multiplied by G1Headroom. A population passes
//     if p is at most 1, and a candidate passes G1 if every population of every class
//     passes at every instant on every counter. The slope of S between the last two
//     windows, the slope between the first and third (2 and 14 days) and the classes
//     it gives (bounded at or below BoundedSlope, linear at or above LinearSlope), and
//     each query's own fit, with the query whose fit is at or above LinearSlope
//     flagged, are diagnostics: they do not decide (the slope between the first and
//     third windows feeds SlopeDiffForTarget, as before).
//   - G2: a retention, with the slowness it leaves in the batches after it, must not
//     stall ingest for longer than the budget, and the block bytes right after it must
//     stay within a factor of the settled value.
//   - G3: the 99th-percentile time to commit a batch while building, within a factor
//     of the best candidate.
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
// Log (2026-10-05, after the first round of scenario runs over the windows and before
// the results of the combined scenario of that round were read; the Stage 3 plan's G1
// slope is log(W14/W2)/log 7, so this is not from the plan: it came from the runs, was
// specified in full, and the owner took the specification as written; the second round
// of runs was made under this rules digest, 3084fa03, before this was committed):
// the growth of G1 is the population's (G1Growth, above): the slope of a least-squares
// line through the population's statistic of the budget ratio in every window, where it
// was the slope of each query between the last two windows, which a single query that
// met a checkpoint by chance in one of those two windows decided, in either direction.
// The slope between the last two windows and each query's own fit became diagnostics,
// and a query whose own fit is at or above LinearSlope is flagged. G1Instants gained
// three hours and nine hours before the end (reads that writes follow, where the end of
// the stream has none) and "dead" (whether a pinned node is alive an hour after the
// end, when each of its refreshes has lapsed: a read that must establish that no
// record holds). Windows whose plans do not ask an instant of the rules are judged on
// the instants they ask, and no pass is shown for them. G1Headroom, G1Budget, the
// floors, the windows, TargetWindow and the populations are unchanged. RulesVersion
// is 4.
//
// Log (2026-10-07, before any timing on CI hardware; the owner's decisions; no field of
// Rules changes, so the digest does not): G2, G3 and G4 are judged on builds that commit
// without a sync of the log, as every build so far has; a build that syncs every commit is
// a separate measurement of what a sync costs a layout that writes checkpoints in a second
// commit, which informs the store's commit design and is never compared with a build that
// does not (the report refuses the mix). The timed builds do not rest after a retention,
// are not rewritten into the canonical layout, and are not read: G2, G3 and G4 come from
// the build's manifest, and G0 is the counters run's on the same plan. The best candidate
// of G3 and G4 is taken over the candidates of the timed run that pass G0: L/off and the
// checkpoint policy chosen by G1, K_min 64, alpha 2, lag 1 ns. M/crdb1, which fails G1, is
// built at the 7-day window in the first timed run; if its 99th-percentile commit there is
// more than 10% below L/off's on either architecture, it joins that set at every window.
// Each candidate is built once per job, in its own job, several times on each
// architecture: G3 and G4 compare each candidate's median over its repetitions with the
// best median, G2 takes the largest longest-retention over the repetitions, and a gate
// passes only if it passes on both architectures.
//
// Log (2026-10-08, before any timing on CI hardware of an engine that keeps the writer's
// state across a retention and settles a retention's tombstones; set after a local
// diagnosis that timed such an engine on one machine, which informs and does not decide;
// the owner's decisions). A retention ends only when the database has settled what it
// wrote: in every layout, Retain flushes and waits, timed with it, until the database is
// at rest (at most a deadline, after which the writer resumes and the build records that
// the deadline was reached); a build says so in its description (settle_tombstones), and
// builds with and without it are never compared. G2 judges what a retention costs the
// writer, its own time and the slowness it leaves behind: for retention i, S_i = R_i +
// E_i, where R_i is how long Retain took and E_i is the sum, over the first
// PostRetentionBatches (100) batches written after it (fewer if the stream or the next
// retention comes first), of max(0, t_j - m), with m the build's median batch commit (its
// bucket bound). A build's G2 value is its largest S_i; a candidate's is the largest over
// its repetitions, and passes if it is at most StallBudgetSeconds. A build that rested
// after each retention, or that did not record the batches after one, is shown and not
// judged on G2. If, at a window and on an architecture, no member of G3's reference set
// that passes G0 is within StallBudgetSeconds, synchronous retention fits the budget there
// for no layout: that is recorded as a finding (the store must retain asynchronously
// before it ingests real data), and G2 there is judged against the best instead: a
// candidate passes if it is within StallBudgetSeconds or its G2 value is within
// CommitFactor of the smallest G2 value of those members, compared as G3's timings are
// (within one CPU model, or pooled when every candidate compared has at least three
// repetitions). G3 is the 99th-percentile batch commit alone, a candidate's median over
// its repetitions within CommitFactor of the best median; the longest first batch after a
// retention is no longer part of G3 (it is the first term of E_i) and is printed, with
// the ratio the former rule took, as a diagnostic, as are R_i alone, the settling time
// and the deadlines reached. A G3 comparison in a scope where L/off's own
// 99th-percentile commit varies by more than a factor of 1.5 over its repetitions is not
// decided. PostRetentionBatches is new and RulesVersion is 5, so the digest changes; no
// other value changes.
//
// Log (2026-10-08, before any timing on CI hardware at 30 days; no field of Rules
// changes): the 30-day timed builds run, every candidate alike, with the Go runtime's
// memory limit set (GOMEMLIMIT, recorded in the description as go_memory_limit), because a
// build that keeps the writer's state of every prefix exceeds the runners' memory without
// it; it changes no stored byte, only when the collector runs, and builds with and without
// it are never compared.
//
// Two properties of the statistic are part of the rule. It is an envelope: a population
// is the largest of its queries (or the lower median) in each window, so a query that
// becomes the largest only at the target window counts for about a quarter of its own
// slope; each query's own fit is a diagnostic, and a query at or above LinearSlope is
// flagged and does not decide. And where the budget of a read is at or below the
// counter's floor (the steps of an answer of 25 items or fewer, the seeks of a read
// after every refresh has lapsed), work that fits the budget is also at or below the
// floor, no growth is counted, and G1 for that read is the ratio at the target window
// alone.
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
	// G1Growth says how the growth of a population is measured: see above.
	G1Growth string
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

	// G2: the stall a retention may cause, in seconds, counting the batches after it,
	// and the factor its block bytes may exceed the settled value by.
	StallBudgetSeconds float64
	PostRetentionBytes float64
	// PostRetentionBatches is how many batches after a retention G2 counts the slowness
	// of: see the log of 2026-10-08.
	PostRetentionBatches int
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
	// An instant is an age of the plan's queries (see [BuildQueries]): each
	// population is judged at each instant its plans ask it at.
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
		G1Growth:   "least-squares slope of ln S(R) on ln R over every window, S the population's statistic of the budget ratio with work at least 1; 0 at or below the floor of the work of the query that is S at the target",
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
		StallBudgetSeconds: 60, PostRetentionBytes: 2, PostRetentionBatches: 100,
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
		G1Instants:    []string{AgeNow, Age3h, Age9h, Age1d, AgeDead},
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
