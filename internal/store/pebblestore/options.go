package pebblestore

import (
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Recorder receives the counters and samples a measurement collects. It is the
// recorder pebblekv takes, so the store's counts and the iterator statistics of
// its reads reach one place. The names, in counts unless noted, are:
//
//   - "write.records" (stored copies), and, for each Write that stores records, six
//     samples of where its time went in nanoseconds, which do not overlap and so add
//     up to at most the Write's duration: "write.phase_ns.validate" (checking the
//     records, before the lock), ".state" (learning what the writer needs of each
//     prefix), ".invalidate" (finding the checkpoints a record makes untrue),
//     ".record_commit", ".ckpt_build" (choosing and building checkpoints) and
//     ".ckpt_commit"; and "write.iterators", how many iterators those opened (left
//     out when it is none). Nothing is timed when the store has no recorder.
//   - checkpoints: "checkpoint.written", "checkpoint.bytes_written" (key and value
//     bytes), "checkpoint.build_ns" (the time spent building them, in nanoseconds),
//     "checkpoint.invalidated", "checkpoint.errors", "checkpoint.loads" (prefixes
//     whose state was read from the database: not those a retention worked out, nor
//     the new ones a complete map knows to be empty), "checkpoint.lookups",
//     "checkpoint.load_keys" (the keys both read) and
//     "checkpoint.build_records_walked".
//   - reads: "read.records_stepped", "read.baseline_entries_decoded",
//     "read.checkpoint_hits", "read.checkpoint_skipped_w",
//     "read.checkpoint_skipped_version" and "read.checkpoint_entries_decoded". Every
//     read also samples Pebble's iterator statistics under "read.<op>.<name>" (see
//     [pebblekv.RecordIter]); those are in one unit for every layout, which
//     "read.records_stepped" is not.
//   - retention: "retain.prefixes_visited", "retain.prefixes_replayed",
//     "retain.prefixes_kept_for_boots", "retain.records_replayed",
//     "retain.baselines_written", "retain.range_deletes", "retain.seeks",
//     "retain.state_keys" (the keys a retention reads to work out the writer's
//     state, when it does; see [Store.Retain]) and, as a sample for each retention
//     that rewrites, "retain.max_prefix_records" (the most records replayed for one
//     prefix); and, when Config.SettleRetention is on, "retain.flush_ns",
//     "retain.settle_ns" and "retain.settle_deadline_hits" (the flush and the wait,
//     and the waits that reached their deadline).
type Recorder = pebblekv.Recorder

// RetentionMode says how [Store.Retain] does its work.
type RetentionMode uint8

const (
	// Synchronous makes Retain do all of its work, settling included, before it
	// returns. It is the only mode there is, and the zero value of
	// [Options.Retention] means it.
	Synchronous RetentionMode = 1
)

// Options says how a store is opened. The key schema is Pebble's default and
// there is no time filter: the layout has no use for one, since every read is one
// contiguous range of one prefix, and Open refuses a Config that asks for either.
type Options struct {
	// Config is the Pebble database's: its tuning, whether commits are synced,
	// the file system, and whether a retention settles the database before it
	// returns.
	//
	// With Config.Sync on, the commits of records and the commit that publishes a
	// retention horizon are synced to disk; the commits of checkpoints, which are
	// derived, and the commits that rewrite history below a horizon already
	// published are not, because a later synced commit (or the log's order) keeps
	// them behind it, and a crash that loses them only leaves history a later
	// retention discards. A retention that returns has therefore published its
	// horizon durably, and may not yet have made its rewriting so.
	pebblekv.Config
	// Policy is the lifecycle policy entity existence is folded with. It is fixed
	// when the database is created: a database created with one boot key is not
	// opened with another.
	Policy lifecycle.Policy
	// Recorder receives the store's counts. Nil discards them.
	Recorder Recorder
	// Retention says how Retain works; the zero value is [Synchronous].
	Retention RetentionMode
	// Checkpoints says when interleaved checkpoints are written. Nil means
	// [DefaultCheckpoints]; a policy that writes none is an explicit
	// &CheckpointOptions{}, and the layout then answers every read by replaying.
	// Negative or non-finite numbers are refused by Open. The policy is not stored:
	// a database may be opened with another, or with none, and the checkpoints it
	// holds are kept true all the same.
	Checkpoints *CheckpointOptions

	// retainBatchBytes overrides [defaultRetainBatchBytes], retainStopAfter makes
	// a retention fail after that many commits of its rewriting, and
	// afterRetainCommit runs after each of them: all for tests.
	retainBatchBytes  int
	retainStopAfter   int
	afterRetainCommit func()
	// beforeRecordApply returns an error in place of a record commit (a commit
	// that failed without landing), afterRecordApply runs after one has landed
	// and an error it returns stands for a commit that failed but is visible, and
	// rereadFails makes the read of the last sequence number after a failed commit
	// fail: all for tests.
	beforeRecordApply func() error
	afterRecordApply  func() error
	rereadFails       func() error
	// checkValue replaces the check of the value of each record, for tests.
	checkValue                            func(pebblekv.Value) error
	beforeHorizonApply, afterHorizonApply func() error
	// beforeCheckpointApply returns an error in place of a checkpoint commit (a
	// commit that failed without landing), and afterCheckpointApply runs after one
	// has landed; an error it returns stands for a commit that failed but is
	// already visible: both for tests.
	beforeCheckpointApply, afterCheckpointApply func() error
	// fullStateRead makes the writer read the whole of a prefix to learn its state,
	// as it once did, and never trust the map to be complete: for tests that compare
	// the two.
	fullStateRead bool
	// clock replaces the clock the phases of a Write are timed with, for tests.
	clock func() time.Time
}

// DefaultOptions are the options of a store that boots with no configuration:
// the benchmark tuning, a retention that settles the database before it returns,
// the lifecycle policy that tells the boots of a host apart, synchronous
// retention, and the default checkpoint policy. Record commits and the commit of a
// retention horizon are synced; the commits of checkpoints and of the rewriting
// below a published horizon are not. A test or a benchmark that does not need the
// durability turns Config.Sync off.
func DefaultOptions() Options {
	ckpt := DefaultCheckpoints()
	return Options{
		Config:      pebblekv.Config{Tuning: pebblekv.BenchTuning(), Sync: true, SettleRetention: true},
		Policy:      lifecycle.Policy{BootKey: lifecycle.BootID},
		Retention:   Synchronous,
		Checkpoints: &ckpt,
	}
}
