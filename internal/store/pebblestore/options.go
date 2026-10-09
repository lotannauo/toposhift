package pebblestore

import (
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Recorder receives the counters and samples a measurement collects. It is the
// recorder pebblekv takes, so the store's counts and the iterator statistics of
// its reads reach one place. The names are "write.records", "read.records_stepped",
// "read.baseline_entries_decoded", "retain.prefixes_visited",
// "retain.prefixes_replayed", "retain.prefixes_kept_for_boots",
// "retain.records_replayed", "retain.baselines_written", "retain.range_deletes",
// "retain.seeks", and, when Config.SettleRetention is on, "retain.flush_ns",
// "retain.settle_ns" and "retain.settle_deadline_hits" (the flush and the wait,
// and the waits that reached their deadline). Every read also samples Pebble's
// iterator statistics under "read.<op>.<name>" (see [pebblekv.RecordIter]); those
// are in one unit for every layout, which "read.records_stepped" is not.
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
	// retention horizon are synced to disk; the commits that rewrite history
	// below a horizon already published are not, because a later synced commit
	// (or the log's order) keeps them behind it, and a crash that loses them only
	// leaves history a later retention discards. A retention that returns has
	// therefore published its horizon durably, and may not yet have made its
	// rewriting so.
	pebblekv.Config
	// Policy is the lifecycle policy entity existence is folded with. It is fixed
	// when the database is created: a database created with one boot key is not
	// opened with another.
	Policy lifecycle.Policy
	// Recorder receives the store's counts. Nil discards them.
	Recorder Recorder
	// Retention says how Retain works; the zero value is [Synchronous].
	Retention RetentionMode

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
}

// DefaultOptions are the options of a store that boots with no configuration:
// the benchmark tuning, a retention that settles the database before it returns,
// the lifecycle policy that tells the boots of a host apart, and synchronous
// retention. Record commits and the commit of a retention horizon are synced; a
// test or a benchmark that does not need the durability turns Config.Sync off.
func DefaultOptions() Options {
	return Options{
		Config:    pebblekv.Config{Tuning: pebblekv.BenchTuning(), Sync: true, SettleRetention: true},
		Policy:    lifecycle.Policy{BootKey: lifecycle.BootID},
		Retention: Synchronous,
	}
}
