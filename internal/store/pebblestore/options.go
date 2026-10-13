package pebblestore

import (
	"time"

	"github.com/cockroachdb/pebble/v2"

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
//     those a complete map knows to be empty, nor the reads below),
//     "checkpoint.loads_cold" (prefixes read before the first scan of the stored
//     prefixes has installed a filter), "checkpoint.lookups",
//     "checkpoint.load_keys" (the keys all of those read) and
//     "checkpoint.build_records_walked".
//   - the writer's map (see the package comment): "writer_map.false_positive_reads"
//     (prefixes a complete map read because its filter or a delta named them, and
//     found to hold nothing), "writer_map.lru_evictions", "writer_map.scan_ns" (the
//     scans of the stored prefixes that installed a filter, start to end) and
//     "writer_map.scan_errors"; and, counted up and down so that their sum is what
//     the store holds now, and back to zero when it is closed,
//     "writer_map.filter_bytes", "writer_map.delta_prefixes" and
//     "writer_map.lru_bytes" (the estimated bytes of the states kept). The counts of
//     a scan are given with the next call to the store that writes, retains, places
//     a checkpoint or waits for a scan, not from the scan's goroutine.
//   - reads: "read.records_stepped", "read.baseline_entries_decoded",
//     "read.checkpoint_hits", "read.checkpoint_skipped_w",
//     "read.checkpoint_skipped_version" and "read.checkpoint_entries_decoded". Every
//     read also samples Pebble's iterator statistics under "read.<op>.<name>" (see
//     [pebblekv.RecordIter]); those are in one unit for every layout, which
//     "read.records_stepped" is not.
//   - lookups (see [Lookup]): "lookup.iterators" (one per [Store.NewLookup]),
//     "lookup.keys_walked" (a sample, the keys one [Lookup.Last] walked) and
//     "lookup.exhausted" (the calls that reached the bound).
//   - retention: "retain.prefixes_visited", "retain.prefixes_replayed",
//     "retain.records_replayed",
//     "retain.baselines_written", "retain.range_deletes", "retain.seeks",
//     "retain.state_keys" (the keys a retention reads to work out the writer's
//     state, when it does; see [Store.Retain]) and, as samples for each retention
//     that rewrites, "retain.max_prefix_records" (the most records replayed for one
//     prefix) and "retain.chunks" (how many chunks it committed); and, as samples
//     for each chunk, given once its commit has landed in this order,
//     "retain.chunk_open_ns" (the opening and first positioning of its iterator,
//     which grows with the range deletions in the memtable),
//     "retain.chunk_prefixes" (the prefixes it rewrote) and "retain.chunk_hold_ns"
//     (the nanoseconds from the chunk's start, before its iterator is opened, to
//     the end of its commit, which is how long a writer waits for it, the lock being
//     released between chunks in a Background store); and, as a sample at the start
//     of each chunk, "retain.unflushed_range_deletes" (the range deletions retention
//     has committed since the start of the last flush known to have completed: an
//     upper bound of those the memtables hold); and "retain.flushes", the flushes a
//     pass started to bound those deletions, one each time they reach a count (one
//     per 32,768 prefixes rewritten or so: more, smaller tables in level 0 during a
//     pass, which its settle or later compactions merge); "retain.flush_deferred",
//     the flushes it put off because level 0 was near the sublevel count at which
//     Pebble stops writes, and "retain.flush_errors", those AsyncFlush refused
//     (which this version of Pebble does only for a database opened read-only):
//     both are tried again after the pass's next chunk; and, in a Background store,
//     "retain.touch_rewrites" (the prefixes a write rewrote in its own batch because
//     the pass had not reached them); and, when Open finishes a retention that
//     an earlier process left, "retain.resumed", counted once before that retention
//     reports as any other does; "retain.superseded", counted when Open removes a
//     marker that the stored horizons have all moved past; and, when
//     Config.SettleRetention is on, "retain.flush_ns", "retain.settle_ns" and
//     "retain.settle_deadline_hits" (the flush and the wait, and the waits that
//     reached their deadline).
type Recorder = pebblekv.Recorder

// RetentionMode says how [Store.Retain] does its work.
type RetentionMode uint8

const (
	// Synchronous makes Retain do all of its work, settling included, before it
	// returns. The zero value of [Options.Retention] means it, and it is the mode
	// of [DefaultOptions], of the benchmarks and of the conformance suite.
	Synchronous RetentionMode = 1
	// Background makes Retain publish the horizons and return, and leaves the
	// rewriting to a goroutine of the store that works in chunks and lets writers
	// through between them; a write to a prefix the pass has not reached rewrites
	// that prefix first, in its own batch. See the package comment. With
	// Config.SettleRetention the pass ends by settling: a flush, then a compaction of
	// the key ranges of each layer it moved, one range after another, with no lock
	// held and no iterator open, so that writers go on while it runs (see
	// [pebblekv.KV.CompactRanges]). Without it the pass leaves its tombstones to the
	// compactions the database schedules. [Instrument.WaitRetained] waits for the
	// pass and its settle to end. It is opt-in: the default is [Synchronous].
	Background RetentionMode = 2
)

// RetainObserver is told when a retention of a [Background] store reaches the points
// that bound its phases, for a measurement that judges each phase. The instants are
// the store's clock at the point, read under its lock where the point is reached.
// The calls are made without the store's lock, by the goroutine that reached the
// point, so a call may arrive after a later one of another goroutine, and the
// instant, not the order of arrival, is what to trust. A call must return quickly
// and must not call back into the store: the retainer waits for it.
//
// A retention is named by its generation, the number its marker carries. For each
// generation that ends:
//
//   - Published is called by the Retain that published it, once the horizons are
//     stored and visible;
//   - RewriteEnded is called when the last chunk of the rewriting is committed, or,
//     for a generation a later Retain replaces before that, by the later Retain with
//     the instant of the later publication;
//   - SettleEnded is called when the settle that follows has ended, whether or not
//     SettleRetention is on (without it the settle is empty), and before
//     [Instrument.WaitRetained] returns for the generation. A generation replaced
//     before its settle ends gets none (a generation replaced by a Retain that comes
//     between the end of its settle and this call has had SettleEnded nonetheless).
//
// A pass that Open resumes gets no Published, and none of the other two for a phase
// that was over before the process stopped. A Synchronous store calls none of them,
// because it does all of its work under the lock, and a store that is closed during
// a settle does not call SettleEnded for it. The clock is not read when there is no
// observer.
type RetainObserver interface {
	Published(gen uint64, at time.Time)
	RewriteEnded(gen uint64, at time.Time)
	SettleEnded(gen uint64, at time.Time)
}

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
	// Retention says how Retain works; the zero value is [Synchronous]. A store
	// opened [Background] and read-only starts no goroutine and runs no pass.
	Retention RetentionMode
	// Observer, if not nil, is told of the phases of each retention of a Background
	// store; see [RetainObserver].
	Observer RetainObserver
	// Checkpoints says when interleaved checkpoints are written. Nil means
	// [DefaultCheckpoints]; a policy that writes none is an explicit
	// &CheckpointOptions{}, and the layout then answers every read by replaying.
	// Negative or non-finite numbers are refused by Open. The policy is not stored:
	// a database may be opened with another, or with none, and the checkpoints it
	// holds are kept true all the same.
	Checkpoints *CheckpointOptions
	// Offsets is the retention offset of each layer, indexed by layer minus L0
	// (Offsets[0] is L0's). Retain(h) sets the horizon of a retained layer to h
	// minus its offset, so a layer with a longer offset keeps more history. The zero
	// value is no offset: every layer's horizon is h. An offset must not be
	// negative; Open refuses one. Offsets are not stored: a database may be opened
	// with others, and every horizon it holds stays as it is (a horizon never moves
	// backward).
	Offsets [4]time.Duration
	// Keep says, per layer and indexed like Offsets, that the layer is kept: Retain
	// never moves its horizon, whatever its offset. A layer that an earlier
	// opening retained keeps the horizon it was given.
	Keep [4]bool
	// StateCacheBytes bounds the memory of the states the writer keeps of the
	// prefixes it has touched most recently, by an estimate of each state's size;
	// the states a Write touches are kept until it returns, whatever the budget. A
	// state not kept is read from the database when it is needed, unless the
	// writer's filter of the stored prefixes says the prefix holds nothing (see the
	// package comment). Zero means [DefaultStateCacheBytes]; Open refuses a negative
	// value.
	StateCacheBytes int64

	// retainBatchBytes overrides [defaultRetainBatchBytes] and retainChunkTime
	// [defaultRetainChunkTime], the two limits of a chunk of a retention.
	// retainStopAfter makes a retention fail after that many chunk commits, and
	// resumeStopAfter does the same to the retention Open finishes, which
	// retainStopAfter leaves alone; with either set, a retainChunkTime of zero means
	// no limit of time, so that the commit a retention stops at does not depend on
	// the clock. afterRetainCommit runs after each commit of either. All for tests.
	retainBatchBytes  int
	retainChunkTime   time.Duration
	retainStopAfter   int
	resumeStopAfter   int
	afterRetainCommit func()
	// beforeRetainChunk runs on the goroutine of a Background store before each chunk
	// of a pending pass, and so before the first, with the store's lock free, and
	// retainerExited, if not nil, is closed by that goroutine when it exits: both
	// for tests. (afterRetainCommit runs after each chunk, the same way.)
	beforeRetainChunk func()
	retainerExited    chan struct{}
	// retainFlushDels overrides [defaultRetainFlushDels], the count of unflushed
	// range deletions at which a pass flushes; a negative value turns the flushes off.
	// retainFlushL0Margin overrides [defaultRetainFlushL0Margin], how far below its
	// stop threshold level 0 must be for a flush to start (a negative value never
	// puts one off), and l0Sublevels replaces the reading of level 0's sublevels and
	// stop threshold.
	// chunkNow replaces the clock a chunk's time and its samples are read from.
	// afterChunkOpen runs in a chunk once its iterator is open and positioned, before
	// the clock is read for the opening's cost. flushHook, if not nil, starts each
	// flush a pass starts, given the database's AsyncFlush. beforeFlushWait runs
	// before a pass waits for a flush, beforeApply before each commit of a chunk or of
	// records is applied, with its batch, and beforeKVClose in Close just before the
	// database is closed. All for tests.
	retainFlushDels     int
	retainFlushL0Margin int
	l0Sublevels         func() (n, stop int)
	chunkNow            func() time.Time
	afterChunkOpen      func()
	flushHook           func(flush func() (<-chan struct{}, error)) (<-chan struct{}, error)
	beforeFlushWait     func()
	beforeApply         func(*pebble.Batch)
	beforeKVClose       func()
	// betweenSettleRanges runs after each range of a Background settle has been
	// compacted, with the number done so far, with the store's lock free (see
	// [pebblekv.WithCompactRangesHook]): for tests.
	betweenSettleRanges func(done int)
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
	// beforeTagApply returns an error in place of the commit that rewrites the tag
	// of a database of stamp format 1 (a commit that failed without landing): for
	// tests.
	beforeTagApply func() error
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
	// scanHook, if not nil, is called by each scan of the stored prefixes at each
	// of its points (see scanPoint), on the scan's goroutine and with the store's
	// lock free, with the prefixes visited so far and the last of them: for tests.
	scanHook func(at scanPoint, hops int, last []byte)
}

// DefaultStateCacheBytes is the memory the writer's states of the prefixes it has
// touched most recently may take when [Options.StateCacheBytes] is zero.
const DefaultStateCacheBytes = 128 << 20

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
