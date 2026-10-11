package pebblekv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// restReading is what [KV.waitAtRest] compares between two polls: the counts of
// flushes and compactions and the live table bytes change whenever the database
// does work.
type restReading struct {
	flushes, compactions int64
	liveTableBytes       uint64
}

// waitAtRest polls until the database is at rest, or until ctx is done: nothing
// flushing or compacting, the statistics of every table loaded, no file marked
// for a compaction that is coming (unless automatic compactions are off, when
// none is), and all of that unchanged for a whole second.
//
// The statistics are waited for table by table, not only by Pebble's own count
// of tables queued for them: Pebble loads them in the background after a flush
// or a compaction, a job at a time, and empties its queue when a job starts, so
// the queue can be empty for as long as a slow job runs. Until a table's
// statistics are loaded, its deletions are not in the tombstone count, and the
// compactions that drop them (an elision-only compaction of a table moved into
// the last level with its tombstones, say) are not picked; Pebble picks them
// when the statistics arrive.
func (k *KV) waitAtRest(ctx context.Context) error {
	const (
		poll   = 20 * time.Millisecond
		stable = time.Second
	)
	var last restReading
	var since time.Time
	for {
		m := k.Metrics()
		cur := restReading{flushes: m.Flush.Count, compactions: m.Compact.Count, liveTableBytes: m.Table.Local.LiveSize}
		statsComplete := m.Table.InitialStatsCollectionComplete && m.Table.PendingStatsCollectionCount == 0
		busy := m.Compact.NumInProgress != 0 || m.Flush.NumInProgress != 0 || !statsComplete ||
			(!k.cfg.DisableAutoCompactions && m.Compact.MarkedFiles != 0)
		if busy || cur != last {
			since = time.Time{}
		} else if since.IsZero() {
			since = time.Now()
		} else if time.Since(since) >= stable {
			// Checked last, and only then, because it lists every table.
			n, err := k.tablesAwaitingStats()
			if err != nil {
				return err
			}
			if n == 0 {
				return nil
			}
			since = time.Time{}
		}
		last = cur
		select {
		case <-ctx.Done():
			return fmt.Errorf("pebblekv: not at rest: %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// SettleAfterRetention is the end of a retention under [Config.SettleRetention]: it
// flushes and waits until the database is at rest, at most deadline
// ([DefaultSettleDeadline] when zero). The deadline bounds the wait, not the flush,
// which runs first and has no limit. It returns how long the flush and the wait
// took, and whether the deadline passed first, which is not an error: the writer
// resumes and what is not settled is paid by the reads and writes after it.
func (k *KV) SettleAfterRetention(deadline time.Duration) (flush, settle time.Duration, deadlineHit bool, err error) {
	if k.cfg.ReadOnly {
		return 0, 0, false, nil
	}
	if deadline == 0 {
		deadline = DefaultSettleDeadline
	}
	start := time.Now()
	if err := k.Flush(); err != nil {
		return time.Since(start), 0, false, err
	}
	flush = time.Since(start)
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start = time.Now()
	err = k.waitAtRest(ctx)
	settle = time.Since(start)
	if errors.Is(err, context.DeadlineExceeded) {
		return flush, settle, true, nil
	}
	return flush, settle, false, err
}

// CompactRanges is the settle of a retention that must not hold the writer: it
// flushes once, and then compacts each of the ranges given, in order, one after
// the other. A range is lo up to hi; Pebble's Compact also takes hi itself, which is
// never a key of the layout here (boundary keys are 1, 3 or 4 bytes, real keys 37).
// A range whose lo is not before its hi is an error. This package knows nothing of the layout of the
// keys; the caller says which ranges its range deletions fell in.
//
// It stops between two ranges, and not before the first, when ctx is done (it
// returns ctx's error, which is not a failure of the database) or when deadline
// ([DefaultSettleDeadline] when zero) has passed since the flush ended (it returns
// deadlineHit and no error: what is not compacted is paid by the reads and writes
// after it, as for [KV.SettleAfterRetention]). The deadline also ends a wait that is
// in progress. It returns how long the flush and the compactions took.
//
// Pebble's Compact, which runs each range, does not stop a compaction that has
// started when its context ends: it cancels the compactions of the call that have
// not been picked up yet, returns the context's error at once, and leaves the
// running ones to finish in the background (Pebble v2.1.7, DB.Compact and
// manualCompact). So a range may still be compacting when this returns early, and
// the database must not be closed meanwhile; closing waits for it. It is the
// caller's to stop calling Compact before closing: Pebble panics on a Compact of a
// closed database.
//
// It does not wait for the database to be at rest: not for the flushes and
// compactions other writes bring on, nor for the statistics of the new tables, which
// is what [KV.SettleAfterRetention] waits for. A read-only database has nothing to
// flush or compact, and returns at once.
func (k *KV) CompactRanges(ctx context.Context, ranges [][2][]byte, deadline time.Duration) (flush, settle time.Duration, deadlineHit bool, err error) {
	if k.cfg.ReadOnly {
		return 0, 0, false, nil
	}
	if deadline == 0 {
		deadline = DefaultSettleDeadline
	}
	start := time.Now()
	if err := k.Flush(); err != nil {
		return time.Since(start), 0, false, err
	}
	flush = time.Since(start)
	timed, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	start = time.Now()
	for i, r := range ranges {
		if err := ctx.Err(); err != nil {
			return flush, time.Since(start), false, err
		}
		if timed.Err() != nil {
			return flush, time.Since(start), true, nil
		}
		if err := k.Compact(timed, r[0], r[1], true); err != nil {
			settle = time.Since(start)
			if cerr := ctx.Err(); cerr != nil {
				return flush, settle, false, cerr
			}
			if errors.Is(err, context.DeadlineExceeded) && timed.Err() != nil {
				return flush, settle, true, nil
			}
			return flush, settle, false, fmt.Errorf("pebblekv: compacting %x to %x: %w", r[0], r[1], err)
		}
		if f, ok := ctx.Value(compactRangesHookKey{}).(func(done int)); ok {
			f(i + 1)
		}
	}
	return flush, time.Since(start), false, nil
}

type compactRangesHookKey struct{}

// WithCompactRangesHook returns a context that makes [KV.CompactRanges] call f with
// the number of ranges compacted so far after each one, from the goroutine that
// compacts, so that a test can count them or hold the settle between two. It is for
// tests.
func WithCompactRangesHook(ctx context.Context, f func(done int)) context.Context {
	return context.WithValue(ctx, compactRangesHookKey{}, f)
}

// tablesAwaitingStats is how many tables Pebble has not loaded the statistics of.
func (k *KV) tablesAwaitingStats() (int, error) {
	levels, err := k.SSTables()
	if err != nil {
		return 0, err
	}
	return awaitingStats(levels), nil
}

// awaitingStats counts the tables whose statistics are not loaded: Pebble reports
// a table's statistics as zero until it has, and once it has they count its
// entries, points and range deletions alike, which every table a layout writes
// has. It needs no table opened, so it does not wait for the job that is reading
// one.
func awaitingStats(levels [][]pebble.SSTableInfo) int {
	n := 0
	for _, level := range levels {
		for _, t := range level {
			if t.TableStats.NumEntries == 0 {
				n++
			}
		}
	}
	return n
}

// CloseClean flushes the memtable and closes the database, so that opening it
// again recovers nothing from the log. Closing without it leaves the memtable's
// contents in the log, and opening replays them and writes them to a table of its
// own at that moment (see [KV.RecoveredBytes]): the data ends up in tables either
// way, but a built database that a measurement opens should not do any work, or
// have tables of a different shape from the ones its own flushes made, at open.
func (k *KV) CloseClean() error {
	if err := k.Flush(); err != nil {
		_ = k.Close()
		return err
	}
	return k.Close()
}

// RecoveredBytes is how many bytes of tables opening this database wrote from
// its log. It is zero for a database that was closed with [KV.CloseClean] (or
// flushed and then closed), and a measurement that opens a built database
// refuses one that is not zero.
func (k *KV) RecoveredBytes() uint64 { return k.recovered }
