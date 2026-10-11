package pebblekv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// rangeDeleted fills a database with many prefixes, one flush per few of them so the
// tables are many, and then range-deletes most of the prefixes in one batch, as a
// retention does. The deletions are committed, not flushed.
func rangeDeleted(t *testing.T, kv *KV) {
	t.Helper()
	const prefixes, perPrefix, deleted = 60, 50, 50
	val := make([]byte, 200)
	for p := range prefixes {
		b := kv.NewBatch()
		for i := range perPrefix {
			if err := b.Set(fmt.Appendf(nil, "p%03d/%05d", p, i), val, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		if (p+1)%5 == 0 {
			if err := kv.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := kv.Flush(); err != nil {
		t.Fatal(err)
	}
	b := kv.NewBatch()
	for p := range deleted {
		if err := b.DeleteRange(fmt.Appendf(nil, "p%03d/", p), fmt.Appendf(nil, "p%03d0", p), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
}

// After a retention's range deletions, settling flushes them and lets the
// compactions they call for run: the deletions leave level 0, where they were
// flushed and where every reader after them would cross them, and the database is
// at rest. Pebble does not necessarily drop them altogether (a deletion in a level
// above the data it covers stays until a compaction of both is picked), so the
// test asserts where they are and what is still running, not that none is left.
// Without the compactions (the same database with automatic compactions off) the
// deletions stay in level 0, so the test has something to settle.
func TestSettleAfterRetentionMovesRangeTombstonesOutOfLevelZero(t *testing.T) {
	t.Parallel()

	t.Run("without automatic compactions the deletions stay in level 0", func(t *testing.T) {
		t.Parallel()
		kv := bytewise(t, vfs.NewMem(), "db", Config{DisableAutoCompactions: true})
		t.Cleanup(func() { _ = kv.Close() })
		rangeDeleted(t, kv)
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
		if n := kv.Metrics().Levels[0].TablesCount; n == 0 {
			t.Error("the range deletions are not in level 0: the test has nothing to settle")
		}
	})

	t.Run("settling moves them out and leaves the database at rest", func(t *testing.T) {
		t.Parallel()
		kv := bytewise(t, vfs.NewMem(), "db", Config{})
		t.Cleanup(func() { _ = kv.Close() })
		rangeDeleted(t, kv)
		flush, settle, hit, err := kv.SettleAfterRetention(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if hit {
			t.Error("the deadline of a minute was reached")
		}
		if flush <= 0 || settle <= 0 {
			t.Errorf("the flush took %s and the wait %s: both took time", flush, settle)
		}
		m := kv.Metrics()
		if n := m.Levels[0].TablesCount; n != 0 {
			t.Errorf("%d tables are still in level 0 after settling", n)
		}
		if !m.Table.InitialStatsCollectionComplete || m.Table.PendingStatsCollectionCount != 0 ||
			m.Compact.NumInProgress != 0 || m.Flush.NumInProgress != 0 || m.Compact.MarkedFiles != 0 {
			t.Errorf("the database is not at rest after settling: stats complete %v, %d pending, %d compactions and %d flushes running, %d files marked",
				m.Table.InitialStatsCollectionComplete, m.Table.PendingStatsCollectionCount,
				m.Compact.NumInProgress, m.Flush.NumInProgress, m.Compact.MarkedFiles)
		}
		props, err := kv.TableProperties()
		if err != nil {
			t.Fatal(err)
		}
		if len(props) == 0 {
			t.Error("no table is left")
		}
	})
}

// A deadline that passes is not an error: the wait returns, says it gave up, and
// does not run on.
func TestSettleAfterRetentionDeadline(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	rangeDeleted(t, kv)
	_, settle, hit, err := kv.SettleAfterRetention(time.Nanosecond)
	if err != nil {
		t.Fatalf("a deadline that passed is an error: %v", err)
	}
	if !hit {
		t.Error("a deadline of one nanosecond was not reported as reached")
	}
	if settle >= time.Second {
		t.Errorf("the wait took %s with a deadline of one nanosecond", settle)
	}
}

// A read-only database has nothing to flush or settle.
func TestSettleAfterRetentionInAReadOnlyDatabase(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	kv := bytewise(t, fs, "db", Config{})
	fill(t, kv, 20, 5)
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}
	ro := bytewise(t, fs, "db", Config{ReadOnly: true})
	t.Cleanup(func() { _ = ro.Close() })
	flush, settle, hit, err := ro.SettleAfterRetention(time.Minute)
	if flush != 0 || settle != 0 || hit || err != nil {
		t.Errorf("SettleAfterRetention on a read-only database = %s, %s, %v, %v; want nothing", flush, settle, hit, err)
	}
}

// CloseClean leaves nothing in the log: opened again, the database recovers no
// bytes. Close alone leaves what was in the memtable in the log, and opening
// writes it to a table, which RecoveredBytes reports. The second half is what
// makes the first mean something.
func TestCloseCleanLeavesNothingToRecover(t *testing.T) {
	t.Parallel()
	for _, clean := range []bool{false, true} {
		fs := vfs.NewMem()
		kv := bytewise(t, fs, "db", Config{Tuning: BenchTuning()}) // a memtable that does not fill
		fill(t, kv, 50, 1000)                                      // never flushed
		if kv.RecoveredBytes() != 0 {
			t.Fatalf("a new database recovered %d bytes", kv.RecoveredBytes())
		}
		var err error
		if clean {
			err = kv.CloseClean()
		} else {
			err = kv.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		again := bytewise(t, fs, "db", Config{Tuning: BenchTuning()})
		if got := count(t, again); got != 50 {
			t.Errorf("clean=%v: %d keys after reopening, want 50", clean, got)
		}
		recovered := again.RecoveredBytes()
		_ = again.Close()
		if clean && recovered != 0 {
			t.Errorf("a clean close left %d bytes to recover on opening", recovered)
		}
		if !clean && recovered == 0 {
			t.Error("an unclean close left nothing to recover: the contrast this test relies on is gone")
		}
	}
}

// A table awaits its statistics until they count its entries, and only then.
func TestATableAwaitsItsStatisticsUntilTheyCountItsEntries(t *testing.T) {
	t.Parallel()
	table := func(entries uint64) pebble.SSTableInfo {
		var info pebble.SSTableInfo
		info.TableStats.NumEntries = entries
		return info
	}
	for _, c := range []struct {
		name   string
		levels [][]pebble.SSTableInfo
		want   int
	}{
		{"no table", nil, 0},
		{"loaded", [][]pebble.SSTableInfo{{table(5)}}, 0},
		{"not loaded", [][]pebble.SSTableInfo{{table(0)}}, 1},
		{"one of two levels", [][]pebble.SSTableInfo{{table(5)}, nil, nil, nil, nil, nil, {table(1), table(0)}}, 1},
		{"every level", [][]pebble.SSTableInfo{{table(0)}, {table(0), table(0)}}, 3},
	} {
		if got := awaitingStats(c.levels); got != c.want {
			t.Errorf("%s: %d tables await their statistics, want %d", c.name, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// CompactRanges.

// regionData writes 20 prefixes of 50 keys under region ("a/p00/00000" and so on),
// and flushes them.
func regionData(t *testing.T, kv *KV, region string) {
	t.Helper()
	val := make([]byte, 200)
	b := kv.NewBatch()
	for p := range 20 {
		for i := range 50 {
			if err := b.Set(fmt.Appendf(nil, "%s/p%02d/%05d", region, p, i), val, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.Commit(kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	if err := kv.Flush(); err != nil {
		t.Fatal(err)
	}
}

// regionTombstones range-deletes 15 of the 20 prefixes of region in one commit, as
// a retention does, and flushes them if asked.
func regionTombstones(t *testing.T, kv *KV, region string, flush bool) {
	t.Helper()
	b := kv.NewBatch()
	for p := range 15 {
		if err := b.DeleteRange(fmt.Appendf(nil, "%s/p%02d/", region, p), fmt.Appendf(nil, "%s/p%02d0", region, p), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	if flush {
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

// tableInfo is a table's file number, its user-key bounds and its range deletions.
type tableInfo struct {
	num          uint64
	lo, hi       string
	rangeDeletes uint64
}

// tablesIn are the tables that overlap [lo, hi).
func tablesIn(t *testing.T, kv *KV, lo, hi string) []tableInfo {
	t.Helper()
	levels, err := kv.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	var out []tableInfo
	for _, level := range levels {
		for _, tb := range level {
			l, h := tb.Smallest.UserKey, tb.Largest.UserKey
			if bytes.Compare(l, []byte(hi)) < 0 && bytes.Compare(h, []byte(lo)) >= 0 {
				out = append(out, tableInfo{num: uint64(tb.FileNum), lo: string(l), hi: string(h), rangeDeletes: tb.Properties.NumRangeDeletions})
			}
		}
	}
	slices.SortFunc(out, func(a, b tableInfo) int { return int(a.num) - int(b.num) })
	return out
}

func rangeOf(lo, hi string) [2][]byte { return [2][]byte{[]byte(lo), []byte(hi)} }

// CompactRanges flushes, and then compacts the ranges given and nothing else: the
// range tombstones that overlap them are gone from the tables afterwards, and the
// tables of a range left alone are the very files they were. The first range given
// holds no key, so a flush that waited for the first compaction to need it would not
// have happened when it ends.
func TestCompactRangesFlushesAndCompactsOnlyTheRanges(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{DisableAutoCompactions: true})
	t.Cleanup(func() { _ = kv.Close() })
	regionData(t, kv, "a")
	regionData(t, kv, "b")
	regionTombstones(t, kv, "b", true)
	regionTombstones(t, kv, "a", false) // still in the memtable
	if n := len(tablesIn(t, kv, "a", "b")); n == 0 {
		t.Fatal("the data of the first region is in no table")
	}
	untouched := tablesIn(t, kv, "b", "c")
	if !slices.ContainsFunc(untouched, func(x tableInfo) bool { return x.rangeDeletes > 0 }) {
		t.Fatalf("no table of the second region holds its tombstones: %+v", untouched)
	}
	flushes := kv.Metrics().Flush.Count
	var flushedAtFirst int64 = -1
	ctx := WithCompactRangesHook(context.Background(), func(done int) {
		if done == 1 {
			flushedAtFirst = kv.Metrics().Flush.Count
		}
	})
	flush, settle, hit, err := kv.CompactRanges(ctx, [][2][]byte{rangeOf("0", "1"), rangeOf("a/", "a/p10"), rangeOf("a/p10", "b")}, time.Minute)
	if err != nil || hit {
		t.Fatalf("CompactRanges = %v, %v", hit, err)
	}
	if flush <= 0 || settle <= 0 {
		t.Errorf("the flush took %s and the compactions %s: both took time", flush, settle)
	}
	if flushedAtFirst != flushes+1 {
		t.Errorf("%d flushes had run when the first range, which holds no key, was done (%d before): the flush is the first thing it does", flushedAtFirst, flushes)
	}
	for _, tb := range tablesIn(t, kv, "a", "b") {
		if tb.rangeDeletes > 0 {
			t.Errorf("table %d (%q to %q) still holds %d range tombstones in a range that was compacted", tb.num, tb.lo, tb.hi, tb.rangeDeletes)
		}
	}
	if got := tablesIn(t, kv, "b", "c"); !slices.Equal(got, untouched) {
		t.Errorf("the tables of the range left alone changed:\n before %+v\n after  %+v", untouched, got)
	}
	if n := count(t, kv); n != 5*50+5*50 {
		t.Errorf("%d keys after compacting, want %d", n, 5*50+5*50)
	}
}

// With no range to compact it still flushes once, and has nothing to wait for.
func TestCompactRangesWithNoRangesOnlyFlushes(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{DisableAutoCompactions: true})
	t.Cleanup(func() { _ = kv.Close() })
	regionData(t, kv, "a")
	regionTombstones(t, kv, "a", false)
	flushes := kv.Metrics().Flush.Count
	flush, settle, hit, err := kv.CompactRanges(context.Background(), nil, time.Minute)
	if err != nil || hit {
		t.Fatalf("CompactRanges = %v, %v", hit, err)
	}
	if got := kv.Metrics().Flush.Count; got != flushes+1 {
		t.Errorf("%d flushes ran, want one", got-flushes)
	}
	if flush <= 0 || settle < 0 || settle > time.Second {
		t.Errorf("the flush took %s and the compactions %s", flush, settle)
	}
}

// A range whose start is not before its end is Pebble's error, not a silent skip.
func TestCompactRangesRefusesAnEmptyRange(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	regionData(t, kv, "a")
	_, _, hit, err := kv.CompactRanges(context.Background(), [][2][]byte{rangeOf("b", "a")}, time.Minute)
	if err == nil || hit {
		t.Errorf("CompactRanges of an empty range = %v, %v", hit, err)
	}
}

// A cancelled context stops the compactions between two ranges, with the context's
// error, and a deadline that passes stops them and says so with no error.
func TestCompactRangesStopsBetweenSubRanges(t *testing.T) {
	t.Parallel()
	ranges := func() [][2][]byte {
		var out [][2][]byte
		for p := range 100 {
			out = append(out, rangeOf(fmt.Sprintf("a/p%02d", p), fmt.Sprintf("a/p%02d", p+1)))
		}
		return out
	}()
	setup := func(t *testing.T) *KV {
		kv := bytewise(t, vfs.NewMem(), "db", Config{DisableAutoCompactions: true})
		t.Cleanup(func() { _ = kv.Close() })
		regionData(t, kv, "a")
		regionTombstones(t, kv, "a", false)
		return kv
	}

	t.Run("a context that ends", func(t *testing.T) {
		t.Parallel()
		kv := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := 0
		hooked := WithCompactRangesHook(ctx, func(n int) {
			done = n
			cancel() // after the first range
		})
		_, _, hit, err := kv.CompactRanges(hooked, ranges, time.Minute)
		if !errors.Is(err, context.Canceled) || hit {
			t.Errorf("CompactRanges = %v, %v; want the context's error", hit, err)
		}
		if done != 1 {
			t.Errorf("%d ranges were compacted after the context ended at the first, want 1", done)
		}
	})

	t.Run("a context that has ended before the first", func(t *testing.T) {
		t.Parallel()
		kv := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := 0
		hooked := WithCompactRangesHook(ctx, func(n int) { done = n })
		_, _, _, err := kv.CompactRanges(hooked, ranges, time.Minute)
		if !errors.Is(err, context.Canceled) || done != 0 {
			t.Errorf("CompactRanges = %v after %d ranges; want the context's error and none", err, done)
		}
	})

	t.Run("a deadline that passes", func(t *testing.T) {
		t.Parallel()
		kv := setup(t)
		// The deadline counts from the end of the flush, and the hook lets it pass after
		// the first range, whatever the speed of the machine: the first range has a
		// second to be done in, and the hook does not return before the deadline is
		// behind it.
		const deadline = time.Second
		done := 0
		hooked := WithCompactRangesHook(context.Background(), func(n int) {
			done = n
			if n == 1 {
				time.Sleep(deadline + 100*time.Millisecond)
			}
		})
		_, settle, hit, err := kv.CompactRanges(hooked, ranges, deadline)
		if err != nil {
			t.Fatalf("a deadline that passed is an error: %v", err)
		}
		if !hit {
			t.Errorf("a deadline of %s was not reported as reached", deadline)
		}
		if done < 1 || done >= len(ranges) {
			t.Errorf("%d of %d ranges were compacted, want the first and not all: the deadline passed while the hook held it", done, len(ranges))
		}
		if settle < deadline {
			t.Errorf("the compactions took %s with a deadline of %s that was reported as reached", settle, deadline)
		}
	})

	t.Run("a deadline of one nanosecond", func(t *testing.T) {
		t.Parallel()
		kv := setup(t)
		done := 0
		hooked := WithCompactRangesHook(context.Background(), func(n int) { done = n })
		_, _, hit, err := kv.CompactRanges(hooked, ranges, time.Nanosecond)
		if err != nil || !hit {
			t.Errorf("CompactRanges = %v, %v; want the deadline reached and no error", hit, err)
		}
		if done == len(ranges) {
			t.Error("every range was compacted before a deadline of one nanosecond")
		}
	})
}

// A read-only database has nothing to flush or compact.
func TestCompactRangesInAReadOnlyDatabase(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	kv := bytewise(t, fs, "db", Config{})
	fill(t, kv, 20, 5)
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}
	ro := bytewise(t, fs, "db", Config{ReadOnly: true})
	t.Cleanup(func() { _ = ro.Close() })
	flush, settle, hit, err := ro.CompactRanges(context.Background(), [][2][]byte{rangeOf("a", "z")}, time.Minute)
	if flush != 0 || settle != 0 || hit || err != nil {
		t.Errorf("CompactRanges on a read-only database = %s, %s, %v, %v; want nothing", flush, settle, hit, err)
	}
}
