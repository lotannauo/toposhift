package pebblekv

import (
	"fmt"
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
