package pebblekv_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// rangeDeleted fills a database with many prefixes, one flush per few of them so the
// tables are many, and then range-deletes most of the prefixes in one batch, as a
// retention does. The deletions are committed, not flushed.
func rangeDeleted(t *testing.T, kv *pebblekv.KV) {
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
// Without the compactions (the same database with automatic compactions off, at
// rest) the deletions stay in level 0, so the test has something to settle.
func TestSettleAfterRetentionMovesRangeTombstonesOutOfLevelZero(t *testing.T) {
	t.Parallel()

	t.Run("without automatic compactions the deletions stay in level 0", func(t *testing.T) {
		t.Parallel()
		kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{DisableAutoCompactions: true})
		t.Cleanup(func() { _ = kv.Close() })
		rangeDeleted(t, kv)
		if err := kv.Quiesce(context.Background()); err != nil {
			t.Fatal(err)
		}
		s := kv.Snapshot()
		if s.TombstoneCount == 0 {
			t.Error("the test has no range deletions to settle")
		}
		if s.TablesPerLevel[0] == 0 {
			t.Error("the range deletions are not in level 0")
		}
	})

	t.Run("settling moves them out and leaves the database at rest", func(t *testing.T) {
		t.Parallel()
		kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{})
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
		s := kv.Snapshot()
		if s.TablesPerLevel[0] != 0 {
			t.Errorf("%d tables are still in level 0 after settling", s.TablesPerLevel[0])
		}
		if !s.StatsComplete || s.PendingStatsTables != 0 || s.CompactionsInProgress != 0 || s.FlushesInProgress != 0 || s.MarkedFiles != 0 {
			t.Errorf("the database is not at rest after settling: %+v", s)
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
	kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{})
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
	kv := bytewise(t, fs, "db", pebblekv.Config{})
	fill(t, kv, 20, 5)
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}
	ro := bytewise(t, fs, "db", pebblekv.Config{ReadOnly: true})
	t.Cleanup(func() { _ = ro.Close() })
	flush, settle, hit, err := ro.SettleAfterRetention(time.Minute)
	if flush != 0 || settle != 0 || hit || err != nil {
		t.Errorf("SettleAfterRetention on a read-only database = %s, %s, %v, %v; want nothing", flush, settle, hit, err)
	}
}

// Quiesce with a context that is already done says it is not at rest and why.
func TestQuiesceWithACancelledContext(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{})
	t.Cleanup(func() { _ = kv.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := kv.Quiesce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Quiesce(cancelled) = %v, want an error wrapping context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "pebblekv: not at rest") {
		t.Errorf("the error %q does not say the database is not at rest", err)
	}
}

// Describe says whether retentions settle and how long they may wait, with the
// default deadline when none is set.
func TestDescribeSaysHowRetentionsSettle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		cfg                    pebblekv.Config
		wantSettle, wantWithin string
	}{
		{"default", pebblekv.Config{}, "false", "2m0s"},
		{"on", pebblekv.Config{SettleRetention: true}, "true", "2m0s"},
		{"on with a deadline", pebblekv.Config{SettleRetention: true, SettleDeadline: 90 * time.Second}, "true", "1m30s"},
		{"off with a deadline", pebblekv.Config{SettleDeadline: 5 * time.Second}, "false", "5s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kv := bytewise(t, vfs.NewMem(), "db", tc.cfg)
			t.Cleanup(func() { _ = kv.Close() })
			d, err := kv.Describe()
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := d["settle_tombstones"]; !ok || got != tc.wantSettle {
				t.Errorf("settle_tombstones = %q (present %v), want %q", got, ok, tc.wantSettle)
			}
			if got, ok := d["settle_deadline"]; !ok || got != tc.wantWithin {
				t.Errorf("settle_deadline = %q (present %v), want %q", got, ok, tc.wantWithin)
			}
		})
	}
}
