package pebblekv

import (
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// optionsFile is the text of the OPTIONS file the database under dir on fs wrote:
// what Pebble itself reports it was opened with.
func optionsFile(t *testing.T, fs vfs.FS, dir string) string {
	t.Helper()
	names, err := fs.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if !strings.HasPrefix(n, "OPTIONS-") {
			continue
		}
		f, err := fs.Open(fs.PathJoin(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	t.Fatalf("no OPTIONS file under %s: %v", dir, names)
	return ""
}

// Tuning.FlushDelayDeleteRange reaches Pebble: the delay the options file reports is
// the one the tuning gives, and zero (off) in both BenchTuning and TinyTuning.
func TestTheFlushDelayOfRangeDeletionsReachesPebble(t *testing.T) {
	t.Parallel()
	if got := BenchTuning().FlushDelayDeleteRange; got != 0 {
		t.Errorf("BenchTuning delays the flush of a range deletion by %s, want none", got)
	}
	if got := TinyTuning().FlushDelayDeleteRange; got != 0 {
		t.Errorf("TinyTuning delays the flush of a range deletion by %s, want none", got)
	}
	for _, tc := range []struct {
		delay time.Duration
		want  string
	}{{0, "flush_delay_delete_range=0s"}, {time.Second, "flush_delay_delete_range=1s"}, {250 * time.Millisecond, "flush_delay_delete_range=250ms"}} {
		fs := vfs.NewMem()
		tune := TinyTuning()
		tune.FlushDelayDeleteRange = tc.delay
		kv := bytewise(t, fs, "db", Config{Tuning: tune})
		if got := kv.Options().FlushDelayDeleteRange; got != tc.delay {
			t.Errorf("the options hold a delay of %s, want %s", got, tc.delay)
		}
		if text := optionsFile(t, fs, "db"); !strings.Contains(text, tc.want) {
			t.Errorf("the options file lacks %q:\n%s", tc.want, text)
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// With a delay, a range deletion in the memtable is flushed without anything else
// asking for it; with none, it stays there.
func TestARangeDeletionIsFlushedAfterTheDelay(t *testing.T) {
	t.Parallel()
	for _, delay := range []time.Duration{0, 20 * time.Millisecond} {
		tune := TinyTuning()
		tune.MemTableSize = 16 << 20 // nothing fills it
		tune.FlushDelayDeleteRange = delay
		kv := bytewise(t, vfs.NewMem(), "db", Config{Tuning: tune})
		if err := kv.Set([]byte("a"), []byte("1"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		before := kv.Metrics().Flush.Count
		if err := kv.DeleteRange([]byte("a"), []byte("b"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		// A flush is waited for ten seconds where one is due, and half a second (25
		// times the other delay) where none is.
		wait := 500 * time.Millisecond
		if delay > 0 {
			wait = 10 * time.Second
		}
		flushed := false
		for deadline := time.Now().Add(wait); !flushed && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			flushed = kv.Metrics().Flush.Count > before
		}
		if flushed != (delay > 0) {
			t.Errorf("with a delay of %s, the range deletion was flushed on its own: %v", delay, flushed)
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// errorLogger records the errors Pebble logs.
type errorLogger struct {
	quietLogger
	errors atomic.Int32
}

func (l *errorLogger) Errorf(string, ...any) { l.errors.Add(1) }

// A Config.EventListener is told of the database's events beside the KV's own
// listener, which counts the write stalls, and a background error is still logged.
func TestTheEventListenerIsToldAndTheStallsAreCounted(t *testing.T) {
	t.Parallel()
	var flushes, stalls atomic.Int32
	l := &pebble.EventListener{
		FlushEnd:        func(pebble.FlushInfo) { flushes.Add(1) },
		WriteStallBegin: func(pebble.WriteStallBeginInfo) { stalls.Add(1) },
	}
	log := &errorLogger{}
	kv, err := Open("db", BytewiseLayout, Config{Schema: SchemaDefault, Tuning: TinyTuning(), FS: vfs.NewMem(), Logger: log, EventListener: l})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	if err := kv.Set([]byte("a"), []byte("1"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := kv.Flush(); err != nil {
		t.Fatal(err)
	}
	if flushes.Load() == 0 {
		t.Error("the listener was not told of the flush")
	}
	if kv.WriteStalls() != 0 {
		t.Errorf("%d write stalls before any", kv.WriteStalls())
	}
	// A stall cannot be made to happen at this scale without holding Pebble's own
	// mutex, so the listener the database runs with is called as Pebble calls it.
	running := kv.opts.EventListener
	running.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "test"})
	if kv.WriteStalls() != 1 || stalls.Load() != 1 {
		t.Errorf("after one stall the KV counts %d and the listener was told of %d", kv.WriteStalls(), stalls.Load())
	}
	running.BackgroundError(errors.New("test"))
	if log.errors.Load() != 1 {
		t.Errorf("a background error was logged %d times, want once", log.errors.Load())
	}
	if l.BackgroundError != nil {
		t.Error("the caller's listener was changed")
	}
}

// Without a Config.EventListener the stalls are counted all the same, and a
// background error is logged.
func TestTheStallsAreCountedWithoutAListener(t *testing.T) {
	t.Parallel()
	log := &errorLogger{}
	kv, err := Open("db", BytewiseLayout, Config{Schema: SchemaDefault, Tuning: TinyTuning(), FS: vfs.NewMem(), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	kv.opts.EventListener.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "test"})
	kv.opts.EventListener.BackgroundError(errors.New("test"))
	if kv.WriteStalls() != 1 || log.errors.Load() != 1 {
		t.Errorf("%d stalls counted and %d errors logged, want one of each", kv.WriteStalls(), log.errors.Load())
	}
}

// L0Sublevels reads level 0's sublevels and the threshold at which Pebble stops
// writes.
func TestL0SublevelsReadsTheDatabase(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{DisableAutoCompactions: true})
	t.Cleanup(func() { _ = kv.Close() })
	if n, stop := kv.L0Sublevels(); n != 0 || stop != 12 {
		t.Errorf("an empty database has %d sublevels of %d", n, stop)
	}
	fill(t, kv, 30, 10)
	if n, _ := kv.L0Sublevels(); n < 1 {
		t.Errorf("%d sublevels after three flushes with compactions off", n)
	}
}
