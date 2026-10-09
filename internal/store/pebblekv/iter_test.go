package pebblekv

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// memRecorder keeps what it is given, for a test to read back.
type memRecorder struct {
	counters map[string]int64
	samples  map[string][]int64
}

func newMemRecorder() *memRecorder {
	return &memRecorder{counters: map[string]int64{}, samples: map[string][]int64{}}
}

func (r *memRecorder) Count(name string, n int64)  { r.counters[name] += n }
func (r *memRecorder) Sample(name string, v int64) { r.samples[name] = append(r.samples[name], v) }
func (r *memRecorder) Samples(name string) []int64 { return r.samples[name] }
func (r *memRecorder) Counter(name string) int64   { return r.counters[name] }
func scan(t *testing.T, kv *KV, rec Recorder, op string) int {
	t.Helper()
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	RecordIter(rec, op, it)
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

// A read of what is only in the memtable loads no blocks, a read of what is in
// tables does, and with a cache too small to hold them a second read loads them
// again. Each of the iterator's numbers is reported under its own name.
func TestRecordIterReportsWhatAReadDid(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := Config{Tuning: TinyTuning()}
	cfg.Tuning.CacheBytes = 1 << 10 // one kilobyte: smaller than a single block
	kv := bytewise(t, fs, "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	val := make([]byte, 100)
	for i := range 50 {
		if err := kv.Set(fmt.Appendf(nil, "k%03d", i), val, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}

	mem := newMemRecorder()
	if n := scan(t, kv, mem, "memtable"); n != 50 {
		t.Fatalf("scanned %d keys, want 50", n)
	}
	if got := mem.Samples("read.memtable.block_bytes"); len(got) != 1 || got[0] != 0 {
		t.Errorf("a read of the memtable loaded blocks: %v", got)
	}
	if got := mem.Samples("read.memtable.points"); len(got) != 1 || got[0] < 50 {
		t.Errorf("points iterated over: %v, want at least 50", got)
	}
	if got := mem.Samples("read.memtable.steps"); len(got) != 1 || got[0] != 50 {
		t.Errorf("Next calls: %v, want 50 (one after each key, the last finding the end)", got)
	}
	if got := mem.Samples("read.memtable.seeks"); len(got) != 1 || got[0] != 1 {
		t.Errorf("seeks: %v, want 1", got)
	}
	if mem.Counter("read.memtable.reads") != 1 {
		t.Errorf("reads counted: %d", mem.Counter("read.memtable.reads"))
	}

	if err := kv.Settle(); err != nil {
		t.Fatal(err)
	}
	// Opened again read-only, the cache is the kilobyte: a database that is written
	// to has room besides for its memtables.
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}
	cfg.ReadOnly = true
	kv = bytewise(t, fs, "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	for round := range 2 {
		scan(t, kv, mem, "tables")
		blocks := mem.Samples("read.tables.block_bytes")
		cached := mem.Samples("read.tables.block_bytes_cached")
		if len(blocks) != round+1 || blocks[round] == 0 {
			t.Fatalf("round %d: a read of tables loaded no blocks: %v", round, blocks)
		}
		if cached[round] > blocks[round] {
			t.Errorf("round %d: %d bytes from the cache of %d loaded", round, cached[round], blocks[round])
		}
		if round == 1 && cached[round] == blocks[round] {
			t.Errorf("a one-kilobyte cache served every block of a second read: it cannot hold them")
		}
	}
	if got := mem.Samples("read.tables.internal_steps"); got[0] < 49 {
		t.Errorf("internal steps %v: the iterator must have stepped its inner iterators at least once per key", got)
	}
}

// The iterator's steps as the caller sees them and as it makes them differ by the
// versions it steps over: with five stored versions of every key, a scan of the
// hundred keys is a hundred Next calls and five hundred inner steps and points.
func TestInnerStepsCountTheVersionsASteppedOver(t *testing.T) {
	t.Parallel()
	cfg := Config{Tuning: TinyTuning(), DisableAutoCompactions: true} // five tables stay five
	cfg.Tuning.CacheBytes = 64 << 20
	kv := bytewise(t, vfs.NewMem(), "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	for round := range 5 {
		for i := range 100 {
			if err := kv.Set(fmt.Appendf(nil, "key-%03d", i), fmt.Appendf(nil, "v%d", round), kv.WriteOptions()); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	mem := newMemRecorder()
	if n := scan(t, kv, mem, "versions"); n != 100 {
		t.Fatalf("scanned %d keys, want 100", n)
	}
	for name, want := range map[string]int64{"seeks": 1, "internal_seeks": 1, "steps": 100, "internal_steps": 500, "points": 500} {
		if got := mem.Samples("read.versions." + name); len(got) != 1 || got[0] != want {
			t.Errorf("read.versions.%s = %v, want %d", name, got, want)
		}
	}
}

// Every name RecordIter records under is documented in one place and is
// recorded, so a renamed field cannot silently stop a measurement.
func TestRecordIterNamesArePinned(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	fill(t, kv, 10, 5)
	mem := newMemRecorder()
	scan(t, kv, mem, "op")
	want := []string{
		"block_bytes", "block_bytes_cached", "block_read_ns", "points", "key_bytes", "value_bytes",
		"seeks", "steps", "internal_seeks", "internal_steps", "covered_by_tombstones", "separated_values", "separated_value_bytes_fetched",
	}
	for _, name := range want {
		if got := mem.Samples("read.op." + name); len(got) != 1 {
			t.Errorf("read.op.%s was sampled %d times, want once", name, len(got))
		}
	}
	if mem.Counter("read.op.reads") != 1 {
		t.Error("read.op.reads was not counted")
	}
	if len(mem.samples) != len(want) {
		t.Errorf("%d names were sampled, want the %d listed", len(mem.samples), len(want))
	}
}

// A nil recorder does nothing, and the iterator is still usable afterwards.
func TestRecordIterWithNoRecorder(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	fill(t, kv, 5, 5)
	if got := scan(t, kv, nil, "op"); got != 5 {
		t.Errorf("scanned %d keys, want 5", got)
	}
}
