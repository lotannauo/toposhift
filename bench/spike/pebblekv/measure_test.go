package pebblekv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// bytewise opens a database in memory under the bytewise layout, whose keys are
// any bytes, so these tests need not build valid versioned keys.
func bytewise(t *testing.T, fs vfs.FS, dir string, cfg pebblekv.Config) *pebblekv.KV {
	t.Helper()
	cfg.FS = fs
	if cfg.Tuning == (pebblekv.Tuning{}) {
		cfg.Tuning = pebblekv.TinyTuning()
	}
	cfg.Schema = pebblekv.SchemaDefault
	kv, err := pebblekv.Open(dir, pebblekv.BytewiseLayout, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return kv
}

// fill writes n keys in batches that each end in a flush, so the database has
// many small tables.
func fill(t *testing.T, kv *pebblekv.KV, n, perFlush int) {
	t.Helper()
	val := make([]byte, 200)
	for i := range n {
		for j := range val {
			val[j] = byte(i * 7)
		}
		if err := kv.Set([]byte(fmt.Sprintf("key-%06d", i)), val, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		if (i+1)%perFlush == 0 {
			if err := kv.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func tables(s pebblekv.Snapshot) (n int64) {
	for _, c := range s.TablesPerLevel {
		n += c
	}
	return n
}

func scan(t *testing.T, kv *pebblekv.KV, rec engine.Recorder, op string) int {
	t.Helper()
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	pebblekv.RecordIter(rec, op, it)
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
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning()}
	cfg.Tuning.CacheBytes = 1 << 10 // one kilobyte: smaller than a single block
	kv := bytewise(t, fs, "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	val := make([]byte, 100)
	for i := range 50 {
		if err := kv.Set([]byte(fmt.Sprintf("k%03d", i)), val, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}

	mem := &engine.MemRecorder{}
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

// With a cache that holds everything, a second read of the same tables is served
// from it, which is what makes the cached share a measure of warmth.
func TestACachedSecondReadLoadsFromTheCache(t *testing.T) {
	t.Parallel()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), DisableAutoCompactions: true} // three tables stay three
	cfg.Tuning.CacheBytes = 64 << 20
	kv := bytewise(t, vfs.NewMem(), "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	fill(t, kv, 300, 100)
	if err := kv.Settle(); err != nil {
		t.Fatal(err)
	}
	mem := &engine.MemRecorder{}
	scan(t, kv, mem, "warm")
	scan(t, kv, mem, "warm")
	blocks, cached := mem.Samples("read.warm.block_bytes"), mem.Samples("read.warm.block_bytes_cached")
	if blocks[1] == 0 || cached[1] != blocks[1] {
		t.Errorf("second read: %d bytes loaded, %d from the cache; want all of them cached", blocks[1], cached[1])
	}
}

// The iterator's steps as the caller sees them and as it makes them differ by the
// versions it steps over: with five stored versions of every key, a scan of the
// hundred keys is a hundred Next calls and five hundred inner steps and points.
// That difference is the cost of dead versions, the thing the two layouts are
// compared on, which is why it is recorded in its own numbers. (Seeks are one for
// the caller and one inside, for a scan or a seek alike.)
func TestInnerStepsCountTheVersionsASteppedOver(t *testing.T) {
	t.Parallel()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), DisableAutoCompactions: true} // five tables stay five
	cfg.Tuning.CacheBytes = 64 << 20
	kv := bytewise(t, vfs.NewMem(), "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	for round := range 5 {
		for i := range 100 {
			if err := kv.Set([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v%d", round)), kv.WriteOptions()); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	mem := &engine.MemRecorder{}
	if n := scan(t, kv, mem, "versions"); n != 100 {
		t.Fatalf("scanned %d keys, want 100", n)
	}
	for name, want := range map[string]int64{"seeks": 1, "internal_seeks": 1, "steps": 100, "internal_steps": 500, "points": 500} {
		if got := mem.Samples("read.versions." + name); len(got) != 1 || got[0] != want {
			t.Errorf("read.versions.%s = %v, want %d", name, got, want)
		}
	}

	// And backwards: Last is a seek and each Prev a step, counted the same way.
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	for ok := it.Last(); ok; ok = it.Prev() {
	}
	pebblekv.RecordIter(mem, "back", it)
	_ = it.Close()
	for name, want := range map[string]int64{"seeks": 1, "steps": 100} {
		if got := mem.Samples("read.back." + name); len(got) != 1 || got[0] != want {
			t.Errorf("read.back.%s = %v, want %d", name, got, want)
		}
	}
}

// Snapshot says what the database holds and has done, in numbers that move when
// the database does.
func TestSnapshotMovesWithTheDatabase(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{DisableAutoCompactions: true})
	t.Cleanup(func() { _ = kv.Close() })
	before := kv.Snapshot()
	fill(t, kv, 400, 100)
	after := kv.Snapshot()

	if after.Flushes-before.Flushes != 4 {
		t.Errorf("flushes went from %d to %d, want 4 more", before.Flushes, after.Flushes)
	}
	if tables(after) != 4 || after.LiveTableBytes == 0 {
		t.Errorf("tables %d, live bytes %d: want 4 tables with bytes", tables(after), after.LiveTableBytes)
	}
	if after.BytesFlushed == 0 || after.BytesIn == 0 {
		t.Errorf("flushed %d bytes of %d in", after.BytesFlushed, after.BytesIn)
	}
	// The live table bytes are the tables' sizes by level, and are less than Size,
	// which counts the log as well: the number to compare layouts on is the first.
	var perLevel int64
	for _, b := range after.BytesPerLevel {
		perLevel += b
	}
	if size, err := kv.Size(); err != nil || int64(after.LiveTableBytes) != perLevel || size <= perLevel {
		t.Errorf("live table bytes %d, tables by level %d, Size %d, %v: want the first two equal and Size larger", after.LiveTableBytes, perLevel, size, err)
	}
	flat := after.Flat()
	if flat["flushes"] != after.Flushes || flat["live_table_bytes"] != int64(after.LiveTableBytes) || flat["tables_l0"] != after.TablesPerLevel[0] {
		t.Errorf("Flat disagrees with the snapshot: %v", flat)
	}
}

// DisableAutoCompactions holds: with it a database that flushed many times keeps
// all its tables, and CompactAll then merges them. Without it Pebble merges them
// on its own, so the first half of this would pass for nothing.
func TestAutomaticCompactionsCanBeTurnedOffAndCompactAllMergesTheTables(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	off := bytewise(t, vfs.NewMem(), "off", pebblekv.Config{DisableAutoCompactions: true})
	t.Cleanup(func() { _ = off.Close() })
	on := bytewise(t, vfs.NewMem(), "on", pebblekv.Config{})
	t.Cleanup(func() { _ = on.Close() })
	fill(t, off, 1200, 100)
	fill(t, on, 1200, 100)
	if err := on.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := off.Snapshot().TablesPerLevel[0]; got != 12 {
		t.Fatalf("with automatic compactions off there are %d tables in L0, want the 12 flushes", got)
	}
	if got := off.Snapshot().Compactions; got != 0 {
		t.Errorf("%d compactions ran with them off", got)
	}
	if on.Snapshot().Compactions == 0 || on.Snapshot().TablesPerLevel[0] >= 12 {
		t.Fatalf("with them on: %d compactions, %d tables in L0: the comparison shows nothing",
			on.Snapshot().Compactions, on.Snapshot().TablesPerLevel[0])
	}

	if err := off.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	s := off.Snapshot()
	if s.Compactions == 0 || s.TablesPerLevel[0] != 0 {
		t.Errorf("after CompactAll: %d compactions, %d tables in L0", s.Compactions, s.TablesPerLevel[0])
	}
	if !s.StatsComplete {
		t.Error("CompactAll returned with the table statistics still being collected")
	}
	// A second CompactAll finds nothing to do and still returns.
	again := s.Compactions
	if err := off.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if off.Snapshot().Compactions < again {
		t.Error("the compaction count went backwards")
	}
}

// Quiesce gives up when its context does, and says why, instead of waiting for a
// rest that is not coming.
func TestQuiesceStopsWhenItsContextDoes(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", pebblekv.Config{})
	t.Cleanup(func() { _ = kv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond) // shorter than the second it must be quiet
	defer cancel()
	start := time.Now()
	err := kv.Quiesce(ctx)
	if err == nil {
		t.Fatal("Quiesce returned nil in less than the second of quiet it needs")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("Quiesce took %s to notice its context", time.Since(start))
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
		kv := bytewise(t, fs, "db", pebblekv.Config{Tuning: pebblekv.BenchTuning()}) // a memtable that does not fill
		fill(t, kv, 50, 1000)                                                        // never flushed
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
		again := bytewise(t, fs, "db", pebblekv.Config{Tuning: pebblekv.BenchTuning()})
		if got := scan(t, again, engine.NopRecorder{}, "x"); got != 50 {
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

// Every name RecordIter records under is documented in one place and is
// recorded, so a renamed field cannot silently stop a measurement. This list is
// what the runner and the README refer to.
func TestRecordIterNamesArePinned(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, newMemFS(), "db", pebblekv.Config{})
	t.Cleanup(func() { _ = kv.Close() })
	fill(t, kv, 10, 5)
	mem := &engine.MemRecorder{}
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
	_ = pebble.InterfaceCall // the statistics the names above come from
}

func newMemFS() vfs.FS { return vfs.NewMem() }
