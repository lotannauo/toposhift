package rockskv

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

func tinyConfig() Config { return Config{Tuning: pebblekv.TinyTuning()} }

func openKV(t *testing.T, dir string, cfg Config) *KV {
	t.Helper()
	k, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k.Close() })
	return k
}

// key is a key of n bytes made of i, for a test's data.
func key(i int) []byte { return fmt.Appendf(nil, "k%06d", i) }

// fill writes n keys with values of size bytes, in batches, and flushes.
func fill(t *testing.T, k *KV, n, size int) {
	t.Helper()
	b := k.NewBatch()
	defer func() { _ = b.Close() }()
	for i := range n {
		if err := b.Set(key(i), bytes.Repeat([]byte{byte(i)}, size), nil); err != nil {
			t.Fatal(err)
		}
		if b.Len() > 16<<10 {
			if err := k.Apply(b, k.WriteOptions()); err != nil {
				t.Fatal(err)
			}
			b.Reset()
		}
	}
	if !b.Empty() {
		if err := k.Apply(b, k.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}
	if err := k.Flush(); err != nil {
		t.Fatal(err)
	}
}

// The ids of the perf metrics are the header's: RocksDB 11.1.2's and 11.8.1's
// c.h agree on them, and a library whose header moved one would fail here.
func TestPerfMetricIDsAreTheHeaders(t *testing.T) {
	t.Parallel()
	header := headerPerfIDs()
	if len(header) != len(perfMetrics) {
		t.Fatalf("%d metrics reported, %d pinned", len(perfMetrics), len(header))
	}
	for _, m := range perfMetrics {
		if got, ok := header[m.name]; !ok || got != m.id {
			t.Errorf("%s has id %d here and %d (present %v) in the header", m.name, m.id, got, ok)
		}
	}
}

// A batch's Len is the size RocksDB holds it at, as Pebble's batch's is: the
// retention commits at the same sizes on both engines.
func TestBatchLenIsTheEncodedSize(t *testing.T) {
	t.Parallel()
	k := openKV(t, t.TempDir(), tinyConfig())
	b := k.NewBatch()
	defer func() { _ = b.Close() }()
	if b.Len() != b.encodedLen() || !b.Empty() {
		t.Fatalf("an empty batch: Len %d, encoded %d, Empty %v", b.Len(), b.encodedLen(), b.Empty())
	}
	ops := []func() error{
		func() error { return b.Set([]byte("a"), []byte("v"), nil) },
		func() error { return b.Set(bytes.Repeat([]byte("k"), 200), bytes.Repeat([]byte("v"), 300), nil) },
		func() error { return b.Delete(bytes.Repeat([]byte("d"), 37), nil) },
		func() error { return b.DeleteRange([]byte("a"), bytes.Repeat([]byte("z"), 130), nil) },
	}
	for i, op := range ops {
		if err := op(); err != nil {
			t.Fatal(err)
		}
		if b.Len() != b.encodedLen() || b.Empty() {
			t.Fatalf("after %d operations: Len %d, encoded %d, Empty %v", i+1, b.Len(), b.encodedLen(), b.Empty())
		}
	}
	b.Reset()
	if b.Len() != b.encodedLen() || !b.Empty() {
		t.Fatalf("after Reset: Len %d, encoded %d", b.Len(), b.encodedLen())
	}
}

// The iterator behaves as Pebble's: First goes to the lower bound, SeekGE does not
// go below it, nothing at or after the upper bound is seen whether RocksDB has the
// bounds or only the iterator does, and the seeks and steps are counted as Pebble
// counts them.
func TestIteratorBoundsAndCounts(t *testing.T) {
	t.Parallel()
	k := openKV(t, t.TempDir(), tinyConfig())
	fill(t, k, 100, 8)
	lo, hi := key(10), key(20)
	walk := func(it *Iterator, start func() bool) [][]byte {
		var out [][]byte
		for ok := start(); ok; ok = it.Next() {
			out = append(out, bytes.Clone(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	var want [][]byte
	for i := 10; i < 20; i++ {
		want = append(want, key(i))
	}

	native, _ := k.NewIter(&IterOptions{LowerBound: lo, UpperBound: hi})
	defer func() { _ = native.Close() }()
	set, _ := k.NewIter(nil)
	defer func() { _ = set.Close() }()
	set.SetBounds(lo, hi)
	for name, it := range map[string]*Iterator{"bounds given to RocksDB": native, "bounds set later": set} {
		if got := walk(it, it.First); !slices.EqualFunc(got, want, bytes.Equal) {
			t.Errorf("%s: First walks %q, want %q", name, got, want)
		}
		if got := walk(it, func() bool { return it.SeekGE(key(3)) }); !slices.EqualFunc(got, want, bytes.Equal) {
			t.Errorf("%s: SeekGE below the lower bound walks %q, want %q", name, got, want)
		}
		if got := walk(it, func() bool { return it.SeekGE(key(15)) }); !slices.EqualFunc(got, want[5:], bytes.Equal) {
			t.Errorf("%s: SeekGE inside walks %q, want %q", name, got, want[5:])
		}
		if it.SeekGE(hi) || it.Valid() || it.Key() != nil || it.Value() != nil {
			t.Errorf("%s: SeekGE at the upper bound is at %q", name, it.Key())
		}
		// Four seeks; each walk steps once past its last key, which Pebble counts.
		if it.seeks != 4 || it.steps != 10+10+5 {
			t.Errorf("%s: %d seeks and %d steps, want 4 and 25", name, it.seeks, it.steps)
		}
	}

	// Re-bounding moves the iterator to another range of the same view.
	set.SetBounds(key(50), key(52))
	if got := walk(set, set.First); !slices.EqualFunc(got, [][]byte{key(50), key(51)}, bytes.Equal) {
		t.Errorf("after SetBounds, First walks %q", got)
	}
	rec := &engine.MemRecorder{}
	RecordIter(rec, "test", set)
	if rec.Counter("read.test.reads") != 1 || !slices.Equal(rec.Samples("read.test.seeks"), []int64{5}) || !slices.Equal(rec.Samples("read.test.steps"), []int64{27}) {
		t.Errorf("recorded reads %d, seeks %v, steps %v", rec.Counter("read.test.reads"), rec.Samples("read.test.seeks"), rec.Samples("read.test.steps"))
	}
	if n := len(rec.Samples("read.test.rocks.block_read_count")); n != 0 {
		t.Errorf("a read without the perf context sampled it %d times", n)
	}
}

// An iterator is one view: a commit after it was made is not seen, by a walk or
// after a SetBounds, as on Pebble.
func TestIteratorIsOneView(t *testing.T) {
	t.Parallel()
	k := openKV(t, t.TempDir(), tinyConfig())
	fill(t, k, 10, 8)
	it, _ := k.NewIter(nil)
	defer func() { _ = it.Close() }()
	if err := k.Set(key(5000), []byte("late"), k.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	it.SetBounds(key(4000), key(6000))
	if it.First() {
		t.Fatalf("the iterator sees %q, written after it was made", it.Key())
	}
}

func TestOpenRefusesAnUnmappedTuning(t *testing.T) {
	t.Parallel()
	for name, tune := range map[string]pebblekv.Tuning{
		"a block size of its own": func() pebblekv.Tuning { x := pebblekv.BenchTuning(); x.BlockSize = 4 << 10; return x }(),
		"a flush delay":           func() pebblekv.Tuning { x := pebblekv.TinyTuning(); x.FlushDelayDeleteRange = time.Second; return x }(),
	} {
		if k, err := Open(t.TempDir(), Config{Tuning: tune}); err == nil {
			_ = k.Close()
			t.Errorf("%s: opened", name)
		}
	}
	for name, tune := range map[string]pebblekv.Tuning{
		"zero": {}, "bench": pebblekv.BenchTuning(), "tiny": pebblekv.TinyTuning(),
		"tiny with its own cache": func() pebblekv.Tuning { x := pebblekv.TinyTuning(); x.CacheBytes = 3 << 20; return x }(),
	} {
		k, err := Open(t.TempDir(), Config{Tuning: tune})
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		_ = k.Close()
	}
}

// The commits are synced exactly when the config says so, and the description says
// the same.
func TestSyncIsPassed(t *testing.T) {
	t.Parallel()
	for _, sync := range []bool{false, true} {
		cfg := tinyConfig()
		cfg.Sync = sync
		k := openKV(t, t.TempDir(), cfg)
		d, err := k.Describe()
		if err != nil {
			t.Fatal(err)
		}
		if k.WriteOptions().wo.IsSync() != sync || d["sync"] != strconv.FormatBool(sync) {
			t.Errorf("Sync %v: the commits sync %v, the description says %q", sync, k.WriteOptions().wo.IsSync(), d["sync"])
		}
	}
}

// The block cache is the tuning's cache plus four memtables when the database is
// written to, and the tuning's cache alone when it is read only, as Pebble's; the
// memtables are charged to it only when written to.
func TestTheCacheIsSizedAsPebbles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{Tuning: pebblekv.BenchTuning()}
	cfg.Tuning.CacheBytes = 5 << 20
	w := openKV(t, dir, cfg)
	reserve := uint64(cfg.Tuning.MemTableReserve())
	check := func(k *KV, want uint64, charged bool) {
		t.Helper()
		d, err := k.Describe()
		if err != nil {
			t.Fatal(err)
		}
		if k.cache.GetCapacity() != want || d["block_cache_capacity"] != fmt.Sprint(want) || d["memtables_charged_to_cache"] != fmt.Sprint(charged) || d["cache_bytes"] != fmt.Sprint(5<<20) {
			t.Errorf("read only %v: capacity %d, described %q, charged %q, cache_bytes %q; want %d, %v",
				k.cfg.ReadOnly, k.cache.GetCapacity(), d["block_cache_capacity"], d["memtables_charged_to_cache"], d["cache_bytes"], want, charged)
		}
	}
	check(w, 5<<20+reserve, true)
	_ = w.Close()
	cfg.ReadOnly = true
	check(openKV(t, dir, cfg), 5<<20, false)
}

// A cold start empties the cache: a read after it loads its blocks again, which the
// perf context counts.
func TestColdStartEmptiesTheCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := openKV(t, dir, tinyConfig())
	fill(t, w, 2000, 64)
	if err := w.CompactAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	cfg := tinyConfig()
	cfg.ReadOnly, cfg.Perf = true, true
	k := openKV(t, dir, cfg)
	read := func() (loaded, hits int64) {
		it, _ := k.NewReadIter(nil)
		it.SetBounds(key(500), key(700))
		for ok := it.First(); ok; ok = it.Next() {
		}
		rec := &engine.MemRecorder{}
		RecordIter(rec, "cold", it)
		_ = it.Close()
		return rec.Samples("read.cold.rocks.block_read_count")[0], rec.Samples("read.cold.rocks.block_cache_hit_count")[0]
	}
	if loaded, _ := read(); loaded == 0 {
		t.Fatal("the first read loaded no block")
	}
	if loaded, hits := read(); loaded != 0 || hits == 0 {
		t.Fatalf("a warm read loaded %d blocks and hit %d", loaded, hits)
	}
	before := k.Snapshot()["cache_misses"]
	k.ColdStart()
	if loaded, _ := read(); loaded == 0 {
		t.Fatal("a read after ColdStart loaded no block: the cache was not emptied")
	}
	if after := k.Snapshot()["cache_misses"]; after <= before {
		t.Fatalf("cache_misses went from %d to %d over a cold read", before, after)
	}
}

// The statistics carry the names the runner reads, and the description every field
// the comparison records; the descriptions that make a variant what it is are the
// same when the database is opened read-only.
//
// Compactions are off, so the deletions are still in a table when the statistics
// are read: a compaction into the last level drops them, and whether one runs
// before the read depends on where the memtables were flushed, which differs
// between builds of RocksDB.
func TestStatsAndDescription(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := tinyConfig()
	cfg.DisableAutoCompactions = true
	w := openKV(t, dir, cfg)
	fill(t, w, 3000, 64)
	b := w.NewBatch()
	_ = b.DeleteRange(key(100), key(200), nil)
	_ = b.Delete(key(300), nil)
	if err := w.Apply(b, w.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := w.Settle(); err != nil {
		t.Fatal(err)
	}
	s := w.Snapshot().Flat()
	for _, name := range []string{
		"live_table_bytes", "tables_l0", "tables_l1", "tables_l2", "tables_l3", "tables_l4", "tables_l5", "tables_l6",
		"cache_misses", "cache_size", "compaction_debt", "memtable_count", "bytes_in", "write_stalls", "tombstones",
		"bytes_flushed", "bytes_compacted", "compactions",
	} {
		if _, ok := s[name]; !ok {
			t.Errorf("the statistics have no %s: %v", name, s)
		}
	}
	if s["compactions"] != 0 {
		t.Fatalf("a compaction ran with compactions off, so the deletions may be gone: %v", s)
	}
	if s["live_table_bytes"] <= 0 || s["bytes_in"] <= 0 || s["memtable_count"] < 1 || s["tombstones"] < 2 {
		t.Errorf("statistics %v", s)
	}
	t.Logf("Stats: %v", s)
	built, err := w.Describe()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"comparer", "key_schema_in_tables", "collectors_in_tables", "time_filter_asked", "block_bytes", "memtable_bytes",
		"target_file_bytes", "l_base_max_bytes", "l0_compaction_threshold", "cache_bytes", "sync", "settle_tombstones", "settle_deadline",
		"engine", "rocksdb_version", "grocksdb_version", "cc_version", "cgo_cflags", "cgo_ldflags", "rocksdb_portable",
		"rocksdb_options", "compression_per_level", "perf_level", "statistics_level", "cpu_model", "recovered_bytes_note",
	} {
		if _, ok := built[name]; !ok {
			t.Errorf("the description has no %s", name)
		}
	}
	if built["comparer"] != ComparerName || built["rocksdb_version"] == "unknown" || built["perf_level"] != "disable" {
		t.Errorf("comparer %q, rocksdb_version %q, perf_level %q", built["comparer"], built["rocksdb_version"], built["perf_level"])
	}
	// What is recorded and not mirrored is what RocksDB runs with: its own options
	// file says so.
	for name, want := range map[string]string{
		"target_file_bytes_by_level_from_base": "16384,16384,32768,65536,131072,262144",
		"memtable_delay_at":                    "3",
		"max_open_files":                       "-1",
		"pin_top_level_index":                  "true",
	} {
		if built[name] != want {
			t.Errorf("%s is %q, want %q", name, built[name], want)
		}
	}
	for option, want := range map[string]string{
		"max_open_files": "-1", "pin_top_level_index_and_filter": "true", "max_write_buffer_number": "4",
		"target_file_size_base": "16384", "target_file_size_multiplier": "2", "level_compaction_dynamic_level_bytes": "true",
	} {
		if got := optionValue(built["rocksdb_options"], option); got != want {
			t.Errorf("the options file has %s=%s, want %s", option, got, want)
		}
	}
	described := maps.Clone(built)
	delete(described, "rocksdb_options")
	t.Logf("Describe (without the options text): %v", described)
	_ = w.Close()
	cfg = tinyConfig()
	cfg.ReadOnly = true
	read, err := openKV(t, dir, cfg).Describe()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"comparer", "key_schema_in_tables", "collectors_in_tables", "time_filter_asked", "block_bytes", "memtable_bytes", "target_file_bytes", "l_base_max_bytes", "l0_compaction_threshold", "cache_bytes", "sync"} {
		if read[name] != built[name] {
			t.Errorf("%s is %q read-only, %q as built", name, read[name], built[name])
		}
	}
}

// holdingFilter is a compaction filter that, once armed, holds every compaction
// that reaches it until it is released: a compaction that runs for as long as a
// test needs, whatever the machine's speed. Unarmed, it keeps every key.
type holdingFilter struct {
	armed   atomic.Bool
	enter   sync.Once
	entered chan struct{}
	release chan struct{}
}

func (f *holdingFilter) Filter(int, []byte, []byte) (bool, []byte) {
	if f.armed.Load() {
		f.enter.Do(func() { close(f.entered) })
		<-f.release
	}
	return false, nil
}

func (f *holdingFilter) Name() string            { return "rockskv.test.holding" }
func (f *holdingFilter) SetIgnoreSnapshots(bool) {}
func (f *holdingFilter) Destroy()                {}

// A settle whose deadline passes while a compaction runs returns without error and
// says the deadline was hit; one with time enough does not.
//
// The compaction is held open by a compaction filter until the first settle has
// returned, so the test does not depend on how fast the machine compacts. A filter
// can only be given when the database is opened, which Open does not offer, so the
// database is opened here and given to a KV that has what the settle uses: the
// database, its flush options and a configuration that is not read-only.
func TestSettleDeadline(t *testing.T) {
	t.Parallel()
	f := &holdingFilter{entered: make(chan struct{}), release: make(chan struct{})}
	o := grocksdb.NewDefaultOptions()
	o.SetCreateIfMissing(true)
	o.SetDisableAutoCompactions(true)
	o.SetCompactionFilter(f)
	db, err := grocksdb.OpenDb(o, filepath.Join(t.TempDir(), "db"))
	if err != nil {
		o.Destroy()
		t.Fatal(err)
	}
	k := &KV{db: db, opts: o, flushOpts: grocksdb.NewDefaultFlushOptions(), cfg: tinyConfig()}
	t.Cleanup(func() {
		db.Close()
		k.flushOpts.Destroy()
		o.Destroy()
	})
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	// Two tables over the same keys, so the compaction merges them and calls the
	// filter for every key.
	for range 2 {
		for i := range 100 {
			if err := db.Put(wo, key(i), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		if err := k.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	f.armed.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		co := grocksdb.NewCompactRangeOptions()
		defer co.Destroy()
		co.SetBottommostLevelCompaction(grocksdb.KForce)
		db.CompactRangeOpt(grocksdb.Range{}, co)
	}()
	// Whatever happens below, the compaction is let go and has ended before the
	// database is closed.
	release := sync.OnceFunc(func() {
		close(f.release)
		<-done
	})
	defer release()
	select {
	case <-f.entered:
	case <-done:
		t.Fatal("the compaction ended without reaching its filter")
	case <-time.After(time.Minute):
		t.Fatal("the compaction did not reach its filter within a minute")
	}

	// A settle that ignored its deadline would wait for the held compaction for
	// ever; after a minute it is let go and the test fails.
	type settled struct {
		hit bool
		err error
	}
	first := make(chan settled, 1)
	go func() {
		_, _, hit, err := k.SettleAfterRetention(time.Microsecond)
		first <- settled{hit, err}
	}()
	select {
	case r := <-first:
		if r.err != nil || !r.hit {
			t.Fatalf("a settle of a microsecond while a compaction runs: hit %v, %v", r.hit, r.err)
		}
	case <-time.After(time.Minute):
		release()
		<-first
		t.Fatal("a settle of a microsecond did not return within a minute while a compaction ran")
	}
	release()
	if _, _, hit, err := k.SettleAfterRetention(time.Minute); err != nil || hit {
		t.Fatalf("a settle with a minute: hit %v, %v", hit, err)
	}
}

// The perf context of a read counts that read and nothing else, even with many reads
// running at once: each read holds its thread from its iterator's making to its
// close. A read whose goroutine moved to another thread would count another read's
// work, or lose its own.
func TestPerfContextIsTheReadsOwn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := openKV(t, dir, tinyConfig())
	fill(t, w, 2000, 64)
	if err := w.CompactAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	cfg := tinyConfig()
	cfg.ReadOnly, cfg.Perf = true, true
	cfg.Tuning.CacheBytes = 64 << 20 // every block stays once loaded
	k := openKV(t, dir, cfg)
	read := func() (touched int64) {
		it, _ := k.NewReadIter(nil)
		defer func() { _ = it.Close() }()
		it.SetBounds(key(100), key(1900))
		for ok := it.First(); ok; ok = it.Next() {
		}
		rec := &engine.MemRecorder{}
		RecordIter(rec, "own", it)
		return rec.Samples("read.own.rocks.block_read_count")[0] + rec.Samples("read.own.rocks.block_cache_hit_count")[0]
	}
	read()
	want := read()
	if want == 0 {
		t.Fatal("a read touched no block")
	}
	const readers, reads = 32, 40
	got := make(chan int64, readers*reads)
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			for range reads {
				got <- read()
			}
		})
	}
	wg.Wait()
	close(got)
	wrong := 0
	for n := range got {
		if n != want {
			wrong++
		}
	}
	if wrong > 0 {
		t.Fatalf("%d of %d reads counted other than the %d blocks one read touches", wrong, readers*reads, want)
	}
}

// The target file sizes RocksDB writes from the base level down are the base
// level's twice, then doubling: for the bench tuning 128, 128, 256 MiB and so on,
// where Pebble writes 128, 256, 512 MiB.
func TestTargetFileBytesFromBase(t *testing.T) {
	t.Parallel()
	got := targetFileBytesFromBase(pebblekv.BenchTuning())
	want := []int64{128 << 20, 128 << 20, 256 << 20, 512 << 20, 1 << 30, 2 << 30}
	if !slices.Equal(got, want) {
		t.Errorf("targetFileBytesFromBase(bench) = %v, want %v", got, want)
	}
}
