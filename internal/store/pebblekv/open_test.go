package pebblekv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// quietLogger drops Pebble's messages, which a test run does not want, and panics
// on a fatal condition, where the package's own logger ends the process.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// bytewise opens a database under the bytewise layout, whose keys are any bytes,
// so these tests need not build valid keys. A nil file system is the real one.
func bytewise(t *testing.T, fs vfs.FS, dir string, cfg Config) *KV {
	t.Helper()
	cfg.FS = fs
	if cfg.Tuning == (Tuning{}) {
		cfg.Tuning = TinyTuning()
	}
	cfg.Schema = SchemaDefault
	cfg.Logger = quietLogger{}
	kv, err := Open(dir, BytewiseLayout, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return kv
}

// fill writes n keys in batches that each end in a flush, so the database has
// many small tables.
func fill(t *testing.T, kv *KV, n, perFlush int) {
	t.Helper()
	val := make([]byte, 200)
	for i := range n {
		for j := range val {
			val[j] = byte(i * 7)
		}
		if err := kv.Set(fmt.Appendf(nil, "key-%06d", i), val, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		if (i+1)%perFlush == 0 {
			if err := kv.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// count reads every key of the database and returns how many there are.
func count(t *testing.T, kv *KV) int {
	t.Helper()
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

func equalAll(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestSchemaNames(t *testing.T) {
	t.Parallel()
	for s, want := range map[Schema]string{SchemaCRDB: "crdb1", SchemaDefault: "default", 9: "Schema(9)"} {
		if got := s.String(); got != want {
			t.Errorf("Schema(%d) = %q, want %q", uint8(s), got, want)
		}
	}
}

// The layout opens, orders keys by their bytes whatever the order they were
// written in, and comes back with everything after being closed, through the log
// and through tables, with and without a sync on every commit.
func TestOpenWriteReadReopen(t *testing.T) {
	t.Parallel()
	tiny := TinyTuning()
	for name, cfg := range map[string]Config{
		"tiny":            {Schema: SchemaDefault, Tuning: tiny},
		"with sync":       {Schema: SchemaDefault, Sync: true, Tuning: tiny},
		"bench settings":  {Schema: SchemaDefault, Tuning: BenchTuning()},
		"no tuning given": {Schema: SchemaDefault},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			cfg.FS = fs
			cfg.Logger = quietLogger{}
			kv, err := Open("db", BytewiseLayout, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := [][]byte{{0}, {1}, {1, 0}, {1, 0, 0xFF}, {1, 1}, {2}, {0xFF, 0xFF}}
			for _, i := range []int{3, 0, 6, 2, 5, 1, 4} {
				if err := kv.Set(want[i], []byte{byte(i)}, kv.WriteOptions()); err != nil {
					t.Fatal(err)
				}
			}
			check := func(kv *KV) {
				t.Helper()
				it, err := kv.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = it.Close() }()
				var got [][]byte
				for ok := it.First(); ok; ok = it.Next() {
					got = append(got, bytes.Clone(it.Key()))
				}
				if !equalAll(got, want) {
					t.Fatalf("keys in order = %x, want %x", got, want)
				}
			}
			check(kv)
			if err := kv.Settle(); err != nil {
				t.Fatal(err)
			}
			check(kv) // now from tables
			if size, err := kv.Size(); err != nil || size <= 0 {
				t.Errorf("Size = %d, %v", size, err)
			}
			if got := kv.Config(); got.Schema != cfg.Schema || got.Sync != cfg.Sync || got.Tuning == (Tuning{}) {
				t.Errorf("Config() = %+v, want %+v", got, cfg)
			}
			if kv.Dir() != "db" {
				t.Errorf("Dir = %q", kv.Dir())
			}
			if err := kv.Close(); err != nil {
				t.Fatal(err)
			}
			if err := kv.Close(); err != nil {
				t.Errorf("a second Close: %v", err)
			}
			kv, err = Open("db", BytewiseLayout, cfg)
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			defer func() { _ = kv.Close() }()
			check(kv)
		})
	}
}

func TestBytewiseLayout(t *testing.T) {
	t.Parallel()
	tiny := TinyTuning()
	for name, cfg := range map[string]Config{
		"the crdb1 schema":  {Schema: SchemaCRDB, Tuning: tiny},
		"no schema":         {Tuning: tiny},
		"a time filter":     {Schema: SchemaDefault, TimeFilter: true, Tuning: tiny},
		"a nonsense schema": {Schema: 9, Tuning: tiny},
	} {
		cfg.FS = vfs.NewMem()
		if _, err := Open("db", BytewiseLayout, cfg); err == nil {
			t.Errorf("the bytewise layout opened with %s", name)
		}
	}
	if _, err := Open("db", Layout{}, Config{Schema: SchemaDefault, Tuning: tiny, FS: vfs.NewMem()}); err == nil {
		t.Error("a layout with no comparer opened")
	}
}

// A layout made with NewLayout keeps what it was given, whatever the caller does
// to the map and the slice afterwards, and offers the filter only if it was given
// collectors.
func TestNewLayout(t *testing.T) {
	t.Parallel()
	def := colblk.DefaultKeySchema(pebble.DefaultComparer, 16)
	schemas := map[Schema]*colblk.KeySchema{SchemaDefault: &def}
	collector := func() pebble.BlockPropertyCollector { return nil } // never called: only installed
	collectors := []func() pebble.BlockPropertyCollector{collector}
	l := NewLayout("custom", pebble.DefaultComparer, schemas, collectors)
	delete(schemas, SchemaDefault)
	collectors[0] = nil

	if l.Name != "custom" || l.Comparer != pebble.DefaultComparer {
		t.Errorf("layout = %q over %v", l.Name, l.Comparer)
	}
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()
	cfg := Config{Schema: SchemaDefault, TimeFilter: true, Tuning: TinyTuning()}
	o, err := buildOptions(l, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.BlockPropertyCollectors) != 1 || o.BlockPropertyCollectors[0] == nil {
		t.Errorf("collectors installed = %d, the first nil: %v", len(o.BlockPropertyCollectors), o.BlockPropertyCollectors[0] == nil)
	}
	if _, err := buildOptions(NewLayout("none", pebble.DefaultComparer, schemas, nil), Config{Schema: SchemaDefault}, cache); err == nil {
		t.Error("a layout whose schemas were all removed offered one")
	}
}

// A database that is written to caches blocks however much of the cache Pebble
// holds back for its memtables, so it gets room for them besides the blocks; a
// read-only one has none and its cache is the size asked for.
func TestTheCacheHasRoomForTheMemtables(t *testing.T) {
	t.Parallel()
	cfg := Config{Tuning: TinyTuning()}
	fs := vfs.NewMem()
	kv := bytewise(t, fs, "db", cfg)
	if got, want := kv.Cache().MaxSize(), cfg.Tuning.CacheBytes+cfg.Tuning.MemTableReserve(); got != want {
		t.Errorf("cache of a database written to = %d, want %d", got, want)
	}
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}
	cfg.ReadOnly = true
	ro := bytewise(t, fs, "db", cfg)
	t.Cleanup(func() { _ = ro.Close() })
	if got, want := ro.Cache().MaxSize(), cfg.Tuning.CacheBytes; got != want {
		t.Errorf("cache of a read-only database = %d, want %d", got, want)
	}
}

// A read-only database reads and cannot be written, and opening it for writing
// afterwards finds nothing left locked or half written.
func TestAReadOnlyDatabaseReadsAndCannotBeWritten(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	kv := bytewise(t, fs, "db", Config{})
	fill(t, kv, 100, 25)
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}

	ro := bytewise(t, fs, "db", Config{ReadOnly: true})
	t.Cleanup(func() { _ = ro.Close() })
	if got := count(t, ro); got != 100 {
		t.Errorf("read %d keys of 100", got)
	}
	if err := ro.Set([]byte("zz"), []byte("v"), ro.WriteOptions()); err == nil {
		t.Error("a write to a read-only database succeeded")
	}
	if err := ro.Flush(); err == nil {
		t.Error("a flush of a read-only database succeeded")
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	rw := bytewise(t, fs, "db", Config{})
	defer func() { _ = rw.Close() }()
	if rw.RecoveredBytes() != 0 {
		t.Errorf("the read-only open left %d bytes to recover", rw.RecoveredBytes())
	}
}

// ColdStart empties the block cache and leaves the tables open: the read that
// follows loads its blocks again, and a read after that loads them from the cache.
func TestColdStartMakesTheNextReadLoadItsBlocksAgain(t *testing.T) {
	t.Parallel()
	cfg := Config{Tuning: TinyTuning(), DisableAutoCompactions: true}
	cfg.Tuning.CacheBytes = 64 << 20
	kv := bytewise(t, vfs.NewMem(), "db", cfg)
	t.Cleanup(func() { _ = kv.Close() })
	fill(t, kv, 300, 100)
	if err := kv.Settle(); err != nil {
		t.Fatal(err)
	}
	misses := func() int64 { return kv.Metrics().BlockCache.Misses }

	count(t, kv) // opens the tables, whose first reads miss for their own sake
	kv.ColdStart()
	before := misses()
	count(t, kv)
	if misses() == before {
		t.Error("a read after ColdStart found every block in the cache")
	}
	before = misses()
	count(t, kv)
	if misses() != before {
		t.Errorf("a second read missed %d times: the cache holds nothing", misses()-before)
	}
	kv.ColdStart()
	if size := kv.Metrics().BlockCache.Size; size != 0 {
		t.Errorf("the cache holds %d bytes after ColdStart", size)
	}
}

// A nil Config.Logger is the package's own, which logs to log/slog and ends the
// process on a fatal condition; a Logger that is set is the one Pebble gets.
func TestConfigLoggerNilIsTheDefaultAndASetOneIsUsed(t *testing.T) {
	t.Parallel()
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()
	cfg := Config{Schema: SchemaDefault, Tuning: TinyTuning()}

	o, err := buildOptions(BytewiseLayout, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Logger.(logger); !ok {
		t.Errorf("a nil Logger gave %T, want the default that ends the process on a fatal condition", o.Logger)
	}

	cfg.Logger = quietLogger{}
	o, err = buildOptions(BytewiseLayout, cfg, cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Logger.(quietLogger); !ok {
		t.Errorf("a set Logger gave %T, want the one that was set", o.Logger)
	}
}
