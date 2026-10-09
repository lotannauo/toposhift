package pebblekv

import (
	"bytes"
	"log/slog"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// pebbleVersion is the Pebble the root module is pinned to. A database written by
// one version of Pebble is only guaranteed to be read by the same or a newer one,
// and a bump changes the bytes the measurements were made on, so it needs a new
// measurement; changing this constant is part of that change. The mise task
// check:pebble-pin keeps the bench module on the same version.
const pebbleVersion = "v2.1.7"

// Everything that shapes what Pebble writes is stated, not inherited: the format
// major version, value separation, and the names the tables persist (the
// comparer, the key schema and the block-property collectors). A database written
// under these names cannot be opened under others, so they are pinned here: a
// Pebble upgrade that renamed one, or moved a default, fails this test instead of
// failing on an existing database.
func TestPebbleIsPinned(t *testing.T) {
	t.Parallel()

	// The linked Pebble is the pinned one. Test binaries list their dependencies.
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("the test binary has no build information, so the linked Pebble version cannot be checked here")
	}
	found := false
	for _, dep := range bi.Deps {
		if dep.Path == "github.com/cockroachdb/pebble/v2" {
			found = true
			if dep.Replace != nil {
				t.Errorf("Pebble is replaced by %s", dep.Replace.Path)
			}
			if dep.Version != pebbleVersion {
				t.Errorf("Pebble is %s, the store is pinned to %s", dep.Version, pebbleVersion)
			}
		}
	}
	if !found {
		t.Error("Pebble is not among the dependencies of the test binary")
	}

	// The format the database is opened at is written out, and is the format the
	// pinned version calls the one with value separation.
	fs := vfs.NewMem()
	kv := bytewise(t, fs, "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	if got := kv.FormatMajorVersion(); got != pebble.FormatValueSeparation {
		t.Errorf("the database is at format major version %d, want %d (FormatValueSeparation)", got, pebble.FormatValueSeparation)
	}
	o := kv.Options()
	if o.FormatMajorVersion != pebble.FormatValueSeparation {
		t.Errorf("the options ask for format major version %d, want %d", o.FormatMajorVersion, pebble.FormatValueSeparation)
	}
	if policy := o.Experimental.ValueSeparationPolicy(); policy.Enabled {
		t.Errorf("value separation is on: %+v", policy)
	}
	// Block compression is Pebble's default, Snappy, which is pure Go. The README
	// says so, and a Pebble whose default moved to Zstd would change the bytes.
	for i := range o.Levels {
		if got := o.Levels[i].Compression().Name; got != "Snappy" {
			t.Errorf("level %d compresses blocks with %q, want Snappy", i, got)
		}
	}
	if o.Comparer.Name != "leveldb.BytewiseComparator" {
		t.Errorf("comparer in the options = %q", o.Comparer.Name)
	}
	if len(o.BlockPropertyCollectors) != 0 {
		t.Errorf("%d block-property collectors are installed", len(o.BlockPropertyCollectors))
	}

	// And the tables say the same.
	for i := range 40 {
		if err := kv.Set([]byte{1, byte(i)}, bytes.Repeat([]byte{byte(i)}, 300), kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Settle(); err != nil {
		t.Fatal(err)
	}
	props, err := kv.TableProperties()
	if err != nil || len(props) == 0 {
		t.Fatalf("TableProperties = %d tables, %v", len(props), err)
	}
	for _, p := range props {
		if p.ComparerName != "leveldb.BytewiseComparator" {
			t.Errorf("a table names the comparer %q", p.ComparerName)
		}
		if p.KeySchemaName != "DefaultKeySchema(leveldb.BytewiseComparator,16)" {
			t.Errorf("a table names the key schema %q", p.KeySchemaName)
		}
		// The layout installs no collector. The one a table does name is Pebble's own,
		// which it adds to every table to find the obsolete keys; it is persisted by
		// name like the others, so it is pinned too.
		if p.PropertyCollectorNames != "[obsolete-key]" {
			t.Errorf("a table names the block-property collectors %q, want only Pebble's own [obsolete-key]", p.PropertyCollectorNames)
		}
		if _, only := p.UserProperties["obsolete-key"]; !only || len(p.UserProperties) != 1 {
			t.Errorf("a table has the user properties %v, want only obsolete-key", p.UserProperties)
		}
		if p.NumValueBlocks != 0 || p.NumValuesInValueBlocks != 0 {
			t.Errorf("a table separated values: %d blocks, %d values", p.NumValueBlocks, p.NumValuesInValueBlocks)
		}
	}
	if got := Schema(SchemaDefault).String(); got != "default" {
		t.Errorf("SchemaDefault = %q", got)
	}
}

// The options a config comes to are what the config says. Whether Pebble then
// compacts on reads cannot be seen at the scale of a test (read-triggered
// compactions do not fire on a few thousand keys), so the setting that turns them
// off is checked here, where it is made.
func TestTheOptionsAreWhatTheConfigSays(t *testing.T) {
	t.Parallel()
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()

	tune := Tuning{MemTableSize: 3 << 20, BlockSize: 6 << 10, TargetFileSize: 5 << 20, LBaseMaxBytes: 7 << 20, L0CompactionThreshold: 3, CacheBytes: 1 << 20}
	base := Config{Schema: SchemaDefault, Tuning: tune}

	o, err := buildOptions(BytewiseLayout, base, cache)
	if err != nil {
		t.Fatal(err)
	}
	if o.MemTableSize != 3<<20 || o.Levels[0].BlockSize != 6<<10 || o.Levels[0].IndexBlockSize != 6<<10 ||
		o.TargetFileSizes[0] != 5<<20 || o.LBaseMaxBytes != 7<<20 || o.L0CompactionThreshold != 3 {
		t.Errorf("tuning did not reach the options: %+v", o)
	}
	if o.Cache != cache || o.FormatMajorVersion != pebble.FormatValueSeparation {
		t.Error("the cache or the format version is not the one asked for")
	}
	if o.DisableAutomaticCompactions || o.Experimental.ReadSamplingMultiplier != 0 {
		t.Errorf("a default config turned compactions off: auto disabled %v, sampling %d",
			o.DisableAutomaticCompactions, o.Experimental.ReadSamplingMultiplier)
	}
	if o.Experimental.ValueSeparationPolicy == nil || o.Experimental.ValueSeparationPolicy().Enabled {
		t.Error("value separation is not explicitly off")
	}
	if _, ok := o.Logger.(logger); !ok {
		t.Errorf("the default logger is %T, want the log/slog one", o.Logger)
	}

	off := base
	off.DisableAutoCompactions, off.DisableReadCompactions = true, true
	o, err = buildOptions(BytewiseLayout, off, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !o.DisableAutomaticCompactions {
		t.Error("DisableAutoCompactions did not reach the options")
	}
	if o.Experimental.ReadSamplingMultiplier != -1 {
		t.Errorf("DisableReadCompactions did not reach the options: the sampling multiplier is %d, want -1, which stops read sampling", o.Experimental.ReadSamplingMultiplier)
	}

	if _, err := buildOptions(BytewiseLayout, Config{Schema: SchemaDefault, TimeFilter: true, Tuning: tune}, cache); err == nil {
		t.Error("layout L was given a time filter it does not have")
	}
	if _, err := buildOptions(BytewiseLayout, Config{Schema: SchemaCRDB, Tuning: tune}, cache); err == nil {
		t.Error("layout L was given a key schema it does not have")
	}
}

// Options hands out a copy: changing it changes nothing in the database.
func TestOptionsIsACopy(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	t.Cleanup(func() { _ = kv.Close() })
	o := kv.Options()
	o.MemTableSize = 1
	o.FormatMajorVersion = pebble.FormatMinSupported
	if again := kv.Options(); again.MemTableSize == 1 || again.FormatMajorVersion != pebble.FormatValueSeparation {
		t.Errorf("a change to the copy reached the database's options: %+v", again.MemTableSize)
	}
}

// Pebble's informational messages go to log/slog at debug, its errors at warn,
// and a fatal condition is logged at error and ends the process, with status 70.
// The test swaps slog's default logger and the exit, so it is not parallel.
func TestTheLoggerSendsPebbleToSlogAndEndsTheProcessOnFatal(t *testing.T) {
	var out bytes.Buffer
	old := slog.Default()
	oldFatal := fatal
	t.Cleanup(func() { slog.SetDefault(old); fatal = oldFatal })
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))

	var l pebble.Logger = logger{}
	l.Infof("flushed %d tables", 3)
	l.Errorf("slow disk %s", "sda")
	got := out.String()
	if !strings.Contains(got, "level=DEBUG") || !strings.Contains(got, "flushed 3 tables") || !strings.Contains(got, "component=pebble") {
		t.Errorf("Infof logged %q, want a debug line saying so", got)
	}
	if !strings.Contains(got, "level=WARN") || !strings.Contains(got, "slow disk sda") {
		t.Errorf("Errorf logged %q, want a warning saying so", got)
	}

	// Below debug level the informational messages are dropped.
	out.Reset()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo})))
	l.Infof("flushed %d tables", 3)
	if out.Len() != 0 {
		t.Errorf("Infof logged %q with debug logging off", out.String())
	}

	// A fatal condition is logged, then ends the process through fatal; here fatal
	// panics so the test can see what it was given.
	out.Reset()
	fatal = func(msg string) { panic("exit: " + msg) }
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		l.Fatalf("lost %d writes", 2)
		t.Error("Fatalf returned: the process would have gone on")
	}()
	if recovered != "exit: pebble: lost 2 writes" {
		t.Errorf("Fatalf ended the process with %v, want the message", recovered)
	}
	if got := out.String(); !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "lost 2 writes") {
		t.Errorf("Fatalf logged %q, want an error line saying so", got)
	}
	if fatalExitCode != 70 {
		t.Errorf("the exit status is %d, want 70", fatalExitCode)
	}
}
