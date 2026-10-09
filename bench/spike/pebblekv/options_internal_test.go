package pebblekv

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// The options a config comes to are what the config says. Whether Pebble then
// compacts on reads cannot be seen at the scale of a test (read-triggered
// compactions do not fire on a few thousand keys), so the setting that turns them
// off is checked here, where it is made.
func TestTheOptionsAreWhatTheConfigSays(t *testing.T) {
	t.Parallel()
	// open opens a database for the config on a memory file system and returns the
	// options it came to, defaults filled in.
	open := func(layout Layout, cfg Config) (*pebble.Options, error) {
		cfg.FS = vfs.NewMem()
		kv, err := Open("db", layout, cfg)
		if err != nil {
			return nil, err
		}
		defer func() { _ = kv.Close() }()
		return kv.Options(), nil
	}

	tune := Tuning{MemTableSize: 3 << 20, BlockSize: 6 << 10, TargetFileSize: 5 << 20, LBaseMaxBytes: 7 << 20, L0CompactionThreshold: 3, CacheBytes: 1 << 20}
	base := Config{Schema: SchemaDefault, Tuning: tune}

	o, err := open(CockroachLayout, base)
	if err != nil {
		t.Fatal(err)
	}
	if o.MemTableSize != 3<<20 || o.Levels[0].BlockSize != 6<<10 || o.Levels[0].IndexBlockSize != 6<<10 ||
		o.TargetFileSizes[0] != 5<<20 || o.LBaseMaxBytes != 7<<20 || o.L0CompactionThreshold != 3 {
		t.Errorf("tuning did not reach the options: %+v", o)
	}
	if o.FormatMajorVersion != pebble.FormatValueSeparation {
		t.Error("the format version is not the pinned one")
	}
	var defaults pebble.Options
	defaults.EnsureDefaults()
	if o.DisableAutomaticCompactions || o.Experimental.ReadSamplingMultiplier != defaults.Experimental.ReadSamplingMultiplier {
		t.Errorf("a default config changed compactions: auto disabled %v, sampling %d, want Pebble's default %d",
			o.DisableAutomaticCompactions, o.Experimental.ReadSamplingMultiplier, defaults.Experimental.ReadSamplingMultiplier)
	}
	if o.BlockPropertyCollectors != nil {
		t.Error("a config without the filter installed collectors")
	}

	off := base
	off.DisableAutoCompactions, off.DisableReadCompactions, off.TimeFilter = true, true, true
	o, err = open(CockroachLayout, off)
	if err != nil {
		t.Fatal(err)
	}
	if !o.DisableAutomaticCompactions {
		t.Error("DisableAutoCompactions did not reach the options")
	}
	if o.Experimental.ReadSamplingMultiplier != -1 {
		t.Errorf("DisableReadCompactions did not reach the options: the sampling multiplier is %d, want -1, which stops read sampling", o.Experimental.ReadSamplingMultiplier)
	}
	if len(o.BlockPropertyCollectors) == 0 {
		t.Error("the filter was asked for and no collector was installed")
	}

	if _, err := open(BytewiseLayout, Config{Schema: SchemaDefault, TimeFilter: true, Tuning: tune}); err == nil {
		t.Error("layout L was given a time filter it does not have")
	}
	if _, err := open(BytewiseLayout, Config{Schema: SchemaCRDB, Tuning: tune}); err == nil {
		t.Error("layout L was given a key schema it does not have")
	}
}

// The block cache is the tuning's, with room for the memtables on top when the
// database is written to, so that it caches blocks while it is written.
func TestTheCacheIsSizedByTheTuning(t *testing.T) {
	t.Parallel()
	tune := Tuning{MemTableSize: 3 << 20, BlockSize: 6 << 10, TargetFileSize: 5 << 20, LBaseMaxBytes: 7 << 20, L0CompactionThreshold: 3, CacheBytes: 1 << 20}
	fs := vfs.NewMem()
	cfg := Config{Schema: SchemaDefault, Tuning: tune, FS: fs}

	kv, err := Open("db", BytewiseLayout, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := kv.Cache().MaxSize(), tune.CacheBytes+tune.MemTableReserve(); got != want {
		t.Errorf("a writable database has a cache of %d bytes, want %d", got, want)
	}
	if err := kv.CloseClean(); err != nil {
		t.Fatal(err)
	}

	cfg.ReadOnly = true
	kv, err = Open("db", BytewiseLayout, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	if got, want := kv.Cache().MaxSize(), tune.CacheBytes; got != want {
		t.Errorf("a read-only database has a cache of %d bytes, want %d", got, want)
	}
}

// A layout that forgets its recorder fails loudly, not with a run that has no
// read counters.
func TestRecordIterPanicsOnANilRecorder(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Error("RecordIter accepted a nil recorder")
		}
	}()
	RecordIter(nil, "probe", nil)
}
