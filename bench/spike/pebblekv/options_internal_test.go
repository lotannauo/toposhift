package pebblekv

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

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

	o, err := buildOptions(CockroachLayout, base, cache)
	if err != nil {
		t.Fatal(err)
	}
	if o.MemTableSize != 3<<20 || o.Levels[0].BlockSize != 6<<10 || o.Levels[0].IndexBlockSize != 6<<10 ||
		o.TargetFileSizes[0] != 5<<20 || o.LBaseMaxBytes != 7<<20 || o.L0CompactionThreshold != 3 {
		t.Errorf("tuning did not reach the options: %+v", o)
	}
	if o.Cache != cache || o.FormatMajorVersion != pebble.FormatNewest {
		t.Error("the cache or the format version is not the one asked for")
	}
	if o.DisableAutomaticCompactions || o.Experimental.ReadSamplingMultiplier != 0 {
		t.Errorf("a default config turned compactions off: auto disabled %v, sampling %d",
			o.DisableAutomaticCompactions, o.Experimental.ReadSamplingMultiplier)
	}
	if o.BlockPropertyCollectors != nil {
		t.Error("a config without the filter installed collectors")
	}

	off := base
	off.DisableAutoCompactions, off.DisableReadCompactions, off.TimeFilter = true, true, true
	o, err = buildOptions(CockroachLayout, off, cache)
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

	if _, err := buildOptions(BytewiseLayout, Config{Schema: SchemaDefault, TimeFilter: true, Tuning: tune}, cache); err == nil {
		t.Error("layout L was given a time filter it does not have")
	}
	if _, err := buildOptions(BytewiseLayout, Config{Schema: SchemaCRDB, Tuning: tune}, cache); err == nil {
		t.Error("layout L was given a key schema it does not have")
	}
}
