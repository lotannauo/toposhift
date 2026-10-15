package rockslog_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// The RL names are RL/off and RL/ with a policy written as layout L writes it, and
// the RL4 names likewise with RL4/; the
// candidates are external, of layout L, and declare cgo; and none can be taken for
// a candidate of the bench module.
func TestVariantNames(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]rockslog.CheckpointOptions{
		"RL/off":           {},
		"RL/k64a2l5m":      {On: true, KMin: 64, Alpha: 2, Lag: 5 * time.Minute},
		"RL/k8a1":          {On: true, KMin: 8, Alpha: 1},
		"RL/k64a2l1ns":     {On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond},
		"RL/k64a2l1h30m":   {On: true, KMin: 64, Alpha: 2, Lag: 90 * time.Minute},
		"RL/k32a0.5l1nsr4": {On: true, KMin: 32, Alpha: 0.5, Lag: time.Nanosecond, Replace: 4},
		"RL4/off":          {},
		"RL4/k64a2l5m":     {On: true, KMin: 64, Alpha: 2, Lag: 5 * time.Minute},
	} {
		v, err := rockslog.Variant(name, false)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if v.Name != name || v.Layout != "L" || !v.CGO() || v.Root() {
			t.Errorf("%s: name %q, layout %q, cgo %v, root %v", name, v.Name, v.Layout, v.CGO(), v.Root())
		}
		if err := candidates.CheckExternalName(v.Name); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		e, err := v.Open(t.TempDir(), candidates.Options{Tuning: pebblekv.TinyTuning()})
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		d, err := e.(engine.Describer).Describe()
		_ = e.Close()
		if err != nil {
			t.Fatal(err)
		}
		_, policy, _ := strings.Cut(name, "/")
		l, _ := candidates.Lookup("L/" + policy)
		if policy == "off" {
			l = candidates.LNoCheckpoints()
		}
		le, err := l.Open("db", candidates.Options{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()})
		if err != nil {
			t.Fatal(err)
		}
		ld, err := le.(engine.Describer).Describe()
		_ = le.Close()
		if err != nil {
			t.Fatal(err)
		}
		if d["checkpoints"] != ld["checkpoints"] || d["layout"] != "L" {
			t.Errorf("%s: checkpoints %q, layout L's %q; layout %q (want %+v)", name, d["checkpoints"], ld["checkpoints"], d["layout"], want)
		}
	}
	for _, name := range []string{"", "RL/", "RL/on", "RL/k64a2l5m0s", "RL/k0a1", "RL/k8", "L/off", "M/crdb1", "Lroot/off", "rl/off", "RL4/", "RL4/on", "rl4/off", "RL2/off"} {
		if _, err := rockslog.Variant(name, false); err == nil {
			t.Errorf("%q is accepted", name)
		}
	}
}

// RL refuses the options it cannot honour: a file system, and a tuning that is not
// mapped to RocksDB's options; and it passes the rest through.
func TestVariantRefusesWhatItCannotHonour(t *testing.T) {
	t.Parallel()
	v, err := rockslog.Variant("RL/off", false)
	if err != nil {
		t.Fatal(err)
	}
	if e, err := v.Open(t.TempDir(), candidates.Options{FS: vfs.NewMem()}); err == nil {
		_ = e.Close()
		t.Error("a file system is accepted")
	}
	odd := pebblekv.BenchTuning()
	odd.TargetFileSize = 1 << 20
	if e, err := v.Open(t.TempDir(), candidates.Options{Tuning: odd}); err == nil {
		_ = e.Close()
		t.Error("an unmapped tuning is accepted")
	}
	cfg, err := rockslog.ConfigOf(candidates.Options{CacheBytes: 7 << 20, Sync: true, DisableAutoCompactions: true, DisableReadCompactions: true, SettleRetention: true, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	want := pebblekv.BenchTuning()
	want.CacheBytes = 7 << 20
	if cfg.Tuning != want || !cfg.Sync || !cfg.DisableAutoCompactions || !cfg.DisableReadCompactions || !cfg.SettleRetention || !cfg.ReadOnly || cfg.Perf {
		t.Errorf("ConfigOf gave %+v", cfg)
	}
	e, err := v.Open(t.TempDir(), candidates.Options{Tuning: pebblekv.TinyTuning(), Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	d, err := e.(engine.Describer).Describe()
	if err != nil {
		t.Fatal(err)
	}
	if d["sync"] != "true" {
		t.Errorf("a candidate opened with Sync describes sync %q", d["sync"])
	}
}

// RL4 writes base-level files of four times the tuning's target file size, where RL
// writes twice it, and says so in its description and its options file; the rest of
// the description is RL's.
func TestRL4DescribesItsBaseFiles(t *testing.T) {
	t.Parallel()
	tiny := pebblekv.TinyTuning()
	describe := func(name string) map[string]string {
		v, err := rockslog.Variant(name, false)
		if err != nil {
			t.Fatal(err)
		}
		e, err := v.Open(t.TempDir(), candidates.Options{Tuning: tiny})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close() }()
		d, err := e.(engine.Describer).Describe()
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	rl, rl4 := describe("RL/k64a2l5m"), describe("RL4/k64a2l5m")
	for _, c := range []struct {
		name   string
		d      map[string]string
		factor int64
	}{{"RL", rl, 2}, {"RL4", rl4, 4}} {
		base := strconv.FormatInt(c.factor*tiny.TargetFileSize, 10)
		if c.d["target_file_size_base"] != base || !strings.Contains(c.d["rocksdb_options"], "target_file_size_base="+base+"\n") {
			t.Errorf("%s: target_file_size_base %q, want %s, in the description and the options file", c.name, c.d["target_file_size_base"], base)
		}
	}
	if got, want := rl4["target_file_bytes_by_level_from_base"], fmt.Sprintf("%d,%d,%d", 4*tiny.TargetFileSize, 4*tiny.TargetFileSize, 8*tiny.TargetFileSize); !strings.HasPrefix(got, want+",") {
		t.Errorf("RL4's file sizes from the base level: %q, want them to begin %s", got, want)
	}
	for _, k := range []string{"checkpoints", "target_file_bytes", "block_bytes", "memtable_bytes", "l_base_max_bytes", "cache_bytes", "sync"} {
		if rl[k] != rl4[k] {
			t.Errorf("described %s: RL %q, RL4 %q", k, rl[k], rl4[k])
		}
	}
}
