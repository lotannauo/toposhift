package rockslog_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// The RL names are RL/off and RL/ with a policy written as layout L writes it; the
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
		l, _ := candidates.Lookup("L/" + name[len("RL/"):])
		if name == "RL/off" {
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
	for _, name := range []string{"", "RL/", "RL/on", "RL/k64a2l5m0s", "RL/k0a1", "RL/k8", "L/off", "M/crdb1", "Lroot/off", "rl/off"} {
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

// RL's Go memory limit is Pebble's less the cache and the four memtables, which
// Pebble holds on the Go heap and RocksDB does not; no limit stays none.
func TestGoMemoryLimit(t *testing.T) {
	t.Parallel()
	bench := pebblekv.BenchTuning()
	bench.CacheBytes = 64 << 20
	got, err := rockslog.GoMemoryLimit(11<<30, bench)
	if err != nil || got != 11<<30-320<<20 {
		t.Errorf("GoMemoryLimit(11 GiB) = %d, %v; want 11 GiB less 320 MiB", got, err)
	}
	if got, err := rockslog.GoMemoryLimit(0, bench); got != 0 || err != nil {
		t.Errorf("no limit gives %d, %v", got, err)
	}
	if _, err := rockslog.GoMemoryLimit(100<<20, bench); err == nil {
		t.Error("a limit below the charge is accepted")
	}
}
