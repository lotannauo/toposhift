package rulejudge

import (
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// The comparable counters are the layout's own and the reads, seeks and steps of
// each kind of read; the cold, allocation and timing counters, the iterator's
// internals and RL's diagnostics are not.
func TestComparable(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"read.neighbors.reads":            true,
		"read.neighbors.seeks":            true,
		"read.window.steps":               true,
		"read.records_stepped":            true,
		"read.checkpoint_hits":            true,
		"records_written":                 true,
		runner.CounterBlockLoads:          false,
		runner.CounterColdBlockBytes:      false,
		runner.CounterColdCacheBytes:      false,
		runner.CounterAllocs:              false,
		runner.CounterAllocBytes:          false,
		"retain_ns":                       false,
		"read.neighbors.internal_seeks":   false,
		"read.neighbors.block_bytes":      false,
		"read.neighbors.rocks.block_read": false,
	} {
		if got := Comparable(name); got != want {
			t.Errorf("Comparable(%q) = %v, want %v", name, got, want)
		}
	}
}

// RL's Go memory limit is Pebble's less the cache and the four memtables, which
// Pebble holds on the Go heap and RocksDB does not; no limit stays none.
func TestGoMemoryLimit(t *testing.T) {
	t.Parallel()
	bench := Tuning(64 << 20)
	got, err := GoMemoryLimit(11<<30, bench)
	if err != nil || got != 11<<30-320<<20 {
		t.Errorf("GoMemoryLimit(11 GiB) = %d, %v; want 11 GiB less 320 MiB", got, err)
	}
	if got, err := GoMemoryLimit(0, bench); got != 0 || err != nil {
		t.Errorf("no limit gives %d, %v", got, err)
	}
	if _, err := GoMemoryLimit(100<<20, bench); err == nil {
		t.Error("a limit below the charge is accepted")
	}
	if Tuning(0) != pebblekv.BenchTuning() {
		t.Error("a plan without a cache does not take the bench tuning's")
	}
}
