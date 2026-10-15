package rockslog_test

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockskv"
	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// TestMain sets how many random workloads rapid runs, as layout L's tests on Pebble
// do: a few by default, TOPOSHIFT_RAPID_CHECKS for more, and a -rapid.checks on
// the command line wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// tiny is the options of a test: the tiny tuning, which RocksDB raises to its
// smallest memtable, so that a few thousand records reach several levels.
func tiny(cp rockslog.CheckpointOptions) rockslog.Options {
	return rockslog.Options{Config: rockskv.Config{Tuning: pebblekv.TinyTuning()}, Checkpoints: cp}
}

func onDisk(opts rockslog.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) { return rockslog.Open(dir, opts) }
}

func open(t *testing.T, f conformance.Factory) engine.Engine {
	t.Helper()
	e, err := f(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// The policies the whole conformance test runs on: the product's default policy
// (RL/k64a2l5m, the candidate the comparison times), and the two settings that
// write the most checkpoints on the conformance workloads: one due after eight
// records, and one after every record at an instant 1 ns behind the newest, which
// puts checkpoint instants on records' instants.
var fullPolicies = map[string]rockslog.CheckpointOptions{
	"k64a2l5m": {On: true, KMin: 64, Alpha: 2, Lag: 5 * time.Minute},
	"k8":       {On: true, KMin: 8, Alpha: 1},
	"lag 1ns":  {On: true, KMin: 1, Lag: time.Nanosecond},
}

// RL passes the whole conformance test ([conformance.Run]: the reopen check, reads
// beside writes, the random and fixed workloads and the scripted checks) on disk
// with the tiny tuning, for each policy of fullPolicies, and the part for a
// configuration that is not the primary ([conformance.RunPartial]) with no
// checkpoints.
func TestConforms(t *testing.T) {
	t.Parallel()
	for name, cp := range fullPolicies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			conformance.Run(t, onDisk(tiny(cp)))
		})
	}
	t.Run("off", func(t *testing.T) {
		t.Parallel()
		conformance.RunPartial(t, onDisk(tiny(rockslog.CheckpointOptions{})))
	})
}

// The scripted check that writes checkpoints at instants of its own choosing runs
// on every policy with checkpoints, and with them off (the hook is there either
// way).
func TestCheckpointCheck(t *testing.T) {
	t.Parallel()
	policies := map[string]rockslog.CheckpointOptions{"off": {}}
	for name, cp := range fullPolicies {
		policies[name] = cp
	}
	for name, cp := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := conformance.CheckCheckpoint(open(t, onDisk(tiny(cp)))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// What the engine says about the bytes it holds is true (the measurement hooks),
// with run extensions in the stream and without.
func TestMeasurementHooksTellTheTruth(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	f := onDisk(tiny(fullPolicies["k8"]))
	t.Run("with run extensions", func(t *testing.T) {
		t.Parallel()
		if err := conformance.CheckMeasurement(open(t, f)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("without", func(t *testing.T) {
		t.Parallel()
		if err := conformance.CheckMeasurementPlain(open(t, f)); err != nil {
			t.Fatal(err)
		}
	})
}

// Compacting everything keeps every answer.
func TestAnswersSurviveCompactingEverything(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	cfgs := conformance.Configs()
	for _, i := range []int{1, 5, 6} {
		t.Run(fmt.Sprintf("config %d", i), func(t *testing.T) {
			t.Parallel()
			f := onDisk(tiny(fullPolicies["k8"]))
			if err := conformance.Check(open(t, f), cfgs[i], conformance.Options{RetainAt: []float64{0.3, 0.7}, CompactAtEnd: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Reads beside writes see whole commits with checkpoints off too, where a retention
// rewrites prefixes beside the reads.
func TestConcurrentReadsWithoutCheckpoints(t *testing.T) {
	t.Parallel()
	if err := conformance.CheckConcurrentReads(open(t, onDisk(tiny(rockslog.CheckpointOptions{})))); err != nil {
		t.Fatal(err)
	}
}
