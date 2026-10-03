package pebblelog_test

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
)

// TestMain lowers how many random workloads rapid runs. Under the race detector
// Pebble also runs its own invariant checks, which makes each workload cost
// seconds, and the six fixed workloads, the scripted checks and the reopen and
// concurrency checks carry the coverage. A deeper run is a flag away:
//
//	go test ./spike/pebblelog -rapid.checks=100
//
// A -rapid.checks on the command line wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", "4"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func options(c pebblelog.CheckpointOptions) pebblelog.Options {
	return pebblelog.Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}, Checkpoints: c}
}

// variant is one checkpoint setting and how much of the conformance test it
// runs. A workload's event times fall on whole seconds, so only a lag of one
// nanosecond puts the checkpoint instant on a record's (and so tests records
// exactly at it), and a lag of whole seconds keeps late records from invalidating.
type variant struct {
	checkpoints pebblelog.CheckpointOptions
	// configs and retains are the fixed workloads and retention schedules run;
	// the primary variant runs everything instead.
	configs    []int
	retains    [][]float64
	concurrent bool
}

const primary = "k8"

var (
	both = [][]float64{nil, {0.3, 0.7}}
	late = [][]float64{{0.3, 0.7}}
)

func variants() map[string]variant {
	return map[string]variant{
		"off":     {pebblelog.CheckpointOptions{}, []int{0}, late, false},
		"k8":      {pebblelog.CheckpointOptions{On: true, KMin: 8, Alpha: 1}, nil, nil, true},
		"stress":  {pebblelog.CheckpointOptions{On: true, KMin: 1}, []int{0}, both, true},
		"lag 1ns": {pebblelog.CheckpointOptions{On: true, KMin: 1, Lag: time.Nanosecond}, []int{5}, both, false},
		"lag 2s":  {pebblelog.CheckpointOptions{On: true, KMin: 3, Alpha: 1, Lag: 2 * time.Second}, []int{0}, late, false},
	}
}

// memFS keeps one in-memory file system per directory, so an engine closed and
// opened again over the same directory finds what it left.
var memFS sync.Map

func inMemory(opts pebblelog.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
		o := opts // the factory is called from many goroutines
		o.FS = fs.(vfs.FS)
		return pebblelog.Open(dir, o)
	}
}

func onDisk(opts pebblelog.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) { return pebblelog.Open(dir, opts) }
}

// The variant the full conformance test runs in must exist and write checkpoints:
// a name that matched no variant once left the full run out without a failure.
func TestThePrimaryVariantRunsWithCheckpoints(t *testing.T) {
	t.Parallel()
	v, ok := variants()[primary]
	if !ok || !v.checkpoints.On {
		t.Fatalf("variant %q is %+v, %v: it must exist and have checkpoints on", primary, v, ok)
	}
}

// The primary variant (a checkpoint when a prefix has had eight records written
// since its last) passes the whole conformance test ([conformance.Run]) on an
// in-memory file system. The others pass the part of it that exercises what they
// change: a fixed workload or two with and without retention, the scripted checks,
// and for two of them the check that reads running beside writes see whole
// batches. Under the race detector Pebble runs its own invariant checks, which
// make each workload cost seconds.
func TestConforms(t *testing.T) {
	t.Parallel()
	for name, v := range variants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := inMemory(options(v.checkpoints))
			if name == primary {
				conformance.Run(t, f)
				return
			}
			conformsPartly(t, f, v)
		})
	}
}

// The scripted check that writes checkpoints at instants of its own choosing runs
// on every variant that has them, the primary included ([conformance.Run] does not
// run it, because it needs an engine that implements the hook), and says so for
// an engine that does not.
func TestCheckpointCheck(t *testing.T) {
	t.Parallel()
	for name, v := range variants() {
		if !v.checkpoints.On {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := conformance.CheckCheckpoint(open(t, inMemory(options(v.checkpoints)))); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("checkpoints off still implements the hook", func(t *testing.T) {
		t.Parallel()
		if err := conformance.CheckCheckpoint(open(t, inMemory(options(pebblelog.CheckpointOptions{})))); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("an engine without the hook", func(t *testing.T) {
		t.Parallel()
		if err := conformance.CheckCheckpoint(oracle.New()); !errors.Is(err, conformance.ErrNotCheckpointer) {
			t.Fatalf("CheckCheckpoint(oracle) = %v, want ErrNotCheckpointer", err)
		}
	})
}

func conformsPartly(t *testing.T, f conformance.Factory, v variant) {
	t.Helper()
	cfgs := conformance.Configs()
	for _, i := range v.configs {
		for _, retain := range v.retains {
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				if err := conformance.Check(open(t, f), cfgs[i], conformance.Options{RetainAt: retain}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	checks := map[string]func(engine.Engine) error{
		"write contract": conformance.CheckWriteContract,
		"read contract":  conformance.CheckReadContract,
		"instant":        conformance.CheckInstant,
		"producers":      conformance.CheckProducers,
		"relations":      conformance.CheckRelations,
		"extremes":       conformance.CheckExtremes,
	}
	if v.concurrent {
		checks["concurrent reads"] = conformance.CheckConcurrentReads
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(open(t, f)); err != nil {
				t.Fatal(err)
			}
		})
	}
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

// The files are the operating system's, not memory: the log is replayed, the
// tables are reopened, and a closed engine comes back with everything, here with
// checkpoints in the database whose list the writer must read back.
func TestReopensFromRealFiles(t *testing.T) {
	t.Parallel()
	if err := conformance.CheckReopen(onDisk(options(variants()[primary].checkpoints))); err != nil {
		t.Fatal(err)
	}
}
