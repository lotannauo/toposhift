package rootlog_test

import (
	"errors"
	"flag"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/rootlog"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// TestMain sets how many random workloads rapid runs; see the same function of
// spike/pebblelog. A -rapid.checks on the command line wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func options(c pebblestore.CheckpointOptions) rootlog.Options {
	return rootlog.Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}, Checkpoints: c}
}

// The two settings the whole conformance test runs in: no checkpoints, and a
// checkpoint when a prefix has had eight records written since its last.
var settings = map[string]pebblestore.CheckpointOptions{
	"off":  {},
	"k8a1": {On: true, KMin: 8, Alpha: 1},
}

// memFS keeps one in-memory file system per directory, so an engine closed and
// opened again over the same directory finds what it left.
var memFS sync.Map

func inMemory(opts rootlog.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
		o := opts // the factory is called from many goroutines
		o.FS = fs.(vfs.FS)
		return rootlog.Open(dir, o)
	}
}

func onDisk(opts rootlog.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) { return rootlog.Open(dir, opts) }
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

// Both settings pass the whole conformance test, the reopen check and the
// concurrent-read check among it, on an in-memory file system.
func TestConforms(t *testing.T) {
	t.Parallel()
	for name, c := range settings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			conformance.Run(t, inMemory(options(c)))
		})
	}
}

// The scripted check that writes checkpoints at instants of its own choosing runs
// with checkpoints on and with them off (the hook is there either way), and says
// so for an engine that does not have it.
func TestCheckpointCheck(t *testing.T) {
	t.Parallel()
	for name, c := range settings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := conformance.CheckCheckpoint(open(t, inMemory(options(c)))); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("an engine without the hook", func(t *testing.T) {
		t.Parallel()
		if err := conformance.CheckCheckpoint(oracle.New()); !errors.Is(err, conformance.ErrNotCheckpointer) {
			t.Fatalf("CheckCheckpoint(oracle) = %v, want ErrNotCheckpointer", err)
		}
	})
}

// The files are the operating system's, not memory: the log is replayed, the
// tables are reopened, and a closed engine comes back with everything, here with
// checkpoints in the database.
func TestReopensFromRealFiles(t *testing.T) {
	t.Parallel()
	if conformance.Trimmed() {
		t.Skip("trimmed run: this check syncs real files and runs in the full tier")
	}
	if err := conformance.CheckReopen(onDisk(options(settings["k8a1"]))); err != nil {
		t.Fatal(err)
	}
}

// What the engine says about the bytes it holds is true: the payload parts add up
// to the payloads of the stream, direction by direction, the extension part is
// there exactly when the stream has run extensions, and the layers it reports are
// the ones written to.
func TestMeasurementHooksTellTheTruth(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // starts no goroutine of its own; the full tier runs it
	f := inMemory(options(settings["k8a1"]))
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
