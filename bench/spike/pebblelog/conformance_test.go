package pebblelog_test

import (
	"flag"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
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

func options() pebblelog.Options {
	return pebblelog.Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}}
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

// The log, with no checkpoints, passes the whole conformance test on an
// in-memory file system: the files and the compactions are Pebble's own, and the
// test is not paying a disk flush for each of the many small databases it opens.
func TestConforms(t *testing.T) {
	t.Parallel()
	conformance.Run(t, inMemory(options()))
}

// The files are the operating system's, not memory: the log is replayed, the
// tables are reopened, and a closed engine comes back with everything.
func TestReopensFromRealFiles(t *testing.T) {
	t.Parallel()
	if err := conformance.CheckReopen(onDisk(options())); err != nil {
		t.Fatal(err)
	}
}
