package pebblestore_test

import (
	"flag"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestMain sets how many random workloads rapid runs. The default is low because
// the full conformance suite already takes seconds per workload; the environment
// variable TOPOSHIFT_RAPID_CHECKS raises it, and a -rapid.checks on the command
// line wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", storetest.RapidChecks("8")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// memFS keeps one in-memory file system per directory, so a store closed and
// opened again over the same directory finds what it left.
var memFS sync.Map

func options(p lifecycle.Policy, settle bool) pebblestore.Options {
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync = false // the suite retains and writes thousands of times
	o.SettleRetention = settle
	o.Policy = p
	return o
}

func inMemory(settle bool) storetest.Factory { return inMemoryWaiting(settle, 0) }

// inMemoryWaiting is inMemory with the longest a settling retention waits for the
// database to be at rest (zero is the default, two minutes).
func inMemoryWaiting(settle bool, deadline time.Duration) storetest.Factory {
	return storetest.Factory{
		Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
			fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
			o := options(p, settle)
			o.SettleDeadline = deadline
			o.FS = fs.(vfs.FS)
			return pebblestore.Open(dir, o)
		},
		Durable: true,
	}
}

func onDisk(settle bool) storetest.Factory {
	return storetest.Factory{
		Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
			return pebblestore.Open(dir, options(p, settle))
		},
		Durable: true,
	}
}

// The store passes the whole conformance suite on real files and on an in-memory
// file system. The real files are the operating system's: the log is replayed and
// the tables are reopened.
func TestConforms(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]storetest.Factory{
		"files":     onDisk(false),
		"in memory": inMemory(false),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			storetest.Run(t, f)
		})
	}
}

// A retention that settles the database waits for its compactions, which costs a
// fixed time per retention, and the suite retains hundreds of times. So the store
// that settles runs the part of the suite a settled retention can change, on an
// in-memory file system: the differential check on the first workload and on the
// one that quarantines, with two retentions each, and reads that run beside writes
// and settling retentions; and, on real files, the reopening of a database whose
// retentions settled. The retention is the same code with and without the settling
// (the store-bytes test of the benchmarks freezes that no key differs).
func TestConformsWhileSettling(t *testing.T) {
	t.Parallel()
	f := inMemory(true)
	workloads := storetest.Workloads()
	for _, w := range []storetest.Workload{workloads[0], workloads[len(workloads)-1]} {
		t.Run("workload "+w.Name, func(t *testing.T) {
			t.Parallel()
			s, err := f.Open(t.TempDir(), w.Policy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err := storetest.Check(s, w, storetest.Options{RetainAt: []float64{0.3, 0.7}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Every retention flushes, and then waits for the database to be at rest for at
	// least a second; with a deadline of 50 ms it flushes, compacts what it can
	// meanwhile, and goes on, which is what reads running beside it have to meet.
	t.Run("concurrent reads", func(t *testing.T) {
		t.Parallel()
		s, err := inMemoryWaiting(true, 50*time.Millisecond).Open(t.TempDir(), lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := storetest.CheckConcurrentReads(s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("reopen", func(t *testing.T) {
		t.Parallel()
		if testing.Short() {
			t.Skip("trimmed run: the reopen check runs in the full tier")
		}
		if err := storetest.CheckReopen(onDisk(true)); err != nil {
			t.Fatal(err)
		}
	})
}
