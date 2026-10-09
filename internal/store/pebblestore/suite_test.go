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

// The checkpoint policies the suite runs under: the default, one that writes a
// checkpoint after a few records so the small workloads reach every checkpoint path
// (written, used, skipped, invalidated, derived by a retention).
var (
	policyDefault = func() *pebblestore.CheckpointOptions { c := pebblestore.DefaultCheckpoints(); return &c }()
	policyK8      = &pebblestore.CheckpointOptions{On: true, KMin: 8, Alpha: 1}
)

func options(p lifecycle.Policy, settle bool, ck *pebblestore.CheckpointOptions) pebblestore.Options {
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync = false // the suite retains and writes thousands of times
	o.SettleRetention = settle
	o.Policy = p
	o.Checkpoints = ck
	return o
}

func inMemory(settle bool, ck *pebblestore.CheckpointOptions) storetest.Factory {
	return inMemoryWaiting(settle, 0, ck)
}

// inMemoryWaiting is inMemory with the longest a settling retention waits for the
// database to be at rest (zero is the default, two minutes).
func inMemoryWaiting(settle bool, deadline time.Duration, ck *pebblestore.CheckpointOptions) storetest.Factory {
	return storetest.Factory{
		Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
			fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
			o := options(p, settle, ck)
			o.SettleDeadline = deadline
			o.FS = fs.(vfs.FS)
			return pebblestore.Open(dir, o)
		},
		Durable: true,
	}
}

func onDisk(settle bool, ck *pebblestore.CheckpointOptions) storetest.Factory {
	return storetest.Factory{
		Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
			return pebblestore.Open(dir, options(p, settle, ck))
		},
		Durable: true,
	}
}

// The store passes the whole conformance suite on real files and on an in-memory
// file system. The real files are the operating system's: the log is replayed and
// the tables are reopened.
func TestConforms(t *testing.T) {
	t.Parallel()
	matrix := map[string]storetest.Factory{
		"default/files":     onDisk(false, policyDefault),
		"default/in memory": inMemory(false, policyDefault),
		"k8/files":          onDisk(false, policyK8),
		"k8/in memory":      inMemory(false, policyK8),
	}
	if pebblestore.RaceEnabled {
		// The suite's one concurrent part runs in every variant and the policy changes
		// nothing about it, so the race detector gets the default policy in memory and
		// on one real file system; the other variants run in plain builds.
		delete(matrix, "k8/files")
		delete(matrix, "k8/in memory")
	}
	for name, f := range matrix {
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
	policies := map[string]*pebblestore.CheckpointOptions{"default": policyDefault, "k8": policyK8}
	for name, ck := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := inMemory(true, ck)
			workloads := storetest.Workloads()
			for _, w := range []storetest.Workload{workloads[0], workloads[len(workloads)-1]} {
				t.Run("workload "+w.Name, func(t *testing.T) {
					t.Parallel()
					if pebblestore.RaceEnabled {
						t.Skip("single goroutine; plain builds run it, the race detector keeps the concurrent reads below, for both policies")
					}
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
				s, err := inMemoryWaiting(true, 50*time.Millisecond, ck).Open(t.TempDir(), lifecycle.Policy{})
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
				if pebblestore.RaceEnabled {
					t.Skip("single goroutine; plain builds run it")
				}
				if err := storetest.CheckReopen(onDisk(true, ck)); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
