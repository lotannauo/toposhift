package pebblemvcc_test

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblemvcc"
)

// TestMain lowers how many random workloads rapid runs in each variant. Under
// the race detector Pebble also runs its own invariant checks (it enables them
// for race builds), which makes each workload cost seconds, and the six fixed
// workloads, the scripted checks and the reopen and concurrency checks already
// give every variant its coverage. A deeper run is a flag away:
//
//	go test ./spike/pebblemvcc -rapid.checks=100
//
// A -rapid.checks on the command line wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", "4"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// variants are the configurations the layout is measured in: the two key
// schemas, each with the time-interval block property off and on.
func variants() map[string]pebblemvcc.Options {
	out := map[string]pebblemvcc.Options{}
	for _, schema := range []pebblekv.Schema{pebblekv.SchemaCRDB, pebblekv.SchemaDefault} {
		for _, filter := range []bool{false, true} {
			name := schema.String()
			if filter {
				name += "+filter"
			}
			out[name] = pebblemvcc.Options{Config: pebblekv.Config{Schema: schema, TimeFilter: filter, Tuning: pebblekv.TinyTuning()}}
		}
	}
	return out
}

// memFS keeps one in-memory file system per directory, so an engine closed and
// opened again over the same directory finds what it left.
var memFS sync.Map

func inMemory(opts pebblemvcc.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
		o := opts // the factory is called from many goroutines
		o.FS = fs.(vfs.FS)
		return pebblemvcc.Open(dir, o)
	}
}

func onDisk(opts pebblemvcc.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) { return pebblemvcc.Open(dir, opts) }
}

// The primary variant, crdb1 with no filter, passes the whole conformance test
// ([conformance.Run]). The others pass the part of it that exercises what they
// change: three of the fixed workloads (ordinary churn, watch mode across 2^32,
// and coalesced runs with lateness) with and without retention, the scripted
// checks, and the check that reads running beside writes see whole batches. The
// long skewed workload is left out because it tests the engine's handling of huge
// sequence numbers and long histories, not a schema or a filter. One variant,
// default with the filter, also reopens from real files. Under the race detector
// Pebble runs its own invariant checks, which make each workload cost seconds, so
// running every check on every variant would cost more than a schema or a filter
// warrants.
func TestConforms(t *testing.T) {
	t.Parallel()
	for name, opts := range variants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if name == primary {
				conformance.Run(t, inMemory(opts))
				return
			}
			conformsPartly(t, inMemory(opts))
		})
	}
}

const primary = "crdb1"

func conformsPartly(t *testing.T, f conformance.Factory) {
	t.Helper()
	cfgs := conformance.Configs()
	for _, i := range []int{0, 2, 5} {
		for _, retain := range [][]float64{nil, {0.3, 0.7}} {
			if retain == nil && i != 0 && i != 5 {
				continue
			}
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				if err := conformance.Check(open(t, f), cfgs[i], conformance.Options{RetainAt: retain}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	for name, check := range map[string]func(engine.Engine) error{
		"write contract":   conformance.CheckWriteContract,
		"read contract":    conformance.CheckReadContract,
		"instant":          conformance.CheckInstant,
		"producers":        conformance.CheckProducers,
		"relations":        conformance.CheckRelations,
		"extremes":         conformance.CheckExtremes,
		"concurrent reads": conformance.CheckConcurrentReads,
	} {
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
// tables are reopened, and a closed engine comes back with everything.
func TestReopensFromRealFiles(t *testing.T) {
	t.Parallel()
	opts := variants()["default+filter"]
	if err := conformance.CheckReopen(onDisk(opts)); err != nil {
		t.Fatal(err)
	}
}
