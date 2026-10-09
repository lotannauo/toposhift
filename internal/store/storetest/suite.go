package storetest

import (
	"fmt"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Factory opens the store under test. The suite owns every directory it passes
// and removes it afterwards.
type Factory struct {
	// Open opens the store kept under dir, folding entity existence with policy.
	// dir is empty (a new, empty store) or holds what a store opened by the same
	// factory with the same policy left when it was closed; the store must then
	// come back exactly as it was: its records, LastSeq and Horizon. A backend
	// must support at least the policy [QuarantinePolicy] returns, and the zero
	// policy.
	Open func(dir string, policy lifecycle.Policy) (store.Store, error)
	// Durable says that a store opened over the directory of a closed one comes
	// back as it was. The reopen check runs only for a durable store.
	Durable bool
	// NotConcurrent is for test doubles only: the concurrent-read check is
	// skipped and the checks run one at a time. A real store never sets it,
	// because the contract requires reads concurrent with each other and with
	// Write.
	NotConcurrent bool
}

// Trimmed reports whether the heavy part of the suite is left out, as it is under
// go test -short. The full tier (every workload and retention schedule, the random
// workloads, the reopen check) is what decides whether a backend conforms; the
// trimmed tier exists so the same code can also run under the race detector, which
// makes each workload cost seconds, with one workload (the last, which is the one
// that quarantines hosts), the scripted checks and the concurrency check, which is
// the only code here that starts goroutines. It is the one place that decides what
// a short run drops.
func Trimmed() bool { return testing.Short() }

// Workload is one generated stream the suite runs: a generator config, and the
// policy the store under test is opened with.
type Workload struct {
	Name   string
	Config Config
	Policy lifecycle.Policy
}

// Workloads returns the fixed workloads Run uses, in this order: ordinary churn,
// late records with a second producer confirming placements, watch mode only with
// sequence numbers that cross 2^32, coalesced runs with lateness, heavy churn with
// sequence numbers near 2^63, and hosts that reboot and are cloned, which is the
// one that quarantines.
func Workloads() []Workload {
	var out []Workload
	for i, w := range []struct {
		name   string
		mod    func(*Config)
		policy lifecycle.Policy
	}{
		{"tiny", func(*Config) {}, lifecycle.Policy{}},
		{"late and confirmed", func(c *Config) {
			c.LateProbability, c.LateMax, c.ConfirmProbability = 0.4, 5*time.Minute, 0.6
		}, lifecycle.Policy{}},
		{"watch mode, seq across 2^32", func(c *Config) {
			c.HeartbeatInterval, c.FirstSeq = 0, 1<<32-200
		}, lifecycle.Policy{}},
		{"runs and lateness", func(c *Config) { c.Runs, c.LateProbability = true, 0.3 }, lifecycle.Policy{}},
		{"busy, seq near 2^63", func(c *Config) { c.ChurnPerMinute, c.FirstSeq = 120, 1<<63-100 }, lifecycle.Policy{}},
		{"reboots and clones", func(c *Config) {
			c.Runs, c.RebootProbability, c.CloneProbability = true, 0.2, 0.5
		}, QuarantinePolicy()},
	} {
		c := Tiny()
		c.Seed = uint64(100 + i)
		w.mod(&c)
		out = append(out, Workload{Name: w.name, Config: c, Policy: w.policy})
	}
	return out
}

// openStore opens a store in a directory the test owns and closes it afterwards.
// Closing twice is part of the contract, so a store that CheckClose closed must
// accept the second call.
func openStore(t *testing.T, f Factory, policy lifecycle.Policy) store.Store {
	t.Helper()
	s, err := f.Open(t.TempDir(), policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// Run checks a store against the reference (memstore) on every workload, with and
// without a mid-stream retention, runs the scripted and contract checks, the check
// that it comes back from being closed and reopened (for a durable store), and the
// check that reads running alongside writes see consistent states. It is the test a
// backend must pass.
//
// Run returns before the parallel subtests have run (Go starts a parallel subtest
// only after its parent function returns), so a caller must not defer teardown
// after it; use t.Cleanup.
func Run(t *testing.T, f Factory) {
	t.Helper()
	// The reopen and concurrent-read checks run first, one at a time, so the
	// concurrent-read check's writer and readers are not also competing with this
	// store's own heavy workloads (other tests in the process may still be running;
	// the check's acceptance rule does not depend on timing, only its running time
	// does). Go holds a parallel subtest until its parent returns, so the workloads
	// and scripted checks, which are independent of one another and each own a
	// store, then run side by side.
	t.Run("reopen", func(t *testing.T) {
		switch {
		case !f.Durable:
			t.Skip("the store is not durable: it does not come back from being closed")
		case Trimmed():
			t.Skip("trimmed run: the reopen check runs in the full tier")
		}
		if err := CheckReopen(f); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("concurrent reads", func(t *testing.T) {
		if f.NotConcurrent {
			t.Skip("the store is not safe for concurrent use")
		}
		if err := CheckConcurrentReads(openStore(t, f, lifecycle.Policy{})); err != nil {
			t.Fatal(err)
		}
	})

	sub := func(t *testing.T, name string, run func(*testing.T)) {
		t.Run(name, func(t *testing.T) {
			if !f.NotConcurrent {
				t.Parallel()
			}
			run(t)
		})
	}
	retains := [][]float64{nil, {0.5}, {0.3, 0.7}}
	if Trimmed() {
		retains = [][]float64{{0.3, 0.7}}
	}
	workloads := Workloads()
	for i, w := range workloads {
		if Trimmed() && i != len(workloads)-1 {
			continue // one workload stays, and it is the one that quarantines
		}
		for _, retain := range retains {
			sub(t, fmt.Sprintf("workload %s retain %v", w.Name, retain), func(t *testing.T) {
				if err := Check(openStore(t, f, w.Policy), w, Options{RetainAt: retain}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	for _, c := range []struct {
		name   string
		policy lifecycle.Policy
		check  func(store.Store) error
	}{
		{"write contract", lifecycle.Policy{}, CheckWriteContract},
		{"read contract", lifecycle.Policy{}, CheckReadContract},
		{"close", lifecycle.Policy{}, CheckClose},
		{"context", lifecycle.Policy{}, CheckContext},
		{"horizon", lifecycle.Policy{}, CheckHorizon},
		{"layer horizons", lifecycle.Policy{}, CheckLayerHorizons},
		{"instant", lifecycle.Policy{}, CheckInstant},
		{"producers", lifecycle.Policy{}, CheckProducers},
		{"relations", lifecycle.Policy{}, CheckRelations},
		{"extremes", lifecycle.Policy{}, CheckExtremes},
		{"entity window", lifecycle.Policy{}, CheckEntityWindow},
		{"quarantine", QuarantinePolicy(), CheckQuarantine},
	} {
		sub(t, c.name, func(t *testing.T) {
			if err := c.check(openStore(t, f, c.policy)); err != nil {
				t.Fatal(err)
			}
		})
	}
	sub(t, "random workloads", func(t *testing.T) {
		if Trimmed() {
			t.Skip("trimmed run: the random workloads run in the full tier")
		}
		rapid.Check(t, func(rt *rapid.T) {
			cfg := Tiny()
			cfg.Seed = rapid.Uint64Range(1, 1<<40).Draw(rt, "seed")
			cfg.Duration = time.Duration(rapid.IntRange(5, 15).Draw(rt, "minutes")) * time.Minute
			cfg.ChurnPerMinute = rapid.Float64Range(5, 120).Draw(rt, "churn")
			cfg.LateProbability = rapid.Float64Range(0, 0.5).Draw(rt, "late")
			cfg.ConfirmProbability = rapid.SampledFrom([]float64{0, 0.3, 0.8}).Draw(rt, "confirm")
			cfg.Runs = rapid.Bool().Draw(rt, "runs")
			cfg.HeartbeatInterval = rapid.SampledFrom([]time.Duration{0, 30 * time.Second, time.Minute}).Draw(rt, "heartbeat")
			cfg.FirstSeq = rapid.SampledFrom([]uint64{1, 1<<32 - 300, 1<<63 - 300}).Draw(rt, "firstSeq")
			policy := lifecycle.Policy{}
			if cfg.HeartbeatInterval > 0 && rapid.Bool().Draw(rt, "reboots") {
				cfg.RebootProbability = rapid.SampledFrom([]float64{0.1, 0.3}).Draw(rt, "reboot")
				cfg.CloneProbability = rapid.SampledFrom([]float64{0.3, 0.8}).Draw(rt, "clone")
				if rapid.Bool().Draw(rt, "quarantine") {
					policy = QuarantinePolicy()
				}
			}
			retain := rapid.SampledFrom([][]float64{nil, {0.3}, {0.6}, {0.25, 0.6}}).Draw(rt, "retain")

			// Each case gets, and removes, its own directory: cases are many, and a
			// disk-backed store is not small.
			dir, err := os.MkdirTemp("", "storetest")
			if err != nil {
				rt.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(dir) }()
			cand, err := f.Open(dir, policy)
			if err != nil {
				rt.Fatal(err)
			}
			defer func() { _ = cand.Close() }()
			w := Workload{Name: "random", Config: cfg, Policy: policy}
			if err := Check(cand, w, Options{RetainAt: retain, CheckEvery: 10}); err != nil {
				rt.Fatal(err)
			}
		})
	})
}
