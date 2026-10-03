// Package conformance checks a candidate engine against the oracle.
//
// Check feeds one generated workload to the candidate and to the oracle in
// the same random batches, and after chunks of the stream asks both the same
// questions: in every layer an entity is in and one it is not, as of the latest
// token and as of earlier ones, at instants chosen to hit the boundaries (exactly
// on a record's event time, one nanosecond either side, before everything and
// after everything), including windows whose edges sit on a record's instant. Any
// difference is an error naming the question and both answers. Mid-stream it
// also moves the retention horizon, after which it keeps offering records older
// than it and requires both engines to refuse them with [engine.ErrBeforeHorizon],
// and from then on asks only about instants at or after the horizon and tokens at
// or above the sequence the engine had reached.
//
// CheckInstant, CheckProducers, CheckExtremes, CheckReadContract and
// CheckWriteContract are scripted: each runs a fixed scenario on a fresh engine.
package conformance

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// ErrMismatch is wrapped by every error that reports a read on which the
// candidate and the oracle disagree, so a caller can tell a wrong answer from a
// failed call or a refused write.
var ErrMismatch = errors.New("candidate and oracle disagree")

// Factory opens a candidate that keeps whatever it stores under dir, a
// directory the harness owns and removes afterwards. The directory is empty (a
// new, empty candidate) or holds what the same candidate left before it was
// closed, in which case the candidate must come back exactly as it was: its
// records, its token and its retention horizon. A candidate with no files, a
// test double, ignores dir and cannot be reopened (see [RunSerial]).
type Factory func(dir string) (engine.Engine, error)

// Options tunes Check.
type Options struct {
	// MaxBatch is the largest write batch; batches are random in [1, MaxBatch].
	MaxBatch int
	// CheckEvery is how many batches pass between rounds of comparisons.
	CheckEvery int
	// Entities is how many entities are queried each round.
	Entities int
	// Probes is how many instants are asked about per entity each round.
	Probes int
	// RetainAt lists the fractions of the simulated period, ascending, at which
	// the retention horizon is moved mid-stream. Moving it twice checks that a
	// second retention works on what the first left behind. Empty disables it.
	RetainAt []float64
	// ReopenAt lists the fractions of the simulated period, ascending, at which
	// the candidate is closed and opened again with Reopen, and then must have
	// every answer, its token and its horizon as it was. Empty disables it.
	ReopenAt []float64
	// Reopen closes old, which the caller no longer owns afterwards, and returns
	// the candidate opened over what it left. Required with ReopenAt.
	Reopen func(old engine.Engine) (engine.Engine, error)
}

func (o Options) withDefaults() Options {
	if o.MaxBatch == 0 {
		o.MaxBatch = 64
	}
	if o.CheckEvery == 0 {
		o.CheckEvery = 25
	}
	if o.Entities == 0 {
		o.Entities = 12
	}
	if o.Probes == 0 {
		o.Probes = 6
	}
	return o
}

// Check runs one workload through the candidate and the oracle and returns the
// first disagreement, or nil. The candidate is not closed, unless ReopenAt is
// set, when the candidate the stream ends with is the one Reopen returned last
// and the caller must close that one.
func Check(cand engine.Engine, cfg workload.Config, opts Options) error {
	opts = opts.withDefaults()
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	ora := oracle.New()
	entities := g.Entities()
	rng := rand.New(rand.NewPCG(cfg.Seed+1, 0x5eed))
	stranger, err := strangerEntity()
	if err != nil {
		return err
	}

	at := func(name string, fractions []float64) ([]time.Time, error) {
		out := make([]time.Time, len(fractions))
		for i, f := range fractions {
			if f <= 0 || f >= 1 || (i > 0 && f <= fractions[i-1]) {
				return nil, fmt.Errorf("conformance: %s must be ascending fractions in (0, 1), got %v", name, fractions)
			}
			out[i] = g.Start().Add(time.Duration(f * float64(g.End().Sub(g.Start()))))
		}
		return out, nil
	}
	horizons, err := at("RetainAt", opts.RetainAt)
	if err != nil {
		return err
	}
	reopens, err := at("ReopenAt", opts.ReopenAt)
	if err != nil {
		return err
	}
	if len(reopens) > 0 && opts.Reopen == nil {
		return errors.New("conformance: ReopenAt needs Reopen")
	}

	var written []engine.Record
	var horizon time.Time // the current retention horizon; zero before the first
	var tokenFloor uint64 // the sequence the engine had reached at the last retention
	next := 0             // the next entry of horizons to apply
	nextReopen := 0       // the next entry of reopens to apply
	batches := 0
	state := func() probeState {
		if cfg.FreshIdentities {
			// Pods keep appearing: probe the ones that have long been gone too.
			entities = g.Entities()
		}
		return probeState{entities: entities, stranger: stranger, written: written, horizon: horizon, tokenFloor: tokenFloor}
	}

	for {
		batch := g.Batch(1 + rng.IntN(opts.MaxBatch))
		if len(batch) == 0 {
			break
		}
		if !horizon.IsZero() && slices.ContainsFunc(batch, func(r engine.Record) bool { return r.EventTime.Before(horizon) }) {
			// Offer the batch as it is, stale records among valid ones: both
			// engines must refuse it whole, refuse it the same way, and leave
			// the token where it was.
			before := ora.LastSeq()
			for name, w := range map[string]func([]engine.Record) error{"candidate": cand.Write, "oracle": ora.Write} {
				if err := w(cloneRecords(batch)); !errors.Is(err, engine.ErrBeforeHorizon) {
					return fmt.Errorf("%s Write of a batch with records before the horizon %s must fail with ErrBeforeHorizon, got %w",
						name, horizon.Format(time.RFC3339), orNil(err))
				}
			}
			if got, want := cand.LastSeq(), ora.LastSeq(); got != want || want != before {
				return fmt.Errorf("a refused batch moved LastSeq: candidate %d, oracle %d, before %d", got, want, before)
			}
			batch = slices.DeleteFunc(batch, func(r engine.Record) bool { return r.EventTime.Before(horizon) })
		}
		if len(batch) > 0 {
			// The candidate gets its own copy, payloads included, so an engine
			// that keeps or alters what it was given cannot change the oracle's.
			if err := cand.Write(cloneRecords(batch)); err != nil {
				return fmt.Errorf("candidate Write: %w", err)
			}
			if err := ora.Write(batch); err != nil {
				return fmt.Errorf("oracle Write: %w", err)
			}
			written = append(written, batch...)
		}
		// Refused batches must leave the token alone too, so this runs after
		// every round, not only after accepted ones.
		if got, want := cand.LastSeq(), ora.LastSeq(); got != want {
			return fmt.Errorf("LastSeq = %d after %d records; oracle says %d", got, len(written), want)
		}
		batches++

		for len(batch) > 0 && next < len(horizons) && !batch[len(batch)-1].EventTime.Before(horizons[next]) {
			horizon = horizons[next]
			next++
			tokenFloor = ora.LastSeq()
			g.SetHorizon(horizon)
			if err := cand.Retain(horizon); err != nil {
				return fmt.Errorf("candidate Retain: %w", err)
			}
			if err := ora.Retain(horizon); err != nil {
				return err
			}
			if got, want := cand.LastSeq(), ora.LastSeq(); got != want {
				return fmt.Errorf("retaining moved LastSeq: candidate %d, oracle %d", got, want)
			}
			if err := compare(cand, ora, rng, opts, state()); err != nil {
				return fmt.Errorf("after Retain(%s): %w", horizon.Format(time.RFC3339), err)
			}
		}
		for len(batch) > 0 && nextReopen < len(reopens) && !batch[len(batch)-1].EventTime.Before(reopens[nextReopen]) {
			nextReopen++
			if cand, err = reopen(cand, opts, written, horizon); err != nil {
				return fmt.Errorf("after %d records: %w", len(written), err)
			}
			if err := compare(cand, ora, rng, opts, state()); err != nil {
				return fmt.Errorf("after reopening at %d records: %w", len(written), err)
			}
		}
		if batches%opts.CheckEvery == 0 {
			if err := compare(cand, ora, rng, opts, state()); err != nil {
				return fmt.Errorf("after %d records: %w", len(written), err)
			}
		}
	}
	if err := compare(cand, ora, rng, opts, state()); err != nil {
		return fmt.Errorf("at the end (%d records): %w", len(written), err)
	}
	return nil
}

// strangerEntity is an entity nothing is ever written about, to put in batched
// reads.
func strangerEntity() (identity.Fingerprint, error) {
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.Host, []identity.Attr{{Key: catalog.HostID, Value: "never-written"}})
	if err != nil {
		return identity.Fingerprint{}, err
	}
	return id.Fingerprint(), nil
}

// cloneRecords copies records, payloads included, so a candidate that keeps or
// alters what it is given cannot change the oracle's copy.
func cloneRecords(rs []engine.Record) []engine.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// Configs returns the workloads Run uses: ordinary churn with a second producer
// confirming placements, heavy lateness, watch mode only with sequence numbers
// that cross 2^32, many outages, heavy skew with sequence numbers near 2^63,
// coalesced runs with lateness, and fresh pod identities with pod heartbeats and
// a backlog.
func Configs() []workload.Config {
	base := workload.Tiny()
	var out []workload.Config
	for i, mod := range []func(*workload.Config){
		func(c *workload.Config) { c.ConfirmProbability, c.ConfirmTTL = 0.5, 3*time.Minute },
		func(c *workload.Config) {
			c.LateProbability, c.LateMeanDelay = 0.4, 5*time.Minute
			c.ConfirmProbability, c.ConfirmTTL = 0.3, 2*time.Minute
		},
		func(c *workload.Config) {
			c.HeartbeatInterval, c.RollupInterval = 0, 0
			c.FirstSeq = 1<<32 - 200
		},
		func(c *workload.Config) {
			c.OutageProbability, c.OutageLength = 0.1, 6*time.Minute
			c.ConfirmProbability, c.ConfirmTTL = 0.5, 5*time.Minute
		},
		// A few pods take most of the churn, so a few edges have long histories.
		func(c *workload.Config) {
			c.EventsPerSecond, c.PodSkew = 3, 3
			c.FirstSeq = 1<<63 - 100
		},
		func(c *workload.Config) {
			c.CoalesceRuns, c.LateProbability, c.OutageProbability = true, 0.3, 0.05
			c.ConfirmProbability, c.ConfirmTTL = 0.4, 4*time.Minute
		},
		// Every pod is a new identity, nodes have a capacity, a cluster-level
		// collector refreshes every placement, a producer's pipeline backs up in
		// order, and refreshes are coalesced and absorbed: the shapes of real churn
		// that the others do not have. The trimmed run keeps this one.
		func(c *workload.Config) {
			c.FreshIdentities, c.MaxPodsPerNode = true, 8
			c.PodHeartbeatInterval, c.HeartbeatTTLFactor = 2*time.Minute, 3
			c.LateProbability = 0
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = 5*time.Minute, time.Minute, 2*time.Minute
			c.CoalesceRuns, c.ExtendEvery = true, 90*time.Second
			c.ConfirmProbability, c.ConfirmTTL = 0.4, 4*time.Minute
		},
	} {
		c := base
		c.Seed = uint64(100 + i)
		mod(&c)
		out = append(out, c)
	}
	return out
}

// RapidChecks is how many random workloads a package's TestMain should ask rapid
// for: the environment variable TOPOSHIFT_RAPID_CHECKS if it is set, otherwise
// def. A -rapid.checks on the command line still wins, because it is parsed
// after TestMain. The fast test tier sets the variable higher than the defaults,
// which were chosen for the race detector.
func RapidChecks(def string) string {
	if v := os.Getenv("TOPOSHIFT_RAPID_CHECKS"); v != "" {
		return v
	}
	return def
}

// Trimmed reports whether the heavy part of the conformance test is left out, as
// it is under `go test -short`. The full tier (Pebble's invariant checks on, every
// workload and retention schedule, the random workloads, the reopen check) is what
// decides whether a candidate conforms; the trimmed tier exists so the same code
// can also run under the race detector, which makes each workload cost seconds,
// with one workload (the last config) and the concurrency checks,
// which are the only code here that starts goroutines. It is the one place that
// decides what a short run drops.
func Trimmed() bool { return testing.Short() }

// SkipWhenTrimmed skips a test that starts no goroutine of its own and is heavy
// under the race detector, in a trimmed run. A test that does start goroutines
// must not call it: those are what the race detector is for.
func SkipWhenTrimmed(t testing.TB) {
	t.Helper()
	if Trimmed() {
		t.Skip("trimmed run (-short): a single-goroutine test, it runs in the full tier")
	}
}

// Run checks a candidate against the oracle on every config, with and without a
// mid-stream retention, and runs the scripted checks, the check that it comes
// back from being closed and reopened, and the check that reads running
// alongside writes see consistent states. It is the test a candidate layout must
// pass. [RunSerial] is the same without the last two, for an engine that has no
// files or is not safe for concurrent use: a test double.
//
// Run returns before the parallel subtests have run (Go starts a parallel
// subtest only after its parent function returns), so a caller must not defer
// teardown after it; use t.Cleanup.
func Run(t *testing.T, newEngine Factory) {
	t.Helper()
	// The reopen and concurrent-read checks run first, one at a time, so the
	// concurrent-read check's writer and readers are not also competing with this
	// candidate's own heavy workloads (other tests in the process may still be
	// running; the check's acceptance rule does not depend on timing, only its
	// running time does). Go holds a parallel subtest until its parent returns, so
	// the workloads and scripted checks, which are independent of one another and
	// each own an engine, then run side by side.
	t.Run("reopen", func(t *testing.T) {
		if Trimmed() {
			t.Skip("trimmed run: the reopen check runs in the full tier")
		}
		if err := CheckReopen(newEngine); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("concurrent reads", func(t *testing.T) {
		if err := CheckConcurrentReads(open(t, newEngine)); err != nil {
			t.Fatal(err)
		}
	})
	runChecks(t, newEngine, true)
}

// RunSerial is [Run] without the reopen check and the concurrent-read check.
func RunSerial(t *testing.T, newEngine Factory) {
	t.Helper()
	runChecks(t, newEngine, false)
}

// open builds a candidate in a directory the test owns and closes it afterwards.
func open(t *testing.T, newEngine Factory) engine.Engine {
	t.Helper()
	e, err := newEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func runChecks(t *testing.T, newEngine Factory, parallel bool) {
	t.Helper()
	// Every subtest opens its own engine from the factory, so with parallel set
	// they may run side by side. RunSerial leaves them serial, for a factory whose
	// engines share state.
	sub := func(t *testing.T, name string, f func(*testing.T)) {
		t.Run(name, func(t *testing.T) {
			if parallel {
				t.Parallel()
			}
			f(t)
		})
	}
	retains := [][]float64{nil, {0.5}, {0.3, 0.7}}
	if Trimmed() {
		retains = [][]float64{{0.3, 0.7}}
	}
	configs := Configs()
	for i, cfg := range configs {
		if Trimmed() && i != len(configs)-1 {
			continue // one workload (the last, with fresh identities and a backlog) stays, as a smoke test
		}
		for _, retain := range retains {
			sub(t, fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				if err := Check(open(t, newEngine), cfg, Options{RetainAt: retain}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	for name, check := range map[string]func(engine.Engine) error{
		"write contract": CheckWriteContract,
		"read contract":  CheckReadContract,
		"instant":        CheckInstant,
		"producers":      CheckProducers,
		"relations":      CheckRelations,
		"extremes":       CheckExtremes,
	} {
		sub(t, name, func(t *testing.T) {
			if err := check(open(t, newEngine)); err != nil {
				t.Fatal(err)
			}
		})
	}
	sub(t, "random workloads", func(t *testing.T) {
		if Trimmed() {
			t.Skip("trimmed run: the random workloads run in the full tier")
		}
		rapid.Check(t, func(rt *rapid.T) {
			cfg := workload.Tiny()
			cfg.Seed = rapid.Uint64Range(1, 1<<40).Draw(rt, "seed")
			cfg.Duration = time.Duration(rapid.IntRange(2, 12).Draw(rt, "minutes")) * time.Minute
			cfg.LateProbability = rapid.Float64Range(0, 0.5).Draw(rt, "late")
			cfg.EventsPerSecond = rapid.Float64Range(0.2, 4).Draw(rt, "rate")
			cfg.ConfirmProbability = rapid.SampledFrom([]float64{0, 0.3, 0.8}).Draw(rt, "confirm")
			cfg.ConfirmTTL = time.Duration(rapid.IntRange(1, 6).Draw(rt, "confirmMinutes")) * time.Minute
			cfg.FirstSeq = rapid.SampledFrom([]uint64{1, 1<<32 - 300, 1<<63 - 300}).Draw(rt, "firstSeq")
			retain := rapid.SampledFrom([][]float64{nil, {0.3}, {0.6}, {0.25, 0.6}}).Draw(rt, "retain")

			// The shapes of real churn, sometimes: fresh identities, a node capacity,
			// pod heartbeats, a producer's backlog instead of independent lateness,
			// and coalescing bounded by each run's TTL.
			cfg.FreshIdentities = rapid.Bool().Draw(rt, "fresh")
			cfg.MaxPodsPerNode = rapid.SampledFrom([]int{0, 0, 8, 12}).Draw(rt, "capacity")
			cfg.PodHeartbeatInterval = rapid.SampledFrom([]time.Duration{0, 0, time.Minute, 3 * time.Minute}).Draw(rt, "podHeartbeat")
			if rapid.Bool().Draw(rt, "backlog") {
				cfg.LateProbability = 0
				cfg.BacklogEvery = time.Duration(rapid.IntRange(2, 8).Draw(rt, "backlogEvery")) * time.Minute
				cfg.BacklogMeanDelay = time.Duration(rapid.IntRange(1, 90).Draw(rt, "backlogDelay")) * time.Second
				cfg.BacklogSpan = time.Duration(rapid.IntRange(1, 5).Draw(rt, "backlogSpan")) * time.Minute
			}
			if rapid.Bool().Draw(rt, "coalesce") {
				cfg.CoalesceRuns = true
				cfg.ExtendTTLFraction = rapid.SampledFrom([]float64{0, 0.5, 1}).Draw(rt, "extendFraction")
			}

			// Each iteration gets, and removes, its own directory: iterations
			// are many, and a disk-backed candidate is not small.
			dir, err := os.MkdirTemp("", "conformance")
			if err != nil {
				rt.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(dir) }()
			cand, err := newEngine(dir)
			if err != nil {
				rt.Fatal(err)
			}
			defer func() { _ = cand.Close() }()
			if err := Check(cand, cfg, Options{RetainAt: retain, CheckEvery: 10}); err != nil {
				rt.Fatal(err)
			}
		})
	})
}

// reopen closes the candidate and opens it again, and checks what a reopening
// must not lose: the token, the rule that Seq only rises, the retention
// horizon, and that an earlier Retain still changes nothing.
func reopen(cand engine.Engine, opts Options, written []engine.Record, horizon time.Time) (engine.Engine, error) {
	before := cand.LastSeq()
	next, err := opts.Reopen(cand)
	if err != nil {
		return nil, fmt.Errorf("reopening: %w", err)
	}
	if got := next.LastSeq(); got != before {
		return next, fmt.Errorf("LastSeq = %d after reopening; it was %d", got, before)
	}
	if len(written) == 0 {
		return next, nil
	}
	last := written[len(written)-1]
	if err := next.Write(cloneRecords([]engine.Record{last})); !errors.Is(err, engine.ErrInvalid) {
		return next, fmt.Errorf("after reopening, a record whose Seq %d was already written must be refused with ErrInvalid, got %w", last.Seq, orNil(err))
	}
	if !horizon.IsZero() {
		if err := next.Retain(horizon.Add(-time.Hour)); err != nil {
			return next, fmt.Errorf("after reopening, an earlier Retain must be accepted and ignored: %w", err)
		}
		stale := last
		stale.Seq, stale.EventTime = before+1, horizon.Add(-time.Second)
		if err := next.Write(cloneRecords([]engine.Record{stale})); !errors.Is(err, engine.ErrBeforeHorizon) {
			return next, fmt.Errorf("after reopening, a record before the horizon %s must be refused with ErrBeforeHorizon, got %w", horizon.Format(time.RFC3339), orNil(err))
		}
	}
	if got := next.LastSeq(); got != before {
		return next, fmt.Errorf("a refused record moved LastSeq to %d after reopening; it was %d", got, before)
	}
	return next, nil
}

// CheckReopen runs workloads through a candidate that is closed and opened
// again three times along the way, with a retention in between, comparing every
// answer to the oracle's after each reopening. It is for a candidate that
// persists; use it on a Factory that opens the same directory again.
func CheckReopen(newEngine Factory) error {
	configs := Configs()
	for _, i := range []int{0, 2, 5} {
		if err := checkReopen(newEngine, configs[i]); err != nil {
			return fmt.Errorf("config %d: %w", i, err)
		}
	}
	return nil
}

func checkReopen(newEngine Factory, cfg workload.Config) error {
	dir, err := os.MkdirTemp("", "conformance-reopen")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cand, err := newEngine(dir)
	if err != nil {
		return err
	}
	// Whatever candidate is current at the end, or when something fails, is
	// closed here; Reopen closes the one it replaces.
	current := &cand
	defer func() { _ = (*current).Close() }()
	return Check(cand, cfg, Options{
		RetainAt: []float64{0.45, 0.8}, ReopenAt: []float64{0.2, 0.5, 0.85}, CheckEvery: 15,
		Reopen: func(old engine.Engine) (engine.Engine, error) {
			if err := old.Close(); err != nil {
				return nil, err
			}
			next, err := newEngine(dir)
			if err != nil {
				return nil, err
			}
			*current = next
			return next, nil
		},
	})
}
