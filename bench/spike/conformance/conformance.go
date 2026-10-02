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

// Factory builds a fresh, empty candidate that keeps whatever it stores under
// dir, an empty directory the harness owns and removes afterwards. A candidate
// with no files ignores it.
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
// first disagreement, or nil. The candidate is not closed.
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

	horizons := make([]time.Time, len(opts.RetainAt))
	for i, f := range opts.RetainAt {
		if f <= 0 || f >= 1 || (i > 0 && f <= opts.RetainAt[i-1]) {
			return fmt.Errorf("conformance: RetainAt must be ascending fractions in (0, 1), got %v", opts.RetainAt)
		}
		horizons[i] = g.Start().Add(time.Duration(f * float64(g.End().Sub(g.Start()))))
	}

	var written []engine.Record
	var horizon time.Time // the current retention horizon; zero before the first
	var tokenFloor uint64 // the sequence the engine had reached at the last retention
	next := 0             // the next entry of horizons to apply
	batches := 0
	state := func() probeState {
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
// that cross 2^32, many outages, heavy skew with sequence numbers near 2^63, and
// coalesced runs with lateness.
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
	} {
		c := base
		c.Seed = uint64(100 + i)
		mod(&c)
		out = append(out, c)
	}
	return out
}

// Run checks a candidate against the oracle on every config, with and without a
// mid-stream retention, and runs the scripted checks. It is the test a
// candidate layout must pass.
func Run(t *testing.T, newEngine Factory) {
	t.Helper()
	open := func(t *testing.T) engine.Engine {
		t.Helper()
		e, err := newEngine(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		return e
	}
	for i, cfg := range Configs() {
		for _, retain := range [][]float64{nil, {0.5}, {0.3, 0.7}} {
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				if err := Check(open(t), cfg, Options{RetainAt: retain}); err != nil {
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
		"extremes":       CheckExtremes,
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(open(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("random workloads", func(t *testing.T) {
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
