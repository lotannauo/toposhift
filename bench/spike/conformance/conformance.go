// Package conformance checks a candidate engine against the oracle.
//
// Check feeds one generated workload to the candidate and to the oracle in
// the same random batches, and after chunks of the stream asks both the same
// questions, at instants chosen to hit the boundaries: exactly on a record's
// event time, one nanosecond either side, before everything and after
// everything. Any difference is an error naming the question and both
// answers. Mid-stream it also moves the retention horizon, after which it keeps
// offering records older than it and requires both engines to refuse them with
// [engine.ErrBeforeHorizon].
package conformance

import (
	"bytes"
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
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

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

	horizons := make([]time.Time, len(opts.RetainAt))
	for i, f := range opts.RetainAt {
		if f <= 0 || f >= 1 || (i > 0 && f <= opts.RetainAt[i-1]) {
			return fmt.Errorf("conformance: RetainAt must be ascending fractions in (0, 1), got %v", opts.RetainAt)
		}
		horizons[i] = g.Start().Add(time.Duration(f * float64(g.End().Sub(g.Start()))))
	}

	var written []engine.Record
	var horizon time.Time // the current retention horizon; zero before the first
	next := 0             // the next entry of horizons to apply
	batches := 0

	for {
		batch := g.Batch(1 + rng.IntN(opts.MaxBatch))
		if len(batch) == 0 {
			break
		}
		if !horizon.IsZero() {
			// Offer what is older than the horizon first. Both engines must
			// refuse it whole, and refuse it the same way.
			var stale []engine.Record
			for _, r := range batch {
				if r.EventTime.Before(horizon) {
					stale = append(stale, r)
				}
			}
			if len(stale) > 0 {
				for name, w := range map[string]func([]engine.Record) error{"candidate": cand.Write, "oracle": ora.Write} {
					own := cloneRecords(stale)
					if err := w(own); !errors.Is(err, engine.ErrBeforeHorizon) {
						return fmt.Errorf("%s Write of %d records before the horizon %s must fail with ErrBeforeHorizon, got %w",
							name, len(stale), horizon.Format(time.RFC3339), orNil(err))
					}
				}
				batch = slices.DeleteFunc(batch, func(r engine.Record) bool { return r.EventTime.Before(horizon) })
			}
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
		batches++

		for len(batch) > 0 && next < len(horizons) && !batch[len(batch)-1].EventTime.Before(horizons[next]) {
			horizon = horizons[next]
			next++
			if err := cand.Retain(horizon); err != nil {
				return fmt.Errorf("candidate Retain: %w", err)
			}
			if err := ora.Retain(horizon); err != nil {
				return err
			}
			if err := compare(cand, ora, entities, written, rng, opts, horizon); err != nil {
				return fmt.Errorf("after Retain(%s): %w", horizon.Format(time.RFC3339), err)
			}
		}
		if batches%opts.CheckEvery == 0 {
			if err := compare(cand, ora, entities, written, rng, opts, horizon); err != nil {
				return fmt.Errorf("after %d records: %w", len(written), err)
			}
		}
	}
	if err := compare(cand, ora, entities, written, rng, opts, horizon); err != nil {
		return fmt.Errorf("at the end (%d records): %w", len(written), err)
	}
	return nil
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

// probeTimes picks instants at or after the horizon: boundaries of real
// records, and the extremes.
func probeTimes(written []engine.Record, rng *rand.Rand, n int, horizon time.Time) []time.Time {
	floor := horizon
	if floor.IsZero() {
		floor = engine.MinEventTime
	}
	ts := []time.Time{floor}
	if len(written) == 0 {
		return ts
	}
	ts = append(ts, written[len(written)-1].EventTime.Add(24*time.Hour))
	// Bounded: instants before the horizon are skipped, and the loop must end
	// even if most records are older than it.
	for attempts := 0; len(ts) < n && attempts < 20*n; attempts++ {
		r := written[rng.IntN(len(written))]
		for _, t := range []time.Time{
			r.EventTime, r.EventTime.Add(-time.Nanosecond), r.EventTime.Add(r.TTL),
			r.EventTime.Add(time.Duration(rng.Int64N(int64(time.Hour)))),
		} {
			if !t.Before(floor) && len(ts) < n {
				ts = append(ts, t)
			}
		}
	}
	return ts
}

func compare(cand engine.Engine, ora *oracle.Oracle, entities []identity.Fingerprint, written []engine.Record, rng *rand.Rand, opts Options, horizon time.Time) error {
	if len(written) == 0 {
		return nil
	}
	// Mostly random entities, but always some that were just written to.
	sample := make([]identity.Fingerprint, 0, opts.Entities)
	for range opts.Entities / 2 {
		sample = append(sample, entities[rng.IntN(len(entities))])
	}
	// Both ends of recent edges, so reverse reads see the newest writes too.
	for len(sample) < opts.Entities {
		r := written[len(written)-1-rng.IntN(min(len(written), 50))]
		end := r.Subject.A
		if r.Subject.Kind == engine.SubjectEdge && rng.IntN(2) == 0 {
			end = r.Subject.B
		}
		sample = append(sample, end)
	}

	for _, fp := range sample {
		times := probeTimes(written, rng, opts.Probes, horizon)
		for _, t := range times {
			wantAlive, err := ora.Alive(fp, t)
			if err != nil {
				return err
			}
			gotAlive, err := cand.Alive(fp, t)
			if err != nil {
				return fmt.Errorf("Alive(%s, %s): %w", fp, offset(written, t), err)
			}
			if gotAlive != wantAlive {
				return fmt.Errorf("Alive(%s, %s) = %v; oracle says %v", fp, offset(written, t), gotAlive, wantAlive)
			}
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				want, err := ora.Neighbors(fp, dir, t)
				if err != nil {
					return err
				}
				got, err := cand.Neighbors(fp, dir, t)
				if err != nil {
					return fmt.Errorf("Neighbors(%s, %s, %s): %w", fp, dir, offset(written, t), err)
				}
				if !slices.Equal(got, want) {
					return fmt.Errorf("Neighbors(%s, %s, %s) = %v; oracle says %v", fp, dir, offset(written, t), got, want)
				}
			}
		}
		slices.SortFunc(times, time.Time.Compare)
		for i := 0; i+1 < len(times); i++ {
			from, to := times[i], times[i+1]
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				want, err := ora.Window(fp, dir, from, to)
				if err != nil {
					return err
				}
				got, err := cand.Window(fp, dir, from, to)
				if err != nil {
					return fmt.Errorf("Window(%s, %s): %w", fp, dir, err)
				}
				if msg := diffRecords(got, want); msg != "" {
					return fmt.Errorf("Window(%s, %s, [%s, %s)): %s", fp, dir, offset(written, from), offset(written, to), msg)
				}
			}
		}
	}
	return nil
}

func offset(written []engine.Record, t time.Time) string {
	return t.Sub(written[0].EventTime).String()
}

func diffRecords(got, want []engine.Record) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d records, oracle says %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Layer != w.Layer || g.Subject != w.Subject || g.Producer != w.Producer || !g.EventTime.Equal(w.EventTime) ||
			g.Seq != w.Seq || g.Kind != w.Kind || g.TTL != w.TTL || !g.Through.Equal(w.Through) || !bytes.Equal(g.Payload, w.Payload) {
			return fmt.Sprintf("record %d is %+v, oracle says %+v", i, g, w)
		}
	}
	return ""
}

// Configs returns the workloads Run uses: ordinary churn, heavy lateness, watch
// mode only, many outages, heavy skew, and coalesced runs with lateness.
func Configs() []workload.Config {
	base := workload.Tiny()
	var out []workload.Config
	for i, mod := range []func(*workload.Config){
		func(*workload.Config) {},
		func(c *workload.Config) { c.LateProbability, c.LateMeanDelay = 0.4, 5*time.Minute },
		func(c *workload.Config) { c.HeartbeatInterval, c.RollupInterval = 0, 0 },
		func(c *workload.Config) { c.OutageProbability, c.OutageLength = 0.1, 6*time.Minute },
		// A few pods take most of the churn, so a few edges have long histories.
		func(c *workload.Config) { c.EventsPerSecond, c.PodSkew = 3, 3 },
		func(c *workload.Config) {
			c.CoalesceRuns, c.LateProbability, c.OutageProbability = true, 0.3, 0.05
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
// mid-stream retention, and checks the write contract. It is the test a
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
	t.Run("write contract", func(t *testing.T) {
		if err := CheckWriteContract(open(t)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("random workloads", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			cfg := workload.Tiny()
			cfg.Seed = rapid.Uint64Range(1, 1<<40).Draw(rt, "seed")
			cfg.Duration = time.Duration(rapid.IntRange(2, 12).Draw(rt, "minutes")) * time.Minute
			cfg.LateProbability = rapid.Float64Range(0, 0.5).Draw(rt, "late")
			cfg.EventsPerSecond = rapid.Float64Range(0.2, 4).Draw(rt, "rate")
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

// CheckWriteContract checks that an engine refuses records out of sequence and
// records its validation rules say are invalid, that a refused batch is
// refused whole (its valid records are not stored and their sequence numbers
// are not consumed), and that the retention horizon only moves forward. It
// leaves the engine with a retention horizon, so use a fresh engine.
func CheckWriteContract(cand engine.Engine) error {
	g, err := workload.New(workload.Tiny())
	if err != nil {
		return err
	}
	recs := g.Batch(12)
	if err := cand.Write(cloneRecords(recs[:10])); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	if err := cand.Write(cloneRecords(recs[:1])); !errors.Is(err, engine.ErrInvalid) {
		return fmt.Errorf("a repeated seq must be refused with ErrInvalid, got %w", orNil(err))
	}

	good := recs[10]
	broken := map[string]func(engine.Record) engine.Record{
		"an event time before 1970": func(r engine.Record) engine.Record {
			r.EventTime = time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC)
			return r
		},
		"an empty producer": func(r engine.Record) engine.Record { r.Producer = ""; return r },
		"an unset layer":    func(r engine.Record) engine.Record { r.Layer = 0; return r },
		"an unset kind":     func(r engine.Record) engine.Record { r.Kind = 0; return r },
		"a delete with a TTL": func(r engine.Record) engine.Record {
			r.Kind, r.Payload, r.TTL = lifecycle.Delete, nil, time.Minute
			return r
		},
	}
	names := make([]string, 0, len(broken))
	for name := range broken {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		bad := broken[name](recs[11])
		// A valid record followed by an invalid one: the whole batch is refused.
		if err := cand.Write(cloneRecords([]engine.Record{good, bad})); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("a batch with %s must be refused with ErrInvalid, got %w", name, orNil(err))
		}
	}
	// If any refused batch had stored its valid record, that record's sequence
	// number would now be used and this would fail.
	if err := cand.Write(cloneRecords([]engine.Record{good})); err != nil {
		return fmt.Errorf("a refused batch left its valid record behind: %w", err)
	}

	// The horizon only moves forward: a later, earlier-dated Retain must not
	// bring back the window between the two.
	start := recs[0].EventTime
	if err := cand.Retain(start.Add(2 * time.Hour)); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if err := cand.Retain(start.Add(time.Hour)); err != nil {
		return fmt.Errorf("an earlier Retain must be accepted and ignored: %w", err)
	}
	between := recs[11]
	between.Seq, between.EventTime = recs[11].Seq+1000, start.Add(90*time.Minute)
	if err := cand.Write(cloneRecords([]engine.Record{between})); !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("after Retain(+2h) then Retain(+1h), a record at +90m must still be refused with ErrBeforeHorizon, got %w", orNil(err))
	}
	return nil
}

// orNil makes "accepted" print as an error value.
func orNil(err error) error {
	if err == nil {
		return errors.New("no error")
	}
	return err
}
