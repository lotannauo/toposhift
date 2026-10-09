package storetest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Options tunes [Check]. Zero fields take the defaults in brackets.
type Options struct {
	MaxBatch   int // largest write batch; batches are random in [1, MaxBatch] [64]
	CheckEvery int // batches between rounds of comparisons [25]
	Entities   int // entities asked about per round [12]
	Probes     int // instants per entity per round [6]
	// RetainAt lists ascending fractions in (0, 1) of the period at which the
	// horizon is moved. Moving it twice checks that a second retention works on
	// what the first left behind. ReopenAt lists ascending fractions at which the
	// store is closed and opened again with Reopen, which is required with it, and
	// then must have every answer, its token and its horizon as it was.
	RetainAt, ReopenAt []float64
	// Reopen closes old, which the caller no longer owns afterwards, and returns
	// the store opened over what it left.
	Reopen func(old store.Store) (store.Store, error)
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

// outcome names how a Write ended, for comparing two stores: it was accepted, or
// refused for the horizon, or refused as invalid, or failed some other way.
func outcome(err error) string {
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, store.ErrBeforeHorizon):
		return "refused for being before the horizon"
	case errors.Is(err, store.ErrInvalid):
		return "refused as invalid"
	}
	return "failed"
}

// Check runs one workload through the store under test and the reference
// (memstore) and returns the first disagreement, or nil. It feeds both the same
// random batches, and after chunks of the stream asks both the same questions: in
// every layer an entity is in and one it is not, as of the latest token and as of
// earlier ones, at instants chosen to hit the boundaries (exactly on a record's
// event time, one nanosecond either side, before everything and after everything),
// including windows whose edges sit on a record's instant. Any difference is an
// error naming the question and both answers. Mid-stream it also moves the
// retention horizon, after which it keeps offering records older than it and
// requires both stores to refuse them with [store.ErrBeforeHorizon], asks every
// kind of read one nanosecond and one token below the horizon and requires both to
// refuse it, and from then on asks about answers only at or after the horizon and
// at tokens at or above its Seq.
//
// cand must have been opened with w.Policy. The store is not closed, unless
// ReopenAt is set: the store the stream ends with is then the one Reopen returned
// last, and the caller must close that one.
func Check(cand store.Store, w Workload, opts Options) error {
	opts = opts.withDefaults()
	g, err := NewGenerator(w.Config)
	if err != nil {
		return err
	}
	ref, err := memstore.Open(memstore.Options{Policy: w.Policy})
	if err != nil {
		return err
	}
	defer func() { _ = ref.Close() }()
	entities := g.Entities()
	rng := rand.New(rand.NewPCG(w.Config.Seed+1, 0x5eed))
	stranger, err := strangerEntity()
	if err != nil {
		return err
	}

	at := func(name string, fractions []float64) ([]time.Time, error) {
		out := make([]time.Time, len(fractions))
		for i, f := range fractions {
			if f <= 0 || f >= 1 || (i > 0 && f <= fractions[i-1]) {
				return nil, fmt.Errorf("storetest: %s must be ascending fractions in (0, 1), got %v", name, fractions)
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
		return errors.New("storetest: ReopenAt needs Reopen")
	}

	var written []store.Record
	var horizon store.Horizon // the current retention horizon; zero before the first
	next := 0                 // the next entry of horizons to apply
	nextReopen := 0           // the next entry of reopens to apply
	batches := 0
	state := func() probeState {
		return probeState{
			entities: entities, stranger: stranger, written: written, horizon: horizon, bootPolicy: w.Policy.BootKey != "",
		}
	}

	for {
		batch := g.Batch(1 + rng.IntN(opts.MaxBatch))
		if len(batch) == 0 {
			break
		}
		if !horizon.IsZero() && slices.ContainsFunc(batch, func(r store.Record) bool { return r.EventTime.Before(horizon.Time) }) {
			// Offer the batch as it is, stale records among valid ones: both stores
			// must refuse it whole, refuse it the same way, and leave the token
			// where it was.
			before := ref.LastSeq()
			for name, s := range map[string]store.Store{"store": cand, "reference": ref} {
				if err := s.Write(bg, cloneRecords(batch)); !errors.Is(err, store.ErrBeforeHorizon) {
					return fmt.Errorf("%s Write of a batch with records before the horizon %s must fail with ErrBeforeHorizon, got %w",
						name, horizon.Time.Format(time.RFC3339), orNil(err))
				}
			}
			if got, want := cand.LastSeq(), ref.LastSeq(); got != want || want != before {
				return fmt.Errorf("a refused batch moved LastSeq: store %d, reference %d, before %d", got, want, before)
			}
			batch = slices.DeleteFunc(batch, func(r store.Record) bool { return r.EventTime.Before(horizon.Time) })
		}
		if len(batch) > 0 {
			if err := writeBoth(cand, ref, batch); err != nil {
				return err
			}
			written = append(written, batch...)
		}
		// Refused batches must leave the token alone too, so this runs after
		// every round, not only after accepted ones.
		if got, want := cand.LastSeq(), ref.LastSeq(); got != want {
			return fmt.Errorf("LastSeq = %d after %d records; reference says %d", got, len(written), want)
		}
		if got, want := cand.Horizon(), ref.Horizon(); !sameHorizon(got, want) {
			return fmt.Errorf("Horizon() = %v after %d records; reference says %v", got, len(written), want)
		}
		batches++

		for len(batch) > 0 && next < len(horizons) && !batch[len(batch)-1].EventTime.Before(horizons[next]) {
			h := horizons[next]
			next++
			g.SetHorizon(h)
			want := store.Horizon{Time: h.UTC(), Seq: ref.LastSeq()}
			if err := cand.Retain(bg, h); err != nil {
				return fmt.Errorf("Retain(%s): %w", h.Format(time.RFC3339), err)
			}
			if err := ref.Retain(bg, h); err != nil {
				return err
			}
			if got := ref.Horizon(); !sameHorizon(got, want) {
				return fmt.Errorf("reference Horizon() = %v after Retain(%s); want %v", got, h.Format(time.RFC3339), want)
			}
			if got := cand.Horizon(); !sameHorizon(got, want) {
				return fmt.Errorf("Horizon() = %v after Retain(%s); want {%s, the LastSeq at the call, %d}",
					got, h.Format(time.RFC3339), h.UTC().Format(time.RFC3339), want.Seq)
			}
			horizon = want
			if err := compareLayerHorizons(cand, ref); err != nil {
				return fmt.Errorf("after Retain(%s): %w", h.Format(time.RFC3339), err)
			}
			if got, want := cand.LastSeq(), ref.LastSeq(); got != want {
				return fmt.Errorf("retaining moved LastSeq: store %d, reference %d", got, want)
			}
			if err := compare(cand, ref, rng, opts, state()); err != nil {
				return fmt.Errorf("after Retain(%s): %w", h.Format(time.RFC3339), err)
			}
		}
		for len(batch) > 0 && nextReopen < len(reopens) && !batch[len(batch)-1].EventTime.Before(reopens[nextReopen]) {
			nextReopen++
			if cand, err = reopen(cand, opts, written, horizon); err != nil {
				return fmt.Errorf("after %d records: %w", len(written), err)
			}
			if err := compareLayerHorizons(cand, ref); err != nil {
				return fmt.Errorf("after reopening at %d records: %w", len(written), err)
			}
			if err := compare(cand, ref, rng, opts, state()); err != nil {
				return fmt.Errorf("after reopening at %d records: %w", len(written), err)
			}
		}
		if batches%opts.CheckEvery == 0 {
			if err := compare(cand, ref, rng, opts, state()); err != nil {
				return fmt.Errorf("after %d records: %w", len(written), err)
			}
		}
	}
	if err := compare(cand, ref, rng, opts, state()); err != nil {
		return fmt.Errorf("at the end (%d records): %w", len(written), err)
	}
	return nil
}

// compareLayerHorizons requires the store under test to report the reference's
// horizon for each of the four layers, and the zero Horizon, as the reference
// does, for layers outside L0 to L3.
func compareLayerHorizons(cand, ref store.Store) error {
	for _, l := range append(slices.Clone(allLayers), outsideLayers...) {
		if got, want := cand.LayerHorizon(l), ref.LayerHorizon(l); !sameHorizon(got, want) {
			return fmt.Errorf("LayerHorizon(%s) = %v; reference says %v", l, got, want)
		}
	}
	return nil
}

func sameHorizon(a, b store.Horizon) bool { return a.Time.Equal(b.Time) && a.Seq == b.Seq }

// writeBoth gives a batch to the store under test and to the reference. The
// store gets its own copy, payloads included, and once it has accepted it the
// copy's payload bytes are overwritten, so a store that keeps the caller's slices
// is found by the next read. The two must end the same way, and agree on LastSeq.
func writeBoth(cand store.Store, ref *memstore.Store, batch []store.Record) error {
	refErr := ref.Write(bg, cloneRecords(batch))
	given := cloneRecords(batch)
	candErr := cand.Write(bg, given)
	if candErr == nil {
		scribble(given)
	}
	first, last := batch[0].Seq, batch[len(batch)-1].Seq
	switch {
	case refErr != nil && candErr != nil && outcome(refErr) == outcome(candErr):
		return fmt.Errorf("both stores refused the batch of seq %d to %d: the store with %w; the reference with %w", first, last, candErr, refErr)
	case refErr != nil:
		return fmt.Errorf("the reference refused the batch of seq %d to %d (%w), which the store under test %s", first, last, refErr, outcome(candErr))
	case candErr != nil:
		return fmt.Errorf("the Write of seq %d to %d, which the reference accepted, was %s: %w", first, last, outcome(candErr), candErr)
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

// reopen closes the store and opens it again, and checks what a reopening must
// not lose: the token, the horizon, that the closed handle is closed, the rule
// that Seq only rises, and that an earlier Retain still changes nothing.
func reopen(cand store.Store, opts Options, written []store.Record, horizon store.Horizon) (store.Store, error) {
	before := cand.LastSeq()
	beforeHorizon := cand.Horizon()
	next, err := opts.Reopen(cand)
	if err != nil {
		return nil, fmt.Errorf("reopening: %w", err)
	}
	if err := cand.Write(bg, nil); !errors.Is(err, store.ErrClosed) {
		return next, fmt.Errorf("after reopening, an empty Write to the old handle must fail with ErrClosed, got %w", orNil(err))
	}
	if got := cand.LastSeq(); got != before {
		return next, fmt.Errorf("the old handle's LastSeq = %d after it was closed; it was %d", got, before)
	}
	if got := next.LastSeq(); got != before {
		return next, fmt.Errorf("LastSeq = %d after reopening; it was %d", got, before)
	}
	if got := next.Horizon(); !sameHorizon(got, beforeHorizon) {
		return next, fmt.Errorf("Horizon() = %v after reopening; it was %v", got, beforeHorizon)
	}
	if len(written) == 0 {
		return next, nil
	}
	last := written[len(written)-1]
	if err := next.Write(bg, cloneRecords([]store.Record{last})); !errors.Is(err, store.ErrInvalid) {
		return next, fmt.Errorf("after reopening, a record whose Seq %d was already written must be refused with ErrInvalid, got %w", last.Seq, orNil(err))
	}
	if !horizon.IsZero() {
		if err := next.Retain(bg, horizon.Time.Add(-time.Hour)); err != nil {
			return next, fmt.Errorf("after reopening, an earlier Retain must be accepted and ignored: %w", err)
		}
		stale := last
		stale.Seq, stale.EventTime, stale.Through = before+1, horizon.Time.Add(-time.Second), time.Time{}
		if err := next.Write(bg, cloneRecords([]store.Record{stale})); !errors.Is(err, store.ErrBeforeHorizon) {
			return next, fmt.Errorf("after reopening, a record before the horizon %s must be refused with ErrBeforeHorizon, got %w", horizon.Time.Format(time.RFC3339), orNil(err))
		}
	}
	if got := next.LastSeq(); got != before {
		return next, fmt.Errorf("a refused record moved LastSeq to %d after reopening; it was %d", got, before)
	}
	if got := next.Horizon(); !sameHorizon(got, beforeHorizon) {
		return next, fmt.Errorf("Horizon() = %v after reopening and an earlier Retain; it was %v", got, beforeHorizon)
	}
	return next, nil
}
