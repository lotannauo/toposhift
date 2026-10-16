package storetest

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// RestoredOptions says what CompareRestored asks about.
type RestoredOptions struct {
	// Entities are the fingerprints asked about. Each is asked about in every layer
	// the live store holds anything of it in.
	Entities []identity.Fingerprint
	// Written are the records the live store accepted. Their instants, and their
	// deadlines, at or after a layer's horizon are asked about in that layer.
	Written []store.Record
	// Span is how far after each layer's horizon every whole second is asked about:
	// where a restore that lost the records deciding what was alive at the horizon
	// answers differently. Every instant and deadline of a record up to Span after
	// the horizon is asked about too.
	Span time.Duration
	// Sample, above 1, asks about only every Sample-th of the instants and deadlines
	// of records later than Span after the horizon, in time order, to bound the
	// cost of a long stream. 0 or 1 asks about all of them.
	Sample int
}

// RestoredMismatch is the first read on which a restored store and the live store
// disagree. It wraps an error that wraps ErrMismatch.
type RestoredMismatch struct {
	// Layer is the layer read.
	Layer catalog.Layer
	// Read names the read: "Alive or Neighbors", "NeighborsBatch", "Window or
	// EntityWindow", "WindowSince", "horizon" or "refusal".
	Read string
	// Entity is the entity read, or the zero fingerprint for a read of many.
	Entity identity.Fingerprint
	// At is the instant read, or the start of the window.
	At  time.Time
	Err error
}

// Error describes the mismatch.
func (e *RestoredMismatch) Error() string {
	return fmt.Sprintf("restored store, layer %s, %s at %s: %v", e.Layer, e.Read, e.At.UTC().Format(time.RFC3339Nano), e.Err)
}

// Unwrap returns the mismatch's error.
func (e *RestoredMismatch) Unwrap() error { return e.Err }

// CompareRestored compares a store restored from a backup with the live store the
// backup was taken of, where a restore must answer the same: in every layer, at
// the latest token and at every instant at or after the layer's horizon. A restore
// holds what the live store retired only as far as its backup kept it, so nothing
// before a horizon is compared.
//
// Per layer it requires the horizon's time to be equal in both, and then compares
// Alive (a quarantine by its collision), Neighbors, NeighborsBatch, Window,
// EntityWindow and WindowSince with payloads, at the horizon, one nanosecond after
// it, every whole second up to Span after it, and every instant and deadline of a
// record of the layer at or after it (every Sample-th, past Span), with windows
// between consecutive instants and one from the horizon on.
// It requires both to refuse a read one nanosecond before the horizon, and the
// restored store to refuse a token in [the live store's horizon Seq, its own): a
// restore retains at its own last Seq. It asks the instants in ascending order, so
// the first mismatch it reports is at the earliest instant it found one.
//
// It returns nil, a *RestoredMismatch wrapping ErrMismatch, or the error of a read
// that failed.
func CompareRestored(live *memstore.Store, restored store.Store, o RestoredOptions) error {
	for _, l := range allLayers {
		if err := compareRestoredLayer(live, restored, o, l); err != nil {
			return err
		}
	}
	return nil
}

func compareRestoredLayer(live *memstore.Store, restored store.Store, o RestoredOptions, l catalog.Layer) error {
	hs, hr := live.LayerHorizon(l), restored.LayerHorizon(l)
	floor := store.MinEventTime
	if !hs.IsZero() {
		floor = hs.Time
	}
	label := func(t time.Time) string { return t.Sub(floor).String() }
	miss := func(read string, fp identity.Fingerprint, at time.Time, err error) error {
		if !errors.Is(err, ErrMismatch) {
			return err
		}
		return &RestoredMismatch{Layer: l, Read: read, Entity: fp, At: at, Err: err}
	}
	if hs.IsZero() != hr.IsZero() || !hs.Time.Equal(hr.Time) {
		return miss("horizon", identity.Fingerprint{}, floor,
			fmt.Errorf("%w: the horizon is %v in the restored store and %v in the live store", ErrMismatch, hr, hs))
	}

	var fps []identity.Fingerprint
	for _, fp := range o.Entities {
		if slices.Contains(live.Layers(fp), l) {
			fps = append(fps, fp)
		}
	}
	if len(fps) == 0 {
		return nil
	}
	times := restoredInstants(o, l, floor)
	rd := reads{cand: restored, ref: live, label: label}
	latest := []uint64{store.Latest}
	layers := []catalog.Layer{l}

	for _, t := range times {
		for _, fp := range fps {
			if err := rd.entity(fp, layers, []time.Time{t}, latest); err != nil {
				return miss("Alive or Neighbors", fp, t, err)
			}
		}
		if err := rd.batch(fps, layers, []time.Time{t}, latest); err != nil {
			return miss("NeighborsBatch", identity.Fingerprint{}, t, err)
		}
	}
	ivs := between(append(slices.Clone(times), store.MaxEventTime.Add(time.Nanosecond)))
	ivs = append(ivs, interval{floor, store.MaxEventTime.Add(time.Nanosecond)})
	for _, fp := range fps {
		for _, iv := range ivs {
			if err := rd.windows(fp, layers, bothDirections, []interval{iv}, latest); err != nil {
				return miss("Window or EntityWindow", fp, iv.from, err)
			}
			if err := rd.entityWindows(fp, layers, []interval{iv}, latest); err != nil {
				return miss("Window or EntityWindow", fp, iv.from, err)
			}
		}
	}
	if err := compareRestoredSince(live, restored, fps, l, times, o.Span); err != nil {
		return miss("WindowSince", identity.Fingerprint{}, floor, err)
	}

	if hs.IsZero() {
		return nil
	}
	if err := rd.refusals(fps[0], l, hs); err != nil {
		return miss("refusal", fps[0], hs.Time.Add(-time.Nanosecond), err)
	}
	// The restore retained at its own last Seq, so the tokens from the live store's
	// horizon up to it are refused there, and answered in the live store.
	if hs.Seq < hr.Seq {
		for _, tok := range []uint64{hs.Seq, hr.Seq - 1} {
			sc := store.Scope{Layer: l, AsOf: tok}
			if _, err := live.Alive(bg, fps[0], hs.Time, sc); err != nil && !isQuarantine(err) {
				return fmt.Errorf("live Alive(%s, %s, %s): %w", fps[0], label(hs.Time), scopeLabel(sc), err)
			}
			if _, err := restored.Alive(bg, fps[0], hs.Time, sc); !errors.Is(err, store.ErrBeforeHorizon) {
				return miss("refusal", fps[0], hs.Time, fmt.Errorf("%w: Alive(%s, %s, %s) in the restored store, whose horizon is at seq %d: want ErrBeforeHorizon, got %w",
					ErrMismatch, fps[0], label(hs.Time), scopeLabel(sc), hr.Seq, orNil(err)))
			}
		}
	}
	return nil
}

// restoredInstants are the instants CompareRestored asks about in layer l, at or
// after floor, ascending and without repeats.
func restoredInstants(o RestoredOptions, l catalog.Layer, floor time.Time) []time.Time {
	ts := []time.Time{floor, floor.Add(time.Nanosecond)}
	for d := time.Second; d <= o.Span; d += time.Second {
		ts = append(ts, floor.Add(d))
	}
	var later []time.Time
	dense := floor.Add(o.Span)
	for _, r := range o.Written {
		if r.Layer != l {
			continue
		}
		last, _ := store.LastOf(r.EventTime, r.Through)
		cand := []time.Time{r.EventTime, last}
		if r.TTL > 0 {
			cand = append(cand, last.Add(r.TTL), last.Add(r.TTL-time.Nanosecond))
		}
		for _, t := range cand {
			switch {
			case t.Before(floor):
			case !t.After(dense):
				ts = append(ts, t)
			default:
				later = append(later, t)
			}
		}
	}
	slices.SortFunc(later, time.Time.Compare)
	later = slices.CompactFunc(later, time.Time.Equal)
	for i, t := range later {
		if o.Sample <= 1 || i%o.Sample == 0 || i == len(later)-1 {
			ts = append(ts, t)
		}
	}
	slices.SortFunc(ts, time.Time.Compare)
	return slices.CompactFunc(ts, time.Time.Equal)
}

// compareRestoredSince compares WindowSince with payloads, for all of fps at once,
// in each direction, over windows that start at the horizon, one nanosecond
// after it, each whole minute of the span, and each tenth instant asked about.
func compareRestoredSince(live *memstore.Store, restored store.Store, fps []identity.Fingerprint, l catalog.Layer, times []time.Time, span time.Duration) error {
	floor := times[0]
	starts := []time.Time{floor, floor.Add(time.Nanosecond)}
	for d := time.Minute; d <= span; d += time.Minute {
		starts = append(starts, floor.Add(d))
	}
	for i := 0; i < len(times); i += 10 {
		starts = append(starts, times[i])
	}
	end := store.MaxEventTime.Add(time.Nanosecond)
	sc := store.Current(l)
	o := store.SinceOptions{Payloads: true}
	for _, from := range starts {
		for _, to := range []time.Time{from.Add(time.Minute), end} {
			for _, dir := range sinceDirs {
				want, err := live.WindowSince(bg, fps, dir, from, to, sc, o)
				if err != nil {
					return fmt.Errorf("live WindowSince: %w", err)
				}
				got, err := restored.WindowSince(bg, fps, dir, from, to, sc, o)
				if err != nil {
					return fmt.Errorf("WindowSince: %w", err)
				}
				if msg := sinceDiff(got, want); msg != "" {
					a := sinceAsk{fps: fps, dir: dir, from: from, to: to, sc: sc, o: o}
					return fmt.Errorf("%w: %s: %s", ErrMismatch, a, msg)
				}
			}
		}
	}
	return nil
}
