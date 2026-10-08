package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// bg is the context every call of the suite is made with, except where a check is
// about contexts.
var bg = context.Background()

// ErrMismatch is wrapped by every error that reports a read on which the store
// under test and the reference disagree, so a caller can tell a wrong answer from
// a failed call or a refused write.
var ErrMismatch = errors.New("store and reference disagree")

// orNil makes "accepted" print as an error value.
func orNil(err error) error {
	if err == nil {
		return errors.New("no error")
	}
	return err
}

// cloneRecords copies records, payloads included, so a store that keeps or alters
// what it is given cannot change the reference's copy.
func cloneRecords(rs []store.Record) []store.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// scribble overwrites the bytes of the payloads of rs in place. It is done to
// what a store was given after a successful Write, and to what a read returned,
// to find a store that still shares the bytes.
func scribble(rs []store.Record) {
	for _, r := range rs {
		for i := range r.Payload {
			r.Payload[i] ^= 0xff
		}
	}
}

// probeState is what a round of comparisons needs to know about the stream so
// far.
type probeState struct {
	entities   []identity.Fingerprint
	stranger   identity.Fingerprint // never written about
	written    []store.Record
	horizon    store.Horizon // zero before the first retention
	bootPolicy bool          // the store tells boots apart, so some entities are quarantined
}

// floorTime is the earliest instant that may be asked about.
func (s probeState) floorTime() time.Time {
	if s.horizon.IsZero() {
		return store.MinEventTime
	}
	return s.horizon.Time
}

var allLayers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

var bothDirections = []store.Direction{store.Forward, store.Reverse}

// interval is a half-open window [from, to).
type interval struct{ from, to time.Time }

// between returns the windows between consecutive instants.
func between(times []time.Time) []interval {
	ts := slices.Clone(times)
	slices.SortFunc(ts, time.Time.Compare)
	var out []interval
	for i := 0; i+1 < len(ts); i++ {
		out = append(out, interval{ts[i], ts[i+1]})
	}
	return out
}

// around returns the windows that put an edge exactly on, one nanosecond before
// and one nanosecond past the instant x, so an off-by-one in either edge of a
// window shows: [x, x+1ns) holds a record at x and nothing else; [x+1ns, x+2ns)
// must not hold it; [x-1ns, x) must not hold it either. The last is skipped if
// x-1ns is before floor, which may not be asked about.
func around(x, floor time.Time) []interval {
	ns := time.Nanosecond
	out := []interval{{x, x.Add(ns)}, {x.Add(ns), x.Add(2 * ns)}}
	if lo := x.Add(-ns); !lo.Before(floor) {
		out = append(out, interval{lo, x})
	}
	return out
}

// reads puts the same questions to the store under test and the reference.
type reads struct {
	cand  store.Store
	ref   *memstore.Store
	label func(time.Time) string // how an instant is shown in a message
	// rotate asks each question under one of the tokens, taken in turn, instead
	// of under every one. The random rounds use it, so the number of questions
	// does not multiply by the number of tokens; the scripted checks, which need
	// every token at every instant, do not. shift moves where the rotation
	// starts, so the first instant is not always asked under the same token.
	rotate bool
	shift  int
}

// tokensFor returns the tokens question i is asked under.
func (r reads) tokensFor(tokens []uint64, i int) []uint64 {
	if r.rotate {
		j := (i + r.shift) % len(tokens)
		return tokens[j : j+1]
	}
	return tokens
}

func scopeLabel(sc store.Scope) string {
	if sc.AsOf == store.Latest {
		return fmt.Sprintf("%s as of latest", sc.Layer)
	}
	return fmt.Sprintf("%s as of %d", sc.Layer, sc.AsOf)
}

func describeAlive(ok bool, err error) string {
	if err != nil {
		return fmt.Sprintf("%v, error %q", ok, err)
	}
	return fmt.Sprint(ok)
}

// aliveDiff compares two answers of Alive and returns how they differ, or "". An
// entity is either alive or not, with no error, or quarantined: false with a
// *store.QuarantineError that names the entity, the layer and the collision.
func aliveDiff(gotOK bool, gotErr error, wantOK bool, wantErr error) string {
	var gq, wq *store.QuarantineError
	gotQ, wantQ := errors.As(gotErr, &gq), errors.As(wantErr, &wq)
	switch {
	case gotQ != wantQ:
		return fmt.Sprintf("%s; reference says %s", describeAlive(gotOK, gotErr), describeAlive(wantOK, wantErr))
	case !gotQ:
		if gotOK != wantOK {
			return fmt.Sprintf("%v; reference says %v", gotOK, wantOK)
		}
		return ""
	case gotOK:
		return fmt.Sprintf("true together with the quarantine error %q", gotErr)
	case gq.Entity != wq.Entity || gq.Layer != wq.Layer:
		return fmt.Sprintf("quarantine of %s in %s; reference says %s in %s", gq.Entity, gq.Layer, wq.Entity, wq.Layer)
	}
	return collisionDiff(gq.Collision, wq.Collision)
}

func collisionDiff(got, want *lifecycle.CloneCollisionError) string {
	switch {
	case got == nil && want == nil:
		return ""
	case got == nil || want == nil:
		return fmt.Sprintf("quarantine collision %s; reference says %s", describeCollision(got), describeCollision(want))
	case got.StaleBoot != want.StaleBoot || got.NewerBoot != want.NewerBoot ||
		!got.ObservedAt.Equal(want.ObservedAt) || !got.NewerFirstSeen.Equal(want.NewerFirstSeen):
		return fmt.Sprintf("quarantine collision %s; reference says %s", describeCollision(got), describeCollision(want))
	}
	return ""
}

func describeCollision(c *lifecycle.CloneCollisionError) string {
	if c == nil {
		return "none"
	}
	return fmt.Sprintf("{stale %q seen %s, newer %q first seen %s}",
		c.StaleBoot, c.ObservedAt.UTC().Format(time.RFC3339Nano), c.NewerBoot, c.NewerFirstSeen.UTC().Format(time.RFC3339Nano))
}

// isQuarantine reports whether err is a quarantine.
func isQuarantine(err error) bool {
	var qe *store.QuarantineError
	return errors.As(err, &qe)
}

// entity compares Alive and Neighbors, both directions, for one entity at each
// instant, layer and token.
func (r reads) entity(fp identity.Fingerprint, layers []catalog.Layer, times []time.Time, tokens []uint64) error {
	for i, t := range times {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := store.Scope{Layer: layer, AsOf: tok}
				wantAlive, wantErr := r.ref.Alive(bg, fp, t, sc)
				if wantErr != nil && !isQuarantine(wantErr) {
					return fmt.Errorf("reference Alive(%s, %s, %s): %w", fp, r.label(t), scopeLabel(sc), wantErr)
				}
				gotAlive, gotErr := r.cand.Alive(bg, fp, t, sc)
				if gotErr != nil && !isQuarantine(gotErr) {
					return fmt.Errorf("Alive(%s, %s, %s): %w", fp, r.label(t), scopeLabel(sc), gotErr)
				}
				if msg := aliveDiff(gotAlive, gotErr, wantAlive, wantErr); msg != "" {
					return fmt.Errorf("%w: Alive(%s, %s, %s) = %s", ErrMismatch, fp, r.label(t), scopeLabel(sc), msg)
				}
				for _, dir := range bothDirections {
					want, err := r.ref.Neighbors(bg, fp, dir, t, sc)
					if err != nil {
						return fmt.Errorf("reference Neighbors(%s, %s, %s, %s): %w", fp, dir, r.label(t), scopeLabel(sc), err)
					}
					got, err := r.cand.Neighbors(bg, fp, dir, t, sc)
					if err != nil {
						return fmt.Errorf("Neighbors(%s, %s, %s, %s): %w", fp, dir, r.label(t), scopeLabel(sc), err)
					}
					if !slices.Equal(got, want) {
						return fmt.Errorf("%w: Neighbors(%s, %s, %s, %s) = %v; reference says %v", ErrMismatch, fp, dir, r.label(t), scopeLabel(sc), got, want)
					}
				}
			}
		}
	}
	return nil
}

// windows compares Window, in each of the given directions, for one entity over
// each interval, layer and token.
func (r reads) windows(fp identity.Fingerprint, layers []catalog.Layer, dirs []store.Direction, ivs []interval, tokens []uint64) error {
	for i, iv := range ivs {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := store.Scope{Layer: layer, AsOf: tok}
				for _, dir := range dirs {
					want, err := r.ref.Window(bg, fp, dir, iv.from, iv.to, sc)
					if err != nil {
						return fmt.Errorf("reference Window(%s, %s, [%s, %s), %s): %w", fp, dir, r.label(iv.from), r.label(iv.to), scopeLabel(sc), err)
					}
					got, err := r.cand.Window(bg, fp, dir, iv.from, iv.to, sc)
					if err != nil {
						return fmt.Errorf("Window(%s, %s, [%s, %s), %s): %w", fp, dir, r.label(iv.from), r.label(iv.to), scopeLabel(sc), err)
					}
					if msg := diffRecords(got, want); msg != "" {
						return fmt.Errorf("%w: Window(%s, %s, [%s, %s), %s): %s", ErrMismatch, fp, dir, r.label(iv.from), r.label(iv.to), scopeLabel(sc), msg)
					}
				}
			}
		}
	}
	return nil
}

// entityWindows compares EntityWindow for one entity over each interval, layer
// and token.
func (r reads) entityWindows(fp identity.Fingerprint, layers []catalog.Layer, ivs []interval, tokens []uint64) error {
	for i, iv := range ivs {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := store.Scope{Layer: layer, AsOf: tok}
				want, err := r.ref.EntityWindow(bg, fp, iv.from, iv.to, sc)
				if err != nil {
					return fmt.Errorf("reference EntityWindow(%s, [%s, %s), %s): %w", fp, r.label(iv.from), r.label(iv.to), scopeLabel(sc), err)
				}
				got, err := r.cand.EntityWindow(bg, fp, iv.from, iv.to, sc)
				if err != nil {
					return fmt.Errorf("EntityWindow(%s, [%s, %s), %s): %w", fp, r.label(iv.from), r.label(iv.to), scopeLabel(sc), err)
				}
				if msg := diffRecords(got, want); msg != "" {
					return fmt.Errorf("%w: EntityWindow(%s, [%s, %s), %s): %s", ErrMismatch, fp, r.label(iv.from), r.label(iv.to), scopeLabel(sc), msg)
				}
			}
		}
	}
	return nil
}

// batch compares NeighborsBatch for a whole set of fingerprints at each
// instant, layer and token against the reference's one-at-a-time answers, and
// requires an empty batch to have an empty answer.
func (r reads) batch(fps []identity.Fingerprint, layers []catalog.Layer, times []time.Time, tokens []uint64) error {
	for i, t := range times {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := store.Scope{Layer: layer, AsOf: tok}
				for _, dir := range bothDirections {
					got, err := r.cand.NeighborsBatch(bg, fps, dir, t, sc)
					if err != nil {
						return fmt.Errorf("NeighborsBatch(%d fingerprints, %s, %s, %s): %w", len(fps), dir, r.label(t), scopeLabel(sc), err)
					}
					if len(got) != len(fps) {
						return fmt.Errorf("%w: NeighborsBatch(%d fingerprints, %s, %s, %s) answered %d", ErrMismatch, len(fps), dir, r.label(t), scopeLabel(sc), len(got))
					}
					for j, fp := range fps {
						want, err := r.ref.Neighbors(bg, fp, dir, t, sc)
						if err != nil {
							return fmt.Errorf("reference Neighbors(%s, %s, %s, %s): %w", fp, dir, r.label(t), scopeLabel(sc), err)
						}
						if !slices.Equal(got[j], want) {
							return fmt.Errorf("%w: NeighborsBatch(%s, %s, %s) answer %d for %s = %v; reference says %v",
								ErrMismatch, dir, r.label(t), scopeLabel(sc), j, fp, got[j], want)
						}
					}
					empty, err := r.cand.NeighborsBatch(bg, nil, dir, t, sc)
					if err != nil {
						return fmt.Errorf("NeighborsBatch of nothing: %w", err)
					}
					if len(empty) != 0 {
						return fmt.Errorf("%w: NeighborsBatch of nothing = %v; want an empty answer", ErrMismatch, empty)
					}
				}
			}
		}
	}
	return nil
}

// refusals asks every kind of read one nanosecond before the horizon's time, and
// one token below its Seq, and requires both stores to refuse each with
// store.ErrBeforeHorizon. A window starts at the early instant and ends an hour
// after the horizon.
func (r reads) refusals(fp identity.Fingerprint, layer catalog.Layer, h store.Horizon) error {
	type probe struct {
		what string
		t    time.Time
		sc   store.Scope
	}
	probes := []probe{{"one nanosecond before the horizon", h.Time.Add(-time.Nanosecond), store.Current(layer)}}
	if h.Seq > 0 {
		probes = append(probes, probe{"the token below the horizon's", h.Time, store.Scope{Layer: layer, AsOf: h.Seq - 1}})
	}
	if h.Seq > 1 {
		probes = append(probes, probe{"token 0", h.Time, store.Scope{Layer: layer, AsOf: 0}})
	}
	for _, p := range probes {
		to := h.Time.Add(time.Hour)
		asks := map[string]func(s store.Store) error{
			"Alive":     func(s store.Store) error { _, err := s.Alive(bg, fp, p.t, p.sc); return err },
			"Neighbors": func(s store.Store) error { _, err := s.Neighbors(bg, fp, store.Forward, p.t, p.sc); return err },
			"NeighborsBatch": func(s store.Store) error {
				_, err := s.NeighborsBatch(bg, []identity.Fingerprint{fp}, store.Reverse, p.t, p.sc)
				return err
			},
			"Window":       func(s store.Store) error { _, err := s.Window(bg, fp, store.Reverse, p.t, to, p.sc); return err },
			"EntityWindow": func(s store.Store) error { _, err := s.EntityWindow(bg, fp, p.t, to, p.sc); return err },
		}
		for _, name := range []string{"Alive", "Neighbors", "NeighborsBatch", "Window", "EntityWindow"} {
			if err := asks[name](r.ref); !errors.Is(err, store.ErrBeforeHorizon) {
				return fmt.Errorf("reference %s of %s at %s, %s, with the horizon at %s (seq %d): want ErrBeforeHorizon, got %w",
					name, fp, r.label(p.t), p.what, r.label(h.Time), h.Seq, orNil(err))
			}
			if err := asks[name](r.cand); !errors.Is(err, store.ErrBeforeHorizon) {
				return fmt.Errorf("%w: %s of %s at %s (%s, %s) with the horizon at %s (seq %d) must be refused with ErrBeforeHorizon, got %w",
					ErrMismatch, name, fp, r.label(p.t), scopeLabel(p.sc), p.what, r.label(h.Time), h.Seq, orNil(err))
			}
		}
	}
	return nil
}

// unrefused asks the reads that sit at the edge of what a store with no
// retention horizon must answer: an instant before the earliest event time, and
// token 0. Both stores must answer, and the same.
func (r reads) unrefused(fp identity.Fingerprint, layer catalog.Layer) error {
	early := store.MinEventTime.Add(-time.Nanosecond)
	late := early.Add(time.Hour)
	for _, tok := range []uint64{store.Latest, 0} {
		if err := r.entity(fp, []catalog.Layer{layer}, []time.Time{early}, []uint64{tok}); err != nil {
			return err
		}
		ivs := []interval{{early, late}, {early, early.Add(time.Nanosecond)}}
		if err := r.windows(fp, []catalog.Layer{layer}, bothDirections, ivs, []uint64{tok}); err != nil {
			return err
		}
		if err := r.entityWindows(fp, []catalog.Layer{layer}, ivs, []uint64{tok}); err != nil {
			return err
		}
	}
	return r.batch([]identity.Fingerprint{fp}, []catalog.Layer{layer}, []time.Time{early}, []uint64{0})
}

// probeTimes picks instants at or after the horizon: boundaries of real
// records, and the extremes.
func probeTimes(written []store.Record, rng *rand.Rand, n int, horizon time.Time) []time.Time {
	floor := horizon
	if floor.IsZero() {
		floor = store.MinEventTime
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
		// A run's deadline is its last observation plus the TTL, and the last
		// observation is its Through if it has one.
		last := r.EventTime
		if r.Through.After(last) {
			last = r.Through
		}
		for _, t := range []time.Time{
			r.EventTime, r.EventTime.Add(-time.Nanosecond), last.Add(r.TTL), last.Add(r.TTL - time.Nanosecond),
			r.EventTime.Add(time.Duration(rng.Int64N(int64(time.Hour)))),
		} {
			if !t.Before(floor) && len(ts) < n {
				ts = append(ts, t)
			}
		}
	}
	return ts
}

// probeTokens picks the snapshot tokens to ask about: the latest, one
// chosen at a record boundary (the record's own Seq, or the one before it,
// which is the token that does not yet see it). After a retention, tokens below
// the floor are not asked about; they are refused.
func probeTokens(written []store.Record, rng *rand.Rand, floor uint64) []uint64 {
	toks := []uint64{store.Latest}
	if len(written) == 0 {
		return toks
	}
	seq := written[rng.IntN(len(written))].Seq
	if seq > 0 && rng.IntN(2) == 0 {
		seq--
	}
	if rng.IntN(20) == 0 {
		seq = 0
	}
	toks = append(toks, max(seq, floor))
	// Now and then a token that is above LastSeq and is not Latest, which reads as
	// LastSeq.
	switch rng.IntN(6) {
	case 0:
		// Not past Latest, whatever the first Seq of the stream was.
		if last := written[len(written)-1].Seq; last < store.Latest-1001 {
			toks = append(toks, last+1+rng.Uint64N(1000))
		}
	case 1:
		toks = append(toks, store.Latest-1)
	}
	return toks
}

// probeLayers picks the layers to ask an entity about: every layer it has
// anything in, and one it does not, which must come back empty.
func probeLayers(ref *memstore.Store, fp identity.Fingerprint, rng *rand.Rand) []catalog.Layer {
	in := ref.Layers(fp)
	layers := slices.Clone(in)
	var absent []catalog.Layer
	for _, l := range allLayers {
		if !slices.Contains(in, l) {
			absent = append(absent, l)
		}
	}
	if len(absent) > 0 {
		layers = append(layers, absent[rng.IntN(len(absent))])
	}
	return layers
}

// recordInstants picks a few event times from recs: the first, the last and one
// between, without repeats. They put window edges where records are.
func recordInstants(recs []store.Record, rng *rand.Rand) []time.Time {
	if len(recs) == 0 {
		return nil
	}
	out := []time.Time{recs[0].EventTime, recs[len(recs)-1].EventTime, recs[rng.IntN(len(recs))].EventTime}
	slices.SortFunc(out, time.Time.Compare)
	return slices.CompactFunc(out, time.Time.Equal)
}

// compare puts a round of questions to both stores.
func compare(cand store.Store, ref *memstore.Store, rng *rand.Rand, opts Options, st probeState) error {
	if len(st.written) == 0 {
		return nil
	}
	first := st.written[0].EventTime
	rd := reads{cand: cand, ref: ref, label: func(t time.Time) string { return t.Sub(first).String() }, rotate: true}
	floor := st.floorTime()

	// Mostly random entities, but always some that were just written to.
	sample := make([]identity.Fingerprint, 0, opts.Entities+2)
	for range opts.Entities / 2 {
		sample = append(sample, st.entities[rng.IntN(len(st.entities))])
	}
	// Both ends of recent edges, so reverse reads see the newest writes too.
	for len(sample) < opts.Entities {
		r := st.written[len(st.written)-1-rng.IntN(min(len(st.written), 50))]
		end := r.Subject.A
		if r.Subject.Kind == store.SubjectEdge && rng.IntN(2) == 0 {
			end = r.Subject.B
		}
		sample = append(sample, end)
	}
	// Quarantine does not depend on the instant, so the instants just around a
	// collision are where a store that folds only what lies before the instant is
	// found.
	collisions := map[identity.Fingerprint][]time.Time{}
	if st.bootPolicy {
		// A quarantined host is rare among the entities, and is asked about every round.
		n := 0
		for _, fp := range st.entities {
			if fp.Type() != catalog.Host || n == 2 {
				continue
			}
			_, err := ref.Alive(bg, fp, floor, store.Current(entityLayer(fp.Type())))
			var qe *store.QuarantineError
			if errors.As(err, &qe) && qe.Collision != nil {
				sample = append(sample, fp)
				n++
				for _, at := range []time.Time{qe.Collision.ObservedAt, qe.Collision.NewerFirstSeen} {
					for _, t := range []time.Time{at, at.Add(-time.Nanosecond)} {
						if !t.Before(floor) {
							collisions[fp] = append(collisions[fp], t)
						}
					}
				}
			}
		}
	}

	var batchTimes []time.Time
	layersSeen := map[catalog.Layer]bool{}
	for _, fp := range sample {
		layers := probeLayers(ref, fp, rng)
		times := append(probeTimes(st.written, rng, opts.Probes, st.horizon.Time), collisions[fp]...)
		tokens := probeTokens(st.written, rng, st.horizon.Seq)
		rd.shift = rng.IntN(len(tokens))
		if err := rd.entity(fp, layers, times, tokens); err != nil {
			return err
		}
		// The first instant a read may be asked about is where a retention puts
		// its baseline, so it is asked under every token, not just its turn.
		all := rd
		all.rotate = false
		if err := all.entity(fp, layers, times[:1], tokens); err != nil {
			return err
		}
		// The windows between instants tile the period, in every layer and both
		// directions; the ones that put an edge on a record are asked only where
		// that record is, which is where an off-by-one in that layer and
		// direction would show.
		ivs := between(times)
		if err := rd.windows(fp, layers, bothDirections, ivs, tokens); err != nil {
			return err
		}
		if err := rd.entityWindows(fp, layers, ivs, tokens); err != nil {
			return err
		}
		for _, layer := range layers {
			layersSeen[layer] = true
			sc := store.Current(layer)
			until := store.MaxEventTime.Add(time.Nanosecond)
			for _, dir := range bothDirections {
				recs, err := ref.Window(bg, fp, dir, floor, until, sc)
				if err != nil {
					return fmt.Errorf("reference Window of %s: %w", fp, err)
				}
				var edgeIvs []interval
				for _, x := range recordInstants(recs, rng) {
					edgeIvs = append(edgeIvs, around(x, floor)...)
				}
				if err := rd.windows(fp, []catalog.Layer{layer}, []store.Direction{dir}, edgeIvs, tokens); err != nil {
					return err
				}
			}
			recs, err := ref.EntityWindow(bg, fp, floor, until, sc)
			if err != nil {
				return fmt.Errorf("reference EntityWindow of %s: %w", fp, err)
			}
			var ownIvs []interval
			for _, x := range recordInstants(recs, rng) {
				ownIvs = append(ownIvs, around(x, floor)...)
			}
			if err := rd.entityWindows(fp, []catalog.Layer{layer}, ownIvs, tokens); err != nil {
				return err
			}
		}
		if len(batchTimes) < 3 {
			batchTimes = append(batchTimes, times[rng.IntN(len(times))])
		}
	}

	// What is refused, and what a store with no horizon must still answer.
	probed := sample[0]
	pl := ref.Layers(probed)
	layer := catalog.L2
	if len(pl) > 0 {
		layer = pl[0]
	}
	if st.horizon.IsZero() {
		if err := rd.unrefused(probed, layer); err != nil {
			return err
		}
	} else {
		if err := rd.refusals(probed, layer, st.horizon); err != nil {
			return err
		}
		// And the horizon's own instant and token are answered, not refused.
		at := rd
		at.rotate = false
		if err := at.entity(probed, []catalog.Layer{layer}, []time.Time{st.horizon.Time}, []uint64{st.horizon.Seq}); err != nil {
			return err
		}
	}

	// A batch with repeats and an entity nothing is written about.
	fps := append(slices.Clone(sample), sample[0], st.stranger)
	rng.Shuffle(len(fps), func(i, j int) { fps[i], fps[j] = fps[j], fps[i] })
	var layers []catalog.Layer
	for _, l := range allLayers {
		if layersSeen[l] {
			layers = append(layers, l)
		}
	}
	tokens := probeTokens(st.written, rng, st.horizon.Seq)
	if err := rd.batch(fps, layers, batchTimes, tokens); err != nil {
		return err
	}
	// And a frontier of several hundred, as a traversal asks for: a store that
	// answers a batch in chunks must index the results of every chunk.
	var frontier []identity.Fingerprint
	for len(frontier) < 300 {
		frontier = append(frontier, fps...)
	}
	rng.Shuffle(len(frontier), func(i, j int) { frontier[i], frontier[j] = frontier[j], frontier[i] })
	return rd.batch(frontier, layers, batchTimes[:1], tokens[:1])
}

// diffRecords says how two lists of records differ, or returns "".
func diffRecords(got, want []store.Record) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d records, reference says %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Layer != w.Layer || g.Subject != w.Subject || g.Producer != w.Producer || !g.EventTime.Equal(w.EventTime) ||
			g.Seq != w.Seq || g.Kind != w.Kind || g.TTL != w.TTL || !g.Through.Equal(w.Through) || !bytes.Equal(g.Payload, w.Payload) ||
			g.Boot != w.Boot {
			return fmt.Sprintf("record %d is %+v, reference says %+v", i, g, w)
		}
	}
	return ""
}
