package conformance

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// probeState is what a round of comparisons needs to know about the stream so
// far.
type probeState struct {
	entities   []identity.Fingerprint
	stranger   identity.Fingerprint // never written about
	written    []engine.Record
	horizon    time.Time // zero before the first retention
	tokenFloor uint64    // after a retention, tokens below this are not asked about
}

// floorTime is the earliest instant that may be asked about.
func (s probeState) floorTime() time.Time {
	if s.horizon.IsZero() {
		return engine.MinEventTime
	}
	return s.horizon
}

var allLayers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

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

// reads puts the same questions to the candidate and the oracle.
type reads struct {
	cand  engine.Engine
	ora   *oracle.Oracle
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

func scopeLabel(sc engine.Scope) string {
	if sc.AsOf == engine.Latest {
		return fmt.Sprintf("%s as of latest", sc.Layer)
	}
	return fmt.Sprintf("%s as of %d", sc.Layer, sc.AsOf)
}

// entity compares Alive and Neighbors, both directions, for one entity at each
// instant, layer and token.
func (r reads) entity(fp identity.Fingerprint, layers []catalog.Layer, times []time.Time, tokens []uint64) error {
	for i, t := range times {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := engine.Scope{Layer: layer, AsOf: tok}
				wantAlive, err := r.ora.Alive(fp, t, sc)
				if err != nil {
					return err
				}
				gotAlive, err := r.cand.Alive(fp, t, sc)
				if err != nil {
					return fmt.Errorf("Alive(%s, %s, %s): %w", fp, r.label(t), scopeLabel(sc), err)
				}
				if gotAlive != wantAlive {
					return fmt.Errorf("%w: Alive(%s, %s, %s) = %v; oracle says %v", ErrMismatch, fp, r.label(t), scopeLabel(sc), gotAlive, wantAlive)
				}
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					want, err := r.ora.Neighbors(fp, dir, t, sc)
					if err != nil {
						return err
					}
					got, err := r.cand.Neighbors(fp, dir, t, sc)
					if err != nil {
						return fmt.Errorf("Neighbors(%s, %s, %s, %s): %w", fp, dir, r.label(t), scopeLabel(sc), err)
					}
					if !slices.Equal(got, want) {
						return fmt.Errorf("%w: Neighbors(%s, %s, %s, %s) = %v; oracle says %v", ErrMismatch, fp, dir, r.label(t), scopeLabel(sc), got, want)
					}
				}
			}
		}
	}
	return nil
}

// windows compares Window, in each of the given directions, for one entity over
// each interval, layer and token.
func (r reads) windows(fp identity.Fingerprint, layers []catalog.Layer, dirs []engine.Direction, ivs []interval, tokens []uint64) error {
	for i, iv := range ivs {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := engine.Scope{Layer: layer, AsOf: tok}
				for _, dir := range dirs {
					want, err := r.ora.Window(fp, dir, iv.from, iv.to, sc)
					if err != nil {
						return err
					}
					got, err := r.cand.Window(fp, dir, iv.from, iv.to, sc)
					if err != nil {
						return fmt.Errorf("Window(%s, %s, %s): %w", fp, dir, scopeLabel(sc), err)
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

// batch compares NeighborsBatch for a whole set of fingerprints at each
// instant, layer and token against the oracle's one-at-a-time answers, and
// requires an empty batch to have an empty answer.
func (r reads) batch(fps []identity.Fingerprint, layers []catalog.Layer, times []time.Time, tokens []uint64) error {
	for i, t := range times {
		for _, tok := range r.tokensFor(tokens, i) {
			for _, layer := range layers {
				sc := engine.Scope{Layer: layer, AsOf: tok}
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					got, err := r.cand.NeighborsBatch(fps, dir, t, sc)
					if err != nil {
						return fmt.Errorf("NeighborsBatch(%d fingerprints, %s, %s, %s): %w", len(fps), dir, r.label(t), scopeLabel(sc), err)
					}
					if len(got) != len(fps) {
						return fmt.Errorf("%w: NeighborsBatch(%d fingerprints, %s, %s, %s) answered %d", ErrMismatch, len(fps), dir, r.label(t), scopeLabel(sc), len(got))
					}
					for j, fp := range fps {
						want, err := r.ora.Neighbors(fp, dir, t, sc)
						if err != nil {
							return err
						}
						if !slices.Equal(got[j], want) {
							return fmt.Errorf("%w: NeighborsBatch(%s, %s, %s) answer %d for %s = %v; oracle says %v",
								ErrMismatch, dir, r.label(t), scopeLabel(sc), j, fp, got[j], want)
						}
					}
					empty, err := r.cand.NeighborsBatch(nil, dir, t, sc)
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

// probeTokens picks the snapshot tokens to ask about: the latest, and one
// chosen at a record boundary (the record's own Seq, or the one before it,
// which is the token that does not yet see it). After a retention, tokens below
// the floor are not asked about.
func probeTokens(written []engine.Record, rng *rand.Rand, floor uint64) []uint64 {
	toks := []uint64{engine.Latest}
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
	return append(toks, max(seq, floor))
}

// probeLayers picks the layers to ask an entity about: every layer it has
// anything in, and one it does not, which must come back empty.
func probeLayers(ora *oracle.Oracle, fp identity.Fingerprint, rng *rand.Rand) []catalog.Layer {
	in := ora.Layers(fp)
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

// recordInstants picks a few event times at which fp has records in the layer
// and direction: the first, the last and one between, without repeats. They put
// window edges where records are.
func recordInstants(ora *oracle.Oracle, fp identity.Fingerprint, layer catalog.Layer, dir engine.Direction, floor time.Time, rng *rand.Rand) []time.Time {
	recs, err := ora.Window(fp, dir, floor, engine.MaxEventTime.Add(time.Nanosecond), engine.Current(layer))
	if err != nil || len(recs) == 0 {
		return nil
	}
	out := []time.Time{recs[0].EventTime, recs[len(recs)-1].EventTime, recs[rng.IntN(len(recs))].EventTime}
	slices.SortFunc(out, time.Time.Compare)
	return slices.CompactFunc(out, time.Time.Equal)
}

func compare(cand engine.Engine, ora *oracle.Oracle, rng *rand.Rand, opts Options, st probeState) error {
	if len(st.written) == 0 {
		return nil
	}
	// Half the rounds ask an engine that can settle to flush and compact first,
	// so reads are checked against its files and not only its memory. The draw is
	// made for every engine, so the questions asked do not depend on whether the
	// engine can settle.
	settle := rng.IntN(2) == 0
	if s, ok := cand.(engine.Settler); ok && settle {
		if err := s.Settle(); err != nil {
			return fmt.Errorf("settling the candidate: %w", err)
		}
	}
	first := st.written[0].EventTime
	rd := reads{cand: cand, ora: ora, label: func(t time.Time) string { return t.Sub(first).String() }, rotate: true}
	floor := st.floorTime()

	// Mostly random entities, but always some that were just written to.
	sample := make([]identity.Fingerprint, 0, opts.Entities)
	for range opts.Entities / 2 {
		sample = append(sample, st.entities[rng.IntN(len(st.entities))])
	}
	// Both ends of recent edges, so reverse reads see the newest writes too.
	for len(sample) < opts.Entities {
		r := st.written[len(st.written)-1-rng.IntN(min(len(st.written), 50))]
		end := r.Subject.A
		if r.Subject.Kind == engine.SubjectEdge && rng.IntN(2) == 0 {
			end = r.Subject.B
		}
		sample = append(sample, end)
	}

	var batchTimes []time.Time
	layersSeen := map[catalog.Layer]bool{}
	bothDirs := []engine.Direction{engine.Forward, engine.Reverse}
	for _, fp := range sample {
		layers := probeLayers(ora, fp, rng)
		times := probeTimes(st.written, rng, opts.Probes, st.horizon)
		tokens := probeTokens(st.written, rng, st.tokenFloor)
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
		if err := rd.windows(fp, layers, bothDirs, between(times), tokens); err != nil {
			return err
		}
		for _, layer := range layers {
			layersSeen[layer] = true
			for _, dir := range bothDirs {
				var ivs []interval
				for _, x := range recordInstants(ora, fp, layer, dir, floor, rng) {
					ivs = append(ivs, around(x, floor)...)
				}
				if err := rd.windows(fp, []catalog.Layer{layer}, []engine.Direction{dir}, ivs, tokens); err != nil {
					return err
				}
			}
		}
		if len(batchTimes) < 3 {
			batchTimes = append(batchTimes, times[rng.IntN(len(times))])
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
	return rd.batch(fps, layers, batchTimes, probeTokens(st.written, rng, st.tokenFloor))
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
