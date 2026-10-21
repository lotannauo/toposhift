package decay_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The worlds of the properties: sightings of dependency edges by producers, in
// arrival order, written through ingest's own coalescer
// (coalesce.New(sequencer.CoalesceConfig()), flushed every 30 s of event time)
// to a store, and read back with Evidence. The truth is the raw sightings, which
// the store never sees whole: a sighting is an instant a producer saw an edge,
// or, for a record that attests continuity, a span, and it is known at the token
// of the last write it caused.

var ctx = context.Background()

// tb is what both *testing.T and *rapid.T offer.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// drawer draws the integers a world is made of: rapid for the property, and a
// fixed generator for the sweep whose counts are claims.
type drawer interface {
	// Int draws an integer in [lo, hi].
	Int(label string, lo, hi int) int
}

type rapidDrawer struct{ t *rapid.T }

func (d rapidDrawer) Int(label string, lo, hi int) int {
	return rapid.IntRange(lo, hi).Draw(d.t, label)
}

type randDrawer struct{ r *rand.Rand }

func (d randDrawer) Int(_ string, lo, hi int) int { return lo + d.r.IntN(hi-lo+1) }

func pick[T any](d drawer, label string, xs []T) T { return xs[d.Int(label, 0, len(xs)-1)] }

var worldBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// worldTTLs are the TTLs a producer may have: from one whose extension interval
// is below the flush lead to the shipped expiry.
var worldTTLs = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 6 * time.Hour}

func fingerprint(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// gaps are the spacings of a stream's sightings: around the extension interval
// X = TTL/2 (a run's age bound), around the TTL (the expiry), and past it.
func gaps(ttl time.Duration) []time.Duration {
	x := ttl / 2
	return []time.Duration{time.Second, x / 4, x - 1, x, x + 1, ttl - 1, ttl, ttl + 1, 2 * ttl}
}

// stream is one producer's sightings of one edge, at one TTL.
type stream struct {
	edge int
	name lifecycle.Producer
	ttl  time.Duration
}

// sighting is a producer's sighting of an edge: an instant, or the span [at,
// through] of a record that attests continuity (a translator's, which is never
// coalesced). stamp is the token at which the store knows of it, and refused says
// the store would not take it: it was before the retention horizon.
type sighting struct {
	stream      int
	at, through time.Time
	stamp       uint64
	refused     bool
}

// lastBy is the latest instant the sighting says the producer saw the edge, at or
// before t: its instant, or for a span the later of its start and t clipped to
// its end; false when it starts after t.
func (s sighting) lastBy(t time.Time) (time.Time, bool) {
	if s.at.After(t) {
		return time.Time{}, false
	}
	if s.through.After(s.at) && s.through.Before(t) {
		return s.through, true
	}
	if s.through.After(s.at) {
		return t, true
	}
	return s.at, true
}

// world is the cast, the sightings in arrival order, and what ingest does
// between them.
type world struct {
	a        identity.Fingerprint
	b        []identity.Fingerprint
	edges    []store.Subject
	streams  []stream
	arrivals []sighting
	// restartAt is the arrival after which the coalescer is replaced by an empty
	// one, or -1; graceful says the old one is flushed whole first, and otherwise
	// it is a crash, which loses the refreshes it absorbed.
	restartAt int
	graceful  bool
	// retainAt is the arrival after which the store retains, back from the
	// clock, or -1.
	retainAt   int
	retainBack time.Duration
	// overlap says the attested spans of one producer may overlap the producer's
	// next sightings, so a world's records can be read whole only: its queries
	// start before every record (see query).
	overlap bool
}

func depLayer() catalog.Layer {
	r, ok := catalog.Default().Relation(catalog.DependsOn)
	if !ok {
		panic("no depends_on")
	}
	return r.Layer()
}

func serviceLayer() catalog.Layer {
	e, ok := catalog.Default().Entity(catalog.Service)
	if !ok {
		panic("no service")
	}
	return e.Layer()
}

// drawWorld draws a world: one or two edges of one service, one or two producers
// of each, each stream steady for a while at a spacing around X or the TTL and
// with an occasional other one, a few sightings that attest a span (which, in half
// the worlds that do not retain, may overlap the stream's next sightings), some
// arriving late, and, when retain is set, a retention.
func drawWorld(d drawer, retain bool) world {
	w := world{a: fingerprint(catalog.Service, 1), restartAt: -1, retainAt: -1}
	w.overlap = !retain && d.Int("overlap", 0, 1) == 1
	nEdges := d.Int("edges", 1, 2)
	for i := range nEdges {
		b := fingerprint(catalog.Service, byte(2+i))
		w.b = append(w.b, b)
		w.edges = append(w.edges, store.EdgeSubject(w.a, b, catalog.DependsOn))
	}
	var all []sighting
	for e := range nEdges {
		for k := range d.Int("producers", 1, 2) {
			ttl := pick(d, "ttl", worldTTLs)
			w.streams = append(w.streams, stream{edge: e, name: lifecycle.Producer(fmt.Sprintf("p%d", k)), ttl: ttl})
			si := len(w.streams) - 1
			set := gaps(ttl)
			cadence := pick(d, "cadence", set)
			at := worldBase.Add(time.Duration(d.Int("start", 0, 600)) * time.Second)
			for range d.Int("sightings", 3, 12) {
				s := sighting{stream: si, at: at}
				if d.Int("attest", 0, 11) == 0 {
					s.through = at.Add(pick(d, "span", []time.Duration{ttl / 4, ttl / 2, ttl, 2 * ttl}))
				}
				all = append(all, s)
				step := cadence
				if d.Int("other", 0, 4) == 0 {
					step = pick(d, "gap", set)
				}
				if !w.overlap && s.through.After(at) {
					at = s.through
				}
				at = at.Add(step)
			}
		}
	}
	slices.SortStableFunc(all, func(x, y sighting) int { return x.at.Compare(y.at) })
	// Late arrivals: a sighting arrives up to four places after its turn.
	keys := make([]int, len(all))
	for i := range all {
		keys[i] = 2 * i
		if d.Int("late", 0, 5) == 0 {
			keys[i] += 2 * d.Int("delay", 1, 4)
			keys[i]++
		}
	}
	idx := make([]int, len(all))
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(x, y int) int { return keys[x] - keys[y] })
	for _, i := range idx {
		w.arrivals = append(w.arrivals, all[i])
	}
	if len(w.arrivals) > 2 && d.Int("restart", 0, 3) == 0 {
		w.restartAt = d.Int("restartAt", 1, len(w.arrivals)-2)
		w.graceful = d.Int("graceful", 0, 1) == 1
	}
	if retain && len(w.arrivals) > 2 {
		w.retainAt = d.Int("retainAt", len(w.arrivals)/3, len(w.arrivals)-2)
		w.retainBack = pick(d, "back", []time.Duration{0, time.Second, 10 * time.Minute, time.Hour, 3 * time.Hour})
	}
	return w
}

// run is a world written to its stores.
type run struct {
	w       world
	stores  []store.Store
	retain  func(h time.Time) // retains every store that retains, nil for none
	horizon time.Time
	// retainedSeq is the token the retention was made at, 0 before it.
	retainedSeq uint64
	last        time.Time // the newest event time of any sighting
}

// build writes the world through the coalescer to the stores, which must be
// empty, numbering the records the same in each. The service entities are
// asserted first, by a producer with no TTL, so every edge's endpoints exist.
func build(t tb, w world, stores []store.Store, retain func(h time.Time)) *run {
	t.Helper()
	r := &run{w: w, stores: stores, retain: retain}
	var seq uint64
	write := func(recs []store.Record) (dropped int) {
		var keep []store.Record
		for _, rec := range recs {
			if !r.horizon.IsZero() {
				if l, _ := store.LastOf(rec.EventTime, rec.Through); l.Before(r.horizon) {
					dropped++
					continue
				}
			}
			seq++
			rec.Seq = seq
			keep = append(keep, rec)
		}
		if len(keep) == 0 {
			return dropped
		}
		for _, st := range stores {
			if err := st.Write(ctx, keep); err != nil {
				t.Fatal(err)
			}
		}
		return dropped
	}
	svc := func(fp identity.Fingerprint) store.Record {
		return store.Record{
			Layer: serviceLayer(), Subject: store.EntitySubject(fp), Producer: "ep",
			EventTime: worldBase.Add(-time.Hour), Kind: lifecycle.Observe,
		}
	}
	recs := []store.Record{svc(w.a)}
	for _, b := range w.b {
		recs = append(recs, svc(b))
	}
	write(recs)

	cfg := sequencer.CoalesceConfig()
	co, err := coalesce.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var clock, tick time.Time
	for i, s := range w.arrivals {
		st := w.streams[s.stream]
		if clock.IsZero() {
			clock, tick = s.at, s.at.Add(coalesce.FlushPeriod)
		}
		if s.at.After(clock) {
			clock = s.at
		}
		for !tick.After(clock) {
			write(co.Flush(tick, nil))
			tick = tick.Add(coalesce.FlushPeriod)
		}
		rec := store.Record{
			Layer: depLayer(), Subject: w.edges[st.edge], Producer: st.name, EventTime: s.at,
			Through: s.through, Kind: lifecycle.Observe, TTL: st.ttl, EventTimeBasis: store.BasisObserved,
		}
		if write(co.Add(rec, nil)) > 0 {
			w.arrivals[i].refused = true
		} else {
			w.arrivals[i].stamp = seq
		}
		if s.at.After(r.last) {
			r.last = s.at
		}
		if i == w.restartAt {
			if w.graceful {
				write(co.FlushAll(nil))
			}
			if co, err = coalesce.New(cfg); err != nil {
				t.Fatal(err)
			}
			co.SetHorizonAll(r.horizon)
		}
		if i == w.retainAt && retain != nil {
			h := clock.Add(-w.retainBack)
			if h.After(r.horizon) {
				retain(h)
				r.horizon, r.retainedSeq = h, seq
				co.SetHorizonAll(h)
			}
		}
	}
	// The flushes that follow the last sighting, until the coalescer has nothing
	// absorbed that a flush at its own clock would write.
	for range 4 {
		write(co.Flush(tick, nil))
		tick = tick.Add(coalesce.FlushPeriod)
	}
	r.w = w
	return r
}

// truth is, for each producer in last, the latest instant at or before t at which
// the producer saw the edge, among the sightings known at the token.
func (r *run) truth(edge int, last []interval.Evidence, t time.Time, token uint64) (map[lifecycle.Producer]time.Time, error) {
	out := map[lifecycle.Producer]time.Time{}
	for _, s := range r.w.arrivals {
		st := r.w.streams[s.stream]
		if st.edge != edge || s.refused || s.stamp > token {
			continue
		}
		at, ok := s.lastBy(t)
		if !ok {
			continue
		}
		if cur, have := out[st.name]; !have || at.After(cur) {
			out[st.name] = at
		}
	}
	for _, e := range last {
		if _, ok := out[e.Producer]; !ok {
			return nil, fmt.Errorf("producer %s is live with no sighting", e.Producer)
		}
	}
	return out, nil
}

// times are the instants a query may ask at: each sighting's instants, moved by
// the offsets that sit on a run's age bound, a TTL and an expiry.
func (r *run) times(d drawer) time.Time {
	s := pick(d, "sighting", r.w.arrivals)
	st := r.w.streams[s.stream]
	x := st.ttl / 2
	base := s.at
	if !s.through.IsZero() && d.Int("end", 0, 1) == 1 {
		base = s.through
	}
	off := pick(d, "offset", []time.Duration{
		-1, 0, 1, x / 2, x - 1, x, x + 1, x + x/4, st.ttl - 1, st.ttl, st.ttl + 1, 2 * st.ttl,
	})
	return base.Add(off)
}

// tally is what a property counts, for the claims its test makes.
type tally struct {
	worlds, retained, queries, withEvidence, positive, exact, beforeHorizon int
	// worst is the largest of (true silence - bound) / X seen.
	worst float64
	// atX is the queries within a hundredth of X of the worst case.
	atX int
}

func (a *tally) add(b tally) {
	a.worlds += b.worlds
	a.retained += b.retained
	a.queries += b.queries
	a.withEvidence += b.withEvidence
	a.positive += b.positive
	a.exact += b.exact
	a.beforeHorizon += b.beforeHorizon
	a.atX += b.atX
	a.worst = max(a.worst, b.worst)
}

// edgeAt reads edge i forward with Evidence, with the cascade, over [from,
// t+1ns) at the token, and returns the element of the edge.
func (r *run) edgeAt(st store.Store, i int, from, t time.Time, token uint64) (interval.Element, error) {
	rd := interval.NewReader(st, interval.Options{Evidence: true})
	res, err := rd.Edges(ctx, r.w.a, store.Forward, interval.Window{From: from, To: t.Add(time.Nanosecond), AsOf: token})
	if err != nil {
		return interval.Element{}, err
	}
	for _, el := range res.Elements {
		if el.Subject == r.w.edges[i] {
			return el, nil
		}
	}
	return interval.Element{}, nil
}

// query draws n queries (an edge, an instant, a window start and a token), reads
// each from every store, and checks, on the first store, that the derived silence
// is a lower bound of the true silence that falls short of it by at most the
// extension interval, and, on every other, that the evidence is the first's.
func (r *run) query(t tb, d drawer, n int) tally {
	t.Helper()
	pol := decay.DefaultPolicy()
	var tl tally
	tl.worlds = 1
	if r.retainedSeq > 0 {
		tl.retained = 1
	}
	lo := uint64(2)
	if r.retainedSeq > 0 {
		lo = r.retainedSeq
	}
	hi := r.stores[0].LastSeq()
	for range n {
		edge := d.Int("edge", 0, len(r.w.edges)-1)
		at := r.times(d)
		if !r.horizon.IsZero() && at.Before(r.horizon) {
			at = r.horizon.Add(time.Duration(d.Int("after", 0, 1)))
		}
		token := uint64(d.Int("token", int(lo), int(hi)+1))
		if token == hi+1 {
			token = store.Latest
		}
		from := at.Add(-pick(d, "from", []time.Duration{0, 1, time.Minute, time.Hour, 12 * time.Hour, 48 * time.Hour}))
		if !r.horizon.IsZero() && from.Before(r.horizon) {
			from = r.horizon
		}
		if r.w.overlap {
			// A record before the window that is not its seed is not read, so a span
			// it attests that reaches past a later record is out of the bound's reach
			// (see TestAnAttestedSpanFromBeforeTheWindowIsOutsideTheBound): these
			// worlds are read from before their first record.
			from = worldBase.Add(-2 * time.Hour)
		}
		tl.queries++
		el, err := r.edgeAt(r.stores[0], edge, from, at, token)
		if err != nil {
			t.Fatalf("edge %d at %s from %s token %d: %v", edge, at.Sub(worldBase), from.Sub(worldBase), token, err)
		}
		for _, st := range r.stores[1:] {
			el2, err := r.edgeAt(st, edge, from, at, token)
			if err != nil {
				t.Fatalf("twin: edge %d at %s from %s token %d: %v", edge, at.Sub(worldBase), from.Sub(worldBase), token, err)
			}
			if !equalEvidence(el.Last, el2.Last) {
				t.Fatalf("edge %d at %s from %s token %d: evidence %+v, its twin's %+v", edge, at.Sub(worldBase), from.Sub(worldBase), token, el.Last, el2.Last)
			}
		}
		if len(el.Last) == 0 {
			continue
		}
		tl.withEvidence++
		for _, e := range el.Last {
			if !r.horizon.IsZero() && e.EventTime.Before(r.horizon) {
				tl.beforeHorizon++
				break
			}
		}
		rec, ok := decay.Of(el.Last, at, pol)
		if !ok {
			t.Fatalf("edge %d at %s: no recency from %+v", edge, at.Sub(worldBase), el.Last)
		}
		seen, err := r.truth(edge, el.Last, at, token)
		if err != nil {
			t.Fatalf("edge %d at %s token %d: %v (evidence %+v)", edge, at.Sub(worldBase), token, err, el.Last)
		}
		trueSilence := time.Duration(1<<63 - 1)
		var maxX time.Duration
		for _, e := range el.Last {
			trueSilence = min(trueSilence, at.Sub(seen[e.Producer]))
			maxX = max(maxX, pol.Extension(e.TTL))
		}
		if rec.SilentAtLeast > trueSilence {
			t.Fatalf("edge %d at %s token %d: the bound %s is above the true silence %s (evidence %+v)", edge, at.Sub(worldBase), token, rec.SilentAtLeast, trueSilence, el.Last)
		}
		if short := trueSilence - rec.SilentAtLeast; short > maxX {
			t.Fatalf("edge %d at %s token %d: the bound %s falls short of the true silence %s by %s, more than X = %s (evidence %+v)",
				edge, at.Sub(worldBase), token, rec.SilentAtLeast, trueSilence, short, maxX, el.Last)
		} else {
			f := float64(short) / float64(maxX)
			tl.worst = max(tl.worst, f)
			if f > 0.99 {
				tl.atX++
			}
		}
		if rec.SilentAtLeast > 0 {
			tl.positive++
			if rec.SilentAtLeast == trueSilence-maxX {
				tl.exact++
			}
		}
	}
	return tl
}

func equalEvidence(a, b []interval.Evidence) bool {
	return slices.EqualFunc(a, b, func(x, y interval.Evidence) bool {
		return x.Producer == y.Producer && x.EventTime.Equal(y.EventTime) && x.Through.Equal(y.Through) && x.TTL == y.TTL && x.Seq == y.Seq && x.Latest.Equal(y.Latest)
	})
}
