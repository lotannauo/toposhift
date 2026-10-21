package interval_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// evidenceOf is what an element's Last must be, from the definitions in the
// oracle: when one of the element's pieces contains the window's last instant,
// for each producer whose reference is live there (sorted), its latest record at
// or before it, with both time axes and the TTL, and the latest last instant of
// the producer's records the call reads: its Observes in the window and its seed,
// the latest record before from if that is an Observe that holds just before
// from. Otherwise nothing.
func (o *oracle) evidenceOf(el interval.Element, from, to time.Time) []interval.Evidence {
	last := to.Add(-time.Nanosecond)
	if el.Quarantine != nil || !pieceContains(el.Pieces, last) {
		return nil
	}
	own := o.bySub[el.Subject]
	_, live := liveAt(own, last)
	in := latest(own, last)
	var out []interval.Evidence
	for _, p := range live {
		r := in[p]
		var read []store.Record
		for _, x := range own {
			if x.Producer == p && x.Kind == lifecycle.Observe && !x.EventTime.Before(from) && x.EventTime.Before(to) {
				read = append(read, x)
			}
		}
		if seed, ok := latest(own, from.Add(-time.Nanosecond))[p]; ok && seed.Kind == lifecycle.Observe {
			if d, has := deadline(seed); !has || from.Add(-time.Nanosecond).Before(d) {
				read = append(read, seed)
			}
		}
		var newest time.Time
		for _, x := range read {
			if l, _ := store.LastOf(x.EventTime, x.Through); l.After(newest) {
				newest = l
			}
		}
		out = append(out, interval.Evidence{
			Producer: p, EventTime: r.EventTime.UTC(), Through: r.Through.UTC(), TTL: r.TTL, Seq: r.Seq,
			Latest: newest.UTC(),
		})
	}
	return out
}

// sameEvidence compares two lists of evidence, telling nil from empty by length
// only.
func sameEvidence(got, want []interval.Evidence) bool {
	return slices.EqualFunc(got, want, func(a, b interval.Evidence) bool {
		return a.Producer == b.Producer && a.EventTime.Equal(b.EventTime) && a.Through.Equal(b.Through) &&
			a.TTL == b.TTL && a.Seq == b.Seq && a.Latest.Equal(b.Latest)
	})
}

// checkEvidence reads every entity and its edges, raw and with the cascade, with
// and without Evidence, at one window and token. It requires the two answers to
// be the same but for Last, to read the same of the store, and Last to be the
// oracle's. It returns how many elements carried evidence.
func checkEvidence(t tb, st store.Store, recs []store.Record, entities []identity.Fingerprint, w interval.Window) int {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	o := newOracle(recs, tok)
	from, to := w.From.UTC(), w.To.UTC()
	carried := 0
	for _, raw := range []bool{false, true} {
		off, on := &spy{Store: st}, &spy{Store: st}
		r0 := interval.NewReader(off, interval.Options{Raw: raw})
		r1 := interval.NewReader(on, interval.Options{Raw: raw, Evidence: true})
		compare := func(what string, a, b []interval.Element) {
			t.Helper()
			if d := diff(b, a); d != "" {
				t.Fatalf("%s with Evidence differs from without it:\n%s", what, d)
			}
			for i := range a {
				if a[i].Last != nil {
					t.Fatalf("%s: Last is set without the option: %+v", what, a[i].Last)
				}
				if want := o.evidenceOf(b[i], from, to); !sameEvidence(b[i].Last, want) {
					t.Fatalf("%s: %s: Last %+v, want %+v", what, describe(b[i]), b[i].Last, want)
				}
				if len(b[i].Last) > 0 {
					carried++
				}
			}
		}
		for _, e := range entities {
			if !raw {
				a, err := r0.Entity(ctx, e, w)
				if err != nil {
					t.Fatal(err)
				}
				b, err := r1.Entity(ctx, e, w)
				if err != nil {
					t.Fatal(err)
				}
				compare("Entity "+e.String(), a.Elements, b.Elements)
			}
			for _, d := range dirs {
				a, err := r0.Edges(ctx, e, d, w)
				if err != nil {
					t.Fatal(err)
				}
				b, err := r1.Edges(ctx, e, d, w)
				if err != nil {
					t.Fatal(err)
				}
				compare("Edges "+e.String()+" "+d.String(), a.Elements, b.Elements)
			}
		}
		if !slices.Equal(off.log, on.log) {
			t.Fatalf("raw=%t: the reads with Evidence are %v, without it %v", raw, on.log, off.log)
		}
	}
	return carried
}

// A reader with Evidence answers as one without it in everything but Last,
// makes the same store reads, and gives the oracle's evidence, on the generated
// streams: runs, confirmations, late records, reboots and clones; at the five
// windows, at the latest token and at half of it.
func TestEvidenceOnTheStreams(t *testing.T) {
	for _, s := range streams() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			st, recs, entities, start, end := openStream(t, s)
			carried := 0
			for _, fw := range windows(start, end) {
				for _, tok := range []uint64{store.Latest, st.LastSeq() / 2} {
					carried += checkEvidence(t, st, recs, entities, interval.Window{From: fw[0], To: fw[1], AsOf: tok})
				}
			}
			t.Logf("%d elements carried evidence", carried)
			if carried == 0 {
				t.Error("no element carried evidence")
			}
		})
	}
}

// The same on the grid generator, whose records land on and one nanosecond off a
// one-second grid, with watch-mode records, deletes and runs, in any write order,
// at any window and token.
func TestEvidenceProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		checkEvidence(t, c.st, c.recs, c.g.entities, c.w)
	})
}

// payloadSpy records the Payloads flag of each WindowSince.
type payloadSpy struct {
	store.Store
	payloads []bool
}

func (s *payloadSpy) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	s.payloads = append(s.payloads, o.Payloads)
	return s.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

// Evidence asks for no payload: only Describe does, whatever else a reader
// asks for. (The reads of an edge's endpoints, for the cascade, ask for none
// either way.)
func TestEvidenceAsksForNoPayload(t *testing.T) {
	st := openStore(t, memstore.Options{})
	a, b := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	sub := edge(a, b, catalog.DependsOn)
	f := newFeed(t, st)
	t0 := gridBase
	f.write(observe(ent(a), "ep", t0, 0), observe(ent(b), "ep", t0, 0), observe(sub, "p", t0, time.Hour))
	w := interval.Window{From: t0.Add(time.Minute), To: t0.Add(2 * time.Minute), AsOf: store.Latest}
	for _, c := range []struct {
		opts interval.Options
		want bool
	}{
		{interval.Options{Evidence: true}, false},
		{interval.Options{Describe: true}, true},
		{interval.Options{Describe: true, Evidence: true}, true},
	} {
		sp := &payloadSpy{Store: st}
		r := interval.NewReader(sp, c.opts)
		if _, err := r.Edges(ctx, a, store.Forward, w); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Entity(ctx, a, w); err != nil {
			t.Fatal(err)
		}
		if len(sp.payloads) == 0 {
			t.Fatalf("%+v: no WindowSince", c.opts)
		}
		if got := slices.Contains(sp.payloads, true); got != c.want {
			t.Fatalf("%+v: a WindowSince with Payloads: %t, want %t (%v)", c.opts, got, c.want, sp.payloads)
		}
	}
}

// evidenceAt reads the edge a -> b forward with Evidence, with the cascade, at
// the window [from, to) and the token.
func evidenceAt(t *testing.T, st store.Store, a, b identity.Fingerprint, from, to time.Time, tok uint64) interval.Element {
	t.Helper()
	res, err := interval.NewReader(st, interval.Options{Evidence: true}).Edges(ctx, a, store.Forward, interval.Window{From: from, To: to, AsOf: tok})
	if err != nil {
		t.Fatal(err)
	}
	for _, el := range res.Elements {
		if el.Subject.B == b {
			return el
		}
	}
	return interval.Element{}
}

func ev(p lifecycle.Producer, at, through time.Time, ttl time.Duration, seq uint64) interval.Evidence {
	last, _ := store.LastOf(at, through)
	return interval.Evidence{Producer: p, EventTime: at, Through: through, TTL: ttl, Seq: seq, Latest: last}
}

// The evidence of an edge is its live producers' deciding records at the
// window's last instant, by event time and then by Seq, at the answer's token:
// a hand-made case with a run extended at its own instant, a producer that has
// lapsed, one whose reference a delete ended, a seed, and a window that ends at
// every critical instant.
func TestEvidenceByHand(t *testing.T) {
	st := openStore(t, memstore.Options{})
	a, b := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	sub := edge(a, b, catalog.DependsOn)
	t0 := gridBase
	m := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	f := newFeed(t, st)
	f.write(observe(ent(a), "ep", t0, 0), observe(ent(b), "ep", t0, 0)) // seq 1, 2
	f.write(observe(sub, "p1", m(0), 10*time.Minute))                   // 3: p1 lapses at 10
	f.write(observe(sub, "p2", m(5), 30*time.Minute))                   // 4
	f.write(run(sub, "p2", m(5), 30*time.Minute, m(20)))                // 5: the same instant, extended
	f.write(observe(sub, "p3", m(6), 60*time.Minute))                   // 6
	f.write(deleted(sub, "p3", m(15)))                                  // 7: p3 ends at 15
	f.write(observe(sub, "p4", m(1), 0))                                // 8: watch mode, live throughout

	cases := []struct {
		name     string
		from, to time.Time
		tok      uint64
		want     []interval.Evidence
	}{
		{"p1 and p4 early", m(0), m(3), store.Latest, []interval.Evidence{
			ev("p1", m(0), time.Time{}, 10*time.Minute, 3), ev("p4", m(1), time.Time{}, 0, 8),
		}},
		{"p2 not yet extended at token 4", m(2), m(8), 4, []interval.Evidence{
			ev("p1", m(0), time.Time{}, 10*time.Minute, 3), ev("p2", m(5), time.Time{}, 30*time.Minute, 4),
		}},
		{"p2 at its extension", m(2), m(8), store.Latest, []interval.Evidence{
			ev("p1", m(0), time.Time{}, 10*time.Minute, 3), ev("p2", m(5), m(20), 30*time.Minute, 5),
			ev("p3", m(6), time.Time{}, time.Hour, 6), ev("p4", m(1), time.Time{}, 0, 8),
		}},
		{"p1 lapsed at 10, p3 deleted at 15", m(12), m(16), store.Latest, []interval.Evidence{
			ev("p2", m(5), m(20), 30*time.Minute, 5), ev("p4", m(1), time.Time{}, 0, 8),
		}},
		{"last instant 14m59.999999999s: p3 still live", m(12), m(15), store.Latest, []interval.Evidence{
			ev("p2", m(5), m(20), 30*time.Minute, 5), ev("p3", m(6), time.Time{}, time.Hour, 6), ev("p4", m(1), time.Time{}, 0, 8),
		}},
		{"seeds only", m(30), m(31), store.Latest, []interval.Evidence{
			ev("p2", m(5), m(20), 30*time.Minute, 5), ev("p4", m(1), time.Time{}, 0, 8),
		}},
		{"every producer lapsed but the watch-mode one", m(51), m(52), store.Latest, []interval.Evidence{
			ev("p4", m(1), time.Time{}, 0, 8),
		}},
	}
	for _, c := range cases {
		el := evidenceAt(t, st, a, b, c.from, c.to, c.tok)
		if !sameEvidence(el.Last, c.want) {
			t.Errorf("%s: Last %+v, want %+v", c.name, el.Last, c.want)
		}
	}
}

// A window whose last instant no piece contains has no evidence, though the
// element has pieces earlier in it; and an edge whose endpoint is gone at the
// last instant is cut there, so it has none either.
func TestEvidenceNeedsAPieceAtTheLastInstant(t *testing.T) {
	st := openStore(t, memstore.Options{})
	a, b := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	sub := edge(a, b, catalog.DependsOn)
	t0 := gridBase
	m := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	f := newFeed(t, st)
	f.write(observe(ent(a), "ep", t0, 0), observe(ent(b), "ep", t0, 0))
	f.write(observe(sub, "p", m(0), 10*time.Minute))
	f.write(deleted(ent(b), "ep", m(20)))
	f.write(observe(sub, "q", m(18), 60*time.Minute))

	if el := evidenceAt(t, st, a, b, m(0), m(30), store.Latest); len(el.Pieces) == 0 || el.Last != nil {
		t.Errorf("the edge is cut at 20 by b: pieces %d, Last %+v; want pieces and no Last", len(el.Pieces), el.Last)
	}
	if el := evidenceAt(t, st, a, b, m(0), m(19), store.Latest); len(el.Last) != 1 || el.Last[0].Producer != "q" {
		t.Errorf("at 18:59.999999999 only q is live: Last %+v", el.Last)
	}
	if el := evidenceAt(t, st, a, b, m(0), m(20), store.Latest); len(el.Last) != 1 {
		t.Errorf("b is alive until 20 exclusive: Last %+v", el.Last)
	}
	if el := evidenceAt(t, st, a, b, m(0), m(20).Add(time.Nanosecond), store.Latest); el.Last != nil {
		t.Errorf("b is gone at 20: Last %+v", el.Last)
	}
	// An entity read has evidence too, from its own producers.
	res, err := interval.NewReader(st, interval.Options{Evidence: true}).Entity(ctx, a, interval.Window{From: m(0), To: m(5), AsOf: store.Latest})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Elements[0].Last; len(got) != 1 || got[0].Producer != "ep" || got[0].TTL != 0 || got[0].Seq != 1 {
		t.Errorf("Entity: Last %+v", got)
	}
}

// Two answers of one batch share no evidence, and a batch gives each fingerprint
// what a single read of it gives.
func TestEvidenceInABatch(t *testing.T) {
	st := openStore(t, memstore.Options{})
	a, b := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	t0 := gridBase
	f := newFeed(t, st)
	f.write(observe(ent(a), "ep", t0, 0), observe(ent(b), "ep", t0, 0), observe(edge(a, b, catalog.DependsOn), "p", t0, time.Hour))
	r := interval.NewReader(st, interval.Options{Evidence: true})
	w := interval.Window{From: t0.Add(time.Minute), To: t0.Add(2 * time.Minute), AsOf: store.Latest}
	_, out, err := r.EdgesBatch(ctx, []identity.Fingerprint{a, b, a}, store.Forward, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(out[0]) != 1 || len(out[0][0].Last) != 1 || len(out[1]) != 0 {
		t.Fatalf("batch: %+v", out)
	}
	out[0][0].Last[0].Producer = "changed"
	if got := out[2][0].Last[0].Producer; got != "p" {
		t.Errorf("two answers share evidence: the second says %q after the first was changed", got)
	}
	_, ents, err := r.Entities(ctx, []identity.Fingerprint{a, b, a}, w)
	if err != nil {
		t.Fatal(err)
	}
	for i, el := range ents {
		if len(el.Last) != 1 || el.Last[0].Producer != "ep" {
			t.Fatalf("Entities[%d]: %+v", i, el.Last)
		}
	}
	ents[0].Last[0].Producer = "changed"
	if ents[2].Last[0].Producer != "ep" {
		t.Error("two entities of one batch share evidence")
	}
}

// Latest is the latest last instant of the producer's records the call reads: a
// record with an earlier event time that attests a span reaching past the
// deciding record's own counts when it is in the window, and does not when it is
// before the window's start and not the seed.
func TestEvidenceLatest(t *testing.T) {
	st := openStore(t, memstore.Options{})
	a, b := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	sub := edge(a, b, catalog.DependsOn)
	t0 := gridBase
	m := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	f := newFeed(t, st)
	f.write(observe(ent(a), "ep", t0, 0), observe(ent(b), "ep", t0, 0)) // 1, 2
	f.write(run(sub, "p", m(0), 6*time.Hour, m(600)))                   // 3: attests 0 through 10h
	f.write(run(sub, "p", m(60), 6*time.Hour, m(120)))                  // 4: replaces it from 1h, through 2h

	whole := evidenceAt(t, st, a, b, m(0), m(300), store.Latest)
	if len(whole.Last) != 1 || whole.Last[0].Seq != 4 || !whole.Last[0].Latest.Equal(m(600)) {
		t.Errorf("both records in the window: %+v", whole.Last)
	}
	seed := evidenceAt(t, st, a, b, m(180), m(300), store.Latest)
	if len(seed.Last) != 1 || seed.Last[0].Seq != 4 || !seed.Last[0].Latest.Equal(m(120)) {
		t.Errorf("only the seed is read: %+v", seed.Last)
	}
	early := evidenceAt(t, st, a, b, m(0), m(300), 3)
	if len(early.Last) != 1 || early.Last[0].Seq != 3 || !early.Last[0].Latest.Equal(m(600)) {
		t.Errorf("at token 3: %+v", early.Last)
	}
}
