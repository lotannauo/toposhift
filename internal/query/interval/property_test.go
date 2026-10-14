package interval_test

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
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

// tb is what both *testing.T and *rapid.T offer.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
}

var dirs = []store.Direction{store.Forward, store.Reverse}

// answers reads every entity, and the edges of every entity in both directions
// raw and with the cascade, at one window and token. It requires every answer to
// name the same token.
func answers(t tb, st store.Store, entities []identity.Fingerprint, w interval.Window) (uint64, map[string][]interval.Element) {
	t.Helper()
	out := map[string][]interval.Element{}
	var token uint64
	seen := false
	note := func(tok uint64) {
		if seen && tok != token {
			t.Fatalf("answers at tokens %d and %d", token, tok)
		}
		token, seen = tok, true
	}
	for _, raw := range []bool{false, true} {
		r := interval.NewReader(st, interval.Options{Raw: raw})
		for _, e := range entities {
			if !raw {
				res, err := r.Entity(ctx, e, w)
				if err != nil {
					t.Fatal(err)
				}
				note(res.Token)
				out["entity "+e.String()] = res.Elements
			}
			for _, d := range dirs {
				res, err := r.Edges(ctx, e, d, w)
				if err != nil {
					t.Fatal(err)
				}
				note(res.Token)
				out[fmt.Sprintf("edges %s %s raw=%t", e, d, raw)] = res.Elements
			}
		}
	}
	return token, out
}

func sameAnswers(t tb, what string, got, want map[string][]interval.Element) {
	t.Helper()
	for k, w := range want {
		if d := diff(got[k], w); d != "" {
			t.Fatalf("%s: %s:\n%s", what, k, d)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d answers, want %d", what, len(got), len(want))
	}
}

// quarantines asks the store which entities it quarantines at token.
func quarantines(t tb, st store.Store, o *oracle, entities []identity.Fingerprint, token uint64) {
	t.Helper()
	for _, e := range entities {
		_, err := st.Alive(ctx, e, gridBase, store.Scope{Layer: layerOf(ent(e)), AsOf: token})
		var q *store.QuarantineError
		switch {
		case errors.As(err, &q):
			o.quarantined[e] = q
		case err != nil:
			t.Fatal(err)
		}
	}
}

// checkOracle is P1: every answer equals the oracle's exactly.
func checkOracle(t tb, st store.Store, recs []store.Record, entities []identity.Fingerprint, w interval.Window) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	o := newOracle(recs, tok)
	quarantines(t, st, o, entities, tok)
	sp := &spy{Store: st}
	got, ans := answers(t, sp, entities, w)
	if got != tok {
		t.Fatalf("token %d, want %d", got, tok)
	}
	from, to := w.From.UTC(), w.To.UTC()
	want := map[string][]interval.Element{}
	for _, e := range entities {
		want["entity "+e.String()] = []interval.Element{o.entity(e, from, to)}
		for _, d := range dirs {
			for _, raw := range []bool{false, true} {
				want[fmt.Sprintf("edges %s %s raw=%t", e, d, raw)] = o.edges(e, d, from, to, raw)
			}
		}
	}
	sameAnswers(t, "against the oracle", ans, want)
	if _, writes := sp.counts(); writes != 0 {
		t.Fatalf("the reader wrote %d times", writes)
	}
}

// link is an edge seen from one end, as Neighbors reports it.
type link = store.Neighbor

func linkOf(el interval.Element, d store.Direction) link {
	if d == store.Forward {
		return link{Peer: el.Subject.B, Relation: el.Subject.Relation}
	}
	return link{Peer: el.Subject.A, Relation: el.Subject.Relation}
}

func alive(t tb, st store.Store, e identity.Fingerprint, x time.Time, tok uint64) (bool, error) {
	t.Helper()
	ok, err := st.Alive(ctx, e, x, store.Scope{Layer: layerOf(ent(e)), AsOf: tok})
	var q *store.QuarantineError
	if err != nil && !errors.As(err, &q) {
		t.Fatal(err)
	}
	return ok, err
}

// neighbors is Neighbors at x merged over the layers of EdgeLayers, sorted as
// Neighbors sorts.
func neighbors(t tb, st store.Store, e identity.Fingerprint, d store.Direction, x time.Time, tok uint64) []store.Neighbor {
	t.Helper()
	var out []store.Neighbor
	for _, l := range interval.EdgeLayers(catalog.Default(), e.Type(), d) {
		ns, err := st.Neighbors(ctx, e, d, x, store.Scope{Layer: l, AsOf: tok})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ns...)
	}
	store.SortNeighbors(out)
	return out
}

// checkPointReads is P2: at every critical instant in the window an edge has a
// piece containing it iff Neighbors lists it (raw), and in addition both its
// endpoints are Alive (cascade); an entity iff Alive. An edge marked
// Uncascaded is compared on its own pieces with Neighbors alone, and a
// quarantined entity by its marker.
func checkPointReads(t tb, st store.Store, entities []identity.Fingerprint, w interval.Window, crit func(identity.Fingerprint) []time.Time) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	raw := interval.NewReader(st, interval.Options{Raw: true})
	cascade := interval.NewReader(st, interval.Options{})
	for _, e := range entities {
		xs := crit(e)
		res, err := cascade.Entity(ctx, e, w)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Elements) != 1 {
			t.Fatalf("Entity(%s) has %d elements", e, len(res.Elements))
		}
		el := res.Elements[0]
		for _, x := range xs {
			ok, aerr := alive(t, st, e, x, tok)
			if aerr != nil {
				if el.Quarantine == nil || el.Quarantine.Error() != aerr.Error() {
					t.Fatalf("Entity(%s) quarantine %v, Alive %v", e, el.Quarantine, aerr)
				}
				continue
			}
			if el.Quarantine != nil {
				t.Fatalf("Entity(%s) quarantined, Alive is not", e)
			}
			if got := len(interval.At(res.Elements, x)) == 1; got != ok {
				t.Fatalf("Entity(%s) at %s: %t, Alive %t", e, ts(x), got, ok)
			}
		}
		for _, d := range dirs {
			rr, err := raw.Edges(ctx, e, d, w)
			if err != nil {
				t.Fatal(err)
			}
			cr, err := cascade.Edges(ctx, e, d, w)
			if err != nil {
				t.Fatal(err)
			}
			for _, x := range xs {
				var wantRaw, wantCascade []link
				for _, n := range neighbors(t, st, e, d, x, tok) {
					wantRaw = append(wantRaw, n)
					a, b := e, n.Peer
					if d == store.Reverse {
						a, b = n.Peer, e
					}
					okA, errA := alive(t, st, a, x, tok)
					okB, errB := alive(t, st, b, x, tok)
					if errA != nil || errB != nil || (okA && okB) {
						// An edge with a quarantined endpoint is judged on its own.
						wantCascade = append(wantCascade, n)
					}
				}
				for _, c := range []struct {
					name string
					els  []interval.Element
					want []link
				}{{"raw", rr.Elements, wantRaw}, {"cascade", cr.Elements, wantCascade}} {
					var got []link
					for _, el := range interval.At(c.els, x) {
						got = append(got, linkOf(el, d))
					}
					if !slices.Equal(got, c.want) {
						t.Fatalf("%s Edges(%s, %s) at %s: %v, point reads %v", c.name, e, d, ts(x), got, c.want)
					}
				}
			}
		}
	}
}

// checkPrefix is P8: the answer at token S equals the answer at the latest
// token of a fresh store fed only the records with Seq <= S, and names S
// clamped to LastSeq.
func checkPrefix(t tb, st store.Store, opts memstore.Options, recs []store.Record, entities []identity.Fingerprint, w interval.Window) {
	t.Helper()
	s := clampToken(w.AsOf, st.LastSeq())
	var prefix []store.Record
	for _, r := range recs {
		if r.Seq <= s {
			prefix = append(prefix, r)
		}
	}
	fresh := freshStore(t, opts, prefix)
	defer func() { _ = fresh.Close() }()
	tok, got := answers(t, st, entities, w)
	if tok != s {
		t.Fatalf("token %d, want %d", tok, s)
	}
	w.AsOf = store.Latest
	_, want := answers(t, fresh, entities, w)
	sameAnswers(t, fmt.Sprintf("at token %d against a store of the records up to it", s), got, want)
}

// gridCase is one drawn store and window.
type gridCase struct {
	g    grid
	st   *memstore.Store
	recs []store.Record
	w    interval.Window
}

func drawCase(t *rapid.T) gridCase {
	g := newGrid()
	st := freshStore(t, memstore.Options{}, nil)
	recs := writeShuffled(t, st, drawRecords(t, g, 60))
	from, to := drawWindow(t)
	return gridCase{g: g, st: st, recs: recs, w: interval.Window{From: from, To: to, AsOf: drawToken(t, st.LastSeq())}}
}

func TestP1Oracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		checkOracle(t, c.st, c.recs, c.g.entities, c.w)
	})
}

func TestP2PointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		crit := newOracle(c.recs, clampToken(c.w.AsOf, c.st.LastSeq())).allCritical(c.w.From, c.w.To)
		checkPointReads(t, c.st, c.g.entities, c.w, func(identity.Fingerprint) []time.Time { return crit })
	})
}

func TestP3InstantEqualsNeighbors(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.w.AsOf, c.st.LastSeq())
		xs := []time.Time{drawInstant(t, "instant", -2, 44, true)}
		for _, r := range c.recs {
			xs = append(xs, r.EventTime)
		}
		r := interval.NewReader(c.st, interval.Options{Raw: true})
		for _, x := range xs {
			for _, e := range c.g.entities {
				for _, d := range dirs {
					res, err := r.Edges(ctx, e, d, interval.Window{From: x, To: x.Add(time.Nanosecond), AsOf: c.w.AsOf})
					if err != nil {
						t.Fatal(err)
					}
					var got []link
					for _, el := range res.Elements {
						got = append(got, linkOf(el, d))
					}
					if want := neighbors(t, c.st, e, d, x, tok); !slices.Equal(got, want) {
						t.Fatalf("Edges(%s, %s) over [%s, +1ns): %v, Neighbors %v", e, d, ts(x), got, want)
					}
				}
			}
		}
	})
}

func TestP4Views(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		crit := newOracle(c.recs, clampToken(c.w.AsOf, c.st.LastSeq())).allCritical(c.w.From, c.w.To)
		_, ans := answers(t, c.st, c.g.entities, c.w)
		for k, els := range ans {
			union, inter := keys(interval.Union(els)), keys(interval.Intersection(els))
			always := map[store.Subject]bool{}
			for _, el := range els {
				always[el.Subject] = true
			}
			for _, x := range crit {
				at := keys(interval.At(els, x))
				for s := range inter {
					if !at[s] {
						t.Fatalf("%s: %v in Intersection, not At(%s)", k, s, ts(x))
					}
				}
				for s := range at {
					if !union[s] {
						t.Fatalf("%s: %v in At(%s), not Union", k, s, ts(x))
					}
				}
				for _, el := range els {
					if el.Quarantine == nil && !at[el.Subject] {
						always[el.Subject] = false
					}
				}
			}
			for s, ok := range always {
				if ok != inter[s] {
					t.Fatalf("%s: %v alive at every critical instant %t, in Intersection %t", k, s, ok, inter[s])
				}
			}
		}
	})
}

// join joins the pieces of two adjacent windows where one ends at m with no
// source and the next starts at m having begun before.
func join(left, right []interval.Piece, m time.Time) []interval.Piece {
	out := slices.Clone(left)
	for i, p := range right {
		if i == 0 && len(out) > 0 {
			last := &out[len(out)-1]
			if last.End.Equal(m) && last.EndSource == lifecycle.EndUnknown && p.Start.Equal(m) && p.Before {
				last.End, last.EndSource = p.End, p.EndSource
				ps := append(slices.Clone(last.Producers), p.Producers...)
				slices.Sort(ps)
				last.Producers = slices.Compact(ps)
				continue
			}
		}
		out = append(out, p)
	}
	return out
}

func piecesBySubject(els []interval.Element) map[store.Subject][]interval.Piece {
	out := map[store.Subject][]interval.Piece{}
	for _, el := range els {
		out[el.Subject] = el.Pieces
	}
	return out
}

func TestP5SplitWindow(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		cands := []time.Time{c.w.From.Add(c.w.To.Sub(c.w.From) / 2)}
		for s := -3; s <= 44; s++ {
			for _, d := range []time.Duration{-1, 0, 1} {
				x := gridBase.Add(time.Duration(s)*time.Second + d)
				if x.After(c.w.From) && x.Before(c.w.To) {
					cands = append(cands, x)
				}
			}
		}
		m := rapid.SampledFrom(cands).Draw(t, "split")
		c.w.AsOf = clampToken(c.w.AsOf, c.st.LastSeq())
		left, right := c.w, c.w
		left.To, right.From = m, m
		_, whole := answers(t, c.st, c.g.entities, c.w)
		_, l := answers(t, c.st, c.g.entities, left)
		_, r := answers(t, c.st, c.g.entities, right)
		for k, els := range whole {
			want := piecesBySubject(els)
			lp, rp := piecesBySubject(l[k]), piecesBySubject(r[k])
			// Every subject of a half must be in the whole.
			for _, half := range []map[store.Subject][]interval.Piece{lp, rp} {
				for s := range half {
					if _, ok := want[s]; !ok {
						want[s] = nil
					}
				}
			}
			for s, ps := range want {
				got := join(lp[s], rp[s], m)
				// A piece of the whole that ends exactly at m ends at the left
				// half's To, which reports no source: nothing after To is known.
				ps = slices.Clone(ps)
				for i := range ps {
					if ps[i].End.Equal(m) {
						if ps[i].EndSource == lifecycle.EndUnknown {
							t.Fatalf("%s: a piece ends inside the window with no source", k)
						}
						ps[i].EndSource = lifecycle.EndUnknown
					}
				}
				g, w := describe(interval.Element{Subject: s, Pieces: got}), describe(interval.Element{Subject: s, Pieces: ps})
				if len(got) == 0 && len(ps) == 0 {
					continue
				}
				if g != w {
					t.Fatalf("%s split at %s:\n got  %s\n want %s", k, ts(m), g, w)
				}
			}
		}
	})
}

// unionJoin is the union of span lists, spans that touch joined.
func unionJoin(lists ...[]interval.Span) []interval.Span {
	var all []interval.Span
	for _, l := range lists {
		all = append(all, l...)
	}
	slices.SortFunc(all, func(a, b interval.Span) int { return a.Start.Compare(b.Start) })
	var out []interval.Span
	for _, s := range all {
		if n := len(out); n > 0 && !s.Start.After(out[n-1].End) {
			if s.End.After(out[n-1].End) {
				out[n-1].End = s.End
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

func overlaps(ss []interval.Span, p interval.Span) bool {
	for _, s := range ss {
		if s.Start.Before(p.End) && p.Start.Before(s.End) {
			return true
		}
	}
	return false
}

func TestP6ProducerUnion(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		c.w.AsOf = store.Latest
		_, all := answers(t, c.st, c.g.entities, c.w)
		per := map[lifecycle.Producer]map[string][]interval.Element{}
		for _, p := range gridProducers {
			var mine []store.Record
			for _, r := range c.recs {
				if r.Producer == p {
					mine = append(mine, r)
				}
			}
			st := freshStore(t, memstore.Options{}, mine)
			_, per[p] = answers(t, st, c.g.entities, c.w)
			_ = st.Close()
		}
		for k, els := range all {
			if !isRaw(k) {
				continue
			}
			spansOf := func(p lifecycle.Producer) map[store.Subject][]interval.Span {
				out := map[store.Subject][]interval.Span{}
				for _, el := range per[p][k] {
					out[el.Subject] = interval.Spans(el)
				}
				return out
			}
			bySub := map[lifecycle.Producer]map[store.Subject][]interval.Span{}
			for _, p := range gridProducers {
				bySub[p] = spansOf(p)
			}
			seen := map[store.Subject]bool{}
			for _, el := range els {
				seen[el.Subject] = true
				var lists [][]interval.Span
				for _, p := range gridProducers {
					lists = append(lists, bySub[p][el.Subject])
				}
				if g, w := fmt.Sprint(interval.Spans(el)), fmt.Sprint(unionJoin(lists...)); g != w {
					t.Fatalf("%s %v: spans %s, union of producers %s", k, el.Subject, g, w)
				}
				for _, pc := range el.Pieces {
					for _, p := range gridProducers {
						if listed, ov := slices.Contains(pc.Producers, p), overlaps(bySub[p][el.Subject], pc.Span); listed != ov {
							t.Fatalf("%s %v piece %v: %s listed %t, its spans overlap %t", k, el.Subject, pc.Span, p, listed, ov)
						}
					}
				}
			}
			for _, p := range gridProducers {
				for s, ss := range bySub[p] {
					if len(ss) > 0 && !seen[s] {
						t.Fatalf("%s: %v has spans of %s and none in the union", k, s, p)
					}
				}
			}
		}
	})
}

// isRaw says whether an answer of answers is an entity or a raw edge read.
func isRaw(k string) bool {
	return strings.HasPrefix(k, "entity ") || strings.HasSuffix(k, "raw=true")
}

func TestP7ArrivalOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		type at struct {
			p   lifecycle.Producer
			sub store.Subject
			t   int64
		}
		seen := map[at]bool{}
		var recs []store.Record
		for _, r := range drawRecords(t, g, 60) {
			k := at{r.Producer, r.Subject, r.EventTime.UnixNano()}
			if !seen[k] {
				seen[k] = true
				recs = append(recs, r)
			}
		}
		from, to := drawWindow(t)
		w := interval.Window{From: from, To: to, AsOf: store.Latest}
		a, b := freshStore(t, memstore.Options{}, nil), freshStore(t, memstore.Options{}, nil)
		writeShuffled(t, a, recs)
		writeShuffled(t, b, recs)
		_, x := answers(t, a, g.entities, w)
		_, y := answers(t, b, g.entities, w)
		sameAnswers(t, "two write orders", x, y)
	})
}

func TestP8TokenPrefix(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		checkPrefix(t, c.st, memstore.Options{}, c.recs, c.g.entities, c.w)
	})
}

func TestP9RecordsAfterToChangeNothing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		c.w.AsOf = store.Latest
		_, before := answers(t, c.st, c.g.entities, c.w)
		shift := c.w.To.Sub(gridBase) + time.Nanosecond
		var later []store.Record
		for _, r := range drawRecords(t, c.g, 10) {
			r.EventTime = r.EventTime.Add(shift)
			if !r.Through.IsZero() {
				r.Through = r.Through.Add(shift)
			}
			if r.EventTime.Before(c.w.To) {
				t.Fatalf("a later record at %s", ts(r.EventTime))
			}
			later = append(later, r)
		}
		writeShuffled(t, c.st, later)
		_, after := answers(t, c.st, c.g.entities, c.w)
		sameAnswers(t, "after records at or after To", after, before)
	})
}

// drawSpans is a sorted list of disjoint, non-touching spans on a grid of
// nanoseconds in [0, 40].
func drawSpans(t *rapid.T, label string) []interval.Span {
	vs := rapid.SliceOfDistinct(rapid.IntRange(0, 40), func(v int) int { return v }).Draw(t, label)
	slices.Sort(vs)
	var out []interval.Span
	for i := 0; i+1 < len(vs); i += 2 {
		out = append(out, interval.Span{Start: gridBase.Add(time.Duration(vs[i])), End: gridBase.Add(time.Duration(vs[i+1]))})
	}
	return out
}

func in(ss []interval.Span, x time.Time) bool {
	for _, s := range ss {
		if s.Contains(x) {
			return true
		}
	}
	return false
}

func sameSpans(a, b []interval.Span) bool {
	return slices.EqualFunc(a, b, func(x, y interval.Span) bool { return x.Start.Equal(y.Start) && x.End.Equal(y.End) })
}

func TestP10Intersect(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, b, c := drawSpans(t, "a"), drawSpans(t, "b"), drawSpans(t, "c")
		ab := interval.Intersect(a, b)
		for i, s := range ab {
			if !s.Start.Before(s.End) || (i > 0 && !ab[i-1].End.Before(s.Start)) {
				t.Fatalf("Intersect is not sorted, disjoint and non-touching: %v", ab)
			}
		}
		for v := -1; v <= 41; v++ {
			x := gridBase.Add(time.Duration(v))
			if in(ab, x) != (in(a, x) && in(b, x)) {
				t.Fatalf("Intersect(%v, %v) = %v at %d", a, b, ab, v)
			}
		}
		if !sameSpans(ab, interval.Intersect(b, a)) {
			t.Fatal("not commutative")
		}
		abc := interval.Intersect(ab, c)
		if !sameSpans(abc, interval.Intersect(a, interval.Intersect(b, c))) || !sameSpans(abc, interval.Along(a, b, c)) {
			t.Fatal("not associative, or Along differs")
		}
		if !sameSpans(interval.Intersect(a, a), a) {
			t.Fatal("Intersect(a, a) is not a")
		}
		if d := interval.Duration(ab); d > min(interval.Duration(a), interval.Duration(b)) {
			t.Fatalf("Duration %s above both", d)
		}
		for _, s := range a {
			if got := interval.Intersect([]interval.Span{s}, []interval.Span{{Start: s.End, End: s.End.Add(time.Nanosecond)}}); len(got) != 0 {
				t.Fatalf("touching spans overlap: %v", got)
			}
		}
		one := interval.Along(a)
		if !sameSpans(one, a) {
			t.Fatal("Along of one leg is not the leg")
		}
		if len(one) > 0 {
			one[0].Start = one[0].Start.Add(-time.Hour)
			if a[0].Start.Equal(one[0].Start) {
				t.Fatal("Along of one leg is not a copy")
			}
		}
		if interval.Along() != nil {
			t.Fatal("Along of no legs is not nil")
		}
	})
}

func TestP12EdgeLayers(t *testing.T) {
	c := catalog.Default()
	for e := range c.Entities() {
		for _, d := range dirs {
			var want []catalog.Layer
			for rel := range c.Relations() {
				if rel.Derived() {
					continue
				}
				for other := range c.Entities() {
					from, to := e.Type(), other.Type()
					if d == store.Reverse {
						from, to = to, from
					}
					if rel.Allows(from, to) && !slices.Contains(want, rel.Layer()) {
						want = append(want, rel.Layer())
					}
				}
			}
			slices.SortFunc(want, cmp.Compare)
			if got := interval.EdgeLayers(c, e.Type(), d); !slices.Equal(got, want) {
				t.Errorf("EdgeLayers(%s, %s) = %v, want %v", e.Type(), d, got, want)
			}
		}
	}
	if got := interval.EdgeLayers(c, catalog.K8sNode, 0); got != nil {
		t.Errorf("EdgeLayers with direction 0 = %v", got)
	}
	for _, tc := range []struct {
		typ  catalog.EntityType
		dir  store.Direction
		want []catalog.Layer
	}{
		{catalog.K8sNode, store.Forward, []catalog.Layer{catalog.L1}},
		{catalog.K8sNode, store.Reverse, []catalog.Layer{catalog.L2}},
		{catalog.Host, store.Forward, []catalog.Layer{catalog.L1}},
		{catalog.Host, store.Reverse, []catalog.Layer{catalog.L1}},
		{catalog.Service, store.Reverse, []catalog.Layer{catalog.L2, catalog.L3}},
		{catalog.Port, store.Forward, nil},
	} {
		if got := interval.EdgeLayers(c, tc.typ, tc.dir); !slices.Equal(got, tc.want) {
			t.Errorf("EdgeLayers(%s, %s) = %v, want %v", tc.typ, tc.dir, got, tc.want)
		}
	}
}
