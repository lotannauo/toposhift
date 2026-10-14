package coresidency_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// tb is what both *testing.T and *rapid.T offer.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func mustFind(t tb, st store.Store, seed identity.Fingerprint, w interval.Window, o coresidency.Options) coresidency.Result {
	t.Helper()
	res, err := coresidency.NewFinder(st).Find(ctx, seed, w, o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// quarantines asks the store which entities it quarantines at token.
func quarantines(t tb, st store.Store, fps []identity.Fingerprint, token uint64) map[identity.Fingerprint]*store.QuarantineError {
	t.Helper()
	out := map[identity.Fingerprint]*store.QuarantineError{}
	for _, fp := range fps {
		_, err := st.Alive(ctx, fp, gridBase, store.Scope{Layer: layerOf(ent(fp)), AsOf: token})
		var q *store.QuarantineError
		switch {
		case errors.As(err, &q):
			out[fp] = q
		case err != nil:
			t.Fatal(err)
		}
	}
	return out
}

// checkOracle is C1 and C12: the answer equals the oracle's exactly, the reads
// equal the budget's formula, and nothing is written.
func checkOracle(t tb, st store.Store, recs []store.Record, entities []identity.Fingerprint, seed identity.Fingerprint, w interval.Window, o coresidency.Options) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	or := newOracle(recs, tok, w.From, w.To, quarantines(t, st, entities, tok))
	sp := &spy{Store: st}
	got := mustFind(t, sp, seed, w, o)
	want, reads := or.find(seed, o.NodeOnly)
	want.Token = tok
	if d := sameResult(got, want); d != "" {
		t.Fatalf("seed %s, window [%s, %s), token %d, node only %t, against the oracle: %s", seed, ts(w.From), ts(w.To), tok, o.NodeOnly, d)
	}
	if got.Reads != reads {
		t.Fatalf("seed %s: reads %+v, the budget's formula gives %+v", seed, got.Reads, reads)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes", sp.writes)
	}
}

func TestC1C12Oracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		ents := append(append(slices.Clone(c.g.pods), c.g.nodes...), c.g.hosts...)
		for _, seed := range c.g.pods {
			checkOracle(t, c.st, c.recs, ents, seed, c.w, c.options())
		}
	})
}

type pathKey struct {
	mech  coresidency.Mechanism
	peer  identity.Fingerprint
	nodes string
	via   identity.Fingerprint
}

func keyOf(c coresidency.CoResident) pathKey {
	return pathKey{c.Mechanism, c.Peer, fmt.Sprint(c.Nodes), c.Via}
}

// pointReader answers the definitions from the store's point reads at x.
type pointReader struct {
	t     tb
	st    store.Store
	tok   uint64
	seed  identity.Fingerprint
	nodes []identity.Fingerprint
}

func (p pointReader) alive(fp identity.Fingerprint, x time.Time) (ok, quarantined bool) {
	ok, err := p.st.Alive(ctx, fp, x, store.Scope{Layer: layerOf(ent(fp)), AsOf: p.tok})
	var q *store.QuarantineError
	switch {
	case errors.As(err, &q):
		return false, true
	case err != nil:
		p.t.Fatal(err)
	}
	return ok, false
}

func (p pointReader) neighbors(fp identity.Fingerprint, d store.Direction, x time.Time, l catalog.Layer, rel catalog.RelationType, typ catalog.EntityType) []identity.Fingerprint {
	ns, err := p.st.Neighbors(ctx, fp, d, x, store.Scope{Layer: l, AsOf: p.tok})
	if err != nil {
		p.t.Fatal(err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		if n.Relation == rel && n.Peer.Type() == typ {
			out = append(out, n.Peer)
		}
	}
	return out
}

// nodesOf is the nodes the pod is on at x, with the cascade.
func (p pointReader) nodesOf(pd identity.Fingerprint, x time.Time) []identity.Fingerprint {
	if ok, _ := p.alive(pd, x); !ok {
		return nil
	}
	var out []identity.Fingerprint
	for _, n := range p.neighbors(pd, store.Forward, x, catalog.L2, catalog.ScheduledOn, catalog.K8sNode) {
		if ok, _ := p.alive(n, x); ok {
			out = append(out, n)
		}
	}
	return out
}

func (p pointReader) podsOn(n identity.Fingerprint, x time.Time) []identity.Fingerprint {
	if ok, _ := p.alive(n, x); !ok {
		return nil
	}
	var out []identity.Fingerprint
	for _, pd := range p.neighbors(n, store.Reverse, x, catalog.L2, catalog.ScheduledOn, catalog.K8sPod) {
		if ok, _ := p.alive(pd, x); ok && pd != p.seed {
			out = append(out, pd)
		}
	}
	return out
}

// hostOf is the one host the node is on at x, if it claims exactly one and that
// one is not quarantined.
func (p pointReader) hostOf(n identity.Fingerprint, x time.Time) (identity.Fingerprint, bool) {
	if ok, _ := p.alive(n, x); !ok {
		return identity.Fingerprint{}, false
	}
	var claims []identity.Fingerprint
	quarantined := false
	for _, h := range p.neighbors(n, store.Forward, x, catalog.L1, catalog.RunsOn, catalog.Host) {
		ok, q := p.alive(h, x)
		if ok || q {
			claims = append(claims, h)
			quarantined = quarantined || q
		}
	}
	if len(claims) != 1 || quarantined {
		return identity.Fingerprint{}, false
	}
	return claims[0], true
}

func (p pointReader) at(x time.Time, nodeOnly bool) map[pathKey]bool {
	out := map[pathKey]bool{}
	ns := p.nodesOf(p.seed, x)
	if len(ns) != 1 {
		return out
	}
	n1 := ns[0]
	for _, xp := range p.podsOn(n1, x) {
		out[pathKey{coresidency.SharedNode, xp, fmt.Sprint([]identity.Fingerprint{n1}), n1}] = true
	}
	if nodeOnly {
		return out
	}
	h, ok := p.hostOf(n1, x)
	if !ok {
		return out
	}
	for _, n2 := range p.nodes {
		if h2, ok := p.hostOf(n2, x); n2 == n1 || !ok || h2 != h {
			continue
		}
		for _, xp := range p.podsOn(n2, x) {
			out[pathKey{coresidency.SharedHost, xp, fmt.Sprint([]identity.Fingerprint{n1, n2}), h}] = true
		}
	}
	return out
}

// checkPointReads is C2: at every critical instant, the entries containing it
// are exactly the paths the store's point reads give there.
func checkPointReads(t tb, st store.Store, seed identity.Fingerprint, nodes []identity.Fingerprint, w interval.Window, o coresidency.Options, crit []time.Time) {
	t.Helper()
	res := mustFind(t, st, seed, w, o)
	p := pointReader{t: t, st: st, tok: res.Token, seed: seed, nodes: nodes}
	for _, x := range crit {
		got := map[pathKey]bool{}
		for _, c := range coresidency.At(res.CoResidents, x) {
			got[keyOf(c)] = true
		}
		if want := p.at(x, o.NodeOnly); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("seed %s at %s: entries %v, point reads %v", seed, ts(x), got, want)
		}
	}
}

func TestC2PointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.w.AsOf, c.st.LastSeq())
		crit := newOracle(c.recs, tok, c.w.From, c.w.To, nil).crit(append(append(slices.Clone(c.g.entities), c.g.scheds...), c.g.runs...)...)
		for _, seed := range c.g.pods {
			checkPointReads(t, c.st, seed, c.g.nodes, c.w, c.options(), crit)
		}
	})
}

// testUnion and testSubtract are written here from their definitions, by
// probing every boundary, so the property shares nothing with the package.
func testUnion(lists ...[]interval.Span) []interval.Span {
	return testCombine(func(in []bool) bool { return slices.Contains(in, true) }, lists...)
}

func testSubtract(a, b []interval.Span) []interval.Span {
	return testCombine(func(in []bool) bool { return in[0] && !in[1] }, a, b)
}

func testCombine(f func([]bool) bool, lists ...[]interval.Span) []interval.Span {
	var xs []time.Time
	for _, l := range lists {
		for _, s := range l {
			xs = append(xs, s.Start, s.End)
		}
	}
	slices.SortFunc(xs, func(a, b time.Time) int { return a.Compare(b) })
	xs = slices.CompactFunc(xs, func(a, b time.Time) bool { return a.Equal(b) })
	var out []interval.Span
	for i := 0; i+1 < len(xs); i++ {
		in := make([]bool, len(lists))
		for j, l := range lists {
			for _, s := range l {
				if s.Contains(xs[i]) {
					in[j] = true
				}
			}
		}
		if !f(in) {
			continue
		}
		if n := len(out); n > 0 && out[n-1].End.Equal(xs[i]) {
			out[n-1].End = xs[i+1]
			continue
		}
		out = append(out, interval.Span{Start: xs[i], End: xs[i+1]})
	}
	return out
}

func sameSpans(a, b []interval.Span) bool {
	return slices.EqualFunc(a, b, func(x, y interval.Span) bool { return x.Start.Equal(y.Start) && x.End.Equal(y.End) })
}

func TestC3SpansAreTheIntersectionOfTheLegs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, seed := range c.g.pods {
			res := mustFind(t, c.st, seed, c.w, c.options())
			w := c.w
			w.AsOf = res.Token
			r := interval.NewReader(c.st, interval.Options{})
			seedOn := func(n identity.Fingerprint) []interval.Span {
				var mine []interval.Span
				var others [][]interval.Span
				for _, el := range res.Placement {
					if el.Subject.B == n {
						mine = interval.Spans(el)
					} else {
						others = append(others, interval.Spans(el))
					}
				}
				return testSubtract(mine, testUnion(others...))
			}
			hostOn := func(n, h identity.Fingerprint) []interval.Span {
				out, err := r.Edges(ctx, n, store.Forward, w)
				if err != nil {
					t.Fatal(err)
				}
				var mine []interval.Span
				var others [][]interval.Span
				for _, el := range out.Elements {
					if el.Subject.Relation != catalog.RunsOn {
						continue
					}
					if el.Subject.B == h && el.Uncascaded == nil {
						mine = interval.Spans(el)
					} else {
						others = append(others, interval.Spans(el))
					}
				}
				return testSubtract(mine, testUnion(others...))
			}
			for _, e := range res.CoResidents {
				var legs [][]interval.Span
				if e.Mechanism == coresidency.SharedNode {
					legs = [][]interval.Span{seedOn(e.Nodes[0]), interval.Spans(e.Legs[1])}
				} else {
					legs = [][]interval.Span{seedOn(e.Nodes[0]), hostOn(e.Nodes[0], e.Via), interval.Spans(e.Legs[2]), hostOn(e.Nodes[1], e.Via)}
				}
				if want := interval.Along(legs...); !sameSpans(e.Spans(), want) {
					t.Fatalf("%s: spans %s, Along of the legs %s", describe(e), spansString(e.Spans()), spansString(want))
				}
				for _, o := range e.Overlaps {
					for i, k := range o.Pieces {
						if p := e.Legs[i].Pieces[k]; p.Start.After(o.Start) || p.End.Before(o.End) {
							t.Fatalf("%s: leg %d's piece %d does not contain [%s, %s)", describe(e), i, k, ts(o.Start), ts(o.End))
						}
					}
				}
			}
		}
	})
}

// ambiguous says whether the pod is on two nodes at once at some critical
// instant of the window.
func ambiguous(o *oracle, pd identity.Fingerprint, crit []time.Time) bool {
	for _, x := range crit {
		if len(o.nodesOf(pd, x)) >= 2 {
			return true
		}
	}
	return false
}

func TestC4Symmetry(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.w.AsOf, c.st.LastSeq())
		o := newOracle(c.recs, tok, c.w.From, c.w.To, nil)
		crit := o.crit(c.g.scheds...)
		for _, seed := range c.g.pods {
			if ambiguous(o, seed, crit) {
				continue
			}
			res := mustFind(t, c.st, seed, c.w, c.options())
			for _, e := range res.CoResidents {
				if ambiguous(o, e.Peer, crit) {
					continue
				}
				back := mustFind(t, c.st, e.Peer, c.w, c.options())
				nodes, mapLeg := e.Nodes, []int{1, 0}
				if e.Mechanism == coresidency.SharedHost {
					nodes, mapLeg = []identity.Fingerprint{e.Nodes[1], e.Nodes[0]}, []int{2, 3, 0, 1}
				}
				want := pathKey{e.Mechanism, seed, fmt.Sprint(nodes), e.Via}
				i := slices.IndexFunc(back.CoResidents, func(b coresidency.CoResident) bool { return keyOf(b) == want })
				if i < 0 {
					t.Fatalf("%s has no reverse entry in %v", describe(e), describeAll(back.CoResidents))
				}
				b := back.CoResidents[i]
				if !sameSpans(b.Spans(), e.Spans()) {
					t.Fatalf("reverse spans %s, want %s", spansString(b.Spans()), spansString(e.Spans()))
				}
				for k, ov := range e.Overlaps {
					var mapped []string
					for _, end := range ov.Ends {
						mapped = append(mapped, fmt.Sprint(coresidency.End{Leg: mapLeg[end.Leg], Source: end.Source, Unattributed: end.Unattributed}))
					}
					var got []string
					for _, end := range b.Overlaps[k].Ends {
						got = append(got, fmt.Sprint(end))
					}
					slices.Sort(mapped)
					slices.Sort(got)
					if !slices.Equal(got, mapped) || b.Overlaps[k].Before != ov.Before {
						t.Fatalf("reverse overlap %d: ends %v before %t, want %v before %t", k, got, b.Overlaps[k].Before, mapped, ov.Before)
					}
				}
			}
		}
	})
}

func spansByKey(cs []coresidency.CoResident) map[pathKey][]coresidency.Overlap {
	out := map[pathKey][]coresidency.Overlap{}
	for _, c := range cs {
		out[keyOf(c)] = c.Overlaps
	}
	return out
}

func TestC5SplitWindow(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		mid := c.w.From.Add(c.w.To.Sub(c.w.From) / 2)
		cands := []time.Time{mid}
		for s := -3; s <= 44; s++ {
			for _, d := range []time.Duration{-1, 0, 1} {
				if x := gridBase.Add(time.Duration(s)*time.Second + d); x.After(c.w.From) && x.Before(c.w.To) {
					cands = append(cands, x)
				}
			}
		}
		m := rapid.SampledFrom(cands).Draw(t, "split")
		c.w.AsOf = clampToken(c.w.AsOf, c.st.LastSeq())
		left, right := c.w, c.w
		left.To, right.From = m, m
		for _, seed := range c.g.pods {
			whole := spansByKey(mustFind(t, c.st, seed, c.w, c.options()).CoResidents)
			l := spansByKey(mustFind(t, c.st, seed, left, c.options()).CoResidents)
			r := spansByKey(mustFind(t, c.st, seed, right, c.options()).CoResidents)
			keys := map[pathKey]bool{}
			for _, mp := range []map[pathKey][]coresidency.Overlap{whole, l, r} {
				for k := range mp {
					keys[k] = true
				}
			}
			for k := range keys {
				var joined []interval.Span
				for _, o := range l[k] {
					joined = append(joined, o.Span)
				}
				for i, o := range r[k] {
					if n := len(joined); i == 0 && n > 0 && joined[n-1].End.Equal(m) && len(l[k][len(l[k])-1].Ends) == 0 && o.Start.Equal(m) && o.Before {
						joined[n-1].End = o.End
						continue
					}
					joined = append(joined, o.Span)
				}
				var want []interval.Span
				for _, o := range whole[k] {
					want = append(want, o.Span)
				}
				if !sameSpans(joined, want) {
					t.Fatalf("seed %s %v split at %s: joined %s, whole %s", seed, k, ts(m), spansString(joined), spansString(want))
				}
			}
		}
	})
}

func TestC6TokenPrefix(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		s := clampToken(c.w.AsOf, c.st.LastSeq())
		var prefix []store.Record
		for _, r := range c.recs {
			if r.Seq <= s {
				prefix = append(prefix, r)
			}
		}
		fresh := freshStore(t, memstore.Options{}, prefix)
		defer func() { _ = fresh.Close() }()
		latest := c.w
		latest.AsOf = store.Latest
		for _, seed := range c.g.pods {
			sp := &spy{Store: c.st}
			got := mustFind(t, sp, seed, c.w, c.options())
			if got.Token != s {
				t.Fatalf("token %d, want %d", got.Token, s)
			}
			for _, tok := range sp.tokens {
				if tok != s {
					t.Fatalf("a read at token %d, want %d", tok, s)
				}
			}
			if d := sameResult(got, mustFind(t, fresh, seed, latest, c.options())); d != "" {
				t.Fatalf("at token %d against a store of the records up to it: %s", s, d)
			}
		}
	})
}

func TestC7ArrivalOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		type at struct {
			p   string
			sub store.Subject
			t   int64
		}
		seen := map[at]bool{}
		var recs []store.Record
		for _, r := range drawRecords(t, g, 70) {
			if k := (at{string(r.Producer), r.Subject, r.EventTime.UnixNano()}); !seen[k] {
				seen[k] = true
				recs = append(recs, r)
			}
		}
		from, to := drawWindow(t)
		w := interval.Window{From: from, To: to, AsOf: store.Latest}
		a, b := freshStore(t, memstore.Options{}, nil), freshStore(t, memstore.Options{}, nil)
		writeShuffled(t, a, recs)
		writeShuffled(t, b, recs)
		for _, seed := range g.pods {
			if d := sameResult(mustFind(t, a, seed, w, coresidency.Options{Policy: wide()}), mustFind(t, b, seed, w, coresidency.Options{Policy: wide()})); d != "" {
				t.Fatalf("two write orders: %s", d)
			}
		}
	})
}

func TestC8RecordsAfterToChangeNothing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		c.w.AsOf = store.Latest
		var before []coresidency.Result
		for _, seed := range c.g.pods {
			before = append(before, mustFind(t, c.st, seed, c.w, c.options()))
		}
		shift := c.w.To.Sub(gridBase) + 5*time.Second + time.Nanosecond
		var later []store.Record
		for _, r := range drawRecords(t, c.g, 15) {
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
		for i, seed := range c.g.pods {
			after := mustFind(t, c.st, seed, c.w, c.options())
			after.Token = before[i].Token
			if d := sameResult(after, before[i]); d != "" {
				t.Fatalf("after records at or after To: %s", d)
			}
		}
	})
}

func TestC9NoHostLiftWithoutRunsOn(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		var recs []store.Record
		for _, r := range drawRecords(t, g, 70) {
			if r.Subject.Relation != catalog.RunsOn {
				recs = append(recs, r)
			}
		}
		st := freshStore(t, memstore.Options{}, nil)
		writeShuffled(t, st, recs)
		from, to := drawWindow(t)
		w := interval.Window{From: from, To: to, AsOf: store.Latest}
		for _, seed := range g.pods {
			res := mustFind(t, st, seed, w, coresidency.Options{Policy: wide()})
			for _, e := range res.CoResidents {
				if e.Mechanism == coresidency.SharedHost {
					t.Fatalf("a shared host with no runs_on: %s", describe(e))
				}
			}
			for _, el := range res.Placement {
				var others [][]interval.Span
				for _, o := range res.Placement {
					if o.Subject != el.Subject {
						others = append(others, interval.Spans(o))
					}
				}
				mine := testSubtract(interval.Spans(el), testUnion(others...))
				if len(mine) == 0 {
					continue
				}
				i := slices.IndexFunc(res.Unattributed, func(u coresidency.Unattributed) bool {
					return u.Subject == el.Subject.B && u.Reason == coresidency.NoHost
				})
				if i < 0 || !sameSpans(res.Unattributed[i].Spans, mine) {
					t.Fatalf("node %s: unattributed %v, want NoHost over %s", el.Subject.B, describeUnattributed(res.Unattributed), spansString(mine))
				}
			}
		}
	})
}

func TestC10PolicyMonotonicity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		pol := wide()
		pol.MinOverlap = time.Duration(uniform(t, "min overlap", 0, 6)) * time.Second
		if rapid.Bool().Draw(t, "cap per via") {
			pol.MaxPerVia = uniform(t, "per via", 1, 4)
		}
		if rapid.Bool().Draw(t, "cap results") {
			pol.MaxResults = uniform(t, "results", 1, 4)
		}
		for _, seed := range c.g.pods {
			all := mustFind(t, c.st, seed, c.w, coresidency.Options{NodeOnly: c.nodeOnly, Policy: wide()})
			res := mustFind(t, c.st, seed, c.w, coresidency.Options{NodeOnly: c.nodeOnly, Policy: pol})
			full := map[pathKey]coresidency.CoResident{}
			for _, e := range all.CoResidents {
				full[keyOf(e)] = e
			}
			for _, e := range res.CoResidents {
				if _, ok := full[keyOf(e)]; !ok {
					t.Fatalf("an entry not in the unconstrained answer: %s", describe(e))
				}
			}
			for _, e := range res.Suppressed {
				if f, ok := full[keyOf(e)]; !ok || e.Duration >= pol.MinOverlap || e.Reason != coresidency.BelowMinOverlap || f.Duration != e.Duration {
					t.Fatalf("a suppressed entry %s", describe(e))
				}
			}
			dropped := res.Truncated.Results + res.Truncated.SuppressedDropped
			for _, v := range res.Truncated.PerVia {
				dropped += v.Dropped
			}
			if n := len(res.CoResidents) + len(res.Suppressed) + dropped; n != len(all.CoResidents) {
				t.Fatalf("%d kept + %d suppressed + %d dropped, want %d", len(res.CoResidents), len(res.Suppressed), dropped, len(all.CoResidents))
			}
		}
	})
}

func TestC11ClassesAgreeWithRangeReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, seed := range c.g.pods {
			for _, e := range mustFind(t, c.st, seed, c.w, c.options()).CoResidents {
				peer := e.Legs[len(e.Legs)/2]
				if sameSpans(e.Spans(), interval.Spans(peer)) && (e.Class != peer.Class || e.Transitions != peer.Transitions) {
					t.Fatalf("%s: class %s %d, the peer leg's %s %d", describe(e), e.Class, e.Transitions, peer.Class, peer.Transitions)
				}
			}
		}
	})
}
