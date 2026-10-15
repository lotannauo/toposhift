package blastradius_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
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

func mustImpact(t tb, st store.Store, seed identity.Fingerprint, w interval.Window, o blastradius.Options) blastradius.Result {
	t.Helper()
	res, err := blastradius.NewWalker(st).Impact(ctx, seed, w, o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// entityFingerprints is every entity with a record of its own.
func entityFingerprints(recs []store.Record) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEntity {
			out = append(out, r.Subject.A)
		}
	}
	return sortedSet(out)
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

func oracleFor(t tb, st store.Store, recs []store.Record, w interval.Window) (*oracle, uint64) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	return newOracle(recs, tok, w.From, w.To, quarantines(t, st, entityFingerprints(recs), tok)), tok
}

// checkOracle is R1 and R11: the answer equals the oracle's exactly, the reads
// equal the budget's formula, and nothing is written.
func checkOracle(t tb, st store.Store, or *oracle, tok uint64, seed identity.Fingerprint, w interval.Window, depth int) {
	t.Helper()
	sp := &spy{Store: st}
	got := mustImpact(t, sp, seed, w, blastradius.Options{Policy: wide(depth)})
	want, reads := or.impact(seed, depth)
	want.Token = tok
	if d := sameResult(got, want); d != "" {
		t.Fatalf("seed %s, window [%s, %s), token %d, depth %d, against the oracle: %s", seed, ts(w.From), ts(w.To), tok, depth, d)
	}
	if got.Reads != reads {
		t.Fatalf("seed %s, depth %d: reads %+v, the budget's formula gives %+v", seed, depth, got.Reads, reads)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes", sp.writes)
	}
}

func TestR1R11Oracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		or, tok := oracleFor(t, c.st, c.recs, c.w)
		for _, seed := range c.g.all {
			checkOracle(t, c.st, or, tok, seed, c.w, c.depth)
		}
	})
}

// pointWalk answers the walk from the store's point reads at one instant:
// store.Neighbors in each relation's layer, Alive for both ends, and the
// single-claim rule from the from entity's forward neighbors. It memoizes per
// instant, so the reads are shared by every seed.
type pointWalk struct {
	t     tb
	st    store.Store
	tok   uint64
	x     time.Time
	alive map[identity.Fingerprint][2]bool // ok, quarantined
	nbrs  map[[2]any][]store.Neighbor
}

func newPointWalk(t tb, st store.Store, tok uint64, x time.Time) *pointWalk {
	return &pointWalk{t: t, st: st, tok: tok, x: x, alive: map[identity.Fingerprint][2]bool{}, nbrs: map[[2]any][]store.Neighbor{}}
}

func (p *pointWalk) isAlive(fp identity.Fingerprint) (ok, quarantined bool) {
	if v, seen := p.alive[fp]; seen {
		return v[0], v[1]
	}
	ok, err := p.st.Alive(ctx, fp, p.x, store.Scope{Layer: layerOf(ent(fp)), AsOf: p.tok})
	var q *store.QuarantineError
	switch {
	case errors.As(err, &q):
		ok, quarantined = false, true
	case err != nil:
		p.t.Fatal(err)
	}
	p.alive[fp] = [2]bool{ok, quarantined}
	return ok, quarantined
}

// neighbors is fp's edges alive at x in dir, in every layer a relation can
// store them in.
func (p *pointWalk) neighbors(fp identity.Fingerprint, dir store.Direction) []store.Neighbor {
	k := [2]any{fp, dir}
	if ns, ok := p.nbrs[k]; ok {
		return ns
	}
	var out []store.Neighbor
	for _, l := range layersOf(fp.Type(), dir) {
		ns, err := p.st.Neighbors(ctx, fp, dir, p.x, store.Scope{Layer: l, AsOf: p.tok})
		if err != nil {
			p.t.Fatal(err)
		}
		out = append(out, ns...)
	}
	p.nbrs[k] = out
	return out
}

// follows says the walk at x follows y→target through rel.
func (p *pointWalk) follows(y, target identity.Fingerprint, rel catalog.RelationType) bool {
	if !propagatesRel(rel) {
		return false
	}
	if ok, _ := p.isAlive(y); !ok {
		return false
	}
	if ok, _ := p.isAlive(target); !ok {
		return false
	}
	if !singleClaimRel(rel) {
		return true
	}
	claims := 0
	for _, n := range p.neighbors(y, store.Forward) {
		if n.Relation != rel || n.Peer.Type() != target.Type() {
			continue
		}
		if ok, q := p.isAlive(n.Peer); ok || q {
			claims++
		}
	}
	return claims == 1
}

func (p *pointWalk) members(seed identity.Fingerprint, maxDepth int) []identity.Fingerprint {
	seen := map[identity.Fingerprint]bool{seed: true}
	var out []identity.Fingerprint
	level := []identity.Fingerprint{seed}
	for d := 1; d <= maxDepth && len(level) > 0; d++ {
		var next []identity.Fingerprint
		for _, x := range level {
			for _, n := range p.neighbors(x, store.Reverse) {
				if !seen[n.Peer] && p.follows(n.Peer, x, n.Relation) {
					seen[n.Peer] = true
					out = append(out, n.Peer)
					next = append(next, n.Peer)
				}
			}
		}
		level = next
	}
	return sortedSet(out)
}

// checkPointReads is R2: at every given instant, the members whose reach
// contains it are exactly the point-read walk's.
func checkPointReads(t tb, st store.Store, seeds []identity.Fingerprint, w interval.Window, depth int, crit []time.Time) {
	t.Helper()
	results := map[identity.Fingerprint]blastradius.Result{}
	for _, seed := range seeds {
		results[seed] = mustImpact(t, st, seed, w, blastradius.Options{Policy: wide(depth)})
	}
	for _, x := range crit {
		var p *pointWalk
		for _, seed := range seeds {
			res := results[seed]
			if p == nil {
				p = newPointWalk(t, st, res.Token, x)
			}
			got := sortedSet(entities(blastradius.At(res.Members, x)))
			if want := p.members(seed, depth); !slices.Equal(got, want) {
				t.Fatalf("seed %s at %s, depth %d: members %v, point reads %v", seed, ts(x), depth, got, want)
			}
		}
	}
}

func TestR2PointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		or, _ := oracleFor(t, c.st, c.recs, c.w)
		checkPointReads(t, c.st, c.g.all, c.w, c.depth, or.crit)
	})
}

// testUnion and testSubtract are written here from their definitions, by
// probing every boundary, so the properties share nothing with the package.
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

func reachOf(res blastradius.Result) map[identity.Fingerprint][]interval.Span {
	out := map[identity.Fingerprint][]interval.Span{}
	for _, m := range res.Members {
		out[m.Entity] = m.Reach
	}
	return out
}

// within says every instant of a is in b.
func within(a, b []interval.Span) bool { return len(testSubtract(a, b)) == 0 }

func TestR3ArrivalsAreTheParentsReachIntersectedWithTheLeg(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		r := interval.NewReader(c.st, interval.Options{})
		for _, seed := range c.g.all {
			res := mustImpact(t, c.st, seed, c.w, c.options())
			w := c.w
			w.AsOf = res.Token
			// The reach by chains shorter than the depth is the reach under one
			// hop less.
			shorter := map[identity.Fingerprint][]interval.Span{}
			if c.depth > 1 {
				shorter = reachOf(mustImpact(t, c.st, seed, w, blastradius.Options{Policy: wide(c.depth - 1)}))
			}
			shorter[seed] = []interval.Span{{Start: res.From, End: res.To}}
			for _, m := range res.Members {
				var all [][]interval.Span
				for _, a := range m.Arrivals {
					eff := interval.Spans(a.Leg)
					if singleClaimRel(a.Relation) {
						fwd, err := r.Edges(ctx, m.Entity, store.Forward, w)
						if err != nil {
							t.Fatal(err)
						}
						var others [][]interval.Span
						for _, el := range fwd.Elements {
							if el.Subject != a.Leg.Subject && el.Subject.Relation == a.Relation && el.Subject.B.Type() == a.Parent.Type() {
								others = append(others, interval.Spans(el))
							}
						}
						eff = testSubtract(eff, testUnion(others...))
					}
					if want := interval.Intersect(shorter[a.Parent], eff); !sameSpans(a.Spans, want) {
						t.Fatalf("%s: arrival from %s has %s, the parent's shorter reach with the leg gives %s", describe(m), a.Parent, spansString(a.Spans), spansString(want))
					}
					if a.Leg.Subject != store.EdgeSubject(m.Entity, a.Parent, a.Relation) || !within(a.Spans, interval.Spans(a.Leg)) || len(a.Spans) == 0 {
						t.Fatalf("%s: arrival from %s outside its leg", describe(m), a.Parent)
					}
					all = append(all, a.Spans)
				}
				if !sameSpans(m.Reach, testUnion(all...)) {
					t.Fatalf("%s: reach is not the union of the arrivals", describe(m))
				}
			}
		}
	})
}

func TestR4MonotoneInDepthAndCaps(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		d := uniform(t, "depth", 1, 5)
		for _, seed := range c.g.all {
			lo := mustImpact(t, c.st, seed, c.w, blastradius.Options{Policy: wide(d)})
			hi := mustImpact(t, c.st, seed, c.w, blastradius.Options{Policy: wide(d + 1)})
			loR, hiR := reachOf(lo), reachOf(hi)
			grew := len(hiR) != len(loR)
			for y, r := range loR {
				if !within(r, hiR[y]) {
					t.Fatalf("seed %s: %s's reach %s at depth %d is not inside %s at depth %d", seed, y, spansString(r), d, spansString(hiR[y]), d+1)
				}
				grew = grew || !sameSpans(r, hiR[y])
			}
			if grew && len(lo.Truncated.Depth) == 0 {
				t.Fatalf("seed %s: depth %d+1 reaches more, but depth %d reports no truncation", seed, d, d)
			}

			full := hi
			fullR := hiR
			p := wide(d + 1)
			p.MaxFrontier = uniform(t, "frontier", 1, 3)
			capped := mustImpact(t, c.st, seed, c.w, blastradius.Options{Policy: p})
			lost := len(capped.Members) != len(full.Members)
			for _, m := range capped.Members {
				f := slices.IndexFunc(full.Members, func(x blastradius.Member) bool { return x.Entity == m.Entity })
				if f < 0 || !within(m.Reach, fullR[m.Entity]) || m.Depth < full.Members[f].Depth {
					t.Fatalf("seed %s, MaxFrontier %d: %s is not in the uncapped answer as reached", seed, p.MaxFrontier, describe(m))
				}
				lost = lost || !sameSpans(m.Reach, fullR[m.Entity])
			}
			if lost && len(capped.Truncated.Frontier) == 0 {
				t.Fatalf("seed %s, MaxFrontier %d: members lost and no frontier cut reported", seed, p.MaxFrontier)
			}
			for _, cut := range capped.Truncated.Frontier {
				for _, fp := range cut.Dropped {
					if !slices.Contains(entities(capped.Members), fp) {
						t.Fatalf("seed %s: dropped %s is not a member", seed, fp)
					}
				}
			}

			p = wide(d + 1)
			p.MaxMembers = uniform(t, "members", 1, 5)
			top := mustImpact(t, c.st, seed, c.w, blastradius.Options{Policy: p})
			k := min(p.MaxMembers, len(full.Members))
			if diff := diffLines("MaxMembers", describeAll(top.Members), describeAll(full.Members[:k])); diff != "" || top.Truncated.Members != len(full.Members)-k {
				t.Fatalf("seed %s, MaxMembers %d, %d left out: %s", seed, p.MaxMembers, top.Truncated.Members, diff)
			}
		}
	})
}

func TestR5SplitWindow(t *testing.T) {
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
		for _, seed := range c.g.all {
			whole := reachOf(mustImpact(t, c.st, seed, c.w, c.options()))
			l := reachOf(mustImpact(t, c.st, seed, left, c.options()))
			r := reachOf(mustImpact(t, c.st, seed, right, c.options()))
			keys := map[identity.Fingerprint]bool{}
			for _, mp := range []map[identity.Fingerprint][]interval.Span{whole, l, r} {
				for k := range mp {
					keys[k] = true
				}
			}
			for k := range keys {
				if joined := testUnion(l[k], r[k]); !sameSpans(joined, whole[k]) {
					t.Fatalf("seed %s, %s split at %s: joined %s, whole %s", seed, k, ts(m), spansString(joined), spansString(whole[k]))
				}
			}
		}
	})
}

func TestR6TokenPrefix(t *testing.T) {
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
		for _, seed := range c.g.all {
			sp := &spy{Store: c.st}
			got := mustImpact(t, sp, seed, c.w, c.options())
			if got.Token != s {
				t.Fatalf("token %d, want %d", got.Token, s)
			}
			for _, tok := range sp.tokens {
				if tok != s {
					t.Fatalf("a read at token %d, want %d", tok, s)
				}
			}
			want := mustImpact(t, fresh, seed, latest, c.options())
			want.Token = s
			if d := sameResult(got, want); d != "" {
				t.Fatalf("at token %d against a store of the records up to it: %s", s, d)
			}
		}
	})
}

func TestR7ArrivalOrder(t *testing.T) {
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
		o := blastradius.Options{Policy: wide(uniform(t, "depth", 1, 6))}
		for _, seed := range g.all {
			if d := sameResult(mustImpact(t, a, seed, w, o), mustImpact(t, b, seed, w, o)); d != "" {
				t.Fatalf("two write orders: %s", d)
			}
		}
	})
}

func TestR8RecordsAfterToChangeNothing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		c.w.AsOf = store.Latest
		var before []blastradius.Result
		for _, seed := range c.g.all {
			before = append(before, mustImpact(t, c.st, seed, c.w, blastradius.Options{}))
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
		for i, seed := range c.g.all {
			after := mustImpact(t, c.st, seed, c.w, blastradius.Options{})
			after.Token = before[i].Token
			if d := sameResult(after, before[i]); d != "" {
				t.Fatalf("after records at or after To: %s", d)
			}
		}
	})
}

func TestR9AnInstantIsThePointWalk(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		or, tok := oracleFor(t, c.st, c.recs, c.w)
		x := rapid.SampledFrom(or.crit).Draw(t, "instant")
		w := interval.Window{From: x, To: x.Add(time.Nanosecond), AsOf: tok}
		p := newPointWalk(t, c.st, tok, x)
		for _, seed := range c.g.all {
			res := mustImpact(t, c.st, seed, w, c.options())
			if got, want := sortedSet(entities(res.Members)), p.members(seed, c.depth); !slices.Equal(got, want) {
				t.Fatalf("seed %s over [%s, +1ns): members %v, point reads %v", seed, ts(x), got, want)
			}
			for _, m := range res.Members {
				if len(m.Reach) != 1 || !m.Reach[0].Start.Equal(x) || m.Duration != time.Nanosecond {
					t.Fatalf("seed %s: %s", seed, describe(m))
				}
			}
		}
	})
}

func arrivalSpans(res blastradius.Result, y, parent identity.Fingerprint) []interval.Span {
	for _, m := range res.Members {
		if m.Entity != y {
			continue
		}
		for _, a := range m.Arrivals {
			if a.Parent == parent {
				return a.Spans
			}
		}
	}
	return nil
}

func TestR10AgreesWithCoResidency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		c.w.AsOf = clampToken(c.w.AsOf, c.st.LastSeq())
		pol := coresidency.Policy{MinOverlap: 0, MaxNodes: 1 << 10, MaxHosts: 1 << 10, MaxPerVia: 1 << 20, MaxResults: 1 << 20}
		walks := map[identity.Fingerprint]blastradius.Result{}
		walk := func(fp identity.Fingerprint) blastradius.Result {
			if res, ok := walks[fp]; ok {
				return res
			}
			res := mustImpact(t, c.st, fp, c.w, blastradius.Options{Policy: wide(2)})
			walks[fp] = res
			return res
		}
		for _, p := range c.g.pods {
			res, err := coresidency.NewFinder(c.st).Find(ctx, p, c.w, coresidency.Options{Policy: pol})
			if err != nil {
				t.Fatal(err)
			}
			if slices.ContainsFunc(res.Unattributed, func(u coresidency.Unattributed) bool { return u.Reason == coresidency.SeveralNodes }) {
				continue
			}
			for _, e := range res.CoResidents {
				var want []interval.Span
				switch e.Mechanism {
				case coresidency.SharedNode:
					n := walk(e.Via)
					want = interval.Intersect(arrivalSpans(n, p, e.Via), arrivalSpans(n, e.Peer, e.Via))
				case coresidency.SharedHost:
					h := walk(e.Via)
					n1, n2 := e.Nodes[0], e.Nodes[1]
					via1 := interval.Intersect(arrivalSpans(h, p, n1), arrivalSpans(h, n1, e.Via))
					via2 := interval.Intersect(arrivalSpans(h, e.Peer, n2), arrivalSpans(h, n2, e.Via))
					want = interval.Intersect(via1, via2)
				}
				if !sameSpans(e.Spans(), want) {
					t.Fatalf("pod %s, %s with %s via %s %v: co-residency %s, the walk %s",
						p, e.Mechanism, e.Peer, e.Via, e.Nodes, spansString(e.Spans()), spansString(want))
				}
			}
		}
	})
}

func TestR12EveryTypeIsASeed(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		or, tok := oracleFor(t, c.st, c.recs, c.w)
		n := byte(uniform(t, "seed number", 1, 4))
		for e := range catalog.Default().Entities() {
			seed := mkfp(e.Type(), n)
			res := mustImpact(t, c.st, seed, c.w, c.options())
			if reverseLayers(seed) == 0 {
				if len(res.Members) != 0 || res.Reads.Calls != 1 || res.Reads.Windows != 0 || res.Reads.Alive != 0 {
					t.Fatalf("%s: %d members, reads %+v", e.Type(), len(res.Members), res.Reads)
				}
			}
			checkOracle(t, c.st, or, tok, seed, c.w, c.depth)
		}
	})
}

// TestEdgeLayersFromTheCatalog checks interval.EdgeLayers, which the walk reads
// through, against the oracle's own reading of the catalog.
func TestEdgeLayersFromTheCatalog(t *testing.T) {
	for e := range catalog.Default().Entities() {
		for _, dir := range []store.Direction{store.Forward, store.Reverse} {
			want := layersOf(e.Type(), dir)
			slices.Sort(want)
			if got := interval.EdgeLayers(catalog.Default(), e.Type(), dir); !slices.Equal(got, want) {
				t.Errorf("%s %s: EdgeLayers %v, the catalog %v", e.Type(), dir, got, want)
			}
		}
	}
}

func TestNamesAndDefaults(t *testing.T) {
	if d := blastradius.DefaultPolicy(); d != (blastradius.Policy{MaxDepth: 6, MaxFrontier: 1024, MaxMembers: 4096}) {
		t.Errorf("DefaultPolicy() = %+v", d)
	}
	if blastradius.PolicyVersion != 1 {
		t.Errorf("PolicyVersion %d", blastradius.PolicyVersion)
	}
	got := fmt.Sprintf("%s %v %d %d %d", blastradius.BudgetDate, blastradius.BudgetWorkFactor, blastradius.BudgetReferencePods,
		blastradius.BudgetReferenceExistence, blastradius.BudgetReferenceWindows)
	if got != "2026-10-14 1.5 110 512 256" {
		t.Errorf("the budget is %s", got)
	}
	got = fmt.Sprintf("%s %d %d %d %d", blastradius.BudgetAroundDate, blastradius.BudgetAroundExistence, blastradius.BudgetAroundWindows,
		blastradius.BudgetAroundExpandedExistence, blastradius.BudgetAroundExpandedWindows)
	if got != "2026-10-15 1024 512 2048 1024" {
		t.Errorf("the Around budget is %s", got)
	}
}
