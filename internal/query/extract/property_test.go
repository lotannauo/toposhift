package extract_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
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

func mustExtract(t tb, st store.Store, seed extract.Seed, w interval.Window, o extract.Options) extract.Result {
	t.Helper()
	res, err := extract.NewExtractor(st).Extract(ctx, seed, w, o)
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

// sameResult returns "" when two extractions agree on everything but Reads and
// the order of contributions, and otherwise the first difference.
func sameResult(got, want extract.Result) string {
	if got.Token != want.Token || !got.From.Equal(want.From) || !got.To.Equal(want.To) || got.Anchor != want.Anchor ||
		got.PolicyVersion != want.PolicyVersion || got.CoResidencyPolicy != want.CoResidencyPolicy || got.IntervalPolicy != want.IntervalPolicy {
		return fmt.Sprintf("header %d %s %s %s, want %d %s %s %s", got.Token, ts(got.From), ts(got.To), got.Anchor, want.Token, ts(want.From), ts(want.To), want.Anchor)
	}
	if len(got.PodResults) != len(want.PodResults) {
		return fmt.Sprintf("finds %v, want %v", seedsOf(got), seedsOf(want))
	}
	for i := range got.PodResults {
		g, w := got.PodResults[i], want.PodResults[i]
		if g.Seed != w.Seed || g.Token != w.Token {
			return fmt.Sprintf("finds %v, want %v", seedsOf(got), seedsOf(want))
		}
		if d := diffLines(fmt.Sprintf("find %s", g.Seed), describeEntries(g.CoResidents), describeEntries(w.CoResidents)); d != "" {
			return d
		}
	}
	for _, d := range []string{
		diffLines("members", describeMembers(got.Members), describeMembers(want.Members)),
		diffLines("pairs", describePairs(got.Pairs, true), describePairs(want.Pairs, true)),
		diffLines("suppressed", describePairs(got.Suppressed, true), describePairs(want.Suppressed, true)),
		diffLines("unmapped", describeUnmapped(got.Unmapped), describeUnmapped(want.Unmapped)),
		diffLines("unattributed", describeUnattributed(got.Unattributed), describeUnattributed(want.Unattributed)),
	} {
		if d != "" {
			return d
		}
	}
	if got.Ubiquity != want.Ubiquity {
		return fmt.Sprintf("ubiquity %+v, want %+v", got.Ubiquity, want.Ubiquity)
	}
	if fmt.Sprint(got.Truncated) != fmt.Sprint(want.Truncated) {
		return fmt.Sprintf("truncated %+v, want %+v", got.Truncated, want.Truncated)
	}
	return ""
}

// checkOracle is Q1 and Q9: the answer equals the oracle's exactly, the reads
// equal the budget's formula, and nothing is written.
func checkOracle(t tb, st store.Store, recs []store.Record, entities []identity.Fingerprint, seed extract.Seed, w interval.Window, o extract.Options) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	or := newOracle(recs, tok, w.From, w.To, quarantines(t, st, entities, tok))
	sp := &spy{Store: st}
	got := mustExtract(t, sp, seed, w, o)
	want, reads := or.extract(seed, o.NodeOnly, o.Policy)
	want.Token = tok
	for i := range want.PodResults {
		want.PodResults[i].Token = tok
	}
	if d := sameResult(got, want); d != "" {
		t.Fatalf("seed %+v, window [%s, %s), token %d, node only %t, against the oracle: %s", seed, ts(w.From), ts(w.To), tok, o.NodeOnly, d)
	}
	if got.Reads != reads {
		t.Fatalf("seed %+v: reads %+v, the budget's formula gives %+v", seed, got.Reads, reads)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes", sp.writes)
	}
}

func TestQ1Q9Oracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, s := range c.seeds() {
			checkOracle(t, c.st, c.recs, c.g.all(), s.seed, c.w, s.o)
		}
	})
}

// pairKey names a pair.
type pairKey struct {
	member, peer identity.Fingerprint
	mech         extract.Mechanism
}

func keyOf(p extract.Pair) pairKey { return pairKey{p.Member, p.Peer, p.Mechanism} }

// pointReader answers the definitions from the store's point reads at x.
type pointReader struct {
	t   tb
	st  store.Store
	tok uint64
}

func (p pointReader) alive(fp identity.Fingerprint, x time.Time) bool {
	ok, err := p.st.Alive(ctx, fp, x, store.Scope{Layer: layerOf(ent(fp)), AsOf: p.tok})
	if err != nil {
		p.t.Fatal(err)
	}
	return ok
}

// neighbors is the peers of fp by rel at x whose own existence holds, with fp's.
func (p pointReader) neighbors(fp identity.Fingerprint, d store.Direction, x time.Time, rel catalog.RelationType, typ catalog.EntityType) []identity.Fingerprint {
	if !p.alive(fp, x) {
		return nil
	}
	r, _ := catalog.Default().Relation(rel)
	ns, err := p.st.Neighbors(ctx, fp, d, x, store.Scope{Layer: r.Layer(), AsOf: p.tok})
	if err != nil {
		p.t.Fatal(err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		if n.Relation == rel && n.Peer.Type() == typ && p.alive(n.Peer, x) {
			out = append(out, n.Peer)
		}
	}
	return out
}

// services is the service each instance of pod pd is mapped singly to at x.
func (p pointReader) services(pd identity.Fingerprint, x time.Time) map[identity.Fingerprint]identity.Fingerprint {
	out := map[identity.Fingerprint]identity.Fingerprint{}
	for _, i := range p.neighbors(pd, store.Reverse, x, catalog.PartOf, catalog.ServiceInstance) {
		pods := p.neighbors(i, store.Forward, x, catalog.PartOf, catalog.K8sPod)
		svcs := p.neighbors(i, store.Forward, x, catalog.InstanceOf, catalog.Service)
		if only(pods, pd) && len(svcs) == 1 {
			out[i] = svcs[0]
		}
	}
	return out
}

// hostOf is the one host node n is on at x, if it claims exactly one live host.
func (p pointReader) hostOf(n identity.Fingerprint, x time.Time) (identity.Fingerprint, bool) {
	hs := p.neighbors(n, store.Forward, x, catalog.RunsOn, catalog.Host)
	if len(hs) != 1 {
		return identity.Fingerprint{}, false
	}
	return hs[0], true
}

// pairsAt is every pair at x from the seed pods, by the definitions over the
// store's point reads: two instances of the seed pod mapped singly to a member
// and another service (shared pod); the seed pod alone on a node, and a pod on
// that node (shared node) or on another node on the same one host (shared
// host), with an instance mapped singly to another service.
func (p pointReader) pairsAt(seedPods, nodes []identity.Fingerprint, members map[identity.Fingerprint]bool, nodeOnly bool, x time.Time) map[pairKey]bool {
	out := map[pairKey]bool{}
	add := func(mech extract.Mechanism, mine, theirs map[identity.Fingerprint]identity.Fingerprint, samePod bool) {
		for i, m := range mine {
			if !members[m] {
				continue
			}
			for j, u := range theirs {
				if u == m || (samePod && i == j) {
					continue
				}
				a, b := m, u
				if members[b] && store.CompareFingerprints(b, a) < 0 {
					a, b = b, a
				}
				out[pairKey{a, b, mech}] = true
			}
		}
	}
	for _, sp := range seedPods {
		mine := p.services(sp, x)
		add(extract.SharedPod, mine, mine, true)
		ns := p.neighbors(sp, store.Forward, x, catalog.ScheduledOn, catalog.K8sNode)
		if len(ns) != 1 {
			continue
		}
		n1 := ns[0]
		for _, xp := range p.neighbors(n1, store.Reverse, x, catalog.ScheduledOn, catalog.K8sPod) {
			if xp != sp {
				add(extract.SharedNode, mine, p.services(xp, x), false)
			}
		}
		if nodeOnly {
			continue
		}
		h, ok := p.hostOf(n1, x)
		if !ok {
			continue
		}
		for _, n2 := range nodes {
			if h2, ok := p.hostOf(n2, x); n2 == n1 || !ok || h2 != h {
				continue
			}
			for _, xp := range p.neighbors(n2, store.Reverse, x, catalog.ScheduledOn, catalog.K8sPod) {
				if xp != sp {
					add(extract.SharedHost, mine, p.services(xp, x), false)
				}
			}
		}
	}
	return out
}

// seedPods is the result's seed pods: every Find of an exploratory seed, and
// the confirmed Finds of an anchored one.
func seedPods(res extract.Result, seed extract.Seed) []identity.Fingerprint {
	a, ok := seed.(extract.Anchored)
	if !ok {
		return seedsOf(res)
	}
	if res.Anchor != extract.OnHost {
		return nil
	}
	var out []identity.Fingerprint
	for _, fr := range res.PodResults {
		if slices.ContainsFunc(fr.SeedHosts, func(h coresidency.HostSpans) bool {
			return h.Host == a.Host && slices.ContainsFunc(h.Spans, func(s interval.Span) bool { return s.Contains(a.At) })
		}) {
			out = append(out, fr.Seed)
		}
	}
	return out
}

// checkPointReads is Q2: at every critical instant, the pairs, suppressed ones
// included, containing it are exactly those the store's point reads give
// there, for every mechanism.
func checkPointReads(t tb, st store.Store, seed extract.Seed, nodes []identity.Fingerprint, w interval.Window, o extract.Options, crit []time.Time) {
	t.Helper()
	res := mustExtract(t, st, seed, w, o)
	p := pointReader{t: t, st: st, tok: res.Token}
	members := map[identity.Fingerprint]bool{}
	for _, m := range res.Members {
		members[m.Service] = true
	}
	sps := seedPods(res, seed)
	for _, x := range crit {
		got := map[pairKey]bool{}
		for _, pr := range slices.Concat(res.Pairs, res.Suppressed) {
			if slices.ContainsFunc(pr.Spans, func(s extract.Span) bool { return s.Contains(x) }) {
				got[keyOf(pr)] = true
			}
		}
		if want := p.pairsAt(sps, nodes, members, o.NodeOnly, x); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("seed %+v at %s: pairs %v, point reads %v", seed, ts(x), got, want)
		}
	}
}

func TestQ2PointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.w.AsOf, c.st.LastSeq())
		crit := newOracle(c.recs, tok, c.w.From, c.w.To, nil).all
		for _, s := range c.seeds() {
			checkPointReads(t, c.st, s.seed, c.g.nodes, c.w, s.o, crit)
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

// TestQ3SpansAreTheUnionOfAlongOfTheLegs reads each instance's claims afresh
// and checks every contribution, every via and every pair span.
func TestQ3SpansAreTheUnionOfAlongOfTheLegs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, s := range c.seeds() {
			res := mustExtract(t, c.st, s.seed, c.w, s.o)
			w := c.w
			w.AsOf = res.Token
			r := interval.NewReader(c.st, interval.Options{})
			// alone is the instants instance i claims target and nothing else
			// of rel.
			alone := func(i, target identity.Fingerprint, rel catalog.RelationType) []interval.Span {
				out, err := r.Edges(ctx, i, store.Forward, w)
				if err != nil {
					t.Fatal(err)
				}
				var mine []interval.Span
				var others [][]interval.Span
				for _, el := range out.Elements {
					if el.Subject.Relation != rel {
						continue
					}
					if el.Subject.B == target {
						mine = interval.Spans(el)
					} else {
						others = append(others, interval.Spans(el))
					}
				}
				return testSubtract(mine, testUnion(others...))
			}
			finds := map[identity.Fingerprint]coresidency.Result{}
			for _, fr := range res.PodResults {
				finds[fr.Seed] = fr
			}
			for _, pr := range slices.Concat(res.Pairs, res.Suppressed) {
				var all [][]interval.Span
				vias := map[identity.Fingerprint][][]interval.Span{}
				for _, k := range pr.Contributions {
					legs := [][]interval.Span{
						alone(k.MemberInst, k.Legs[0].Subject.B, catalog.InstanceOf), alone(k.MemberInst, k.MemberPod, catalog.PartOf),
						alone(k.PeerInst, k.PeerPod, catalog.PartOf), alone(k.PeerInst, k.Legs[3].Subject.B, catalog.InstanceOf),
					}
					if k.Entry >= 0 {
						e := finds[k.MemberPod].CoResidents[k.Entry]
						if e.Peer != k.PeerPod || e.Via != k.Via {
							t.Fatalf("contribution %s names entry %d, %s via %s", contribKey(k), k.Entry, e.Peer, e.Via)
						}
						legs = append(legs, e.Spans())
					} else if k.MemberPod != k.PeerPod || k.Via != k.MemberPod || pr.Mechanism != extract.SharedPod {
						t.Fatalf("a shared pod contribution %s in a %s pair", contribKey(k), pr.Mechanism)
					}
					if want := interval.Along(legs...); !sameSpans(k.Spans, want) || len(want) == 0 {
						t.Fatalf("%s: contribution %s spans %s, Along of the legs %s", describePair(pr, true), contribKey(k), spansString(k.Spans), spansString(want))
					}
					all = append(all, k.Spans)
					vias[k.Via] = append(vias[k.Via], k.Spans)
				}
				if want := testUnion(all...); !sameSpans(extract.Spans(pr), want) {
					t.Fatalf("%s: spans %s, the union of the contributions %s", describePair(pr, true), spansString(extract.Spans(pr)), spansString(want))
				}
				for v, ss := range vias {
					if want := testUnion(ss...); !sameSpans(pr.Vias[v], want) {
						t.Fatalf("%s: via %s %s, want %s", describePair(pr, true), v, spansString(pr.Vias[v]), spansString(want))
					}
				}
				if len(pr.Vias) != len(vias) {
					t.Fatalf("%s: %d vias, want %d", describePair(pr, true), len(pr.Vias), len(vias))
				}
			}
		}
	})
}

func TestQ4OneToken(t *testing.T) {
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
		for _, sc := range c.seeds() {
			sp := &spy{Store: c.st}
			got := mustExtract(t, sp, sc.seed, c.w, sc.o)
			if got.Token != s {
				t.Fatalf("token %d, want %d", got.Token, s)
			}
			for _, tok := range sp.tokens {
				if tok != s {
					t.Fatalf("a read at token %d, want %d", tok, s)
				}
			}
			for _, fr := range got.PodResults {
				if fr.Token != s {
					t.Fatalf("a Find at token %d, want %d", fr.Token, s)
				}
			}
			if d := sameResult(got, mustExtract(t, fresh, sc.seed, latest, sc.o)); d != "" {
				t.Fatalf("at token %d against a store of the records up to it: %s", s, d)
			}
		}
	})
}

func pairSpans(ps []extract.Pair) map[pairKey][]interval.Span {
	out := map[pairKey][]interval.Span{}
	for _, p := range ps {
		out[keyOf(p)] = extract.Spans(p)
	}
	return out
}

// checkSplit is Q5 for one split: it splits w at m and, for each service as an
// exploratory seed, checks the property where the members, with their roles,
// are the same in the whole window and both halves; the other seeds are
// skipped. It returns how many seeds it compared and how many pairs it checked
// at the seam, for a caller whose world is fixed to assert on.
//
// The halves' spans join back to the whole's. At the seam, wherever a
// contribution of the whole crosses m, the left half's span ends at m with no
// Ends and the right half's starts at m with Before, as co-residency's split
// joins. The converse does not hold: two contributions that only touch at m
// (one pod leaves a node as another of the same service arrives) are one span
// of the whole but no contribution crosses m, so the right half's span starts
// at m without Before.
func checkSplit(t tb, st store.Store, services []identity.Fingerprint, w interval.Window, m time.Time, o extract.Options) (compared, seams int) {
	t.Helper()
	left, right := w, w
	left.To, right.From = m, m
	for _, s := range services {
		seed := extract.Exploratory{Service: s}
		whole, l, r := mustExtract(t, st, seed, w, o), mustExtract(t, st, seed, left, o), mustExtract(t, st, seed, right, o)
		ms := func(res extract.Result) string {
			var out []string
			for _, m := range res.Members {
				out = append(out, fmt.Sprintf("%s %s", m.Service, m.Role))
			}
			return fmt.Sprint(out)
		}
		if ms(whole) != ms(l) || ms(whole) != ms(r) {
			continue
		}
		compared++
		lps, rps := map[pairKey]extract.Pair{}, map[pairKey]extract.Pair{}
		for _, p := range l.Pairs {
			lps[keyOf(p)] = p
		}
		for _, p := range r.Pairs {
			rps[keyOf(p)] = p
		}
		for _, p := range whole.Pairs {
			crosses := slices.ContainsFunc(p.Contributions, func(k extract.Contribution) bool {
				return slices.ContainsFunc(k.Spans, func(sp interval.Span) bool { return sp.Start.Before(m) && sp.End.After(m) })
			})
			if !crosses {
				continue
			}
			seams++
			lp, rp := lps[keyOf(p)], rps[keyOf(p)]
			if len(lp.Spans) == 0 || len(rp.Spans) == 0 {
				t.Fatalf("service %s %v crosses %s but a half has no span", s, keyOf(p), ts(m))
			}
			last, first := lp.Spans[len(lp.Spans)-1], rp.Spans[0]
			if !last.End.Equal(m) || len(last.Ends) != 0 || !first.Start.Equal(m) || !first.Before {
				t.Fatalf("service %s %v at the seam %s: left ends %s with %d ends, right starts %s before=%t", s, keyOf(p), ts(m), ts(last.End), len(last.Ends), ts(first.Start), first.Before)
			}
		}
		wp, lp, rp := pairSpans(whole.Pairs), pairSpans(l.Pairs), pairSpans(r.Pairs)
		keys := map[pairKey]bool{}
		for _, mp := range []map[pairKey][]interval.Span{wp, lp, rp} {
			for k := range mp {
				keys[k] = true
			}
		}
		for k := range keys {
			if joined := testUnion(lp[k], rp[k]); !sameSpans(joined, wp[k]) {
				t.Fatalf("service %s %v split at %s: joined %s, whole %s", s, k, ts(m), spansString(joined), spansString(wp[k]))
			}
		}
	}
	return compared, seams
}

// TestQ5SplitWindow is Q5 on drawn worlds, split at a drawn instant. How many
// draws reach a seam is up to the draws, so the test asserts nothing about it:
// the cases that must cross one are the fixed worlds of
// TestQ5SplitWindowFixedWorlds.
func TestQ5SplitWindow(t *testing.T) {
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
		checkSplit(t, c.st, c.g.services, c.w, m, extract.Options{NodeOnly: c.nodeOnly, Policy: wide()})
	})
}

// TestQ5SplitWindowFixedWorlds is Q5 on worlds written to cross a seam, so the
// property is checked, not skipped, whatever a random draw gives. The first
// world is split at its one seam and the counts of what was compared are
// exact; the second is swept over many splits, and some of them must cross.
func TestQ5SplitWindowFixedWorlds(t *testing.T) {
	o := extract.Options{Policy: wide()}
	t.Run("a pod co-resident from 10:10 to 10:20, split at 10:15", func(t *testing.T) {
		st, _ := scenarioQA(t, memstore.Options{})
		compared, seams := checkSplit(t, st, []identity.Fingerprint{S, T, C, K}, w30, hm(10, 15), o)
		if compared != 4 || seams != 4 {
			t.Errorf("compared %d seeds and checked %d seams, want 4 and 4", compared, seams)
		}
	})
	t.Run("the scripted cluster, split at each minute and a nanosecond either side of each change", func(t *testing.T) {
		st, _, _ := scriptedCluster(t)
		services := []identity.Fingerprint{S, svc(2), svc(3), svc(4), svc(5)}
		changes := []int{5, 8, 9, 10, 12, 15, 16, 18, 20, 22, 25}
		compared, seams := 0, 0
		for _, no := range []bool{false, true} {
			for k := 1; k < 30; k++ {
				ds := []time.Duration{0}
				if slices.Contains(changes, k) {
					ds = []time.Duration{-1, 0, 1}
				}
				for _, d := range ds {
					c, s := checkSplit(t, st, services, w30, hm(10, k).Add(d), extract.Options{NodeOnly: no, Policy: wide()})
					compared, seams = compared+c, seams+s
				}
			}
		}
		t.Logf("compared %d seeds, checked %d seams", compared, seams)
		if compared == 0 || seams == 0 {
			t.Errorf("compared %d seeds and checked %d seams in a fixed world: none to check", compared, seams)
		}
	})
}

func TestQ6ArrivalOrderAndRecordsAfterTo(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		type at struct {
			p   string
			sub store.Subject
			t   int64
		}
		seen := map[at]bool{}
		var recs []store.Record
		for _, r := range c.recs {
			r.Seq = 0
			if k := (at{string(r.Producer), r.Subject, r.EventTime.UnixNano()}); !seen[k] {
				seen[k] = true
				recs = append(recs, r)
			}
		}
		w := c.w
		w.AsOf = store.Latest
		a, b := freshStore(t, memstore.Options{}, nil), freshStore(t, memstore.Options{}, nil)
		writeShuffled(t, a, recs)
		writeShuffled(t, b, recs)
		var before []extract.Result
		for _, s := range c.seeds() {
			ra, rb := mustExtract(t, a, s.seed, w, s.o), mustExtract(t, b, s.seed, w, s.o)
			rb.Token = ra.Token
			for i := range rb.PodResults {
				rb.PodResults[i].Token = ra.Token
			}
			if d := sameResult(ra, rb); d != "" {
				t.Fatalf("two write orders: %s", d)
			}
			before = append(before, ra)
		}
		shift := w.To.Sub(gridBase) + 7*time.Second + time.Nanosecond
		var later []store.Record
		for _, r := range drawRecords(t, c.g, 15) {
			r.EventTime = r.EventTime.Add(shift)
			if !r.Through.IsZero() {
				r.Through = r.Through.Add(shift)
			}
			if r.EventTime.Before(w.To) {
				t.Fatalf("a later record at %s", ts(r.EventTime))
			}
			later = append(later, r)
		}
		writeShuffled(t, a, later)
		for i, s := range c.seeds() {
			after := mustExtract(t, a, s.seed, w, s.o)
			after.Token = before[i].Token
			for k := range after.PodResults {
				after.PodResults[k].Token = before[i].Token
			}
			if d := sameResult(after, before[i]); d != "" {
				t.Fatalf("after records at or after To: %s", d)
			}
		}
	})
}

// TestQ7NoMappingByDescription writes every record twice over, once with
// descriptions that name a workload, a node and a service, and once with none:
// the answers agree.
func TestQ7NoMappingByDescription(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		described, bare := slices.Clone(c.recs), slices.Clone(c.recs)
		for i := range described {
			if described[i].Kind == lifecycle.Observe {
				described[i].Payload = []byte(`{"owner":{"kind":"Deployment","name":"web"},"k8s.workload.name":"web","node_name":"n1","service.name":"web"}`)
				bare[i].Payload = nil
			}
		}
		a, b := freshStore(t, memstore.Options{}, described), freshStore(t, memstore.Options{}, bare)
		for _, s := range c.seeds() {
			if d := sameResult(mustExtract(t, a, s.seed, c.w, s.o), mustExtract(t, b, s.seed, c.w, s.o)); d != "" {
				t.Fatalf("with and without descriptions: %s", d)
			}
		}
	})
}

// TestQ8UbiquityMonotonicity lowers the ubiquity fraction: pairs only move into
// Suppressed. With UbiquityMinNodes above the frontier, nothing is suppressed.
func TestQ8UbiquityMonotonicity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		hi := wide()
		hi.UbiquityDen = uniform(t, "den", 1, 5)
		hi.UbiquityNum = uniform(t, "num", 0, hi.UbiquityDen)
		hi.UbiquityMinNodes = uniform(t, "min nodes", 1, 3)
		lo := hi
		lo.UbiquityDen = uniform(t, "lower den", hi.UbiquityDen, 6)
		lo.UbiquityNum = uniform(t, "lower num", 0, hi.UbiquityNum*lo.UbiquityDen/hi.UbiquityDen)
		for _, s := range c.g.services {
			seed := extract.Exploratory{Service: s}
			a := mustExtract(t, c.st, seed, c.w, extract.Options{NodeOnly: c.nodeOnly, Policy: hi})
			b := mustExtract(t, c.st, seed, c.w, extract.Options{NodeOnly: c.nodeOnly, Policy: lo})
			kept := pairSpans(b.Pairs)
			for k := range kept {
				if _, ok := pairSpans(a.Pairs)[k]; !ok {
					t.Fatalf("%v kept at %d/%d but not at %d/%d", k, lo.UbiquityNum, lo.UbiquityDen, hi.UbiquityNum, hi.UbiquityDen)
				}
			}
			if len(a.Pairs)+len(a.Suppressed) != len(b.Pairs)+len(b.Suppressed) {
				t.Fatalf("%d+%d pairs at %d/%d, %d+%d at %d/%d", len(a.Pairs), len(a.Suppressed), hi.UbiquityNum, hi.UbiquityDen, len(b.Pairs), len(b.Suppressed), lo.UbiquityNum, lo.UbiquityDen)
			}
			for _, p := range b.Suppressed {
				if p.Reason != coresidency.Ubiquitous || p.Coverage[1] != b.Ubiquity.FrontierNodes {
					t.Fatalf("a suppressed pair %s", describePair(p, true))
				}
			}
			above := hi
			above.UbiquityMinNodes = b.Ubiquity.FrontierNodes + 1
			if r := mustExtract(t, c.st, seed, c.w, extract.Options{NodeOnly: c.nodeOnly, Policy: above}); len(r.Suppressed) != 0 || r.Ubiquity.Judged {
				t.Fatalf("judged with UbiquityMinNodes above the frontier: %v", describePairs(r.Suppressed, true))
			}
		}
	})
}

// TestQ10OneExtraction checks that an anchored seed is the exploratory
// extraction run on the anchored seed pods. Those are found here from the
// exploratory answer and point reads: the exploratory seed pods that their own
// Find places on the anchor's host at the instant, with an instance mapped
// singly to a member there. The anchor is on the host exactly when one of them
// is the seed service's; then the anchored answer has the same members, those
// seed pods with the same Finds, and exactly the exploratory contributions
// from them, with the same spans, in pairs whose spans contain its own.
func TestQ10OneExtraction(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		o := extract.Options{Policy: wide()}
		for _, a := range c.anchors {
			an := mustExtract(t, c.st, a, c.w, o)
			ex := mustExtract(t, c.st, extract.Exploratory{Service: a.Service}, c.w, o)
			members := map[identity.Fingerprint]bool{}
			for _, m := range ex.Members {
				members[m.Service] = true
			}
			pr := pointReader{t: t, st: c.st, tok: ex.Token}
			var want []identity.Fingerprint
			onHost := false
			for _, fr := range ex.PodResults {
				if !slices.ContainsFunc(fr.SeedHosts, func(h coresidency.HostSpans) bool {
					return h.Host == a.Host && slices.ContainsFunc(h.Spans, func(s interval.Span) bool { return s.Contains(a.At) })
				}) {
					continue
				}
				for _, svc := range pr.services(fr.Seed, a.At) {
					if members[svc] && !slices.Contains(want, fr.Seed) {
						want = append(want, fr.Seed)
					}
					onHost = onHost || svc == a.Service
				}
			}
			if !onHost {
				want = nil
			}
			got := seedPods(an, a)
			slices.SortFunc(got, store.CompareFingerprints)
			slices.SortFunc(want, store.CompareFingerprints)
			if (an.Anchor == extract.OnHost) != onHost || !slices.Equal(got, want) {
				t.Fatalf("anchor %s, seed pods %v; from the exploratory answer: on the host %t, seed pods %v", an.Anchor, got, onHost, want)
			}
			anContribs := map[string]bool{}
			for _, p := range an.Pairs {
				for _, k := range p.Contributions {
					anContribs[contribKey(k)] = true
				}
			}
			for _, p := range slices.Concat(ex.Pairs, ex.Suppressed) {
				for _, k := range p.Contributions {
					if slices.Contains(want, k.MemberPod) && !anContribs[contribKey(k)] {
						t.Fatalf("exploratory contribution %s from an anchored seed pod is not in the anchored answer", contribKey(k))
					}
				}
			}
			var am, em []string
			for _, m := range an.Members {
				am = append(am, fmt.Sprintf("%s %s", m.Service, m.Role))
			}
			for _, m := range ex.Members {
				em = append(em, fmt.Sprintf("%s %s", m.Service, m.Role))
			}
			if !slices.Equal(am, em) {
				t.Fatalf("members %v, exploratory %v", am, em)
			}
			exFinds := map[identity.Fingerprint]coresidency.Result{}
			for _, fr := range ex.PodResults {
				exFinds[fr.Seed] = fr
			}
			for _, p := range seedPods(an, a) {
				fr, ok := exFinds[p]
				if !ok {
					t.Fatalf("anchored seed pod %s is not an exploratory one: %v", p, seedsOf(ex))
				}
				i := slices.IndexFunc(an.PodResults, func(r coresidency.Result) bool { return r.Seed == p })
				if d := diffLines("find "+p.String(), describeEntries(an.PodResults[i].CoResidents), describeEntries(fr.CoResidents)); d != "" {
					t.Fatal(d)
				}
			}
			exPairs := map[pairKey]extract.Pair{}
			for _, p := range ex.Pairs {
				exPairs[keyOf(p)] = p
			}
			for _, p := range an.Pairs {
				e, ok := exPairs[keyOf(p)]
				if !ok {
					t.Fatalf("anchored pair %s is not an exploratory one", describePair(p, true))
				}
				if !sameSpans(testUnion(extract.Spans(p), extract.Spans(e)), extract.Spans(e)) {
					t.Fatalf("anchored pair %s is not within the exploratory %s", describePair(p, true), describePair(e, true))
				}
				for _, k := range p.Contributions {
					if !slices.ContainsFunc(e.Contributions, func(x extract.Contribution) bool {
						return contribKey(x) == contribKey(k) && sameSpans(x.Spans, k.Spans)
					}) {
						t.Fatalf("anchored contribution %s is not an exploratory one", contribKey(k))
					}
				}
			}
		}
	})
}
