package subgraph_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func endpoint(n int) identity.Fingerprint { return mkfp(catalog.NetworkEndpoint, n) }

func hasAddress(from, e identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(from, e, catalog.HasAddress)
}

// dropSet is a drop function over a set of entities.
func dropSet(fps ...identity.Fingerprint) func(identity.Fingerprint) bool {
	set := map[identity.Fingerprint]bool{}
	for _, fp := range fps {
		set[fp] = true
	}
	return func(fp identity.Fingerprint) bool { return set[fp] }
}

func dropNothing(identity.Fingerprint) bool { return false }

// ordered names keys in the order of a subgraph's elements.
func ordered(ks ...subgraph.Key) []string {
	ks = slices.Clone(ks)
	slices.SortFunc(ks, subgraph.Key.Compare)
	return strs(ks...)
}

func dropEndpoints(fp identity.Fingerprint) bool { return fp.Type() == catalog.NetworkEndpoint }

// without runs Without and fails on an error.
func without(t tb, sg subgraph.Subgraph, drop func(identity.Fingerprint) bool) (subgraph.Subgraph, subgraph.Dropped) {
	t.Helper()
	got, d, err := subgraph.Without(sg, drop)
	if err != nil {
		t.Fatalf("Without: %v", err)
	}
	return got, d
}

// sameSubgraph fails unless got and want are equal in every field.
func sameSubgraph(t tb, what string, got, want subgraph.Subgraph) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\ngot:  %+v\nwant: %+v", what, got, want)
	}
}

// A scope of a pod and its node, both of which hold an address. Taking out the
// endpoints takes out their existence, the edges to them and the read of one,
// and leaves what is left as it would be built without them.
func TestWithoutTakesOutWhatNamesADroppedEntity(t *testing.T) {
	const w = 10
	p, n, h, e1, e2 := pod(1), node(1), host(1), endpoint(1), endpoint(2)
	kept := []subgraph.Source{
		mk(w, ent(p), sp(0, -1)), mk(w, ent(n), sp(0, -1)),
		mk(w, sched(p, n), sp(0, -1)), mk(w, runsOn(n, h), sp(3, -1)),
	}
	in := input(w, append(slices.Clone(kept),
		mk(w, ent(e1), sp(0, 6)), mk(w, hasAddress(p, e1), sp(2, -1)), mk(w, hasAddress(n, e2), sp(0, 5)))...)
	in.Read = []identity.Fingerprint{p, n, e1}
	sg := build(t, in)

	got, d := without(t, sg, dropEndpoints)
	if want := (subgraph.Dropped{Entities: 1, Edges: 2}); d != want {
		t.Errorf("dropped %+v, want %+v", d, want)
	}
	expectEqual(t, "elements", keysOf(got), ordered(ent(p), ent(n), sched(p, n), runsOn(n, h)))
	var dict []identity.Fingerprint
	for _, e := range got.Entities {
		dict = append(dict, e.Fingerprint)
	}
	wantDict := []identity.Fingerprint{p, n, h}
	slices.SortFunc(wantDict, store.CompareFingerprints)
	expectEqual(t, "dictionary", dict, wantDict)
	if id, _ := got.EntityID(h); !got.Entities[id].Boundary {
		t.Error("the host at the far end of runs_on is not a boundary")
	}
	want := input(w, kept...)
	want.Read = []identity.Fingerprint{p, n}
	sameSubgraph(t, "the subgraph without the endpoints", got, build(t, want))
	check(t, want, got)

	// A request that names E1 drops only E2: E1, its existence and its edge stay.
	got, d = without(t, sg, func(fp identity.Fingerprint) bool { return dropEndpoints(fp) && fp != e1 })
	if want := (subgraph.Dropped{Edges: 1}); d != want {
		t.Errorf("dropped %+v, want %+v", d, want)
	}
	expectEqual(t, "elements", keysOf(got), ordered(ent(p), ent(n), ent(e1), sched(p, n), hasAddress(p, e1), runsOn(n, h)))
}

// An edge is taken out when either end is dropped, the source or the target,
// and an edge between two kept entities stays.
func TestWithoutChecksBothEndsOfAnEdge(t *testing.T) {
	const w = 10
	n1, n2, h1, h2 := node(1), node(2), host(1), host(2)
	in := input(w, mk(w, runsOn(n1, h1), sp(0, -1)), mk(w, runsOn(n2, h1), sp(0, -1)), mk(w, runsOn(n2, h2), sp(1, 4)))
	sg := build(t, in)
	for _, tc := range []struct {
		name string
		drop identity.Fingerprint
		want []string
	}{
		{"the source", n1, ordered(runsOn(n2, h1), runsOn(n2, h2))},
		{"the target", h1, ordered(runsOn(n2, h2))},
		{"neither end", host(3), ordered(runsOn(n1, h1), runsOn(n2, h1), runsOn(n2, h2))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := without(t, sg, dropSet(tc.drop))
			expectEqual(t, "elements", keysOf(got), tc.want)
		})
	}
}

// A derived element whose two ends are kept is taken out with a leg: it holds
// only while its legs exist, and a leg is gone. One whose end is dropped is
// taken out by its key.
func TestWithoutTakesOutADerivedElementWithALegTakenOut(t *testing.T) {
	in := goodPaths()
	in.Sources = append(in.Sources, mk(pw, ent(node(1)), sp(0, -1)), mk(pw, ent(pod(1)), sp(0, -1)))
	sg := build(t, in)

	got, d := without(t, sg, dropSet(node(1)))
	if want := (subgraph.Dropped{Entities: 1, Edges: 2, Derived: 1}); d != want {
		t.Errorf("dropping the node: dropped %+v, want %+v", d, want)
	}
	expectEqual(t, "elements", keysOf(got), ordered(ent(pod(1))))

	// Pod 2 is only the target of a leg and the derived element's second end.
	got, d = without(t, sg, dropSet(pod(2)))
	if want := (subgraph.Dropped{Edges: 1, Derived: 1}); d != want {
		t.Errorf("dropping pod 2: dropped %+v, want %+v", d, want)
	}
	expectEqual(t, "elements", keysOf(got), ordered(ent(pod(1)), ent(node(1)), legA))

	// The same with the derived element's ends kept: pod 1 and pod 2 are its
	// ends, and the leg is taken out through node 1 alone.
	in = input(pw,
		mk(pw, legA, sp(0, 20)), mk(pw, legB, sp(5, 15)), mk(pw, sched(pod(1), node(2)), sp(0, 25)),
		withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}},
			[][]subgraph.LegEnd{{{Path: 0, Leg: 1, Reason: subgraph.EndReasonProducer}}}, reachEnded(5, 15)),
	)
	sg = build(t, in)
	got, d = without(t, sg, dropSet(node(1)))
	if want := (subgraph.Dropped{Edges: 2, Derived: 1}); d != want {
		t.Errorf("dropping the shared node: dropped %+v, want %+v", d, want)
	}
	expectEqual(t, "elements", keysOf(got), ordered(sched(pod(1), node(2))))
	if _, ok := got.EntityID(pod(2)); ok {
		t.Error("pod 2, named only by what was taken out, is still in the dictionary")
	}
}

// Dropping nothing returns the subgraph as it was, every field of every
// section, a derived element's legs and leg ends, pieces that began before
// From, producers, the static section and the entities read included.
func TestWithoutNothingReturnsTheSameSubgraph(t *testing.T) {
	in := goodPaths()
	q := &store.QuarantineError{Entity: host(9), Layer: catalog.L1}
	before := span{start: 0, end: 7, reason: subgraph.EndReasonLivenessExpiry, before: true, producers: []lifecycle.Producer{"a", "b"}}
	in.Sources = append(in.Sources,
		mk(pw, ent(pod(3)), before, sp(9, -1)),
		subgraph.Source{Key: ent(host(9)), Quarantine: q},
		withUncascaded(mk(pw, runsOn(node(5), host(9)), sp(2, 8)), q),
		mk(pw, sched(pod(4), node(6)), sp(0, -1)),
	)
	// Node 6 is read, has no element of its own and is named by an edge: it is
	// not a boundary, and only the reads say so.
	in.Read = []identity.Fingerprint{node(6), pod(3), pod(99)}
	in.Sections = section(pw)
	in.Sections.Pods[0].Quarantined = []subgraph.Key{runsOn(node(5), host(9)), ent(host(9))}
	sg := build(t, in)
	if id, ok := sg.EntityID(node(6)); !ok || sg.Entities[id].Boundary {
		t.Fatal("the fixture does not have a read entity named only by an edge")
	}
	got, d := without(t, sg, dropNothing)
	if d != (subgraph.Dropped{}) {
		t.Errorf("dropped %+v", d)
	}
	sameSubgraph(t, "dropping nothing", got, sg)
}

// A section is never cut: one that names a dropped entity keeps it in the
// dictionary, as a boundary, for the caller to refuse.
func TestWithoutKeepsASectionAndTheEntitiesItNames(t *testing.T) {
	in := input(pw, mk(pw, ent(pod(1)), sp(0, -1)), mk(pw, hasAddress(pod(1), endpoint(1)), sp(0, -1)))
	in.Sections.Unattributed = []subgraph.Unattributed{{
		Subject: pod(1), Reason: subgraph.UnattributedSeveralClaims, Relation: catalog.HasAddress, Target: catalog.NetworkEndpoint,
		Spans:      []subgraph.Span{{Start: 0, End: pw * uint64(time.Second), Before: true}},
		Candidates: []identity.Fingerprint{endpoint(1), endpoint(2)},
	}}
	in.Read = []identity.Fingerprint{pod(1), endpoint(1)}
	sg := build(t, in)
	got, d := without(t, sg, dropEndpoints)
	if want := (subgraph.Dropped{Edges: 1}); d != want {
		t.Errorf("dropped %+v, want %+v", d, want)
	}
	if !sameSections(got.Sections, sg.Sections) {
		t.Errorf("sections:\n%+v\nwant\n%+v", got.Sections, sg.Sections)
	}
	for _, fp := range []identity.Fingerprint{endpoint(1), endpoint(2)} {
		id, ok := got.EntityID(fp)
		if !ok || !got.Entities[id].Boundary {
			t.Errorf("%s, named by a section, is in the dictionary %t, as a boundary %t", fp, ok, ok && got.Entities[id].Boundary)
		}
	}
}

// Without leaves its argument as it was, and the result shares no slice with
// it.
func TestWithoutLeavesItsArgumentAlone(t *testing.T) {
	mkIn := func() subgraph.Input {
		in := goodPaths()
		in.Sources = append(in.Sources, mk(pw, ent(pod(1)), span{start: 0, end: 9, reason: subgraph.EndReasonProducer, producers: []lifecycle.Producer{"a"}}))
		in.Sections = section(pw)
		return in
	}
	sg := build(t, mkIn())
	without(t, sg, dropSet(pod(2)))
	sameSubgraph(t, "the argument after Without", sg, build(t, mkIn()))

	got, _ := without(t, sg, dropNothing)
	for i := range got.Elements {
		for j := range got.Elements[i].Pieces {
			p := &got.Elements[i].Pieces[j]
			p.Start++
			for k := range p.Producers {
				p.Producers[k] = "changed"
			}
			for k := range p.Ends {
				p.Ends[k].Reason = subgraph.EndReasonCascade
			}
		}
		for _, path := range got.Elements[i].Paths {
			for k := range path {
				path[k] = 99
			}
		}
	}
	got.Sections.Truncated.Nodes[0] = node(99)
	got.Sections.Pods[0].Suppressed[0].Spans[0].End = 1
	sameSubgraph(t, "the argument after its result changed", sg, build(t, mkIn()))
}

// Every refusal: a nil drop, a subgraph Build could not have made, and an input
// Build refuses once things are taken out. Each wraps store.ErrInvalid and
// returns nothing else.
func TestWithoutRefuses(t *testing.T) {
	q := &store.QuarantineError{Entity: host(9), Layer: catalog.L1}
	good := func() subgraph.Subgraph {
		in := goodPaths()
		in.Sources = append(in.Sources, subgraph.Source{Key: ent(host(9)), Quarantine: q},
			withUncascaded(mk(pw, runsOn(node(5), host(9)), sp(2, 8)), q))
		return build(t, in)
	}
	for _, tc := range []struct {
		name string
		sg   func() subgraph.Subgraph
		drop func(identity.Fingerprint) bool
	}{
		{"a nil drop", good, nil},
		{"a basis of fact", func() subgraph.Subgraph { sg := good(); sg.Basis = subgraph.BasisFact; return sg }, dropNothing},
		{"another policy version", func() subgraph.Subgraph { sg := good(); sg.PolicyVersion--; return sg }, dropNothing},
		{"an empty window", func() subgraph.Subgraph {
			return subgraph.Subgraph{Basis: subgraph.BasisDerived, PolicyVersion: subgraph.PolicyVersion}
		}, dropNothing},
		{"a quarantined entity without its collision", func() subgraph.Subgraph {
			sg := good()
			sg.Quarantine.Entities[0].Cause = nil
			return sg
		}, dropNothing},
		{"an uncascaded edge without its collision", func() subgraph.Subgraph {
			sg := good()
			sg.Quarantine.Edges[0].Cause = nil
			return sg
		}, dropNothing},
		{"a leg that is not an element", func() subgraph.Subgraph {
			sg := good()
			id, _ := sg.ElementID(coKeyOf())
			sg.Elements[id].Paths = [][]uint32{{0, uint32(len(sg.Elements))}}
			return sg
		}, dropNothing},
		{"a piece past the window", func() subgraph.Subgraph {
			sg := good()
			sg.Elements[0].Pieces[0].End = uint64(sg.To.Sub(sg.From)) + 1
			return sg
		}, dropNothing},
		{"a piece past the window by a wrapping amount", func() subgraph.Subgraph {
			sg := good()
			sg.Elements[0].Pieces[0].End = ^uint64(0)
			return sg
		}, dropNothing},
		{"an uncascaded edge's piece past the window", func() subgraph.Subgraph {
			sg := good()
			sg.Quarantine.Edges[0].Pieces[0].End = uint64(sg.To.Sub(sg.From)) + 1
			return sg
		}, dropNothing},
		{"a class the pieces do not have", func() subgraph.Subgraph {
			sg := good()
			sg.Elements[0].Class = subgraph.ClassFlapping
			return sg
		}, dropNothing},
		{"a pod section that lists a quarantined edge taken out", func() subgraph.Subgraph {
			in := goodPaths()
			in.Sources = append(in.Sources, subgraph.Source{Key: ent(host(9)), Quarantine: q},
				withUncascaded(mk(pw, runsOn(node(5), host(9)), sp(2, 8)), q))
			in.Sections.Pods = []subgraph.PodSection{{Seed: pod(1), Quarantined: []subgraph.Key{runsOn(node(5), host(9))}}}
			return build(t, in)
		}, dropSet(node(5))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, d, err := subgraph.Without(tc.sg(), tc.drop)
			if !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("err %v, want store.ErrInvalid", err)
			}
			if !reflect.DeepEqual(got, subgraph.Subgraph{}) || d != (subgraph.Dropped{}) {
				t.Errorf("a refusal returned %+v and %+v", got, d)
			}
			t.Log(err)
		})
	}
}

// The generator for the properties below draws on a cast of three pods, two
// nodes, two hosts and two endpoints, every scheduled_on, runs_on and
// has_address edge among them and the same_as edges between the hosts, a
// window of 4 to 16 steps of a millisecond, and for each subject absent, a few
// pieces whose interior boundaries are about one in four a nanosecond off their
// step, or quarantined. Up to three derived elements are made of one or two
// paths of the asserted edges drawn, as the union of their legs' intersections,
// with the legs that end where a piece does. A few entities are read, one of
// them named by nothing, and a few sections name pods, nodes, endpoints and
// quarantined entries. Each entity is dropped one time in four, an endpoint
// three times in four.

// drawer is what the generator draws with: a rapid.T, or a seeded source so
// that what the generator covers is the same on every run.
type drawer interface {
	Int(label string, lo, hi int) int
}

type rapidDrawer struct{ t *rapid.T }

func (d rapidDrawer) Int(label string, lo, hi int) int {
	return rapid.IntRange(lo, hi).Draw(d.t, label)
}

type seededDrawer struct{ r *rand.Rand }

func (d seededDrawer) Int(_ string, lo, hi int) int { return lo + d.r.IntN(hi-lo+1) }

type withoutCast struct {
	pods, nodes, hosts, endpoints []identity.Fingerprint
	// all is every entity of the cast above; loner is read and named by
	// nothing else.
	all   []identity.Fingerprint
	loner identity.Fingerprint
	edges []subgraph.Key
}

func newWithoutCast() withoutCast {
	c := withoutCast{
		pods: []identity.Fingerprint{pod(1), pod(2), pod(3)}, nodes: []identity.Fingerprint{node(1), node(2)},
		hosts: []identity.Fingerprint{host(1), host(2)}, endpoints: []identity.Fingerprint{endpoint(1), endpoint(2)},
		loner: pod(77),
	}
	c.all = slices.Concat(c.pods, c.nodes, c.hosts, c.endpoints)
	for _, n := range c.nodes {
		for _, p := range c.pods {
			c.edges = append(c.edges, sched(p, n))
		}
		for _, h := range c.hosts {
			c.edges = append(c.edges, runsOn(n, h))
		}
	}
	for _, e := range c.endpoints {
		for _, from := range slices.Concat(c.pods, c.nodes) {
			c.edges = append(c.edges, hasAddress(from, e))
		}
	}
	c.edges = append(c.edges, subgraph.EdgeKey(c.hosts[0], c.hosts[1], catalog.SameAs), subgraph.EdgeKey(c.hosts[1], c.hosts[0], catalog.SameAs))
	return c
}

type withoutCase struct {
	in   subgraph.Input
	drop map[identity.Fingerprint]bool
	// named are the entities the sections name.
	named map[identity.Fingerprint]bool
}

func (c withoutCase) dropFunc() func(identity.Fingerprint) bool {
	return func(fp identity.Fingerprint) bool { return c.drop[fp] }
}

func pick[T any](d drawer, label string, xs []T) T { return xs[d.Int(label, 0, len(xs)-1)] }

// drawPieces draws up to three pieces of a subject over width steps.
func drawPieces(d drawer, subject subgraph.Subject, width int) []subgraph.SourcePiece {
	var out []subgraph.SourcePiece
	shift := func(t time.Time) time.Time {
		if d.Int("shift", 0, 3) == 0 {
			return t.Add(time.Duration(d.Int("ns", -1, 1)))
		}
		return t
	}
	reasons := []subgraph.EndReason{
		subgraph.EndReasonProducer, subgraph.EndReasonLivenessExpiry, subgraph.EndReasonCascade,
		subgraph.EndReasonRetention, subgraph.EndReasonOperator,
	}
	if subject == subgraph.SubjectDerived {
		reasons = append(reasons, subgraph.EndReasonUnattributed, subgraph.EndReasonReachEnded)
	}
	next := 0
	for range d.Int("pieces", 0, 3) {
		start := next + d.Int("gap", 0, 3)
		if start >= width {
			break
		}
		end := min(width, start+1+d.Int("length", 0, 8))
		p := subgraph.SourcePiece{Start: base.Add(time.Duration(start) * step), End: base.Add(time.Duration(end) * step)}
		if start == 0 {
			p.Before = d.Int("before", 0, 1) == 1
		} else {
			p.Start = shift(p.Start)
		}
		if end < width {
			p.End = shift(p.End)
			p.EndReason = pick(d, "end reason", reasons)
		}
		for _, name := range []lifecycle.Producer{"a", "b", "c"} {
			if d.Int("producer", 0, 1) == 1 {
				p.Producers = append(p.Producers, name)
			}
		}
		out = append(out, p)
		if end == width {
			break
		}
		next = end + 1
	}
	return out
}

func drawPieceSource(d drawer, k subgraph.Key, width int) subgraph.Source {
	s := subgraph.Source{Key: k, Pieces: drawPieces(d, subjectOf(k), width)}
	s.Class, s.Transitions = classOf(s.Pieces, base, base.Add(time.Duration(width)*step))
	return s
}

// relSpans are a source's pieces in nanoseconds after base.
func relSpans(s subgraph.Source) []gridSpan {
	out := make([]gridSpan, len(s.Pieces))
	for i, p := range s.Pieces {
		out[i] = gridSpan{start: int(p.Start.Sub(base)), end: int(p.End.Sub(base)), reason: p.EndReason}
	}
	return out
}

// drawDerivedOver draws a derived element made of paths of the legs given, or
// reports that its paths never hold.
func drawDerivedOver(d drawer, c withoutCast, legs []subgraph.Source, width, i int) (subgraph.Source, bool) {
	rule := pick(d, "rule", []string{subgraph.RuleCoSharedNode, subgraph.RulePairSharedNode, subgraph.RuleReach})
	key := subgraph.Key{Kind: subgraph.KindDerived, A: pick(d, "first end", c.pods), B: pick(d, "second end", c.all), Rule: rule, Detail: fmt.Sprint(i)}
	w := width * int(step)
	var paths [][]int
	var covers [][]gridSpan
	for range d.Int("paths", 1, 2) {
		var p []int
		for range d.Int("path legs", 1, 3) {
			if l := d.Int("leg", 0, len(legs)-1); !slices.Contains(p, l) {
				p = append(p, l)
			}
		}
		cover := []gridSpan{{start: 0, end: w}}
		for _, l := range p {
			cover = meetGrid(cover, relSpans(legs[l]))
		}
		paths, covers = append(paths, p), append(covers, cover)
	}
	var union []gridSpan
	for _, s := range slices.SortedFunc(slices.Values(slices.Concat(covers...)), func(a, b gridSpan) int { return a.start - b.start }) {
		if n := len(union); n > 0 && s.start <= union[n-1].end {
			union[n-1].end = max(union[n-1].end, s.end)
			continue
		}
		union = append(union, s)
	}
	if len(union) == 0 {
		return subgraph.Source{}, false
	}
	s := subgraph.Source{Key: key}
	for _, p := range paths {
		var ks []subgraph.Key
		for _, l := range p {
			ks = append(ks, legs[l].Key)
		}
		s.Paths = append(s.Paths, ks)
	}
	for _, u := range union {
		p := subgraph.SourcePiece{Start: base.Add(time.Duration(u.start)), End: base.Add(time.Duration(u.end))}
		p.Before = u.start == 0 && d.Int("derived before", 0, 1) == 1
		if u.end < w {
			p.EndReason = subgraph.EndReasonReachEnded
			if rule != subgraph.RuleReach {
				for pi, path := range paths {
					if !slices.ContainsFunc(covers[pi], func(c gridSpan) bool { return c.end == u.end }) {
						continue
					}
					for li, l := range path {
						for _, lp := range relSpans(legs[l]) {
							if lp.end == u.end {
								p.Ends = append(p.Ends, subgraph.LegEnd{Path: pi, Leg: li, Reason: lp.reason})
							}
						}
					}
				}
			}
		}
		s.Pieces = append(s.Pieces, p)
	}
	s.Class, s.Transitions = classOf(s.Pieces, base, base.Add(time.Duration(width)*step))
	return s, true
}

// drawSpan draws one span of a section.
func drawSpan(d drawer, width int) []subgraph.Span {
	start := d.Int("span start", 0, width-1)
	end := start + 1 + d.Int("span length", 0, width-start-1)
	s := subgraph.Span{Start: uint64(start) * uint64(step), End: uint64(end) * uint64(step)}
	s.Before = start == 0 && d.Int("span before", 0, 1) == 1
	if end < width {
		s.EndReason = subgraph.EndReasonReachEnded
	}
	return []subgraph.Span{s}
}

func drawWithoutCase(d drawer, c withoutCast) withoutCase {
	width := d.Int("width", 4, 16)
	to := base.Add(time.Duration(width) * step)
	in := subgraph.Input{Token: uint64(d.Int("token", 1, 1<<20)), From: base, To: to}
	var legs []subgraph.Source
	var quarantined []subgraph.Key
	for _, fp := range c.all {
		switch d.Int("entity", 0, 5) {
		case 0:
		case 1:
			in.Sources = append(in.Sources, subgraph.Source{Key: ent(fp), Quarantine: &store.QuarantineError{Entity: fp, Layer: catalog.L2}})
			quarantined = append(quarantined, ent(fp))
		default:
			in.Sources = append(in.Sources, drawPieceSource(d, ent(fp), width))
		}
	}
	for _, k := range c.edges {
		if d.Int("edge", 0, 2) == 0 {
			continue
		}
		s := drawPieceSource(d, k, width)
		switch {
		case len(s.Pieces) == 0 || k.Relation == catalog.SameAs:
		case d.Int("uncascade", 0, 5) == 0:
			s.Uncascaded = &store.QuarantineError{Entity: pick(d, "uncascaded end", []identity.Fingerprint{k.A, k.B}), Layer: catalog.L1}
			quarantined = append(quarantined, k)
		default:
			legs = append(legs, s)
		}
		in.Sources = append(in.Sources, s)
	}
	for i := range d.Int("derived", 0, 3) {
		if len(legs) == 0 {
			break
		}
		if s, ok := drawDerivedOver(d, c, legs, width, i); ok {
			in.Sources = append(in.Sources, s)
		}
	}
	for _, fp := range append(slices.Clone(c.all), c.loner) {
		if d.Int("read", 0, 2) == 0 {
			in.Read = append(in.Read, fp)
		}
	}

	named := map[identity.Fingerprint]bool{}
	name := func(fps ...identity.Fingerprint) {
		for _, fp := range fps {
			named[fp] = true
		}
	}
	sec := &in.Sections
	if d.Int("truncated", 0, 1) == 1 {
		n := pick(d, "truncated node", c.nodes)
		sec.Truncated = subgraph.Truncation{Nodes: []identity.Fingerprint{n}, Results: 1}
		name(n)
	}
	if d.Int("unattributed", 0, 1) == 1 {
		u := subgraph.Unattributed{
			Subject: pick(d, "unattributed pod", c.pods), Reason: subgraph.UnattributedSeveralClaims,
			Relation: catalog.HasAddress, Target: catalog.NetworkEndpoint, Spans: drawSpan(d, width), Candidates: c.endpoints,
		}
		sec.Unattributed = append(sec.Unattributed, u)
		name(u.Subject)
		name(u.Candidates...)
	}
	if d.Int("unmapped", 0, 1) == 1 {
		u := subgraph.Unmapped{Pod: pick(d, "unmapped pod", c.pods), Spans: drawSpan(d, width)}
		sec.Unmapped = append(sec.Unmapped, u)
		name(u.Pod)
	}
	if d.Int("suppressed", 0, 1) == 1 {
		x := subgraph.Suppressed{
			Key:    subgraph.Key{Kind: subgraph.KindDerived, A: pick(d, "suppressed pod", c.pods), B: pick(d, "suppressed peer", c.all), Rule: subgraph.RuleCoSharedNode, Detail: "suppressed"},
			Reason: subgraph.SuppressedBelowMinOverlap, Spans: drawSpan(d, width),
		}
		ps := make([]subgraph.SourcePiece, len(x.Spans))
		for i, s := range x.Spans {
			ps[i] = subgraph.SourcePiece{Start: base.Add(time.Duration(s.Start)), End: base.Add(time.Duration(s.End))}
		}
		x.Class, x.Transitions = classOf(ps, base, to)
		sec.Suppressed = append(sec.Suppressed, x)
		name(x.Key.A, x.Key.B)
	}
	if d.Int("pod section", 0, 1) == 1 {
		p := subgraph.PodSection{Seed: pick(d, "seed pod", c.pods)}
		for _, k := range quarantined {
			if d.Int("listed", 0, 1) == 1 {
				p.Quarantined = append(p.Quarantined, k)
			}
		}
		sec.Pods = append(sec.Pods, p)
		name(p.Seed)
		for _, k := range p.Quarantined {
			name(k.A)
			if !k.B.IsZero() {
				name(k.B)
			}
		}
	}

	drop := map[identity.Fingerprint]bool{}
	for _, fp := range append(slices.Clone(c.all), c.loner) {
		switch {
		case fp.Type() == catalog.NetworkEndpoint && d.Int("drop endpoint", 0, 3) > 0,
			fp.Type() != catalog.NetworkEndpoint && d.Int("drop", 0, 3) == 0:
			drop[fp] = true
		}
	}
	return withoutCase{in: in, drop: drop, named: named}
}

// filterInput is the input without what names a dropped entity, written from
// the definition and apart from the package: the sources whose key names one,
// as entity, source, target or end; then the derived sources with a leg among
// those; and the dropped reads. It returns the keys it took out.
func filterInput(in subgraph.Input, drop map[identity.Fingerprint]bool) (subgraph.Input, map[subgraph.Key]bool) {
	gone := map[subgraph.Key]bool{}
	for _, s := range in.Sources {
		if drop[s.Key.A] || drop[s.Key.B] {
			gone[s.Key] = true
		}
	}
	for _, s := range in.Sources {
		for _, path := range s.Paths {
			for _, leg := range path {
				if gone[leg] {
					gone[s.Key] = true
				}
			}
		}
	}
	out := in
	out.Sources, out.Read = nil, nil
	for _, s := range in.Sources {
		if !gone[s.Key] {
			out.Sources = append(out.Sources, s)
		}
	}
	for _, fp := range in.Read {
		if !drop[fp] {
			out.Read = append(out.Read, fp)
		}
	}
	return out, gone
}

// subgraphKeys are the keys of the elements and of the static section.
func subgraphKeys(sg subgraph.Subgraph) map[subgraph.Key]bool {
	out := map[subgraph.Key]bool{}
	for _, el := range sg.Elements {
		out[el.Key] = true
	}
	for _, q := range sg.Quarantine.Entities {
		out[ent(q.Fingerprint)] = true
	}
	for _, e := range sg.Quarantine.Edges {
		out[e.Key] = true
	}
	return out
}

// droppedOf counts the keys of sg in gone, per list.
func droppedOf(sg subgraph.Subgraph, gone map[subgraph.Key]bool) subgraph.Dropped {
	var d subgraph.Dropped
	for _, el := range sg.Elements {
		if !gone[el.Key] {
			continue
		}
		switch el.Key.Kind {
		case subgraph.KindEntity:
			d.Entities++
		case subgraph.KindEdge:
			d.Edges++
		case subgraph.KindDerived:
			d.Derived++
		}
	}
	for _, q := range sg.Quarantine.Entities {
		if gone[ent(q.Fingerprint)] {
			d.QuarantinedEntities++
		}
	}
	for _, e := range sg.Quarantine.Edges {
		if gone[e.Key] {
			d.QuarantinedEdges++
		}
	}
	return d
}

// checkWithout checks one drawn case: dropping nothing returns the subgraph;
// dropping the drawn set returns the subgraph built from the filtered input,
// with the counts of what was taken out, the oracle and both bounds holding; or,
// when the filtered input is refused, a refusal. It counts what the case
// exercised into seen.
func checkWithout(t tb, c withoutCase, seen map[string]int) {
	t.Helper()
	sg := build(t, c.in)
	same, d0 := without(t, sg, dropNothing)
	if d0 != (subgraph.Dropped{}) {
		t.Fatalf("dropping nothing dropped %+v", d0)
	}
	sameSubgraph(t, "dropping nothing", same, sg)

	filtered, gone := filterInput(c.in, c.drop)
	want, wantErr := subgraph.Build(filtered)
	got, d, err := subgraph.Without(sg, c.dropFunc())
	if wantErr != nil {
		if !errors.Is(err, store.ErrInvalid) {
			t.Fatalf("the filtered input is refused (%v), and Without returned %v", wantErr, err)
		}
		listed := false
		for _, p := range c.in.Sections.Pods {
			listed = listed || slices.ContainsFunc(p.Quarantined, func(k subgraph.Key) bool { return gone[k] })
		}
		if !listed {
			t.Fatalf("the filtered input is refused, and no pod section lists an entry taken out: %v", wantErr)
		}
		seen["refused: a pod section lists a quarantined entry taken out"]++
		return
	}
	if err != nil {
		t.Fatalf("Without: %v", err)
	}
	sameSubgraph(t, "the subgraph without the dropped entities", got, want)
	if wantD := droppedOf(sg, gone); d != wantD {
		t.Fatalf("dropped %+v, want %+v", d, wantD)
	}
	kept := subgraphKeys(got)
	for k := range subgraphKeys(sg) {
		if kept[k] == gone[k] {
			t.Fatalf("%s: kept %t, taken out %t", k, kept[k], gone[k])
		}
	}
	for k := range kept {
		if c.drop[k.A] || c.drop[k.B] {
			t.Fatalf("%s names a dropped entity and is kept", k)
		}
	}
	for _, e := range got.Entities {
		if c.drop[e.Fingerprint] && (!c.named[e.Fingerprint] || !e.Boundary) {
			t.Fatalf("the dropped entity %s is in the dictionary: named by a section %t, a boundary %t", e.Fingerprint, c.named[e.Fingerprint], e.Boundary)
		}
	}
	check(t, filtered, got)

	seen["cases"]++
	for _, el := range sg.Elements {
		k := el.Key
		for _, p := range el.Pieces {
			if p.Before {
				seen["pieces that began before From"]++
			}
			if len(p.Ends) > 0 {
				seen["pieces with leg ends"]++
			}
			if len(p.Producers) > 0 {
				seen["pieces with producers"]++
			}
		}
		if !gone[k] {
			continue
		}
		switch {
		case k.Kind == subgraph.KindEdge && c.drop[k.A] && !c.drop[k.B]:
			seen["edges taken out by the source alone"]++
		case k.Kind == subgraph.KindEdge && !c.drop[k.A] && c.drop[k.B]:
			seen["edges taken out by the target alone"]++
		case k.Kind == subgraph.KindDerived && !c.drop[k.A] && !c.drop[k.B]:
			seen["derived taken out by a leg alone"]++
		case k.Kind == subgraph.KindDerived:
			seen["derived taken out by an end"]++
		}
	}
	for _, e := range sg.Entities {
		if _, own := sg.ElementID(ent(e.Fingerprint)); !e.Boundary && !own && !slices.ContainsFunc(sg.Quarantine.Entities, func(q subgraph.QuarantinedEntity) bool { return q.Fingerprint == e.Fingerprint }) {
			seen["read entities with no element of their own"]++
		}
		if c.drop[e.Fingerprint] && c.named[e.Fingerprint] && slices.Contains(c.in.Read, e.Fingerprint) {
			seen["dropped reads a section names"]++
		}
	}
	seen["quarantined entities taken out"] += d.QuarantinedEntities
	seen["uncascaded edges taken out"] += d.QuarantinedEdges
	for _, e := range got.Entities {
		if c.drop[e.Fingerprint] {
			seen["dropped entities a section keeps"]++
		}
	}
	if len(got.Deltas) > 0 {
		seen["results with deltas"]++
	}
}

// Over drawn inputs, dropping nothing returns the subgraph, and dropping a
// drawn set of entities returns the subgraph built from the input without what
// names them, with the counts, the oracle and both bounds.
func TestPropertyWithoutEqualsBuildWithout(t *testing.T) {
	c := newWithoutCast()
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn cases held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		checkWithout(t, drawWithoutCase(rapidDrawer{t}, c), seen)
	})
}

// The same over inputs from fixed seeds, so that the claims about what the
// generator covers hold on every run: every way an element is taken out, and
// every part of the inverse that a lossy one would lose.
func TestWithoutOnFixedInputs(t *testing.T) {
	c := newWithoutCast()
	const cases = 300
	seen := map[string]int{}
	for seed := range cases {
		checkWithout(t, drawWithoutCase(seededDrawer{rand.New(rand.NewPCG(uint64(seed), 0x77697468))}, c), seen)
	}
	t.Logf("%d inputs: %v", cases, seen)
	for _, what := range []string{
		"edges taken out by the source alone", "edges taken out by the target alone",
		"derived taken out by a leg alone", "derived taken out by an end",
		"quarantined entities taken out", "uncascaded edges taken out",
		"refused: a pod section lists a quarantined entry taken out",
		"dropped entities a section keeps", "dropped reads a section names",
		"read entities with no element of their own",
		"pieces that began before From", "pieces with leg ends", "pieces with producers",
	} {
		if seen[what] < 10 {
			t.Errorf("the generator is too thin: %d %s over %d inputs", seen[what], what, cases)
		}
	}
}

// Over drawn worlds, what the scope reader and the three adapters build from a
// store comes back equal when nothing is dropped, and with a drawn set of
// entities dropped, every element left is the one the subgraph had, no key
// names a dropped entity or has a leg taken out, the counts are those of what
// was taken out, and the oracle and both bounds hold.
func TestPropertyWithoutOnWhatTheAdaptersBuild(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn subgraphs held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		w := newWorld()
		st, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		writeShuffled(t, st, drawWorldRecords(t, w))
		from := uniform(t, "from", -3, 30)
		win := interval.Window{From: at(from), To: at(from + rapid.SampledFrom([]int{3, 8, 15, 25}).Draw(t, "width")), AsOf: store.Latest}
		all := slices.Concat(w.pods, w.insts, w.nodes, w.svcs, w.hosts)

		var sgs []subgraph.Subgraph
		scope := rapid.SliceOfN(rapid.SampledFrom(all), 1, 4).Draw(t, "scope")
		ssg, err := subgraph.ReadScope(ctx, st, scope, win, subgraph.ScopePolicy{})
		if err != nil {
			t.Fatalf("ReadScope: %v", err)
		}
		cres, err := coresidency.NewFinder(st).Find(ctx, rapid.SampledFrom(w.pods).Draw(t, "seed pod"), win, coresidency.Options{})
		if err != nil {
			t.Fatal(err)
		}
		csg, err := subgraph.FromCoResidency(ctx, st, cres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromCoResidency: %v", err)
		}
		ex := extract.DefaultPolicy()
		if rapid.Bool().Draw(t, "extract caps") {
			ex.MaxMembers, ex.MaxInstances, ex.MaxPairs = 2, 2, 2
		}
		eres, err := extract.NewExtractor(st).Extract(ctx, extract.Exploratory{Service: rapid.SampledFrom(w.svcs).Draw(t, "seed service")}, win, extract.Options{Policy: ex})
		if err != nil {
			t.Fatal(err)
		}
		esg, err := subgraph.FromExtraction(ctx, st, eres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromExtraction: %v", err)
		}
		bres, err := blastradius.NewWalker(st).Impact(ctx, rapid.SampledFrom(slices.Concat(w.hosts, w.nodes, w.svcs)).Draw(t, "blast seed"), win, blastradius.Options{})
		if err != nil {
			t.Fatal(err)
		}
		bsg, err := subgraph.FromBlastRadius(ctx, st, bres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromBlastRadius: %v", err)
		}
		sgs = append(sgs, ssg, csg, esg, bsg)

		drop := map[identity.Fingerprint]bool{}
		for _, fp := range all {
			if uniform(t, "drop", 0, 3) == 0 {
				drop[fp] = true
			}
		}
		names := func(k subgraph.Key) bool { return drop[k.A] || drop[k.B] }
		for i, sg := range sgs {
			same, d0 := without(t, sg, dropNothing)
			if d0 != (subgraph.Dropped{}) {
				t.Fatalf("subgraph %d: dropping nothing dropped %+v", i, d0)
			}
			sameSubgraph(t, fmt.Sprintf("subgraph %d, dropping nothing", i), same, sg)

			// What is taken out, from the subgraph itself: what names a dropped
			// entity, and a derived element with a leg that does.
			gone := map[subgraph.Key]bool{}
			for k := range subgraphKeys(sg) {
				gone[k] = names(k)
			}
			for _, el := range sg.Elements {
				for _, path := range el.Paths {
					for _, id := range path {
						gone[el.Key] = gone[el.Key] || names(sg.Elements[id].Key)
					}
				}
			}
			got, d, err := subgraph.Without(sg, func(fp identity.Fingerprint) bool { return drop[fp] })
			if err != nil {
				listed := false
				for _, p := range sg.Sections.Pods {
					listed = listed || slices.ContainsFunc(p.Quarantined, func(k subgraph.Key) bool { return gone[k] })
				}
				if !errors.Is(err, store.ErrInvalid) || !listed {
					t.Fatalf("subgraph %d: Without: %v", i, err)
				}
				seen["refused"]++
				continue
			}
			if wantD := droppedOf(sg, gone); d != wantD {
				t.Fatalf("subgraph %d: dropped %+v, want %+v", i, d, wantD)
			}
			kept := subgraphKeys(got)
			for k := range subgraphKeys(sg) {
				if kept[k] == gone[k] {
					t.Fatalf("subgraph %d: %s kept %t, taken out %t", i, k, kept[k], gone[k])
				}
			}
			for _, el := range got.Elements {
				id, _ := sg.ElementID(el.Key)
				was := sg.Elements[id]
				if !reflect.DeepEqual(el.Pieces, was.Pieces) || el.Class != was.Class || el.Subject != was.Subject || el.Rule != was.Rule || len(el.Paths) != len(was.Paths) {
					t.Fatalf("subgraph %d: %s changed:\n%+v\nwas\n%+v", i, el.Key, el, was)
				}
				for p := range el.Paths {
					for l := range el.Paths[p] {
						if got.Elements[el.Paths[p][l]].Key != sg.Elements[was.Paths[p][l]].Key {
							t.Fatalf("subgraph %d: %s: leg %d of path %d changed", i, el.Key, l, p)
						}
					}
				}
			}
			for _, e := range got.Entities {
				if drop[e.Fingerprint] && !e.Boundary {
					t.Fatalf("subgraph %d: the dropped entity %s is in the dictionary and is not a boundary", i, e.Fingerprint)
				}
			}
			check(t, inputOf(got), got)
			seen["taken out"] += len(sg.Elements) - len(got.Elements)
			seen["derived taken out"] += d.Derived
		}
	})
}
