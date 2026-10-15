package subgraph_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// a small window with every shape of piece: stable, appeared, disappeared,
// transient and flapping elements, and edges between them.
func TestBuildSmall(t *testing.T) {
	const w = 10
	p1, p2, p3, n1, h1 := pod(1), pod(2), pod(3), node(1), host(1)
	in := input(w,
		mk(w, ent(n1), sp(0, -1)),
		mk(w, ent(p1), sp(0, -1)),
		mk(w, ent(p2), sp(0, 4)),
		mk(w, ent(p3), sp(6, -1)),
		mk(w, ent(h1), sp(2, 3), sp(5, 8)),
		mk(w, sched(p1, n1), sp(0, -1)),
		mk(w, sched(p2, n1), sp(0, 4)),
		mk(w, sched(p3, n1), sp(6, -1)),
	)
	sg := build(t, in)
	check(t, in, sg)

	if sg.Basis != subgraph.BasisDerived || sg.PolicyVersion != subgraph.PolicyVersion || sg.Token != 7 || !sg.From.Equal(at(0)) || !sg.To.Equal(at(w)) {
		t.Errorf("header %v %d %d %s %s", sg.Basis, sg.PolicyVersion, sg.Token, sg.From, sg.To)
	}
	wantKeys := []string{
		ent(n1).String(), ent(p1).String(), ent(p2).String(), ent(p3).String(), ent(h1).String(),
		sched(p1, n1).String(), sched(p2, n1).String(), sched(p3, n1).String(),
	}
	// Entities sort by type name, then hash; edges after all entities.
	if got := sortedKeys(sg); len(got) != len(wantKeys) {
		t.Errorf("elements %v", got)
	}
	for i := 1; i < len(sg.Elements); i++ {
		if sg.Elements[i-1].Key.Compare(sg.Elements[i].Key) >= 0 {
			t.Errorf("elements %d and %d are not ascending", i-1, i)
		}
	}
	if sg.Elements[len(sg.Elements)-1].Key.Kind != subgraph.KindEdge || sg.Elements[0].Key.Kind != subgraph.KindEntity {
		t.Errorf("entities do not come before edges")
	}
	// The dictionary is every entity a key names, ascending, none a boundary.
	if len(sg.Entities) != 5 {
		t.Fatalf("dictionary %+v", sg.Entities)
	}
	for i, e := range sg.Entities {
		if e.Boundary {
			t.Errorf("entity %s is a boundary but has an existence element", e.Fingerprint)
		}
		if i > 0 && store.CompareFingerprints(sg.Entities[i-1].Fingerprint, e.Fingerprint) >= 0 {
			t.Errorf("dictionary entries %d and %d are not ascending", i-1, i)
		}
		if id, ok := sg.EntityID(e.Fingerprint); !ok || int(id) != i {
			t.Errorf("EntityID(%s) = %d, %t", e.Fingerprint, id, ok)
		}
	}
	if _, ok := sg.EntityID(pod(99)); ok {
		t.Error("EntityID found an entity that is not in the dictionary")
	}
	if _, ok := sg.ElementID(ent(pod(99))); ok {
		t.Error("ElementID found an element that is not there")
	}
}

// Deltas are ordered by time, then entity before edge before derived subject,
// then subject, then appearance first; and carry the states they invert.
func TestDeltaOrderAndStates(t *testing.T) {
	const w = 20
	p1, p2, n1 := pod(1), pod(2), node(1)
	gone := span{start: 0, end: 5, reason: subgraph.EndReasonLivenessExpiry}
	in := input(w,
		mk(w, ent(n1), sp(0, -1)),
		mk(w, ent(p1), sp(0, -1)),
		// p2 disappears for good at 5 by expiry, and reappears at 5 (never: gaps
		// are real), so it leaves at 5 and returns at 9 after a producer delete.
		mk(w, ent(p2), gone, sp(9, 12), sp(15, -1)),
		mk(w, sched(p1, n1), span{start: 5, end: 9, reason: subgraph.EndReasonCascade}, sp(12, -1)),
		mk(w, derived("blast_radius.reach", "a"), span{start: 5, end: 7, reason: subgraph.EndReasonReachEnded}),
		mk(w, derived("blast_radius.reach", "b"), span{start: 5, end: 7, reason: subgraph.EndReasonUnattributed}),
	)
	sg := build(t, in)
	check(t, in, sg)

	type row struct {
		at       uint64
		kind     subgraph.Kind
		appear   bool
		from, to subgraph.State
	}
	sec := func(n int) uint64 { return uint64(n) * uint64(time.Second) }
	absent := func(r subgraph.EndReason) subgraph.State { return subgraph.State{Reason: r} }
	present := subgraph.State{Present: true}
	var got []row
	for _, d := range sg.Deltas {
		got = append(got, row{d.At, sg.Elements[d.Element].Key.Kind, d.Appear(), d.Before, d.After})
	}
	want := []row{
		// 5: the entity p2 leaves (expiry), the edge appears (before: not seen),
		// then the two derived subjects appear, in key order.
		{sec(5), subgraph.KindEntity, false, present, absent(subgraph.EndReasonLivenessExpiry)},
		{sec(5), subgraph.KindEdge, true, absent(subgraph.EndReasonNotSeenInWindow), present},
		{sec(5), subgraph.KindDerived, true, absent(subgraph.EndReasonNotSeenInWindow), present},
		{sec(5), subgraph.KindDerived, true, absent(subgraph.EndReasonNotSeenInWindow), present},
		{sec(7), subgraph.KindDerived, false, present, absent(subgraph.EndReasonReachEnded)},
		{sec(7), subgraph.KindDerived, false, present, absent(subgraph.EndReasonUnattributed)},
		// 9: p2 comes back after the expiry that ended its first piece; the edge
		// goes by the cascade.
		{sec(9), subgraph.KindEntity, true, absent(subgraph.EndReasonLivenessExpiry), present},
		{sec(9), subgraph.KindEdge, false, present, absent(subgraph.EndReasonCascade)},
		{sec(12), subgraph.KindEntity, false, present, absent(subgraph.EndReasonProducer)},
		{sec(12), subgraph.KindEdge, true, absent(subgraph.EndReasonCascade), present},
		{sec(15), subgraph.KindEntity, true, absent(subgraph.EndReasonProducer), present},
	}
	if !slices.Equal(got, want) {
		t.Errorf("deltas:\ngot:\n%v\nwant:\n%v", got, want)
	}
	// Two derived subjects at one instant are ordered by their keys.
	var detail []string
	for _, d := range sg.Deltas {
		if d.At == sec(5) && sg.Elements[d.Element].Key.Kind == subgraph.KindDerived {
			detail = append(detail, sg.Elements[d.Element].Key.Detail)
		}
	}
	expectEqual(t, "derived subjects at 5", detail, []string{"a", "b"})
	// No disappearance carries unknown, and no appearance does.
	for _, d := range sg.Deltas {
		if r := d.After.Reason; !d.After.Present && (r == subgraph.EndReasonUnknown || r == subgraph.EndReasonNotSeenInWindow) {
			t.Errorf("a disappearance carries %s", r)
		}
	}
}

// A subject that returns shows, as the before-state of each appearance, how its
// previous piece ended.
func TestAppearanceBeforeStateIsThePreviousEnd(t *testing.T) {
	const w = 12
	reasons := []subgraph.EndReason{
		subgraph.EndReasonProducer, subgraph.EndReasonLivenessExpiry, subgraph.EndReasonCascade,
		subgraph.EndReasonRetention, subgraph.EndReasonOperator,
	}
	for _, r := range reasons {
		in := input(w, mk(w, ent(pod(1)), span{start: 0, end: 3, reason: r}, sp(6, -1)))
		sg := build(t, in)
		check(t, in, sg)
		if len(sg.Deltas) != 2 || sg.Deltas[1].Before != (subgraph.State{Reason: r}) || sg.Deltas[0].After != (subgraph.State{Reason: r}) {
			t.Errorf("reason %s: deltas %+v", r, sg.Deltas)
		}
	}
}

// Pieces carry their end reasons, Before, producers and class, as the interval
// list of an element.
func TestIntervalLists(t *testing.T) {
	const w = 10
	p := pod(1)
	in := input(w, mk(w, ent(p),
		span{start: 0, end: 2, reason: subgraph.EndReasonCascade, before: true, producers: []lifecycle.Producer{"a", "b"}},
		span{start: 4, end: -1, producers: []lifecycle.Producer{"b"}}))
	sg := build(t, in)
	check(t, in, sg)
	if len(sg.Elements) != 1 {
		t.Fatalf("%d elements", len(sg.Elements))
	}
	el := sg.Elements[0]
	sec := func(n int) uint64 { return uint64(n) * uint64(time.Second) }
	want := []subgraph.Piece{
		{Start: 0, End: sec(2), Before: true, EndReason: subgraph.EndReasonCascade, Producers: []lifecycle.Producer{"a", "b"}},
		{Start: sec(4), End: sec(10), EndReason: subgraph.EndReasonUnknown, Producers: []lifecycle.Producer{"b"}},
	}
	if fmt.Sprint(el.Pieces) != fmt.Sprint(want) || el.Class != subgraph.ClassFlapping || el.Transitions != 2 ||
		el.Subject != subgraph.SubjectAsserted || el.Rule != subgraph.RuleExistence {
		t.Errorf("element %+v, want pieces %+v", el, want)
	}
	// The output shares nothing with the input.
	in.Sources[0].Pieces[0].Producers[0] = "z"
	if el.Pieces[0].Producers[0] != "a" {
		t.Error("the output shares producers with the input")
	}
}

// The labels: the subject is asserted for an entity and an edge of a stored
// relation, derived for an edge of a derived relation and for a derived key.
func TestLabels(t *testing.T) {
	const w = 4
	h1, h2, n1, p1 := host(1), host(2), node(1), pod(1)
	same := subgraph.EdgeKey(h1, h2, catalog.SameAs)
	in := input(w,
		mk(w, ent(p1), sp(0, -1)),
		mk(w, sched(p1, n1), sp(0, -1)),
		mk(w, runsOn(n1, h1), sp(0, -1)),
		mk(w, same, sp(0, -1)),
		mk(w, derived("pair.shared_node", "x"), sp(0, -1)),
		mk(w, derived("pair.shared_node", "y"), sp(0, -1)),
	)
	sg := build(t, in)
	check(t, in, sg)
	got := map[subgraph.Key][2]string{}
	for _, el := range sg.Elements {
		got[el.Key] = [2]string{el.Subject.String(), el.Rule}
	}
	want := map[subgraph.Key][2]string{
		ent(p1):                          {"asserted", "existence"},
		sched(p1, n1):                    {"asserted", "existence"},
		runsOn(n1, h1):                   {"asserted", "existence"},
		same:                             {"derived", "same_as"},
		derived("pair.shared_node", "x"): {"derived", "pair.shared_node"},
		derived("pair.shared_node", "y"): {"derived", "pair.shared_node"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("labels:\ngot:  %v\nwant: %v", got, want)
	}
	// Two derived subjects of one rule with one detail are one key, refused.
	dup := input(w, mk(w, derived("pair.shared_node", "x"), sp(0, -1)), mk(w, derived("pair.shared_node", "x"), sp(1, -1)))
	if _, err := subgraph.Build(dup); err == nil {
		t.Error("two sources with one derived key were built")
	}
}

// The quarantine section: a quarantined entity and an uncascaded edge are in
// the dictionary and the static section, in no keyframe and no delta, and the
// edge's transitions are not in E.
func TestStaticQuarantineSection(t *testing.T) {
	const w = 10
	n1, h1, p1, h2 := node(1), host(1), pod(1), host(2)
	qe := &store.QuarantineError{Entity: h1, Layer: catalog.L1}
	in := input(w,
		mk(w, ent(n1), sp(0, -1)),
		mk(w, ent(p1), sp(2, 6)),
		mk(w, sched(p1, n1), sp(2, 6)),
		subgraph.Source{Key: ent(h1), Quarantine: qe},
		// the edge is uncascaded by its quarantined target h1, and flaps.
		withUncascaded(mk(w, runsOn(n1, h1), sp(0, 3), sp(5, 8)), qe),
		// an edge to an unquarantined far host whose far end is a boundary.
		mk(w, runsOn(n1, h2), sp(0, -1)),
	)
	sg := build(t, in)
	check(t, in, sg)

	if len(sg.Quarantine.Entities) != 1 || sg.Quarantine.Entities[0].Fingerprint != h1 || sg.Quarantine.Entities[0].Cause != qe {
		t.Errorf("quarantined entities %+v", sg.Quarantine.Entities)
	}
	if len(sg.Quarantine.Edges) != 1 || sg.Quarantine.Edges[0].Key != runsOn(n1, h1) || sg.Quarantine.Edges[0].Cause != qe ||
		len(sg.Quarantine.Edges[0].Pieces) != 2 || sg.Quarantine.Edges[0].Transitions != 3 {
		t.Errorf("uncascaded edges %+v", sg.Quarantine.Edges)
	}
	for _, k := range []subgraph.Key{ent(h1), runsOn(n1, h1)} {
		if _, ok := sg.ElementID(k); ok {
			t.Errorf("%s is an element", k)
		}
	}
	// Dictionary: h1 is quarantined, so not a boundary; h2 has only an edge.
	boundary := map[string]bool{}
	for _, e := range sg.Entities {
		boundary[e.Fingerprint.String()] = e.Boundary
	}
	if len(boundary) != 4 || boundary[h1.String()] || !boundary[h2.String()] || boundary[n1.String()] || boundary[p1.String()] {
		t.Errorf("boundary marks %v", boundary)
	}
	// E excludes the static edge three transitions: the plain elements have
	// 0 (n1), 2 (p1), 2 (its edge) and 0 (the edge to h2).
	if len(sg.Deltas) != 4 {
		t.Errorf("%d deltas, want 4", len(sg.Deltas))
	}
	for _, d := range sg.Deltas {
		if k := sg.Elements[d.Element].Key; k == runsOn(n1, h1) || k == ent(h1) {
			t.Errorf("a delta for the quarantined %s", k)
		}
	}
	for _, kf := range sg.Keyframes {
		if len(kf.Elements) > 4 {
			t.Errorf("keyframe %+v holds a static element", kf)
		}
	}
}

func withUncascaded(s subgraph.Source, qe *store.QuarantineError) subgraph.Source {
	s.Uncascaded = qe
	return s
}

// An element with no piece is not in the subgraph, and neither are the
// entities only it names.
func TestElementWithNoPieceIsDropped(t *testing.T) {
	const w = 5
	in := input(w, mk(w, ent(pod(1)), sp(0, -1)), mk(w, ent(pod(2))), mk(w, sched(pod(2), node(3))))
	sg := build(t, in)
	check(t, in, sg)
	if len(sg.Elements) != 1 || len(sg.Entities) != 1 || len(sg.Deltas) != 0 || len(sg.Keyframes) != 1 {
		t.Errorf("elements %d, entities %d, deltas %d, keyframes %d", len(sg.Elements), len(sg.Entities), len(sg.Deltas), len(sg.Keyframes))
	}
}

// An empty input is one empty keyframe at From.
func TestEmptyInput(t *testing.T) {
	sg := build(t, input(3))
	check(t, input(3), sg)
	if len(sg.Keyframes) != 1 || len(sg.Keyframes[0].Elements) != 0 || sg.Entries() != 0 || len(sg.Entities) != 0 {
		t.Errorf("%+v", sg)
	}
}

// The builder does not change its input.
func TestBuildLeavesTheInputAlone(t *testing.T) {
	const w = 6
	in := input(w, mk(w, ent(pod(2)), sp(1, 3)), mk(w, ent(pod(1)), sp(0, -1)))
	in.Sources[0].Pieces[0].Producers = []lifecycle.Producer{"a"}
	before := fmt.Sprint(in.Sources)
	build(t, in)
	if fmt.Sprint(in.Sources) != before {
		t.Error("the input changed")
	}
}

// The builder refuses an input that breaks its rules, and says why.
func TestBuildRefusesInvalidInput(t *testing.T) {
	const w = 10
	good := func() subgraph.Source { return mk(w, ent(pod(1)), sp(2, 5)) }
	cases := map[string]func() subgraph.Input{
		"an empty window": func() subgraph.Input { in := input(w, good()); in.To = in.From; return in },
		"a window before the store's times": func() subgraph.Input {
			in := input(w, good())
			in.From = time.Unix(-5, 0)
			return in
		},
		"the zero fingerprint": func() subgraph.Input { return input(w, mk(w, subgraph.EntityKey(identity.Fingerprint{}), sp(2, 5))) },
		"an unknown relation": func() subgraph.Input {
			return input(w, mk(w, subgraph.EdgeKey(pod(1), node(1), "no_such_relation"), sp(2, 5)))
		},
		"a relation that does not connect the types": func() subgraph.Input {
			return input(w, mk(w, subgraph.EdgeKey(node(1), pod(1), catalog.ScheduledOn), sp(2, 5)))
		},
		"an entity key with a target": func() subgraph.Input {
			k := ent(pod(1))
			k.B = node(1)
			return input(w, mk(w, k, sp(2, 5)))
		},
		"a derived key without a rule": func() subgraph.Input { return input(w, mk(w, derived("", "x"), sp(2, 5))) },
		"a derived key with a relation": func() subgraph.Input {
			k := derived("pair.shared_node", "x")
			k.Relation = catalog.ScheduledOn
			return input(w, mk(w, k, sp(2, 5)))
		},
		"a derived key without a first end": func() subgraph.Input {
			k := derived("pair.shared_node", "x")
			k.A = identity.Fingerprint{}
			return input(w, mk(w, k, sp(2, 5)))
		},
		"a derived key without a second end": func() subgraph.Input {
			k := derived("pair.shared_node", "x")
			k.B = identity.Fingerprint{}
			return input(w, mk(w, k, sp(2, 5)))
		},
		"a derived key of a rule outside the table": func() subgraph.Input {
			return input(w, mk(w, derived("pair.shared_galaxy", "x"), sp(2, 5)))
		},
		"a derived key of the rule of an edge": func() subgraph.Input {
			return input(w, mk(w, derived("same_as", "x"), sp(2, 5)))
		},
		"an edge key with a rule": func() subgraph.Input {
			k := sched(pod(1), node(1))
			k.Rule = "x"
			return input(w, mk(w, k, sp(2, 5)))
		},
		"an unknown kind":        func() subgraph.Input { return input(w, mk(w, subgraph.Key{Kind: 9}, sp(2, 5))) },
		"two sources of one key": func() subgraph.Input { return input(w, good(), good()) },
		"a piece before the window": func() subgraph.Input {
			s := good()
			s.Pieces[0].Start = at(-1)
			return input(w, s)
		},
		"a piece after the window": func() subgraph.Input {
			s := good()
			s.Pieces[0].End = at(w + 1)
			return input(w, s)
		},
		"an empty piece": func() subgraph.Input {
			s := good()
			s.Pieces[0].End = s.Pieces[0].Start
			return input(w, s)
		},
		"touching pieces": func() subgraph.Input {
			s := mk(w, ent(pod(1)), sp(2, 5), sp(5, 7))
			return input(w, s)
		},
		"pieces out of order": func() subgraph.Input {
			s := mk(w, ent(pod(1)), sp(6, 7), sp(2, 5))
			return input(w, s)
		},
		"before on a piece that starts after From": func() subgraph.Input {
			s := good()
			s.Pieces[0].Before = true
			return input(w, s)
		},
		"a piece reaching To that has a reason": func() subgraph.Input {
			s := mk(w, ent(pod(1)), sp(2, -1))
			s.Pieces[0].EndReason = subgraph.EndReasonProducer
			return input(w, s)
		},
		"a piece ending inside the window with no reason": func() subgraph.Input {
			s := good()
			s.Pieces[0].EndReason = subgraph.EndReasonUnknown
			return input(w, s)
		},
		"a piece ended by not seen": func() subgraph.Input {
			s := good()
			s.Pieces[0].EndReason = subgraph.EndReasonNotSeenInWindow
			return input(w, s)
		},
		"an end reason outside the table": func() subgraph.Input {
			s := good()
			s.Pieces[0].EndReason = 77
			return input(w, s)
		},
		"a derived-only reason on an asserted subject": func() subgraph.Input {
			s := good()
			s.Pieces[0].EndReason = subgraph.EndReasonReachEnded
			return input(w, s)
		},
		"unsorted producers": func() subgraph.Input {
			s := good()
			s.Pieces[0].Producers = []lifecycle.Producer{"b", "a"}
			return input(w, s)
		},
		"repeated producers": func() subgraph.Input {
			s := good()
			s.Pieces[0].Producers = []lifecycle.Producer{"a", "a"}
			return input(w, s)
		},
		"a wrong class": func() subgraph.Input {
			s := good()
			s.Class = subgraph.ClassStable
			return input(w, s)
		},
		"wrong transitions": func() subgraph.Input {
			s := good()
			s.Transitions = 1
			return input(w, s)
		},
		"a quarantined entity with pieces": func() subgraph.Input {
			s := good()
			s.Quarantine = &store.QuarantineError{Entity: pod(1)}
			return input(w, s)
		},
		"a quarantine of another entity": func() subgraph.Input {
			return input(w, subgraph.Source{Key: ent(pod(1)), Quarantine: &store.QuarantineError{Entity: pod(2)}})
		},
		"a quarantined edge": func() subgraph.Input {
			s := mk(w, sched(pod(1), node(1)))
			s.Quarantine = &store.QuarantineError{Entity: pod(1)}
			return input(w, s)
		},
		"an uncascaded entity": func() subgraph.Input {
			s := good()
			s.Uncascaded = &store.QuarantineError{Entity: pod(1)}
			return input(w, s)
		},
		"an uncascaded mark of a stranger": func() subgraph.Input {
			s := mk(w, sched(pod(1), node(1)), sp(2, 5))
			s.Uncascaded = &store.QuarantineError{Entity: node(2)}
			return input(w, s)
		},
		"both marks": func() subgraph.Input {
			s := mk(w, sched(pod(1), node(1)), sp(2, 5))
			s.Uncascaded = &store.QuarantineError{Entity: pod(1)}
			s.Quarantine = &store.QuarantineError{Entity: pod(1)}
			return input(w, s)
		},
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			sg, err := subgraph.Build(f())
			if err == nil {
				t.Fatalf("built %+v", sg)
			}
			if !errors.Is(err, store.ErrInvalid) {
				t.Errorf("the error does not wrap store.ErrInvalid: %v", err)
			}
		})
	}
}

// An entity that was read and found absent is not a boundary, though an edge
// names it; one that was not read is, and a read entity no key names is not in
// the dictionary.
func TestReadEntitiesAreNotBoundaries(t *testing.T) {
	const w = 10
	n1, h1, h2 := node(1), host(1), host(2)
	qe := &store.QuarantineError{Entity: h1, Layer: catalog.L1}
	srcs := []subgraph.Source{
		withUncascaded(mk(w, runsOn(n1, h1), sp(0, -1)), qe),
		mk(w, runsOn(n1, h2), sp(0, -1)),
	}
	marks := func(read ...identity.Fingerprint) map[identity.Fingerprint]bool {
		in := input(w, srcs...)
		in.Read = read
		sg := build(t, in)
		out := map[identity.Fingerprint]bool{}
		for _, e := range sg.Entities {
			out[e.Fingerprint] = e.Boundary
		}
		return out
	}
	want := func(m map[identity.Fingerprint]bool) string { return fmt.Sprint(m) }
	if got, exp := marks(), map[identity.Fingerprint]bool{n1: true, h1: true, h2: true}; want(got) != want(exp) {
		t.Errorf("nothing read: %v, want %v", got, exp)
	}
	if got, exp := marks(n1, pod(9)), map[identity.Fingerprint]bool{n1: false, h1: true, h2: true}; want(got) != want(exp) {
		t.Errorf("the node read: %v, want %v", got, exp)
	}
	in := input(w, srcs...)
	in.Read = []identity.Fingerprint{{}}
	if _, err := subgraph.Build(in); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a zero fingerprint read: %v", err)
	}
}
