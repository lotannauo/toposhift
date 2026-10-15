package subgraph_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

const pw = 30

var (
	legA = sched(pod(1), node(1))
	legB = sched(pod(2), node(1))
)

// reachEnded is a derived piece that ends by the end of an intersection.
func reachEnded(start, end int) span {
	return span{start: start, end: end, reason: subgraph.EndReasonReachEnded}
}

// withPath is a derived source over its pieces with the paths and the ends of
// each piece.
func withPath(key subgraph.Key, paths [][]subgraph.Key, ends [][]subgraph.LegEnd, spans ...span) subgraph.Source {
	s := mk(pw, key, spans...)
	s.Paths = paths
	for i := range ends {
		s.Pieces[i].Ends = ends[i]
	}
	return s
}

func coKeyOf() subgraph.Key {
	return subgraph.Key{Kind: subgraph.KindDerived, A: pod(1), B: pod(2), Rule: subgraph.RuleCoSharedNode, Detail: "via=n1"}
}

func goodPaths() subgraph.Input {
	return input(pw,
		mk(pw, legA, sp(0, 20)), mk(pw, legB, sp(5, 15)),
		withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}},
			[][]subgraph.LegEnd{{{Path: 0, Leg: 1, Reason: subgraph.EndReasonProducer}}}, reachEnded(5, 15)),
	)
}

// A derived element has the legs it is made of as element ids, and the legs
// that end where a piece does; the span is checked against the legs' own
// pieces, never taken on trust.
func TestDerivedPathsAndEnds(t *testing.T) {
	in := goodPaths()
	sg := build(t, in)
	check(t, in, sg)
	id, ok := sg.ElementID(coKeyOf())
	if !ok {
		t.Fatal("no derived element")
	}
	el := sg.Elements[id]
	a, _ := sg.ElementID(legA)
	b, _ := sg.ElementID(legB)
	if len(el.Paths) != 1 || !slices.Equal(el.Paths[0], []uint32{a, b}) {
		t.Errorf("paths %v, want [[%d %d]]", el.Paths, a, b)
	}
	if want := []subgraph.LegEnd{{Path: 0, Leg: 1, Reason: subgraph.EndReasonProducer}}; !slices.Equal(el.Pieces[0].Ends, want) {
		t.Errorf("ends %+v", el.Pieces[0].Ends)
	}
	// The ends are the input's own: changing the input after the build changes
	// nothing in it.
	in.Sources[2].Pieces[0].Ends[0].Reason = subgraph.EndReasonCascade
	if sg.Elements[id].Pieces[0].Ends[0].Reason != subgraph.EndReasonProducer {
		t.Error("the subgraph shares the input's ends")
	}
	// A leg that goes on but is ambiguous is unattributed.
	in = goodPaths()
	in.Sources[2].Pieces[0].Ends[0].Reason = subgraph.EndReasonUnattributed
	if _, err := subgraph.Build(in); err != nil {
		t.Errorf("an unattributed leg end: %v", err)
	}
	// Two paths: the union of what each holds.
	k := coKeyOf()
	k.Rule = subgraph.RulePairSharedNode
	two := input(pw,
		mk(pw, legA, sp(0, 10)), mk(pw, legB, sp(0, 10)),
		mk(pw, sched(pod(3), node(1)), sp(8, 20)), mk(pw, sched(pod(4), node(1)), sp(8, 20)),
		withPath(k, [][]subgraph.Key{{legA, legB}, {sched(pod(3), node(1)), sched(pod(4), node(1))}},
			[][]subgraph.LegEnd{{{Path: 1, Leg: 0, Reason: subgraph.EndReasonCascade}}}, reachEnded(0, 20)),
	)
	sg = build(t, two)
	check(t, two, sg)
}

func TestDerivedPathsAreRefused(t *testing.T) {
	end := func(path, leg int, r subgraph.EndReason) subgraph.LegEnd {
		return subgraph.LegEnd{Path: path, Leg: leg, Reason: r}
	}
	reach := subgraph.Key{Kind: subgraph.KindDerived, A: pod(1), B: pod(2), Rule: subgraph.RuleReach}
	cases := map[string]func() subgraph.Input{
		"a leg that is not an element": func() subgraph.Input {
			return input(pw, mk(pw, legA, sp(0, 20)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}}, [][]subgraph.LegEnd{{end(0, 1, subgraph.EndReasonProducer)}}, reachEnded(5, 15)))
		},
		"a leg that is an entity": func() subgraph.Input {
			return input(pw, mk(pw, ent(pod(1)), sp(0, 20)),
				withPath(coKeyOf(), [][]subgraph.Key{{ent(pod(1))}}, [][]subgraph.LegEnd{{end(0, 0, subgraph.EndReasonProducer)}}, reachEnded(5, 15)))
		},
		"a path with no leg": func() subgraph.Input {
			return input(pw, withPath(coKeyOf(), [][]subgraph.Key{{}}, nil, sp(5, 30)))
		},
		"a span the legs do not hold": func() subgraph.Input {
			// The second leg exists only from 5 to 15, and the span runs to 18.
			return input(pw, mk(pw, legA, sp(0, 20)), mk(pw, legB, sp(5, 15)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}}, [][]subgraph.LegEnd{{end(0, 1, subgraph.EndReasonProducer)}}, reachEnded(5, 18)))
		},
		"a span before the legs": func() subgraph.Input {
			return input(pw, mk(pw, legA, sp(0, 20)), mk(pw, legB, sp(5, 15)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}}, [][]subgraph.LegEnd{{end(0, 1, subgraph.EndReasonProducer)}}, reachEnded(4, 15)))
		},
		"a span that is held by no single path": func() subgraph.Input {
			// Each path holds one half; no path holds the whole [0, 20) apart from
			// the instant 10, which neither leg of the other holds.
			return input(pw, mk(pw, legA, sp(0, 10)), mk(pw, legB, sp(12, 20)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA}, {legB}}, [][]subgraph.LegEnd{{end(1, 0, subgraph.EndReasonProducer)}}, reachEnded(0, 20)))
		},
		"paths on an entity": func() subgraph.Input {
			s := mk(pw, ent(pod(1)), sp(0, 20))
			s.Paths = [][]subgraph.Key{{legA}}
			return input(pw, mk(pw, legA, sp(0, 20)), s)
		},
		"a piece that ends and names no leg": func() subgraph.Input {
			return input(pw, mk(pw, legA, sp(0, 20)), mk(pw, legB, sp(5, 15)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}}, [][]subgraph.LegEnd{nil}, reachEnded(5, 15)))
		},
		"a piece that reaches To and names a leg": func() subgraph.Input {
			return input(pw, mk(pw, legA, sp(0, -1)), mk(pw, legB, sp(5, -1)),
				withPath(coKeyOf(), [][]subgraph.Key{{legA, legB}}, [][]subgraph.LegEnd{{end(0, 1, subgraph.EndReasonProducer)}}, span{start: 5, end: -1}))
		},
		"a reach that names a leg": func() subgraph.Input {
			return input(pw, mk(pw, legA, sp(0, 20)),
				withPath(reach, [][]subgraph.Key{{legA}}, [][]subgraph.LegEnd{{end(0, 0, subgraph.EndReasonProducer)}}, reachEnded(5, 15)))
		},
		"an end of a path that does not exist": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(1, 0, subgraph.EndReasonProducer)}
			return in
		},
		"an end of a leg that does not exist": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, 2, subgraph.EndReasonProducer)}
			return in
		},
		"an end of a negative leg": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, -1, subgraph.EndReasonProducer)}
			return in
		},
		"ends out of order": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, 1, subgraph.EndReasonProducer), end(0, 0, subgraph.EndReasonProducer)}
			return in
		},
		"an end named twice": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, 1, subgraph.EndReasonProducer), end(0, 1, subgraph.EndReasonCascade)}
			return in
		},
		"a leg end with no reason": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, 1, subgraph.EndReasonUnknown)}
			return in
		},
		"a leg end with the reach's reason": func() subgraph.Input {
			in := goodPaths()
			in.Sources[2].Pieces[0].Ends = []subgraph.LegEnd{end(0, 1, subgraph.EndReasonReachEnded)}
			return in
		},
		"leg ends on an asserted piece": func() subgraph.Input {
			s := mk(pw, legA, sp(0, 20))
			s.Pieces[0].Ends = []subgraph.LegEnd{end(0, 0, subgraph.EndReasonProducer)}
			return input(pw, s)
		},
	}
	for name, mkInput := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := subgraph.Build(mkInput())
			if !errors.Is(err, store.ErrInvalid) {
				t.Errorf("Build = %v, want a refusal wrapping ErrInvalid", err)
			}
		})
	}
	// The same inputs, made right, are accepted: each refusal above is the fault
	// it names.
	if _, err := subgraph.Build(goodPaths()); err != nil {
		t.Errorf("the good input: %v", err)
	}
	in := input(pw, mk(pw, legA, sp(0, 20)),
		withPath(reach, [][]subgraph.Key{{legA}}, nil, reachEnded(5, 15)))
	if _, err := subgraph.Build(in); err != nil {
		t.Errorf("a reach with a leg and no ends: %v", err)
	}
}

// A derived key's ends are its dictionary references; the rest of the identity
// is the detail.
func TestDerivedKeyEndsAreDictionaryEntries(t *testing.T) {
	in := goodPaths()
	sg := build(t, in)
	for _, fp := range []identity.Fingerprint{pod(1), pod(2)} {
		id, ok := sg.EntityID(fp)
		if !ok {
			t.Fatalf("%s is not in the dictionary", fp)
		}
		if !sg.Entities[id].Boundary {
			t.Errorf("%s is read by no one here and is not a boundary", fp)
		}
	}
	// Read, it is not a boundary.
	in.Read = []identity.Fingerprint{pod(1), pod(2)}
	sg = build(t, in)
	for _, e := range sg.Entities {
		if e.Fingerprint.Type() == catalog.K8sPod && e.Boundary {
			t.Errorf("%s is a boundary after being read", e.Fingerprint)
		}
	}
}

func section(width int) subgraph.Sections {
	return subgraph.Sections{
		Truncated: subgraph.Truncation{
			Nodes: []identity.Fingerprint{node(40)}, Hosts: []identity.Fingerprint{host(40)},
			Members: []identity.Fingerprint{svc(40)}, Instances: []identity.Fingerprint{inst(40)}, SeedPods: []identity.Fingerprint{pod(40)},
			Depth:    []identity.Fingerprint{pod(41)},
			Frontier: []subgraph.FrontierCut{{Depth: 1, Dropped: []identity.Fingerprint{pod(42)}}},
			PerVia:   []subgraph.ViaCount{{Via: node(41), Mechanism: subgraph.MechanismSharedNode, Dropped: 2}},
			Results:  3, Pairs: 4, MembersDropped: 5, SuppressedDropped: 6,
		},
		Unattributed: []subgraph.Unattributed{{
			SeedPod: pod(43), Subject: node(42), Reason: subgraph.UnattributedNoHost,
			Spans: []subgraph.Span{{Start: 0, End: 5 * uint64(time.Second), Before: true, EndReason: subgraph.EndReasonReachEnded}}, Candidates: []identity.Fingerprint{host(41), host(42)},
		}, {
			Subject: node(43), Reason: subgraph.UnattributedSeveralClaims, Relation: catalog.RunsOn, Target: catalog.Host,
			Spans: []subgraph.Span{{Start: 5 * uint64(time.Second), End: uint64(width) * uint64(time.Second)}}, Candidates: []identity.Fingerprint{host(43), host(44)},
		}},
		Unmapped: []subgraph.Unmapped{{Pod: pod(44), Spans: []subgraph.Span{{Start: 0, End: uint64(width) * uint64(time.Second)}}}},
		Suppressed: []subgraph.Suppressed{{
			Key:    subgraph.Key{Kind: subgraph.KindDerived, A: svc(41), B: svc(42), Rule: subgraph.RulePairSharedNode},
			Reason: subgraph.SuppressedUbiquitous, Coverage: [2]int{5, 6}, Class: subgraph.ClassStable,
			Spans: []subgraph.Span{{Start: 0, End: uint64(width) * uint64(time.Second), Before: true}},
		}},
		Pods: []subgraph.PodSection{{
			Seed:      pod(45),
			Truncated: subgraph.Truncation{Nodes: []identity.Fingerprint{node(44)}, Results: 1},
			Suppressed: []subgraph.Suppressed{{
				Key:    subgraph.Key{Kind: subgraph.KindDerived, A: pod(45), B: pod(46), Rule: subgraph.RuleCoSharedNode, Detail: "x"},
				Reason: subgraph.SuppressedBelowMinOverlap, Class: subgraph.ClassTransient, Transitions: 2,
				Spans: []subgraph.Span{{Start: uint64(time.Second), End: 2 * uint64(time.Second), EndReason: subgraph.EndReasonReachEnded}},
			}},
		}},
		Anchor:   subgraph.AnchorOnHost,
		Ubiquity: subgraph.Ubiquity{Judged: true, FrontierNodes: 6},
	}
}

// Every field of every section is carried, and every entity a section names is
// in the dictionary, as a boundary when nothing read it.
func TestSectionsAreCarriedWhole(t *testing.T) {
	in := input(pw, mk(pw, ent(pod(1)), sp(0, -1)))
	in.Sections = section(pw)
	sg := build(t, in)
	if got, want := sg.Sections, in.Sections; !sameSections(got, want) {
		t.Errorf("sections:\n%+v\nwant\n%+v", got, want)
	}
	for _, fp := range []identity.Fingerprint{
		node(40), host(40), svc(40), inst(40), pod(40), pod(41), pod(42), node(41), pod(43), node(42), host(41), host(42), node(43), host(43), host(44),
		pod(44), svc(41), svc(42), pod(45), node(44), pod(46),
	} {
		id, ok := sg.EntityID(fp)
		if !ok {
			t.Errorf("%s is named by a section and not in the dictionary", fp)
			continue
		}
		if !sg.Entities[id].Boundary {
			t.Errorf("%s is named by a section only and is not a boundary", fp)
		}
	}
	// The input is copied.
	in.Sections.Unattributed[0].Spans[0].End = 1
	in.Sections.Truncated.Nodes[0] = node(99)
	in.Sections.Pods[0].Suppressed[0].Spans[0].End = 1
	if sg.Sections.Unattributed[0].Spans[0].End == 1 || sg.Sections.Truncated.Nodes[0] == node(99) || sg.Sections.Pods[0].Suppressed[0].Spans[0].End == 1 {
		t.Error("the subgraph shares the input's sections")
	}
}

func sameSections(a, b subgraph.Sections) bool {
	return fmtSections(a) == fmtSections(b)
}

func TestSectionsAreRefused(t *testing.T) {
	sec := func(f func(s *subgraph.Sections)) func() subgraph.Input {
		return func() subgraph.Input {
			in := input(pw, mk(pw, ent(pod(1)), sp(0, -1)))
			in.Sections = section(pw)
			f(&in.Sections)
			return in
		}
	}
	width := uint64(pw) * uint64(time.Second)
	cases := map[string]func() subgraph.Input{
		"a span past the window":                 sec(func(s *subgraph.Sections) { s.Unmapped[0].Spans[0].End = width + 1 }),
		"a span that ends at To and says it was": sec(func(s *subgraph.Sections) { s.Unmapped[0].Spans[0].EndReason = subgraph.EndReasonReachEnded }),
		"a span that ends early and says nothing": sec(func(s *subgraph.Sections) {
			s.Unmapped[0].Spans[0].End = width - 1
		}),
		"an empty span":        sec(func(s *subgraph.Sections) { s.Unmapped[0].Spans[0].End = 0 }),
		"a span with no spans": sec(func(s *subgraph.Sections) { s.Unmapped[0].Spans = nil }),
		"touching spans": sec(func(s *subgraph.Sections) {
			s.Unmapped[0].Spans = []subgraph.Span{{Start: 0, End: 5, EndReason: subgraph.EndReasonReachEnded}, {Start: 5, End: width}}
		}),
		"a before that starts after From": sec(func(s *subgraph.Sections) {
			s.Unmapped[0].Spans = []subgraph.Span{{Start: 3, End: width, Before: true}}
		}),
		"an unattributed reason outside the table": sec(func(s *subgraph.Sections) { s.Unattributed[0].Reason = 77 }),
		"claims with no relation":                  sec(func(s *subgraph.Sections) { s.Unattributed[1].Relation = "" }),
		"claims of an unknown type":                sec(func(s *subgraph.Sections) { s.Unattributed[1].Target = "no.such" }),
		"a relation on a placement":                sec(func(s *subgraph.Sections) { s.Unattributed[0].Relation = catalog.RunsOn }),
		"an unknown entity in a cut":               sec(func(s *subgraph.Sections) { s.Truncated.Nodes = []identity.Fingerprint{{}} }),
		"a zero per-via count":                     sec(func(s *subgraph.Sections) { s.Truncated.PerVia[0].Dropped = 0 }),
		"a per-via mechanism outside the table":    sec(func(s *subgraph.Sections) { s.Truncated.PerVia[0].Mechanism = 9 }),
		"a frontier cut with no entity":            sec(func(s *subgraph.Sections) { s.Truncated.Frontier[0].Dropped = nil }),
		"a negative count":                         sec(func(s *subgraph.Sections) { s.Truncated.Results = -1 }),
		"a suppressed entry that is an entity":     sec(func(s *subgraph.Sections) { s.Suppressed[0].Key = ent(pod(1)) }),
		"a suppressed entry without a reason":      sec(func(s *subgraph.Sections) { s.Suppressed[0].Reason = 0 }),
		"a suppressed entry of an unknown rule": sec(func(s *subgraph.Sections) {
			s.Suppressed[0].Key.Rule = "pair.shared_galaxy"
		}),
		"a coverage on a below-minimum entry": sec(func(s *subgraph.Sections) {
			s.Pods[0].Suppressed[0].Coverage = [2]int{1, 2}
		}),
		"a coverage over its frontier":        sec(func(s *subgraph.Sections) { s.Suppressed[0].Coverage = [2]int{7, 6} }),
		"a suppressed entry of a wrong class": sec(func(s *subgraph.Sections) { s.Suppressed[0].Class = subgraph.ClassTransient }),
		"two suppressed entries of one key": sec(func(s *subgraph.Sections) {
			s.Suppressed = append(s.Suppressed, s.Suppressed[0])
		}),
		"a suppressed entry that is an element's key": func() subgraph.Input {
			in := input(pw, mk(pw, derived(subgraph.RulePairSharedNode, "x"), sp(0, -1)))
			in.Sections.Suppressed = []subgraph.Suppressed{{
				Key: derived(subgraph.RulePairSharedNode, "x"), Reason: subgraph.SuppressedUbiquitous, Class: subgraph.ClassStable,
				Spans: []subgraph.Span{{Start: 0, End: width}},
			}}
			return in
		},
		"two pod sections of one seed": sec(func(s *subgraph.Sections) { s.Pods = append(s.Pods, s.Pods[0]) }),
		"a pod section quarantine that the static section lacks": sec(func(s *subgraph.Sections) {
			s.Pods[0].Quarantined = []subgraph.Key{runsOn(node(1), host(1))}
		}),
		"an anchor outside the table": sec(func(s *subgraph.Sections) { s.Anchor = 9 }),
		"a negative frontier":         sec(func(s *subgraph.Sections) { s.Ubiquity.FrontierNodes = -1 }),
	}
	for name, mkInput := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := subgraph.Build(mkInput())
			if !errors.Is(err, store.ErrInvalid) {
				t.Errorf("Build = %v, want a refusal wrapping ErrInvalid", err)
			}
		})
	}
	// A pod section may name a quarantined edge the static section holds.
	qe := &store.QuarantineError{Entity: host(1), Layer: catalog.L1}
	in := input(pw, mk(pw, ent(pod(1)), sp(0, -1)))
	e := mk(pw, runsOn(node(1), host(1)), sp(0, -1))
	e.Uncascaded = qe
	in.Sources = append(in.Sources, e)
	in.Sections = subgraph.Sections{Pods: []subgraph.PodSection{{Seed: pod(1), Quarantined: []subgraph.Key{runsOn(node(1), host(1))}}}}
	if _, err := subgraph.Build(in); err != nil {
		t.Errorf("a pod section naming a quarantined edge: %v", err)
	}
}
