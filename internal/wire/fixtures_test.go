package wire_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// fixture is a document that is written to testdata/golden as a .bin and its
// .json twin.
type fixture struct {
	name string
	doc  func(testing.TB) wire.Document
}

func fixtures() []fixture {
	return []fixture{
		{"minimal", func(testing.TB) wire.Document { return minimalDoc() }},
		{"scope", scopeDoc},
		{"coresidents", coresidentsDoc},
		{"extraction", extractionDoc},
		{"quarantine", quarantineDoc},
		{"window_over_2pow53", windowDoc},
	}
}

// minimalDoc is written as a literal, with nothing from the builder, so that the
// hand-decoded bytes in TestMinimalBytes stand alone: one host present in a
// window of 100 nanoseconds.
func minimalDoc() wire.Document {
	h := host(1)
	pol := fixturePolicy()
	pol.CatalogIDs = [32]byte{}
	t0 := time.Unix(0, 1000).UTC()
	return wire.Document{
		Query:  wire.Query{Kind: wire.QueryScope, Seeds: []identity.Fingerprint{h}, AsOf: store.Latest},
		Policy: pol,
		Subgraph: subgraph.Subgraph{
			Basis: subgraph.BasisDerived, PolicyVersion: 2, Token: 7, From: t0, To: t0.Add(100),
			Entities:  []subgraph.Entity{{Fingerprint: h}},
			Elements:  []subgraph.Element{{Key: ent(h), Subject: subgraph.SubjectAsserted, Rule: subgraph.RuleExistence, Class: subgraph.ClassStable, Pieces: []subgraph.Piece{{Start: 0, End: 100}}}},
			Keyframes: []subgraph.Keyframe{{At: 0, Elements: []uint32{0}}},
		},
	}
}

const (
	pApp  = lifecycle.Producer("k8s")
	pOtel = lifecycle.Producer("otel")
)

func document(kind wire.QueryKind, seeds []identity.Fingerprint, opts []wire.Option, asof uint64, sg subgraph.Subgraph) wire.Document {
	// The subgraph's policy version is written out, not read from the builder: a
	// version bump there must not change what a fixture says.
	sg.PolicyVersion = 2
	return wire.Document{
		Query:    wire.Query{Kind: kind, Seeds: seeds, Options: opts, AsOf: asof},
		Policy:   fixturePolicy(),
		Subgraph: sg,
	}
}

// scopeDoc is a scope of one minute: a flapping pod, one that appears, one
// that is there for ten seconds, edges to two nodes (one of them a boundary: it
// is only named by an edge), a derived same_as edge, and enough deltas for
// keyframes after From.
func scopeDoc(tb testing.TB) wire.Document {
	w := 60 * sec
	srcs := []subgraph.Source{
		source(w, ent(pod(1)),
			pc{s: 0, e: 15 * sec, reason: subgraph.EndReasonLivenessExpiry, before: true, prod: []lifecycle.Producer{pApp, pOtel}},
			pc{s: 25 * sec, e: 45 * sec, reason: subgraph.EndReasonCascade, prod: []lifecycle.Producer{pApp}},
			pc{s: 50 * sec, prod: []lifecycle.Producer{pOtel}}),
		source(w, ent(pod(2)), pc{s: 10 * sec, prod: []lifecycle.Producer{pApp}}),
		source(w, ent(pod(3)), pc{s: 20 * sec, e: 30 * sec, reason: subgraph.EndReasonProducer, prod: []lifecycle.Producer{pApp}}),
		source(w, ent(node(1)), pc{s: 0, before: true, prod: []lifecycle.Producer{pApp}}),
		source(w, ent(host(1)), pc{s: 0, prod: []lifecycle.Producer{pOtel}}),
		source(w, sched(pod(1), node(1)), pc{s: 0, before: true, prod: []lifecycle.Producer{pApp}}),
		source(w, sched(pod(2), node(1)), pc{s: 10 * sec, prod: []lifecycle.Producer{pApp}}),
		source(w, sched(pod(3), node(1)), pc{s: 20 * sec, e: 30 * sec, reason: subgraph.EndReasonProducer, prod: []lifecycle.Producer{pApp}}),
		source(w, sched(pod(4), node(2)), pc{s: 40 * sec, prod: []lifecycle.Producer{pApp, pOtel}}),
		source(w, subgraph.EdgeKey(host(1), host(2), catalog.SameAs), pc{s: 5 * sec, e: 35 * sec, reason: subgraph.EndReasonReachEnded}),
	}
	sg := mustBuild(tb, subgraph.Input{Token: 1042, From: from, To: from.Add(time.Duration(w)), Sources: srcs})
	return document(wire.QueryScope, []identity.Fingerprint{pod(1), pod(2), pod(3)},
		[]wire.Option{{Name: "max_scope", Value: 1000}}, 1042, sg)
}

// coresidentsDoc has co-residents with legs and the legs that end with them, and
// the sections a co-residency search reports.
func coresidentsDoc(tb testing.TB) wire.Document {
	w := 60 * sec
	legSeedNode := sched(pod(1), node(1))
	legPeer2 := sched(pod(2), node(1))
	legPeer3 := sched(pod(3), node(1))
	co1 := derivedKey(subgraph.RuleCoSharedNode, pod(1), pod(2), "node1")
	co2 := derivedKey(subgraph.RuleCoSharedNode, pod(1), pod(3), "node1")
	srcs := []subgraph.Source{
		source(w, ent(pod(1)), pc{s: 0, before: true}),
		source(w, ent(pod(2)), pc{s: 10 * sec}),
		source(w, ent(pod(3)), pc{s: 5 * sec, e: 30 * sec, reason: subgraph.EndReasonProducer}),
		source(w, ent(node(1)), pc{s: 0, before: true}),
		source(w, legSeedNode, pc{s: 0, e: 40 * sec, reason: subgraph.EndReasonCascade, before: true, prod: []lifecycle.Producer{pApp}}),
		source(w, legPeer2, pc{s: 10 * sec, prod: []lifecycle.Producer{pApp}}),
		source(w, legPeer3, pc{s: 5 * sec, e: 30 * sec, reason: subgraph.EndReasonProducer, prod: []lifecycle.Producer{pApp}}),
		withPaths(source(w, co1, pc{
			s: 10 * sec, e: 40 * sec, reason: subgraph.EndReasonReachEnded,
			ends: []subgraph.LegEnd{{Path: 0, Leg: 0, Reason: subgraph.EndReasonCascade}},
		}),
			[]subgraph.Key{legSeedNode, legPeer2}),
		withPaths(source(w, co2, pc{
			s: 5 * sec, e: 30 * sec, reason: subgraph.EndReasonReachEnded,
			ends: []subgraph.LegEnd{{Path: 0, Leg: 1, Reason: subgraph.EndReasonProducer}},
		}),
			[]subgraph.Key{legSeedNode, legPeer3}),
	}
	secs := subgraph.Sections{
		Truncated: subgraph.Truncation{
			Nodes: []identity.Fingerprint{node(2)}, Hosts: []identity.Fingerprint{host(1)},
			PerVia:  []subgraph.ViaCount{{Via: node(1), Mechanism: subgraph.MechanismSharedNode, Dropped: 3}},
			Results: 2,
		},
		Unattributed: []subgraph.Unattributed{
			{Subject: node(2), Reason: subgraph.UnattributedNoHost, Spans: []subgraph.Span{span(w, 5*sec, 35*sec, false), span(w, 50*sec, 0, false)}},
			{
				SeedPod: pod(1), Subject: pod(3), Reason: subgraph.UnattributedSeveralClaims,
				Relation: catalog.ScheduledOn, Target: catalog.K8sNode,
				Spans:      []subgraph.Span{span(w, 0, 10*sec, true)},
				Candidates: []identity.Fingerprint{node(1), node(2)},
			},
		},
		Suppressed: []subgraph.Suppressed{
			suppressed(w, derivedKey(subgraph.RuleCoSharedNode, pod(1), pod(4), "node1"), subgraph.SuppressedBelowMinOverlap, [2]int{},
				span(w, 12*sec, 14*sec, false)),
		},
	}
	sg := mustBuild(tb, subgraph.Input{Token: 77, From: from, To: from.Add(time.Duration(w)), Sources: srcs, Sections: secs})
	return document(wire.QueryCoResidents, []identity.Fingerprint{pod(1)},
		[]wire.Option{{Name: "max_nodes", Value: 50}, {Name: "min_overlap_ns", Value: int64(5 * sec)}}, store.Latest, sg)
}

func withPaths(s subgraph.Source, legs ...[]subgraph.Key) subgraph.Source {
	s.Paths = legs
	return s
}

// extractionDoc has pairs through instances, the pod sections, an anchor and the
// ubiquity judgment, with the quarantined edge a pod section names.
func extractionDoc(tb testing.TB) wire.Document {
	w := 60 * sec
	legA := instOf(inst(1), svc(1))
	legB := instOf(inst(2), svc(2))
	legC := instOf(inst(3), svc(3))
	pairPod := derivedKey(subgraph.RulePairSharedPod, svc(1), svc(2), "pod1")
	pairNode := derivedKey(subgraph.RulePairSharedNode, svc(1), svc(3), "node1")
	quarantinedEdge := sched(pod(9), node(9))
	srcs := []subgraph.Source{
		source(w, ent(svc(1)), pc{s: 0, before: true}),
		source(w, legA, pc{s: 0, before: true, prod: []lifecycle.Producer{pOtel}}),
		source(w, legB, pc{s: 0, e: 50 * sec, reason: subgraph.EndReasonLivenessExpiry, before: true, prod: []lifecycle.Producer{pOtel}}),
		source(w, legC, pc{s: 20 * sec, prod: []lifecycle.Producer{pApp}}),
		withPaths(source(w, pairPod, pc{
			s: 0, e: 50 * sec, reason: subgraph.EndReasonReachEnded, before: true,
			ends: []subgraph.LegEnd{{Path: 0, Leg: 1, Reason: subgraph.EndReasonLivenessExpiry}},
		}),
			[]subgraph.Key{legA, legB}),
		withPaths(source(w, pairNode, pc{s: 20 * sec}), []subgraph.Key{legA, legC}),
		{Key: ent(node(9)), Class: subgraph.ClassNone, Quarantine: &store.QuarantineError{Entity: node(9), Layer: catalog.L2}},
		func() subgraph.Source {
			s := source(w, quarantinedEdge, pc{s: 0, e: 25 * sec, reason: subgraph.EndReasonProducer, before: true})
			s.Uncascaded = &store.QuarantineError{Entity: node(9), Layer: catalog.L2}
			return s
		}(),
	}
	secs := subgraph.Sections{
		Unmapped: []subgraph.Unmapped{{Pod: pod(1), Spans: []subgraph.Span{span(w, 0, 20*sec, true)}}},
		Suppressed: []subgraph.Suppressed{
			suppressed(w, derivedKey(subgraph.RulePairSharedNode, svc(1), svc(4), "node1"), subgraph.SuppressedUbiquitous, [2]int{3, 4},
				span(w, 0, 0, true)),
		},
		Pods: []subgraph.PodSection{
			{
				Seed: pod(1),
				Truncated: subgraph.Truncation{
					Members: []identity.Fingerprint{svc(5)}, Instances: []identity.Fingerprint{inst(9)},
					Frontier: []subgraph.FrontierCut{{Depth: 2, Dropped: []identity.Fingerprint{node(5), node(6)}}},
					Pairs:    1, SuppressedDropped: 2,
				},
				Quarantined: []subgraph.Key{quarantinedEdge, ent(node(9))},
				Suppressed: []subgraph.Suppressed{
					suppressed(w, derivedKey(subgraph.RulePairSharedHost, svc(1), svc(6), "host1"), subgraph.SuppressedBelowMinOverlap, [2]int{},
						span(w, 30*sec, 31*sec, false), span(w, 40*sec, 42*sec, false)),
				},
			},
			{Seed: pod(2)},
		},
		Anchor:   subgraph.AnchorOnHost,
		Ubiquity: subgraph.Ubiquity{Judged: true, FrontierNodes: 4},
	}
	sg := mustBuild(tb, subgraph.Input{Token: 9001, From: from, To: from.Add(time.Duration(w)), Sources: srcs, Sections: secs})
	return document(wire.QueryExtraction, []identity.Fingerprint{svc(1)},
		[]wire.Option{{Name: "anchored", Value: 1}, {Name: "max_pairs", Value: 100}}, 9001, sg)
}

// quarantineDoc has a quarantined entity with its collision, one whose collision
// carried no detail, and uncascaded edges, with and without a collision.
func quarantineDoc(tb testing.TB) wire.Document {
	w := 60 * sec
	collision := &lifecycle.CloneCollisionError{
		StaleBoot: "boot-a", ObservedAt: from.Add(20 * time.Second), NewerBoot: "boot-é", NewerFirstSeen: from.Add(10 * time.Second),
	}
	quarantinedEdge := func(key subgraph.Key, cause *store.QuarantineError, pcs ...pc) subgraph.Source {
		s := source(w, key, pcs...)
		s.Uncascaded = cause
		return s
	}
	srcs := []subgraph.Source{
		source(w, ent(pod(5)), pc{s: 0, before: true}),
		source(w, ent(node(1)), pc{s: 0, before: true}),
		{Key: ent(pod(6)), Quarantine: &store.QuarantineError{Entity: pod(6), Layer: catalog.L2, Collision: collision}},
		{Key: ent(node(7)), Quarantine: &store.QuarantineError{Entity: node(7), Layer: catalog.L2}},
		quarantinedEdge(sched(pod(5), node(7)), &store.QuarantineError{Entity: node(7), Layer: catalog.L2},
			pc{s: 0, e: 30 * sec, reason: subgraph.EndReasonProducer, before: true, prod: []lifecycle.Producer{pApp}},
			pc{s: 40 * sec}),
		quarantinedEdge(sched(pod(6), node(1)), &store.QuarantineError{Entity: pod(6), Layer: catalog.L2, Collision: collision},
			pc{s: 15 * sec, prod: []lifecycle.Producer{pOtel}}),
	}
	sg := mustBuild(tb, subgraph.Input{Token: 5, From: from, To: from.Add(time.Duration(w)), Sources: srcs})
	return document(wire.QueryScope, []identity.Fingerprint{pod(5), pod(6)}, nil, store.Latest, sg)
}

// windowDoc is a window of 120 days, 1.04e16 nanoseconds, over 2^53 (9.007e15),
// with pieces, deltas and spans whose relative times pass 2^53 and one whose
// length does.
func windowDoc(tb testing.TB) wire.Document {
	w := 120 * 24 * 3600 * sec
	if w <= pow53 {
		tb.Fatalf("the window of %d ns is not over 2^53", w)
	}
	leg := sched(pod(1), node(1))
	reach := derivedKey(subgraph.RuleReach, host(1), pod(1), "")
	srcs := []subgraph.Source{
		source(w, ent(host(1)), pc{s: 0, before: true}),
		source(w, ent(pod(1)),
			pc{s: 0, e: pow53 + 7, reason: subgraph.EndReasonProducer, before: true, prod: []lifecycle.Producer{pApp}},
			pc{s: pow53 + 1000, prod: []lifecycle.Producer{pApp}}),
		source(w, ent(pod(2)), pc{s: pow53 + 12345, e: w - 1, reason: subgraph.EndReasonOperator}),
		source(w, ent(node(1)), pc{s: 0, before: true}),
		source(w, leg, pc{s: 1, e: pow53 + 7, reason: subgraph.EndReasonProducer}, pc{s: pow53 + 1000, prod: []lifecycle.Producer{pOtel}}),
		withPaths(source(w, reach, pc{s: 2, e: pow53 + 5, reason: subgraph.EndReasonReachEnded}, pc{s: pow53 + 2000}), []subgraph.Key{leg}),
	}
	secs := subgraph.Sections{
		Truncated: subgraph.Truncation{
			Depth:    []identity.Fingerprint{pod(2)},
			Frontier: []subgraph.FrontierCut{{Depth: 2, Dropped: []identity.Fingerprint{pod(3)}}},
		},
		Unattributed: []subgraph.Unattributed{
			{Subject: node(1), Reason: subgraph.UnattributedSeveralHosts, Spans: []subgraph.Span{span(w, pow53+1, 0, false)}},
		},
	}
	sg := mustBuild(tb, subgraph.Input{Token: 42, From: from, To: from.Add(time.Duration(w)), Sources: srcs, Sections: secs})
	return document(wire.QueryBlastRadius, []identity.Fingerprint{host(1)}, []wire.Option{{Name: "depth", Value: 3}}, 41, sg)
}
