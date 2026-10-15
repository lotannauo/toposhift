package wire_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// The generator draws a window of up to 30 steps, each step a nanosecond, a
// millisecond, a second or sixteen days, so that a window passes 2^53 nanoseconds
// about one time in four; a pool of entities, edges, and derived subjects whose
// one leg is an edge of the pool; pieces on the steps, an interior boundary a
// nanosecond off its step where a step is a millisecond or more; quarantined
// entities and uncascaded edges; every section; and a query echo and a policy
// vector. The document is made by the builder, so it is one a builder makes, and
// then given extra legs that the codec carries and the builder never checks.

var (
	units    = []uint64{1, uint64(time.Millisecond), sec, 400 * 3600 * sec}
	producer = []lifecycle.Producer{"a", "b", "c", "é"}
)

type gen struct {
	t     *rapid.T
	steps uint64
	unit  uint64
	width uint64
}

// at is the offset of step b, shifted by a nanosecond about one time in four
// when the step is not at either end of the window.
func (g *gen) at(b uint64) uint64 {
	d := b * g.unit
	if g.unit > 1 && b > 0 && b < g.steps && rapid.IntRange(0, 3).Draw(g.t, "shift") == 0 {
		d = d + uint64(rapid.IntRange(0, 1).Draw(g.t, "ns"))
		if rapid.Bool().Draw(g.t, "down") {
			d -= 2
		}
	}
	return d
}

// bounds draws 2k strictly ascending steps.
func (g *gen) bounds(k int) []uint64 {
	if k == 0 {
		return nil
	}
	raw := rapid.SliceOfNDistinct(rapid.Uint64Range(0, g.steps), 2*k, 2*k, func(x uint64) uint64 { return x }).Draw(g.t, "boundaries")
	slices.Sort(raw)
	return raw
}

func (g *gen) reason(derived bool) subgraph.EndReason {
	max := 5
	if derived {
		max = 7
	}
	return subgraph.EndReason(rapid.IntRange(1, max).Draw(g.t, "reason"))
}

func (g *gen) pieces(derived bool, maxK int) []pc {
	k := rapid.IntRange(0, maxK).Draw(g.t, "pieces")
	b := g.bounds(k)
	var out []pc
	for i := range k {
		p := pc{s: g.at(b[2*i]), before: b[2*i] == 0 && rapid.Bool().Draw(g.t, "before")}
		if b[2*i] == 0 {
			p.s = 0
		}
		if b[2*i+1] == g.steps {
			p.e = 0
		} else {
			p.e, p.reason = g.at(b[2*i+1]), g.reason(derived)
		}
		for _, name := range producer {
			if rapid.Bool().Draw(g.t, "producer") {
				p.prod = append(p.prod, name)
			}
		}
		out = append(out, p)
	}
	return out
}

func (g *gen) spans() []subgraph.Span {
	k := rapid.IntRange(1, 3).Draw(g.t, "spans")
	b := g.bounds(k)
	var out []subgraph.Span
	for i := range k {
		s, e := g.at(b[2*i]), g.at(b[2*i+1])
		if b[2*i] == 0 {
			s = 0
		}
		if b[2*i+1] == g.steps {
			e = 0
		}
		out = append(out, span(g.width, s, e, b[2*i] == 0 && rapid.Bool().Draw(g.t, "before")))
	}
	return out
}

func (g *gen) fp() identity.Fingerprint {
	return rapid.SampledFrom([]identity.Fingerprint{
		pod(1), pod(2), pod(3), pod(4), pod(5), pod(6), node(1), node(2), node(3), host(1), host(2),
		svc(1), svc(2), svc(3), inst(1), inst(2), inst(3), pod(70), node(70),
	}).Draw(g.t, "entity")
}

func (g *gen) fps(max int) []identity.Fingerprint {
	n := rapid.IntRange(0, max).Draw(g.t, "n entities")
	var out []identity.Fingerprint
	for range n {
		out = append(out, g.fp())
	}
	return out
}

func (g *gen) truncation() subgraph.Truncation {
	t := subgraph.Truncation{
		Nodes: g.fps(2), Hosts: g.fps(2), Members: g.fps(2), Instances: g.fps(2), SeedPods: g.fps(2), Depth: g.fps(2),
	}
	for range rapid.IntRange(0, 2).Draw(g.t, "frontier") {
		t.Frontier = append(t.Frontier, subgraph.FrontierCut{Depth: rapid.IntRange(1, 5).Draw(g.t, "depth"), Dropped: append(g.fps(2), g.fp())})
	}
	for range rapid.IntRange(0, 2).Draw(g.t, "per via") {
		t.PerVia = append(t.PerVia, subgraph.ViaCount{
			Via: g.fp(), Mechanism: subgraph.Mechanism(rapid.IntRange(1, 3).Draw(g.t, "mechanism")), Dropped: rapid.IntRange(1, 1<<20).Draw(g.t, "dropped"),
		})
	}
	if rapid.Bool().Draw(g.t, "counts") {
		t.Results, t.Pairs = rapid.IntRange(0, 1<<40).Draw(g.t, "results"), rapid.IntRange(0, 9).Draw(g.t, "pairs")
		t.MembersDropped, t.SuppressedDropped = rapid.IntRange(0, 300).Draw(g.t, "members dropped"), rapid.IntRange(0, 1<<35).Draw(g.t, "suppressed dropped")
	}
	return t
}

func (g *gen) suppressed(n *int) []subgraph.Suppressed {
	var out []subgraph.Suppressed
	for range rapid.IntRange(0, 2).Draw(g.t, "suppressed") {
		*n++
		rule := rapid.SampledFrom([]string{subgraph.RuleCoSharedNode, subgraph.RulePairSharedHost, subgraph.RuleReach}).Draw(g.t, "rule")
		reason := subgraph.SuppressReason(rapid.IntRange(1, 2).Draw(g.t, "suppress reason"))
		var cov [2]int
		if reason == subgraph.SuppressedUbiquitous {
			cov = [2]int{rapid.IntRange(0, 9).Draw(g.t, "cov"), rapid.IntRange(10, 99).Draw(g.t, "frontier")}
		}
		out = append(out, suppressed(g.width, derivedKey(rule, g.fp(), g.fp(), fmt.Sprintf("s%d-é", *n)), reason, cov, g.spans()...))
	}
	return out
}

func drawDoc(t *rapid.T) wire.Document {
	g := &gen{t: t, steps: rapid.Uint64Range(1, 30).Draw(t, "steps"), unit: rapid.SampledFrom(units).Draw(t, "unit")}
	g.width = g.steps * g.unit

	var srcs []subgraph.Source
	var legs []subgraph.Key
	entities := []identity.Fingerprint{pod(1), pod(2), pod(3), pod(4), node(1), node(2), host(1), host(2), svc(1), inst(1)}
	for _, fp := range entities {
		if rapid.Bool().Draw(t, "entity element") {
			srcs = append(srcs, source(g.width, ent(fp), g.pieces(false, 3)...))
		}
	}
	edges := []subgraph.Key{
		sched(pod(1), node(1)), sched(pod(2), node(1)), sched(pod(3), node(2)), sched(pod(4), node(2)),
		instOf(inst(1), svc(1)), instOf(inst(2), svc(2)), subgraph.EdgeKey(node(1), host(1), catalog.RunsOn),
		subgraph.EdgeKey(host(1), host(2), catalog.SameAs),
	}
	for _, k := range edges {
		if !rapid.Bool().Draw(t, "edge element") {
			continue
		}
		ps := g.pieces(k.Relation == catalog.SameAs, 3)
		srcs = append(srcs, source(g.width, k, ps...))
		if k.Relation != catalog.SameAs && len(ps) > 0 {
			legs = append(legs, k)
		}
	}
	if len(legs) > 0 {
		rules := []string{
			subgraph.RuleCoSharedNode, subgraph.RuleCoSharedHost, subgraph.RulePairSharedPod, subgraph.RulePairSharedNode,
			subgraph.RulePairSharedHost, subgraph.RuleReach,
		}
		n := rapid.IntRange(0, 3).Draw(t, "derived")
		for i := range n {
			leg := rapid.SampledFrom(legs).Draw(t, "leg")
			var legSrc subgraph.Source
			for _, s := range srcs {
				if s.Key == leg {
					legSrc = s
				}
			}
			rule := rapid.SampledFrom(rules).Draw(t, "rule")
			d := subgraph.Source{Key: derivedKey(rule, g.fp(), g.fp(), fmt.Sprintf("d%d-é", i)), Paths: [][]subgraph.Key{{leg}}}
			for _, p := range legSrc.Pieces {
				dp := subgraph.SourcePiece{Start: p.Start, End: p.End, Before: p.Before, EndReason: p.EndReason}
				if p.EndReason != subgraph.EndReasonUnknown {
					dp.EndReason = subgraph.EndReasonReachEnded
					if rule != subgraph.RuleReach {
						dp.Ends = []subgraph.LegEnd{{Path: 0, Leg: 0, Reason: p.EndReason}}
					}
				}
				d.Pieces = append(d.Pieces, dp)
			}
			d.Class, d.Transitions = legSrc.Class, legSrc.Transitions
			srcs = append(srcs, d)
		}
	}

	// Quarantine: entities with no element, and uncascaded edges to them.
	cause := func(fp identity.Fingerprint) *store.QuarantineError {
		q := &store.QuarantineError{Entity: fp, Layer: catalog.Layer(rapid.IntRange(1, 4).Draw(t, "layer"))}
		if rapid.Bool().Draw(t, "collision") {
			q.Collision = &lifecycle.CloneCollisionError{
				StaleBoot: "b-é", ObservedAt: from.Add(time.Duration(rapid.Int64Range(0, 1e12).Draw(t, "observed"))),
				NewerBoot: "n", NewerFirstSeen: from.Add(time.Duration(rapid.Int64Range(0, 1e12).Draw(t, "first seen"))),
			}
		}
		return q
	}
	var quarantined []subgraph.Key
	for _, fp := range []identity.Fingerprint{pod(70), node(70)} {
		if rapid.Bool().Draw(t, "quarantined") {
			srcs = append(srcs, subgraph.Source{Key: ent(fp), Quarantine: cause(fp)})
			quarantined = append(quarantined, ent(fp))
			if rapid.Bool().Draw(t, "uncascaded") {
				key := sched(pod(70), node(1))
				if fp == node(70) {
					key = sched(pod(5), node(70))
				}
				s := source(g.width, key, g.pieces(false, 2)...)
				s.Uncascaded = cause(fp)
				srcs = append(srcs, s)
				quarantined = append(quarantined, key)
			}
		}
	}

	secs := subgraph.Sections{Truncated: g.truncation()}
	for range rapid.IntRange(0, 2).Draw(t, "unattributed") {
		u := subgraph.Unattributed{
			Subject: g.fp(), Reason: subgraph.UnattributedReason(rapid.IntRange(1, 7).Draw(t, "unattributed reason")),
			Spans: g.spans(), Candidates: g.fps(3),
		}
		if rapid.Bool().Draw(t, "seed pod") {
			u.SeedPod = g.fp()
		}
		if u.Reason == subgraph.UnattributedSeveralClaims {
			u.Relation, u.Target = rapid.SampledFrom([]catalog.RelationType{catalog.ScheduledOn, catalog.RunsOn}).Draw(t, "relation"), catalog.K8sNode
		}
		secs.Unattributed = append(secs.Unattributed, u)
	}
	for range rapid.IntRange(0, 2).Draw(t, "unmapped") {
		secs.Unmapped = append(secs.Unmapped, subgraph.Unmapped{Pod: g.fp(), Spans: g.spans()})
	}
	counter := 0
	secs.Suppressed = g.suppressed(&counter)
	for i := range rapid.IntRange(0, 3).Draw(t, "pods") {
		p := subgraph.PodSection{Seed: pod(10 + i), Truncated: g.truncation(), Suppressed: g.suppressed(&counter)}
		for _, k := range quarantined {
			if rapid.Bool().Draw(t, "quarantined key") {
				p.Quarantined = append(p.Quarantined, k)
			}
		}
		secs.Pods = append(secs.Pods, p)
	}
	secs.Anchor = subgraph.AnchorState(rapid.IntRange(0, 3).Draw(t, "anchor"))
	secs.Ubiquity = subgraph.Ubiquity{Judged: rapid.Bool().Draw(t, "judged"), FrontierNodes: rapid.IntRange(0, 1000).Draw(t, "frontier nodes")}

	in := subgraph.Input{
		Token: rapid.Uint64().Draw(t, "token"), From: from, To: from.Add(time.Duration(g.width)),
		Sources: rapid.Permutation(srcs).Draw(t, "order of sources"), Sections: secs,
	}
	sg, err := subgraph.Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Extra legs: the codec carries any edge element as a leg.
	var edgeIDs []uint32
	for i, el := range sg.Elements {
		if el.Key.Kind == subgraph.KindEdge {
			edgeIDs = append(edgeIDs, uint32(i))
		}
	}
	if len(edgeIDs) > 0 {
		for i := range sg.Elements {
			el := &sg.Elements[i]
			if el.Key.Kind != subgraph.KindDerived || !rapid.Bool().Draw(t, "extra legs") {
				continue
			}
			el.Paths[0] = append(el.Paths[0], rapid.SampledFrom(edgeIDs).Draw(t, "extra leg"))
			el.Paths = append(el.Paths, []uint32{rapid.SampledFrom(edgeIDs).Draw(t, "second path")})
		}
	}

	pol := wire.Policy{
		Interval: rapid.IntRange(0, 1<<20).Draw(t, "interval"), CoResidency: rapid.IntRange(0, 9).Draw(t, "coresidency"),
		Extract: rapid.IntRange(0, 9).Draw(t, "extract"), BlastRadius: rapid.IntRange(0, 9).Draw(t, "blastradius"),
		GraphDiff: rapid.IntRange(0, 9).Draw(t, "graphdiff"), IdentityEncoding: rapid.IntRange(0, 9).Draw(t, "identity"),
		CatalogIDs: [32]byte(rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "catalog digest")),
	}
	if rapid.Bool().Draw(t, "rank") {
		pol.Rank = rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "rank digest")
	}
	q := wire.Query{
		Kind:  wire.QueryKind(rapid.IntRange(1, 4).Draw(t, "kind")),
		Seeds: g.fps(3), AsOf: rapid.Uint64().Draw(t, "asof"),
	}
	if rapid.Bool().Draw(t, "latest") {
		q.AsOf = store.Latest
	}
	for _, name := range []string{"depth", "max_pairs", "min_overlap_ns", "ünïcode"} {
		if rapid.Bool().Draw(t, "option") {
			q.Options = append(q.Options, wire.Option{Name: name, Value: rapid.Int64().Draw(t, "value")})
		}
	}
	slices.SortFunc(q.Options, func(a, b wire.Option) int { return bytes.Compare([]byte(a.Name), []byte(b.Name)) })
	return wire.Document{Query: q, Policy: pol, Subgraph: sg}
}

// Decode(Encode(x)) is x, and the encoding of the decoded document is the same
// bytes, for every document drawn.
func TestRoundTripProperty(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn documents held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		d := drawDoc(t)
		b, err := wire.Encode(d)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		back, err := wire.Decode(b)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !equal(back, d) {
			t.Fatalf("Decode(Encode(x)) is not x:\n%+v\n%+v", back, d)
		}
		again, err := wire.Encode(back)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("Encode(Decode(b)) is not b (%v)", err)
		}
		js, err := wire.JSON(d)
		if err != nil || !json.Valid(js) {
			t.Fatalf("JSON: %v", err)
		}
		if js2, _ := wire.JSON(back); !bytes.Equal(js, js2) {
			t.Fatalf("the twins of x and Decode(Encode(x)) differ")
		}

		sg := d.Subgraph
		seen["documents"]++
		if uint64(sg.To.Sub(sg.From)) > pow53 {
			seen["windows over 2^53"]++
		}
		seen["elements"] += len(sg.Elements)
		seen["deltas"] += len(sg.Deltas)
		seen["keyframes after From"] += len(sg.Keyframes) - 1
		seen["quarantined entities"] += len(sg.Quarantine.Entities)
		seen["static edges"] += len(sg.Quarantine.Edges)
		for _, e := range sg.Entities {
			if e.Boundary {
				seen["boundary entities"]++
			}
		}
		for _, el := range sg.Elements {
			seen["class "+el.Class.String()]++
			seen[el.Key.Kind.String()+" elements"]++
			if len(el.Paths) > 1 {
				seen["derived with two paths"]++
			}
			for _, p := range el.Pieces {
				if p.Before {
					seen["pieces before From"]++
				}
				seen["leg ends"] += len(p.Ends)
				seen["producers"] += len(p.Producers)
			}
		}
		seen["pod sections"] += len(sg.Sections.Pods)
		seen["unattributed"] += len(sg.Sections.Unattributed)
		seen["suppressed"] += len(sg.Sections.Suppressed)
	})
}
