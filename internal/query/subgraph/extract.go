package subgraph

import (
	"context"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/store"
)

// mappingLegs is the number of mapping legs of a contribution: the member
// instance's two and the peer instance's two. The legs of its pod-level entry
// follow them in a path, in the order of that entry's legs.
const mappingLegs = 4

// FromExtraction builds the temporal subgraph of an extraction result.
//
// Each pair is a derived element: a member and a peer service that shared a
// place by one mechanism, keyed by the two services, which are dictionary
// entries, and its rule is pair.shared_pod, pair.shared_node or
// pair.shared_host. It has one path for each contribution, whose legs are the
// four mapping legs and then the legs of the pod-level entry the contribution
// went through; its pieces are the pair's spans. A span ends where an
// intersection of legs ends, [EndReasonReachEnded] with no source of its own,
// and the legs that end there are listed beside it by contribution and leg, each
// with its own end, or as unattributed when the leg goes on but an instance
// became part of several pods or an instance of several services. A leg of the
// pod-level entry is a leg of the path, so the nested end an extraction reports
// is a leg end like any other. The pod-level entries are not elements.
//
// The members' dependency and instance edges, the legs, and the edges read and
// not used because an endpoint is quarantined are elements, as the interval
// package read them, with their cascade. The result has no element for the
// existence of the entities it names: one call of the interval package's
// Entities at its own token gives them (see [BudgetDate]).
//
// Carried whole as sections: the result's cuts, the mappings and placements it
// refused to guess, the unmapped pods, the suppressed pairs with their reason and
// coverage, the anchor state, the ubiquity judgment, and for each search of
// the pod-level results its own cuts, quarantined edges and suppressed entries.
// A contribution's own spans, a pair's per-via spans and each search's
// placement are not carried.
//
// Everything in it is derived. Any error, a retained layer included, fails the
// whole call, with no partial answer.
func FromExtraction(ctx context.Context, st store.Store, res extract.Result, p ReadPolicy) (Subgraph, error) {
	fail := func(err error) (Subgraph, error) { return Subgraph{}, fmt.Errorf("subgraph: FromExtraction: %w", err) }
	c, err := newCollector(res.From, res.To)
	if err != nil {
		return fail(err)
	}
	for i, m := range res.Members {
		c.name(m.Service)
		for _, el := range slices.Concat(m.Dependency, m.Instances) {
			if _, err := c.element(el); err != nil {
				return fail(fmt.Errorf("member %d: %w", i, err))
			}
		}
	}
	for _, el := range res.Quarantined {
		if _, err := c.element(el); err != nil {
			return fail(err)
		}
	}
	for i, pr := range res.PodResults {
		for _, el := range pr.Quarantined {
			if _, err := c.element(el); err != nil {
				return fail(fmt.Errorf("pod result %d: %w", i, err))
			}
		}
	}
	for i, pair := range res.Pairs {
		if pair.Reason != coresidency.ReasonNone {
			return fail(invalid("pair %d is suppressed for %s", i, pair.Reason))
		}
		src, err := c.pair(res, pair)
		if err != nil {
			return fail(fmt.Errorf("pair %d: %w", i, err))
		}
		if err := c.add(src); err != nil {
			return fail(err)
		}
	}
	sec, err := c.exSections(res)
	if err != nil {
		return fail(err)
	}
	sg, err := c.build(ctx, st, res.Token, sec, p)
	if err != nil {
		return fail(err)
	}
	return sg, nil
}

// pairKey is the key of a pair: its two services and the rule of its mechanism.
func pairKey(p extract.Pair) (Key, error) {
	m, err := MechanismOfExtract(p.Mechanism)
	if err != nil {
		return Key{}, err
	}
	rule, err := m.pairRule()
	if err != nil {
		return Key{}, err
	}
	return Key{Kind: KindDerived, A: p.Member, B: p.Peer, Rule: rule.Rule()}, nil
}

// podEntry is the pod-level entry a contribution went through: the entry of the
// search whose seed is the contribution's member pod.
func podEntry(res extract.Result, ct extract.Contribution) (coresidency.CoResident, error) {
	i := slices.IndexFunc(res.PodResults, func(r coresidency.Result) bool { return r.Seed == ct.MemberPod })
	if i < 0 {
		return coresidency.CoResident{}, invalid("no search of the pod %s ran", ct.MemberPod)
	}
	entries := res.PodResults[i].CoResidents
	if ct.Entry < 0 || ct.Entry >= len(entries) {
		return coresidency.CoResident{}, invalid("the entry %d of the search of %s, which has %d", ct.Entry, ct.MemberPod, len(entries))
	}
	return entries[ct.Entry], nil
}

func (c *collector) pair(res extract.Result, pair extract.Pair) (Source, error) {
	key, err := pairKey(pair)
	if err != nil {
		return Source{}, err
	}
	class, err := ClassOf(pair.Class)
	if err != nil {
		return Source{}, err
	}
	src := Source{Key: key, Class: class, Transitions: pair.Transitions}
	// podLegs[i] is the number of legs of the pod-level entry of contribution i.
	for i, ct := range pair.Contributions {
		if len(ct.Legs) != mappingLegs {
			return Source{}, invalid("contribution %d has %d mapping legs, not %d", i, len(ct.Legs), mappingLegs)
		}
		els := slices.Clone(ct.Legs)
		switch {
		case pair.Mechanism == extract.SharedPod && ct.Entry >= 0:
			return Source{}, invalid("contribution %d of a shared pod has the pod-level entry %d", i, ct.Entry)
		case pair.Mechanism != extract.SharedPod:
			entry, err := podEntry(res, ct)
			if err != nil {
				return Source{}, fmt.Errorf("contribution %d: %w", i, err)
			}
			els = append(els, entry.Legs...)
		}
		path := make([]Key, len(els))
		for j, el := range els {
			if path[j], err = c.element(el); err != nil {
				return Source{}, fmt.Errorf("contribution %d: %w", i, err)
			}
		}
		src.Paths = append(src.Paths, path)
	}
	for _, s := range pair.Spans {
		ends := make([]LegEnd, len(s.Ends))
		for i, e := range s.Ends {
			if ends[i], err = legEndOf(e); err != nil {
				return Source{}, err
			}
		}
		src.Pieces = append(src.Pieces, c.piece(s.Span, s.Before, ends))
	}
	return src, nil
}

// legEndOf is an end of a pair's span as a leg end of the contribution's path.
// Legs 0 to 3 are the mapping legs. A nested end of the pod-level entry is a leg
// of the path after them: the extraction numbers it 4 plus the entry's own leg.
func legEndOf(e extract.End) (LegEnd, error) {
	if e.Pod == nil {
		if e.Leg < 0 || e.Leg >= mappingLegs {
			return LegEnd{}, invalid("an end of the leg %d with no pod-level end", e.Leg)
		}
		reason, err := legReason(e.Source, e.Unattributed)
		if err != nil {
			return LegEnd{}, err
		}
		return LegEnd{Path: e.Contribution, Leg: e.Leg, Reason: reason}, nil
	}
	if e.Unattributed || e.Source != 0 || e.Leg != mappingLegs+e.Pod.Leg {
		return LegEnd{}, invalid("a nested end of the leg %d, with the pod-level leg %d", e.Leg, e.Pod.Leg)
	}
	reason, err := legReason(e.Pod.Source, e.Pod.Unattributed)
	if err != nil {
		return LegEnd{}, err
	}
	return LegEnd{Path: e.Contribution, Leg: e.Leg, Reason: reason}, nil
}

func (c *collector) exSections(res extract.Result) (Sections, error) {
	var sec Sections
	var err error
	sec.Truncated = Truncation{
		Members: slices.Clone(res.Truncated.Members), Instances: slices.Clone(res.Truncated.Instances),
		SeedPods: slices.Clone(res.Truncated.SeedPods),
		Pairs:    res.Truncated.Pairs, SuppressedDropped: res.Truncated.SuppressedDropped,
	}
	for _, u := range res.Unattributed {
		x := Unattributed{SeedPod: u.SeedPod, Subject: u.Subject, Candidates: slices.Clone(u.Candidates)}
		switch {
		case u.Mapping != 0 && u.Placement == 0:
			x.Reason, err = UnattributedReasonOfExtract(u.Mapping)
		case u.Mapping == 0 && u.Placement != 0:
			x.Reason, err = UnattributedReasonOfCoResidency(u.Placement)
		default:
			err = invalid("an unattributed entry of the subject %s with a mapping reason %s and a placement reason %s", u.Subject, u.Mapping, u.Placement)
		}
		if err != nil {
			return Sections{}, err
		}
		if x.Spans, err = c.spans(u.Spans); err != nil {
			return Sections{}, err
		}
		sec.Unattributed = append(sec.Unattributed, x)
	}
	for _, u := range res.Unmapped {
		spans, err := c.spans(u.Spans)
		if err != nil {
			return Sections{}, err
		}
		sec.Unmapped = append(sec.Unmapped, Unmapped{Pod: u.Pod, Spans: spans})
	}
	for i, pair := range res.Suppressed {
		x, err := c.suppressedPair(pair)
		if err != nil {
			return Sections{}, fmt.Errorf("suppressed %d: %w", i, err)
		}
		sec.Suppressed = append(sec.Suppressed, x)
	}
	if sec.Anchor, err = AnchorStateOf(res.Anchor); err != nil {
		return Sections{}, err
	}
	sec.Ubiquity = Ubiquity{Judged: res.Ubiquity.Judged, FrontierNodes: res.Ubiquity.FrontierNodes}
	for i, pr := range res.PodResults {
		ps := PodSection{Seed: pr.Seed}
		if ps.Truncated, err = coTruncation(pr.Truncated); err != nil {
			return Sections{}, fmt.Errorf("pod result %d: %w", i, err)
		}
		for _, el := range pr.Quarantined {
			k, err := c.element(el)
			if err != nil {
				return Sections{}, fmt.Errorf("pod result %d: %w", i, err)
			}
			ps.Quarantined = append(ps.Quarantined, k)
		}
		if ps.Suppressed, err = c.coSuppressed(pr.Seed, pr.Suppressed); err != nil {
			return Sections{}, fmt.Errorf("pod result %d: %w", i, err)
		}
		sec.Pods = append(sec.Pods, ps)
	}
	return sec, nil
}

func (c *collector) suppressedPair(pair extract.Pair) (Suppressed, error) {
	key, err := pairKey(pair)
	if err != nil {
		return Suppressed{}, err
	}
	reason, err := SuppressReasonOf(pair.Reason)
	if err != nil {
		return Suppressed{}, err
	}
	class, err := ClassOf(pair.Class)
	if err != nil {
		return Suppressed{}, err
	}
	x := Suppressed{Key: key, Reason: reason, Coverage: pair.Coverage, Class: class, Transitions: pair.Transitions}
	for _, s := range pair.Spans {
		sp, err := c.span(s.Span, s.Before)
		if err != nil {
			return Suppressed{}, err
		}
		x.Spans = append(x.Spans, sp)
	}
	return x, nil
}
