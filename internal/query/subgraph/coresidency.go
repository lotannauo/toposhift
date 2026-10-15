package subgraph

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/store"
)

// FromCoResidency builds the temporal subgraph of a co-residency result.
//
// Each co-resident is a derived element: a pod that shared a node or a host with
// the seed, keyed by the seed and the peer, which are dictionary entries, and by
// its via and nodes in the key's detail. Its rule is co_residency.shared_node or
// co_residency.shared_host, and its pieces are its overlaps. An overlap ends
// where an intersection of its legs ends, which is [EndReasonReachEnded] with
// no source of its own; the legs that end there are listed beside it, each with
// its own end, or as unattributed when the leg goes on but its placement became
// ambiguous. The legs, the seed's placement and the edges read and not used
// because an endpoint is quarantined are elements too, as the interval package
// read them, with their cascade.
//
// The result has no element for the existence of the seed, the peers, the nodes
// and the hosts it names. One call of the interval package's Entities, at the
// result's own token and so in the world it was read in, gives them (see
// [BudgetDate]); a write made since is not seen. The result's cuts, its
// unattributed placements and its suppressed entries are carried as sections,
// whole. The seed's attributed host spans (SeedHosts) are not carried: a host the
// seed was on that has no shared-host co-resident is therefore not in the
// subgraph. They come with a later change.
//
// Everything in it is derived. Any error, a refusal for retention included,
// fails the whole call, with no partial answer.
func FromCoResidency(ctx context.Context, st store.Store, res coresidency.Result, p ReadPolicy) (Subgraph, error) {
	fail := func(err error) (Subgraph, error) { return Subgraph{}, fmt.Errorf("subgraph: FromCoResidency: %w", err) }
	c, err := newCollector(res.From, res.To)
	if err != nil {
		return fail(err)
	}
	if res.Seed.IsZero() {
		return fail(invalid("the seed is the zero fingerprint"))
	}
	c.name(res.Seed)
	for _, el := range res.Placement {
		if _, err := c.element(el); err != nil {
			return fail(err)
		}
	}
	for _, el := range res.Quarantined {
		if _, err := c.element(el); err != nil {
			return fail(err)
		}
	}
	for i, co := range res.CoResidents {
		if co.Reason != coresidency.ReasonNone {
			return fail(invalid("co-resident %d is suppressed for %s", i, co.Reason))
		}
		src, err := c.coResident(res.Seed, co)
		if err != nil {
			return fail(fmt.Errorf("co-resident %d: %w", i, err))
		}
		if err := c.add(src); err != nil {
			return fail(err)
		}
	}
	sec, err := c.coSections(res)
	if err != nil {
		return fail(err)
	}
	sg, err := c.build(ctx, st, res.Token, sec, p)
	if err != nil {
		return fail(err)
	}
	return sg, nil
}

// coKey is the key of a co-resident: the seed and the peer, the rule of its
// mechanism, and the via and the nodes it shared.
func coKey(seed identity.Fingerprint, co coresidency.CoResident) (Key, Mechanism, error) {
	m, err := MechanismOfCoResidency(co.Mechanism)
	if err != nil {
		return Key{}, 0, err
	}
	rule, err := m.coRule()
	if err != nil {
		return Key{}, 0, err
	}
	nodes := make([]string, len(co.Nodes))
	for i, n := range co.Nodes {
		nodes[i] = n.String()
	}
	return Key{
		Kind: KindDerived, A: seed, B: co.Peer, Rule: rule.Rule(),
		Detail: "via=" + co.Via.String() + ";nodes=" + strings.Join(nodes, ","),
	}, m, nil
}

func (c *collector) coResident(seed identity.Fingerprint, co coresidency.CoResident) (Source, error) {
	key, _, err := coKey(seed, co)
	if err != nil {
		return Source{}, err
	}
	class, err := ClassOf(co.Class)
	if err != nil {
		return Source{}, err
	}
	src := Source{Key: key, Class: class, Transitions: co.Transitions}
	legs := make([]Key, len(co.Legs))
	for i, el := range co.Legs {
		if legs[i], err = c.element(el); err != nil {
			return Source{}, err
		}
	}
	src.Paths = [][]Key{legs}
	for _, o := range co.Overlaps {
		ends := make([]LegEnd, len(o.Ends))
		for i, e := range o.Ends {
			reason, err := legReason(e.Source, e.Unattributed)
			if err != nil {
				return Source{}, err
			}
			ends[i] = LegEnd{Path: 0, Leg: e.Leg, Reason: reason}
		}
		src.Pieces = append(src.Pieces, c.piece(o.Span, o.Before, ends))
	}
	return src, nil
}

func (c *collector) coSections(res coresidency.Result) (Sections, error) {
	var sec Sections
	var err error
	if sec.Truncated, err = coTruncation(res.Truncated); err != nil {
		return Sections{}, err
	}
	for _, u := range res.Unattributed {
		reason, err := UnattributedReasonOfCoResidency(u.Reason)
		if err != nil {
			return Sections{}, err
		}
		spans, err := c.spans(u.Spans)
		if err != nil {
			return Sections{}, err
		}
		sec.Unattributed = append(sec.Unattributed, Unattributed{
			Subject: u.Subject, Reason: reason, Spans: spans, Candidates: slices.Clone(u.Candidates),
		})
	}
	if sec.Suppressed, err = c.coSuppressed(res.Seed, res.Suppressed); err != nil {
		return Sections{}, err
	}
	return sec, nil
}

func (c *collector) coSuppressed(seed identity.Fingerprint, cos []coresidency.CoResident) ([]Suppressed, error) {
	var out []Suppressed
	for i, co := range cos {
		key, _, err := coKey(seed, co)
		if err != nil {
			return nil, fmt.Errorf("suppressed %d: %w", i, err)
		}
		reason, err := SuppressReasonOf(co.Reason)
		if err != nil {
			return nil, fmt.Errorf("suppressed %d: %w", i, err)
		}
		class, err := ClassOf(co.Class)
		if err != nil {
			return nil, fmt.Errorf("suppressed %d: %w", i, err)
		}
		x := Suppressed{Key: key, Reason: reason, Class: class, Transitions: co.Transitions}
		for _, o := range co.Overlaps {
			s, err := c.span(o.Span, o.Before)
			if err != nil {
				return nil, fmt.Errorf("suppressed %d: %w", i, err)
			}
			x.Spans = append(x.Spans, s)
		}
		out = append(out, x)
	}
	return out, nil
}

func coTruncation(t coresidency.Truncation) (Truncation, error) {
	out := Truncation{
		Nodes: slices.Clone(t.Nodes), Hosts: slices.Clone(t.Hosts),
		Results: t.Results, SuppressedDropped: t.SuppressedDropped,
	}
	for _, v := range t.PerVia {
		m, err := MechanismOfCoResidency(v.Mechanism)
		if err != nil {
			return Truncation{}, err
		}
		out.PerVia = append(out.PerVia, ViaCount{Via: v.Via, Mechanism: m, Dropped: v.Dropped})
	}
	return out, nil
}
