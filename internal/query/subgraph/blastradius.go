package subgraph

import (
	"context"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/store"
)

// FromBlastRadius builds the temporal subgraph of a blast-radius result.
//
// Each member is a derived element: an entity the failing seed reaches, keyed by
// the seed and the member, which are dictionary entries, with the rule
// blast_radius.reach, and its pieces are its reach. A reach has no source of
// its own, and it does not begin before the window: it is the intersection of
// the legs along the paths to the seed, so a piece that ends inside the window
// ends with [EndReasonReachEnded] and lists no leg. It has one path for each
// arrival, whose leg is the edge from the member to the parent it came through.
// The legs, and the edges read and not used because an endpoint is quarantined,
// are elements, as the interval package read them, with their cascade. The
// result has no element for the existence of the seed and the members: one call
// of the interval package's Entities at its own token gives them (see
// [BudgetDate]).
//
// The placements it refused to guess, and its cuts, are carried as sections.
// Package blastradius's Around result is not carried: its substrates are
// walked from a pod, and are a different shape.
//
// Everything in it is derived. Any error, a retained layer included, fails the
// whole call, with no partial answer.
func FromBlastRadius(ctx context.Context, st store.Store, res blastradius.Result, p ReadPolicy) (Subgraph, error) {
	fail := func(err error) (Subgraph, error) { return Subgraph{}, fmt.Errorf("subgraph: FromBlastRadius: %w", err) }
	c, err := newCollector(res.From, res.To)
	if err != nil {
		return fail(err)
	}
	if res.Seed.IsZero() {
		return fail(invalid("the seed is the zero fingerprint"))
	}
	c.name(res.Seed)
	for _, el := range res.Quarantined {
		if _, err := c.element(el); err != nil {
			return fail(err)
		}
	}
	for i, m := range res.Members {
		class, err := ClassOf(m.Class)
		if err != nil {
			return fail(fmt.Errorf("member %d: %w", i, err))
		}
		src := Source{
			Key:   Key{Kind: KindDerived, A: res.Seed, B: m.Entity, Rule: RuleReach},
			Class: class, Transitions: m.Transitions,
		}
		for _, a := range m.Arrivals {
			leg, err := c.element(a.Leg)
			if err != nil {
				return fail(fmt.Errorf("member %d: %w", i, err))
			}
			src.Paths = append(src.Paths, []Key{leg})
		}
		for _, s := range m.Reach {
			src.Pieces = append(src.Pieces, c.piece(s, false, nil))
		}
		if err := c.add(src); err != nil {
			return fail(err)
		}
	}
	var sec Sections
	sec.Truncated = Truncation{
		Depth: slices.Clone(res.Truncated.Depth), MembersDropped: res.Truncated.Members,
	}
	for _, f := range res.Truncated.Frontier {
		sec.Truncated.Frontier = append(sec.Truncated.Frontier, FrontierCut{Depth: f.Depth, Dropped: slices.Clone(f.Dropped)})
	}
	for _, u := range res.Unattributed {
		spans, err := c.spans(u.Spans)
		if err != nil {
			return fail(err)
		}
		sec.Unattributed = append(sec.Unattributed, Unattributed{
			Subject: u.Subject, Reason: UnattributedSeveralClaims, Relation: u.Relation, Target: u.Target,
			Spans: spans, Candidates: slices.Clone(u.Candidates),
		})
	}
	sg, err := c.build(ctx, st, res.Token, sec, p)
	if err != nil {
		return fail(err)
	}
	return sg, nil
}
