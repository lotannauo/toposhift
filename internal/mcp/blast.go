package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

// maxDepth is the most hops the blast_radius tool follows, and its default.
const maxDepth = 6

// BlastRadiusInput is the input of the blast_radius tool. Give at, or from and
// to.
type BlastRadiusInput struct {
	// Entity is the id of the failing entity.
	Entity string `json:"entity"`
	// At is one instant, RFC 3339 with a zone: the window [at, at+1ns).
	At string `json:"at,omitempty"`
	// From and To are the window [from, to).
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	// Depth is the most hops to follow, 1 to 6; the default is 6.
	Depth int `json:"depth,omitempty"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
	// Limit is how many members to show, 1 to 500; the default is 50.
	Limit int `json:"limit,omitempty"`
}

// BlastRadiusOutput is the answer of the blast_radius tool.
type BlastRadiusOutput struct {
	Token        string           `json:"token"`
	Derived      bool             `json:"derived"`
	Policy       Policy           `json:"policy"`
	Seed         Ref              `json:"seed"`
	Window       Window           `json:"window"`
	DepthLimit   int              `json:"depth_limit"`
	Members      []Member         `json:"members"`
	Unattributed []UnattributedAt `json:"unattributed"`
	Quarantined  []Quarantined    `json:"quarantined"`
	Counts       BlastCounts      `json:"counts"`
	Truncation   BlastTruncation  `json:"truncation"`
	Notes        []string         `json:"notes"`
}

// Member is an entity the failing seed reaches. Its reach is derived: the
// instants at which a chain of edges, every leg live at once, leads to it.
type Member struct {
	Entity      Ref    `json:"entity"`
	Depth       int    `json:"depth"`
	Subject     string `json:"subject"`
	Basis       string `json:"basis"`
	Rule        string `json:"rule"`
	Reach       []Span `json:"reach"`
	Duration    string `json:"duration"`
	Class       string `json:"class"`
	Transitions int    `json:"transitions"`
	Via         []Via  `json:"via"`
}

// Via is one edge by which impact reaches a member from a parent. The leg is the
// edge as stored relations assert it, read with the cascade; its spans say how
// each stretch of it ended.
type Via struct {
	Parent      Ref      `json:"parent"`
	Relation    string   `json:"relation"`
	Propagation string   `json:"propagation"`
	Subject     string   `json:"subject"`
	Basis       string   `json:"basis"`
	Rule        string   `json:"rule"`
	AssertedBy  []string `json:"asserted_by"`
	Leg         []Span   `json:"leg"`
}

// BlastCounts are the exact sizes of an answer, whatever it shows.
type BlastCounts struct {
	// Members counts every member the walk found, listed or not.
	Members      int `json:"members"`
	Shown        int `json:"members_shown"`
	Unattributed int `json:"unattributed"`
	Quarantined  int `json:"quarantined"`
}

// BlastTruncation reports what the walk's own caps left out. While incomplete is
// true the answer is not the whole reach.
type BlastTruncation struct {
	Incomplete bool `json:"incomplete"`
	// DepthCut counts the members reached at the depth limit with edges that
	// were not followed.
	DepthCut int `json:"depth_cut"`
	// FrontierCuts lists, by depth, how many entities were reached and not
	// expanded.
	FrontierCuts []FrontierCut `json:"frontier_cuts"`
	// MembersUnlisted counts the members the walk found and does not list.
	MembersUnlisted int `json:"members_unlisted"`
}

// FrontierCut is the entities of one depth that the frontier cap left unexpanded.
type FrontierCut struct {
	Depth   int `json:"depth"`
	Dropped int `json:"dropped"`
}

// BlastRadius lists what a failing entity reaches over a window.
func (s *Source) BlastRadius(ctx context.Context, in BlastRadiusInput) (BlastRadiusOutput, error) {
	req := s.newRequest()
	req.nameText(in.Entity)
	return run(s, ctx, true, req, func(ctx context.Context) (BlastRadiusOutput, error) {
		var zero BlastRadiusOutput
		seed, err := s.parseEntity("entity", in.Entity)
		if err != nil {
			return zero, err
		}
		from, to, err := windowArgs(in.At, in.From, in.To)
		if err != nil {
			return zero, err
		}
		depth := in.Depth
		switch {
		case depth == 0:
			depth = maxDepth
		case depth < 1 || depth > maxDepth:
			return zero, invalid("The argument \"depth\" must be between 1 and %d; got %d.", maxDepth, depth)
		}
		limit, err := parseLimit(in.Limit)
		if err != nil {
			return zero, err
		}
		token, ahead, err := s.pin(in.AsOf)
		if err != nil {
			return zero, err
		}

		pol := blastradius.DefaultPolicy()
		pol.MaxDepth = depth
		res, err := blastradius.NewWalker(s.st).Impact(ctx, seed, interval.Window{From: from, To: to, AsOf: token}, blastradius.Options{Policy: pol})
		if err != nil {
			return zero, err
		}

		out := BlastRadiusOutput{
			Token: tokenString(token), Derived: true,
			Policy: Policy{
				Interval: interval.PolicyVersion, BlastRadius: blastradius.PolicyVersion,
				Rank: s.rankDigest, Names: NameTableVersion, Withhold: WithholdVersion,
			},
			Seed: ref(seed), Window: windowOf(from, to), DepthLimit: depth,
			Members: []Member{}, Unattributed: []UnattributedAt{}, Quarantined: []Quarantined{},
			Notes: notesFor(ahead),
		}
		for _, m := range head(res.Members, limit) {
			out.Members = append(out.Members, s.member(m, to))
		}
		un := head(res.Unattributed, limit)
		for _, u := range un {
			out.Unattributed = append(out.Unattributed, blastUnattributed(u, to))
		}
		for _, el := range head(res.Quarantined, maxQuarantined) {
			if q, ok := quarantinedOf(el); ok {
				out.Quarantined = append(out.Quarantined, q)
			}
		}

		total := len(res.Members) + res.Truncated.Members
		out.Counts = BlastCounts{
			Members: total, Shown: len(out.Members),
			Unattributed: len(res.Unattributed), Quarantined: len(res.Quarantined),
		}
		out.Truncation = blastTruncation(res.Truncated)
		out.Notes = append(out.Notes, blastNotes(res.Truncated, depth, pol)...)
		if len(out.Members) < total {
			out.Notes = append(out.Notes, shown(len(out.Members), total, "members", "nearest first"))
		}
		if len(un) < len(res.Unattributed) {
			out.Notes = append(out.Notes, shown(len(un), len(res.Unattributed), "unattributed placements", "in the package's order"))
		}
		if len(out.Quarantined) < len(res.Quarantined) {
			out.Notes = append(out.Notes, shown(len(out.Quarantined), len(res.Quarantined), "quarantined edges", "in the package's order"))
		}

		err = s.finish(ctx, &out, &out.Notes, token, from, to,
			namedTail{"members", listTail(&out.Members)},
			namedTail{"unattributed placements", listTail(&out.Unattributed)},
			namedTail{"quarantined edges", listTail(&out.Quarantined)})
		if err != nil {
			return zero, err
		}
		out.Counts.Shown = len(out.Members)
		return out, nil
	})
}

func (s *Source) member(m blastradius.Member, to time.Time) Member {
	out := Member{
		Entity: ref(m.Entity), Depth: m.Depth,
		Subject: SubjectDerived, Basis: BasisDerived, Rule: RuleReach,
		Reach: derivedSpans(m.Reach, to, "reach"), Duration: m.Duration.String(),
		Class: m.Class.String(), Transitions: m.Transitions, Via: []Via{},
	}
	for _, a := range m.Arrivals {
		out.Via = append(out.Via, Via{
			Parent: ref(a.Parent), Relation: string(a.Relation), Propagation: a.Propagation.String(),
			Subject: subjectOfRelation(a.Relation), Basis: BasisDerived, Rule: RuleExistence,
			AssertedBy: piecesProducers(a.Leg.Pieces), Leg: pieceSpans(a.Leg.Pieces, to),
		})
	}
	return out
}

func blastUnattributed(u blastradius.Unattributed, to time.Time) UnattributedAt {
	out := UnattributedAt{
		Entity: ref(u.Subject), Reason: "several_claims",
		Subject: SubjectDerived, Basis: BasisDerived, Rule: RuleUnattributed + "several_claims",
		Relation: string(u.Relation), TargetType: string(u.Target),
		Spans:      ambiguitySpans(u.Spans, to),
		Candidates: refs(u.Candidates),
		Note: fmt.Sprintf("The entity claimed %d targets of type %s through %s at once while one of them was reached, so it is placed on none of them.",
			len(u.Candidates), u.Target, u.Relation),
	}
	return out
}

func blastTruncation(t blastradius.Truncation) BlastTruncation {
	out := BlastTruncation{
		DepthCut: len(t.Depth), MembersUnlisted: t.Members, FrontierCuts: []FrontierCut{},
	}
	for _, c := range t.Frontier {
		out.FrontierCuts = append(out.FrontierCuts, FrontierCut{Depth: c.Depth, Dropped: len(c.Dropped)})
	}
	out.Incomplete = out.DepthCut > 0 || len(out.FrontierCuts) > 0 || out.MembersUnlisted > 0
	return out
}

// blastNotes writes a sentence for each cap of the walk that cut the answer.
func blastNotes(t blastradius.Truncation, depth int, pol blastradius.Policy) []string {
	var out []string
	if n := len(t.Depth); n > 0 {
		out = append(out, fmt.Sprintf("The walk stopped at depth %d, the depth limit; %d members at that depth have edges that were not followed.", depth, n))
	}
	for _, c := range t.Frontier {
		out = append(out, fmt.Sprintf("At depth %d, %d entities were reached but not expanded: the walk expands at most %d entities from each depth.", c.Depth, len(c.Dropped), pol.MaxFrontier))
	}
	if t.Members > 0 {
		out = append(out, fmt.Sprintf("The walk found %d more members than the %d it lists; they are counted and not listed.", t.Members, pol.MaxMembers))
	}
	return out
}
