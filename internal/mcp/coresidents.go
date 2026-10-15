package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

// CoResidentsInput is the input of the co_residents tool.
type CoResidentsInput struct {
	// Pod is the id of the pod to find the others sharing a place with.
	Pod string `json:"pod"`
	// From and To are the window [from, to).
	From string `json:"from"`
	To   string `json:"to"`
	// NodeOnly derives the pods on a shared node only, with no lift to the host.
	NodeOnly bool `json:"node_only,omitempty"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
	// Limit is how many co-residents to show, 1 to 500; the default is 50.
	Limit int `json:"limit,omitempty"`
}

// CoResidentsOutput is the answer of the co_residents tool. Co-residency is
// inferred, never asserted: two pods are co-resident only over the instants at
// which every leg of the path that joins them exists.
type CoResidentsOutput struct {
	Token        string           `json:"token"`
	Derived      bool             `json:"derived"`
	Policy       Policy           `json:"policy"`
	Seed         Ref              `json:"seed"`
	Window       Window           `json:"window"`
	NodeOnly     bool             `json:"node_only"`
	Placement    []Leg            `json:"placement"`
	CoResidents  []CoResident     `json:"co_residents"`
	Suppressed   []CoResident     `json:"suppressed"`
	Unattributed []UnattributedAt `json:"unattributed"`
	Quarantined  []Quarantined    `json:"quarantined"`
	Counts       CoResidentCounts `json:"counts"`
	Truncation   CoResidentCut    `json:"truncation"`
	Notes        []string         `json:"notes"`
}

// Leg is an edge as it was read, with the cascade, and when it existed.
type Leg struct {
	Edge       EdgeRef  `json:"edge"`
	Subject    string   `json:"subject"`
	Basis      string   `json:"basis"`
	Rule       string   `json:"rule"`
	AssertedBy []string `json:"asserted_by"`
	Spans      []Span   `json:"spans"`
}

// CoResident is one path by which a pod shared a place with the seed.
type CoResident struct {
	// Mechanism is "shared_node", or "shared_host" for two pods on two nodes that
	// are each placed on one host by an asserted runs_on.
	Mechanism string `json:"mechanism"`
	Peer      Ref    `json:"peer"`
	// Nodes is the node, or the seed's node then the peer's.
	Nodes []Ref `json:"nodes"`
	// Via is the node or the host shared.
	Via         Ref       `json:"via"`
	Subject     string    `json:"subject"`
	Basis       string    `json:"basis"`
	Rule        string    `json:"rule"`
	Overlaps    []Overlap `json:"overlaps"`
	Duration    string    `json:"duration"`
	Class       string    `json:"class"`
	Transitions int       `json:"transitions"`
	Legs        []Leg     `json:"legs"`
	// SuppressedReason is set only in the suppressed list.
	SuppressedReason string `json:"suppressed_reason,omitempty"`
}

// Overlap is a span over which every leg existed at once. It ends where a leg
// does; ends says which legs, and why.
type Overlap struct {
	Start             string       `json:"start"`
	End               string       `json:"end"`
	EndReason         *string      `json:"end_reason"`
	EndSentence       string       `json:"end_sentence"`
	BeganBeforeWindow bool         `json:"began_before_window,omitempty"`
	AssertedBy        []string     `json:"asserted_by"`
	Ends              []OverlapEnd `json:"ends"`
}

// OverlapEnd is a leg that ends where an overlap does.
type OverlapEnd struct {
	// Leg is the index in the co-resident's legs.
	Leg         int     `json:"leg"`
	EndReason   *string `json:"end_reason"`
	EndSentence string  `json:"end_sentence"`
}

// CoResidentCounts are the exact sizes of an answer.
type CoResidentCounts struct {
	CoResidents  int `json:"co_residents"`
	Shown        int `json:"co_residents_shown"`
	Suppressed   int `json:"suppressed"`
	Unattributed int `json:"unattributed"`
	Quarantined  int `json:"quarantined"`
}

// CoResidentCut reports what the query's own caps left out. While incomplete is
// true the answer is not every co-resident.
type CoResidentCut struct {
	Incomplete bool `json:"incomplete"`
	// NodesNotRead and HostsNotRead count the nodes and hosts over the frontier cap.
	NodesNotRead int `json:"nodes_not_read"`
	HostsNotRead int `json:"hosts_not_read"`
	// PerViaDropped counts the entries dropped by the cap on each node or host.
	PerViaDropped int `json:"per_via_dropped"`
	// ResultsDropped and SuppressedDropped count the entries over the result cap.
	ResultsDropped    int `json:"results_dropped"`
	SuppressedDropped int `json:"suppressed_dropped"`
}

// CoResidents lists the pods that shared a place with a pod over a window.
func (s *Source) CoResidents(ctx context.Context, in CoResidentsInput) (CoResidentsOutput, error) {
	return run(s, ctx, true, func(ctx context.Context) (CoResidentsOutput, error) {
		var zero CoResidentsOutput
		seed, err := s.parseEntity("pod", in.Pod)
		if err != nil {
			return zero, err
		}
		from, to, err := rangeArgs(in.From, in.To)
		if err != nil {
			return zero, err
		}
		limit, err := parseLimit(in.Limit)
		if err != nil {
			return zero, err
		}
		token, ahead, err := s.pin(in.AsOf)
		if err != nil {
			return zero, err
		}

		res, err := coresidency.NewFinder(s.st).Find(ctx, seed, interval.Window{From: from, To: to, AsOf: token}, coresidency.Options{NodeOnly: in.NodeOnly})
		if err != nil {
			return zero, err
		}

		out := CoResidentsOutput{
			Token: tokenString(token), Derived: true,
			Policy: Policy{
				Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion,
				Rank: s.rankDigest, Names: NameTableVersion,
			},
			Seed: ref(seed), Window: windowOf(from, to), NodeOnly: in.NodeOnly,
			Placement: []Leg{}, CoResidents: []CoResident{}, Suppressed: []CoResident{},
			Unattributed: []UnattributedAt{}, Quarantined: []Quarantined{},
			Notes: notesFor(ahead),
		}
		for _, el := range res.Placement {
			out.Placement = append(out.Placement, leg(el, to))
		}
		for _, c := range head(res.CoResidents, limit) {
			out.CoResidents = append(out.CoResidents, coResident(c, to))
		}
		for _, c := range head(res.Suppressed, limit) {
			out.Suppressed = append(out.Suppressed, coResident(c, to))
		}
		un := head(res.Unattributed, limit)
		for _, u := range un {
			out.Unattributed = append(out.Unattributed, coUnattributed(u, to))
		}
		for _, el := range head(res.Quarantined, maxQuarantined) {
			if q, ok := quarantinedOf(el); ok {
				out.Quarantined = append(out.Quarantined, q)
			}
		}

		perVia := 0
		for _, v := range res.Truncated.PerVia {
			perVia += v.Dropped
		}
		out.Counts = CoResidentCounts{
			CoResidents: len(res.CoResidents), Shown: len(out.CoResidents), Suppressed: len(res.Suppressed),
			Unattributed: len(res.Unattributed), Quarantined: len(res.Quarantined),
		}
		out.Truncation = CoResidentCut{
			NodesNotRead: len(res.Truncated.Nodes), HostsNotRead: len(res.Truncated.Hosts), PerViaDropped: perVia,
			ResultsDropped: res.Truncated.Results, SuppressedDropped: res.Truncated.SuppressedDropped,
		}
		cut := out.Truncation
		out.Truncation.Incomplete = cut.NodesNotRead+cut.HostsNotRead+cut.PerViaDropped+cut.ResultsDropped+cut.SuppressedDropped > 0
		out.Notes = append(out.Notes, coResidentNotes(out.Truncation)...)
		if len(out.CoResidents) < len(res.CoResidents) {
			out.Notes = append(out.Notes, shown(len(out.CoResidents), len(res.CoResidents), "co-residents", "longest overlap first"))
		}
		if len(out.Suppressed) < len(res.Suppressed) {
			out.Notes = append(out.Notes, shown(len(out.Suppressed), len(res.Suppressed), "suppressed co-residents", "longest overlap first"))
		}
		if len(un) < len(res.Unattributed) {
			out.Notes = append(out.Notes, shown(len(un), len(res.Unattributed), "unattributed placements", "in the package's order"))
		}
		if len(out.Quarantined) < len(res.Quarantined) {
			out.Notes = append(out.Notes, shown(len(out.Quarantined), len(res.Quarantined), "quarantined edges", "in the package's order"))
		}
		if len(res.Suppressed) > 0 {
			out.Notes = append(out.Notes, fmt.Sprintf("Suppressed co-residents overlapped for less than %s inside the window, which event times cannot tell from an artefact of ordering.",
				coresidency.DefaultPolicy().MinOverlap))
		}

		err = s.finish(ctx, &out, &out.Notes, token, from, to,
			namedTail{"suppressed co-residents", listTail(&out.Suppressed)},
			namedTail{"co-residents", listTail(&out.CoResidents)},
			namedTail{"unattributed placements", listTail(&out.Unattributed)},
			namedTail{"quarantined edges", listTail(&out.Quarantined)})
		if err != nil {
			return zero, err
		}
		out.Counts.Shown = len(out.CoResidents)
		return out, nil
	})
}

func leg(el interval.Element, to time.Time) Leg {
	return Leg{
		Edge: edgeRef(el.Subject), Subject: subjectOfElement(el.Subject), Basis: BasisDerived, Rule: RuleExistence,
		AssertedBy: piecesProducers(el.Pieces), Spans: pieceSpans(el.Pieces, to),
	}
}

func coResident(c coresidency.CoResident, to time.Time) CoResident {
	out := CoResident{
		Mechanism: c.Mechanism.String(), Peer: ref(c.Peer), Nodes: refs(c.Nodes), Via: ref(c.Via),
		Subject: SubjectDerived, Basis: BasisDerived, Duration: c.Duration.String(),
		Class: c.Class.String(), Transitions: c.Transitions, Overlaps: []Overlap{}, Legs: []Leg{},
	}
	out.Rule = RuleSharedNode
	if c.Mechanism == coresidency.SharedHost {
		out.Rule = RuleSharedHost
	}
	if c.Reason != coresidency.ReasonNone {
		out.SuppressedReason = c.Reason.String()
	}
	for _, l := range c.Legs {
		out.Legs = append(out.Legs, leg(l, to))
	}
	for _, o := range c.Overlaps {
		sp := derivedSpan(o.Span, to, "overlap")
		ov := Overlap{
			Start: sp.Start, End: sp.End, EndReason: sp.EndReason, EndSentence: sp.EndSentence,
			BeganBeforeWindow: o.Before, Ends: []OverlapEnd{},
		}
		var by []lifecycle.Producer
		for i, p := range o.Pieces {
			by = append(by, c.Legs[i].Pieces[p].Producers...)
		}
		ov.AssertedBy = producerNames(by)
		for _, e := range o.Ends {
			oe := OverlapEnd{Leg: e.Leg}
			if e.Unattributed {
				oe.EndReason, oe.EndSentence = strPtr(endReasonNotKnown), "the leg goes on, but its placement became ambiguous here"
			} else {
				oe.EndReason, oe.EndSentence = sourceReason(e.Source)
			}
			ov.Ends = append(ov.Ends, oe)
		}
		out.Overlaps = append(out.Overlaps, ov)
	}
	return out
}

func coUnattributed(u coresidency.Unattributed, to time.Time) UnattributedAt {
	reason := u.Reason.String()
	return UnattributedAt{
		Entity: ref(u.Subject), Reason: reason,
		Subject: SubjectDerived, Basis: BasisDerived, Rule: RuleUnattributed + reason,
		Spans: ambiguitySpans(u.Spans, to), Candidates: refs(u.Candidates),
		Note: unattributedNote(u.Reason),
	}
}

func unattributedNote(r coresidency.UnattributedReason) string {
	switch r {
	case coresidency.NoHost:
		return "The node has no live runs_on, so its host is not known; only pods on the same node are co-resident."
	case coresidency.SeveralHosts:
		return "The node claimed two or more hosts at once, so it is on none of them."
	case coresidency.QuarantinedHost:
		return "The node's only live runs_on is to a quarantined host, so its host is not used."
	case coresidency.SeveralNodes:
		return "The pod was scheduled on two or more nodes at once, so its node is not known; those instants are left out of every path."
	}
	return "The placement is not known."
}

// coResidentNotes writes a sentence for each cap of the query that cut the answer.
func coResidentNotes(c CoResidentCut) []string {
	pol := coresidency.DefaultPolicy()
	var out []string
	if c.NodesNotRead > 0 {
		out = append(out, fmt.Sprintf("%d nodes of the pod were not read: the query reads at most %d.", c.NodesNotRead, pol.MaxNodes))
	}
	if c.HostsNotRead > 0 {
		out = append(out, fmt.Sprintf("%d hosts were not read: the query reads at most %d.", c.HostsNotRead, pol.MaxHosts))
	}
	if c.PerViaDropped > 0 {
		out = append(out, fmt.Sprintf("%d entries were dropped: the query keeps at most %d for each node or host.", c.PerViaDropped, pol.MaxPerVia))
	}
	if c.ResultsDropped > 0 {
		out = append(out, fmt.Sprintf("%d co-residents were dropped: the query keeps at most %d.", c.ResultsDropped, pol.MaxResults))
	}
	if c.SuppressedDropped > 0 {
		out = append(out, fmt.Sprintf("%d suppressed co-residents were dropped: the query keeps at most %d.", c.SuppressedDropped, pol.MaxResults))
	}
	return out
}
