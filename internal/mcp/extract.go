package mcp

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// ServiceContextInput is the input of the service_context tool.
type ServiceContextInput struct {
	Service  string `json:"service" jsonschema:"The id of the service, such as service:0123456789abcdef0123456789abcdef. find_entities finds one by name; resolve_entity computes one from service.name and service.namespace."`
	From     string `json:"from" jsonschema:"The start of the window [from, to), RFC 3339 with a zone."`
	To       string `json:"to" jsonschema:"The end of the window, excluded."`
	NodeOnly bool   `json:"node_only,omitempty" jsonschema:"Pair services on a shared pod or node only: no host is read, so no pair is shared_host."`
	AsOf     string `json:"asof,omitempty" jsonschema:"A token from an earlier answer, to read the same world again."`
	Limit    int    `json:"limit,omitempty" jsonschema:"How many pairs, suppressed pairs, unattributed mappings and unmapped pods to show, each list apart, 1 to 500; the default is 50."`
}

// ServiceContextOutput is the answer of the service_context tool. Everything in
// it is derived: the service's one depends_on hop over the window, and the
// services that shared a pod, a node or a host with a pod of that hop, by
// interval intersection over instances, at one token.
type ServiceContextOutput struct {
	Token        string           `json:"token"`
	Derived      bool             `json:"derived"`
	Policy       Policy           `json:"policy"`
	Service      Ref              `json:"service"`
	Window       Window           `json:"window"`
	NodeOnly     bool             `json:"node_only"`
	Callers      []HopService     `json:"callers"`
	Callees      []HopService     `json:"callees"`
	Pairs        []ServicePair    `json:"pairs"`
	Suppressed   []ServicePair    `json:"suppressed"`
	Unattributed []UnattributedAt `json:"unattributed"`
	Unmapped     []UnmappedPod    `json:"unmapped"`
	Quarantined  []Quarantined    `json:"quarantined"`
	Ubiquity     ContextUbiquity  `json:"ubiquity"`
	Counts       ContextCounts    `json:"counts"`
	Truncation   ContextCut       `json:"truncation"`
	Notes        []string         `json:"notes"`
}

// HopService is a caller or a callee: the depends_on edge between it and the
// service, as stored relations assert it, read with the cascade.
type HopService struct {
	Service    Ref      `json:"service"`
	Edge       EdgeRef  `json:"edge"`
	Subject    string   `json:"subject"`
	Basis      string   `json:"basis"`
	Rule       string   `json:"rule"`
	AssertedBy []string `json:"asserted_by"`
	Spans      []Span   `json:"spans"`
	Duration   string   `json:"duration"`
	// Instances counts the instance_of elements read for the service.
	Instances int `json:"instances"`
	// Recency is how long the producer that saw the edge most recently is known
	// not to have seen it, at the window's last instant; null when no span of the
	// edge reaches that instant, and when a producer of it has no TTL.
	Recency *Recency `json:"recency"`
}

// Recency is the derived recency of a dependency at the last instant of the
// answer's window (to less one nanosecond): the freshest of the producers whose
// reference to the edge is live there, the last instant the stored records say it
// saw the edge, a lower bound on how long it has not seen it since, and a
// confidence that halves with that silence. It is labelled derived here and
// nowhere inside it, it is not a stored fact, and it decides no member, order or
// cut. The bound subtracts the extension interval the coalescer may have absorbed
// refreshes over, so a silence shorter than that does not show, and it is
// measured to the window's last instant, so a window that ends after the newest
// records shows the time since them as silence.
type Recency struct {
	Basis   string `json:"basis"`
	Subject string `json:"subject"`
	Rule    string `json:"rule"`
	// Producer is the producer seen most recently.
	Producer string `json:"producer"`
	// LastSeen is the last instant the records say Producer saw the edge, not after
	// the window's last instant.
	LastSeen string `json:"last_seen"`
	// SilentAtLeast is a duration: a lower bound on how long Producer has not seen
	// the edge, never more than the truth.
	SilentAtLeast string `json:"silent_at_least"`
	// Confidence is 2 to the power of minus the bound over the half-life, rounded to
	// four decimals: 1 for a bound of zero, and one half for each half-life more.
	Confidence float64 `json:"confidence"`
}

// ServicePair is a member of the hop and another service that shared a place by
// one mechanism: the union over instances of the intersection of every leg.
type ServicePair struct {
	Member Ref `json:"member"`
	// MemberRole is "seed", "caller", "callee" or "caller|callee".
	MemberRole string `json:"member_role"`
	Peer       Ref    `json:"peer"`
	// PeerRole is "none" for a service outside the hop.
	PeerRole string `json:"peer_role"`
	// Mechanism is "shared_pod", "shared_node" or "shared_host".
	Mechanism   string     `json:"mechanism"`
	Subject     string     `json:"subject"`
	Basis       string     `json:"basis"`
	Rule        string     `json:"rule"`
	Overlaps    []PairSpan `json:"overlaps"`
	Duration    string     `json:"duration"`
	Class       string     `json:"class"`
	Transitions int        `json:"transitions"`
	// Vias are the pods, nodes or hosts shared, the first pairVias by id.
	Vias      []PairVia `json:"vias"`
	ViasTotal int       `json:"vias_total"`
	// Paths counts the contributions: the seed pods and the entries the pair was
	// found through.
	Paths int `json:"paths"`
	// Coverage is zero unless ubiquity was judged for the peer.
	Coverage Coverage `json:"coverage"`
	// SuppressedReason is set only in the suppressed list.
	SuppressedReason string `json:"suppressed_reason,omitempty"`
}

// PairSpan is a span of a pair. It ends where a leg of a contribution live just
// before it ends; ends names those legs, once for each leg and reason.
type PairSpan struct {
	Start             string    `json:"start"`
	End               string    `json:"end"`
	EndReason         *string   `json:"end_reason"`
	EndSentence       string    `json:"end_sentence"`
	BeganBeforeWindow bool      `json:"began_before_window,omitempty"`
	Ends              []PairEnd `json:"ends"`
}

// PairEnd is a leg that ends where a pair span does. Leg is member_instance_of,
// member_part_of, peer_part_of, peer_instance_of or placement.
type PairEnd struct {
	Leg         string  `json:"leg"`
	EndReason   *string `json:"end_reason"`
	EndSentence string  `json:"end_sentence"`
}

// PairVia is a pod, node or host a pair shared, and when.
type PairVia struct {
	Via   Ref    `json:"via"`
	Spans []Span `json:"spans"`
}

// Coverage is how many of the frontier's nodes a peer shared with the seed pods.
type Coverage struct {
	Covered  int `json:"covered"`
	Frontier int `json:"frontier"`
}

// UnmappedPod is a pod that shared a place with a seed pod and had no instance
// mapped singly to a service over its spans: shown, never guessed.
type UnmappedPod struct {
	Pod     Ref    `json:"pod"`
	Subject string `json:"subject"`
	Basis   string `json:"basis"`
	Rule    string `json:"rule"`
	Spans   []Span `json:"spans"`
}

// ContextUbiquity says whether the ubiquity rule was judged, and over how many
// nodes.
type ContextUbiquity struct {
	Judged        bool   `json:"judged"`
	FrontierNodes int    `json:"frontier_nodes"`
	Statement     string `json:"statement"`
}

// ContextCounts are the exact sizes of an answer, whatever it shows.
type ContextCounts struct {
	Callers      int `json:"callers"`
	Callees      int `json:"callees"`
	Pairs        int `json:"pairs"`
	PairsShown   int `json:"pairs_shown"`
	Suppressed   int `json:"suppressed"`
	Unattributed int `json:"unattributed"`
	Unmapped     int `json:"unmapped"`
	Quarantined  int `json:"quarantined"`
	// SeedPods counts the pods the query searched from.
	SeedPods int `json:"seed_pods"`
	// PodQuarantined counts the quarantined edges the pod searches read and did
	// not follow, which the answer does not list.
	PodQuarantined int `json:"pod_quarantined"`
}

// ContextCut reports what the extraction's own caps left out. While incomplete is
// true the answer is not the whole context.
type ContextCut struct {
	Incomplete        bool `json:"incomplete"`
	MembersDropped    int  `json:"members_dropped"`
	InstancesDropped  int  `json:"instances_dropped"`
	SeedPodsDropped   int  `json:"seed_pods_dropped"`
	PairsDropped      int  `json:"pairs_dropped"`
	SuppressedDropped int  `json:"suppressed_dropped"`
	// PodSearchesCut counts the pod searches that a cap of their own cut.
	PodSearchesCut int `json:"pod_searches_cut"`
}

// pairVias is how many vias of a pair an answer lists.
const pairVias = 8

// The legs a pair span can end by, in the order they are listed. The first four
// are the mapping legs of a contribution, named from the pair's member; the last
// is the pod-level entry.
const (
	legMemberInstanceOf = "member_instance_of"
	legMemberPartOf     = "member_part_of"
	legPeerPartOf       = "peer_part_of"
	legPeerInstanceOf   = "peer_instance_of"
	legPlacement        = "placement"
)

var pairLegNames = []string{legMemberInstanceOf, legMemberPartOf, legPeerPartOf, legPeerInstanceOf, legPlacement}

const (
	mappingAmbiguous   = "the leg goes on, but its instance became part of several pods, or an instance of several services, here"
	placementAmbiguous = "the leg goes on, but its placement became ambiguous here"
	unmappedEnded      = "the pod's instances became mapped again, or the co-residency ended"
	endLivenessExpiry  = "liveness_expiry"
)

// ServiceContext shows the services a service calls and is called by, and the
// services that shared a place with them, over a window. It runs one
// extraction under the package's default policy, reads the service's depends_on
// edges once more in each direction the hop has a member in, for the recency of
// each caller and callee, and reads nothing else but the names.
func (s *Source) ServiceContext(ctx context.Context, in ServiceContextInput) (ServiceContextOutput, error) {
	req := s.newRequest()
	req.nameText(in.Service)
	return run(s, ctx, true, req, func(ctx context.Context) (ServiceContextOutput, error) {
		var zero ServiceContextOutput
		seed, err := s.parseEntity("service", in.Service)
		if err != nil {
			return zero, err
		}
		if seed.Type() != catalog.Service {
			return zero, invalid("The argument \"service\" must be a service id; got a %s id.", seed.Type())
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

		res, err := extract.NewExtractor(s.st).Extract(ctx, extract.Exploratory{Service: seed},
			interval.Window{From: from, To: to, AsOf: token}, extract.Options{NodeOnly: in.NodeOnly})
		if err != nil {
			return zero, err
		}

		pol := extract.DefaultPolicy()
		out := ServiceContextOutput{
			Token: tokenString(token), Derived: true,
			Policy: Policy{
				Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion, Extract: extract.PolicyVersion,
				Rank: s.rankDigest, Names: NameTableVersion, Withhold: WithholdVersion, Decay: decay.PolicyVersion,
			},
			Service: ref(seed), Window: windowOf(from, to), NodeOnly: in.NodeOnly,
			Callers: []HopService{}, Callees: []HopService{}, Pairs: []ServicePair{}, Suppressed: []ServicePair{},
			Unattributed: []UnattributedAt{}, Unmapped: []UnmappedPod{}, Quarantined: []Quarantined{},
			Ubiquity: ContextUbiquity{
				Judged: res.Ubiquity.Judged, FrontierNodes: res.Ubiquity.FrontierNodes,
				Statement: fmt.Sprintf("a service outside the hop that shares a node with the seed pods on at least %d/%d of the frontier, judged only on a frontier of at least %d nodes, is suppressed as ubiquitous",
					pol.UbiquityNum, pol.UbiquityDen, pol.UbiquityMinNodes),
			},
			Notes: notesFor(ahead),
		}

		roles := map[identity.Fingerprint]string{}
		for _, m := range res.Members {
			roles[m.Service] = m.Role.String()
		}
		last, err := s.hopEvidence(ctx, seed, interval.Window{From: from, To: to, AsOf: res.Token}, res.Members[1:])
		if err != nil {
			return zero, err
		}
		at := to.Add(-time.Nanosecond)
		for _, m := range res.Members[1:] {
			for _, el := range m.Dependency {
				switch {
				case el.Subject.B == seed:
					out.Callers = append(out.Callers, hopService(m, el, to, recencyOf(last[el.Subject], at)))
				case el.Subject.A == seed:
					out.Callees = append(out.Callees, hopService(m, el, to, recencyOf(last[el.Subject], at)))
				}
			}
		}
		for _, p := range head(res.Pairs, limit) {
			out.Pairs = append(out.Pairs, servicePair(p, roles, to))
		}
		for _, p := range head(res.Suppressed, limit) {
			out.Suppressed = append(out.Suppressed, servicePair(p, roles, to))
		}
		unattributed := dedupeUnattributed(res.Unattributed)
		for _, u := range head(unattributed, limit) {
			out.Unattributed = append(out.Unattributed, contextUnattributed(u, to))
		}
		for _, u := range head(res.Unmapped, limit) {
			spans := make([]Span, len(u.Spans))
			for i, sp := range u.Spans {
				spans[i] = endedSpan(sp, to, unmappedEnded)
			}
			out.Unmapped = append(out.Unmapped, UnmappedPod{
				Pod: ref(u.Pod), Subject: SubjectDerived, Basis: BasisDerived, Rule: RuleUnmapped, Spans: spans,
			})
		}
		for _, el := range head(res.Quarantined, maxQuarantined) {
			if q, ok := quarantinedOf(el); ok {
				out.Quarantined = append(out.Quarantined, q)
			}
		}

		podQuarantined, podsCut := 0, 0
		for _, fr := range res.PodResults {
			podQuarantined += len(fr.Quarantined)
			if podSearchCut(fr.Truncated) {
				podsCut++
			}
		}
		out.Counts = ContextCounts{
			Callers: len(out.Callers), Callees: len(out.Callees),
			Pairs: len(res.Pairs), PairsShown: len(out.Pairs), Suppressed: len(res.Suppressed),
			Unattributed: len(unattributed), Unmapped: len(res.Unmapped), Quarantined: len(res.Quarantined),
			SeedPods: len(res.PodResults), PodQuarantined: podQuarantined,
		}
		tr := res.Truncated
		out.Truncation = ContextCut{
			MembersDropped: len(tr.Members), InstancesDropped: len(tr.Instances), SeedPodsDropped: len(tr.SeedPods),
			PairsDropped: tr.Pairs, SuppressedDropped: tr.SuppressedDropped, PodSearchesCut: podsCut,
		}
		cut := out.Truncation
		out.Truncation.Incomplete = cut.MembersDropped+cut.InstancesDropped+cut.SeedPodsDropped+cut.PairsDropped+cut.SuppressedDropped+cut.PodSearchesCut > 0

		if len(res.Members[0].Instances) == 0 {
			out.Notes = append(out.Notes, "No instance of the service was read in the window, so no pod of it is known and it has no pair.")
		}
		if endsByExpiry(out) {
			out.Notes = append(out.Notes, "A dependency, an instance and its pod mapping are sightings with a TTL: a span that ends by liveness_expiry ends one TTL after the last sighting, so a service that stopped calling, or an instance that stopped reporting, is shown until then.")
		}
		out.Notes = append(out.Notes, contextCutNotes(out.Truncation, pol)...)
		if !res.Ubiquity.Judged && len(res.PodResults) > 0 {
			out.Notes = append(out.Notes, fmt.Sprintf("Ubiquity was not judged: the seed pods were on %d nodes alone, fewer than %d.", res.Ubiquity.FrontierNodes, pol.UbiquityMinNodes))
		}
		if len(out.Pairs) < len(res.Pairs) {
			out.Notes = append(out.Notes, shown(len(out.Pairs), len(res.Pairs), "pairs", "longest first"))
		}
		if len(out.Suppressed) < len(res.Suppressed) {
			out.Notes = append(out.Notes, shown(len(out.Suppressed), len(res.Suppressed), "suppressed pairs", "longest first"))
		}
		if len(out.Unattributed) < len(unattributed) {
			out.Notes = append(out.Notes, shown(len(out.Unattributed), len(unattributed), "unattributed mappings", "in the package's order"))
		}
		if len(out.Unmapped) < len(res.Unmapped) {
			out.Notes = append(out.Notes, shown(len(out.Unmapped), len(res.Unmapped), "unmapped pods", "in the package's order"))
		}
		if len(out.Quarantined) < len(res.Quarantined) {
			out.Notes = append(out.Notes, shown(len(out.Quarantined), len(res.Quarantined), "quarantined edges", "in the package's order"))
		}
		if len(res.Suppressed) > 0 {
			out.Notes = append(out.Notes, "Suppressed pairs are with a service that shares a node with the seed pods on most of the frontier, such as a DaemonSet's; they are shown, never dropped.")
		}

		err = s.finish(ctx, &out, &out.Notes, token, from, to,
			namedTail{"unmapped pods", listTail(&out.Unmapped)},
			namedTail{"suppressed pairs", listTail(&out.Suppressed)},
			namedTail{"pairs", listTail(&out.Pairs)},
			namedTail{"unattributed mappings", listTail(&out.Unattributed)},
			namedTail{"quarantined edges", listTail(&out.Quarantined)},
			namedTail{"callees", listTail(&out.Callees)},
			namedTail{"callers", listTail(&out.Callers)})
		if err != nil {
			return zero, err
		}
		out.Counts.PairsShown = len(out.Pairs)
		return out, nil
	})
}

// hopService writes the depends_on element between a member and the seed.
func hopService(m extract.Member, el interval.Element, to time.Time, recency *Recency) HopService {
	return HopService{
		Service: ref(m.Service), Edge: edgeRef(el.Subject), Subject: subjectOfElement(el.Subject),
		Basis: BasisDerived, Rule: RuleExistence, AssertedBy: piecesProducers(el.Pieces),
		Spans: pieceSpans(el.Pieces, to), Duration: interval.Duration(interval.Spans(el)).String(),
		Instances: len(m.Instances), Recency: recency,
	}
}

// hopEvidence reads the seed's depends_on edges again with the evidence option,
// at the token and window the extraction read them at, and returns, for each
// edge with a span at the window's last instant, the evidence there. It makes one
// interval read in each direction the hop has a member in, reverse for the
// callers and forward for the callees, and none for an answer with no member.
// The extraction, its policy and what it returns are untouched, and the option
// adds no store read to a read that has it off.
func (s *Source) hopEvidence(ctx context.Context, seed identity.Fingerprint, w interval.Window, members []extract.Member) (map[store.Subject][]interval.Evidence, error) {
	var dirs []store.Direction
	if slices.ContainsFunc(members, func(m extract.Member) bool { return m.Role&extract.RoleCaller != 0 }) {
		dirs = append(dirs, store.Reverse)
	}
	if slices.ContainsFunc(members, func(m extract.Member) bool { return m.Role&extract.RoleCallee != 0 }) {
		dirs = append(dirs, store.Forward)
	}
	out := map[store.Subject][]interval.Evidence{}
	rd := interval.NewReader(s.st, interval.Options{Evidence: true})
	for _, d := range dirs {
		res, err := rd.Edges(ctx, seed, d, w)
		if err != nil {
			return nil, err
		}
		for _, el := range res.Elements {
			if el.Subject.Relation == catalog.DependsOn && el.Last != nil {
				out[el.Subject] = el.Last
			}
		}
	}
	return out, nil
}

// recencyOf is the recency of an edge from its evidence at the instant it was read
// at, or nil when the log proves no silence: no span at that instant, or a
// producer with no TTL.
func recencyOf(last []interval.Evidence, at time.Time) *Recency {
	r, ok := decay.Of(last, at, decay.DefaultPolicy())
	if !ok {
		return nil
	}
	return &Recency{
		Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency,
		Producer: string(r.Producer), LastSeen: fmtTime(r.LastSeen), SilentAtLeast: r.SilentAtLeast.String(),
		// Rounded so that the same bytes come out on every platform: the last bit of
		// a float power can differ between architectures.
		Confidence: math.Round(r.Confidence*1e4) / 1e4,
	}
}

// servicePair writes a pair of the extraction.
func servicePair(p extract.Pair, roles map[identity.Fingerprint]string, to time.Time) ServicePair {
	out := ServicePair{
		Member: ref(p.Member), MemberRole: roles[p.Member], Peer: ref(p.Peer), PeerRole: "none",
		Mechanism: p.Mechanism.String(), Subject: SubjectDerived, Basis: BasisDerived,
		Overlaps: []PairSpan{}, Duration: p.Duration.String(), Class: p.Class.String(), Transitions: p.Transitions,
		Vias: []PairVia{}, ViasTotal: len(p.Vias), Paths: len(p.Contributions),
		Coverage: Coverage{Covered: p.Coverage[0], Frontier: p.Coverage[1]},
	}
	if r, ok := roles[p.Peer]; ok {
		out.PeerRole = r
	}
	switch p.Mechanism {
	case extract.SharedPod:
		out.Rule = RulePairSharedPod
	case extract.SharedNode:
		out.Rule = RulePairSharedNode
	case extract.SharedHost:
		out.Rule = RulePairSharedHost
	}
	if p.Reason != coresidency.ReasonNone {
		out.SuppressedReason = p.Reason.String()
	}
	for _, sp := range p.Spans {
		d := derivedSpan(sp.Span, to, "pair")
		out.Overlaps = append(out.Overlaps, PairSpan{
			Start: d.Start, End: d.End, EndReason: d.EndReason, EndSentence: d.EndSentence,
			BeganBeforeWindow: sp.Before, Ends: pairEnds(p, sp.Ends),
		})
	}
	// The package keeps the vias in a map: they are listed in id order.
	vias := make([]identity.Fingerprint, 0, len(p.Vias))
	for fp := range p.Vias {
		vias = append(vias, fp)
	}
	slices.SortFunc(vias, store.CompareFingerprints)
	for _, fp := range head(vias, pairVias) {
		out.Vias = append(out.Vias, PairVia{Via: ref(fp), Spans: derivedSpans(p.Vias[fp], to, "co-residence through it")})
	}
	return out
}

// pairEnds names the legs that end where a pair span does, once for each leg and
// reason, ordered by leg and then by reason.
//
// The package names the mapping legs of a contribution from its seed pod's side:
// the instance_of and part_of of the instance on the seed pod, then the peer's.
// It orders the two members of a pair by id, so a contribution found from the
// pod of the pair's second member has the member's legs last, and its names are
// swapped here: leg 0 with 3 and 1 with 2.
func pairEnds(p extract.Pair, ends []extract.End) []PairEnd {
	out := []PairEnd{}
	type key struct {
		leg    int
		reason string
	}
	seen := map[key]bool{}
	for _, e := range ends {
		leg := 4
		var reason *string
		var sentence string
		switch {
		case e.Leg < 4:
			leg = e.Leg
			if p.Contributions[e.Contribution].Legs[0].Subject.B != p.Member {
				leg = 3 - e.Leg
			}
			if e.Unattributed {
				reason, sentence = strPtr(endReasonNotKnown), mappingAmbiguous
			} else {
				reason, sentence = sourceReason(e.Source)
			}
		case e.Pod != nil && !e.Pod.Unattributed:
			reason, sentence = sourceReason(e.Pod.Source)
		default:
			reason, sentence = strPtr(endReasonNotKnown), placementAmbiguous
		}
		k := key{leg, *reason}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, PairEnd{Leg: pairLegNames[leg], EndReason: reason, EndSentence: sentence})
	}
	slices.SortFunc(out, func(a, b PairEnd) int {
		if c := cmp.Compare(slices.Index(pairLegNames, a.Leg), slices.Index(pairLegNames, b.Leg)); c != 0 {
			return c
		}
		return cmp.Compare(*a.EndReason, *b.EndReason)
	})
	return out
}

// dedupeUnattributed drops the repeats among the placement refusals of the pod
// searches: each seed pod's search reports the refusal of a node it shares, and
// the answer has no seed pod to tell them apart by. The extraction's own refusals
// of a mapping are kept as they are.
func dedupeUnattributed(us []extract.Unattributed) []extract.Unattributed {
	var out []extract.Unattributed
	seen := map[string]bool{}
	for _, u := range us {
		if u.Mapping == 0 {
			var b strings.Builder
			fmt.Fprintf(&b, "%s|%d|", u.Subject, u.Placement)
			for _, sp := range u.Spans {
				fmt.Fprintf(&b, "%s-%s,", fmtTime(sp.Start), fmtTime(sp.End))
			}
			b.WriteByte('|')
			for _, c := range u.Candidates {
				fmt.Fprintf(&b, "%s,", c)
			}
			if seen[b.String()] {
				continue
			}
			seen[b.String()] = true
		}
		out = append(out, u)
	}
	return out
}

// contextUnattributed writes a refusal of the extraction: a mapping it would not
// guess, or a placement one of its pod searches would not.
func contextUnattributed(u extract.Unattributed, to time.Time) UnattributedAt {
	if u.Mapping == 0 {
		return coUnattributed(coresidency.Unattributed{Subject: u.Subject, Spans: u.Spans, Reason: u.Placement, Candidates: u.Candidates}, to)
	}
	reason := u.Mapping.String()
	out := UnattributedAt{
		Entity: ref(u.Subject), Reason: reason, Subject: SubjectDerived, Basis: BasisDerived, Rule: RuleUnattributed + reason,
		Spans: ambiguitySpans(u.Spans, to), Candidates: refs(u.Candidates),
	}
	switch u.Mapping {
	case extract.SeveralPods:
		out.Relation, out.TargetType = string(catalog.PartOf), string(catalog.K8sPod)
		out.Note = "The instance was part of two or more pods at once, so it counts for none of them."
	case extract.SeveralServices:
		out.Relation, out.TargetType = string(catalog.InstanceOf), string(catalog.Service)
		out.Note = "The instance was an instance of two or more services at once, so it counts for none of them."
	}
	return out
}

// podSearchCut says a pod search was cut by a cap of its own.
func podSearchCut(t coresidency.Truncation) bool {
	perVia := 0
	for _, v := range t.PerVia {
		perVia += v.Dropped
	}
	return len(t.Nodes)+len(t.Hosts)+perVia+t.Results+t.SuppressedDropped > 0
}

// endsByExpiry says a span of the answer ends by liveness expiry.
func endsByExpiry(out ServiceContextOutput) bool {
	expiry := func(r *string) bool { return r != nil && *r == endLivenessExpiry }
	for _, hs := range [][]HopService{out.Callers, out.Callees} {
		for _, h := range hs {
			if slices.ContainsFunc(h.Spans, func(sp Span) bool { return expiry(sp.EndReason) }) {
				return true
			}
		}
	}
	for _, ps := range [][]ServicePair{out.Pairs, out.Suppressed} {
		for _, p := range ps {
			for _, sp := range p.Overlaps {
				if slices.ContainsFunc(sp.Ends, func(e PairEnd) bool { return expiry(e.EndReason) }) {
					return true
				}
			}
		}
	}
	return false
}

// contextCutNotes writes a sentence for each cap of the extraction that cut the
// answer.
func contextCutNotes(c ContextCut, pol extract.Policy) []string {
	var out []string
	if c.MembersDropped > 0 {
		out = append(out, fmt.Sprintf("%d neighbours were not read: the hop keeps at most %d services, the service included, longest dependency first.", c.MembersDropped, pol.MaxMembers))
	}
	if c.InstancesDropped > 0 {
		out = append(out, fmt.Sprintf("%d instances were not read: a member keeps at most %d.", c.InstancesDropped, pol.MaxInstances))
	}
	if c.SeedPodsDropped > 0 {
		out = append(out, fmt.Sprintf("%d pods were not searched: the query searches at most %d seed pods.", c.SeedPodsDropped, pol.MaxSeedPods))
	}
	if c.PairsDropped > 0 {
		out = append(out, fmt.Sprintf("%d pairs were dropped: the query keeps at most %d.", c.PairsDropped, pol.MaxPairs))
	}
	if c.SuppressedDropped > 0 {
		out = append(out, fmt.Sprintf("%d suppressed pairs were dropped: the query keeps at most %d.", c.SuppressedDropped, pol.MaxPairs))
	}
	if c.PodSearchesCut > 0 {
		out = append(out, fmt.Sprintf("%d pod searches were cut by their own caps; co_residents of a seed pod shows what.", c.PodSearchesCut))
	}
	return out
}
