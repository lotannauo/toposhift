package mcp

import (
	"context"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

// DescribeSchemaInput is the input of the describe_schema tool: none.
type DescribeSchemaInput struct{}

// DescribeSchemaOutput is the answer of the describe_schema tool: what the graph
// can hold, how failure travels along each relation, how names and descriptions
// are read, and the vocabulary every answer uses. It reads no store.
type DescribeSchemaOutput struct {
	// RangeReads says whether the range tools run on this server's store.
	RangeReads bool             `json:"range_reads"`
	Policy     Policy           `json:"policy"`
	Entities   []SchemaEntity   `json:"entities"`
	Relations  []SchemaRelation `json:"relations"`
	Names      SchemaNames      `json:"names"`
	Payload    SchemaPayload    `json:"payload"`
	Vocabulary Vocabulary       `json:"vocabulary"`
	Notes      []string         `json:"notes"`
}

// SchemaEntity is an entity type of the catalog.
type SchemaEntity struct {
	Type            string      `json:"type"`
	Layer           string      `json:"layer"`
	LayerMeaning    string      `json:"layer_meaning"`
	IdentifyingKeys []SchemaKey `json:"identifying_keys"`
	Named           bool        `json:"named"`
}

// SchemaKey is an identifying attribute of an entity type.
type SchemaKey struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Optional bool   `json:"optional"`
}

// SchemaRelation is a relation type of the catalog.
type SchemaRelation struct {
	Type      string           `json:"type"`
	Endpoints []SchemaEndpoint `json:"endpoints"`
	// Propagation is "down", "up" or "none"; propagation_meaning says it in words.
	Propagation        string `json:"propagation"`
	PropagationMeaning string `json:"propagation_meaning"`
	// Layer is where the relation's edges are stored; null for a derived relation.
	Layer *string `json:"layer"`
	// Derived says the relation is computed at read time and never stored.
	Derived bool   `json:"derived"`
	Subject string `json:"subject"`
}

// SchemaEndpoint is a pair of entity types a relation connects.
type SchemaEndpoint struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// SchemaNames is the table a name is read from.
type SchemaNames struct {
	Version int        `json:"version"`
	Keys    []NameKeys `json:"keys"`
	How     string     `json:"how"`
	Unnamed []string   `json:"unnamed_types"`
	Note    string     `json:"note"`
}

// NameKeys are the top-level keys a type's name and namespace are read from.
type NameKeys struct {
	Type      string   `json:"type"`
	Name      []string `json:"name"`
	Namespace []string `json:"namespace"`
}

// SchemaPayload is the table of keys a description may print.
type SchemaPayload struct {
	Version int          `json:"version"`
	Types   []PayloadKey `json:"types"`
	How     string       `json:"how"`
}

// PayloadKey is the keys of one type: those printed, and those withheld on
// purpose. Every other key is withheld too, and an answer names it.
type PayloadKey struct {
	Type     string   `json:"type"`
	Allowed  []string `json:"allowed"`
	Withheld []string `json:"withheld"`
}

// Vocabulary is the words every answer uses.
type Vocabulary struct {
	Basis      []Word `json:"basis"`
	Subject    []Word `json:"subject"`
	Rules      []Word `json:"rules"`
	EndReasons []Word `json:"end_reasons"`
	Classes    []Word `json:"classes"`
	// Recency is the fields of a recency, the object service_context gives each
	// caller and callee.
	Recency []Word `json:"recency"`
}

// Word is a term and what it means.
type Word struct {
	Term    string `json:"term"`
	Meaning string `json:"meaning"`
}

// DescribeSchema describes the graph's types, relations and vocabulary.
func (s *Source) DescribeSchema(ctx context.Context, _ DescribeSchemaInput) (DescribeSchemaOutput, error) {
	req := s.newRequest()
	if err := ctx.Err(); err != nil {
		return DescribeSchemaOutput{}, s.fail(err, req)
	}
	out := DescribeSchemaOutput{
		RangeReads: s.rangeReads,
		Policy: Policy{
			Interval: interval.PolicyVersion, BlastRadius: blastradius.PolicyVersion, GraphDiff: graphdiff.PolicyVersion,
			CoResidency: coresidency.PolicyVersion, Extract: extract.PolicyVersion, Names: NameTableVersion, Payload: PayloadTableVersion, Withhold: WithholdVersion,
			Find: FindVersion, Decay: decay.PolicyVersion,
		},
		Entities: []SchemaEntity{}, Relations: []SchemaRelation{},
		Vocabulary: vocabulary,
		Notes: []string{
			"Every id is an entity type, a colon, and 32 lowercase hex digits: a hash of the exact identifying attributes. Use resolve_entity to compute one.",
			"Hosts are unnamed on purpose: the node agent drops the host's name and every address from its description.",
			"An endpoint (topo.endpoint) is an address within its realm, \"global\" or \"k8s/<cluster>\". It is unnamed and has no description. Its id reveals its address, because the realm and the address can be enumerated and hashed, so an endpoint's id, or an edge to an endpoint, is printed only when the request named that endpoint: by its id, or through resolve_entity's realm and address. Elsewhere such an edge is withheld and counted under counts.withheld, and no error sentence prints such an id. resolve_entity takes the realm and the address exactly as stored: IPv4 in dotted decimal, IPv6 in lowercase compressed form, an IPv4-mapped address as IPv4.",
			"Every range answer is read at one token, named in the answer; pass it as asof to read the same world again.",
			"To find an id from a name, call find_entities: it compares names only, never another key, and never lists an endpoint or a host.",
			"service_context runs the extraction of one service: its callers and callees, one depends_on hop, and the services that shared a pod, a node or a host with them, each pair checked by interval intersection over instances. A pair span's ends name the leg that ended: member_instance_of, member_part_of, peer_part_of, peer_instance_of or placement.",
		},
	}
	for e := range s.cat.Entities() {
		se := SchemaEntity{
			Type: string(e.Type()), Layer: e.Layer().String(), LayerMeaning: layerMeaning(e.Layer()),
			IdentifyingKeys: []SchemaKey{},
		}
		for k := range e.Keys() {
			se.IdentifyingKeys = append(se.IdentifyingKeys, SchemaKey{Name: string(k.Name), Kind: k.Kind.String(), Optional: k.Optional})
		}
		_, se.Named = nameTable[e.Type()]
		out.Entities = append(out.Entities, se)
	}
	for r := range s.cat.Relations() {
		sr := SchemaRelation{
			Type: string(r.Type()), Endpoints: []SchemaEndpoint{},
			Propagation: r.Propagation().String(), PropagationMeaning: propagationMeaning(r.Propagation()),
			Derived: r.Derived(), Subject: subjectOfRelation(r.Type()),
		}
		for ep := range r.Endpoints() {
			sr.Endpoints = append(sr.Endpoints, SchemaEndpoint{From: string(ep.From), To: string(ep.To)})
		}
		if !r.Derived() {
			l := r.Layer().String()
			sr.Layer = &l
		}
		out.Relations = append(out.Relations, sr)
	}

	out.Names = SchemaNames{
		Version: NameTableVersion, Keys: []NameKeys{}, Unnamed: []string{},
		How:  "A name is read from the description of the producer that wins under the store's ranks, at the last instant of the answer's window, else at its first, from a top-level JSON string under the first of these keys. It is never taken from an identity attribute (except a service's, whose description is its identity), from a losing producer or from a peer. A name or a namespace that is over 253 bytes, holds a control character (a newline, a tab, U+2028 and U+2029 included), holds a format character (Unicode Cf: zero-width, bidi controls, tag characters) or holds an endpoint id's text is not printed: it is null, with a name_note, and find_entities never matches it.",
		Note: "A type not listed has no name.",
	}
	for e := range s.cat.Entities() {
		k, ok := nameTable[e.Type()]
		if !ok {
			out.Names.Unnamed = append(out.Names.Unnamed, string(e.Type()))
			continue
		}
		out.Names.Keys = append(out.Names.Keys, NameKeys{Type: string(e.Type()), Name: slices.Clone(k.name), Namespace: orEmpty(k.namespace)})
	}
	out.Payload = SchemaPayload{
		Version: PayloadTableVersion, Types: []PayloadKey{},
		How: "A description is printed only through this table of top-level keys. A key that is not allowed is withheld, and the answer names it; a payload that is not a JSON object is withheld whole. A name, a namespace or a service instance's version in it that is over 253 bytes, holds a control character, holds a format character (Unicode Cf: zero-width, bidi controls, tag characters) or holds an endpoint id's text is null, and its key is named in withheld_keys.",
	}
	for e := range s.cat.Entities() {
		if t, ok := payloadTable[e.Type()]; ok {
			out.Payload.Types = append(out.Payload.Types, PayloadKey{
				Type: string(e.Type()), Allowed: slices.Clone(t.allowed), Withheld: orEmpty(t.withheld),
			})
		}
	}
	if err := s.egress(&out, req); err != nil {
		return DescribeSchemaOutput{}, s.fail(err, req)
	}
	return out, nil
}

func orEmpty(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return slices.Clone(xs)
}

func layerMeaning(l catalog.Layer) string {
	switch l {
	case catalog.L0:
		return "the physical fabric: racks, switches, ports; churn in weeks"
	case catalog.L1:
		return "host placement: hosts and zones; churn in hours"
	case catalog.L2:
		return "workload placement: nodes, pods, containers, processes; churn in seconds"
	case catalog.L3:
		return "logical dependency between services"
	}
	return ""
}

func propagationMeaning(p catalog.Propagation) string {
	switch p {
	case catalog.PropagateDown:
		return "the failure of the to side degrades the from side: a substrate failing under what runs on it"
	case catalog.PropagateUp:
		return "the failure of the to side degrades the from side: a callee failing under its callers"
	case catalog.PropagateNone:
		return "failure does not travel along it: the relation is structural or an aggregation"
	}
	return ""
}

var vocabulary = Vocabulary{
	Basis: []Word{
		{"fact", "a stored record, returned as stored"},
		{"derived", "computed from records at a token under a policy version"},
	},
	Subject: []Word{
		{"asserted", "an entity or a stored relation that a producer asserted"},
		{"derived", "anything no producer asserted: a reach, a co-resident, a pair"},
	},
	Rules: []Word{
		{RuleExistence, "an entity's or an edge's existence, folded from its records; asserted_by lists the producers"},
		{RuleRecency, "a dependency's recency at the window's last instant: a lower bound on how long its freshest producer has not seen the edge, from the stored records, so a silence shorter than half the producer's expiry may not show, and a window that ends after the newest records shows the time since them as silence"},
		{RuleReach, "an entity a failing seed reaches over a chain of edges that all exist at once"},
		{RuleSharedNode, "two pods scheduled on one node at once"},
		{RuleSharedHost, "two pods on two nodes at once, each node placed on one host by an asserted runs_on"},
		{RulePairSharedPod, "two services, each through an instance mapped singly, that shared one pod"},
		{RulePairSharedNode, "two services, each through an instance mapped singly, that had a pod on one node at once"},
		{RulePairSharedHost, "two services, each through an instance mapped singly, that had pods on two nodes at once, each node placed on one host by an asserted runs_on"},
		{RuleUnmapped, "a co-resident pod with no instance mapped singly to a service over these spans; shown, never guessed"},
		{RuleUnattributed + "<reason>", "a placement or a mapping the server refuses to guess; the reason says why"},
		{RuleDiff + "<bucket>", "an element of a diff bucket: created, deleted, changed, transient, or event"},
	},
	EndReasons: []Word{
		{"producer", "deleted by its producer"},
		{"liveness_expiry", "its producer went silent past its TTL (not known to be gone)"},
		{"cascade", "an endpoint ended"},
		{"retention", "the retention horizon moved past it; history before the horizon is gone"},
		{"operator", "purged by an operator"},
		{"unattributed", "not known"},
		{endReasonReachEnded, "a derived span ended where one of its legs did (see the legs' ends), or where an ambiguity did"},
		{"null", "the span reaches the window's end; nothing after the window is reported"},
	},
	Classes: []Word{
		{"stable", "one span covering the whole window"},
		{"appeared", "one span that starts after the window's start and reaches its end"},
		{"disappeared", "one span that starts at the window's start and ends before its end"},
		{"transient", "one span that starts after the window's start and ends before its end"},
		{"flapping", "two spans or more"},
	},
	Recency: []Word{
		{"producer", "the producer that saw the edge most recently"},
		{"last_seen", "the last instant the stored records say that producer saw the edge, not after the window's last instant"},
		{"silent_at_least", "a lower bound on how long that producer has not seen the edge at the window's last instant: never more than the truth, and short of it by at most half the producer's expiry"},
		{"confidence", "2 to the power of minus silent_at_least over an eighth of the producer's expiry, rounded to four decimals: 1 for no silence, halved for each eighth of the expiry more; it is no probability and decides no member, order or cut"},
	},
}
