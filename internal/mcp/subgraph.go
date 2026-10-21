package mcp

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// The questions a temporal subgraph answers, as an answer names them.
const (
	QuestionScope          = "scope"
	QuestionCoResidents    = "co_residents"
	QuestionServiceContext = "service_context"
	QuestionBlastRadius    = "blast_radius"
)

// MaxSubgraphAnswerBytes is the cap on the JSON of a subgraph answer. A larger
// answer is refused whole, never cut. It is a placeholder, like the bounds on
// calls: no read budget has been measured.
const MaxSubgraphAnswerBytes = 16 << 20

// subgraphKnobs are for tests; production never sets them. The tests of this
// package do not run in parallel.
var subgraphKnobs struct {
	// maxBytes replaces [MaxSubgraphAnswerBytes] when it is not zero.
	maxBytes int
	// alter is called on the subgraph a question built, before anything is
	// withheld from it.
	alter func(*subgraph.Subgraph)
}

// ScopeSubgraphInput is a scope question: the entities of the scope and every
// edge with an end among them, one hop out, over a window.
type ScopeSubgraphInput struct {
	// Scope is the ids of the entities, 1 to 256; an id given twice is read once.
	Scope []string `json:"scope"`
	// From and To are the window [from, to), RFC 3339 with a zone.
	From string `json:"from"`
	To   string `json:"to"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
}

// CoResidentsSubgraphInput is a co-residency question: the pods that shared a
// node or a host with a pod over a window.
type CoResidentsSubgraphInput struct {
	Pod      string `json:"pod"`
	From     string `json:"from"`
	To       string `json:"to"`
	NodeOnly bool   `json:"node_only,omitempty"`
	AsOf     string `json:"asof,omitempty"`
}

// ServiceContextSubgraphInput is a service's context: its one depends_on hop and
// the services that shared a pod, a node or a host with it, over a window.
type ServiceContextSubgraphInput struct {
	Service  string `json:"service"`
	From     string `json:"from"`
	To       string `json:"to"`
	NodeOnly bool   `json:"node_only,omitempty"`
	AsOf     string `json:"asof,omitempty"`
}

// BlastRadiusSubgraphInput is a blast radius: what a failing entity reaches
// over a window.
type BlastRadiusSubgraphInput struct {
	Entity string `json:"entity"`
	From   string `json:"from"`
	To     string `json:"to"`
	// Depth is the most hops to follow, 1 to 6; the default is 6.
	Depth int    `json:"depth,omitempty"`
	AsOf  string `json:"asof,omitempty"`
}

// SubgraphAnswer is the answer to a subgraph question: the temporal subgraph as
// the wire format's JSON twin as a JSON value, and beside it what the format does not
// carry, the names of its entities and what was withheld from it.
type SubgraphAnswer struct {
	// Token is the one token every read of the answer used, the names' included;
	// it equals the document's.
	Token   string `json:"token"`
	Derived bool   `json:"derived"`
	// Question is scope, co_residents, service_context or blast_radius.
	Question string `json:"question"`
	Window   Window `json:"window"`
	Policy   Policy `json:"policy"`
	// Incomplete says a cap of the question's query cut what it read: the
	// subgraph's truncated section, or a pod section's, is not empty, and the
	// notes say what each cut left out.
	Incomplete bool `json:"incomplete"`
	// Names names the entities of the document's dictionary: Names[i] is
	// document.entities[i], in the same order.
	Names  []Ref          `json:"names"`
	Counts SubgraphCounts `json:"counts"`
	Notes  []string       `json:"notes"`
	// Document is the wire document's JSON twin, as a JSON value: it decodes to what
	// wire.JSON writes (the bytes sent are compact).
	Document json.RawMessage `json:"document"`
}

// SubgraphCounts are the counts of an answer.
type SubgraphCounts struct {
	Withheld SubgraphWithheld `json:"withheld"`
}

// SubgraphWithheld counts what was taken out of a subgraph because it is, or
// names, an endpoint the request did not name.
type SubgraphWithheld struct {
	Entities            int `json:"entities"`
	Edges               int `json:"edges"`
	Derived             int `json:"derived"`
	QuarantinedEntities int `json:"quarantined_entities"`
	QuarantinedEdges    int `json:"quarantined_edges"`
}

func (w SubgraphWithheld) total() int {
	return w.Entities + w.Edges + w.Derived + w.QuarantinedEntities + w.QuarantinedEdges
}

// subgraphCall is what a question resolved before its subgraph is built.
type subgraphCall struct {
	question string
	from, to time.Time
	token    uint64
	policy   Policy
	query    wire.Query
}

// ScopeSubgraph answers a scope question with the temporal subgraph of the scope.
func (s *Source) ScopeSubgraph(ctx context.Context, in ScopeSubgraphInput) (SubgraphAnswer, error) {
	req := s.newRequest()
	req.nameText(in.Scope...)
	return run(s, ctx, true, req, func(ctx context.Context) (SubgraphAnswer, error) {
		var zero SubgraphAnswer
		if len(in.Scope) == 0 {
			return zero, invalid("The argument \"scope\" must name at least one entity.")
		}
		if len(in.Scope) > maxScope {
			return zero, invalid("The argument \"scope\" names %d entities; at most %d.", len(in.Scope), maxScope)
		}
		scope := make([]identity.Fingerprint, len(in.Scope))
		for i, id := range in.Scope {
			fp, err := s.parseEntity(fmt.Sprintf("scope[%d]", i), id)
			if err != nil {
				return zero, err
			}
			scope[i] = fp
		}
		from, to, err := rangeArgs(in.From, in.To)
		if err != nil {
			return zero, err
		}
		token, asOf, err := s.pinStrict(in.AsOf)
		if err != nil {
			return zero, err
		}
		pol := subgraph.DefaultScopePolicy()
		sg, err := subgraph.ReadScope(ctx, s.st, scope, interval.Window{From: from, To: to, AsOf: token}, pol)
		if err != nil {
			return zero, err
		}
		c := subgraphCall{
			question: QuestionScope, from: from, to: to, token: token,
			policy: Policy{Interval: interval.PolicyVersion},
			query:  scopeQuery(scope, pol, asOf),
		}
		return s.subgraphAnswer(ctx, req, c, sg, nil)
	})
}

// CoResidentsSubgraph answers a co-residency question with the temporal subgraph
// of the co-residency query's result.
func (s *Source) CoResidentsSubgraph(ctx context.Context, in CoResidentsSubgraphInput) (SubgraphAnswer, error) {
	req := s.newRequest()
	req.nameText(in.Pod)
	return run(s, ctx, true, req, func(ctx context.Context) (SubgraphAnswer, error) {
		var zero SubgraphAnswer
		seed, err := s.parseEntity("pod", in.Pod)
		if err != nil {
			return zero, err
		}
		from, to, err := rangeArgs(in.From, in.To)
		if err != nil {
			return zero, err
		}
		token, asOf, err := s.pinStrict(in.AsOf)
		if err != nil {
			return zero, err
		}
		res, err := coresidency.NewFinder(s.st).Find(ctx, seed, interval.Window{From: from, To: to, AsOf: token}, coresidency.Options{NodeOnly: in.NodeOnly})
		if err != nil {
			return zero, err
		}
		sg, err := subgraph.FromCoResidency(ctx, s.st, res, subgraph.DefaultReadPolicy())
		if err != nil {
			return zero, err
		}
		c := subgraphCall{
			question: QuestionCoResidents, from: from, to: to, token: token,
			policy: Policy{Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion},
			query:  coResidentsQuery(seed, in.NodeOnly, asOf),
		}
		return s.subgraphAnswer(ctx, req, c, sg, coResidentNotes(coResidentCut(res.Truncated)))
	})
}

// ServiceContextSubgraph answers a service's context with the temporal subgraph
// of the exploratory extraction, under the package's default policy.
func (s *Source) ServiceContextSubgraph(ctx context.Context, in ServiceContextSubgraphInput) (SubgraphAnswer, error) {
	req := s.newRequest()
	req.nameText(in.Service)
	return run(s, ctx, true, req, func(ctx context.Context) (SubgraphAnswer, error) {
		var zero SubgraphAnswer
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
		token, asOf, err := s.pinStrict(in.AsOf)
		if err != nil {
			return zero, err
		}
		res, err := extract.NewExtractor(s.st).Extract(ctx, extract.Exploratory{Service: seed},
			interval.Window{From: from, To: to, AsOf: token}, extract.Options{NodeOnly: in.NodeOnly})
		if err != nil {
			return zero, err
		}
		sg, err := subgraph.FromExtraction(ctx, s.st, res, subgraph.DefaultReadPolicy())
		if err != nil {
			return zero, err
		}
		pol := extract.DefaultPolicy()
		c := subgraphCall{
			question: QuestionServiceContext, from: from, to: to, token: token,
			policy: Policy{Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion, Extract: extract.PolicyVersion},
			query:  serviceContextQuery(seed, in.NodeOnly, asOf),
		}
		return s.subgraphAnswer(ctx, req, c, sg, contextCutNotes(contextCut(res), pol))
	})
}

// BlastRadiusSubgraph answers a blast radius with the temporal subgraph of the
// walk's result.
func (s *Source) BlastRadiusSubgraph(ctx context.Context, in BlastRadiusSubgraphInput) (SubgraphAnswer, error) {
	req := s.newRequest()
	req.nameText(in.Entity)
	return run(s, ctx, true, req, func(ctx context.Context) (SubgraphAnswer, error) {
		var zero SubgraphAnswer
		seed, err := s.parseEntity("entity", in.Entity)
		if err != nil {
			return zero, err
		}
		from, to, err := rangeArgs(in.From, in.To)
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
		token, asOf, err := s.pinStrict(in.AsOf)
		if err != nil {
			return zero, err
		}
		pol := blastradius.DefaultPolicy()
		pol.MaxDepth = depth
		res, err := blastradius.NewWalker(s.st).Impact(ctx, seed, interval.Window{From: from, To: to, AsOf: token}, blastradius.Options{Policy: pol})
		if err != nil {
			return zero, err
		}
		sg, err := subgraph.FromBlastRadius(ctx, s.st, res, subgraph.DefaultReadPolicy())
		if err != nil {
			return zero, err
		}
		c := subgraphCall{
			question: QuestionBlastRadius, from: from, to: to, token: token,
			policy: Policy{Interval: interval.PolicyVersion, BlastRadius: blastradius.PolicyVersion},
			query:  blastRadiusQuery(seed, pol, asOf),
		}
		return s.subgraphAnswer(ctx, req, c, sg, blastNotes(res.Truncated, depth, pol))
	})
}

// pinStrict resolves the token of one subgraph answer, once: an absent asof is
// the store's LastSeq. Unlike [Source.pin], a token above LastSeq is refused and
// never clamped: a store restored from a backup reuses tokens, so a clamp could
// show a client that pinned a token another world under that number. asOf is
// what the query echo carries: the token the request named, or [store.Latest].
func (s *Source) pinStrict(arg string) (token, asOf uint64, err error) {
	last := s.st.LastSeq()
	if arg == "" {
		return last, store.Latest, nil
	}
	n, err := strconv.ParseUint(arg, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != arg {
		return 0, 0, invalid("The argument \"asof\" must be a token as a decimal number, such as 81234; got %s.", quote(arg))
	}
	if n > last {
		return 0, 0, &Error{Message: fmt.Sprintf("Token %d is ahead of the store (at %d); it is never clamped here.", n, last), Cause: ErrFutureToken}
	}
	return n, n, nil
}

// internalError is a failure of the answer's own code: a subgraph that cannot be
// rebuilt without what is withheld, or a document the codec refuses. It does not
// unwrap, so a cause that wraps [store.ErrInvalid] is never told to the client as
// a refused argument: it is an internal error, logged through redactLog.
type internalError struct {
	what string
	err  error
}

func (e *internalError) Error() string { return e.what + ": " + e.err.Error() }

// subgraphAnswer withholds from sg what the request may not see and writes the
// answer, in this order: Without takes out every element that names an endpoint
// the request did not name, and builds the subgraph again; a backstop refuses
// the answer if such an endpoint is still in the dictionary, which only a
// section naming it can keep there; the names are read at the answer's token;
// and the answer over the size cap is refused whole. run's last check then scans
// the whole answer, the document's text included.
//
// No section of the four questions names an endpoint: the query packages read
// placements (scheduled_on, runs_on), mappings (part_of, instance_of) and calls
// (depends_on), and a has_address edge propagates nothing, so it is never a leg,
// a claim, a cut or a member. The backstop is the net under that.
//
// A failure of Without, such as a pod section that lists as quarantined an edge
// it took out, fails the whole answer as an internal error: the subgraph from
// before is never answered instead.
func (s *Source) subgraphAnswer(ctx context.Context, req *request, c subgraphCall, sg subgraph.Subgraph, cutNotes []string) (SubgraphAnswer, error) {
	var zero SubgraphAnswer
	if sg.Token != c.token {
		return zero, fmt.Errorf("the subgraph was read at the token %d, not the answer's %d", sg.Token, c.token)
	}
	if subgraphKnobs.alter != nil {
		subgraphKnobs.alter(&sg)
	}
	kept, dropped, err := subgraph.Without(sg, req.hides)
	if err != nil {
		return zero, &internalError{"withholding from the subgraph", err}
	}
	if n := hiddenEntities(kept, req); n > 0 {
		// The refusal of the last check, before anything is written: its sentence
		// carries no id, and its log line counts them.
		return zero, fmt.Errorf("the rebuilt subgraph's dictionary still names %d entities it dropped: %w", n, &egressError{n: n, typ: "SubgraphAnswer"})
	}
	doc, err := wire.JSON(wire.Document{Query: c.query, Policy: wire.CurrentPolicy(), Subgraph: kept})
	if err != nil {
		return zero, &internalError{"writing the document", err}
	}

	withheld := SubgraphWithheld{
		Entities: dropped.Entities, Edges: dropped.Edges, Derived: dropped.Derived,
		QuarantinedEntities: dropped.QuarantinedEntities, QuarantinedEdges: dropped.QuarantinedEdges,
	}
	out := SubgraphAnswer{
		Token: tokenString(c.token), Derived: true, Question: c.question, Window: windowOf(c.from, c.to),
		Policy: c.policy, Incomplete: incomplete(kept.Sections),
		Names: make([]Ref, len(kept.Entities)), Counts: SubgraphCounts{Withheld: withheld},
		Notes: slices.Clone(cutNotes), Document: doc,
	}
	out.Policy.Rank, out.Policy.Names, out.Policy.Withhold = s.rankDigest, NameTableVersion, WithholdVersion
	if note := subgraphWithheldNote(withheld); note != "" {
		out.Notes = append(out.Notes, note)
	}
	for i, e := range kept.Entities {
		out.Names[i] = ref(e.Fingerprint)
	}
	// One read of the names, on the list alone: finish would cut lists at the
	// tools' cap, and a walk of the whole answer would visit every byte of the
	// document for no ref.
	note, err := s.fillNames(ctx, &out.Names, c.token, c.from, c.to)
	if err != nil {
		return zero, err
	}
	if note != "" {
		out.Notes = append(out.Notes, note)
	}
	out.Notes = append(out.Notes, passAsOf(c.token))

	b, err := json.Marshal(&out)
	if err != nil {
		return zero, &internalError{"writing the answer", err}
	}
	if limit := subgraphCap(); len(b) > limit {
		return zero, &Error{
			Message: fmt.Sprintf("The answer is %d bytes, over the cap of %d bytes; it is never cut. Narrow the window or the scope.", len(b), limit),
			Cause:   ErrAnswerTooLarge,
		}
	}
	return out, nil
}

func subgraphCap() int {
	if subgraphKnobs.maxBytes > 0 {
		return subgraphKnobs.maxBytes
	}
	return MaxSubgraphAnswerBytes
}

// hiddenEntities counts the entities of the dictionary the request may not see.
func hiddenEntities(sg subgraph.Subgraph, req *request) int {
	n := 0
	for _, e := range sg.Entities {
		if req.hides(e.Fingerprint) {
			n++
		}
	}
	return n
}

// incomplete says a cap of the query cut what it read.
func incomplete(sec subgraph.Sections) bool {
	if !sec.Truncated.IsZero() {
		return true
	}
	return slices.ContainsFunc(sec.Pods, func(p subgraph.PodSection) bool { return !p.Truncated.IsZero() })
}

// subgraphWithheldNote says what was withheld, or "" if nothing.
func subgraphWithheldNote(w SubgraphWithheld) string {
	if w.total() == 0 {
		return ""
	}
	var parts []string
	for _, p := range []struct {
		n           int
		one, plural string
	}{
		{w.Entities, "entity", "entities"},
		{w.Edges, "edge", "edges"},
		{w.Derived, "derived element", "derived elements"},
		{w.QuarantinedEntities, "quarantined entity", "quarantined entities"},
		{w.QuarantinedEdges, "quarantined edge", "quarantined edges"},
	} {
		switch {
		case p.n == 1:
			parts = append(parts, "1 "+p.one)
		case p.n > 1:
			parts = append(parts, fmt.Sprintf("%d %s", p.n, p.plural))
		}
	}
	return "Withheld because each is or names an endpoint the request did not name (an endpoint's id reveals its address): " +
		strings.Join(parts, ", ") + ". Name the endpoint, by its id, to see it."
}

// coResidentCut is the cut of a co-residency result, as co_residents reports it.
func coResidentCut(t coresidency.Truncation) CoResidentCut {
	perVia := 0
	for _, v := range t.PerVia {
		perVia += v.Dropped
	}
	return CoResidentCut{
		NodesNotRead: len(t.Nodes), HostsNotRead: len(t.Hosts), PerViaDropped: perVia,
		ResultsDropped: t.Results, SuppressedDropped: t.SuppressedDropped,
	}
}

// contextCut is the cut of an extraction, as service_context reports it.
func contextCut(res extract.Result) ContextCut {
	podsCut := 0
	for _, fr := range res.PodResults {
		if podSearchCut(fr.Truncated) {
			podsCut++
		}
	}
	tr := res.Truncated
	return ContextCut{
		MembersDropped: len(tr.Members), InstancesDropped: len(tr.Instances), SeedPodsDropped: len(tr.SeedPods),
		PairsDropped: tr.Pairs, SuppressedDropped: tr.SuppressedDropped, PodSearchesCut: podsCut,
	}
}

// The query echoes. Each carries the question's kind, its seeds, every policy
// value the question used as an option, ascending by name, and the token the
// request named. The option names are the wire's. The requested token is not an
// option: a cache key is the echo with AsOf left out.

func scopeQuery(scope []identity.Fingerprint, pol subgraph.ScopePolicy, asOf uint64) wire.Query {
	seeds := slices.Clone(scope)
	slices.SortFunc(seeds, store.CompareFingerprints)
	seeds = slices.Compact(seeds)
	return echo(wire.QueryScope, seeds, map[string]int64{"max_scope": int64(pol.MaxScope)}, asOf)
}

func coResidentsQuery(seed identity.Fingerprint, nodeOnly bool, asOf uint64) wire.Query {
	return echo(wire.QueryCoResidents, []identity.Fingerprint{seed}, coOptions(coresidency.DefaultPolicy(), nodeOnly, map[string]int64{}), asOf)
}

func serviceContextQuery(seed identity.Fingerprint, nodeOnly bool, asOf uint64) wire.Query {
	p := extract.DefaultPolicy()
	opts := coOptions(p.CoResidency, nodeOnly, map[string]int64{})
	opts["anchored"] = 0
	opts["max_members"], opts["max_instances"] = int64(p.MaxMembers), int64(p.MaxInstances)
	opts["max_seed_pods"], opts["max_pairs"] = int64(p.MaxSeedPods), int64(p.MaxPairs)
	opts["ubiquity_num"], opts["ubiquity_den"], opts["ubiquity_min_nodes"] = int64(p.UbiquityNum), int64(p.UbiquityDen), int64(p.UbiquityMinNodes)
	return echo(wire.QueryExtraction, []identity.Fingerprint{seed}, opts, asOf)
}

func blastRadiusQuery(seed identity.Fingerprint, pol blastradius.Policy, asOf uint64) wire.Query {
	opts := map[string]int64{"depth": int64(pol.MaxDepth), "max_frontier": int64(pol.MaxFrontier), "max_members": int64(pol.MaxMembers)}
	return echo(wire.QueryBlastRadius, []identity.Fingerprint{seed}, opts, asOf)
}

func coOptions(p coresidency.Policy, nodeOnly bool, out map[string]int64) map[string]int64 {
	out["node_only"] = 0
	if nodeOnly {
		out["node_only"] = 1
	}
	out["max_nodes"], out["max_hosts"] = int64(p.MaxNodes), int64(p.MaxHosts)
	out["max_per_via"], out["max_results"] = int64(p.MaxPerVia), int64(p.MaxResults)
	out["min_overlap_ns"] = int64(p.MinOverlap)
	return out
}

func echo(kind wire.QueryKind, seeds []identity.Fingerprint, opts map[string]int64, asOf uint64) wire.Query {
	q := wire.Query{Kind: kind, Seeds: seeds, AsOf: asOf}
	for name, v := range opts {
		q.Options = append(q.Options, wire.Option{Name: name, Value: v})
	}
	slices.SortFunc(q.Options, func(a, b wire.Option) int { return cmp.Compare(a.Name, b.Name) })
	return q
}
