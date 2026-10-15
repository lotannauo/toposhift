package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// JSON is the canonical JSON twin of a document: the same content, written for a
// reader and for a test in another language to compare a decode against. It is
// indented by two spaces, ends with a newline, and has one form for one
// document. A token and every int64 (the window, and every time in nanoseconds,
// absolute or after From) is a decimal string, as MCP writes a token, so no
// reader squeezes one into a float; counts, ids and indexes are numbers. An
// entity is its fingerprint, "type:hash". An element is named by its index, as
// keyframes, deltas and legs name it.
func JSON(d Document) ([]byte, error) {
	if err := validate(&d); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(twin(&d)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type jsonDoc struct {
	Format     jsonFormat     `json:"format"`
	Basis      string         `json:"basis"`
	Token      string         `json:"token"`
	From       string         `json:"from"`
	To         string         `json:"to"`
	Policy     jsonPolicy     `json:"policy"`
	Query      jsonQuery      `json:"query"`
	Entities   []jsonEntity   `json:"entities"`
	Elements   []jsonElement  `json:"elements"`
	Keyframes  []jsonKeyframe `json:"keyframes"`
	Deltas     []jsonDelta    `json:"deltas"`
	Quarantine jsonQuarantine `json:"quarantine"`
	Sections   jsonSections   `json:"sections"`
}

type jsonFormat struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}

type jsonPolicy struct {
	Subgraph         int    `json:"subgraph"`
	Interval         int    `json:"interval"`
	CoResidency      int    `json:"coresidency"`
	Extract          int    `json:"extract"`
	BlastRadius      int    `json:"blastradius"`
	GraphDiff        int    `json:"graphdiff"`
	CatalogIDs       string `json:"catalog_ids"`
	IdentityEncoding int    `json:"identity_encoding"`
	Rank             string `json:"rank,omitempty"`
}

type jsonOption struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type jsonQuery struct {
	Kind    string       `json:"kind"`
	Seeds   []string     `json:"seeds"`
	Options []jsonOption `json:"options"`
	AsOf    string       `json:"asof"`
}

type jsonEntity struct {
	Fingerprint string `json:"fingerprint"`
	Boundary    bool   `json:"boundary"`
}

type jsonKey struct {
	Kind     string  `json:"kind"`
	A        string  `json:"a"`
	B        string  `json:"b,omitempty"`
	Relation string  `json:"relation,omitempty"`
	Rule     string  `json:"rule,omitempty"`
	Detail   *string `json:"detail,omitempty"`
}

type jsonLegEnd struct {
	Path   int    `json:"path"`
	Leg    int    `json:"leg"`
	Reason string `json:"reason"`
}

type jsonPiece struct {
	Start     string       `json:"start"`
	End       string       `json:"end"`
	Before    bool         `json:"before"`
	EndReason string       `json:"end_reason"`
	Producers []string     `json:"producers"`
	Ends      []jsonLegEnd `json:"ends"`
}

type jsonElement struct {
	Key         jsonKey     `json:"key"`
	Subject     string      `json:"subject"`
	Rule        string      `json:"rule"`
	Class       string      `json:"class"`
	Transitions int         `json:"transitions"`
	Paths       [][]uint32  `json:"paths"`
	Pieces      []jsonPiece `json:"pieces"`
}

type jsonKeyframe struct {
	At       string   `json:"at"`
	Elements []uint32 `json:"elements"`
}

type jsonDelta struct {
	At      string `json:"at"`
	Element uint32 `json:"element"`
	Op      string `json:"op"`
	Before  string `json:"before"`
	After   string `json:"after"`
}

type jsonCollision struct {
	StaleBoot      string `json:"stale_boot"`
	ObservedAt     string `json:"observed_at"`
	NewerBoot      string `json:"newer_boot"`
	NewerFirstSeen string `json:"newer_first_seen"`
}

type jsonCause struct {
	Entity    string         `json:"entity"`
	Layer     string         `json:"layer"`
	Collision *jsonCollision `json:"collision"`
}

type jsonQuarantinedEntity struct {
	Fingerprint string    `json:"fingerprint"`
	Cause       jsonCause `json:"cause"`
}

type jsonStaticEdge struct {
	jsonElement
	Cause jsonCause `json:"cause"`
}

type jsonQuarantine struct {
	Entities []jsonQuarantinedEntity `json:"entities"`
	Edges    []jsonStaticEdge        `json:"edges"`
}

type jsonFrontier struct {
	Depth   int      `json:"depth"`
	Dropped []string `json:"dropped"`
}

type jsonViaCount struct {
	Via       string `json:"via"`
	Mechanism string `json:"mechanism"`
	Dropped   int    `json:"dropped"`
}

type jsonTruncated struct {
	Nodes             []string       `json:"nodes"`
	Hosts             []string       `json:"hosts"`
	Members           []string       `json:"members"`
	Instances         []string       `json:"instances"`
	SeedPods          []string       `json:"seed_pods"`
	Depth             []string       `json:"depth"`
	Frontier          []jsonFrontier `json:"frontier"`
	PerVia            []jsonViaCount `json:"per_via"`
	Results           int            `json:"results"`
	Pairs             int            `json:"pairs"`
	MembersDropped    int            `json:"members_dropped"`
	SuppressedDropped int            `json:"suppressed_dropped"`
}

type jsonSpan struct {
	Start     string `json:"start"`
	End       string `json:"end"`
	Before    bool   `json:"before"`
	EndReason string `json:"end_reason"`
}

type jsonUnattributed struct {
	SeedPod    string     `json:"seed_pod,omitempty"`
	Subject    string     `json:"subject"`
	Reason     string     `json:"reason"`
	Relation   string     `json:"relation,omitempty"`
	Target     string     `json:"target,omitempty"`
	Spans      []jsonSpan `json:"spans"`
	Candidates []string   `json:"candidates"`
}

type jsonUnmapped struct {
	Pod   string     `json:"pod"`
	Spans []jsonSpan `json:"spans"`
}

type jsonSuppressed struct {
	Key         jsonKey    `json:"key"`
	Reason      string     `json:"reason"`
	Coverage    *[2]int    `json:"coverage,omitempty"`
	Class       string     `json:"class"`
	Transitions int        `json:"transitions"`
	Spans       []jsonSpan `json:"spans"`
}

type jsonPod struct {
	Seed        string           `json:"seed"`
	Truncated   jsonTruncated    `json:"truncated"`
	Quarantined []jsonKey        `json:"quarantined"`
	Suppressed  []jsonSuppressed `json:"suppressed"`
}

type jsonUbiquity struct {
	Judged        bool `json:"judged"`
	FrontierNodes int  `json:"frontier_nodes"`
}

type jsonSections struct {
	Truncated    jsonTruncated      `json:"truncated"`
	Unattributed []jsonUnattributed `json:"unattributed"`
	Unmapped     []jsonUnmapped     `json:"unmapped"`
	Suppressed   []jsonSuppressed   `json:"suppressed"`
	Pods         []jsonPod          `json:"pods"`
	Anchor       string             `json:"anchor"`
	Ubiquity     jsonUbiquity       `json:"ubiquity"`
}

func u64s(x uint64) string { return strconv.FormatUint(x, 10) }
func i64s(x int64) string  { return strconv.FormatInt(x, 10) }

func fpStrings(fps []identity.Fingerprint) []string {
	out := make([]string, len(fps))
	for i, fp := range fps {
		out[i] = fp.String()
	}
	return out
}

func twinKey(k subgraph.Key) jsonKey {
	out := jsonKey{Kind: k.Kind.String(), A: k.A.String()}
	switch k.Kind {
	case subgraph.KindEdge:
		out.B, out.Relation = k.B.String(), string(k.Relation)
	case subgraph.KindDerived:
		detail := k.Detail
		out.B, out.Rule, out.Detail = k.B.String(), k.Rule, &detail
	}
	return out
}

func twinElement(el subgraph.Element) jsonElement {
	out := jsonElement{
		Key: twinKey(el.Key), Subject: el.Subject.String(), Rule: el.Rule, Class: el.Class.String(), Transitions: el.Transitions,
		Paths: make([][]uint32, len(el.Paths)), Pieces: make([]jsonPiece, len(el.Pieces)),
	}
	for i, p := range el.Paths {
		out.Paths[i] = append([]uint32{}, p...)
	}
	for i, p := range el.Pieces {
		jp := jsonPiece{
			Start: u64s(p.Start), End: u64s(p.End), Before: p.Before, EndReason: p.EndReason.String(),
			Producers: make([]string, len(p.Producers)), Ends: make([]jsonLegEnd, len(p.Ends)),
		}
		for j, pr := range p.Producers {
			jp.Producers[j] = string(pr)
		}
		for j, e := range p.Ends {
			jp.Ends[j] = jsonLegEnd{Path: e.Path, Leg: e.Leg, Reason: e.Reason.String()}
		}
		out.Pieces[i] = jp
	}
	return out
}

func twinState(s subgraph.State) string {
	if s.Present {
		return "present"
	}
	return "absent:" + s.Reason.String()
}

func twinCause(c *store.QuarantineError) jsonCause {
	out := jsonCause{Entity: c.Entity.String(), Layer: c.Layer.String()}
	if cc := c.Collision; cc != nil {
		out.Collision = &jsonCollision{
			StaleBoot: cc.StaleBoot, ObservedAt: i64s(cc.ObservedAt.UnixNano()),
			NewerBoot: cc.NewerBoot, NewerFirstSeen: i64s(cc.NewerFirstSeen.UnixNano()),
		}
	}
	return out
}

func twinSpans(ss []subgraph.Span) []jsonSpan {
	out := make([]jsonSpan, len(ss))
	for i, s := range ss {
		out[i] = jsonSpan{Start: u64s(s.Start), End: u64s(s.End), Before: s.Before, EndReason: s.EndReason.String()}
	}
	return out
}

func twinTruncated(t subgraph.Truncation) jsonTruncated {
	out := jsonTruncated{
		Nodes: fpStrings(t.Nodes), Hosts: fpStrings(t.Hosts), Members: fpStrings(t.Members),
		Instances: fpStrings(t.Instances), SeedPods: fpStrings(t.SeedPods), Depth: fpStrings(t.Depth),
		Frontier: make([]jsonFrontier, len(t.Frontier)), PerVia: make([]jsonViaCount, len(t.PerVia)),
		Results: t.Results, Pairs: t.Pairs, MembersDropped: t.MembersDropped, SuppressedDropped: t.SuppressedDropped,
	}
	for i, f := range t.Frontier {
		out.Frontier[i] = jsonFrontier{Depth: f.Depth, Dropped: fpStrings(f.Dropped)}
	}
	for i, v := range t.PerVia {
		out.PerVia[i] = jsonViaCount{Via: v.Via.String(), Mechanism: v.Mechanism.String(), Dropped: v.Dropped}
	}
	return out
}

func twinSuppressed(ss []subgraph.Suppressed) []jsonSuppressed {
	out := make([]jsonSuppressed, len(ss))
	for i, x := range ss {
		out[i] = jsonSuppressed{
			Key: twinKey(x.Key), Reason: x.Reason.String(), Class: x.Class.String(), Transitions: x.Transitions,
			Spans: twinSpans(x.Spans),
		}
		if x.Reason == subgraph.SuppressedUbiquitous {
			cov := x.Coverage
			out[i].Coverage = &cov
		}
	}
	return out
}

func twin(d *Document) jsonDoc {
	sg := &d.Subgraph
	p := d.Policy
	out := jsonDoc{
		Format: jsonFormat{FormatMajor, FormatMinor},
		Basis:  sg.Basis.String(), Token: u64s(sg.Token), From: i64s(sg.From.UnixNano()), To: i64s(sg.To.UnixNano()),
		Policy: jsonPolicy{
			Subgraph: sg.PolicyVersion, Interval: p.Interval, CoResidency: p.CoResidency, Extract: p.Extract,
			BlastRadius: p.BlastRadius, GraphDiff: p.GraphDiff, CatalogIDs: hex.EncodeToString(p.CatalogIDs[:]),
			IdentityEncoding: p.IdentityEncoding, Rank: hex.EncodeToString(p.Rank),
		},
		Query:     jsonQuery{Kind: d.Query.Kind.String(), Seeds: fpStrings(d.Query.Seeds), Options: make([]jsonOption, len(d.Query.Options))},
		Entities:  make([]jsonEntity, len(sg.Entities)),
		Elements:  make([]jsonElement, len(sg.Elements)),
		Keyframes: make([]jsonKeyframe, len(sg.Keyframes)),
		Deltas:    make([]jsonDelta, len(sg.Deltas)),
		Quarantine: jsonQuarantine{
			Entities: make([]jsonQuarantinedEntity, len(sg.Quarantine.Entities)),
			Edges:    make([]jsonStaticEdge, len(sg.Quarantine.Edges)),
		},
	}
	out.Query.AsOf = u64s(d.Query.AsOf)
	if d.Query.AsOf == store.Latest {
		out.Query.AsOf = "latest"
	}
	for i, o := range d.Query.Options {
		out.Query.Options[i] = jsonOption{Name: o.Name, Value: i64s(o.Value)}
	}
	for i, e := range sg.Entities {
		out.Entities[i] = jsonEntity{Fingerprint: e.Fingerprint.String(), Boundary: e.Boundary}
	}
	for i, el := range sg.Elements {
		out.Elements[i] = twinElement(el)
	}
	for i, k := range sg.Keyframes {
		out.Keyframes[i] = jsonKeyframe{At: u64s(k.At), Elements: append([]uint32{}, k.Elements...)}
	}
	for i, dl := range sg.Deltas {
		op := "disappear"
		if dl.Appear() {
			op = "appear"
		}
		out.Deltas[i] = jsonDelta{At: u64s(dl.At), Element: dl.Element, Op: op, Before: twinState(dl.Before), After: twinState(dl.After)}
	}
	for i, q := range sg.Quarantine.Entities {
		out.Quarantine.Entities[i] = jsonQuarantinedEntity{Fingerprint: q.Fingerprint.String(), Cause: twinCause(q.Cause)}
	}
	for i, e := range sg.Quarantine.Edges {
		out.Quarantine.Edges[i] = jsonStaticEdge{jsonElement: twinElement(e.Element), Cause: twinCause(e.Cause)}
	}
	s := sg.Sections
	out.Sections = jsonSections{
		Truncated: twinTruncated(s.Truncated), Unattributed: make([]jsonUnattributed, len(s.Unattributed)),
		Unmapped: make([]jsonUnmapped, len(s.Unmapped)), Suppressed: twinSuppressed(s.Suppressed),
		Pods: make([]jsonPod, len(s.Pods)), Anchor: s.Anchor.String(),
		Ubiquity: jsonUbiquity{Judged: s.Ubiquity.Judged, FrontierNodes: s.Ubiquity.FrontierNodes},
	}
	for i, u := range s.Unattributed {
		ju := jsonUnattributed{
			Subject: u.Subject.String(), Reason: u.Reason.String(), Relation: string(u.Relation), Target: string(u.Target),
			Spans: twinSpans(u.Spans), Candidates: fpStrings(u.Candidates),
		}
		if !u.SeedPod.IsZero() {
			ju.SeedPod = u.SeedPod.String()
		}
		out.Sections.Unattributed[i] = ju
	}
	for i, u := range s.Unmapped {
		out.Sections.Unmapped[i] = jsonUnmapped{Pod: u.Pod.String(), Spans: twinSpans(u.Spans)}
	}
	for i, p := range s.Pods {
		jp := jsonPod{
			Seed: p.Seed.String(), Truncated: twinTruncated(p.Truncated),
			Quarantined: make([]jsonKey, len(p.Quarantined)), Suppressed: twinSuppressed(p.Suppressed),
		}
		for j, k := range p.Quarantined {
			jp.Quarantined[j] = twinKey(k)
		}
		out.Sections.Pods[i] = jp
	}
	return out
}
