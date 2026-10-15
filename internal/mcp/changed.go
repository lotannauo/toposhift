package mcp

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// maxScope is the most entities the what_changed tool takes in a scope.
const maxScope = 256

// WhatChangedInput is the input of the what_changed tool.
type WhatChangedInput struct {
	// Scope is the ids of the entities to diff, at most 256. Every edge with an
	// end in the scope is diffed too.
	Scope []string `json:"scope"`
	// From and To are the two instants, RFC 3339 with a zone. The window read is
	// [from, to+1ns), so both instants are inside it.
	From string `json:"from"`
	To   string `json:"to"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
	// Limit is how many entries of each list to show, 1 to 500; the default is 50.
	Limit int `json:"limit,omitempty"`
}

// WhatChangedOutput is the answer of the what_changed tool. Created, deleted and
// changed are the net buckets and are exclusive; transient lists every element
// that came and went inside the window, or flapped, whatever its net bucket.
type WhatChangedOutput struct {
	Token       string           `json:"token"`
	Derived     bool             `json:"derived"`
	Policy      Policy           `json:"policy"`
	Scope       []Ref            `json:"scope"`
	Window      Window           `json:"window"`
	Created     []ChangeEntry    `json:"created"`
	Deleted     []ChangeEntry    `json:"deleted"`
	Changed     []ChangeEntry    `json:"changed"`
	Transient   []ChangeEntry    `json:"transient"`
	Events      []ChangeEvent    `json:"events"`
	Quarantined []Quarantined    `json:"quarantined"`
	Counts      ChangeCounts     `json:"counts"`
	Truncation  ChangeTruncation `json:"truncation"`
	Notes       []string         `json:"notes"`
}

// ChangeEntry is an entity or a relation in one bucket of a diff.
type ChangeEntry struct {
	// Kind is "entity" or "relation".
	Kind        string   `json:"kind"`
	Entity      *Ref     `json:"entity,omitempty"`
	Edge        *EdgeRef `json:"edge,omitempty"`
	Subject     string   `json:"subject"`
	Basis       string   `json:"basis"`
	Rule        string   `json:"rule"`
	Spans       []Span   `json:"spans"`
	Class       string   `json:"class"`
	Transitions int      `json:"transitions"`
	// DeletedAt is the end of the last span of a deleted element.
	DeletedAt string  `json:"deleted_at,omitempty"`
	Change    *Change `json:"change,omitempty"`
}

// Change says how an entity's description differs between the two instants,
// by key. The values are not printed. A key the payload table withholds is
// named in withheld_keys and in neither of the other two lists.
type Change struct {
	// Kind is "keys", "opaque" (the descriptions are not JSON objects) or
	// "source_changed" (another producer's description wins at the second
	// instant; two producers' descriptions are not compared).
	Kind          string   `json:"kind"`
	StateKeys     []string `json:"state_keys"`
	AttributeKeys []string `json:"attribute_keys"`
	WithheldKeys  []string `json:"withheld_keys"`
	FromProducer  string   `json:"from_producer"`
	ToProducer    string   `json:"to_producer"`
}

// ChangeEvent is one appearance or disappearance strictly inside the window.
type ChangeEvent struct {
	At      string   `json:"at"`
	Event   string   `json:"event"`
	Kind    string   `json:"kind"`
	Entity  *Ref     `json:"entity,omitempty"`
	Edge    *EdgeRef `json:"edge,omitempty"`
	Subject string   `json:"subject"`
	Basis   string   `json:"basis"`
	Rule    string   `json:"rule"`
	// EndReason is how a disappearance came about; an appearance has none.
	EndReason   *string `json:"end_reason,omitempty"`
	EndSentence string  `json:"sentence"`
}

// Pair is a number of entities and a number of relations, counted apart.
type Pair struct {
	Entities  int `json:"entities"`
	Relations int `json:"relations"`
}

// ChangeCounts are the exact sizes of the buckets, whatever the answer shows.
type ChangeCounts struct {
	Created     Pair `json:"created"`
	Deleted     Pair `json:"deleted"`
	Changed     Pair `json:"changed"`
	Transient   Pair `json:"transient"`
	Quarantined Pair `json:"quarantined"`
	Events      int  `json:"events"`
}

// ChangeTruncation counts, per list, the entries the diff's own cap left out. The
// counts of the buckets are exact whatever it left out.
type ChangeTruncation struct {
	Incomplete  bool `json:"incomplete"`
	Created     int  `json:"created"`
	Deleted     int  `json:"deleted"`
	Changed     int  `json:"changed"`
	Transient   int  `json:"transient"`
	Quarantined int  `json:"quarantined"`
}

// WhatChanged diffs a scope between two instants.
func (s *Source) WhatChanged(ctx context.Context, in WhatChangedInput) (WhatChangedOutput, error) {
	return run(s, ctx, true, func(ctx context.Context) (WhatChangedOutput, error) {
		var zero WhatChangedOutput
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
		t1, t2, err := rangeArgs(in.From, in.To)
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

		res, err := graphdiff.NewDiffer(s.st).Diff(ctx, scope, t1, t2, token, graphdiff.Options{Rank: s.rank})
		if err != nil {
			return zero, err
		}
		wTo := t2.Add(time.Nanosecond)

		out := WhatChangedOutput{
			Token: tokenString(token), Derived: true,
			Policy: Policy{
				Interval: interval.PolicyVersion, GraphDiff: graphdiff.PolicyVersion,
				Rank: s.rankDigest, Names: NameTableVersion, Payload: PayloadTableVersion,
			},
			Scope: refs(res.Scope), Window: windowOf(t1, wTo),
			Created: []ChangeEntry{}, Deleted: []ChangeEntry{}, Changed: []ChangeEntry{}, Transient: []ChangeEntry{},
			Events: []ChangeEvent{}, Quarantined: []Quarantined{},
			Notes: append(notesFor(ahead), "The window read is [from, to+1ns): both instants are inside it."),
		}
		buckets := []struct {
			name string
			from []graphdiff.Entry
			to   *[]ChangeEntry
			n    Pair
			cut  int
		}{
			{"created", res.Created, &out.Created, pairOf(res.Counts.Created), res.Truncated.Created},
			{"deleted", res.Deleted, &out.Deleted, pairOf(res.Counts.Deleted), res.Truncated.Deleted},
			{"changed", res.Changed, &out.Changed, pairOf(res.Counts.Changed), res.Truncated.Changed},
			{"transient", res.Transient, &out.Transient, pairOf(res.Counts.Transient), res.Truncated.Transient},
		}
		for _, b := range buckets {
			for _, e := range head(b.from, limit) {
				*b.to = append(*b.to, changeEntry(b.name, e, wTo))
			}
			if total := b.n.Entities + b.n.Relations; len(*b.to) < total {
				out.Notes = append(out.Notes, shown(len(*b.to), total, b.name+" entries", "entities first"))
			}
		}
		events := graphdiff.Events(res)
		for _, ev := range head(events, limit) {
			out.Events = append(out.Events, changeEvent(ev))
		}
		if len(out.Events) < len(events) {
			out.Notes = append(out.Notes, shown(len(out.Events), len(events), "events", "oldest first"))
		}
		for _, el := range head(res.Quarantined, maxQuarantined) {
			if q, ok := quarantinedOf(el); ok {
				out.Quarantined = append(out.Quarantined, q)
			}
		}
		if n := res.Counts.Quarantined.Entities + res.Counts.Quarantined.Relations; len(out.Quarantined) < n {
			out.Notes = append(out.Notes, shown(len(out.Quarantined), n, "quarantined elements", "in the package's order"))
		}

		out.Counts = ChangeCounts{
			Created: pairOf(res.Counts.Created), Deleted: pairOf(res.Counts.Deleted),
			Changed: pairOf(res.Counts.Changed), Transient: pairOf(res.Counts.Transient),
			Quarantined: pairOf(res.Counts.Quarantined), Events: len(events),
		}
		tr := res.Truncated
		out.Truncation = ChangeTruncation{
			Created: tr.Created, Deleted: tr.Deleted, Changed: tr.Changed, Transient: tr.Transient, Quarantined: tr.Quarantined,
			Incomplete: tr.Created+tr.Deleted+tr.Changed+tr.Transient+tr.Quarantined > 0,
		}
		out.Notes = append(out.Notes, diffNotes(tr)...)

		err = s.finish(ctx, &out, &out.Notes, token, t1, wTo,
			namedTail{"transient entries", listTail(&out.Transient)},
			namedTail{"events", listTail(&out.Events)},
			namedTail{"changed entries", listTail(&out.Changed)},
			namedTail{"deleted entries", listTail(&out.Deleted)},
			namedTail{"created entries", listTail(&out.Created)},
			namedTail{"quarantined elements", listTail(&out.Quarantined)})
		if err != nil {
			return zero, err
		}
		return out, nil
	})
}

func changeEntry(bucket string, e graphdiff.Entry, to time.Time) ChangeEntry {
	el := e.Element
	out := ChangeEntry{
		Subject: subjectOfElement(el.Subject), Basis: BasisDerived, Rule: RuleDiff + bucket,
		Spans: pieceSpans(el.Pieces, to),
		Class: el.Class.String(), Transitions: el.Transitions,
	}
	out.Kind, out.Entity, out.Edge = subjectRefs(el.Subject)
	if !e.DeletedAt.IsZero() {
		out.DeletedAt = fmtTime(e.DeletedAt)
	}
	if ch := e.Change; ch != nil {
		out.Change = changeOf(el.Subject.A.Type(), ch)
	}
	return out
}

// subjectRefs is the kind of a subject and the one of its two forms that it has.
func subjectRefs(s store.Subject) (kind string, entity *Ref, edge *EdgeRef) {
	if s.Kind == store.SubjectEntity {
		r := ref(s.A)
		return "entity", &r, nil
	}
	e := edgeRef(s)
	return "relation", nil, &e
}

// changeOf prints a change of description by key, through the payload table: the
// keys it withholds are named apart.
func changeOf(typ catalog.EntityType, ch *graphdiff.DescriptionChange) *Change {
	out := &Change{
		Kind: ch.Kind.String(), StateKeys: []string{}, AttributeKeys: []string{}, WithheldKeys: []string{},
		FromProducer: string(ch.FromProducer), ToProducer: string(ch.ToProducer),
	}
	split := func(keys []string, to *[]string) {
		for _, k := range keys {
			if payloadClass(typ, k) == keyAllowed {
				*to = append(*to, k)
			} else {
				out.WithheldKeys = append(out.WithheldKeys, k)
			}
		}
	}
	split(ch.StateKeys, &out.StateKeys)
	split(ch.AttributeKeys, &out.AttributeKeys)
	slices.Sort(out.WithheldKeys)
	return out
}

func changeEvent(ev graphdiff.Event) ChangeEvent {
	out := ChangeEvent{
		At: fmtTime(ev.At), Subject: subjectOfElement(ev.Subject), Basis: BasisDerived, Rule: RuleDiff + "event",
	}
	out.Kind, out.Entity, out.Edge = subjectRefs(ev.Subject)
	if ev.Appear {
		out.Event, out.EndSentence = "appeared", "appeared"
		return out
	}
	out.Event = "disappeared"
	out.EndReason, out.EndSentence = sourceReason(ev.Source)
	return out
}

func pairOf(c graphdiff.Count) Pair { return Pair{Entities: c.Entities, Relations: c.Relations} }

// diffNotes writes a sentence for each list the diff's own cap cut.
func diffNotes(t graphdiff.Truncation) []string {
	cap := graphdiff.DefaultPolicy().MaxListed
	var out []string
	for _, l := range []struct {
		what string
		n    int
	}{{"created", t.Created}, {"deleted", t.Deleted}, {"changed", t.Changed}, {"transient", t.Transient}, {"quarantined", t.Quarantined}} {
		if l.n > 0 {
			out = append(out, fmt.Sprintf("The diff lists at most %d %s entries; %d more are counted and not listed, and the event log lacks their boundaries.", cap, l.what, l.n))
		}
	}
	return out
}
