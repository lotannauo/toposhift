package mcp

import (
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Quarantined is an element the store quarantined: an entity, or an edge read
// with a quarantined endpoint. A quarantine is never an error and never dropped.
type Quarantined struct {
	Kind        string     `json:"kind"`
	Entity      *Ref       `json:"entity,omitempty"`
	Edge        *EdgeRef   `json:"edge,omitempty"`
	Subject     string     `json:"subject"`
	Basis       string     `json:"basis"`
	Rule        string     `json:"rule"`
	Quarantined Quarantine `json:"quarantined"`
	Note        string     `json:"note"`
}

// quarantinedOf describes an element of the interval package that carries a
// quarantine: an entity (Quarantine) or an edge (Uncascaded).
func quarantinedOf(el interval.Element) (Quarantined, bool) {
	out := Quarantined{Basis: BasisDerived, Rule: RuleExistence, Subject: subjectOfElement(el.Subject)}
	switch {
	case el.Quarantine != nil:
		r := ref(el.Subject.A)
		out.Kind, out.Entity = "entity", &r
		out.Quarantined = quarantineOf(el.Quarantine)
		out.Note = "The entity is quarantined: two boots of it were live at once, so its existence is not folded."
	case el.Uncascaded != nil:
		e := edgeRef(el.Subject)
		out.Kind, out.Edge = "edge", &e
		out.Quarantined = quarantineOf(el.Uncascaded)
		out.Note = "An endpoint of the edge is quarantined, so the edge is shown as its producers asserted it, without cutting it by its endpoints' existence."
	default:
		return Quarantined{}, false
	}
	return out, true
}

// subjectOfElement is "asserted" for an entity and for an edge of a stored
// relation, and "derived" for anything else.
func subjectOfElement(s store.Subject) string {
	if s.Kind == store.SubjectEntity {
		return SubjectAsserted
	}
	return subjectOfRelation(s.Relation)
}

func subjectOfRelation(r catalog.RelationType) string {
	if rel, ok := catalog.Default().Relation(r); ok && rel.Storage() == catalog.StorageFrom {
		return SubjectAsserted
	}
	return SubjectDerived
}

// pieceSpans writes the pieces of an element read in a window that ends at to.
func pieceSpans(ps []interval.Piece, to time.Time) []Span {
	out := make([]Span, len(ps))
	for i, p := range ps {
		reason, sentence := pieceEnd(p, to)
		out[i] = Span{
			Start: fmtTime(p.Start), End: fmtTime(p.End),
			EndReason: reason, EndSentence: sentence,
			BeganBeforeWindow: p.Before, AssertedBy: producerNames(p.Producers),
		}
	}
	return out
}

// pieceEnd is why a piece ends. A piece that reaches the window's end has no
// reason, because nothing after the window is reported, and so never carries an
// end source. A piece that ends inside the window has the source of its end.
func pieceEnd(p interval.Piece, to time.Time) (*string, string) {
	if !p.End.Before(to) {
		return nil, stillPresent
	}
	return sourceReason(p.EndSource)
}

// sourceReason is the end reason and sentence of an end source. A source that is
// not known inside a window is "unattributed", never "unknown".
func sourceReason(src lifecycle.EndSource) (*string, string) {
	switch src {
	case lifecycle.EndProducer:
		return strPtr("producer"), "deleted by its producer"
	case lifecycle.EndLivenessExpiry:
		return strPtr("liveness_expiry"), "its producer went silent past its TTL (not known to be gone)"
	case lifecycle.EndCascade:
		return strPtr("cascade"), "an endpoint ended"
	case lifecycle.EndRetention:
		return strPtr("retention"), "the retention horizon moved past it; history before the horizon is gone"
	case lifecycle.EndOperator:
		return strPtr("operator"), "purged by an operator"
	}
	return strPtr(endReasonNotKnown), "not known"
}

// derivedSpans writes the spans of a derived path or overlap. Such a span has
// no end source of its own: it ends where one of its legs does, and the legs say
// how. A span that reaches the window's end has no reason.
func derivedSpans(ss []interval.Span, to time.Time, what string) []Span {
	out := make([]Span, len(ss))
	for i, s := range ss {
		out[i] = derivedSpan(s, to, what)
	}
	return out
}

func derivedSpan(s interval.Span, to time.Time, what string) Span {
	return endedSpan(s, to, what+" ended; see the legs' ends")
}

// ambiguitySpans writes the spans over which a placement is unattributed. They
// list no legs: the span ends where the ambiguity does. The value is the same
// derived end, reach_ended.
func ambiguitySpans(ss []interval.Span, to time.Time) []Span {
	out := make([]Span, len(ss))
	for i, s := range ss {
		out[i] = endedSpan(s, to, "the ambiguity ended")
	}
	return out
}

func endedSpan(s interval.Span, to time.Time, sentence string) Span {
	out := Span{Start: fmtTime(s.Start), End: fmtTime(s.End)}
	if !s.End.Before(to) {
		out.EndSentence = stillPresent
		return out
	}
	out.EndReason = strPtr(endReasonReachEnded)
	out.EndSentence = sentence
	return out
}

func strPtr(s string) *string { return &s }

func producerNames(ps []lifecycle.Producer) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = string(p)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// piecesProducers lists once each, sorted, the producers of all the pieces.
func piecesProducers(ps []interval.Piece) []string {
	var all []lifecycle.Producer
	for _, p := range ps {
		all = append(all, p.Producers...)
	}
	return producerNames(all)
}
