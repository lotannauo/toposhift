package interval

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy the classes and the
// transition counts follow. A cache of derived results keys on it, and a change
// to the class table or to what a transition is raises it.
const PolicyVersion = 1

// Window is a half-open range [From, To) of event time, read at one snapshot
// token. From must be before To.
type Window struct {
	From, To time.Time
	// AsOf is [store.Latest] or a snapshot token. It is resolved once per answer:
	// Latest, and a token above [store.Store.LastSeq], become LastSeq.
	AsOf uint64
}

// Span is a half-open interval [Start, End) of event time, with Start before
// End.
type Span struct{ Start, End time.Time }

// Contains reports whether t is in the span: Start <= t < End.
func (s Span) Contains(t time.Time) bool { return !t.Before(s.Start) && t.Before(s.End) }

// Piece is one interval of an element inside the window. It is derived.
type Piece struct {
	Span
	// Before says the existence began before the window's From (and then Start
	// is From). With the cascade it is the existence of the edge and both its
	// endpoints at once.
	Before bool
	// EndSource is how the piece ends: lifecycle.EndUnknown exactly when End is
	// the window's To, because nothing after To is reported; otherwise
	// lifecycle.EndProducer, lifecycle.EndLivenessExpiry or lifecycle.EndCascade.
	EndSource lifecycle.EndSource
	// Producers lists, sorted and once each, the producers whose reference was
	// live at some instant of the piece.
	Producers []lifecycle.Producer
}

// Class is the lifecycle class of an element in a window, a total function of
// its pieces. The zero value is ClassNone.
type Class uint8

const (
	// ClassNone is an element with no piece in the window.
	ClassNone Class = iota
	// ClassStable is one piece covering the whole window.
	ClassStable
	// ClassAppeared is one piece that starts after From and reaches To.
	ClassAppeared
	// ClassDisappeared is one piece that starts at From and ends before To.
	ClassDisappeared
	// ClassTransient is one piece that starts after From and ends before To.
	ClassTransient
	// ClassFlapping is two pieces or more.
	ClassFlapping
)

// String names the class: "none", "stable", "appeared", "disappeared",
// "transient" or "flapping", and "Class(n)" for any other value.
func (c Class) String() string {
	switch c {
	case ClassNone:
		return "none"
	case ClassStable:
		return "stable"
	case ClassAppeared:
		return "appeared"
	case ClassDisappeared:
		return "disappeared"
	case ClassTransient:
		return "transient"
	case ClassFlapping:
		return "flapping"
	}
	return fmt.Sprintf("Class(%d)", uint8(c))
}

// Element is one subject of a range answer: an entity, or one edge
// (from, to, relation). It is derived.
type Element struct {
	Subject store.Subject
	// Layer is where the subject is stored: the entity type's catalog layer, or
	// the relation's.
	Layer catalog.Layer
	// Pieces are ascending, disjoint and never touching.
	Pieces []Piece
	// Class and Transitions follow the class table. Transitions counts the piece
	// boundaries strictly inside (From, To).
	Class       Class
	Transitions int
	// Quarantine is set for an entity the store has quarantined: Pieces is then
	// nil and Class is ClassNone.
	Quarantine *store.QuarantineError
	// Uncascaded is set for an edge one of whose endpoints is quarantined: the
	// pieces are then the edge's own, not cut by its endpoints. When both are,
	// it is the source's.
	Uncascaded *store.QuarantineError
}

// Result is a range answer: the token every read of it used, the window, and
// the elements. It is derived, and nothing in this package writes it to a
// store.
type Result struct {
	Token    uint64
	From, To time.Time
	Elements []Element
}

// Union returns the elements with at least one piece, and every quarantined
// entity, which is shown rather than dropped.
func Union(els []Element) []Element {
	var out []Element
	for _, el := range els {
		if len(el.Pieces) > 0 || el.Quarantine != nil {
			out = append(out, el)
		}
	}
	return out
}

// Intersection returns the elements with one piece covering the whole window
// (ClassStable), and every quarantined entity.
func Intersection(els []Element) []Element {
	var out []Element
	for _, el := range els {
		if el.Class == ClassStable || el.Quarantine != nil {
			out = append(out, el)
		}
	}
	return out
}

// At returns the elements with a piece containing t, and every quarantined
// entity. It trusts its caller to pass a t inside the window the elements were
// read in; [AtChecked] refuses one outside it.
func At(els []Element, t time.Time) []Element {
	var out []Element
	for _, el := range els {
		if el.Quarantine != nil || containsAt(el.Pieces, t) {
			out = append(out, el)
		}
	}
	return out
}

// AtChecked is [At] over a result, refusing a t outside [From, To) with an
// error wrapping [store.ErrInvalid].
func AtChecked(res Result, t time.Time) ([]Element, error) {
	if t.Before(res.From) || !t.Before(res.To) {
		return nil, fmt.Errorf("interval: At: instant %s is outside the window [%s, %s): %w",
			formatTime(t), formatTime(res.From), formatTime(res.To), store.ErrInvalid)
	}
	return At(res.Elements, t), nil
}

func containsAt(ps []Piece, t time.Time) bool {
	for _, p := range ps {
		if p.Contains(t) {
			return true
		}
	}
	return false
}

// Spans returns the spans of an element's pieces.
func Spans(el Element) []Span {
	if len(el.Pieces) == 0 {
		return nil
	}
	out := make([]Span, len(el.Pieces))
	for i, p := range el.Pieces {
		out[i] = p.Span
	}
	return out
}

// Intersect returns the instants in both a and b, each a sorted list of
// disjoint, non-touching spans, as such a list. It is a linear merge-join:
// spans that only touch do not overlap, and nothing is sorted.
func Intersect(a, b []Span) []Span {
	var out []Span
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		start, end := a[i].Start, a[i].End
		if b[j].Start.After(start) {
			start = b[j].Start
		}
		if b[j].End.Before(end) {
			end = b[j].End
		}
		if start.Before(end) {
			out = append(out, Span{Start: start, End: end})
		}
		switch c := a[i].End.Compare(b[j].End); {
		case c < 0:
			i++
		case c > 0:
			j++
		default:
			i++
			j++
		}
	}
	return out
}

// Along is the existence of a path: the instants at which every leg exists,
// Intersect folded over the legs. One leg gives a copy of it, and no legs give
// nil.
func Along(legs ...[]Span) []Span {
	if len(legs) == 0 {
		return nil
	}
	out := append([]Span(nil), legs[0]...)
	for _, l := range legs[1:] {
		out = Intersect(out, l)
	}
	return out
}

// Duration is the total length of the spans.
func Duration(ss []Span) time.Duration {
	var d time.Duration
	for _, s := range ss {
		d += s.End.Sub(s.Start)
	}
	return d
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
