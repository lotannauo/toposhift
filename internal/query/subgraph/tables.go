package subgraph

import (
	"fmt"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The enums of this package are the wire's own. Each is a frozen table with
// explicit values, and a value of another package's enum reaches one only
// through a function that names every case and refuses the rest: a Go enum is
// an iota that another change can renumber, and a wire value never moves. A
// table's golden test lists every value, so a change to one fails a test
// rather than the bytes of a stored result. A value is appended to a table
// under a new policy version; none is renumbered or reused.

// Basis says what a subgraph is made of. The header carries it once, never per
// element.
type Basis uint8

const (
	// BasisFact is a stored record exactly as written. A subgraph is never a
	// fact; the value is part of the vocabulary, and nothing in this package
	// produces it.
	BasisFact Basis = 1
	// BasisDerived is everything computed from stored records, which names the
	// rule it follows. A subgraph is always this.
	BasisDerived Basis = 2
)

// String names the basis: "fact" or "derived", and "Basis(n)" for any other
// value.
func (b Basis) String() string {
	switch b {
	case BasisFact:
		return "fact"
	case BasisDerived:
		return "derived"
	}
	return fmt.Sprintf("Basis(%d)", uint8(b))
}

// Kind says what a key names: an entity, an edge, or a derived subject. The
// order of the values is the order of the subgraph: entities, then edges, then
// derived subjects.
type Kind uint8

const (
	// KindEntity is the existence of one entity.
	KindEntity Kind = 1
	// KindEdge is one directed relation between two entities.
	KindEdge Kind = 2
	// KindDerived is a subject computed from other facts, such as a path.
	KindDerived Kind = 3
)

// String names the kind: "entity", "edge" or "derived", and "Kind(n)" for any
// other value.
func (k Kind) String() string {
	switch k {
	case KindEntity:
		return "entity"
	case KindEdge:
		return "edge"
	case KindDerived:
		return "derived"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Subject says whose assertion an element is. It is not the basis: every
// element is derived, since existence is a function of the facts, a token and
// a policy; what differs is whether a producer asserted the subject itself.
type Subject uint8

const (
	// SubjectAsserted is an entity, or an edge whose relation a producer
	// asserts (catalog.StorageFrom).
	SubjectAsserted Subject = 1
	// SubjectDerived is a subject no producer asserted: a derived relation, or
	// a derived kind.
	SubjectDerived Subject = 2
)

// String names the subject: "asserted" or "derived", and "Subject(n)" for any
// other value.
func (s Subject) String() string {
	switch s {
	case SubjectAsserted:
		return "asserted"
	case SubjectDerived:
		return "derived"
	}
	return fmt.Sprintf("Subject(%d)", uint8(s))
}

// RuleExistence is the rule of an asserted subject: its existence, folded from
// the records that assert it.
const RuleExistence = "existence"

// EndReason says why an element is absent, or how a piece ended. The zero
// value is [EndReasonUnknown], which a piece that reaches the window's end has
// and a disappearance never does.
type EndReason uint8

const (
	// EndReasonUnknown is the end of a piece that reaches the window's end:
	// nothing after the window is reported, so it has not ended yet. It is
	// lifecycle.EndUnknown, and is never relabeled to a known reason.
	EndReasonUnknown EndReason = 0
	// EndReasonProducer is a producer's Delete (lifecycle.EndProducer).
	EndReasonProducer EndReason = 1
	// EndReasonLivenessExpiry is silence past the producer's own TTL
	// (lifecycle.EndLivenessExpiry): not knowing, not an absence.
	EndReasonLivenessExpiry EndReason = 2
	// EndReasonCascade is an endpoint dying (lifecycle.EndCascade).
	EndReasonCascade EndReason = 3
	// EndReasonRetention is the retention horizon moving
	// (lifecycle.EndRetention).
	EndReasonRetention EndReason = 4
	// EndReasonOperator is an operator purge (lifecycle.EndOperator).
	EndReasonOperator EndReason = 5
	// EndReasonUnattributed is the end a leg gives (LegEnd.Reason) when the leg
	// goes on but its placement or mapping became ambiguous: the store prefers
	// unattributed to a guess. The adapters end the derived span itself with
	// [EndReasonReachEnded]; the format also accepts unattributed as the end of
	// a derived piece. It is for derived subjects only.
	EndReasonUnattributed EndReason = 6
	// EndReasonReachEnded is the end_reason of a derived span whose end is the
	// end of an intersection or of an ambiguity: a blast-radius reach, a
	// co-residency overlap, an unattributed span. It has no producer or expiry
	// source of its own; where the span has legs, their own ends are listed
	// beside it, and nothing is inferred. It is for derived subjects only.
	EndReasonReachEnded EndReason = 7
	// EndReasonNotSeenInWindow is the absence before a subject's first
	// appearance in the window: nothing says why it was absent. It is the
	// before-state of such an appearance, never the end of a piece.
	EndReasonNotSeenInWindow EndReason = 8
)

// endReasonNames names the values of the table.
var endReasonNames = [...]string{
	EndReasonUnknown:         "unknown",
	EndReasonProducer:        "producer",
	EndReasonLivenessExpiry:  "liveness_expiry",
	EndReasonCascade:         "cascade",
	EndReasonRetention:       "retention",
	EndReasonOperator:        "operator",
	EndReasonUnattributed:    "unattributed",
	EndReasonReachEnded:      "reach_ended",
	EndReasonNotSeenInWindow: "not_seen_in_window",
}

// Valid reports whether r is a value of the table.
func (r EndReason) Valid() bool { return int(r) < len(endReasonNames) }

// String names the reason as the wire's vocabulary does, and
// "EndReason(n)" for a value outside the table.
func (r EndReason) String() string {
	if r.Valid() {
		return endReasonNames[r]
	}
	return fmt.Sprintf("EndReason(%d)", uint8(r))
}

// lifecycleOnly says r is one of the lifecycle's own end sources, which a
// producer or the store authors: what an asserted subject's piece can end by.
func (r EndReason) lifecycleOnly() bool {
	return r >= EndReasonProducer && r <= EndReasonOperator
}

// EndReasonOf maps a lifecycle end source to its wire value, naming every
// case. A value outside lifecycle's table is refused rather than carried
// across.
func EndReasonOf(s lifecycle.EndSource) (EndReason, error) {
	switch s {
	case lifecycle.EndUnknown:
		return EndReasonUnknown, nil
	case lifecycle.EndProducer:
		return EndReasonProducer, nil
	case lifecycle.EndLivenessExpiry:
		return EndReasonLivenessExpiry, nil
	case lifecycle.EndCascade:
		return EndReasonCascade, nil
	case lifecycle.EndRetention:
		return EndReasonRetention, nil
	case lifecycle.EndOperator:
		return EndReasonOperator, nil
	}
	return 0, fmt.Errorf("subgraph: the end source %s has no wire value: %w", s, store.ErrInvalid)
}

// Class is the lifecycle class of an element in the window, a total function of
// its pieces. The zero value is [ClassNone].
type Class uint8

const (
	// ClassNone is an element with no piece in the window.
	ClassNone Class = 0
	// ClassStable is one piece covering the whole window.
	ClassStable Class = 1
	// ClassAppeared is one piece that starts after From and reaches To.
	ClassAppeared Class = 2
	// ClassDisappeared is one piece that starts at From and ends before To.
	ClassDisappeared Class = 3
	// ClassTransient is one piece that starts after From and ends before To.
	ClassTransient Class = 4
	// ClassFlapping is two pieces or more.
	ClassFlapping Class = 5
)

// String names the class as package interval does: "none", "stable",
// "appeared", "disappeared", "transient" or "flapping", and "Class(n)" for any
// other value.
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

// ClassOf maps an interval class to its wire value, naming every case. A value
// outside interval's table is refused rather than carried across.
func ClassOf(c interval.Class) (Class, error) {
	switch c {
	case interval.ClassNone:
		return ClassNone, nil
	case interval.ClassStable:
		return ClassStable, nil
	case interval.ClassAppeared:
		return ClassAppeared, nil
	case interval.ClassDisappeared:
		return ClassDisappeared, nil
	case interval.ClassTransient:
		return ClassTransient, nil
	case interval.ClassFlapping:
		return ClassFlapping, nil
	}
	return 0, fmt.Errorf("subgraph: the interval class %s has no wire value: %w", c, store.ErrInvalid)
}
