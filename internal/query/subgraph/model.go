package subgraph

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the keyframe rule, the
// order of deltas, the labels and the tables of this package. A cache of
// subgraphs keys on it, and a change to any of them raises it.
const PolicyVersion = 2

// Key names one element: an entity, an edge, or a derived subject. It is
// comparable, and no two elements of a subgraph have the same key.
type Key struct {
	Kind Kind
	// A is the entity, the edge's source, or a derived subject's first end: the
	// seed of a co-resident or a reach, the member of a pair.
	A identity.Fingerprint
	// B is the edge's target, or a derived subject's second end: the peer of a
	// co-resident or a pair, the member a reach reaches. Zero for an entity.
	B identity.Fingerprint
	// Relation is the edge's relation. Empty otherwise.
	Relation catalog.RelationType
	// Rule names the rule that derives a derived subject, one of the strings of
	// the derived-kind table ([DerivedKinds]). Empty otherwise.
	Rule string
	// Detail is the rest of a derived subject's identity within its rule and
	// ends, a canonical string the adapter that derives the subject defines. The
	// two ends are dictionary references through A and B and are not in it.
	// Empty otherwise.
	Detail string
}

// EntityKey is the key of one entity's existence.
func EntityKey(fp identity.Fingerprint) Key { return Key{Kind: KindEntity, A: fp} }

// EdgeKey is the key of one directed edge.
func EdgeKey(from, to identity.Fingerprint, rel catalog.RelationType) Key {
	return Key{Kind: KindEdge, A: from, B: to, Relation: rel}
}

// KeyOf is the key of a store subject.
func KeyOf(s store.Subject) Key {
	if s.Kind == store.SubjectEntity {
		return EntityKey(s.A)
	}
	return EdgeKey(s.A, s.B, s.Relation)
}

// Compare orders keys: entities before edges before derived subjects, then by
// store.CompareFingerprints of the entity or the edge's source, then of its
// target, then by relation, rule and detail. It is graphdiff's order of
// subjects, with derived subjects last.
func (k Key) Compare(o Key) int {
	if c := cmp.Compare(k.Kind, o.Kind); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(k.A, o.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(k.B, o.B); c != 0 {
		return c
	}
	if c := cmp.Compare(k.Relation, o.Relation); c != 0 {
		return c
	}
	if c := cmp.Compare(k.Rule, o.Rule); c != 0 {
		return c
	}
	return cmp.Compare(k.Detail, o.Detail)
}

func (k Key) String() string {
	switch k.Kind {
	case KindEntity:
		return "entity " + k.A.String()
	case KindEdge:
		return fmt.Sprintf("edge %s -[%s]-> %s", k.A, k.Relation, k.B)
	case KindDerived:
		return fmt.Sprintf("derived %s %s -> %s %q", k.Rule, k.A, k.B, k.Detail)
	}
	return fmt.Sprintf("key of %s", k.Kind)
}

// Source is one element as a builder takes it: a subject, its pieces in the
// window, and the class and the transitions derived from them. An adapter makes
// one per result element; the builder checks every claim it makes.
type Source struct {
	Key Key
	// Pieces are ascending, disjoint and never touching, each inside the window.
	Pieces []SourcePiece
	// Class and Transitions follow the class table of the window: Transitions
	// counts the piece boundaries strictly inside (From, To). The builder
	// computes both from the pieces and refuses a claim that differs.
	Class       Class
	Transitions int
	// Paths are the legs of a derived subject: the edges whose intersection is
	// its span, one list per way it holds (a co-resident has one, a pair one for
	// each contribution, a reach one for each arrival). Each leg is the key of an
	// edge that is an element of the input. Empty for any other subject.
	Paths [][]Key
	// Quarantine is set for an entity the store has quarantined. Pieces is then
	// empty and Class is [ClassNone].
	Quarantine *store.QuarantineError
	// Uncascaded is set for an edge one of whose endpoints is quarantined: its
	// pieces are its own, not cut by its endpoints.
	Uncascaded *store.QuarantineError
}

// SourcePiece is one interval of a [Source] inside the window.
type SourcePiece struct {
	Start, End time.Time
	// Before says the existence began before From (and then Start is From).
	Before bool
	// End is why the piece ended: [EndReasonUnknown] exactly when End is the
	// window's To, because nothing after To is reported.
	EndReason EndReason
	// Producers lists, sorted and once each, the producers whose reference was
	// live at some instant of the piece. A derived subject has none.
	Producers []lifecycle.Producer
	// Ends lists, for a derived subject whose rule names them, the legs that end
	// where the piece does, ascending by path and leg: it is empty exactly when
	// the piece reaches To, and for a rule that names no leg ends (a reach).
	Ends []LegEnd
}

// LegEnd is one leg of a derived subject that ends where a piece of the subject
// ends.
type LegEnd struct {
	// Path and Leg index [Source].Paths and the path's legs.
	Path, Leg int
	// Reason is the leg's own end: a lifecycle reason, or [EndReasonUnattributed]
	// when the leg goes on but its placement or mapping became ambiguous.
	Reason EndReason
}

// Input is what a builder takes: the one token the elements were read at, the
// window, and the elements.
type Input struct {
	Token    uint64
	From, To time.Time
	Sources  []Source
	// Read lists the entities the adapter read in: each was asked for, whether or
	// not it existed in the window. An entity a key names that is neither read,
	// nor an element, nor quarantined is a boundary. An entity listed here that no
	// key names is not in the dictionary.
	Read []identity.Fingerprint
	// Sections are what the query reports that is not an element. They are in the
	// shape of the output, with times in nanoseconds after From.
	Sections Sections
}

// Subgraph is a temporal subgraph: the elements present in a window, as
// keyframes, invertible deltas and interval lists. It is derived, a pure
// function of its [Input] and [PolicyVersion], and nothing in this package
// writes it to a store.
//
// Every time in it is a count of nanoseconds after From.
type Subgraph struct {
	Basis         Basis
	PolicyVersion int
	// Token is the snapshot token every read of the input used.
	Token    uint64
	From, To time.Time
	// Entities is the dictionary: every entity a key names, ascending by
	// store.CompareFingerprints. An entity's id is its index.
	Entities []Entity
	// Elements are the elements that exist at some instant of the window,
	// ascending by [Key.Compare]. An element's id is its index.
	Elements []Element
	// Keyframes are ascending by time; the first is at 0, the window's From.
	Keyframes []Keyframe
	// Deltas are ordered by time, then element id (entity before edge before
	// derived subject, then subject), then appearance before disappearance.
	// Their times are strictly inside the window and never decrease.
	Deltas []Delta
	// Quarantine is the static section: quarantined entities, and uncascaded
	// edges, which no keyframe or delta holds.
	Quarantine Quarantine
	// Sections are what the query reported that is not an element, carried whole.
	Sections Sections
}

// Entity is a dictionary entry.
type Entity struct {
	Fingerprint identity.Fingerprint
	// Boundary says the entity is named by a key, a leg or a section and was not
	// read, was not found quarantined, and has no element: the far end of a
	// scope's edge is. An entity that was read and found absent from the window
	// is not a boundary, and neither is a quarantined one, which has an entry in
	// the static section.
	Boundary bool
}

// Element is one subject of a subgraph, with its pieces: its interval list.
type Element struct {
	Key Key
	// Subject says whether a producer asserts the subject itself.
	Subject Subject
	// Rule is [RuleExistence] for an asserted subject, and for a derived one the
	// rule that derives it.
	Rule        string
	Class       Class
	Transitions int
	// Paths are the legs of a derived subject, as element ids of edges, in the
	// order of the input's paths. Empty for any other subject.
	Paths  [][]uint32
	Pieces []Piece
}

// Piece is one interval of an element, in nanoseconds after From.
type Piece struct {
	Start, End uint64
	Before     bool
	// EndReason is why the piece ended; [EndReasonUnknown] when End is the
	// window's end.
	EndReason EndReason
	Producers []lifecycle.Producer
	// Ends are the legs of a derived subject that end where the piece does.
	Ends []LegEnd
}

// State is whether an element is present. When it is not, Reason says why.
type State struct {
	Present bool
	// Reason is set only when the element is absent: why it left, or, before its
	// first appearance, [EndReasonNotSeenInWindow].
	Reason EndReason
}

// Delta is one appearance or disappearance strictly inside the window. It
// carries both states, so it can be applied forward and inverted.
type Delta struct {
	At uint64
	// Element is the id of the element.
	Element       uint32
	Before, After State
}

// Appear reports whether the delta is an appearance.
func (d Delta) Appear() bool { return d.After.Present }

// Keyframe is the set of elements present at one instant, as ascending element
// ids. It is emitted only between instants, so it equals the state at its time.
type Keyframe struct {
	At       uint64
	Elements []uint32
}

// Quarantine is the static section. Neither list is in a keyframe or a delta.
type Quarantine struct {
	// Entities are the quarantined entities, ascending by fingerprint.
	Entities []QuarantinedEntity
	// Edges are the uncascaded edges, ascending by key, with their own uncut
	// pieces.
	Edges []StaticEdge
}

// QuarantinedEntity is an entity the store quarantined, with the collision.
type QuarantinedEntity struct {
	Fingerprint identity.Fingerprint
	Cause       *store.QuarantineError
}

// StaticEdge is an edge with a quarantined endpoint. Its pieces are the edge's
// own and it has no deltas, but it keeps its transitions.
type StaticEdge struct {
	Element
	Cause *store.QuarantineError
}

// Entries is the transfer cost in entries: one per element id in a keyframe and
// one per delta.
func (s Subgraph) Entries() int {
	n := len(s.Deltas)
	for _, k := range s.Keyframes {
		n += len(k.Elements)
	}
	return n
}

// EntityID returns the dictionary id of fp.
func (s Subgraph) EntityID(fp identity.Fingerprint) (uint32, bool) {
	i, ok := slices.BinarySearchFunc(s.Entities, fp, func(e Entity, fp identity.Fingerprint) int {
		return store.CompareFingerprints(e.Fingerprint, fp)
	})
	return uint32(i), ok
}

// ElementID returns the id of the element with key k.
func (s Subgraph) ElementID(k Key) (uint32, bool) {
	i, ok := slices.BinarySearchFunc(s.Elements, k, func(e Element, k Key) int { return e.Key.Compare(k) })
	return uint32(i), ok
}

func cloneProducers(ps []lifecycle.Producer) []lifecycle.Producer {
	if len(ps) == 0 {
		return nil
	}
	return slices.Clone(ps)
}
