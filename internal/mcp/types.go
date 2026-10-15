package mcp

import (
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// NameTableVersion is the version of the table of keys a name is read from (see
// names.go). Every answer that carries names carries it in its policy, and a
// change to the table raises it.
const NameTableVersion = 1

// PayloadTableVersion is the version of the per-type table of top-level keys a
// description may print (see payload.go). Every answer that prints a description
// carries it in its policy, and a change to the table raises it.
const PayloadTableVersion = 1

// The labels of an element. basis says how a value was obtained; subject says
// what it is about. An element that is a stored record is a fact. Every
// range-read element is derived.
const (
	BasisFact       = "fact"
	BasisDerived    = "derived"
	SubjectAsserted = "asserted"
	SubjectDerived  = "derived"
)

// The rules of derived elements. A rule that ends in a reason or a bucket is
// completed with it: unattributed.<reason> and diff.<bucket>.
const (
	RuleExistence       = "existence"
	RuleReach           = "blast_radius.reach"
	RuleSharedNode      = "co_residency.shared_node"
	RuleSharedHost      = "co_residency.shared_host"
	RuleUnattributed    = "unattributed."
	RuleDiff            = "diff."
	endReasonReachEnded = "reach_ended"
	endReasonNotKnown   = "unattributed"
)

// Policy lists the versions an answer was derived under. rank is a digest of
// the authority order of producers, which decides which description wins; it is
// part of the world an answer was read in.
type Policy struct {
	Interval    int    `json:"interval"`
	BlastRadius int    `json:"blastradius,omitempty"`
	GraphDiff   int    `json:"graphdiff,omitempty"`
	CoResidency int    `json:"coresidency,omitempty"`
	Rank        string `json:"rank,omitempty"`
	Names       int    `json:"names,omitempty"`
	Payload     int    `json:"payload,omitempty"`
}

// Ref is an entity id next to its name. The name is derived from a description
// and never stored as a fact; it is null when the description has none, and for
// a type that is unnamed on purpose.
type Ref struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Name      *string `json:"name"`
	Namespace *string `json:"namespace,omitempty"`
	// NameFrom is the producer whose description the name was read from.
	NameFrom string `json:"name_from,omitempty"`
	// NameNote says why a name is missing when the reason is that the entity was
	// not there to describe.
	NameNote string `json:"name_note,omitempty"`
	// FP is the fingerprint, for the code that fills the names.
	FP identity.Fingerprint `json:"-"`
}

func ref(fp identity.Fingerprint) Ref {
	return Ref{ID: fp.String(), Type: string(fp.Type()), FP: fp}
}

// EdgeRef is a directed relation between two entities.
type EdgeRef struct {
	From     Ref    `json:"from"`
	To       Ref    `json:"to"`
	Relation string `json:"relation"`
}

func edgeRef(s store.Subject) EdgeRef {
	return EdgeRef{From: ref(s.A), To: ref(s.B), Relation: string(s.Relation)}
}

// Window is a half-open range [from, to) of event time.
type Window struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func windowOf(from, to time.Time) Window { return Window{From: fmtTime(from), To: fmtTime(to)} }

// Span is an interval [start, end) of event time. end_reason says why it ends,
// and is null exactly when the span reaches the window's end.
type Span struct {
	Start       string  `json:"start"`
	End         string  `json:"end"`
	EndReason   *string `json:"end_reason"`
	EndSentence string  `json:"end_sentence"`
	// BeganBeforeWindow says the existence began before the window; start is then
	// the window's start.
	BeganBeforeWindow bool `json:"began_before_window,omitempty"`
	// AssertedBy lists the producers whose reference was live in the span.
	AssertedBy []string `json:"asserted_by,omitempty"`
}

const stillPresent = "still present at the window's end"

// Quarantine is an entity whose existence cannot be folded because two boots of
// it were live at once. It is shown, never dropped or guessed. collision is null
// only if the store gave no detail.
type Quarantine struct {
	Entity    Ref        `json:"entity"`
	Collision *Collision `json:"collision"`
}

// Collision names the conflicting boots and the times they were seen.
type Collision struct {
	StaleBoot      string `json:"stale_boot"`
	ObservedAt     string `json:"observed_at"`
	NewerBoot      string `json:"newer_boot"`
	NewerFirstSeen string `json:"newer_first_seen"`
}

func quarantineOf(q *store.QuarantineError) Quarantine {
	out := Quarantine{Entity: ref(q.Entity)}
	if c := q.Collision; c != nil {
		out.Collision = &Collision{
			StaleBoot: c.StaleBoot, ObservedAt: fmtTime(c.ObservedAt),
			NewerBoot: c.NewerBoot, NewerFirstSeen: fmtTime(c.NewerFirstSeen),
		}
	}
	return out
}
