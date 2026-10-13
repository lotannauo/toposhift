package store

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The types below are those of the lookup of a producer's last assertion of a
// subject, which pebblestore and memstore each provide as
//
//	NewLookup() (*Lookup, error)
//	(*Lookup).Last(ctx, LookupRef) (LastAssertion, error)
//	(*Lookup).Close() error
//
// It is outside [Store]: no conformance suite asks for it, and a layout that does
// not provide it is still a store. The types are shared so that a caller can use
// either layout, and a test can compare them, without an adapter.

// LookupRef identifies one reference exactly: the subject, the layer it is stored
// in, and the producer that holds the reference. An edge's subject is its source,
// target and relation; an entity's is the entity alone. Nothing is matched
// loosely: a producer whose name differs by a byte is another producer.
type LookupRef struct {
	Layer    catalog.Layer
	Subject  Subject
	Producer lifecycle.Producer
}

// Validate checks the shape of the reference, as [Record.Validate] checks a
// record's: an entity subject has one fingerprint and an edge subject two and a
// relation, the layer is one of L0 to L3, and the producer has the format of
// [ValidProducer]. It does not ask whether anything is stored under the reference.
// Every error wraps [ErrInvalid].
func (r LookupRef) Validate() error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("lookup reference: %s: %w", fmt.Sprintf(format, args...), ErrInvalid)
	}
	switch r.Subject.Kind {
	case SubjectEntity:
		if r.Subject.A.IsZero() || !r.Subject.B.IsZero() || r.Subject.Relation != "" {
			return fail("an entity subject needs exactly one fingerprint")
		}
	case SubjectEdge:
		if r.Subject.A.IsZero() || r.Subject.B.IsZero() || r.Subject.Relation == "" {
			return fail("an edge subject needs two fingerprints and a relation")
		}
	default:
		return fail("subject kind %d", r.Subject.Kind)
	}
	if r.Layer < catalog.L0 || r.Layer > catalog.L3 {
		return fail("layer %s", r.Layer)
	}
	if err := ValidProducer(r.Producer); err != nil {
		return fail("%v", err)
	}
	return nil
}

// LookupOutcome says how a lookup ended. The zero value is [LookupInvalid], which
// no lookup returns without an error: an empty [LastAssertion] never reads as an
// answer.
type LookupOutcome uint8

const (
	// LookupInvalid is the zero value. A lookup never returns it as an outcome.
	LookupInvalid LookupOutcome = iota
	// LookupFound means a record of the reference was found, and the
	// [LastAssertion] describes the newest one.
	LookupFound
	// LookupNone means the whole side was searched and holds no record of the
	// reference, or holds only a baseline that does not name it: what was asserted
	// before the horizon and is no longer alive there is not kept.
	LookupNone
	// LookupExhausted means the search reached its bound of keys before it found
	// the reference or the end of the side. The answer is unknown, not "none".
	LookupExhausted
	// LookupBaselineOnly means the reference's newest assertion is one a retention
	// folded into the baseline: its times, kind and basis are known, its payload is
	// not.
	LookupBaselineOnly
)

// String returns the outcome as a name for a log or a counter.
func (o LookupOutcome) String() string {
	switch o {
	case LookupInvalid:
		return "invalid"
	case LookupFound:
		return "found"
	case LookupNone:
		return "none"
	case LookupExhausted:
		return "exhausted"
	case LookupBaselineOnly:
		return "baseline_only"
	}
	return fmt.Sprintf("LookupOutcome(%d)", uint8(o))
}

// LastAssertion is a lookup's answer. Apart from Outcome it is set only for
// [LookupFound] and [LookupBaselineOnly].
type LastAssertion struct {
	Outcome LookupOutcome
	// Kind is Observe or Delete.
	Kind lifecycle.Kind
	// EventTime and Through are the record's own; Through is zero if it has none.
	EventTime, Through time.Time
	// Last is the latest instant the record says the producer asserted the subject:
	// Through if it is after EventTime, else EventTime. LastFromThrough says it came
	// from Through.
	Last            time.Time
	LastFromThrough bool
	// TTL is the record's, which the deadline of an Observe is Last plus.
	TTL time.Duration
	// Basis is where the event time came from.
	Basis EventTimeBasis
	// Payload is a copy of the record's payload; nil for [LookupBaselineOnly], whose
	// entry keeps none, and for a record that has none.
	Payload []byte
	// Seq is the record's sequence number.
	Seq uint64
}

// LastOf is the latest instant a record with the event time and Through says the
// producer asserted its subject, and whether it came from the Through: it does only
// if the Through is after the event time ([lifecycle.Assertion] counts it the same
// way).
func LastOf(eventTime, through time.Time) (last time.Time, fromThrough bool) {
	if through.After(eventTime) {
		return through, true
	}
	return eventTime, false
}
