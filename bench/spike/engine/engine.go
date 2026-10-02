// Package engine defines what a storage candidate must do for the
// storage-engine spike, and the record it stores.
//
// It is deliberately not the product's Store interface, which is the job of a
// later task. It is the narrowest surface that exercises the reads the design
// depends on (neighbors as of an instant in both directions, what changed in a
// window, retention) so candidate layouts can be compared and checked against
// the oracle.
package engine

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Direction is which way an edge is read from an entity.
type Direction uint8

const (
	// Forward reads the edges an entity is the source of: pod scheduled_on node
	// read from the pod.
	Forward Direction = iota + 1
	// Reverse reads the edges an entity is the target of: the same edge read
	// from the node.
	Reverse
)

func (d Direction) String() string {
	switch d {
	case Forward:
		return "forward"
	case Reverse:
		return "reverse"
	}
	return fmt.Sprintf("Direction(%d)", uint8(d))
}

// SubjectKind says whether a record is about an entity or an edge.
type SubjectKind uint8

const (
	// SubjectEntity is the existence of one entity.
	SubjectEntity SubjectKind = iota + 1
	// SubjectEdge is one directed relation between two entities.
	SubjectEdge
)

// Subject is what a record asserts something about.
type Subject struct {
	Kind SubjectKind
	// A is the entity, or the edge's source.
	A identity.Fingerprint
	// B is the edge's target. Zero for an entity.
	B identity.Fingerprint
	// Relation is the edge's relation. Empty for an entity.
	Relation catalog.RelationType
}

// EntitySubject is the subject for one entity's existence.
func EntitySubject(fp identity.Fingerprint) Subject {
	return Subject{Kind: SubjectEntity, A: fp}
}

// EdgeSubject is the subject for one directed edge.
func EdgeSubject(from, to identity.Fingerprint, rel catalog.RelationType) Subject {
	return Subject{Kind: SubjectEdge, A: from, B: to, Relation: rel}
}

// The range of event times a store can represent: Unix nanoseconds in an int64
// (1970 to 2262; an MVCC-style layout could stretch to 2554 with a uint64, but
// time.Time cannot express it). The lifecycle specification allows wider times;
// a real store must reject, not clamp, any outside this range.
var (
	MinEventTime = time.Unix(0, 0).UTC()
	MaxEventTime = time.Unix(0, math.MaxInt64).UTC() // 2262-04-11
)

// Record is one stored assertion: a lifecycle assertion about a subject, in
// the form a storage layout holds it.
type Record struct {
	Layer     catalog.Layer
	Subject   Subject
	Producer  lifecycle.Producer
	EventTime time.Time
	// Seq is the ingest sequence number: unique, and ascending in write order.
	Seq     uint64
	Kind    lifecycle.Kind
	TTL     time.Duration
	Through time.Time
	// Payload is the producer's opaque description. It is incompressible in the
	// workload so byte counts are honest.
	Payload []byte
}

// ErrInvalid marks a record or call that breaks a rule of the spike.
var ErrInvalid = errors.New("invalid")

// ErrBeforeHorizon is returned by Write for a record whose event time is before
// the retention horizon last given to Retain. History before the horizon has
// been discarded, so such a record can no longer be placed correctly, and the
// engine refuses it loudly instead of ignoring it. It wraps [ErrInvalid].
var ErrBeforeHorizon = fmt.Errorf("event time is before the retention horizon: %w", ErrInvalid)

// Validate checks the rules a storage layout relies on. The assertion rules
// (producer, kind, TTL, Through, the delete rules) are the lifecycle
// specification's own, by folding the record's single assertion, so the
// engine contract cannot drift from the oracle. Event times must also fit in
// an int64 of Unix nanoseconds (1970 to 2262), and so must the deadline of a
// record with a TTL: a layout may carry them in a narrower field than the
// lifecycle specification allows.
//
// Validate is thorough, not fast: it folds an assertion. A candidate under
// measurement should validate once per batch at the ingest boundary, outside
// the timed region.
func (r Record) Validate() error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("record seq %d: %s: %w", r.Seq, fmt.Sprintf(format, args...), ErrInvalid)
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
	if r.EventTime.Before(MinEventTime) || r.EventTime.After(MaxEventTime) {
		return fail("event time %s is outside the representable range", r.EventTime.Format(time.RFC3339))
	}
	if r.Through.After(MaxEventTime) {
		return fail("through %s is out of range", r.Through.Format(time.RFC3339))
	}
	if r.TTL > 0 {
		// A layout may index the deadline (last observation plus TTL) as an
		// int64 of nanoseconds, so it must be representable too.
		last := r.EventTime
		if r.Through.After(last) {
			last = r.Through
		}
		if last.Add(r.TTL).After(MaxEventTime) {
			return fail("deadline %s is outside the representable range", last.Add(r.TTL).Format(time.RFC3339))
		}
	}
	if r.Kind == lifecycle.Delete && len(r.Payload) != 0 {
		return fail("a delete cannot carry a payload")
	}
	if _, err := lifecycle.Fold([]lifecycle.Assertion{r.Assertion()}, lifecycle.Policy{}); err != nil {
		return fmt.Errorf("record seq %d: %w: %w", r.Seq, err, ErrInvalid)
	}
	return nil
}

// payloadKey is the attribute that carries the payload into a lifecycle
// assertion, so a run of identical refreshes coalesces.
const payloadKey catalog.AttributeKey = "payload"

// Assertion converts the record to the lifecycle assertion it stands for.
func (r Record) Assertion() lifecycle.Assertion {
	a := lifecycle.Assertion{
		Producer:  r.Producer,
		EventTime: r.EventTime,
		Seq:       r.Seq,
		Kind:      r.Kind,
		TTL:       r.TTL,
		Through:   r.Through,
	}
	if r.Kind == lifecycle.Observe {
		a.Attrs = []identity.Attr{{Key: payloadKey, Value: string(r.Payload)}}
	}
	return a
}

// Neighbor is one edge alive at an instant, seen from one end.
type Neighbor struct {
	Peer     identity.Fingerprint
	Relation catalog.RelationType
}

// Engine is a storage candidate.
//
// Every method that takes a time answers "as of" that instant in event time,
// using every record written so far. Nothing here is concurrent-safe unless an
// implementation says so; the spike's writer is a single sequencer.
type Engine interface {
	// Write stores a batch. Records must be in ascending Seq, and above every
	// Seq already written. The engine writes both directions of every edge.
	// A batch with any invalid record, or any record before the retention
	// horizon ([ErrBeforeHorizon]), is refused whole: nothing from it is
	// stored, and its sequence numbers stay unused.
	Write(batch []Record) error

	// Neighbors returns the edges alive at t that touch fp in the given
	// direction, ordered by peer fingerprint then relation. An edge is alive if
	// any producer's reference to it is live by the lifecycle specification;
	// endpoint existence is not consulted.
	Neighbors(fp identity.Fingerprint, dir Direction, t time.Time) ([]Neighbor, error)

	// Alive reports whether the entity exists at t by the same rule.
	Alive(fp identity.Fingerprint, t time.Time) (bool, error)

	// Window returns the edge records touching fp in the given direction with
	// from <= EventTime < to, ordered by (EventTime, Seq). Payloads are
	// included.
	Window(fp identity.Fingerprint, dir Direction, from, to time.Time) ([]Record, error)

	// Retain lets the engine discard history before horizon. Every answer for
	// an instant at or after horizon must be unchanged. Answers before it are
	// unspecified, and callers do not ask. From then on Write refuses records
	// with an event time before horizon. The horizon only moves forward.
	Retain(horizon time.Time) error

	// Size is the bytes the engine holds on disk, or its best estimate.
	Size() (int64, error)

	// Close releases the engine.
	Close() error
}

// SortNeighbors orders neighbors by peer fingerprint then relation, the order
// Neighbors promises.
func SortNeighbors(ns []Neighbor) {
	slices.SortFunc(ns, func(a, b Neighbor) int {
		if c := CompareFingerprints(a.Peer, b.Peer); c != 0 {
			return c
		}
		return cmp.Compare(a.Relation, b.Relation)
	})
}

// CompareFingerprints orders fingerprints by type name then hash. It is the
// order of every sorted result.
func CompareFingerprints(a, b identity.Fingerprint) int {
	if c := cmp.Compare(a.Type(), b.Type()); c != 0 {
		return c
	}
	ha, hb := a.Hash(), b.Hash()
	return bytes.Compare(ha[:], hb[:])
}

// SortRecords orders records by (EventTime, Seq), the order Window promises.
func SortRecords(rs []Record) {
	slices.SortFunc(rs, func(a, b Record) int {
		if c := a.EventTime.Compare(b.EventTime); c != 0 {
			return c
		}
		return cmp.Compare(a.Seq, b.Seq)
	})
}
