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

// Latest is the snapshot token that sees everything committed.
const Latest uint64 = math.MaxUint64

// Scope says which part of the stored history a read sees: one layer, and the
// records whose Seq is at or below the snapshot token. It is the layer and
// snapshot token the product's store takes on every read.
type Scope struct {
	// Layer is the churn layer read. Only subjects stored in it are visible, and
	// the zero value is invalid.
	Layer catalog.Layer
	// AsOf is the snapshot token: the read behaves as if only records with
	// Seq <= AsOf had been written, which is lifecycle.Visible. [Latest] sees
	// every record in the snapshot the read is answered from; a caller that needs
	// several reads to see one world passes [Engine.LastSeq] taken before them.
	AsOf uint64
}

// Current is the scope that reads everything in layer.
func Current(layer catalog.Layer) Scope { return Scope{Layer: layer, AsOf: Latest} }

// Validate reports whether the scope can be read.
func (s Scope) Validate() error {
	if s.Layer < catalog.L0 || s.Layer > catalog.L3 {
		return fmt.Errorf("scope layer %s: %w", s.Layer, ErrInvalid)
	}
	return nil
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
		// An entity lives in the layer its type belongs to. Edges have no such
		// table yet (it is the caller's precondition, see [Engine.Write]).
		e, ok := catalog.Default().Entity(r.Subject.A.Type())
		if !ok {
			return fail("unknown entity type %q", r.Subject.A.Type())
		}
		if r.Layer != e.Layer() {
			return fail("entity type %s is in layer %s, not %s", e.Type(), e.Layer(), r.Layer)
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
// Every read answers "as of" an instant in event time and a [Scope]: one layer,
// and the records up to a snapshot token. The instant may be any time, inside
// the representable range or not: before [MinEventTime] nothing exists, and
// after [MaxEventTime] the answer is the state after every record.
//
// # Concurrency and consistency
//
// Reads may run concurrently with each other and with Write. Write and Retain
// are serialized by the caller, as the spike's single sequencer is. A read, and
// a whole batched read, is answered from one consistent snapshot of whole
// committed batches: it never sees part of a batch, and never mixes the world
// before a batch with the world after it.
type Engine interface {
	// Write stores a batch. Records must be in ascending Seq, and above every
	// Seq already written; Seq starts at 1, because token 0 means "nothing".
	// The engine writes both directions of every edge.
	// A batch with any invalid record, or any record before the retention
	// horizon ([ErrBeforeHorizon]), is refused whole: nothing from it is
	// stored, and its sequence numbers stay unused. An engine whose layout has a
	// finite capacity may also refuse a valid record with [ErrInvalid] when it is
	// reached (the MVCC layout, after 2^32-1 versions of one key at one instant).
	//
	// Every record of one subject must carry the same Layer. That is the
	// caller's precondition: an engine is not required to detect a violation,
	// because that would cost a lookup per record. (Entity records are checked
	// by [Record.Validate] against the catalog.)
	Write(batch []Record) error

	// LastSeq is the highest Seq committed, or 0 if none: the current snapshot
	// token. It changes only after the batch that raised it is visible to reads,
	// so a read pinned to it never sees less than it names.
	LastSeq() uint64

	// Neighbors returns the edges in the scope that are alive at t and touch fp
	// in the given direction, ordered by peer fingerprint then relation. An edge
	// is alive if any producer's reference to it is live by the lifecycle
	// specification; endpoint existence is not consulted.
	Neighbors(fp identity.Fingerprint, dir Direction, t time.Time, s Scope) ([]Neighbor, error)

	// NeighborsBatch is Neighbors for many fingerprints at once, the form a
	// blast-radius traversal uses for a whole frontier. The result is parallel
	// to fps: result[i] answers fps[i]. fps may be in any order and may repeat,
	// and an entity with no edges has an empty answer. The whole call is answered
	// from one snapshot. (It takes no order because a caller cannot know the one
	// an engine's keys use; an engine sorts internally if that helps its seeks.)
	NeighborsBatch(fps []identity.Fingerprint, dir Direction, t time.Time, s Scope) ([][]Neighbor, error)

	// Alive reports whether the entity exists at t, in the scope, by the same
	// rule as Neighbors.
	Alive(fp identity.Fingerprint, t time.Time, s Scope) (bool, error)

	// Window returns the edge records in the scope touching fp in the given
	// direction with from <= EventTime < to, ordered by (EventTime, Seq).
	// Payloads are included. Every stored record is returned, including those a
	// later record of the same producer at the same instant overwrites in effect:
	// a query pinned to an earlier token sees them, so they are kept. Derived
	// events (an expiry leaves no record at its instant) are not returned.
	Window(fp identity.Fingerprint, dir Direction, from, to time.Time, s Scope) ([]Record, error)

	// Retain lets the engine discard history before horizon. Called when the
	// highest committed Seq is L, it must leave every answer unchanged for an
	// instant at or after horizon and a token at or above L. That includes
	// Window: every record with an event time at or after horizon stays, so a
	// window starting at or after it is unchanged. Only records strictly before
	// horizon may go, and the engine may replace them with a baseline standing
	// for the state they left. Answers for earlier instants, or earlier tokens,
	// are unspecified and callers do not ask (the product's store refuses them).
	// From then on Write refuses records with an event time before horizon.
	//
	// The horizon only moves forward: a Retain that does not move it changes
	// nothing, and L is not raised by it. Reads may overlap a Retain; for an
	// instant at or after horizon and a token at or above L they see the same
	// answer before, during and after it. LastSeq does not change.
	Retain(horizon time.Time) error

	// Size is the bytes the engine holds on disk, or its best estimate.
	Size() (int64, error)

	// Close releases the engine.
	Close() error
}

// NeighborsEach answers a batched read by asking read once for each
// fingerprint, in order. It is for an engine that has no faster form yet: read
// must be bound to one snapshot by the caller, or the batch is not consistent
// under concurrent writes.
func NeighborsEach(fps []identity.Fingerprint, read func(identity.Fingerprint) ([]Neighbor, error)) ([][]Neighbor, error) {
	out := make([][]Neighbor, len(fps))
	for i, fp := range fps {
		ns, err := read(fp)
		if err != nil {
			return nil, err
		}
		out[i] = ns
	}
	return out, nil
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
