package store

import (
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

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
	// Kind says whether the subject is an entity or an edge.
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
	// MinEventTime is the earliest event time a record may carry.
	MinEventTime = time.Unix(0, 0).UTC()
	// MaxEventTime is the latest event time a record may carry.
	MaxEventTime = time.Unix(0, math.MaxInt64).UTC() // 2262-04-11
)

// EventTimeBasis says which clock stamped a record's event time: the object's, the
// producer's, a collector's, or this store's. It is descriptive: no fold, existence
// or answer depends on it; it tells a later reader whose clock to distrust. The zero
// value is unknown.
type EventTimeBasis uint8

const (
	// BasisUnknown means the producer or the ingest layer did not say.
	BasisUnknown EventTimeBasis = iota
	// BasisObjectField is a time the object itself carries, such as a Kubernetes
	// creationTimestamp or a condition's lastTransitionTime.
	BasisObjectField
	// BasisObserved is the time a collector observed the event, on the collector's
	// clock (OTLP observed_time_unix_nano; the only timestamp k8sobjects sets).
	BasisObserved
	// BasisReceipt is the time this store's ingest layer received the record, on its
	// own clock (a pure refresh).
	BasisReceipt
	// BasisProducerEvent is the time the producer stamped on the event itself, on
	// the producer's clock (OTLP time_unix_nano: a state change that keeps the
	// producer's time).
	BasisProducerEvent
)

// String returns the basis as the name a file or a log shows: "unknown",
// "object_field", "observed", "receipt" or "producer_event". Any other value is
// "EventTimeBasis(n)".
func (b EventTimeBasis) String() string {
	switch b {
	case BasisUnknown:
		return "unknown"
	case BasisObjectField:
		return "object_field"
	case BasisObserved:
		return "observed"
	case BasisReceipt:
		return "receipt"
	case BasisProducerEvent:
		return "producer_event"
	}
	return fmt.Sprintf("EventTimeBasis(%d)", uint8(b))
}

// Valid reports whether b is one of the defined bases.
func (b EventTimeBasis) Valid() bool { return b <= BasisProducerEvent }

// Record is one stored assertion: a lifecycle assertion about a subject, in
// the form a storage layout holds it. It carries both time axes: EventTime is
// when the statement was true, and Seq is the ingest sequence number, which is
// the snapshot token that first sees it.
type Record struct {
	// Layer is the churn layer the subject is stored in.
	Layer catalog.Layer
	// Subject is what the record asserts something about.
	Subject Subject
	// Producer is who made the assertion.
	Producer lifecycle.Producer
	// EventTime is when the statement was true.
	EventTime time.Time
	// Seq is the ingest sequence number: unique, and ascending in write order.
	Seq uint64
	// Kind says whether the producer observed the subject or deleted it: Observe
	// or Delete. The value 3 is reserved for an operator purge, and is refused
	// until its meaning is decided.
	Kind lifecycle.Kind
	// TTL is how long an observation stays live without a refresh.
	TTL time.Duration
	// Through is the last instant a run of identical refreshes was observed, if
	// the record stands for a run.
	Through time.Time
	// Payload is the producer's opaque description. It is empty for a delete.
	Payload []byte
	// Boot identifies the boot of the machine a host observation was made in
	// (the attribute topo.host.boot.id, copied by the ingest layer). It is empty
	// when the producer reports none. Only an observation of a host carries one:
	// a delete, an edge record and any other entity type never do. It is valid
	// UTF-8 of at most [MaxBootLen] bytes, and is compared as given: trimming or
	// normalising a producer's value is the ingest layer's job, because two
	// values that differ in white space are two boots.
	// A store opened with a [lifecycle.Policy] that names [lifecycle.BootID]
	// tells boots apart, and reports two live boots of one host as a clone
	// collision, from this field.
	Boot string
	// EventTimeBasis says where EventTime came from. It is descriptive: no fold,
	// existence or answer depends on it, and it is not an attribute of the
	// assertion. A record of any kind may carry any valid basis. The zero value
	// is [BasisUnknown].
	EventTimeBasis EventTimeBasis
}

// Validate checks the rules a storage layout relies on. The assertion rules
// (producer, kind, TTL, Through, the delete rules) are the lifecycle
// specification's own, by folding the record's single assertion, so the store
// contract cannot drift from the specification. Event times must also fit in
// an int64 of Unix nanoseconds (1970 to 2262), and so must the deadline of a
// record with a TTL: a layout may carry them in a narrower field than the
// lifecycle specification allows. Every error wraps [ErrInvalid].
//
// Validate is thorough, not fast: it folds an assertion. A store should
// validate once per batch at the ingest boundary.
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
		// table yet (it is the caller's precondition, see [Store.Write]).
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
	if !r.EventTimeBasis.Valid() {
		return fail("event time basis %s", r.EventTimeBasis)
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
	policy := lifecycle.Policy{}
	if r.Boot != "" {
		switch {
		case r.Kind != lifecycle.Observe:
			return fail("only an observation carries a boot id")
		case r.Subject.Kind != SubjectEntity || r.Subject.A.Type() != catalog.Host:
			return fail("only a host record carries a boot id")
		case len(r.Boot) > MaxBootLen:
			return fail("boot id is %d bytes, more than %d", len(r.Boot), MaxBootLen)
		case !utf8.ValidString(r.Boot):
			return fail("boot id is not valid UTF-8")
		}
		// The specification refuses a blank boot id (empty or white space only).
		policy.BootKey = lifecycle.BootID
	}
	if _, err := lifecycle.Fold([]lifecycle.Assertion{r.Assertion()}, policy); err != nil {
		return fmt.Errorf("record seq %d: %w: %w", r.Seq, err, ErrInvalid)
	}
	return nil
}

// MaxBootLen is the longest boot id, in bytes, a record may carry. Boot ids are
// identifiers (a UUID is 36 bytes); the cap keeps a layout's value encoding and
// a file format's column small and bounded.
const MaxBootLen = 256

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
		if r.Boot != "" {
			a.Attrs = append(a.Attrs, identity.Attr{Key: lifecycle.BootID, Value: r.Boot})
		}
	}
	return a
}
