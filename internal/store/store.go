package store

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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

// String names the direction: "forward" or "reverse", and "Direction(n)" for any
// other value.
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
// records whose Seq is at or below the snapshot token. Every read of a [Store]
// takes one.
type Scope struct {
	// Layer is the churn layer read. Only subjects stored in it are visible, and
	// the zero value is invalid.
	Layer catalog.Layer
	// AsOf is the snapshot token: the read behaves as if only records with
	// Seq <= AsOf had been written, which is lifecycle.Visible. [Latest] sees
	// every record in the snapshot the read is answered from; a caller that needs
	// several reads to see one world passes [Store.LastSeq] taken before them.
	// A token below [Horizon.Seq] is refused with [ErrBeforeHorizon].
	AsOf uint64
}

// Current is the scope that reads everything in layer.
func Current(layer catalog.Layer) Scope { return Scope{Layer: layer, AsOf: Latest} }

// Validate reports whether the scope can be read. A layer outside L0 to L3 is
// an error wrapping [ErrInvalid].
func (s Scope) Validate() error {
	if s.Layer < catalog.L0 || s.Layer > catalog.L3 {
		return fmt.Errorf("scope layer %s: %w", s.Layer, ErrInvalid)
	}
	return nil
}

// Neighbor is one edge alive at an instant, seen from one end.
type Neighbor struct {
	// Peer is the entity at the other end of the edge.
	Peer identity.Fingerprint
	// Relation is the edge's relation.
	Relation catalog.RelationType
}

// Horizon is the retention horizon a store has applied: Time is the instant
// before which history may be gone, and Seq is the highest token that was
// committed when the horizon was applied (the L of [Store.Retain]). The zero
// Horizon means no retention has happened yet, and while the horizon is zero no
// read is refused for the horizon.
type Horizon struct {
	// Time is the instant before which history may have been discarded.
	Time time.Time
	// Seq is the highest Seq that was committed when Time was applied.
	Seq uint64
}

// IsZero reports whether no retention has been applied.
func (h Horizon) IsZero() bool { return h.Time.IsZero() && h.Seq == 0 }

// Store is the contract every storage backend satisfies.
//
// Every read answers "as of" an instant in event time and a [Scope]: one layer,
// and the records up to a snapshot token. The instant may be any time, inside
// the representable range or not: before [MinEventTime] nothing exists, and
// after [MaxEventTime] the answer is the state after every record. The one
// limit is the retention horizon: a read whose instant t (for [Store.Window] and
// [Store.EntityWindow], whose from) is before [Store.Horizon] Time, or whose AsOf
// is below the horizon's Seq, is refused with an error wrapping
// [ErrBeforeHorizon], because the history it asks about may be gone. An AsOf of
// [Latest] is never below the horizon, and while [Store.Horizon] is zero no read
// is refused for the horizon. A token above [Store.LastSeq] that is not [Latest]
// is treated as LastSeq at the snapshot the read is answered from.
//
// Existence of an entity is folded by the lifecycle specification with the
// store's lifecycle.Policy, which is fixed when the store is opened and may name
// a boot key.
//
// # Arguments
//
// A read whose scope is not valid, whose direction is not [Forward] or
// [Reverse], or whose fingerprint is the zero fingerprint (identity.Fingerprint
// has IsZero) returns an error wrapping [ErrInvalid]; so does a
// [Store.NeighborsBatch] with a zero fingerprint among its fps. These checks come
// before the horizon check, and none of them depends on what is stored. For
// [Store.Window] and [Store.EntityWindow], from >= to is not an error: the answer
// is empty with a nil error (the horizon check still applies to from). An empty
// fps for NeighborsBatch gives an empty result and a nil error once the scope and
// direction are checked. An empty batch for [Store.Write] is a no-op that returns
// nil.
//
// # Contexts and failure
//
// Every method that may do I/O takes a context as its first argument. A read
// whose context is cancelled returns an error wrapping the context's error.
//
// A batch given to Write is whole or absent. After Write returns a non-nil error,
// the caller learns which from [Store.LastSeq]: if it has advanced to the batch's
// last Seq the batch was stored, and otherwise nothing from it was and its
// sequence numbers are unused. A context error or a validation error always
// means nothing was stored. Write does not promise that every other error means
// nothing was stored, because a storage engine can report an error from a commit
// that landed.
//
// # Concurrency and consistency
//
// Reads may run concurrently with each other and with Write. Write and Retain
// are serialized by the caller, as the single sequencer of the product is. A
// read, and a whole batched read, is answered from one consistent snapshot of
// whole committed batches: it never sees part of a batch, and never mixes the
// world before a batch with the world after it.
//
// # Closing
//
// After [Store.Close], every method that returns an error, other than Close,
// returns one wrapping [ErrClosed]; this check comes first, so even an empty
// Write is refused. LastSeq and Horizon return the values they had when the store
// closed. A second Close returns nil. Close must not be called concurrently with
// calls still in flight: the caller stops using the store first, and the store
// is not required to wait for or interrupt them.
type Store interface {
	// Write stores a batch. Records must be in ascending Seq, and above every
	// Seq already written; Seq starts at 1, because token 0 means "nothing".
	// The store writes both directions of every edge.
	// A batch with any invalid record, or any record before the retention
	// horizon ([ErrBeforeHorizon]), is refused whole: nothing from it is
	// stored, and its sequence numbers stay unused. A store whose layout has a
	// finite capacity may also refuse a valid record with [ErrInvalid] when it is
	// reached. An empty batch changes nothing and returns nil.
	//
	// A batch is whole or absent on any error. A context error or a validation
	// error always means nothing was stored. For any other error the caller
	// reads [Store.LastSeq]: if it advanced to the batch's last Seq the batch was
	// stored, and otherwise nothing was and its sequence numbers are unused.
	//
	// Every record of one subject must carry the same Layer. That is the
	// caller's precondition: a store is not required to detect a violation,
	// because that would cost a lookup per record. (Entity records are checked
	// by [Record.Validate] against the catalog.)
	Write(ctx context.Context, batch []Record) error

	// LastSeq is the highest Seq committed, or 0 if none: the current snapshot
	// token. It changes only after the batch that raised it is visible to reads,
	// so a read pinned to it never sees less than it names. After Close it
	// returns the value it had.
	LastSeq() uint64

	// Horizon returns the retention horizon the store has applied, which is the
	// zero Horizon until the first [Store.Retain] that moves it. Reads before it
	// are refused with [ErrBeforeHorizon]; while it is zero no read is refused
	// for the horizon. After Close it returns the value it had.
	Horizon() Horizon

	// Neighbors returns the edges in the scope that are alive at t and touch fp
	// in the given direction, ordered by peer fingerprint then relation. An edge
	// is alive if any producer's reference to it is live by the lifecycle
	// specification; endpoint existence is not consulted, so Neighbors never
	// returns a [*QuarantineError]. An entity with no edges has an empty answer,
	// which may be nil.
	Neighbors(ctx context.Context, fp identity.Fingerprint, dir Direction, t time.Time, s Scope) ([]Neighbor, error)

	// NeighborsBatch is Neighbors for many fingerprints at once, the form a
	// blast-radius traversal uses for a whole frontier. The result is parallel
	// to fps: result[i] answers fps[i]. fps may be in any order and may repeat,
	// and an entity with no edges has an empty answer, which may be nil, so a
	// caller compares with len. The whole call is answered from one snapshot.
	// One failure fails the whole call, with a nil result, as [NeighborsEach]
	// does. Empty fps give an empty result and a nil error. (It takes no order
	// because a caller cannot know the one a store's keys use; a store sorts
	// internally if that helps its seeks.)
	NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, dir Direction, t time.Time, s Scope) ([][]Neighbor, error)

	// Alive reports whether the entity exists at t, in the scope, by the same
	// rule as Neighbors. An entity whose existence cannot be folded because two
	// of its boots were live at once is quarantined, not dropped: Alive returns a
	// [*QuarantineError] naming the conflicting boots and times, instead of a
	// boolean that would hide it. The boolean is false whenever the error is
	// not nil.
	//
	// Whether the entity is quarantined is decided by folding every record of the
	// entity visible at s.AsOf, whatever t is, so the answer does not depend on t.
	// A retention baseline must preserve a quarantine: a store that replaces
	// records before the horizon must keep an entity quarantined if folding them
	// would have quarantined it.
	Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s Scope) (bool, error)

	// Window returns the edge records in the scope touching fp in the given
	// direction with from <= EventTime < to, ordered by (EventTime, Seq).
	// Payloads are included. Every stored record is returned, including those a
	// later record of the same producer at the same instant overwrites in effect:
	// a query pinned to an earlier token sees them, so they are kept. Derived
	// events (an expiry leaves no record at its instant) are not returned.
	// Window does not fold endpoint existence and never returns a
	// [*QuarantineError]. From >= to gives an empty result and a nil error.
	Window(ctx context.Context, fp identity.Fingerprint, dir Direction, from, to time.Time, s Scope) ([]Record, error)

	// EntityWindow returns the existence records of fp itself (subject kind
	// entity) in the scope with from <= EventTime < to, ordered by
	// (EventTime, Seq). It treats them exactly the way Window treats edge
	// records: payloads are included, every stored record is returned, and
	// derived events are not. It does not fold existence and never returns a
	// [*QuarantineError]. From >= to gives an empty result and a nil error.
	EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s Scope) ([]Record, error)

	// Retain lets the store discard history before horizon. Called when the
	// highest committed Seq is L, it must leave every answer unchanged for an
	// instant at or after horizon and a token at or above L. That includes
	// Window and EntityWindow: every record with an event time at or after
	// horizon stays, so a window starting at or after it is unchanged. Only
	// records strictly before horizon may go, and the store may replace them with
	// a baseline standing for the state they left (a baseline must preserve a
	// quarantine, see [Store.Alive]). Answers for earlier instants, or earlier
	// tokens, are not given: a read whose instant (or, for Window and
	// EntityWindow, whose from) is before horizon, or whose AsOf is below L, is
	// refused with an error wrapping [ErrBeforeHorizon]. From then on Write
	// refuses records with an event time before horizon.
	//
	// The new horizon is published, so that the refusal applies, before any
	// history is discarded. A Retain that returns an error has either moved the
	// horizon whole or not at all; discarding may be partial only below a horizon
	// that is already published.
	//
	// The horizon only moves forward: a Retain that does not move it changes
	// nothing, and L is not raised by it. While [Store.Horizon] is zero no read is
	// refused for the horizon. Reads may overlap a Retain; for an instant at or
	// after horizon and a token at or above L they see the same answer before,
	// during and after it. A read whose instant lies between the old and the new
	// horizon while a Retain is in progress may be answered or refused, but is
	// never answered wrongly. LastSeq does not change.
	Retain(ctx context.Context, horizon time.Time) error

	// Close releases the store. After it, every method that returns an error,
	// other than Close, returns one wrapping [ErrClosed]; LastSeq and Horizon
	// return their last values; a second Close returns nil. Close must not be
	// called concurrently with calls in flight: the caller stops using the store
	// first.
	Close() error
}
