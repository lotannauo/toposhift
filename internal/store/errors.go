package store

import (
	"errors"
	"fmt"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrInvalid marks a record or call that breaks a rule of the store.
var ErrInvalid = errors.New("invalid")

// ErrBeforeHorizon is returned by Write for a record whose event time is before
// the retention horizon last given to Retain, and by a read whose instant (or,
// for Window and EntityWindow, whose from) is before the horizon's time or whose
// snapshot token is below the horizon's Seq. History before the horizon has been
// discarded, so such a record can no longer be placed correctly and such a read
// can no longer be answered correctly, and the store refuses them loudly instead
// of ignoring or guessing. It wraps [ErrInvalid].
var ErrBeforeHorizon = fmt.Errorf("before the retention horizon: %w", ErrInvalid)

// ErrClosed is wrapped by the error every method of a store that returns one,
// other than Close, gives after Close. LastSeq and Horizon return their last
// values instead, and a second Close returns nil.
var ErrClosed = errors.New("store closed")

// ErrQuarantined marks an entity whose existence cannot be folded because it
// was quarantined. A [*QuarantineError] matches it with [errors.Is].
var ErrQuarantined = errors.New("quarantined")

// QuarantineError reports an entity whose existence cannot be folded because two
// boots of it were live at once (a clone collision). The entity is quarantined,
// not dropped: a read says so, with the conflicting boots and times.
type QuarantineError struct {
	// Entity is the quarantined entity.
	Entity identity.Fingerprint
	// Layer is the layer the entity was read in.
	Layer catalog.Layer
	// Collision names the conflicting boots and the times they were seen. It
	// is nil only if the fold's error carried no detail.
	Collision *lifecycle.CloneCollisionError
}

// Error names the entity, the layer and the collision.
func (e *QuarantineError) Error() string {
	if e.Collision == nil {
		return fmt.Sprintf("entity %s in layer %s: %v", e.Entity, e.Layer, ErrQuarantined)
	}
	return fmt.Sprintf("entity %s in layer %s: %v: %v", e.Entity, e.Layer, ErrQuarantined, e.Collision)
}

// Unwrap returns [ErrQuarantined], and the collision when there is one, so that
// errors.Is matches both ErrQuarantined and lifecycle.ErrCloneCollision, and
// errors.As finds the [*lifecycle.CloneCollisionError].
func (e *QuarantineError) Unwrap() []error {
	if e.Collision == nil {
		return []error{ErrQuarantined}
	}
	return []error{ErrQuarantined, e.Collision}
}
