package lifecycle

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// Boot is one boot of a subject, as seen by its producers.
type Boot struct {
	// ID is the value of the policy's boot key.
	ID string
	// FirstSeen and LastSeen bound the observations that carried this boot ID.
	FirstSeen, LastSeen time.Time
}

// CloneCollisionError reports two boots live at once for one subject. It
// unwraps to [ErrCloneCollision].
type CloneCollisionError struct {
	// StaleBoot is the older boot that was observed again.
	StaleBoot string
	// ObservedAt is when StaleBoot was observed again.
	ObservedAt time.Time
	// NewerBoot first appeared after StaleBoot did.
	NewerBoot string
	// NewerFirstSeen is when NewerBoot first appeared.
	NewerFirstSeen time.Time
}

func (e *CloneCollisionError) Error() string {
	return fmt.Sprintf("%v: boot %q was observed at %s, after boot %q first appeared at %s",
		ErrCloneCollision, e.StaleBoot, e.ObservedAt.Format(time.RFC3339Nano),
		e.NewerBoot, e.NewerFirstSeen.Format(time.RFC3339Nano))
}

func (e *CloneCollisionError) Unwrap() error { return ErrCloneCollision }

// bootID returns the boot ID an Observe carries under key, or "" if it carries
// none. A boot ID that is present must be a non-empty string.
func bootID(a Assertion, key catalog.AttributeKey) (string, error) {
	for _, attr := range a.Attrs {
		if attr.Key != key {
			continue
		}
		s, ok := attr.Value.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", fmt.Errorf("attribute %q must be a non-empty string, got %#v", key, attr.Value)
		}
		return s, nil
	}
	return "", nil
}

// Sighting is one instant at which a subject was seen in one boot.
type Sighting struct {
	// At is the instant: an Observe's EventTime, or the last instant of a run.
	At time.Time
	// ID is the boot ID the Observe carried.
	ID string
}

// CompareSightings orders sightings by instant, and those at one instant by boot
// ID: the order a fold takes them in.
func CompareSightings(x, y Sighting) int {
	if c := x.At.Compare(y.At); c != 0 {
		return c
	}
	return strings.Compare(x.ID, y.ID)
}

// Sightings returns the boot sightings a fold of as under p takes, in the order
// it takes them ([CompareSightings]), each (instant, ID) once, or none if p tracks
// no boots. They come from the assertions that have an effect: per producer, the
// one with the highest Seq at each event time. Each such Observe that carries a
// boot is seen at its EventTime and, when it is a run, at its last instant too. It
// reports invalid assertions as [Fold] does, and does not modify its input.
//
// Folding the sightings in order with a [BootTracker] gives the boots and the
// collision Fold gives. So a caller that discards the assertions before some
// instant can keep the tracker's state, fed with the sightings before that
// instant, and the sightings at or after it, and go on from there.
func Sightings(as []Assertion, p Policy) ([]Sighting, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	sorted, err := prepare(as, p)
	if err != nil {
		return nil, err
	}
	return sightings(effective(latestPerInstant(sorted)), p), nil
}

// sightings lists the boot sightings of the assertions that have an effect, in
// (EventTime, Seq) order and validated, sorted and without repeats.
//
// A run of refreshes is observed at its first and last instants; the ones
// between can neither start a boot nor, being earlier than the last, collide when
// the last does not. Observations are taken in time order, and those at the same
// instant by boot ID, so that which boot "came first" at one instant never
// depends on sequence numbers, which differ between a run and the refreshes it
// replaced. Taking one sighting twice changes nothing, so repeats are dropped.
func sightings(effective []Assertion, p Policy) []Sighting {
	if p.BootKey == "" {
		return nil
	}
	var out []Sighting
	for _, a := range effective {
		if a.Kind != Observe {
			continue
		}
		id, _ := bootID(a, p.BootKey) // validated
		if id == "" {
			continue
		}
		out = append(out, Sighting{a.EventTime, id})
		if end := a.last(); end.After(a.EventTime) {
			out = append(out, Sighting{end, id})
		}
	}
	slices.SortFunc(out, CompareSightings)
	return slices.CompactFunc(out, func(x, y Sighting) bool { return CompareSightings(x, y) == 0 })
}

// boots lists the boots in order of first appearance and detects clone
// collisions. effective must be in (EventTime, Seq) order and already validated.
func boots(effective []Assertion, p Policy) ([]Boot, error) {
	if p.BootKey == "" {
		return nil, nil
	}
	tr := NewBootTracker(p)
	for _, s := range sightings(effective, p) {
		if err := tr.Observe(s.ID, s.At); err != nil {
			return nil, err
		}
	}
	return tr.Boots(), nil
}

// BootTracker folds the boot sightings of one subject, taken in the order of
// [CompareSightings], into its boots and its first clone collision. [Fold] uses
// one. Its state is [BootTracker.Boots] and [BootTracker.Collision]: a caller that
// has folded the sightings before some instant and then discarded the assertions
// they came from can keep that state, [BootTracker.Seed] a new tracker with it, and
// go on with the sightings at or after that instant, as if it had never stopped.
//
// A tracker is not safe for concurrent use. Make one with [NewBootTracker].
type BootTracker struct {
	skew  time.Duration
	list  []Boot
	index map[string]int
	// collision is the first collision, after which the state no longer changes.
	collision *CloneCollisionError
	// last is the latest sighting taken, or the latest instant of the seed with no
	// ID, so that a sighting out of order is refused.
	last    Sighting
	started bool
}

// NewBootTracker returns a tracker with no boots, under the skew of p, which
// must not be negative ([Fold] refuses such a policy).
func NewBootTracker(p Policy) *BootTracker {
	return &BootTracker{skew: p.Skew, index: map[string]int{}}
}

// Observe takes the sighting of boot id at instant at. It returns a
// [*CloneCollisionError] if the sighting collides, or if one taken or seeded
// before it did: the first collision is kept, and nothing changes after it. A
// sighting before the previous one in the order of [CompareSightings], or with
// an empty ID, is refused with an error wrapping [ErrInvalid] and changes
// nothing.
func (b *BootTracker) Observe(id string, at time.Time) error {
	if b.collision != nil {
		c := *b.collision
		return &c
	}
	s := Sighting{at, id}
	switch {
	case id == "":
		return fmt.Errorf("boot tracker: a sighting at %s with no boot ID: %w", at.Format(time.RFC3339Nano), ErrInvalid)
	case b.started && CompareSightings(s, b.last) < 0:
		return fmt.Errorf("boot tracker: boot %q at %s is before the sighting taken last, %q at %s: %w",
			id, at.Format(time.RFC3339Nano), b.last.ID, b.last.At.Format(time.RFC3339Nano), ErrInvalid)
	}
	b.last, b.started = s, true
	i, known := b.index[id]
	if !known {
		b.index[id] = len(b.list)
		b.list = append(b.list, Boot{ID: id, FirstSeen: at, LastSeen: at})
		return nil
	}
	// The boot that first appeared right after this one is the one that has been
	// live longest, so it is the only one worth checking.
	if i+1 < len(b.list) {
		newer := b.list[i+1]
		if at.Sub(newer.FirstSeen) > b.skew {
			b.collision = &CloneCollisionError{
				StaleBoot: id, ObservedAt: at,
				NewerBoot: newer.ID, NewerFirstSeen: newer.FirstSeen,
			}
			c := *b.collision
			return &c
		}
	}
	b.list[i].LastSeen = at
	return nil
}

// Boots returns the boots taken so far, in order of first appearance, or nil if
// there are none. It returns a copy.
func (b *BootTracker) Boots() []Boot { return slices.Clone(b.list) }

// Collision returns the first collision taken or seeded, or nil if there is
// none. It returns a copy.
func (b *BootTracker) Collision() *CloneCollisionError {
	if b.collision == nil {
		return nil
	}
	c := *b.collision
	return &c
}

// Seed replaces the state of the tracker with boots and collision, as
// [BootTracker.Boots] and [BootTracker.Collision] returned them. Later sightings
// must not be earlier than any instant in the seed. It refuses, with an error
// wrapping [ErrInvalid] and leaving the tracker as it was, a state no fold
// gives: an empty or repeated ID, a boot last seen before it was first seen,
// boots out of the order of first appearance, or a collision with no boots, or
// whose newer boot is not the one that first appeared right after its stale
// boot, or that is not the stale boot's sighting after the newer boot's first.
func (b *BootTracker) Seed(boots []Boot, collision *CloneCollisionError) error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("boot tracker: seed: %s: %w", fmt.Sprintf(format, args...), ErrInvalid)
	}
	index := make(map[string]int, len(boots))
	var latest time.Time
	for i, bt := range boots {
		switch _, dup := index[bt.ID]; {
		case bt.ID == "":
			return fail("boot %d has no ID", i)
		case dup:
			return fail("boot %q appears twice", bt.ID)
		case bt.LastSeen.Before(bt.FirstSeen):
			return fail("boot %q was last seen before it was first seen", bt.ID)
		case i > 0 && CompareSightings(Sighting{bt.FirstSeen, bt.ID}, Sighting{boots[i-1].FirstSeen, boots[i-1].ID}) < 0:
			return fail("boot %q first appeared before boot %q, which is listed before it", bt.ID, boots[i-1].ID)
		}
		index[bt.ID] = i
		if bt.LastSeen.After(latest) {
			latest = bt.LastSeen
		}
	}
	if collision != nil {
		i, ok := index[collision.StaleBoot]
		switch {
		case !ok || i+1 >= len(boots) || boots[i+1].ID != collision.NewerBoot:
			return fail("collision of %q with %q: not a boot and the one that first appeared after it", collision.StaleBoot, collision.NewerBoot)
		case !collision.NewerFirstSeen.Equal(boots[i+1].FirstSeen):
			return fail("collision with %q, which first appeared at %s, not %s", collision.NewerBoot,
				boots[i+1].FirstSeen.Format(time.RFC3339Nano), collision.NewerFirstSeen.Format(time.RFC3339Nano))
		case !collision.ObservedAt.After(collision.NewerFirstSeen):
			return fail("collision observed at %s, not after the newer boot first appeared", collision.ObservedAt.Format(time.RFC3339Nano))
		}
		if collision.ObservedAt.After(latest) {
			latest = collision.ObservedAt
		}
	}
	b.list = slices.Clone(boots)
	b.index = index
	b.collision = nil
	if collision != nil {
		c := *collision
		b.collision = &c
	}
	b.last, b.started = Sighting{At: latest}, len(boots) > 0
	return nil
}
