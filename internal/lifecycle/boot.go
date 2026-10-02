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

// boots lists the boots in order of first appearance and detects clone
// collisions. sorted must be in (EventTime, Seq) order and already validated.
func boots(sorted []Assertion, p Policy) ([]Boot, error) {
	if p.BootKey == "" {
		return nil, nil
	}
	var list []Boot
	index := make(map[string]int)
	observe := func(id string, at time.Time) error {
		i, known := index[id]
		if !known {
			index[id] = len(list)
			list = append(list, Boot{ID: id, FirstSeen: at, LastSeen: at})
			return nil
		}
		// The boot that first appeared right after this one is the one that
		// has been live longest, so it is the only one worth checking.
		if i+1 < len(list) {
			newer := list[i+1]
			if at.Sub(newer.FirstSeen) > p.Skew {
				return &CloneCollisionError{
					StaleBoot: id, ObservedAt: at,
					NewerBoot: newer.ID, NewerFirstSeen: newer.FirstSeen,
				}
			}
		}
		list[i].LastSeen = at
		return nil
	}

	// A run of refreshes is observed at its first and last instants; the ones
	// between can neither start a boot nor, being earlier than the last,
	// collide when the last does not. Observations are taken in time order,
	// and those at the same instant by boot ID, so that which boot "came
	// first" at one instant never depends on sequence numbers, which differ
	// between a run and the refreshes it replaced.
	type point struct {
		at time.Time
		id string
	}
	var points []point
	for _, a := range sorted {
		if a.Kind != Observe {
			continue
		}
		id, _ := bootID(a, p.BootKey) // validated
		if id == "" {
			continue
		}
		points = append(points, point{a.EventTime, id})
		if end := a.last(); end.After(a.EventTime) {
			points = append(points, point{end, id})
		}
	}
	slices.SortFunc(points, func(x, y point) int {
		if c := x.at.Compare(y.at); c != 0 {
			return c
		}
		return strings.Compare(x.id, y.id)
	})
	for _, pt := range points {
		if err := observe(pt.id, pt.at); err != nil {
			return nil, err
		}
	}
	return list, nil
}
