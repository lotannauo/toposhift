package lifecycle

import (
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"
)

// EndSource says how an existence interval ended. The zero value means
// unknown and is never relabeled to a known source after the fact.
type EndSource uint8

const (
	// EndUnknown means the source is not known. It is also what an open-ended
	// interval reports, since it has not ended.
	EndUnknown EndSource = iota
	// EndProducer means a producer's Delete ended the last live reference.
	EndProducer
	// EndLivenessExpiry means the last live reference passed its deadline.
	EndLivenessExpiry
	// EndCascade, EndRetention and EndOperator are authored by the store
	// layer, never by Fold: an endpoint dying, the retention horizon moving,
	// and an operator purge.
	EndCascade
	EndRetention
	EndOperator
)

func (s EndSource) String() string {
	switch s {
	case EndUnknown:
		return "unknown"
	case EndProducer:
		return "producer"
	case EndLivenessExpiry:
		return "liveness_expiry"
	case EndCascade:
		return "cascade"
	case EndRetention:
		return "retention"
	case EndOperator:
		return "operator"
	}
	return "EndSource(" + strconv.Itoa(int(s)) + ")"
}

// Interval is a maximal run of existence, [Start, End). An open-ended interval
// has a zero End and an unknown EndSource.
type Interval struct {
	Start, End time.Time
	EndSource  EndSource
}

// Open reports whether the interval has no end.
func (i Interval) Open() bool { return i.End.IsZero() }

// Timeline is the result of a fold. It is immutable and safe for concurrent
// use. The zero value describes a subject that never existed.
type Timeline struct {
	existence []Interval
	boots     []Boot
	refs      map[Producer][]Assertion // ascending, one assertion per event time
	policy    Policy
}

// Fold computes the timeline of one subject from the assertions made about it.
// It reports every invalid assertion, joined, wrapping [ErrInvalid], and
// returns [ErrCloneCollision] with no timeline when two boots were live at
// once. It does not modify its input.
func Fold(as []Assertion, p Policy) (Timeline, error) {
	if err := p.validate(); err != nil {
		return Timeline{}, err
	}
	sorted, err := prepare(as, p)
	if err != nil {
		return Timeline{}, err
	}
	refs := latestPerInstant(sorted)
	incs, err := boots(effective(refs), p)
	if err != nil {
		return Timeline{}, err
	}

	var segs []segment
	for _, list := range refs {
		segs = append(segs, segments(list)...)
	}
	p.Rank = maps.Clone(p.Rank)
	return Timeline{existence: merge(segs), boots: incs, refs: refs, policy: p}, nil
}

// latestPerInstant groups the assertions by producer, keeping only the last
// one by sequence at each event time: the earlier ones are overwritten at the
// instant they were made and have no effect on existence or description.
// sorted must be in (EventTime, Seq) order.
func latestPerInstant(sorted []Assertion) map[Producer][]Assertion {
	refs := make(map[Producer][]Assertion)
	for _, a := range sorted {
		list := refs[a.Producer]
		if n := len(list); n > 0 && list[n-1].EventTime.Equal(a.EventTime) {
			list[n-1] = a
		} else {
			list = append(list, a)
		}
		refs[a.Producer] = list
	}
	return refs
}

// effective flattens the per-producer lists back into one slice in
// (EventTime, Seq) order: the assertions that have an effect.
func effective(refs map[Producer][]Assertion) []Assertion {
	var out []Assertion
	for _, list := range refs {
		out = append(out, list...)
	}
	slices.SortFunc(out, before)
	return out
}

// segment is one stretch during which a producer's reference is live: from
// one Observe until the next assertion by that producer or its deadline,
// whichever is first.
type segment struct {
	start, end time.Time
	open       bool
	cause      EndSource // unknown while a later Observe continues the reference
}

// segments lists a producer's live stretches. list is ascending with one
// assertion per event time, so every segment has positive length.
func segments(list []Assertion) []segment {
	var out []segment
	for i, a := range list {
		if a.Kind != Observe {
			continue
		}
		s := segment{start: a.EventTime}
		deadline := a.deadline()
		switch {
		case i+1 == len(list) && a.TTL == 0:
			s.open = true
		case i+1 == len(list):
			s.end, s.cause = deadline, EndLivenessExpiry
		case a.TTL > 0 && deadline.Before(list[i+1].EventTime):
			s.end, s.cause = deadline, EndLivenessExpiry
		default:
			// Replaced or released at the next assertion. A Delete ends the
			// reference there; an Observe continues it, and the segment it
			// starts touches this one and takes over the cause.
			s.end = list[i+1].EventTime
			if list[i+1].Kind == Delete {
				s.cause = EndProducer
			}
		}
		out = append(out, s)
	}
	return out
}

// strength orders end sources for a tie at one instant: an explicit statement
// outranks silence, which outranks not knowing.
func strength(s EndSource) int {
	switch s {
	case EndProducer:
		return 2
	case EndLivenessExpiry:
		return 1
	}
	return 0
}

// merge unions the segments of all producers into maximal intervals. Runs
// that touch are one interval.
func merge(segs []segment) []Interval {
	slices.SortFunc(segs, func(a, b segment) int { return a.start.Compare(b.start) })

	var out []Interval
	var cur segment
	have := false
	flush := func() {
		iv := Interval{Start: cur.start}
		if !cur.open {
			iv.End, iv.EndSource = cur.end, cur.cause
		}
		out = append(out, iv)
	}
	for _, s := range segs {
		switch {
		case !have:
			cur, have = s, true
		case cur.open:
			// Already unbounded: nothing can extend it.
		case !s.start.After(cur.end):
			switch {
			case s.open:
				cur.open, cur.cause = true, EndUnknown
			case s.end.After(cur.end):
				cur.end, cur.cause = s.end, s.cause
			case s.end.Equal(cur.end) && strength(s.cause) > strength(cur.cause):
				cur.cause = s.cause
			}
		default:
			flush()
			cur = s
		}
	}
	if have {
		flush()
	}
	return out
}

// Existence returns the maximal runs of existence in ascending order. It
// returns a copy.
func (t Timeline) Existence() []Interval { return slices.Clone(t.existence) }

// Boots returns the boots seen, in order of first appearance, or nil if
// the policy tracks none. It returns a copy.
func (t Timeline) Boots() []Boot { return slices.Clone(t.boots) }

// AliveAt reports whether the subject exists at x.
func (t Timeline) AliveAt(x time.Time) bool {
	i := sort.Search(len(t.existence), func(i int) bool { return t.existence[i].Start.After(x) })
	if i == 0 {
		return false
	}
	iv := t.existence[i-1]
	return iv.Open() || x.Before(iv.End)
}
