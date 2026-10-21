package pgmodel

import (
	"math"

	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Row is one interval row: the range [Start, End) of an int8range over Unix
// nanoseconds, unbounded above when Open, with the source of its end.
//
// A row a writer keeps provisional (a deadline that has not passed yet, which the
// writer moves forward as the subject is refreshed) has an unbounded range and the
// current end in Deadline, which no index covers, so that moving it forward is a
// heap-only update. [Row.Contains] and [Row.End] apply that rule, so every read is
// the SQL's, whichever writer made the row.
type Row struct {
	Start int64
	// End is the upper bound of the range; it is meaningless when Open.
	End  int64
	Open bool
	// Deadline is the current end of a provisional row; it is meaningless unless
	// HasDeadline.
	Deadline    int64
	HasDeadline bool
	EndSource   lifecycle.EndSource
}

// Final returns a row for the interval [start, end), ended by source.
func Final(start, end int64, source lifecycle.EndSource) Row {
	return Row{Start: start, End: end, EndSource: source}
}

// OpenRow returns a row for the open interval that begins at start.
func OpenRow(start int64) Row { return Row{Start: start, Open: true} }

// Provisional returns a row for an interval that begins at start and ends, so far,
// at deadline.
func Provisional(start, deadline int64, source lifecycle.EndSource) Row {
	return Row{Start: start, Open: true, Deadline: deadline, HasDeadline: true, EndSource: source}
}

// EffectiveEnd is the end of the row: its Deadline if it has one, else the range's
// upper bound; open is true if it has neither.
func (r Row) EffectiveEnd() (end int64, open bool) {
	switch {
	case r.HasDeadline:
		return r.Deadline, false
	case r.Open:
		return math.MaxInt64, true
	}
	return r.End, false
}

// Contains is the SQL read rule: the range contains t and, for a provisional row,
// t is before the deadline.
func (r Row) Contains(t int64) bool {
	if t < r.Start {
		return false
	}
	end, open := r.EffectiveEnd()
	return open || t < end
}

// EndsAtOrBefore reports whether the row ends at or before t: a closed row whose
// effective end is not after t. It is the retention rule, which deletes such rows.
func (r Row) EndsAtOrBefore(t int64) bool {
	end, open := r.EffectiveEnd()
	return !open && end <= t
}
