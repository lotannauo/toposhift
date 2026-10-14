package extract

import (
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/query/interval"
)

// union and subtract work on the alternatives of one hop (the several pods of
// one instance, the several services of one instance, the several nodes of one
// pod) and on the contributions of one pair. They never compose two hops; every
// path is interval.Along of its legs.

// union is the instants in any of the lists, as a sorted list of disjoint,
// non-touching spans: spans that touch are joined.
func union(lists ...[]interval.Span) []interval.Span {
	var all []interval.Span
	for _, l := range lists {
		all = append(all, l...)
	}
	slices.SortFunc(all, func(a, b interval.Span) int { return a.Start.Compare(b.Start) })
	var out []interval.Span
	for _, s := range all {
		if n := len(out); n > 0 && !s.Start.After(out[n-1].End) {
			if s.End.After(out[n-1].End) {
				out[n-1].End = s.End
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

// subtract is the instants of a not in b, both sorted lists of disjoint spans.
func subtract(a, b []interval.Span) []interval.Span {
	var out []interval.Span
	j := 0
	for _, s := range a {
		cur := s
		for j < len(b) && !b[j].End.After(cur.Start) {
			j++
		}
		k := j
		for k < len(b) && b[k].Start.Before(cur.End) {
			if b[k].Start.After(cur.Start) {
				out = append(out, interval.Span{Start: cur.Start, End: b[k].Start})
			}
			if !b[k].End.Before(cur.End) {
				cur.Start = cur.End
				break
			}
			cur.Start = b[k].End
			k++
		}
		if cur.Start.Before(cur.End) {
			out = append(out, cur)
		}
	}
	return out
}

// containing returns the index of the span of ss that contains s whole, or -1.
func containing(ss []interval.Span, s interval.Span) int {
	return slices.IndexFunc(ss, func(e interval.Span) bool { return !e.Start.After(s.Start) && !e.End.Before(s.End) })
}

// at reports whether some span of ss contains t.
func at(ss []interval.Span, t time.Time) bool {
	return slices.ContainsFunc(ss, func(s interval.Span) bool { return s.Contains(t) })
}

// classOf is the class table of range reads over a pair's spans: none for none,
// flapping for two or more, and for one, by whether it starts at From and
// whether it ends at To. Transitions counts the boundaries strictly inside the
// window.
func classOf(ss []interval.Span, from, to time.Time) (interval.Class, int) {
	n := 0
	for _, s := range ss {
		if s.Start.After(from) {
			n++
		}
		if s.End.Before(to) {
			n++
		}
	}
	switch len(ss) {
	case 0:
		return interval.ClassNone, 0
	case 1:
	default:
		return interval.ClassFlapping, n
	}
	a, b := ss[0].Start.Equal(from), ss[0].End.Equal(to)
	switch {
	case a && b:
		return interval.ClassStable, n
	case b:
		return interval.ClassAppeared, n
	case a:
		return interval.ClassDisappeared, n
	}
	return interval.ClassTransient, n
}
