package coresidency

import (
	"slices"

	"github.com/lotannauo/toposhift/internal/query/interval"
)

// union and subtract work on the alternatives of one hop: the several nodes a
// seed is scheduled on, or the several hosts a node claims. They never compose
// two hops; every path is interval.Along of its legs.

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
