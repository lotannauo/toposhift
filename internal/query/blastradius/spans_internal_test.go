package blastradius

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

var t0 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

// drawSpans is a sorted list of disjoint, non-touching spans on a grid of
// nanoseconds in [0, 30].
func drawSpans(t *rapid.T, label string) []interval.Span {
	vs := rapid.SliceOfDistinct(rapid.IntRange(0, 30), func(v int) int { return v }).Draw(t, label)
	slices.Sort(vs)
	var out []interval.Span
	for i := 0; i+1 < len(vs); i += 2 {
		out = append(out, interval.Span{Start: t0.Add(time.Duration(vs[i])), End: t0.Add(time.Duration(vs[i+1]))})
	}
	return out
}

func in(ss []interval.Span, x time.Time) bool {
	return slices.ContainsFunc(ss, func(s interval.Span) bool { return s.Contains(x) })
}

func wellFormed(ss []interval.Span) bool {
	for i, s := range ss {
		if !s.Start.Before(s.End) || (i > 0 && !ss[i-1].End.Before(s.Start)) {
			return false
		}
	}
	return true
}

func TestUnionAndSubtractPointwise(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, b, c := drawSpans(t, "a"), drawSpans(t, "b"), drawSpans(t, "c")
		u, d := union(a, b, c), subtract(a, b)
		if !wellFormed(u) || !wellFormed(d) {
			t.Fatalf("union %v or subtract %v is not sorted, disjoint and non-touching", u, d)
		}
		for v := -1; v <= 31; v++ {
			x := t0.Add(time.Duration(v))
			if in(u, x) != (in(a, x) || in(b, x) || in(c, x)) {
				t.Fatalf("union at %d: %v of %v %v %v", v, u, a, b, c)
			}
			if in(d, x) != (in(a, x) && !in(b, x)) {
				t.Fatalf("subtract at %d: %v of %v minus %v", v, d, a, b)
			}
		}
	})
}

// TestClassOfIsTheRangeReadsTable checks classOf row by row against the class
// names of range reads.
func TestClassOfIsTheRangeReadsTable(t *testing.T) {
	from, to := t0, t0.Add(30)
	s := func(a, b int) interval.Span {
		return interval.Span{Start: t0.Add(time.Duration(a)), End: t0.Add(time.Duration(b))}
	}
	for _, tc := range []struct {
		spans []interval.Span
		class interval.Class
		n     int
	}{
		{nil, interval.ClassNone, 0},
		{[]interval.Span{s(0, 30)}, interval.ClassStable, 0},
		{[]interval.Span{s(5, 30)}, interval.ClassAppeared, 1},
		{[]interval.Span{s(0, 5)}, interval.ClassDisappeared, 1},
		{[]interval.Span{s(5, 10)}, interval.ClassTransient, 2},
		{[]interval.Span{s(0, 5), s(10, 30)}, interval.ClassFlapping, 2},
		{[]interval.Span{s(1, 5), s(10, 20)}, interval.ClassFlapping, 4},
	} {
		if c, n := classOf(tc.spans, from, to); c != tc.class || n != tc.n {
			t.Errorf("classOf(%v) = %s %d, want %s %d", tc.spans, c, n, tc.class, tc.n)
		}
	}
}

// TestSingleClaimTable fixes the single-claim table of policy version 1.
func TestSingleClaimTable(t *testing.T) {
	var got []catalog.RelationType
	for r := range catalog.Default().Relations() {
		if singleClaim(r.Type()) {
			got = append(got, r.Type())
		}
	}
	if !slices.Equal(got, []catalog.RelationType{catalog.RunsOn, catalog.LocatedIn}) {
		t.Errorf("single-claim relations %v", got)
	}
}
