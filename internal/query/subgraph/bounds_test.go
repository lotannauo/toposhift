package subgraph_test

import (
	"fmt"
	"testing"

	"github.com/lotannauo/toposhift/internal/query/subgraph"
)

// podN is the n-th of any number of pods.
func podN(n int) subgraph.Key { return ent(pod(n)) }

// F1: the subgraph grows from 10 to 1010 elements, one a second. A keyframe held
// at the size at From would be written every 10 deltas, 100 times over, and
// move about 52,500 entries; the rule moves at most N0 + 2E = 2,010.
func TestBoundsGrowth(t *testing.T) {
	const n0, e = 10, 1000
	w := e + 2
	var srcs []subgraph.Source
	for i := range n0 {
		srcs = append(srcs, mk(w, podN(i+1), sp(0, -1)))
	}
	for j := range e {
		srcs = append(srcs, mk(w, podN(n0+j+1), sp(j+1, -1)))
	}
	in := input(w, srcs...)
	sg := build(t, in)
	entries, deltas, size0 := check(t, in, sg)
	if deltas != e || size0 != n0 || entries > n0+2*e {
		t.Errorf("entries %d, E %d, N0 %d", entries, deltas, size0)
	}
	// The set outgrows the deltas since the last keyframe at every instant, so
	// a seek is cheap without another keyframe, and none is written.
	if len(sg.Keyframes) != 1 || entries != n0+e {
		t.Errorf("%d keyframes and %d entries, want one keyframe and %d entries", len(sg.Keyframes), entries, n0+e)
	}
}

// The set shrinks from 1000 elements to none, one a second.
func TestBoundsShrink(t *testing.T) {
	const n0 = 1000
	w := n0 + 2
	var srcs []subgraph.Source
	for i := range n0 {
		srcs = append(srcs, mk(w, podN(i+1), sp(0, i+1)))
	}
	in := input(w, srcs...)
	sg := build(t, in)
	entries, e, _ := check(t, in, sg)
	if e != n0 || entries > n0+2*e {
		t.Errorf("entries %d, E %d", entries, e)
	}
	// The last keyframe is the empty state.
	if last := sg.Keyframes[len(sg.Keyframes)-1]; len(last.Elements) != 0 {
		t.Errorf("the last keyframe holds %d elements", len(last.Elements))
	}
}

// A relist: all 1000 elements go at one instant and all return at the next.
func TestBoundsRelist(t *testing.T) {
	const n = 1000
	w := 30
	var srcs []subgraph.Source
	for i := range n {
		srcs = append(srcs, mk(w, podN(i+1), sp(0, 10), sp(11, -1)))
	}
	in := input(w, srcs...)
	sg := build(t, in)
	entries, e, n0 := check(t, in, sg)
	if e != 2*n || n0 != n || entries > n0+2*e {
		t.Errorf("entries %d, E %d, N0 %d", entries, e, n0)
	}
	// At 10 the set is empty after 1000 deltas: an empty keyframe, and at 11 the
	// full one.
	if len(sg.Keyframes) != 3 || len(sg.Keyframes[1].Elements) != 0 || len(sg.Keyframes[2].Elements) != n {
		t.Errorf("keyframes %d: %v", len(sg.Keyframes), keyframeSizes(sg))
	}
}

func keyframeSizes(sg subgraph.Subgraph) []int {
	var out []int
	for _, k := range sg.Keyframes {
		out = append(out, len(k.Elements))
	}
	return out
}

// An element that comes and goes inside the window leaves the set empty, and
// the empty set still gets its keyframe: max(1, |S|) is 1, so a seek costs at
// most 0 deltas there rather than the whole run.
func TestBoundsEmptyState(t *testing.T) {
	const w = 20
	in := input(w, mk(w, podN(1), sp(3, 4)), mk(w, podN(2), sp(6, 9)))
	sg := build(t, in)
	check(t, in, sg)
	sizes := keyframeSizes(sg)
	// While the set is this small every instant gets a keyframe: d = 1 reaches
	// max(1, |S|) at once, with one element and with none.
	if len(sizes) != 5 {
		t.Errorf("keyframe sizes %v", sizes)
	}
	empty := 0
	for _, k := range sg.Keyframes[1:] {
		if len(k.Elements) == 0 {
			empty++
		}
	}
	if empty != 2 {
		t.Errorf("%d empty keyframes after From, want 2: %v", empty, sizes)
	}
}

// Two deltas at one instant, an element out and another in, are one instant: no
// keyframe falls between them, so a keyframe never holds a state that never
// was.
func TestKeyframeNeverFallsInsideAnInstant(t *testing.T) {
	const w = 12
	in := input(w, mk(w, podN(1), sp(0, 5)), mk(w, podN(2), sp(5, -1)))
	sg := build(t, in)
	check(t, in, sg)
	if len(sg.Keyframes) != 2 || sg.Keyframes[1].At != 5*1e9 || len(sg.Keyframes[1].Elements) != 1 {
		t.Errorf("keyframes %+v", sg.Keyframes)
	}
	if id, _ := sg.ElementID(podN(2)); sg.Keyframes[1].Elements[0] != id {
		t.Errorf("the keyframe at the swap holds element %d, want %d", sg.Keyframes[1].Elements[0], id)
	}
}

// Steady churn is where the bound is tight. N elements are present throughout
// and, at each of m instants, one goes and one arrives, so E = 2m. With N even
// and E a multiple of N, a keyframe falls at every N/2 instants, last of all on
// the last delta instant, and the entries are exactly N0 + 2E. There is nothing
// left over for a keyframe that should not be there, at To or anywhere else.
func TestBoundsSteadyChurnIsExact(t *testing.T) {
	for _, tc := range []struct{ n, m int }{{2, 1}, {2, 5}, {4, 2}, {4, 6}, {6, 9}, {10, 5}, {10, 20}, {50, 100}} {
		t.Run(fmt.Sprintf("N=%d,m=%d", tc.n, tc.m), func(t *testing.T) {
			if tc.n%2 != 0 || (2*tc.m)%tc.n != 0 {
				t.Fatal("the fixture needs an even N and an E that is a multiple of N")
			}
			w := tc.m + 2
			// Element j is present from max(0, j-N+1) to j+1 (or To when that
			// is past the last instant): at instant k, j = k-1 leaves and
			// j = k+N-1 arrives.
			var srcs []subgraph.Source
			for j := range tc.m + tc.n {
				start := max(0, j-tc.n+1)
				end := j + 1
				if end > tc.m {
					end = -1
				}
				srcs = append(srcs, mk(w, podN(j+1), sp(start, end)))
			}
			in := input(w, srcs...)
			sg := build(t, in)
			entries, e, n0 := check(t, in, sg)
			if n0 != tc.n || e != 2*tc.m {
				t.Fatalf("N0 %d, E %d", n0, e)
			}
			if entries != n0+2*e {
				t.Errorf("entries %d, want exactly N0 + 2E = %d", entries, n0+2*e)
			}
			// A keyframe every N/2 instants, the last on the last delta instant.
			if want := 1 + tc.m/(tc.n/2); len(sg.Keyframes) != want {
				t.Errorf("%d keyframes, want %d", len(sg.Keyframes), want)
			}
			if last := sg.Keyframes[len(sg.Keyframes)-1]; last.At != sg.Deltas[len(sg.Deltas)-1].At {
				t.Errorf("the last keyframe is at +%dns, the last delta at +%dns", last.At, sg.Deltas[len(sg.Deltas)-1].At)
			}
		})
	}
}
