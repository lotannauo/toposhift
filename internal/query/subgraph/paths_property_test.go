package subgraph_test

import (
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/query/subgraph"
)

// The derived-path generator draws two to four legs, each with up to two pieces
// on a grid of seconds, one or two paths of them, and the derived element that
// the paths make: the union over the paths of the intersection of their legs'
// pieces, each piece ending by the end of an intersection and naming the legs
// of the live paths that end where it does, with the reasons of their own ends.

type gridSpan struct {
	start, end int
	reason     subgraph.EndReason
}

func meetGrid(a, b []gridSpan) []gridSpan {
	var out []gridSpan
	for _, x := range a {
		for _, y := range b {
			if lo, hi := max(x.start, y.start), min(x.end, y.end); lo < hi {
				out = append(out, gridSpan{start: lo, end: hi})
			}
		}
	}
	slices.SortFunc(out, func(a, b gridSpan) int { return a.start - b.start })
	return out
}

type derivedDraw struct {
	width  int
	legs   []subgraph.Key
	pieces [][]gridSpan
	paths  [][]int
	covers [][]gridSpan
	union  []gridSpan
}

func drawDerived(t *rapid.T) derivedDraw {
	d := derivedDraw{width: rapid.IntRange(4, 30).Draw(t, "width")}
	n := rapid.IntRange(2, 4).Draw(t, "legs")
	for i := 1; i <= n; i++ {
		d.legs = append(d.legs, sched(pod(i), node(1)))
		k := rapid.IntRange(1, 2).Draw(t, "pieces")
		bounds := rapid.SliceOfNDistinct(rapid.IntRange(0, d.width), 2*k, 2*k, func(i int) int { return i }).Draw(t, "bounds")
		slices.Sort(bounds)
		var ps []gridSpan
		for j := 0; j < k; j++ {
			// Pieces never touch.
			if j > 0 && bounds[2*j] == bounds[2*j-1] {
				continue
			}
			ps = append(ps, gridSpan{start: bounds[2*j], end: bounds[2*j+1], reason: drawEndReason(t, subgraph.SubjectAsserted)})
		}
		d.pieces = append(d.pieces, ps)
	}
	np := rapid.IntRange(1, 2).Draw(t, "paths")
	for i := 0; i < np; i++ {
		size := rapid.IntRange(1, n).Draw(t, "path size")
		d.paths = append(d.paths, rapid.SliceOfNDistinct(rapid.IntRange(0, n-1), size, size, func(i int) int { return i }).Draw(t, "path"))
	}
	var all []gridSpan
	for _, p := range d.paths {
		cover := []gridSpan{{start: 0, end: d.width}}
		for _, l := range p {
			cover = meetGrid(cover, d.pieces[l])
		}
		d.covers = append(d.covers, cover)
		all = append(all, cover...)
	}
	slices.SortFunc(all, func(a, b gridSpan) int { return a.start - b.start })
	for _, s := range all {
		if n := len(d.union); n > 0 && s.start <= d.union[n-1].end {
			d.union[n-1].end = max(d.union[n-1].end, s.end)
			continue
		}
		d.union = append(d.union, s)
	}
	return d
}

// source is the input the draw makes: the legs, and the derived element with
// its ends.
func (d derivedDraw) input(extend int) subgraph.Input {
	var srcs []subgraph.Source
	for i, k := range d.legs {
		var spans []span
		for _, p := range d.pieces[i] {
			spans = append(spans, span{start: p.start, end: p.end, reason: p.reason})
		}
		srcs = append(srcs, mk(d.width, k, spans...))
	}
	key := subgraph.Key{Kind: subgraph.KindDerived, A: pod(900), B: pod(901), Rule: subgraph.RulePairSharedNode}
	var spans []span
	var ends [][]subgraph.LegEnd
	for i, u := range d.union {
		end := u.end
		if i == extend {
			end++
		}
		sp := span{start: u.start, end: end, reason: subgraph.EndReasonReachEnded}
		spans = append(spans, sp)
		var es []subgraph.LegEnd
		if u.end < d.width {
			for pi, p := range d.paths {
				live := slices.ContainsFunc(d.covers[pi], func(c gridSpan) bool { return c.end == u.end })
				if !live {
					continue
				}
				for li, l := range p {
					for _, lp := range d.pieces[l] {
						if lp.end == u.end {
							es = append(es, subgraph.LegEnd{Path: pi, Leg: li, Reason: lp.reason})
						}
					}
				}
			}
		}
		ends = append(ends, es)
	}
	var paths [][]subgraph.Key
	for _, p := range d.paths {
		var ks []subgraph.Key
		for _, l := range p {
			ks = append(ks, d.legs[l])
		}
		paths = append(paths, ks)
	}
	if len(spans) > 0 {
		srcs = append(srcs, withPathW(d.width, key, paths, ends, spans...))
	}
	return input(d.width, srcs...)
}

func withPathW(width int, key subgraph.Key, paths [][]subgraph.Key, ends [][]subgraph.LegEnd, spans ...span) subgraph.Source {
	s := mk(width, key, spans...)
	s.Paths = paths
	for i := range ends {
		s.Pieces[i].Ends = ends[i]
	}
	return s
}

// A derived element made of its paths' legs is built, and the oracle and the
// bounds hold: it is present exactly while some path holds all its legs.
func TestPropertyDerivedPaths(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn elements held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		d := drawDerived(t)
		in := d.input(-1)
		sg := build(t, in)
		check(t, in, sg)
		seen["inputs"]++
		seen["derived pieces"] += len(d.union)
		seen["paths"] += len(d.paths)
		for _, u := range d.union {
			if u.end < d.width {
				seen["derived ends inside"]++
			}
		}
		// The independent check: while the derived element is present, some
		// path of the draw holds every leg, from the legs' own pieces.
		held := func(x int) bool {
			for _, p := range d.paths {
				all := true
				for _, l := range p {
					all = all && slices.ContainsFunc(d.pieces[l], func(s gridSpan) bool { return s.start <= x && x < s.end })
				}
				if all {
					return true
				}
			}
			return false
		}
		for x := 0; x < d.width; x++ {
			rel := uint64(x) * uint64(1e9)
			got := false
			for _, id := range forward(t, sg, rel) {
				got = got || sg.Elements[id].Key.Kind == subgraph.KindDerived
			}
			if got != held(x) {
				t.Fatalf("at %d s the derived element is present=%t, the legs say %t", x, got, held(x))
			}
		}
	})
}

// A derived piece that runs one second past the time its paths hold is refused.
func TestPropertyDerivedPieceBeyondItsLegsIsRefused(t *testing.T) {
	refused := 0
	rapid.Check(t, func(t *rapid.T) {
		d := drawDerived(t)
		if len(d.union) == 0 {
			t.Skip("no derived piece")
		}
		i := rapid.IntRange(0, len(d.union)-1).Draw(t, "piece")
		u := d.union[i]
		// Extending the end by a second must stay inside the window and must not
		// touch the next piece, or the draw is not a case of this property.
		if u.end >= d.width || (i+1 < len(d.union) && d.union[i+1].start <= u.end+1) {
			t.Skip("cannot extend")
		}
		_, err := subgraph.Build(d.input(i))
		if err == nil {
			t.Fatalf("a piece [%d, %d) one second past its paths was built", u.start, u.end+1)
		}
		refused++
	})
	if refused == 0 {
		t.Fatal("no case was drawn")
	}
}
