package subgraph_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// The generator draws a pool of keys of every kind, a window of up to 40 steps
// of a millisecond, and for each drawn key up to three pieces whose boundaries
// fall on the steps, an interior boundary about one in four a nanosecond off
// its step. Pieces never touch, since boundaries are drawn distinct and in
// order. A few entities are quarantined and a few edges uncascaded.

func keyPool() []subgraph.Key {
	var pool []subgraph.Key
	for i := 1; i <= 8; i++ {
		pool = append(pool, ent(pod(i)))
	}
	for i := 1; i <= 3; i++ {
		pool = append(pool, ent(node(i)), ent(host(i)))
	}
	for i := 1; i <= 8; i++ {
		pool = append(pool, sched(pod(i), node(1+i%3)))
	}
	for i := 1; i <= 3; i++ {
		pool = append(pool, runsOn(node(i), host(i)), subgraph.EdgeKey(host(i), host(1+i%3), catalog.SameAs))
		pool = append(pool, derived("pair.shared_node", fmt.Sprint(i)), derived("blast_radius.reach", fmt.Sprint(i)))
	}
	return pool
}

func subjectOf(k subgraph.Key) subgraph.Subject {
	switch {
	case k.Kind == subgraph.KindDerived, k.Relation == catalog.SameAs:
		return subgraph.SubjectDerived
	}
	return subgraph.SubjectAsserted
}

var step = time.Millisecond

func drawEndReason(t *rapid.T, s subgraph.Subject) subgraph.EndReason {
	rs := []subgraph.EndReason{
		subgraph.EndReasonProducer, subgraph.EndReasonLivenessExpiry, subgraph.EndReasonCascade,
		subgraph.EndReasonRetention, subgraph.EndReasonOperator,
	}
	if s == subgraph.SubjectDerived {
		rs = append(rs, subgraph.EndReasonUnattributed, subgraph.EndReasonReachEnded)
	}
	return rapid.SampledFrom(rs).Draw(t, "end reason")
}

// drawSource draws the pieces of one key in a window of width steps.
func drawSource(t *rapid.T, key subgraph.Key, width int) subgraph.Source {
	subject := subjectOf(key)
	k := rapid.IntRange(0, 3).Draw(t, "pieces")
	bounds := rapid.SliceOfNDistinct(rapid.IntRange(0, width), 2*k, 2*k, func(i int) int { return i }).Draw(t, "boundaries")
	slices.Sort(bounds)
	off := func(b int) time.Duration {
		d := time.Duration(b) * step
		if b > 0 && b < width && rapid.IntRange(0, 3).Draw(t, "shift") == 0 {
			d += time.Duration(rapid.IntRange(-1, 1).Draw(t, "ns"))
		}
		return d
	}
	s := subgraph.Source{Key: key}
	for i := 0; i < k; i++ {
		p := subgraph.SourcePiece{Start: base.Add(off(bounds[2*i])), End: base.Add(off(bounds[2*i+1]))}
		p.Before = bounds[2*i] == 0 && rapid.Bool().Draw(t, "before")
		if bounds[2*i+1] == width {
			p.End = base.Add(time.Duration(width) * step)
		} else {
			p.EndReason = drawEndReason(t, subject)
		}
		for _, name := range []lifecycle.Producer{"a", "b", "c"} {
			if rapid.Bool().Draw(t, "producer") {
				p.Producers = append(p.Producers, name)
			}
		}
		s.Pieces = append(s.Pieces, p)
	}
	s.Class, s.Transitions = classOf(s.Pieces, base, base.Add(time.Duration(width)*step))
	return s
}

func drawInput(t *rapid.T) subgraph.Input {
	width := rapid.IntRange(1, 40).Draw(t, "width")
	pool := keyPool()
	n := rapid.IntRange(0, len(pool)).Draw(t, "elements")
	keys := rapid.SliceOfNDistinct(rapid.SampledFrom(pool), n, n, func(k subgraph.Key) subgraph.Key { return k }).Draw(t, "keys")
	in := subgraph.Input{Token: rapid.Uint64().Draw(t, "token"), From: base, To: base.Add(time.Duration(width) * step)}
	for _, k := range keys {
		switch {
		case k.Kind == subgraph.KindEntity && rapid.IntRange(0, 9).Draw(t, "quarantine") == 0:
			in.Sources = append(in.Sources, subgraph.Source{Key: k, Quarantine: &store.QuarantineError{Entity: k.A, Layer: catalog.L2}})
			continue
		}
		s := drawSource(t, k, width)
		if k.Kind == subgraph.KindEdge && k.Relation != catalog.SameAs && len(s.Pieces) > 0 && rapid.IntRange(0, 5).Draw(t, "uncascade") == 0 {
			s.Uncascaded = &store.QuarantineError{Entity: k.B, Layer: catalog.L1}
		}
		in.Sources = append(in.Sources, s)
	}
	in.Sources = rapid.Permutation(in.Sources).Draw(t, "order of sources")
	return in
}

// The oracle, the structure and both bounds hold for every drawn input.
func TestPropertyOracleAndBounds(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn subgraphs held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		in := drawInput(t)
		sg := build(t, in)
		_, e, _ := check(t, in, sg)
		seen["inputs"]++
		seen["deltas"] += e
		seen["keyframes after From"] += len(sg.Keyframes) - 1
		seen["quarantined entities"] += len(sg.Quarantine.Entities)
		seen["uncascaded edges"] += len(sg.Quarantine.Edges)
		for _, kf := range sg.Keyframes[1:] {
			if len(kf.Elements) == 0 {
				seen["empty keyframes"]++
			}
		}
		for _, el := range sg.Elements {
			seen["class "+el.Class.String()]++
			seen[el.Subject.String()+" elements"]++
		}
		for _, d := range sg.Deltas {
			if !d.After.Present {
				seen["disappearance by "+d.After.Reason.String()]++
			}
		}
	})
}

// Build is a function of its input: the order of the sources changes nothing.
func TestPropertyBuildIgnoresInputOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		in := drawInput(t)
		a := build(t, in)
		in.Sources = rapid.Permutation(slices.Clone(in.Sources)).Draw(t, "order")
		b := build(t, in)
		if fmt.Sprintf("%+v", a) != fmt.Sprintf("%+v", b) {
			t.Fatalf("the order of the sources changed the subgraph:\n%+v\n%+v", a, b)
		}
	})
}

// A churn property: a constant size N, and a draw of how many instants swap one
// element for another. The entries never pass N0 + 2E.
func TestPropertyChurn(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 12).Draw(t, "N")
		m := rapid.IntRange(1, 40).Draw(t, "instants")
		w := m + 2
		var srcs []subgraph.Source
		for j := range m + n {
			start, end := max(0, j-n+1), j+1
			if end > m {
				end = -1
			}
			srcs = append(srcs, mk(w, podN(j+1), sp(start, end)))
		}
		in := input(w, srcs...)
		sg := build(t, in)
		entries, e, n0 := check(t, in, sg)
		if entries > n0+2*e {
			t.Fatalf("entries %d > %d", entries, n0+2*e)
		}
	})
}
