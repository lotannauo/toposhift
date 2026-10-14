package main

import (
	"cmp"
	"container/heap"
	"slices"
)

// The kinds of dirt: what the trace does that the model does not expect.
// They are anomalies relative to this model, kept as the producer said them.
const (
	dirtDuplicate       = "duplicate"        // identical (timestamp, instance, node) rows
	dirtDoublePlacement = "double_placement" // one instance on two nodes at one timestamp
	dirtMove            = "move"             // an instance on node A, later on node B
	dirtMSNameChange    = "msname_change"    // one instance with two msnames
)

var dirtKinds = []string{dirtDuplicate, dirtDoublePlacement, dirtMove, dirtMSNameChange}

// dirtCase is one listed anomaly. For a duplicate or a double placement A is the
// node (the second node, for a double placement), for a move A and B are the
// nodes before and after, and for an msname change they are the two msnames.
type dirtCase struct {
	kind   string
	token  string // the instance
	minute int32
	a, b   string
}

func compareCases(x, y dirtCase) int {
	return cmp.Or(
		cmp.Compare(x.token, y.token),
		cmp.Compare(x.minute, y.minute),
		cmp.Compare(x.a, y.a),
		cmp.Compare(x.b, y.b),
	)
}

// caseHeap is a max-heap by (token, minute, a, b): it holds the smallest cases seen.
type caseHeap []dirtCase

func (h caseHeap) Len() int           { return len(h) }
func (h caseHeap) Less(i, j int) bool { return compareCases(h[i], h[j]) > 0 }
func (h caseHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *caseHeap) Push(x any)        { *h = append(*h, x.(dirtCase)) }
func (h *caseHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// dirtLog counts every anomaly and lists the first limit of each kind, in the
// order of (token, minute): a deterministic choice, whatever order they were met.
type dirtLog struct {
	limit  int
	counts map[string]int64
	heaps  map[string]*caseHeap
}

func newDirtLog(limit int) *dirtLog {
	d := &dirtLog{limit: limit, counts: make(map[string]int64), heaps: make(map[string]*caseHeap)}
	for _, k := range dirtKinds {
		d.heaps[k] = &caseHeap{}
	}
	return d
}

func (d *dirtLog) add(c dirtCase) {
	d.counts[c.kind]++
	h := d.heaps[c.kind]
	if len(*h) >= d.limit {
		if compareCases(c, (*h)[0]) >= 0 {
			return
		}
		heap.Pop(h)
	}
	heap.Push(h, c)
}

// listed is the listed cases of a kind in ascending order.
func (d *dirtLog) listed(kind string) []dirtCase {
	out := slices.Clone(*d.heaps[kind])
	slices.SortFunc(out, compareCases)
	return out
}
