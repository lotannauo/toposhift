package main

import (
	"cmp"
	"path/filepath"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The two streams a run may write besides the coalesced one, for the same
// cadence and the same samples:
//
//   - S, the every-sample stream, is every assertion the reduction hands to the
//     coalescer, unchanged and in the order it was made, with no coalescer and no
//     flush: what a store would hold if nothing were coalesced.
//   - I, the interval stream, is one record per maximal interval of a pod, a node
//     or a pod-on-node edge, which is the fewest records that say the same
//     existence. It is a derived lower bound, not what any producer writes.
//
// They are named "arms" because each is one arm of a comparison of what the same
// samples cost to store. The coalesced stream is the third arm and is unchanged by
// the other two: a run with no arms writes what it always wrote.

// armPath is the file of the stream of a cadence: placement_c60_s.parquet.
func armPath(outDir string, c *cadence, arm string) string {
	return filepath.Join(outDir, "placement_"+c.spec.name+"_"+arm+".parquet")
}

// armName is the name a stream of a cadence has in the manifest: placement_c60_s.
func armName(c *cadence, arm string) string {
	return "placement_" + c.spec.name + "_" + arm
}

// The classes of subject in an interval stream, in the order they are written
// among intervals that start together.
const (
	classNode = iota
	classPod
	classEdge
)

// ivRef points at one interval of one of the three lists.
type ivRef struct {
	start int32
	class uint8
	key   uint64
	i     int32 // the index in the class's list
}

// intervalStream writes the I stream of a cadence from its finished intervals:
// one Observe per interval, layer L2, at the interval's first sample, with the
// cadence's TTL, and Through set to the interval's last sample if it has more than
// one. The record's existence is then [start, last + TTL), which is the tracker's
// definition of the interval. The records are in ascending event time; those that
// start together are ordered nodes, pods, edges, and then by key.
func (r *placeRun) intervalStream(c *cadence, path string, maxBytes int64, pods, nodes, edges []interval) (*sink, error) {
	lists := [3][]interval{classNode: nodes, classPod: pods, classEdge: edges}
	refs := make([]ivRef, 0, len(nodes)+len(pods)+len(edges))
	for class, list := range lists {
		for i, iv := range list {
			refs = append(refs, ivRef{start: iv.start, class: uint8(class), key: iv.key, i: int32(i)})
		}
	}
	slices.SortFunc(refs, func(a, b ivRef) int {
		return cmp.Or(cmp.Compare(a.start, b.start), cmp.Compare(a.class, b.class), cmp.Compare(a.key, b.key))
	})
	s, err := newPlainSink(armName(c, "i"), path, maxBytes, false)
	if err != nil {
		return nil, err
	}
	ttl := c.spec.ttl()
	for _, ref := range refs {
		iv := lists[ref.class][ref.i]
		rec := store.Record{
			Layer: catalog.L2, Producer: placementProducer, EventTime: r.at(iv.start), Kind: lifecycle.Observe, TTL: ttl,
			EventTimeBasis: store.BasisProducerEvent,
		}
		if iv.last > iv.start {
			rec.Through = r.at(iv.last)
			if r.mut == mutIntervalThroughDeadline {
				rec.Through = rec.Through.Add(ttl)
			}
		}
		switch ref.class {
		case classNode:
			rec.Subject = store.EntitySubject(r.nodeFP.get(uint32(iv.key)))
		case classPod:
			rec.Subject = store.EntitySubject(r.podFP.get(uint32(iv.key)))
			rec.Payload = r.payload(iv.tag)
		default:
			p, n := unpair(iv.key)
			rec.Subject = store.EdgeSubject(r.podFP.get(p), r.nodeFP.get(n), catalog.ScheduledOn)
		}
		if err := s.assert(rec); err != nil {
			return nil, err
		}
	}
	return s, nil
}
