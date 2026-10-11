package main

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// The questions asked of the stores: is an entity alive at a sample time, and which edges
// leave it. Both stores are asked every question, and they must answer alike.

// answer is what a store says about a subject at a time.
type answer struct {
	Alive bool
	// Neighbors are the edges that leave the subject and Incoming those that reach it,
	// asked only if edges are.
	Neighbors []store.Neighbor
	Incoming  []store.Neighbor
}

// ask puts the question to one store. Neighbors are asked only if withEdges.
func ask(ctx context.Context, s store.Store, fp identity.Fingerprint, t time.Time, withEdges bool) (answer, error) {
	sc := store.Current(catalog.L2)
	alive, err := s.Alive(ctx, fp, t, sc)
	if err != nil {
		return answer{}, fmt.Errorf("Alive(%s, %s): %w", fp, t.Format(time.RFC3339), err)
	}
	a := answer{Alive: alive}
	if withEdges {
		if a.Neighbors, err = s.Neighbors(ctx, fp, store.Forward, t, sc); err != nil {
			return answer{}, fmt.Errorf("Neighbors(%s, %s): %w", fp, t.Format(time.RFC3339), err)
		}
		if a.Incoming, err = s.Neighbors(ctx, fp, store.Reverse, t, sc); err != nil {
			return answer{}, fmt.Errorf("Neighbors(%s, reverse, %s): %w", fp, t.Format(time.RFC3339), err)
		}
	}
	return a, nil
}

func (a answer) equal(b answer) bool {
	return a.Alive == b.Alive && slices.Equal(a.Neighbors, b.Neighbors) && slices.Equal(a.Incoming, b.Incoming)
}

// askBoth puts the question to the memory store and to the Pebble store and returns the
// first one's answer, and whether the second agrees.
func askBoth(ctx context.Context, first, second store.Store, fp identity.Fingerprint, t time.Time, withEdges bool) (answer, bool, error) {
	a, err := ask(ctx, first, fp, t, withEdges)
	if err != nil {
		return answer{}, false, err
	}
	b, err := ask(ctx, second, fp, t, withEdges)
	if err != nil {
		return answer{}, false, err
	}
	return a, a.equal(b), nil
}

// edgeCode says what the edges of a pod were against the node the truth names.
type edgeCode uint8

const (
	// edgeNone: no scheduled_on edge is alive, and the truth names no node.
	edgeNone edgeCode = iota
	// edgeMissing: none is alive, and the truth names a node.
	edgeMissing
	// edgeExact: one, to the node the truth names.
	edgeExact
	// edgeExactPlus: one to that node, and another to another node.
	edgeExactPlus
	// edgeWrong: one or more, none to that node.
	edgeWrong
	// edgeUnexpected: one or more, where the truth names no node (or no node UID).
	edgeUnexpected
)

// codeOf classifies the scheduled_on edges alive against the expected node, which is
// the zero fingerprint if the truth names none.
func codeOf(ns []store.Neighbor, want identity.Fingerprint) edgeCode {
	var sched, match int
	for _, n := range ns {
		if n.Relation != catalog.ScheduledOn {
			continue
		}
		sched++
		if !want.IsZero() && n.Peer == want {
			match++
		}
	}
	switch {
	case sched == 0 && want.IsZero():
		return edgeNone
	case sched == 0:
		return edgeMissing
	case want.IsZero():
		return edgeUnexpected
	case match > 0 && sched == match:
		return edgeExact
	case match > 0:
		return edgeExactPlus
	}
	return edgeWrong
}

// podAnswers are the stores' answers for every (pod, sample) pair.
type podAnswers struct {
	// Alive and Edge are indexed by the UID's index in the truth, then by sample.
	Alive [][]bool
	Edge  [][]edgeCode
	// AnyEdge is whether any edge of the pod was alive: a scheduled_on edge that leaves it,
	// or a part_of edge of a container that reaches it.
	AnyEdge [][]bool
	// Disagree are the pairs the two stores answered differently.
	Disagree violation
}

// nodeAnswers are the stores' answers for every (node, sample) pair.
type nodeAnswers struct {
	Alive    [][]bool
	Disagree violation
}

// parallelFor runs f for 0..n-1 on several goroutines and returns the first error.
func parallelFor(ctx context.Context, n int, f func(i int) error) error {
	workers := min(runtime.GOMAXPROCS(0), max(n, 1))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		next  int
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= n {
					return
				}
				if err := f(i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

// queryPods asks both stores about every pod UID of the truth at every sample, entity and
// edges, and keeps the first store's answers. The stores disagreeing is counted, not an
// error.
func queryPods(ctx context.Context, p *pass, pods *podsTruth, nodes *nodesTruth) (*podAnswers, error) {
	pa := &podAnswers{Alive: make([][]bool, len(pods.UIDs)), Edge: make([][]edgeCode, len(pods.UIDs)), AnyEdge: make([][]bool, len(pods.UIDs))}
	var mu sync.Mutex
	nodeFP := map[string]identity.Fingerprint{}
	for _, uid := range nodes.UIDs {
		fp, err := fingerprintOf(p.resolver, catalog.K8sNode, catalog.K8sNodeUID, uid)
		if err != nil {
			return nil, err
		}
		nodeFP[uid] = fp
	}
	err := parallelFor(ctx, len(pods.UIDs), func(i int) error {
		pt := pods.Pods[pods.UIDs[i]]
		fp, err := fingerprintOf(p.resolver, catalog.K8sPod, catalog.K8sPodUID, pt.UID)
		if err != nil {
			return err
		}
		alive := make([]bool, len(pods.Times))
		edge := make([]edgeCode, len(pods.Times))
		anyEdge := make([]bool, len(pods.Times))
		rows := map[int]podRow{}
		for _, r := range pt.Rows {
			rows[r.Sample] = r
		}
		for s, t := range pods.Times {
			if pods.Unavail[s] {
				continue
			}
			var want identity.Fingerprint
			if row, ok := rows[s]; ok && row.Node != "" {
				want = nodeFP[nodes.nodeUIDAt(row.Node, t)]
			}
			a, agree, err := askBoth(ctx, p.mem, p.peb, fp, t, true)
			if err != nil {
				return err
			}
			if !agree {
				mu.Lock()
				pa.Disagree.add("pod %s (%s) at %s", pt.UID, pt.Name, t.Format(time.RFC3339))
				mu.Unlock()
			}
			alive[s] = a.Alive
			edge[s] = codeOf(a.Neighbors, want)
			anyEdge[s] = hasRelation(a.Neighbors, catalog.ScheduledOn) || hasRelation(a.Incoming, catalog.PartOf)
		}
		pa.Alive[i], pa.Edge[i], pa.AnyEdge[i] = alive, edge, anyEdge
		return nil
	})
	return pa, err
}

// queryNodes asks both stores about every node UID of the truth at every node sample.
func queryNodes(ctx context.Context, p *pass, nodes *nodesTruth) (*nodeAnswers, error) {
	na := &nodeAnswers{Alive: make([][]bool, len(nodes.UIDs))}
	var mu sync.Mutex
	err := parallelFor(ctx, len(nodes.UIDs), func(i int) error {
		nt := nodes.Nodes[nodes.UIDs[i]]
		fp, err := fingerprintOf(p.resolver, catalog.K8sNode, catalog.K8sNodeUID, nt.UID)
		if err != nil {
			return err
		}
		alive := make([]bool, len(nodes.Times))
		for s, t := range nodes.Times {
			if nodes.Unavail[s] {
				continue
			}
			a, agree, err := askBoth(ctx, p.mem, p.peb, fp, t, false)
			if err != nil {
				return err
			}
			if !agree {
				mu.Lock()
				na.Disagree.add("node %s (%s) at %s", nt.UID, nt.Name, t.Format(time.RFC3339))
				mu.Unlock()
			}
			alive[s] = a.Alive
		}
		na.Alive[i] = alive
		return nil
	})
	return na, err
}

func fingerprintOf(r *identity.Resolver, typ catalog.EntityType, key catalog.AttributeKey, v string) (identity.Fingerprint, error) {
	id, err := r.Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("%s %q: %w", typ, v, err)
	}
	return id.Fingerprint(), nil
}

// hasRelation says whether any of the neighbors is of the relation.
func hasRelation(ns []store.Neighbor, rel catalog.RelationType) bool {
	for _, n := range ns {
		if n.Relation == rel {
			return true
		}
	}
	return false
}
