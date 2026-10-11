package main

import (
	"context"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// liar is a store that answers the opposite for one entity.
type liar struct {
	store.Store
	flip identity.Fingerprint
}

func (l liar) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	a, err := l.Store.Alive(ctx, fp, t, s)
	if fp == l.flip {
		a = !a
	}
	return a, err
}

func twoMemStores(t *testing.T) (*memstore.Store, *memstore.Store) {
	t.Helper()
	a, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

// If one store is wrong the self-check sees it, and if both answer alike it does not.
func TestStoresAreComparedWithEachOther(t *testing.T) {
	pods, nodes := loadPods(t), loadNodes(t)
	mem, other := twoMemStores(t)

	p, err := newPass("A", false, mem, other)
	if err != nil {
		t.Fatal(err)
	}
	pa, err := queryPods(context.Background(), p, pods, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Disagree.Count != 0 {
		t.Fatalf("two empty stores disagree: %v", pa.Disagree)
	}

	fpA, err := fingerprintOf(p.resolver, catalog.K8sPod, catalog.K8sPodUID, uidA)
	if err != nil {
		t.Fatal(err)
	}
	p.peb = liar{Store: other, flip: fpA}
	pa, err = queryPods(context.Background(), p, pods, nodes)
	if err != nil {
		t.Fatal(err)
	}
	// Pod a is asked about at the four samples the API was up for.
	if pa.Disagree.Count != 4 {
		t.Errorf("%d disagreements, want 4 (one for each sample the pod is asked about)", pa.Disagree.Count)
	}

	// The failure reaches the report as a failed self-check.
	pr := &passReport{Name: "A"}
	pr.Disagree.Pods = pa.Disagree
	if got := selfChecks(pr); len(got) != 1 {
		t.Errorf("self-checks failed: %v", got)
	}
	if got := selfChecks(&passReport{Name: "A"}); len(got) != 0 {
		t.Errorf("a pass with no disagreement fails a self-check: %v", got)
	}
}

func TestStoresAreComparedOnNodesToo(t *testing.T) {
	nodes := loadNodes(t)
	mem, other := twoMemStores(t)
	p, err := newPass("A", false, mem, other)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fingerprintOf(p.resolver, catalog.K8sNode, catalog.K8sNodeUID, n1)
	if err != nil {
		t.Fatal(err)
	}
	p.peb = liar{Store: other, flip: fp}
	na, err := queryNodes(context.Background(), p, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if na.Disagree.Count != 4 { // five node samples, one unavailable
		t.Errorf("%d disagreements, want 4", na.Disagree.Count)
	}
}

func TestEdgeCodes(t *testing.T) {
	r := identity.NewResolver(catalog.Default())
	fp := func(uid string) identity.Fingerprint {
		f, err := fingerprintOf(r, catalog.K8sNode, catalog.K8sNodeUID, uid)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	a, b := fp(n1), fp(n2)
	sched := func(p identity.Fingerprint) store.Neighbor {
		return store.Neighbor{Peer: p, Relation: catalog.ScheduledOn}
	}
	partOf := store.Neighbor{Peer: a, Relation: catalog.PartOf}
	var none identity.Fingerprint
	for _, c := range []struct {
		name string
		ns   []store.Neighbor
		want identity.Fingerprint
		code edgeCode
	}{
		{"no edge, no node named", nil, none, edgeNone},
		{"no edge, a node named", nil, a, edgeMissing},
		{"another relation is not an edge to a node", []store.Neighbor{partOf}, a, edgeMissing},
		{"the named node", []store.Neighbor{sched(a)}, a, edgeExact},
		{"the named node and a second", []store.Neighbor{sched(a), sched(b)}, a, edgeExactPlus},
		{"another node", []store.Neighbor{sched(b)}, a, edgeWrong},
		{"an edge where no node is named", []store.Neighbor{sched(a)}, none, edgeUnexpected},
	} {
		if got := codeOf(c.ns, c.want); got != c.code {
			t.Errorf("%s: code %d, want %d", c.name, got, c.code)
		}
	}
}

// A part_of edge of a container reaches the pod: it is an edge of the pod, found by reading
// the pod's edges in the reverse direction.
func TestPodEdgesIncludeTheContainersThatReachIt(t *testing.T) {
	pods, nodes := loadPods(t), loadNodes(t)
	mem, other := twoMemStores(t)
	p, err := newPass("A", false, mem, other)
	if err != nil {
		t.Fatal(err)
	}
	pod := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, uidA)
	ctr := fpOf(t, p.resolver, catalog.Container, catalog.ContainerID, "c1")
	rec := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(ctr, pod, catalog.PartOf), Producer: producerObjects,
		EventTime: pods.Times[0], Seq: 1, Kind: lifecycle.Observe, TTL: time.Minute, Payload: []byte("{}"),
	}
	for _, s := range []store.Store{mem, other} {
		if err := s.Write(context.Background(), []store.Record{rec}); err != nil {
			t.Fatal(err)
		}
	}
	pa, err := queryPods(context.Background(), p, pods, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Disagree.Count != 0 {
		t.Errorf("disagreements: %v", pa.Disagree)
	}
	if !pa.AnyEdge[0][0] || pa.AnyEdge[0][3] || pa.Alive[0][0] {
		t.Errorf("pod a: edge at the first sample %v (want true), at the fourth %v (want false, the edge lapsed after a minute), alive %v (want false)", pa.AnyEdge[0][0], pa.AnyEdge[0][3], pa.Alive[0][0])
	}
	if pa.Edge[0][0] != edgeMissing && pa.Edge[0][0] != edgeNone {
		t.Errorf("a part_of edge is no scheduled_on edge: code %d", pa.Edge[0][0])
	}
}
