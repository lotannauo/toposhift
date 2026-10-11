package main

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func observeEntity(fp identity.Fingerprint, at, through time.Time) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: producerObjects,
		EventTime: at, Through: through, Kind: lifecycle.Observe, TTL: entityTTL, Payload: []byte("{}"),
	}
}

func observeSchedule(pod, node identity.Fingerprint, at, through time.Time) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: producerObjects,
		EventTime: at, Through: through, Kind: lifecycle.Observe, TTL: entityTTL, Payload: []byte("{}"),
	}
}

// startedPass is a pass that has begun its first run.
func startedPass(t *testing.T) *pass {
	t.Helper()
	p := passForTest(t)
	if err := p.startRun(1); err != nil {
		t.Fatal(err)
	}
	return p
}

// An Observe must not lapse before the time its record describes plus its TTL: what it
// asserts, through its Through if it has one, reaches the record's time.
func TestAnObserveThatEndsBeforeItsSourceTimeLapses(t *testing.T) {
	p := startedPass(t)
	pod := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, "p1")
	st := t0.Add(time.Hour)

	p.observeRecords(srcObjects, st, []store.Record{observeEntity(pod, st, time.Time{})})
	if p.Lapsed.Count != 0 {
		t.Fatalf("an Observe timed at its source time lapses: %v", p.Lapsed)
	}
	p.observeRecords(srcObjects, st, []store.Record{observeEntity(pod, t0, st)})
	if p.Lapsed.Count != 0 {
		t.Errorf("an Observe asserted through its source time lapses: %v", p.Lapsed)
	}
	p.observeRecords(srcObjects, st, []store.Record{observeEntity(pod, st.Add(-time.Second), time.Time{})})
	if p.Lapsed.Count != 1 {
		t.Errorf("an Observe that ends a second before its source time: %d lapses, want 1", p.Lapsed.Count)
	}
	p.observeRecords(srcObjects, st, []store.Record{observeEntity(pod, t0, st.Add(-time.Minute))})
	if p.Lapsed.Count != 2 {
		t.Errorf("an Observe asserted through a minute before its source time: %d lapses, want 2", p.Lapsed.Count)
	}
	// A Delete promises nothing, and so cannot lapse.
	del := observeEntity(pod, t0, time.Time{})
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	p.observeRecords(srcObjects, st, []store.Record{del})
	if p.Lapsed.Count != 2 {
		t.Errorf("a Delete lapses: %d", p.Lapsed.Count)
	}
}

// A pending scheduled_on edge is resolved by the record of its node, and is asserted
// through the latest record of its pod: the node's arrival says nothing about the pod.
func TestAPendingEdgeIsHeldToItsPodsLastRecordNotItsNodes(t *testing.T) {
	p := startedPass(t)
	pod := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, "p1")
	node := fpOf(t, p.resolver, catalog.K8sNode, catalog.K8sNodeUID, n1)
	podAt, nodeAt := t0.Add(time.Hour), t0.Add(2*time.Hour)

	// The pod's record, whose edge is pending, and later the node's, which resolves it.
	p.observeRecords(srcObjects, podAt, []store.Record{observeEntity(pod, podAt, time.Time{})})
	resolve := func(through time.Time) {
		p.observeRecords(srcObjects, nodeAt, []store.Record{
			observeEntity(node, nodeAt, time.Time{}),
			observeSchedule(pod, node, t0, through),
		})
	}
	resolve(podAt)
	if p.Lapsed.Count != 0 {
		t.Fatalf("an edge asserted through the pod's last record lapses: %v", p.Lapsed)
	}
	resolve(podAt.Add(-time.Minute))
	if p.Lapsed.Count != 1 {
		t.Errorf("an edge asserted through a minute before the pod's last record: %d lapses, want 1", p.Lapsed.Count)
	}
	// The other translator has not seen the pod, so its edge is held to nothing.
	p.observeRecords(srcCluster, nodeAt, []store.Record{observeSchedule(pod, node, t0, time.Time{})})
	if p.Lapsed.Count != 1 {
		t.Errorf("an edge of the other translator, which has not seen the pod: %d lapses, want still 1", p.Lapsed.Count)
	}
}

// What the store is left with of a pod is its last asserted instant plus the TTL: for an
// extended run, the extension's Through, not the event time it is keyed at.
func TestPodDeadlineFollowsThroughOfAnExtendedRun(t *testing.T) {
	p := startedPass(t)
	pod := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, "p1")
	p.noteDeadlines([]store.Record{observeEntity(pod, t0, time.Time{})})
	if got, want := p.podDeadline[pod], t0.Add(entityTTL); !got.Equal(want) {
		t.Fatalf("deadline %s, want %s", got, want)
	}
	// An extension of the run, keyed at its start and asserted through ten minutes in.
	p.noteDeadlines([]store.Record{observeEntity(pod, t0, t0.Add(10*time.Minute))})
	if got, want := p.podDeadline[pod], t0.Add(10*time.Minute+entityTTL); !got.Equal(want) {
		t.Errorf("deadline %s after an extension through 10 minutes, want %s", got, want)
	}
	// An older record does not bring the deadline back.
	p.noteDeadlines([]store.Record{observeEntity(pod, t0, time.Time{})})
	if got, want := p.podDeadline[pod], t0.Add(10*time.Minute+entityTTL); !got.Equal(want) {
		t.Errorf("deadline %s after an older record, want %s", got, want)
	}
	// A Delete and an edge are not a pod's deadline.
	other := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, "p2")
	del := observeEntity(other, t0, time.Time{})
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	p.noteDeadlines([]store.Record{del, observeSchedule(other, fpOf(t, p.resolver, catalog.K8sNode, catalog.K8sNodeUID, n1), t0, time.Time{})})
	if _, ok := p.podDeadline[other]; ok {
		t.Error("a Delete or an edge sets a pod's deadline")
	}
}
