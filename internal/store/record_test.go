package store_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// fp resolves one fingerprint of the given type, failing the test if the
// identity does not resolve.
func fp(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func TestValidate(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	good := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 1, Kind: lifecycle.Observe,
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("a good record was rejected: %v", err)
	}

	edge := func(f func(*store.Record)) store.Record { r := good; f(&r); return r }
	invalid := map[string]store.Record{
		"zero subject":      edge(func(r *store.Record) { r.Subject = store.Subject{} }),
		"edge without peer": edge(func(r *store.Record) { r.Subject.B = identity.Fingerprint{} }),
		"edge without rel":  edge(func(r *store.Record) { r.Subject.Relation = "" }),
		"entity with peer":  edge(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Subject.B = node }),
		"entity with rel":   edge(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Subject.Relation = catalog.PartOf }),
		"empty producer":    edge(func(r *store.Record) { r.Producer = "" }),
		"before 1970":       edge(func(r *store.Record) { r.EventTime = time.Unix(-1, 0) }),
		"after 2262":        edge(func(r *store.Record) { r.EventTime = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC) }),
		"through before":    edge(func(r *store.Record) { r.Through = r.EventTime.Add(-time.Second) }),
		"unset kind":        edge(func(r *store.Record) { r.Kind = 0 }),
		"unknown kind":      edge(func(r *store.Record) { r.Kind = 9 }),
		"entity in a layer other than its type's": edge(func(r *store.Record) {
			r.Subject = store.EntitySubject(pod) // a pod lives in L2
			r.Layer = catalog.L1
		}),
		"negative TTL":       edge(func(r *store.Record) { r.TTL = -time.Second }),
		"unset layer":        edge(func(r *store.Record) { r.Layer = 0 }),
		"layer out of range": edge(func(r *store.Record) { r.Layer = catalog.L3 + 1 }),
		"delete with TTL":    edge(func(r *store.Record) { r.Kind, r.TTL = lifecycle.Delete, time.Minute }),
		"delete with through": edge(func(r *store.Record) {
			r.Kind, r.Through = lifecycle.Delete, r.EventTime.Add(time.Minute)
		}),
		"delete with payload": edge(func(r *store.Record) { r.Kind, r.Payload = lifecycle.Delete, []byte("x") }),
		"through after 2262": edge(func(r *store.Record) {
			r.Through = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC)
		}),
		"deadline after 2262": edge(func(r *store.Record) {
			r.EventTime, r.TTL = store.MaxEventTime.Add(-time.Second), time.Minute
		}),
		"run deadline after 2262": edge(func(r *store.Record) {
			r.EventTime = store.MaxEventTime.Add(-time.Hour)
			r.Through, r.TTL = store.MaxEventTime.Add(-time.Second), time.Minute
		}),
	}
	for name, r := range invalid {
		if err := r.Validate(); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	entity := good
	entity.Subject = store.EntitySubject(pod)
	atEnd := good
	atEnd.EventTime, atEnd.TTL = store.MaxEventTime.Add(-time.Minute), time.Minute
	atMin, atMax := good, good
	atMin.EventTime, atMax.EventTime = store.MinEventTime, store.MaxEventTime
	valid := map[string]store.Record{
		"an entity in the layer its type belongs to": entity,
		"a deadline exactly at the end of the range": atEnd,
		"the first representable event time":         atMin,
		"the last representable event time":          atMax,
	}
	for name, r := range valid {
		if err := r.Validate(); err != nil {
			t.Errorf("%s was rejected: %v", name, err)
		}
	}

	if store.MaxEventTime.UnixNano() != math.MaxInt64 {
		t.Errorf("MaxEventTime is %d ns, want the largest int64", store.MaxEventTime.UnixNano())
	}
}

// TestValidateWrapsTheFoldError checks that a record the lifecycle
// specification rejects reports both errors, so a caller can match either.
func TestValidateWrapsTheFoldError(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn),
		EventTime: time.Unix(10, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, // no producer
	}
	err := r.Validate()
	if !errors.Is(err, store.ErrInvalid) || !errors.Is(err, lifecycle.ErrInvalid) {
		t.Errorf("err = %v, want both store.ErrInvalid and lifecycle.ErrInvalid", err)
	}
}

func TestAssertionCarriesThePayloadAsADescription(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	obs := store.Record{
		Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: time.Unix(10, 0).UTC(), Seq: 3,
		Kind: lifecycle.Observe, TTL: time.Minute, Through: time.Unix(20, 0).UTC(), Payload: []byte("abc"),
	}
	a := obs.Assertion()
	if a.Producer != "k8s" || a.Seq != 3 || a.TTL != time.Minute || !a.Through.Equal(obs.Through) || len(a.Attrs) != 1 {
		t.Errorf("assertion = %+v", a)
	}
	if !a.EventTime.Equal(obs.EventTime) {
		t.Errorf("assertion event time = %s, want the record's %s", a.EventTime, obs.EventTime)
	}
	if a.Kind != lifecycle.Observe {
		t.Errorf("assertion kind = %v, want Observe", a.Kind)
	}

	// Equal payloads give equal descriptions, which is what lets a run of
	// refreshes coalesce into one assertion that extends through the last.
	later := obs
	later.Seq, later.EventTime, later.Through = 4, time.Unix(20, 0).UTC(), time.Time{}
	obs.Through = time.Time{}
	first, second := obs.Assertion(), later.Assertion()
	got := lifecycle.Coalesce([]lifecycle.Assertion{first, second})
	if len(got) != 1 || !got[0].EventTime.Equal(first.EventTime) || !got[0].Through.Equal(second.EventTime) {
		t.Errorf("two identical refreshes coalesced to %+v, want one run from 10s through 20s", got)
	}
	changed := later
	changed.Payload = []byte("different")
	if got := lifecycle.Coalesce([]lifecycle.Assertion{first, changed.Assertion()}); len(got) != 2 {
		t.Errorf("a changed payload coalesced to %d assertions, want 2", len(got))
	}

	del := obs
	del.Kind, del.TTL, del.Through, del.Payload = lifecycle.Delete, 0, time.Time{}, nil
	if got := del.Assertion(); len(got.Attrs) != 0 {
		t.Errorf("a delete carries attributes: %+v", got)
	}
}

func TestSubjectConstructors(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	if got, want := store.EntitySubject(pod), (store.Subject{Kind: store.SubjectEntity, A: pod}); got != want {
		t.Errorf("EntitySubject = %+v, want %+v", got, want)
	}
	if got, want := store.EdgeSubject(pod, node, catalog.ScheduledOn), (store.Subject{Kind: store.SubjectEdge, A: pod, B: node, Relation: catalog.ScheduledOn}); got != want {
		t.Errorf("EdgeSubject = %+v, want %+v", got, want)
	}
}
