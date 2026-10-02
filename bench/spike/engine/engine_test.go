package engine_test

import (
	"errors"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

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
	good := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 1, Kind: lifecycle.Observe,
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("a good record was rejected: %v", err)
	}

	edge := func(f func(*engine.Record)) engine.Record { r := good; f(&r); return r }
	for name, r := range map[string]engine.Record{
		"zero subject":      edge(func(r *engine.Record) { r.Subject = engine.Subject{} }),
		"edge without peer": edge(func(r *engine.Record) { r.Subject.B = identity.Fingerprint{} }),
		"edge without rel":  edge(func(r *engine.Record) { r.Subject.Relation = "" }),
		"entity with peer":  edge(func(r *engine.Record) { r.Subject = engine.EntitySubject(pod); r.Subject.B = node }),
		"entity with rel":   edge(func(r *engine.Record) { r.Subject = engine.EntitySubject(pod); r.Subject.Relation = catalog.PartOf }),
		"empty producer":    edge(func(r *engine.Record) { r.Producer = "" }),
		"before 1970":       edge(func(r *engine.Record) { r.EventTime = time.Unix(-1, 0) }),
		"after 2262":        edge(func(r *engine.Record) { r.EventTime = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC) }),
		"through before":    edge(func(r *engine.Record) { r.Through = r.EventTime.Add(-time.Second) }),
		"unset kind":        edge(func(r *engine.Record) { r.Kind = 0 }),
		"unknown kind":      edge(func(r *engine.Record) { r.Kind = 9 }),
		"entity in a layer other than its type's": edge(func(r *engine.Record) {
			r.Subject = engine.EntitySubject(pod) // a pod lives in L2
			r.Layer = catalog.L1
		}),
		"negative TTL":       edge(func(r *engine.Record) { r.TTL = -time.Second }),
		"unset layer":        edge(func(r *engine.Record) { r.Layer = 0 }),
		"layer out of range": edge(func(r *engine.Record) { r.Layer = catalog.L3 + 1 }),
		"delete with TTL":    edge(func(r *engine.Record) { r.Kind, r.TTL = lifecycle.Delete, time.Minute }),
		"delete with through": edge(func(r *engine.Record) {
			r.Kind, r.Through = lifecycle.Delete, r.EventTime.Add(time.Minute)
		}),
		"delete with payload": edge(func(r *engine.Record) { r.Kind, r.Payload = lifecycle.Delete, []byte("x") }),
		"through after 2262": edge(func(r *engine.Record) {
			r.Through = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC)
		}),
		"deadline after 2262": edge(func(r *engine.Record) {
			r.EventTime, r.TTL = engine.MaxEventTime.Add(-time.Second), time.Minute
		}),
		"run deadline after 2262": edge(func(r *engine.Record) {
			r.EventTime = engine.MaxEventTime.Add(-time.Hour)
			r.Through, r.TTL = engine.MaxEventTime.Add(-time.Second), time.Minute
		}),
	} {
		if err := r.Validate(); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	// An entity record in the layer its type belongs to is fine.
	entity := good
	entity.Subject = engine.EntitySubject(pod)
	if err := entity.Validate(); err != nil {
		t.Errorf("a pod entity in L2 was rejected: %v", err)
	}

	// A deadline exactly at the end of the range is representable.
	edgeCase := good
	edgeCase.EventTime, edgeCase.TTL = engine.MaxEventTime.Add(-time.Minute), time.Minute
	if err := edgeCase.Validate(); err != nil {
		t.Errorf("a deadline at the end of the range was rejected: %v", err)
	}

	// The representable range includes both ends.
	for _, at := range []time.Time{engine.MinEventTime, engine.MaxEventTime} {
		r := good
		r.EventTime = at
		if err := r.Validate(); err != nil {
			t.Errorf("event time %s was rejected: %v", at, err)
		}
	}
	if engine.MaxEventTime.UnixNano() != math.MaxInt64 {
		t.Errorf("MaxEventTime is %d ns, want the largest int64", engine.MaxEventTime.UnixNano())
	}
}

func TestAssertionCarriesThePayloadAsADescription(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	obs := engine.Record{
		Subject: engine.EntitySubject(pod), Producer: "k8s", EventTime: time.Unix(10, 0).UTC(), Seq: 3,
		Kind: lifecycle.Observe, TTL: time.Minute, Through: time.Unix(20, 0).UTC(), Payload: []byte("abc"),
	}
	a := obs.Assertion()
	if a.Producer != "k8s" || a.Seq != 3 || a.TTL != time.Minute || !a.Through.Equal(obs.Through) || len(a.Attrs) != 1 {
		t.Errorf("assertion = %+v", a)
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

func TestSortingIsByTypeThenHashThenRelation(t *testing.T) {
	t.Parallel()

	a, b, c := fp(t, catalog.K8sPod, catalog.K8sPodUID, "a"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "b"), fp(t, catalog.Host, catalog.HostID, "h")
	ns := []engine.Neighbor{{Peer: b, Relation: catalog.RunsOn}, {Peer: a, Relation: catalog.RunsOn}, {Peer: c, Relation: catalog.RunsOn}, {Peer: a, Relation: catalog.PartOf}}
	engine.SortNeighbors(ns)
	if ns[0].Peer != c { // "host" sorts before "k8s.pod"
		t.Errorf("first neighbor is %s, want the host", ns[0].Peer)
	}
	if !slices.IsSortedFunc(ns, func(x, y engine.Neighbor) int { return engine.CompareFingerprints(x.Peer, y.Peer) }) {
		t.Errorf("not sorted by fingerprint: %v", ns)
	}
	if ns[1].Peer == ns[2].Peer && ns[1].Relation > ns[2].Relation {
		t.Errorf("relations out of order for one peer: %v", ns)
	}
	if engine.CompareFingerprints(a, a) != 0 {
		t.Error("a fingerprint does not equal itself")
	}

	rs := []engine.Record{{EventTime: time.Unix(5, 0), Seq: 2}, {EventTime: time.Unix(5, 0), Seq: 1}, {EventTime: time.Unix(1, 0), Seq: 9}}
	engine.SortRecords(rs)
	if rs[0].Seq != 9 || rs[1].Seq != 1 || rs[2].Seq != 2 {
		t.Errorf("records sorted to %+v", rs)
	}
}

func TestDirectionString(t *testing.T) {
	t.Parallel()
	if engine.Forward.String() != "forward" || engine.Reverse.String() != "reverse" || engine.Direction(0).String() != "Direction(0)" {
		t.Error("Direction.String")
	}
}

func TestErrBeforeHorizonIsInvalid(t *testing.T) {
	t.Parallel()
	if !errors.Is(engine.ErrBeforeHorizon, engine.ErrInvalid) {
		t.Error("ErrBeforeHorizon does not wrap ErrInvalid")
	}
}

func TestScope(t *testing.T) {
	t.Parallel()

	cur := engine.Current(catalog.L2)
	if cur.Layer != catalog.L2 || cur.AsOf != engine.Latest || engine.Latest != math.MaxUint64 {
		t.Errorf("Current(L2) = %+v", cur)
	}
	if err := cur.Validate(); err != nil {
		t.Errorf("a current scope was rejected: %v", err)
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if err := (engine.Scope{Layer: l}).Validate(); err != nil {
			t.Errorf("layer %s: %v", l, err)
		}
	}
	for _, l := range []catalog.Layer{0, catalog.L3 + 1, 200} {
		if err := (engine.Scope{Layer: l, AsOf: engine.Latest}).Validate(); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("layer %d: err = %v, want ErrInvalid", l, err)
		}
	}
}

func TestNeighborsEachAsksOncePerFingerprintInOrder(t *testing.T) {
	t.Parallel()

	a, b := fp(t, catalog.K8sPod, catalog.K8sPodUID, "a"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "b")
	var asked []string
	got, err := engine.NeighborsEach([]identity.Fingerprint{b, a, b}, func(f identity.Fingerprint) ([]engine.Neighbor, error) {
		asked = append(asked, f.String())
		return []engine.Neighbor{{Peer: f, Relation: catalog.PartOf}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0][0].Peer != b || got[1][0].Peer != a || got[2][0].Peer != b {
		t.Errorf("results are not parallel to the input: %v", got)
	}
	if len(asked) != 3 {
		t.Errorf("read was called %d times, want 3", len(asked))
	}
	if empty, err := engine.NeighborsEach(nil, nil); err != nil || len(empty) != 0 {
		t.Errorf("an empty input answered %v, %v", empty, err)
	}

	boom := errors.New("boom")
	if _, err := engine.NeighborsEach([]identity.Fingerprint{a, b}, func(identity.Fingerprint) ([]engine.Neighbor, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the read's error", err)
	}
}

func TestMemRecorderKeepsCountsAndSamples(t *testing.T) {
	t.Parallel()

	var r engine.MemRecorder
	var _ engine.Recorder = &r
	var _ engine.Recorder = engine.NopRecorder{}

	if r.Counter("never") != 0 || r.Samples("never") != nil {
		t.Error("an unused recorder holds something")
	}
	r.Count("hits", 2)
	r.Count("hits", 3)
	r.Count("misses", 1)
	r.Sample("replay", 4)
	r.Sample("replay", 9)
	if r.Counter("hits") != 5 || r.Counter("misses") != 1 {
		t.Errorf("counters = %d, %d", r.Counter("hits"), r.Counter("misses"))
	}
	got := r.Samples("replay")
	if !slices.Equal(got, []int64{4, 9}) {
		t.Errorf("samples = %v", got)
	}
	got[0] = 100 // a copy
	if r.Samples("replay")[0] != 4 {
		t.Error("Samples returned the recorder's own slice")
	}
	r.Reset()
	if r.Counter("hits") != 0 || len(r.Samples("replay")) != 0 {
		t.Error("Reset did not forget")
	}

	// Safe for concurrent use.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				r.Count("n", 1)
				r.Sample("v", 1)
			}
		}()
	}
	wg.Wait()
	if r.Counter("n") != 4000 || len(r.Samples("v")) != 4000 {
		t.Errorf("lost updates: %d counted, %d sampled", r.Counter("n"), len(r.Samples("v")))
	}

	engine.NopRecorder{}.Count("x", 1) // does nothing, and does not panic
	engine.NopRecorder{}.Sample("x", 1)
}
