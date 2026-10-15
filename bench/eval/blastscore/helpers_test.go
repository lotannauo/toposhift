package blastscore

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var ctx = context.Background()

// The scenarios' times are on 2026-01-01 UTC, written hh:mm; every entity is
// observed at 09:00 with no end, and the window is [10:00, 10:30), unless a
// scenario says otherwise.

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

var w30From, w30To = hm(10, 0), hm(10, 30)

// mkfp is an entity of type typ whose hash starts with n, so the tests fix the
// order of entities: store.CompareFingerprints orders by type name, then hash.
func mkfp(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func svc(n byte) identity.Fingerprint  { return mkfp(catalog.Service, n) }
func inst(n byte) identity.Fingerprint { return mkfp(catalog.ServiceInstance, n) }
func pod(n byte) identity.Fingerprint  { return mkfp(catalog.K8sPod, n) }
func node(n byte) identity.Fingerprint { return mkfp(catalog.K8sNode, n) }
func host(n byte) identity.Fingerprint { return mkfp(catalog.Host, n) }

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func edge(a, b identity.Fingerprint, rel catalog.RelationType) store.Subject {
	return store.EdgeSubject(a, b, rel)
}

func dependsOn(a, b identity.Fingerprint) store.Subject { return edge(a, b, catalog.DependsOn) }
func sched(p, n identity.Fingerprint) store.Subject     { return edge(p, n, catalog.ScheduledOn) }

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

const (
	k8s    lifecycle.Producer = "k8sobjects/default"
	traces lifecycle.Producer = "traces/default"
	agent  lifecycle.Producer = "nodeagent/default"
)

// producerOf is the scenarios' producer of a subject.
func producerOf(sub store.Subject) lifecycle.Producer {
	switch {
	case sub.Relation == catalog.DependsOn, sub.Relation == catalog.InstanceOf,
		sub.Kind == store.SubjectEntity && (sub.A.Type() == catalog.Service || sub.A.Type() == catalog.ServiceInstance),
		sub.Relation == catalog.PartOf && sub.A.Type() == catalog.ServiceInstance:
		return traces
	case sub.Kind == store.SubjectEntity && sub.A.Type() == catalog.Host:
		return agent
	}
	return k8s
}

func obs(sub store.Subject, at time.Time) store.Record {
	return store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: producerOf(sub), EventTime: at,
		Kind: lifecycle.Observe, Payload: []byte("x"),
	}
}

func del(sub store.Subject, at time.Time) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: producerOf(sub), EventTime: at, Kind: lifecycle.Delete}
}

// live is an edge present from one instant to another.
func live(sub store.Subject, from, to time.Time) []store.Record {
	return []store.Record{obs(sub, from), del(sub, to)}
}

// feed writes batches to a store, numbering records after its LastSeq.
type feed struct {
	t   testing.TB
	st  store.Store
	seq uint64
}

func newFeed(t testing.TB, st store.Store) *feed { return &feed{t: t, st: st, seq: st.LastSeq()} }

func (f *feed) write(rs ...store.Record) {
	f.t.Helper()
	rs = slices.Clone(rs)
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(ctx, rs); err != nil {
		f.t.Fatal(err)
	}
}

// world is a store with the entities observed at 09:00 with no end.
func world(t testing.TB, opts memstore.Options, fps ...identity.Fingerprint) (*memstore.Store, *feed) {
	t.Helper()
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	f := newFeed(t, st)
	var rs []store.Record
	for _, fp := range fps {
		rs = append(rs, obs(ent(fp), hm(9, 0)))
	}
	if len(rs) > 0 {
		f.write(rs...)
	}
	return st, f
}

// place writes instance i of service s, part of pod p, from 09:00.
func place(f *feed, s, i, p identity.Fingerprint) {
	f.write(obs(edge(i, s, catalog.InstanceOf), hm(9, 0)), obs(edge(i, p, catalog.PartOf), hm(9, 0)))
}

// service-level case over the given services, seeded at seed, with the truth.
func svcCase(id string, seed identity.Fingerprint, services []identity.Fingerprint, truth ...identity.Fingerprint) Case {
	return Case{
		ID: id, Class: ClassCallGraph, From: w30From, To: w30To, Seed: seed,
		Services: services, Service: Truth{Entities: truth},
	}
}

func scoreFor(t *testing.T, run CaseRun, l Level, p Predictor) CaseScore {
	t.Helper()
	s, ok := run.scoreOf(l, p)
	if !ok {
		t.Fatalf("case %s has no %s %s score; scores: %v, skips: %v", run.ID, l, p, scoreNames(run), run.Skips)
	}
	return s
}

func scoreNames(run CaseRun) []string {
	var out []string
	for _, s := range run.Scores {
		out = append(out, string(s.Level)+"/"+string(s.Predictor))
	}
	return out
}

func runCase(t *testing.T, st store.Store, c Case, cfg Config) CaseRun {
	t.Helper()
	run, err := RunCase(ctx, st, c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func fpsOf(ps []Prediction) []identity.Fingerprint {
	out := make([]identity.Fingerprint, len(ps))
	for i, p := range ps {
		out[i] = p.Entity
	}
	return out
}

func approx(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}
