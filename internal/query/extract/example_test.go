package extract_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Service 1 calls service 2 and is called by service 3. Each runs one instance
// in one pod on one node: service 1's for the whole window, service 2's for its
// first five minutes and service 3's for its last 24. A union graph draws
// services 2 and 3 on the node together; they never were, and only service 1
// shared the node with both.
func Example() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	fp := func(typ catalog.EntityType, n byte) identity.Fingerprint {
		f, _ := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{n})
		return f
	}
	n := fp(catalog.K8sNode, 1)
	var seq uint64
	rec := func(sub store.Subject, kind lifecycle.Kind, at time.Time) store.Record {
		seq++
		layer := catalog.L2
		if sub.Relation == catalog.DependsOn || (sub.Kind == store.SubjectEntity && sub.A.Type() == catalog.Service) {
			layer = catalog.L3
		}
		r := store.Record{Layer: layer, Subject: sub, Producer: "traces", EventTime: at, Seq: seq, Kind: kind}
		if kind == lifecycle.Observe {
			r.Payload = []byte("spec")
		}
		return r
	}
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	batch := []store.Record{rec(store.EntitySubject(n), lifecycle.Observe, clock(9, 0))}
	for i := byte(1); i <= 3; i++ {
		s, in, p := fp(catalog.Service, i), fp(catalog.ServiceInstance, i), fp(catalog.K8sPod, i)
		for _, e := range []identity.Fingerprint{s, in, p} {
			batch = append(batch, rec(store.EntitySubject(e), lifecycle.Observe, clock(9, 0)))
		}
		batch = append(batch,
			rec(store.EdgeSubject(in, s, catalog.InstanceOf), lifecycle.Observe, clock(9, 0)),
			rec(store.EdgeSubject(in, p, catalog.PartOf), lifecycle.Observe, clock(9, 0)))
	}
	s1, s2, s3 := fp(catalog.Service, 1), fp(catalog.Service, 2), fp(catalog.Service, 3)
	p1, p2, p3 := fp(catalog.K8sPod, 1), fp(catalog.K8sPod, 2), fp(catalog.K8sPod, 3)
	batch = append(batch,
		rec(store.EdgeSubject(s1, s2, catalog.DependsOn), lifecycle.Observe, clock(9, 0)),
		rec(store.EdgeSubject(s3, s1, catalog.DependsOn), lifecycle.Observe, clock(9, 0)),
		rec(store.EdgeSubject(p1, n, catalog.ScheduledOn), lifecycle.Observe, clock(9, 0)),
		rec(store.EdgeSubject(p2, n, catalog.ScheduledOn), lifecycle.Observe, clock(9, 58)),
		rec(store.EdgeSubject(p2, n, catalog.ScheduledOn), lifecycle.Delete, clock(10, 5)),
		rec(store.EdgeSubject(p3, n, catalog.ScheduledOn), lifecycle.Observe, clock(10, 6)),
	)
	_ = st.Write(context.Background(), batch)

	w := interval.Window{From: clock(10, 0), To: clock(10, 30), AsOf: store.Latest}
	res, err := extract.NewExtractor(st).Extract(context.Background(), extract.Exploratory{Service: s1}, w, extract.Options{NodeOnly: true})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, m := range res.Members {
		fmt.Printf("service %d: %s\n", m.Service.Hash()[0], m.Role)
	}
	for _, p := range res.Pairs {
		fmt.Printf("services %d and %d, %s, for %s\n", p.Member.Hash()[0], p.Peer.Hash()[0], p.Mechanism, p.Duration)
	}
	// Output:
	// service 1: seed
	// service 2: callee
	// service 3: caller
	// services 1 and 3, shared_node, for 24m0s
	// services 1 and 2, shared_node, for 5m0s
}
