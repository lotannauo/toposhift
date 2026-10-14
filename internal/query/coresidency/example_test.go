package coresidency_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// S ran on a node for the whole window, A for its first five minutes and B
// for its last 24. A union graph draws A and B on the node together; they never
// were, and only S was co-resident with both.
func Example() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	fp := func(typ catalog.EntityType, n byte) identity.Fingerprint {
		f, _ := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{n})
		return f
	}
	s, a, b, n := fp(catalog.K8sPod, 1), fp(catalog.K8sPod, 2), fp(catalog.K8sPod, 3), fp(catalog.K8sNode, 1)
	var seq uint64
	rec := func(sub store.Subject, kind lifecycle.Kind, at time.Time) store.Record {
		seq++
		r := store.Record{Layer: catalog.L2, Subject: sub, Producer: "k8sobjects", EventTime: at, Seq: seq, Kind: kind}
		if kind == lifecycle.Observe {
			r.Payload = []byte("spec")
		}
		return r
	}
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	var batch []store.Record
	for _, e := range []identity.Fingerprint{s, a, b, n} {
		batch = append(batch, rec(store.EntitySubject(e), lifecycle.Observe, clock(9, 0)))
	}
	batch = append(batch,
		rec(store.EdgeSubject(s, n, catalog.ScheduledOn), lifecycle.Observe, clock(9, 0)),
		rec(store.EdgeSubject(a, n, catalog.ScheduledOn), lifecycle.Observe, clock(9, 58)),
		rec(store.EdgeSubject(a, n, catalog.ScheduledOn), lifecycle.Delete, clock(10, 5)),
		rec(store.EdgeSubject(b, n, catalog.ScheduledOn), lifecycle.Observe, clock(10, 6)),
	)
	_ = st.Write(context.Background(), batch)

	f := coresidency.NewFinder(st)
	w := interval.Window{From: clock(10, 0), To: clock(10, 30), AsOf: store.Latest}
	for _, seed := range []identity.Fingerprint{s, a} {
		res, err := f.Find(context.Background(), seed, w, coresidency.Options{NodeOnly: true})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("with pod %d:\n", seed.Hash()[0])
		for _, c := range res.CoResidents {
			fmt.Printf("  pod %d, %s, for %s\n", c.Peer.Hash()[0], c.Mechanism, c.Duration)
		}
	}
	// Output:
	// with pod 1:
	//   pod 3, shared_node, for 24m0s
	//   pod 2, shared_node, for 5m0s
	// with pod 2:
	//   pod 1, shared_node, for 5m0s
}
