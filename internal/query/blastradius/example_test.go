package blastradius_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// A node ran on a host until 10:10, and a pod was scheduled on the node from
// 10:15. A union graph of the window draws host, node and pod as one path; it
// never existed, so the host's failure reaches the node and not the pod.
func Example() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	fp := func(typ catalog.EntityType) identity.Fingerprint {
		f, _ := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{1})
		return f
	}
	h, n, p := fp(catalog.Host), fp(catalog.K8sNode), fp(catalog.K8sPod)
	var seq uint64
	rec := func(layer catalog.Layer, sub store.Subject, kind lifecycle.Kind, at time.Time) store.Record {
		seq++
		r := store.Record{Layer: layer, Subject: sub, Producer: "example", EventTime: at, Seq: seq, Kind: kind}
		if kind == lifecycle.Observe {
			r.Payload = []byte("spec")
		}
		return r
	}
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	_ = st.Write(context.Background(), []store.Record{
		rec(catalog.L1, store.EntitySubject(h), lifecycle.Observe, clock(9, 0)),
		rec(catalog.L2, store.EntitySubject(n), lifecycle.Observe, clock(9, 0)),
		rec(catalog.L2, store.EntitySubject(p), lifecycle.Observe, clock(9, 0)),
		rec(catalog.L1, store.EdgeSubject(n, h, catalog.RunsOn), lifecycle.Observe, clock(9, 0)),
		rec(catalog.L1, store.EdgeSubject(n, h, catalog.RunsOn), lifecycle.Delete, clock(10, 10)),
		rec(catalog.L2, store.EdgeSubject(p, n, catalog.ScheduledOn), lifecycle.Observe, clock(10, 15)),
	})

	w := interval.Window{From: clock(10, 0), To: clock(10, 30), AsOf: store.Latest}
	res, err := blastradius.NewWalker(st).Impact(context.Background(), h, w, blastradius.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, m := range res.Members {
		fmt.Printf("%s at depth %d, for %s\n", m.Entity.Type(), m.Depth, m.Duration)
	}
	// Output:
	// k8s.node at depth 1, for 10m0s
}
