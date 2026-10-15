package graphdiff_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Between 10:00 and 10:30, pod 2 left the node, pod 3 arrived, and pod 1,
// there throughout, went from Pending to Running.
func Example() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	fp := func(typ catalog.EntityType, n byte) identity.Fingerprint {
		f, _ := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{n})
		return f
	}
	p1, p2, p3, n := fp(catalog.K8sPod, 1), fp(catalog.K8sPod, 2), fp(catalog.K8sPod, 3), fp(catalog.K8sNode, 1)
	var seq uint64
	rec := func(sub store.Subject, kind lifecycle.Kind, at time.Time, payload string) store.Record {
		seq++
		return store.Record{Layer: catalog.L2, Subject: sub, Producer: "k8sobjects/c", EventTime: at, Seq: seq, Kind: kind, Payload: []byte(payload)}
	}
	sched := func(p identity.Fingerprint) store.Subject { return store.EdgeSubject(p, n, catalog.ScheduledOn) }
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	_ = st.Write(context.Background(), []store.Record{
		rec(store.EntitySubject(n), lifecycle.Observe, clock(9, 0), `{"name":"n"}`),
		rec(store.EntitySubject(p1), lifecycle.Observe, clock(9, 0), `{"name":"web-0","phase":"Pending"}`),
		rec(store.EntitySubject(p1), lifecycle.Observe, clock(10, 12), `{"name":"web-0","phase":"Running"}`),
		rec(store.EntitySubject(p2), lifecycle.Observe, clock(9, 0), `{"name":"web-1"}`),
		rec(store.EntitySubject(p3), lifecycle.Observe, clock(10, 6), `{"name":"web-2"}`),
		rec(sched(p1), lifecycle.Observe, clock(9, 0), `{}`),
		rec(sched(p2), lifecycle.Observe, clock(9, 0), `{}`),
		rec(sched(p2), lifecycle.Delete, clock(10, 5), ``),
		rec(sched(p3), lifecycle.Observe, clock(10, 6), `{}`),
	})

	res, err := graphdiff.NewDiffer(st).Diff(context.Background(), []identity.Fingerprint{n, p1}, clock(10, 0), clock(10, 30), store.Latest,
		graphdiff.Options{Rank: map[lifecycle.Producer]int{"k8sobjects/c": 2}})
	if err != nil {
		fmt.Println(err)
		return
	}
	name := func(s store.Subject) string {
		if s.Kind == store.SubjectEntity {
			return fmt.Sprintf("%s %d", s.A.Type(), s.A.Hash()[0])
		}
		return fmt.Sprintf("pod %d %s node", s.A.Hash()[0], s.Relation)
	}
	for _, e := range res.Created {
		fmt.Printf("created: %s\n", name(e.Element.Subject))
	}
	for _, e := range res.Deleted {
		fmt.Printf("deleted: %s (%s at %s)\n", name(e.Element.Subject), e.DeleteSource, e.DeletedAt.Format("15:04"))
	}
	for _, e := range res.Changed {
		fmt.Printf("changed: %s, state %v, attributes %v\n", name(e.Element.Subject), e.Change.StateKeys, e.Change.AttributeKeys)
	}
	fmt.Printf("relations created %d, deleted %d; entities changed %d\n",
		res.Counts.Created.Relations, res.Counts.Deleted.Relations, res.Counts.Changed.Entities)
	for _, ev := range graphdiff.Events(res) {
		fmt.Printf("%s %s appear=%t\n", ev.At.Format("15:04"), name(ev.Subject), ev.Appear)
	}
	// Output:
	// created: pod 3 scheduled_on node
	// deleted: pod 2 scheduled_on node (producer at 10:05)
	// changed: k8s.pod 1, state [phase], attributes []
	// relations created 1, deleted 1; entities changed 1
	// 10:05 pod 2 scheduled_on node appear=false
	// 10:06 pod 3 scheduled_on node appear=true
}
