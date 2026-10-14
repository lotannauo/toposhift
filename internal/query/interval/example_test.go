package interval_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Two pods were scheduled on one node during a window, one after the other. A
// union graph shows them on the node together; the intersection of their
// intervals shows they never were.
func Example() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	fp := func(typ catalog.EntityType, n byte) identity.Fingerprint {
		f, _ := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{n})
		return f
	}
	a, b, node := fp(catalog.K8sPod, 1), fp(catalog.K8sPod, 2), fp(catalog.K8sNode, 1)
	rec := func(seq uint64, sub store.Subject, layer catalog.Layer, kind lifecycle.Kind, at time.Time) store.Record {
		r := store.Record{Layer: layer, Subject: sub, Producer: "k8sobjects", EventTime: at, Seq: seq, Kind: kind}
		if kind == lifecycle.Observe {
			r.Payload = []byte("spec")
		}
		return r
	}
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	_ = st.Write(context.Background(), []store.Record{
		rec(1, store.EntitySubject(a), catalog.L2, lifecycle.Observe, clock(9, 0)),
		rec(2, store.EntitySubject(b), catalog.L2, lifecycle.Observe, clock(9, 0)),
		rec(3, store.EntitySubject(node), catalog.L2, lifecycle.Observe, clock(9, 0)),
		rec(4, store.EdgeSubject(a, node, catalog.ScheduledOn), catalog.L2, lifecycle.Observe, clock(9, 58)),
		rec(5, store.EdgeSubject(a, node, catalog.ScheduledOn), catalog.L2, lifecycle.Delete, clock(10, 5)),
		rec(6, store.EdgeSubject(b, node, catalog.ScheduledOn), catalog.L2, lifecycle.Observe, clock(10, 6)),
	})

	r := interval.NewReader(st, interval.Options{})
	res, err := r.Edges(context.Background(), node, store.Reverse, interval.Window{From: clock(10, 0), To: clock(10, 30), AsOf: store.Latest})
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(res.Elements) != 2 {
		fmt.Println("elements:", len(res.Elements))
		return
	}
	for _, el := range res.Elements {
		p := el.Pieces[0]
		fmt.Printf("%s: [%s, %s) %s, began before: %t, %s\n", el.Subject.A.Type(),
			p.Start.Format("15:04"), p.End.Format("15:04"), p.EndSource, p.Before, el.Class)
	}
	together := interval.Intersect(interval.Spans(res.Elements[0]), interval.Spans(res.Elements[1]))
	fmt.Printf("together for %s, at token %d\n", interval.Duration(together), res.Token)
	// Output:
	// k8s.pod: [10:00, 10:05) producer, began before: true, disappeared
	// k8s.pod: [10:06, 10:30) unknown, began before: false, appeared
	// together for 0s, at token 6
}

// Two producers describe one pod, and the object watcher outranks the cluster
// receiver. The watcher's reference lapses at 10:05, so the pod's description
// at the window's last instant is the receiver's: a change of source, while
// the pod exists throughout.
func ExampleReader_Entities() {
	clock := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	pod, _ := identity.FingerprintFromHash(catalog.K8sPod, [identity.FingerprintBytes]byte{1})
	node, _ := identity.FingerprintFromHash(catalog.K8sNode, [identity.FingerprintBytes]byte{1})
	st, _ := memstore.Open(memstore.Options{})
	defer st.Close()
	_ = st.Write(context.Background(), []store.Record{
		{
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8sobjects", EventTime: clock(9, 0), Seq: 1,
			Kind: lifecycle.Observe, TTL: 10 * time.Minute, Through: clock(9, 55), Payload: []byte(`{"phase":"Running"}`),
		},
		{
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8scluster", EventTime: clock(9, 0), Seq: 2,
			Kind: lifecycle.Observe, Payload: []byte(`{"k8s.pod.phase":"Running"}`),
		},
		{
			Layer: catalog.L2, Subject: store.EntitySubject(node), Producer: "k8sobjects", EventTime: clock(10, 10), Seq: 3,
			Kind: lifecycle.Observe, Payload: []byte(`{"name":"n"}`),
		},
	})

	r := interval.NewReader(st, interval.Options{
		Describe: true,
		Rank:     map[lifecycle.Producer]int{"k8sobjects": 2, "k8scluster": 1},
	})
	w := interval.Window{From: clock(10, 0), To: clock(10, 30), AsOf: store.Latest}
	token, els, err := r.Entities(context.Background(), []identity.Fingerprint{pod, node}, w)
	if err != nil {
		fmt.Println(err)
		return
	}
	show := func(d *interval.Description) string {
		if d == nil {
			return "none"
		}
		return fmt.Sprintf("%s %s", d.Producer, d.Payload)
	}
	fmt.Println("token", token)
	for _, el := range els {
		fmt.Printf("%s, %s. From: %s. Last: %s.\n", el.Subject.A.Type(), el.Class, show(el.AtFrom), show(el.AtLast))
	}
	// Output:
	// token 3
	// k8s.pod, stable. From: k8sobjects {"phase":"Running"}. Last: k8scluster {"k8s.pod.phase":"Running"}.
	// k8s.node, appeared. From: none. Last: k8sobjects {"name":"n"}.
}
