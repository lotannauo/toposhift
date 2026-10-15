package mcphttp

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

const (
	objs  lifecycle.Producer = "k8sobjects/c1"
	clus  lifecycle.Producer = "k8scluster/c1"
	agent lifecycle.Producer = "nodeagent/c1"
)

// hostSecret is a host.id that must never appear in an answer: the server prints
// no identity attribute it did not receive in the request.
const hostSecret = "HOST-ID-DISTINCT-7f3a"

// nodeBootSecret, nodeAddress and nodeProvider are the node's own report of
// itself, which the payload table withholds.
const (
	nodeBootSecret = "NODE-BOOT-SECRET-91c2"
	nodeAddress    = "10.0.0.5"
	nodeProvider   = "aws:///eu-1a/i-0123456789"
	podHostIP      = "10.0.0.5"
	podIP          = "10.1.2.3"
	hostResourceID = "i-0abc-RESOURCE-ID"
)

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

var rankObjs = map[lifecycle.Producer]int{objs: 3, clus: 2, agent: 1}

var rankClus = map[lifecycle.Producer]int{clus: 3, objs: 2, agent: 1}

var testResolver = identity.NewResolver(catalog.Default())

func fpOf(tb testing.TB, typ catalog.EntityType, kv ...any) identity.Fingerprint {
	tb.Helper()
	var attrs []identity.Attr
	for i := 0; i < len(kv); i += 2 {
		attrs = append(attrs, identity.Attr{Key: kv[i].(catalog.AttributeKey), Value: kv[i+1]})
	}
	id, err := testResolver.Resolve(typ, attrs)
	if err != nil {
		tb.Fatal(err)
	}
	return id.Fingerprint()
}

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func edge(a, b identity.Fingerprint, rel catalog.RelationType) store.Subject {
	return store.EdgeSubject(a, b, rel)
}

func observe(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration, payload string) store.Record {
	return store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte(payload),
	}
}

func deleted(sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Delete}
}

// feed writes batches to a store, numbering the records after its LastSeq.
type feed struct {
	tb  testing.TB
	st  store.Store
	seq uint64
}

func newFeed(tb testing.TB, st store.Store) *feed { return &feed{tb: tb, st: st, seq: st.LastSeq()} }

func (f *feed) write(rs ...store.Record) {
	f.tb.Helper()
	rs = slices.Clone(rs)
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(context.Background(), rs); err != nil {
		f.tb.Fatal(err)
	}
}

func openMem(tb testing.TB, opts memstore.Options) *memstore.Store {
	tb.Helper()
	st, err := memstore.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

// world is the fixture every tool is tested on, the one the core's own tests use. Times are on 2026-01-01 UTC. The
// window is [10:00, 10:30).
//
//	host H1 (nodeagent, a payload with a cloud resource id) in zone Z1
//	node N1, N2 on H1; node N3 on H2, a host quarantined by a clone collision
//	P1 on N1, with two producers describing it and a container C1; its phase
//	    changes at 10:12
//	P2 on N1, deleted at 10:05 (its edge ends with it)
//	P3 on N1 from 10:06
//	P4 on N1, its edge deleted at 10:10 and back at 10:20
//	P5 on N3
//	P6 on N1 from 10:00, silent from 10:10 (its TTL)
//	P7 on N2
type world struct {
	st                         *memstore.Store
	h1, h2, z1                 identity.Fingerprint
	n1, n2, n3                 identity.Fingerprint
	p1, p2, p3, p4, p5, p6, p7 identity.Fingerprint
	c1                         identity.Fingerprint
	feed                       *feed
}

func newWorld(tb testing.TB, rank map[lifecycle.Producer]int) *world {
	tb.Helper()
	st := openMem(tb, memstore.Options{Policy: lifecycle.Policy{Rank: rank, BootKey: lifecycle.BootID}})
	w := &world{
		st: st, feed: newFeed(tb, st),
		h1: fpOf(tb, catalog.Host, catalog.HostID, hostSecret),
		h2: fpOf(tb, catalog.Host, catalog.HostID, "host-two"),
		z1: fpOf(tb, catalog.Zone, catalog.ZoneID, "aws/eu-1/eu-1a"),
		n1: fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-1"),
		n2: fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-2"),
		n3: fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-3"),
		c1: fpOf(tb, catalog.Container, catalog.ContainerID, "container-1"),
	}
	pods := []*identity.Fingerprint{&w.p1, &w.p2, &w.p3, &w.p4, &w.p5, &w.p6, &w.p7}
	for i, p := range pods {
		*p = fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-"+string(rune('1'+i)))
	}

	nine := hm(9, 0)
	host := func(boot string, at time.Time, fp identity.Fingerprint, payload string) store.Record {
		r := observe(ent(fp), agent, at, 0, payload)
		r.Boot = boot
		return r
	}
	w.feed.write(
		host("boot-h1", nine, w.h1, `{"cloud.resource_id":"`+hostResourceID+`","host.arch":"arm64","host.id":"`+hostSecret+`","os.type":"linux"}`),
		host("b1", nine, w.h2, `{"host.arch":"arm64"}`),
		host("b2", hm(9, 5), w.h2, `{"host.arch":"arm64"}`),
		host("b1", hm(9, 10), w.h2, `{"host.arch":"arm64"}`),
		observe(ent(w.z1), agent, nine, 0, `{"zone":"eu-1a"}`),
		observe(edge(w.h1, w.z1, catalog.LocatedIn), agent, nine, 0, `{}`),

		observe(ent(w.n1), objs, nine, 0, `{"addresses":[{"address":"`+nodeAddress+`","type":"InternalIP"}],"conditions":[{"status":"True","type":"Ready"}],"labels":{"kubernetes.io/hostname":"worker-1"},"name":"worker-1","node_info":{"boot_id":"`+nodeBootSecret+`"},"provider_id":"`+nodeProvider+`"}`),
		observe(ent(w.n2), objs, nine, 0, `{"name":"worker-2"}`),
		observe(ent(w.n3), objs, nine, 0, `{"name":"worker-3"}`),
		observe(edge(w.n1, w.h1, catalog.RunsOn), agent, nine, 0, `{}`),
		observe(edge(w.n2, w.h1, catalog.RunsOn), agent, nine, 0, `{}`),
		observe(edge(w.n3, w.h2, catalog.RunsOn), agent, nine, 0, `{}`),

		observe(ent(w.p1), objs, nine, 0, `{"host_ip":"`+podHostIP+`","name":"web-0","namespace":"shop","phase":"Pending","pod_ips":["`+podIP+`"]}`),
		observe(ent(w.p1), clus, nine, 0, `{"k8s.namespace.name":"shop-cluster","k8s.pod.name":"web-0-cluster","k8s.pod.phase":"Pending"}`),
		observe(edge(w.p1, w.n1, catalog.ScheduledOn), objs, nine, 0, `{}`),
		observe(ent(w.c1), objs, nine, 0, `{"name":"nginx","pod_uid":"pod-uid-1"}`),
		observe(edge(w.c1, w.p1, catalog.PartOf), objs, nine, 0, `{}`),

		observe(ent(w.p2), objs, nine, 0, `{"name":"web-1","namespace":"shop","phase":"Running"}`),
		observe(edge(w.p2, w.n1, catalog.ScheduledOn), objs, nine, 0, `{}`),
		deleted(ent(w.p2), objs, hm(10, 5)),

		observe(ent(w.p3), objs, hm(10, 6), 0, `{"name":"web-2","namespace":"shop","phase":"Running"}`),
		observe(edge(w.p3, w.n1, catalog.ScheduledOn), objs, hm(10, 6), 0, `{}`),

		observe(ent(w.p4), objs, nine, 0, `{"name":"web-3","namespace":"shop","phase":"Running"}`),
		observe(edge(w.p4, w.n1, catalog.ScheduledOn), objs, nine, 0, `{}`),
		deleted(edge(w.p4, w.n1, catalog.ScheduledOn), objs, hm(10, 10)),
		observe(edge(w.p4, w.n1, catalog.ScheduledOn), objs, hm(10, 20), 0, `{}`),

		observe(ent(w.p5), objs, nine, 0, `{"name":"web-4","namespace":"shop"}`),
		observe(edge(w.p5, w.n3, catalog.ScheduledOn), objs, nine, 0, `{}`),

		observe(ent(w.p6), objs, hm(10, 0), 10*time.Minute, `{"name":"web-5","namespace":"shop"}`),
		observe(edge(w.p6, w.n1, catalog.ScheduledOn), objs, hm(10, 0), 10*time.Minute, `{}`),

		observe(ent(w.p7), objs, nine, 0, `{"name":"web-6","namespace":"shop","phase":"Running"}`),
		observe(edge(w.p7, w.n2, catalog.ScheduledOn), objs, nine, 0, `{}`),

		observe(ent(w.p1), objs, hm(10, 12), 0, `{"host_ip":"`+podHostIP+`","name":"web-0","namespace":"shop","phase":"Running","pod_ips":["`+podIP+`"]}`),
	)
	return w
}
