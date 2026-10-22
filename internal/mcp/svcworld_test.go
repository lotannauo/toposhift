package mcp

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The service world is the fixture of service_context. It is written the way the
// translators write: k8sobjects (pods, nodes, scheduled_on, has_address),
// nodeagent (hosts, runs_on), spancount (services, instances, instance_of, part_of
// the instance's own pod) and svcgraph (depends_on). A sighting is written every
// minute with a TTL of five minutes, so a series ends five minutes after its last
// sighting. The window is [10:00, 10:30); every series starts at 09:55.
//
// Placement, written before any assertion. Each instance is part of its own pod;
// checkout-0 holds two instances, checkout's and an envoy sidecar's.
//
//	pod          service (instance)       node    host    interval
//	checkout-0   checkout (seed), envoy   node-1  host-a  the whole window
//	checkout-1   checkout                 node-3  host-c  the whole window
//	cart-0       cart                     node-1  host-a  the whole window; cart's instance is last sighted at 10:20
//	frontend-0   frontend                 node-4  none    the whole window
//	legacy-0     legacy-pay               node-4  none    the whole window
//	payments-0   payments                 node-6  host-f  the whole window
//	payments-1   payments                 node-5  host-e  the whole window
//	redis-0      redis (redis-1)          node-1  host-a  10:05 to 10:25, its scheduled_on deleted at 10:25
//	search-0     search                   node-2  host-a  the whole window
//	exporter-N   node-exporter            node-N  as the node  the whole window (N = 1 to 6)
//	weird-0      a service whose name     node-3  host-c  the whole window
//	             holds U+202E
//	bare-0       no instance              node-1  host-a  the whole window
//	redis-x      redis (redis-2, which is node-3  host-c  10:12 to 10:17
//	             also part of redis-0)
//
// Dependencies (depends_on, from the caller): frontend to checkout throughout;
// legacy-pay to checkout, last sighted at 10:10, so it ends at 10:15; checkout to
// payments throughout; checkout to cart and cart to checkout throughout.
//
// node-4 has no runs_on, so the two seed pods on it each refuse its host in
// their search, and the answer lists the refusal once.
//
// The seed pods are the pods of checkout and its hop: checkout-0, checkout-1,
// cart-0, frontend-0, legacy-0, payments-0 and payments-1. They stand on node-1,
// node-3, node-4, node-5 and node-6, so the frontier is five nodes; node-2 holds
// no seed pod.
type svcWorld struct {
	st                   store.Store
	feed                 *feed
	svc, pod, node, host map[string]identity.Fingerprint
	inst                 map[string]identity.Fingerprint
	recs                 []store.Record
}

const (
	spanProducer  lifecycle.Producer = "spancount/c1"
	graphProducer lifecycle.Producer = "svcgraph/c1"
	svcNamespace                     = "shop"
	svcTTL                           = 5 * time.Minute
	weirdName                        = "web\u202eird"
)

// minute is a time of the world's day, in minutes past 09:00.
func minute(min int) time.Time { return hm(9, 0).Add(time.Duration(min) * time.Minute) }

func newSvcMem(tb testing.TB) *memstore.Store {
	return openMem(tb, memstore.Options{Policy: servePolicy(rankObjs)})
}

func (w *svcWorld) add(rs ...store.Record) { w.recs = append(w.recs, rs...) }

// sight writes a sighting of sub every minute from first to last, inclusive.
func (w *svcWorld) sight(p lifecycle.Producer, sub store.Subject, payload string, first, last time.Time) {
	for t := first; !t.After(last); t = t.Add(time.Minute) {
		w.add(observe(sub, p, t, svcTTL, payload))
	}
}

func svcFP(tb testing.TB, name string) identity.Fingerprint {
	return fpOf(tb, catalog.Service, catalog.ServiceNamespace, svcNamespace, catalog.ServiceName, name)
}

func svcDescription(name string) string {
	b, err := json.Marshal(map[string]string{"name": name, "namespace": svcNamespace})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// newSvcWorld writes the world to st, which must be empty and opened with the
// policy of the test.
func newSvcWorld(tb testing.TB, st store.Store) *svcWorld {
	tb.Helper()
	return newSvcWorldKeeping(tb, st, nil)
}

// newSvcWorldKeeping is newSvcWorld writing only the records keep says to write; a
// nil keep writes them all.
func newSvcWorldKeeping(tb testing.TB, st store.Store, keep func(store.Record) bool) *svcWorld {
	tb.Helper()
	w := &svcWorld{
		st: st, feed: newFeed(tb, st),
		svc: map[string]identity.Fingerprint{}, pod: map[string]identity.Fingerprint{},
		node: map[string]identity.Fingerprint{}, host: map[string]identity.Fingerprint{}, inst: map[string]identity.Fingerprint{},
	}
	first, last := minute(55), minute(89) // 09:55 and 10:29
	nine := hm(9, 0)

	// Hosts, nodes and their placement.
	nodeHost := map[string]string{"node-1": "host-a", "node-2": "host-a", "node-3": "host-c", "node-5": "host-e", "node-6": "host-f"}
	for _, h := range []string{"host-a", "host-c", "host-e", "host-f"} {
		w.host[h] = fpOf(tb, catalog.Host, catalog.HostID, h)
		r := observe(ent(w.host[h]), agent, nine, 0, `{"host.arch":"arm64"}`)
		r.Boot = "boot-" + h
		w.add(r)
	}
	for i := 1; i <= 6; i++ {
		n := fmt.Sprintf("node-%d", i)
		w.node[n] = fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, n+"-uid")
		ep := fpOf(tb, catalog.NetworkEndpoint, catalog.EndpointRealm, "k8s/c1", catalog.EndpointAddress, fmt.Sprintf("10.0.0.%d", i))
		w.add(
			observe(ent(w.node[n]), objs, nine, 0, fmt.Sprintf(`{"name":%q}`, n)),
			observe(ent(ep), objs, nine, 0, `{}`),
			observe(edge(w.node[n], ep, catalog.HasAddress), objs, nine, 0, `{}`),
		)
		if h, ok := nodeHost[n]; ok {
			w.add(observe(edge(w.node[n], w.host[h], catalog.RunsOn), agent, nine, 0, `{}`))
		}
	}

	// Pods, with the scheduling of each.
	type podSpec struct {
		name, node string
		from, to   time.Time // to is zero for a pod that stays
	}
	pods := []podSpec{
		{"checkout-0", "node-1", nine, time.Time{}},
		{"checkout-1", "node-3", nine, time.Time{}},
		{"cart-0", "node-1", nine, time.Time{}},
		{"frontend-0", "node-4", nine, time.Time{}},
		{"legacy-0", "node-4", nine, time.Time{}},
		{"payments-0", "node-6", nine, time.Time{}},
		{"payments-1", "node-5", nine, time.Time{}},
		{"redis-0", "node-1", minute(65), minute(85)},
		{"search-0", "node-2", nine, time.Time{}},
		{"weird-0", "node-3", nine, time.Time{}},
		{"bare-0", "node-1", nine, time.Time{}},
		{"redis-x", "node-3", minute(72), minute(77)},
	}
	for i := 1; i <= 6; i++ {
		pods = append(pods, podSpec{fmt.Sprintf("exporter-%d", i), fmt.Sprintf("node-%d", i), nine, time.Time{}})
	}
	for i, p := range pods {
		w.pod[p.name] = fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, p.name+"-uid")
		ep := fpOf(tb, catalog.NetworkEndpoint, catalog.EndpointRealm, "k8s/c1", catalog.EndpointAddress, fmt.Sprintf("10.1.0.%d", i+1))
		w.add(
			observe(ent(w.pod[p.name]), objs, p.from, 0, fmt.Sprintf(`{"name":%q,"namespace":%q}`, p.name, svcNamespace)),
			observe(ent(ep), objs, p.from, 0, `{}`),
			observe(edge(w.pod[p.name], ep, catalog.HasAddress), objs, p.from, 0, `{}`),
			observe(edge(w.pod[p.name], w.node[p.node], catalog.ScheduledOn), objs, p.from, 0, `{}`),
		)
		if !p.to.IsZero() {
			w.add(deleted(edge(w.pod[p.name], w.node[p.node], catalog.ScheduledOn), objs, p.to))
		}
	}

	// Services and their dependencies.
	svcNames := []string{"checkout", "frontend", "legacy-pay", "payments", "cart", "envoy", "redis", "search", "node-exporter", weirdName}
	for _, n := range svcNames {
		w.svc[n] = svcFP(tb, n)
		w.sight(spanProducer, ent(w.svc[n]), svcDescription(n), first, last)
	}
	dep := func(from, to string, lastSeen time.Time) {
		w.sight(graphProducer, ent(w.svc[from]), svcDescription(from), first, lastSeen)
		w.sight(graphProducer, ent(w.svc[to]), svcDescription(to), first, lastSeen)
		w.sight(graphProducer, edge(w.svc[from], w.svc[to], catalog.DependsOn), `{}`, first, lastSeen)
	}
	dep("frontend", "checkout", last)
	dep("legacy-pay", "checkout", minute(70))
	dep("checkout", "payments", last)
	dep("checkout", "cart", last)
	dep("cart", "checkout", last)

	// Instances, each part of its own pod.
	instance := func(svcName, id, pod string, firstSeen, lastSeen time.Time) identity.Fingerprint {
		fp := fpOf(tb, catalog.ServiceInstance, catalog.ServiceNamespace, svcNamespace, catalog.ServiceName, svcName, catalog.ServiceInstanceID, id)
		w.inst[id] = fp
		w.sight(spanProducer, ent(fp), `{"version":"1.0.0"}`, firstSeen, lastSeen)
		w.sight(spanProducer, edge(fp, w.svc[svcName], catalog.InstanceOf), `{}`, firstSeen, lastSeen)
		if pod != "" {
			w.sight(spanProducer, edge(fp, w.pod[pod], catalog.PartOf), `{}`, firstSeen, lastSeen)
		}
		return fp
	}
	instance("checkout", "checkout-0", "checkout-0", first, last)
	instance("envoy", "envoy-0", "checkout-0", first, last)
	instance("checkout", "checkout-1", "checkout-1", first, last)
	instance("cart", "cart-0", "cart-0", first, minute(80)) // last sighted at 10:20
	instance("frontend", "frontend-0", "frontend-0", first, last)
	instance("legacy-pay", "legacy-0", "legacy-0", first, last)
	instance("payments", "payments-0", "payments-0", first, last)
	instance("payments", "payments-1", "payments-1", first, last)
	instance("redis", "redis-1", "redis-0", minute(65), last)
	instance("search", "search-0", "search-0", first, last)
	instance(weirdName, "weird-0", "weird-0", first, last)
	for i := 1; i <= 6; i++ {
		instance("node-exporter", fmt.Sprintf("exporter-%d", i), fmt.Sprintf("exporter-%d", i), first, last)
	}
	// redis-2 is part of redis-0 and of redis-x from 10:12 to 10:17: several pods at once.
	r2 := instance("redis", "redis-2", "", minute(72), last)
	for _, pod := range []string{"redis-0", "redis-x"} {
		w.add(
			observe(edge(r2, w.pod[pod], catalog.PartOf), spanProducer, minute(72), 0, `{}`),
			deleted(edge(r2, w.pod[pod], catalog.PartOf), spanProducer, minute(77)),
		)
	}

	if keep != nil {
		w.recs = slices.DeleteFunc(w.recs, func(r store.Record) bool { return !keep(r) })
	}
	w.flush()
	return w
}

// flush writes the records added so far, in event time, in batches.
func (w *svcWorld) flush() {
	sort.SliceStable(w.recs, func(i, j int) bool { return w.recs[i].EventTime.Before(w.recs[j].EventTime) })
	for len(w.recs) > 0 {
		n := min(len(w.recs), 500)
		w.feed.write(w.recs[:n]...)
		w.recs = w.recs[n:]
	}
}

func (w *svcWorld) source(tb testing.TB) *Source {
	tb.Helper()
	s, err := NewSource(Config{Store: w.st, Rank: rankObjs, RangeReads: RangeReadsFor(w.st)})
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// lateWrites changes the world the way a writer does while a call is running: a
// new service calls checkout, and checkout has a new instance.
func (w *svcWorld) lateWrites(tb testing.TB) {
	tb.Helper()
	inst := fpOf(tb, catalog.ServiceInstance, catalog.ServiceNamespace, svcNamespace, catalog.ServiceName, "checkout", catalog.ServiceInstanceID, "checkout-9")
	w.add(
		observe(edge(w.svc["search"], w.svc["checkout"], catalog.DependsOn), graphProducer, minute(70), svcTTL, `{}`),
		observe(ent(inst), spanProducer, minute(70), svcTTL, `{"version":"1.0.1"}`),
		observe(edge(inst, w.svc["checkout"], catalog.InstanceOf), spanProducer, minute(70), svcTTL, `{}`),
		observe(edge(inst, w.pod["search-0"], catalog.PartOf), spanProducer, minute(70), svcTTL, `{}`),
	)
	w.flush()
}

// id is the id of a service of the world.
func (w *svcWorld) id(name string) string { return w.svc[name].String() }

// svcAnswers asks the questions of the world every test of the vocabulary walks:
// checkout over the window, node only, and cart.
func svcAnswers(tb testing.TB, w *svcWorld, s *Source) []answer {
	tb.Helper()
	var out []answer
	for _, q := range []struct {
		name string
		in   ServiceContextInput
	}{
		{"service context of checkout", ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030}},
		{"service context of checkout, node only", ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030, NodeOnly: true}},
		{"service context of cart", ServiceContextInput{Service: w.id("cart"), From: w10, To: w1030}},
		{"service context of redis", ServiceContextInput{Service: w.id("redis"), From: w10, To: w1030}},
	} {
		v, err := s.ServiceContext(ctx, q.in)
		out = append(out, marshal(tb, q.name, "service_context", v, err))
	}
	return out
}
