package factload

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

const ns = "shop-ns"

func sp(trace, id, parent string, kind SpanKind, service string, start time.Time) Span {
	return Span{Start: start, TraceID: trace, SpanID: id, ParentID: parent, Kind: kind, Service: service, Namespace: ns}
}

// call is a client span of from with a server child in to, both at start.
func call(trace string, from, to string, start time.Time) []Span {
	return []Span{
		sp(trace, trace+"c", "", KindClient, from, start),
		sp(trace, trace+"s", trace+"c", KindServer, to, start),
	}
}

func edgesOf(t *testing.T, st store.Store, from identity.Fingerprint, w interval.Window) []interval.Element {
	t.Helper()
	res, err := interval.NewReader(st, interval.Options{}).Edges(ctx, from, store.Forward, w)
	if err != nil {
		t.Fatal(err)
	}
	return res.Elements
}

func window(from, to time.Time) interval.Window {
	return interval.Window{From: from, To: to, AsOf: store.Latest}
}

// A client span with a child server span in another service is a dependency of
// the caller on the callee; a producer span with a consumer child is one too, and
// is marked. The edge goes from the caller to the callee, as the catalog stores
// it. A client span with no instrumented child writes nothing and is counted.
func TestPairingMakesDependenciesFromTheCallerToTheCallee(t *testing.T) {
	var spans []Span
	spans = append(spans, call("t1", "shop-front", "shop-cart", at(0, 5))...)
	spans = append(spans,
		sp("t2", "t2p", "", KindProducer, "shop-checkout", at(0, 20)),
		sp("t2", "t2c", "t2p", KindConsumer, "shop-mail", at(0, 21)),
		// A client span with no instrumented child: a database or a cache.
		sp("t3", "t3c", "", KindClient, "shop-cart", at(0, 30)),
		// A server span under a server span, and a consumer under a client: not pairs.
		sp("t4", "t4s", "", KindServer, "shop-front", at(0, 40)),
		sp("t4", "t4x", "t4s", KindServer, "shop-pay", at(0, 41)),
		sp("t5", "t5c", "", KindClient, "shop-front", at(0, 50)),
		sp("t5", "t5x", "t5c", KindConsumer, "shop-pay", at(0, 51)),
		// A parent in another trace is no parent.
		sp("t6", "t6s", "t4s", KindServer, "shop-pay", at(0, 55)),
	)
	st, stats := load(t, Scenario{Spans: spans, Source: "set@rev"})

	front, cart := svcFP(t, ns, "shop-front"), svcFP(t, ns, "shop-cart")
	checkout, mail := svcFP(t, ns, "shop-checkout"), svcFP(t, ns, "shop-mail")
	w := window(at(0, 0), at(30, 0))

	deps := map[store.Subject]interval.Element{}
	for _, svc := range []identity.Fingerprint{front, cart, checkout, mail, svcFP(t, ns, "shop-pay")} {
		for _, el := range edgesOf(t, st, svc, w) {
			if el.Subject.Relation == catalog.DependsOn {
				deps[el.Subject] = el
			}
		}
	}
	if len(deps) != 2 {
		t.Fatalf("%d dependencies, want 2: %v", len(deps), deps)
	}
	if _, ok := deps[store.EdgeSubject(front, cart, catalog.DependsOn)]; !ok {
		t.Errorf("no front -> cart: %v", deps)
	}
	if _, ok := deps[store.EdgeSubject(cart, front, catalog.DependsOn)]; ok {
		t.Error("the dependency is stored from the callee to the caller")
	}
	if _, ok := deps[store.EdgeSubject(checkout, mail, catalog.DependsOn)]; !ok {
		t.Errorf("no checkout -> mail: %v", deps)
	}

	if stats.Pairs != 2 || stats.ClientsWithoutChild != 2 {
		// t3's client has no child, and nor has t5's, whose consumer child is not a
		// server's.
		t.Errorf("pairs %d, clients without a child %d; want 2, 2", stats.Pairs, stats.ClientsWithoutChild)
	}
	// The messaging mark is on the producer's dependency and not the other.
	if len(stats.Messaging) != 1 {
		t.Errorf("messaging pairs %v", stats.Messaging)
	}
	for _, r := range st.records() {
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.DependsOn {
			want := ""
			if r.Subject.A == checkout {
				want = MessagingPayload
			}
			if string(r.Payload) != want {
				t.Errorf("payload of a dependency from %s is %q, want %q", r.Subject.A, r.Payload, want)
			}
		}
	}
}

// The first and only record of a minute is at the earliest span start in it. A
// run of observations five minutes apart or less is one piece, an observation at
// the deadline joins it, and one a nanosecond later leaves a gap of a nanosecond.
func TestMinuteBucketsAndTheTTLGiveTheIntervals(t *testing.T) {
	var spans []Span
	// Two calls in one minute: the later one is not a record of its own.
	spans = append(spans, call("a1", "shop-front", "shop-cart", at(0, 5))...)
	spans = append(spans, call("a2", "shop-front", "shop-cart", at(0, 50))...)
	spans = append(spans, call("a3", "shop-front", "shop-cart", at(3, 0))...)
	// 10:08:00 is the deadline of the observation at 10:03:00.
	spans = append(spans, call("a4", "shop-front", "shop-cart", at(8, 0))...)
	// A nanosecond after the deadline of the one at 10:08:00.
	gap := at(13, 0).Add(time.Nanosecond)
	spans = append(spans, call("a5", "shop-front", "shop-cart", gap)...)
	st, stats := load(t, Scenario{Spans: spans})

	front, cart := svcFP(t, ns, "shop-front"), svcFP(t, ns, "shop-cart")
	var dep *interval.Element
	for _, el := range edgesOf(t, st, front, window(at(0, 0), at(30, 0))) {
		if el.Subject == store.EdgeSubject(front, cart, catalog.DependsOn) {
			dep = &el
		}
	}
	if dep == nil {
		t.Fatal("no front -> cart")
	}
	want := []interval.Span{
		{Start: at(0, 5), End: at(13, 0)},
		{Start: gap, End: gap.Add(5 * time.Minute)},
	}
	got := interval.Spans(*dep)
	if len(got) != len(want) {
		t.Fatalf("pieces %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Start.Equal(want[i].Start) || !got[i].End.Equal(want[i].End) {
			t.Errorf("piece %d is [%s, %s), want [%s, %s)", i, got[i].Start.Format("15:04:05.000000000"), got[i].End.Format("15:04:05.000000000"),
				want[i].Start.Format("15:04:05.000000000"), want[i].End.Format("15:04:05.000000000"))
		}
	}
	// One record for each of four minutes, at the earliest span start in it.
	var times []time.Time
	for _, r := range st.records() {
		if r.Subject == store.EdgeSubject(front, cart, catalog.DependsOn) {
			times = append(times, r.EventTime)
		}
	}
	wantTimes := []time.Time{at(0, 5), at(3, 0), at(8, 0), gap}
	if len(times) != len(wantTimes) || stats.DependsOnRecords != 4 || stats.DependsOnRuns != 2 {
		t.Fatalf("dependency records at %v (stats %d), runs %d; want %v and 2 runs", times, stats.DependsOnRecords, stats.DependsOnRuns, wantTimes)
	}
	for i := range times {
		if !times[i].Equal(wantTimes[i]) {
			t.Errorf("record %d at %s, want %s", i, times[i], wantTimes[i])
		}
	}
}

// A call from a service to itself writes nothing. A span with no namespace is the
// service (none, name) when its name has no span with a namespace, and an ordinary
// service then. When the name also has spans with one, it would be a second
// identity of one service: it is counted and written to nothing.
func TestSelfCallsAndTheNamespaceSplit(t *testing.T) {
	spans := call("s1", "shop-front", "shop-front", at(0, 1))
	twin := Span{Start: at(0, 2), TraceID: "s2", SpanID: "s2a", Kind: KindServer, Service: "shop-front"}
	only := Span{Start: at(0, 3), TraceID: "s3", SpanID: "s3a", Kind: KindServer, Service: "shop-bare"}
	spans = append(spans, twin, only,
		// A call into a service whose spans never carry a namespace: an ordinary pair.
		sp("s4", "s4c", "", KindClient, "shop-front", at(0, 4)),
		Span{Start: at(0, 4), TraceID: "s4", SpanID: "s4s", ParentID: "s4c", Kind: KindServer, Service: "shop-cart"},
		// A call into the namespace-less twin of its own caller: the twin is not written,
		// so there is no pair, and the client has no instrumented child.
		sp("s5", "s5c", "", KindClient, "shop-front", at(0, 5)),
		Span{Start: at(0, 5), TraceID: "s5", SpanID: "s5s", ParentID: "s5c", Kind: KindServer, Service: "shop-front"})
	st, stats := load(t, Scenario{Spans: spans})

	if stats.SelfPairs != 1 || stats.Pairs != 1 || stats.NamespaceSplit != 2 || stats.ClientsWithoutChild != 1 {
		t.Errorf("self %d, pairs %d, split spans %d, clients without a child %d; want 1, 1, 2, 1",
			stats.SelfPairs, stats.Pairs, stats.NamespaceSplit, stats.ClientsWithoutChild)
	}
	named := svcFP(t, ns, "shop-front")
	twinFP := svcFP(t, "", "shop-front")
	cartBare := svcFP(t, "", "shop-cart")
	if twinFP == named {
		t.Fatal("the two identities are one fingerprint")
	}
	written := map[store.Subject]bool{}
	for _, r := range st.records() {
		written[r.Subject] = true
	}
	for _, sub := range []store.Subject{
		store.EntitySubject(named), store.EntitySubject(svcFP(t, "", "shop-bare")), store.EntitySubject(cartBare),
		store.EdgeSubject(named, cartBare, catalog.DependsOn),
	} {
		if !written[sub] {
			t.Errorf("not written: %+v", sub)
		}
	}
	for _, r := range st.records() {
		if r.Subject.A == twinFP || r.Subject.B == twinFP {
			t.Errorf("the namespace-less twin of a namespaced service was written: %+v", r.Subject)
		}
	}
	if written[store.EdgeSubject(named, named, catalog.DependsOn)] {
		t.Error("a self call was written")
	}
	if len(stats.Instrumented) != 3 {
		t.Errorf("instrumented %v, want three identities", stats.Instrumented)
	}
}

// Instances come from service.instance.id; an instance whose id is the pod uid,
// and every bridge, carry the label that says the pod uid came from an address.
// The bridge is made from the pod uid only, never otherwise.
func TestInstancesAndTheBridge(t *testing.T) {
	front := func(inst, pod string, at time.Time) Span {
		s := sp("i"+inst+pod, "x", "", KindServer, "shop-front", at)
		s.InstanceID, s.PodUID = inst, pod
		return s
	}
	spans := []Span{
		front("pod-1", "pod-1", at(0, 1)),  // an id that is the pod uid
		front("inst-2", "pod-2", at(0, 2)), // an id of its own, in a pod
		front("inst-3", "", at(0, 3)),      // an instance with no pod uid
		{Start: at(0, 4), TraceID: "i4", SpanID: "x", Kind: KindServer, Service: "shop-front", Namespace: ns}, // no instance
	}
	st, stats := load(t, Scenario{Spans: spans, Source: "set@rev"})
	label := string(AddressPayload("set@rev"))
	if label != `{"basis":"address-associated","source":"set@rev"}` {
		t.Fatalf("label %s", label)
	}

	payloads := map[store.Subject]string{}
	for _, r := range st.records() {
		payloads[r.Subject] = string(r.Payload)
	}
	front1 := svcFP(t, ns, "shop-front")
	i1, i2, i3 := instFP(t, ns, "shop-front", "pod-1"), instFP(t, ns, "shop-front", "inst-2"), instFP(t, ns, "shop-front", "inst-3")
	cases := []struct {
		sub  store.Subject
		want string
		have bool
	}{
		{store.EntitySubject(i1), label, true},
		{store.EntitySubject(i2), "", true},
		{store.EntitySubject(i3), "", true},
		{store.EdgeSubject(i1, front1, catalog.InstanceOf), "", true},
		{store.EdgeSubject(i2, front1, catalog.InstanceOf), "", true},
		{store.EdgeSubject(i3, front1, catalog.InstanceOf), "", true},
		{store.EdgeSubject(i1, podFP(t, "pod-1"), catalog.PartOf), label, true},
		{store.EdgeSubject(i2, podFP(t, "pod-2"), catalog.PartOf), label, true},
	}
	for _, c := range cases {
		got, ok := payloads[c.sub]
		if ok != c.have || got != c.want {
			t.Errorf("%v: written %t with payload %q, want %t with %q", c.sub, ok, got, c.have, c.want)
		}
	}
	// No bridge for the instance with no pod uid, and exactly two in all.
	bridges := 0
	for sub := range payloads {
		if sub.Kind == store.SubjectEdge && sub.Relation == catalog.PartOf {
			bridges++
			if sub.A == i3 {
				t.Error("a bridge was made for an instance with no pod uid")
			}
		}
	}
	if bridges != 2 || stats.BridgeRecords != 2 || stats.InstanceRecords != 3 {
		t.Errorf("bridges %d (stats %d), instances %d; want 2, 2, 3", bridges, stats.BridgeRecords, stats.InstanceRecords)
	}
}

// The writer's records all come from one producer that a store accepts, are
// timed at a span start with the producer's clock, carry the placeholder TTL, and
// are written in event time order: there are no late events.
func TestTheServiceWriterIsOneProducerInEventTimeOrder(t *testing.T) {
	if err := store.WritableProducer(ProducerTraces); err != nil {
		t.Errorf("the traces producer is refused: %v", err)
	}
	if err := store.WritableProducer(ProducerObjects); err != nil {
		t.Errorf("the objects producer is refused: %v", err)
	}
	var spans []Span
	// Out of order on purpose.
	spans = append(spans, call("o3", "shop-front", "shop-cart", at(9, 0))...)
	spans = append(spans, call("o1", "shop-front", "shop-cart", at(1, 0))...)
	spans = append(spans, call("o2", "shop-cart", "shop-db", at(4, 0))...)
	for i := range spans {
		spans[i].InstanceID, spans[i].PodUID = "pod-"+spans[i].Service, "pod-"+spans[i].Service
	}
	st, _ := load(t, Scenario{Spans: spans})

	recs := st.records()
	if len(recs) == 0 {
		t.Fatal("nothing written")
	}
	var last time.Time
	for i, r := range recs {
		if r.Producer != ProducerTraces || r.Kind != lifecycle.Observe || r.TTL != 5*time.Minute || r.EventTimeBasis != store.BasisProducerEvent {
			t.Errorf("record %d: producer %s, kind %s, TTL %s, basis %s", i, r.Producer, r.Kind, r.TTL, r.EventTimeBasis)
		}
		if r.EventTime.Before(last) {
			t.Fatalf("record %d at %s follows one at %s: a late event", i, r.EventTime, last)
		}
		last = r.EventTime
		if r.Seq != uint64(i+1) {
			t.Errorf("record %d has Seq %d", i, r.Seq)
		}
	}
}

// Nothing the loader writes depends on the order of the spans.
func TestTheRecordsDoNotDependOnTheOrderOfTheSpans(t *testing.T) {
	var spans []Span
	spans = append(spans, call("d1", "shop-front", "shop-cart", at(0, 5))...)
	spans = append(spans, call("d2", "shop-cart", "shop-db", at(0, 7))...)
	spans = append(spans, call("d3", "shop-front", "shop-cart", at(2, 5))...)
	rev := make([]Span, len(spans))
	for i, s := range spans {
		rev[len(spans)-1-i] = s
	}
	a, _ := load(t, Scenario{Spans: spans})
	b, _ := load(t, Scenario{Spans: rev})
	ra, rb := a.records(), b.records()
	if len(ra) != len(rb) {
		t.Fatalf("%d records, then %d", len(ra), len(rb))
	}
	for i := range ra {
		if ra[i].Subject != rb[i].Subject || !ra[i].EventTime.Equal(rb[i].EventTime) {
			t.Fatalf("record %d differs: %+v, %+v", i, ra[i], rb[i])
		}
	}
}
