package demo

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
)

// The service layer is data laid over the L2 cluster: with it taken away, the world
// of a seed is the world it was, field by field, pod by pod. It is checked for the
// seeds the shape is checked for.
func TestTheServiceLayerIsDataOverTheSameCluster(t *testing.T) {
	for _, seed := range seeds {
		with, without := NewWorld(seed), newWorld(seed, false)
		stripped := *with
		stripped.Workloads = slices.Clone(with.Workloads)
		for i := range stripped.Workloads {
			stripped.Workloads[i].Service = ""
		}
		stripped.Calls = nil
		if !reflect.DeepEqual(&stripped, without) {
			t.Errorf("seed %d: the cluster with the service layer taken away is not the cluster without it", seed)
		}
		if len(without.Calls) != 0 {
			t.Errorf("seed %d: the cluster without the layer has %d calls", seed, len(without.Calls))
		}
	}
}

// Ten workloads are services, in three namespaces; the databases, the metrics agent,
// the report Job and the cluster's own components are not.
func TestTheServicesAreTheInstrumentedWorkloads(t *testing.T) {
	want := map[string][]string{
		"platform": {"ingress"},
		"shop":     {"cart", "catalog", "checkout", "frontend", "recommend", "search"},
		"payments": {"ledger", "legacy-pay", "payments-api"},
	}
	for _, seed := range seeds {
		w := NewWorld(seed)
		got := map[string][]string{}
		for _, wl := range w.Workloads {
			if wl.Service != "" {
				if wl.Service != wl.Name {
					t.Errorf("seed %d: %s/%s implements the service %s, want the one of its own name", seed, wl.Namespace, wl.Name, wl.Service)
				}
				got[wl.Namespace] = append(got[wl.Namespace], wl.Name)
			}
		}
		for _, names := range got {
			slices.Sort(names)
		}
		for _, names := range want {
			slices.Sort(names)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("seed %d: the services are %v, want %v", seed, got, want)
		}

		// An instance is a pod, and no two pods hold one id.
		ids := map[string]string{}
		instances := 0
		for _, p := range w.Pods {
			in, ok := w.Instance(p)
			if ok != (w.Workloads[p.Workload].Service != "") {
				t.Errorf("seed %d: %s is an instance %v, its workload is a service %v", seed, p.Name, ok, !ok)
			}
			if !ok {
				continue
			}
			instances++
			if in.PodUID != p.UID || in.Version != p.ImageTag || in.ID == "" {
				t.Errorf("seed %d: the instance of %s is %+v", seed, p.Name, in)
			}
			if other, dup := ids[in.ID]; dup {
				t.Errorf("seed %d: the instance id %s is held by %s and by %s", seed, in.ID, other, p.Name)
			}
			ids[in.ID] = p.Name
		}
		// 26 pods of services at the anchor, and their replacements.
		atAnchor := 0
		for _, p := range w.Pods {
			if _, ok := w.Instance(p); ok && p.phaseAt(anchor) == phaseRunning {
				atAnchor++
			}
		}
		if atAnchor != 26 {
			t.Errorf("seed %d: %d instances at the anchor, want 26", seed, atAnchor)
		}

		// The two pods the story names on node-2 are pods of services.
		s := w.Story()
		for _, ref := range []PodRef{s.Checkout, s.LegacyPay} {
			var found bool
			for _, p := range w.Pods {
				if p.UID == ref.UID {
					_, found = w.Instance(p)
				}
			}
			if !found {
				t.Errorf("seed %d: %s on node-2 is not a pod of a service", seed, ref.Name)
			}
		}
	}
}

// The calls are the ones written out below, and the numbers the
// oracle's rules rely on hold: a call starts and ends on a whole minute, and a
// series that starts after the anchor starts after the translator's warm-up of two
// flushes, because one inside it would be set aside and sighted a flush late.
func TestTheCallsAreTheDesignedOnes(t *testing.T) {
	type call struct {
		client, server string
		from, to       time.Duration
		db             bool
	}
	const whole, end = -2 * time.Hour, 6 * time.Hour
	want := []call{
		{"platform/ingress", "shop/frontend", whole, end, false},
		{"shop/frontend", "shop/catalog", whole, end, false},
		{"shop/frontend", "shop/search", whole, end, false},
		{"shop/frontend", "shop/recommend", whole, end, false},
		{"shop/frontend", "shop/cart", whole, end, false},
		{"shop/frontend", "shop/checkout", whole, end, false},
		{"shop/search", "shop/catalog", whole, end, false},
		{"shop/recommend", "shop/catalog", whole, end, false},
		{"shop/checkout", "shop/cart", whole, end, false},
		{"shop/checkout", "payments/legacy-pay", whole, 2*time.Hour + 10*time.Minute, false},
		{"shop/checkout", "payments/payments-api", 2 * time.Hour, end, false},
		{"payments/legacy-pay", "payments/ledger", whole, end, false},
		{"payments/payments-api", "payments/ledger", 2 * time.Hour, end, false},
		{"shop/cart", "/redis", whole, end, true},
		{"payments/ledger", "/db", whole, end, true},
	}
	for _, seed := range seeds {
		w := NewWorld(seed)
		var got []call
		for _, c := range w.Calls {
			got = append(got, call{c.ClientNamespace + "/" + c.Client, c.ServerNamespace + "/" + c.Server, c.From.Sub(anchor), c.To.Sub(anchor), c.Database})
			if c.Rate <= 0 || c.From.Nanosecond() != 0 || c.From.Sub(anchor)%time.Minute != 0 || c.To.Sub(anchor)%time.Minute != 0 || c.To.Before(c.From) {
				t.Errorf("seed %d: the call %+v is not whole minutes at a rate", seed, c)
			}
			if c.Database {
				continue
			}
			// The first flush that sees the call, and whether it is past the warm-up.
			first := -1
			for k := 0; k < oFlushes && first < 0; k++ {
				if !oFlushAt(k).Before(c.From) {
					first = k
				}
			}
			if c.From.After(anchor) && first < 2 {
				t.Errorf("seed %d: the call %s -> %s first appears at flush %d, inside the translator's warm-up", seed, c.Client, c.Server, first)
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("seed %d: the calls are\n%v\nwant\n%v", seed, got, want)
		}
	}
}

// ---- the requests of the trace collector ----

// metricsRequests are the world's metrics requests, with their instants.
func metricsRequests(w *World) []Request {
	var out []Request
	for _, r := range w.Requests() {
		if r.IsMetrics() {
			out = append(out, r)
		}
	}
	return out
}

// A service graph series is a cumulative counter from the collector's start: its
// start time never moves and its value never falls; it is sent at every flush from
// its first call until fifteen minutes after its last, and at no other; a flush is
// one resource of the one cluster; and the call that ends is last sent at the
// 144th flush and not at the 145th.
func TestTheServiceGraphFlushesAreTheConnectors(t *testing.T) {
	w := NewWorld(DefaultSeed)
	type point struct {
		k     int
		start uint64
		value float64
	}
	series := map[string][]point{}
	flushes := 0
	for _, r := range metricsRequests(w) {
		if r.Emitter != EmitServiceGraph {
			continue
		}
		k := flushes
		flushes++
		d := r.Metrics()
		if !r.At.Equal(oFlushAt(k).Add(time.Second)) {
			t.Fatalf("flush %d arrives at %s, want a second after %s", k, r.At, oFlushAt(k))
		}
		if len(d.Resources) != 1 {
			t.Fatalf("flush %d has %d resources, want one", k, len(d.Resources))
		}
		res := d.Resources[0]
		if len(res.Attributes) != 1 || res.Attributes[0].Key != "k8s.cluster.name" || *res.Attributes[0].Value.StringValue != ClusterName {
			t.Errorf("flush %d: resource attributes %v, want the cluster name only", k, res.Attributes)
		}
		for i, p := range res.Points {
			if p.Scope != "traces_service_graph" || p.Metric != "traces_service_graph_request_total" ||
				p.Temporality != otlpmetrics.Cumulative || !p.Monotonic || p.TimeUnixNano != uint64(oFlushAt(k).UnixNano()) || res.NewestTimeUnixNano != p.TimeUnixNano {
				t.Fatalf("flush %d point %d: %+v", k, i, p)
			}
			var key []string
			for _, kv := range p.Attributes {
				key = append(key, kv.Key+"="+*kv.Value.StringValue)
			}
			wantKeys := "client server connection_type failed client_service.namespace server_service.namespace virtual_node"
			var names []string
			for _, kv := range p.Attributes {
				names = append(names, kv.Key)
			}
			if strings.Join(names, " ") != wantKeys {
				t.Fatalf("flush %d point %d has attributes %v, want %s", k, i, names, wantKeys)
			}
			if i > 0 && orderOf(p) <= orderOf(res.Points[i-1]) {
				t.Errorf("flush %d: the series are not in order of client and server", k)
			}
			series[strings.Join(key, ",")] = append(series[strings.Join(key, ",")], point{k, p.StartTimeUnixNano, p.Value})
		}
	}
	if flushes != 360 {
		t.Fatalf("%d flushes, want 360", flushes)
	}

	for _, c := range w.Calls {
		connection := ""
		if c.Database {
			connection = "database"
		}
		key := strings.Join([]string{
			"client=" + c.Client, "server=" + c.Server, "connection_type=" + connection, "failed=false",
			"client_service.namespace=" + c.ClientNamespace, "server_service.namespace=" + c.ServerNamespace, "virtual_node=",
		}, ",")
		pts := series[key]
		if len(pts) == 0 {
			t.Errorf("no series for %s -> %s", c.Client, c.Server)
			continue
		}
		// The expected flushes, by counting minutes: the series is sent while its last
		// call is at most fifteen minutes before the flush.
		var wantK []int
		for k := 0; k < 360; k++ {
			f := oFlushAt(k)
			var last time.Time
			for m := c.From; !m.After(c.To) && !m.After(f); m = m.Add(time.Minute) {
				last = m
			}
			if !last.IsZero() && f.Sub(last) <= 15*time.Minute {
				wantK = append(wantK, k)
			}
		}
		var gotK []int
		for i, p := range pts {
			gotK = append(gotK, p.k)
			if p.start != uint64(anchor.Add(-2*time.Hour).UnixNano()) {
				t.Errorf("%s -> %s: start time %d at flush %d, want the collector's start", c.Client, c.Server, p.start, p.k)
			}
			if i > 0 && p.value < pts[i-1].value {
				t.Errorf("%s -> %s: the value fell from %v to %v at flush %d", c.Client, c.Server, pts[i-1].value, p.value, p.k)
			}
			if p.value <= 0 {
				t.Errorf("%s -> %s: value %v", c.Client, c.Server, p.value)
			}
		}
		if !slices.Equal(gotK, wantK) {
			t.Errorf("%s -> %s is sent at flushes %v..%v (%d), want %v..%v (%d)", c.Client, c.Server, gotK[0], gotK[len(gotK)-1], len(gotK), wantK[0], wantK[len(wantK)-1], len(wantK))
		}
		if c.Client == "checkout" && c.Server == "legacy-pay" && (gotK[len(gotK)-1] != 144) {
			t.Errorf("checkout -> legacy-pay is last sent at flush %d, want 144", gotK[len(gotK)-1])
		}
	}
}

// orderOf is the key the series of a flush are sorted by: the client's namespace
// and name, then the server's.
func orderOf(p otlpmetrics.SumPoint) string {
	attrs := map[string]string{}
	for _, kv := range p.Attributes {
		attrs[kv.Key] = *kv.Value.StringValue
	}
	return strings.Join([]string{attrs["client_service.namespace"], attrs["client"], attrs["server_service.namespace"], attrs["server"]}, "/")
}

// A span count resource is an instance whose pod runs on a node that is ready at
// the instant, with the six attributes the translator reads, one delta point of the
// minute that ends there, and nothing else; every instance that should be there is.
func TestTheSpanCountsAreOfInstancesThatRun(t *testing.T) {
	w := NewWorld(DefaultSeed)
	byUID := map[string]*Pod{}
	for _, p := range w.Pods {
		byUID[p.UID] = p
	}
	wantKeys := []string{"k8s.cluster.name", "service.name", "service.namespace", "service.instance.id", "service.version", "k8s.pod.uid"}
	k, resources := 0, 0
	for _, r := range metricsRequests(w) {
		if r.Emitter != EmitSpanCount {
			continue
		}
		at := oSpanAt(k)
		if !r.At.Equal(at.Add(time.Second)) {
			t.Fatalf("span count %d arrives at %s, want a second after %s", k, r.At, at)
		}
		sent := map[string]bool{}
		for _, res := range r.Metrics().Resources {
			resources++
			var keys []string
			attrs := map[string]string{}
			for _, kv := range res.Attributes {
				keys = append(keys, kv.Key)
				attrs[kv.Key] = *kv.Value.StringValue
			}
			if !slices.Equal(keys, wantKeys) || attrs["k8s.cluster.name"] != ClusterName {
				t.Fatalf("span count %d: attributes %v", k, res.Attributes)
			}
			p := byUID[attrs["k8s.pod.uid"]]
			if p == nil || !podSends(w, p, at) {
				t.Errorf("span count %d: the pod %s does not run on a ready node at %s", k, attrs["k8s.pod.uid"], at)
				continue
			}
			wl := w.Workloads[p.Workload]
			if attrs["service.name"] != wl.Name || attrs["service.namespace"] != wl.Namespace ||
				attrs["service.instance.id"] != wl.Namespace+"."+p.Name+"."+wl.Name || attrs["service.version"] != p.ImageTag {
				t.Errorf("span count %d: attributes %v for %s", k, attrs, p.Name)
			}
			sent[p.UID] = true
			if len(res.Points) != 1 {
				t.Fatalf("span count %d: %d points", k, len(res.Points))
			}
			pt := res.Points[0]
			if pt.Metric != "trace.span.count" || pt.Temporality != otlpmetrics.Delta || !pt.Monotonic || pt.Value <= 0 ||
				pt.TimeUnixNano != uint64(at.UnixNano()) || pt.StartTimeUnixNano != uint64(at.Add(-time.Minute).UnixNano()) ||
				pt.Attributes != nil || res.NewestTimeUnixNano != pt.TimeUnixNano {
				t.Errorf("span count %d: point %+v", k, pt)
			}
		}
		// Every pod of a service that runs on a ready node is sent.
		for _, p := range w.Pods {
			if _, ok := w.Instance(p); ok && podSends(w, p, at) && !sent[p.UID] {
				t.Errorf("span count %d: %s runs and is not sent", k, p.Name)
			}
		}
		k++
	}
	if k != 360 || resources == 0 {
		t.Errorf("%d span count requests with %d resources, want 360 requests", k, resources)
	}

	// Node-2's instances stop with the last span count before it is not ready.
	last := map[string]time.Time{}
	for k := 0; k < 360; k++ {
		for _, p := range w.Pods {
			if _, ok := w.Instance(p); ok && p.Node == failedNode && podSends(w, p, oSpanAt(k)) {
				last[p.UID] = oSpanAt(k)
			}
		}
	}
	if len(last) == 0 {
		t.Fatal("no instance on node-2")
	}
	for uid, at := range last {
		if !at.Equal(anchor.Add(2*time.Hour + 59*time.Minute + 50*time.Second)) {
			t.Errorf("the instance of %s on node-2 is last sent at %s, want 02:59:50Z", byUID[uid].Name, at)
		}
	}
}

// Requests are in order of instant, and at one instant of the emitters.
func TestTheMetricsRequestsAreInOrder(t *testing.T) {
	w := NewWorld(DefaultSeed)
	reqs := w.Requests()
	if len(metricsRequests(w)) != 720 {
		t.Errorf("%d metrics requests, want 720", len(metricsRequests(w)))
	}
	for i := 1; i < len(reqs); i++ {
		a, b := reqs[i-1], reqs[i]
		if b.At.Before(a.At) || (b.At.Equal(a.At) && b.Emitter < a.Emitter) {
			t.Fatalf("%s at %s comes after %s at %s", b.What, b.At, a.What, a.At)
		}
		if b.At.Nanosecond() != 0 {
			t.Errorf("%s at %s is not at a whole second", b.What, b.At)
		}
	}
	for _, r := range reqs {
		if r.IsMetrics() != (r.Emitter == EmitServiceGraph || r.Emitter == EmitSpanCount) {
			t.Errorf("%s: metrics %v from %s", r.What, r.IsMetrics(), r.Emitter)
		}
		if r.IsMetrics() && len(r.Data().ResourceLogs) != 0 {
			t.Errorf("%s: a metrics request has logs", r.What)
		}
		if !r.IsMetrics() && len(r.Metrics().Resources) != 0 {
			t.Errorf("%s: a logs request has metrics", r.What)
		}
	}
}

// ---- the receiver's image ----

func pstr(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

// toProto writes the narrow model as the protobuf a collector would send: the
// points of a resource grouped by scope and metric in the order they come, as a
// sum each. It is the test's own encoder, one to one with the model.
func toProto(t testing.TB, d otlpmetrics.Data) *metricspb.MetricsData {
	t.Helper()
	out := &metricspb.MetricsData{}
	for _, res := range d.Resources {
		rm := &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{}}
		for _, kv := range res.Attributes {
			if kv.Value.StringValue == nil {
				t.Fatalf("attribute %s is not a string", kv.Key)
			}
			rm.Resource.Attributes = append(rm.Resource.Attributes, &commonpb.KeyValue{Key: kv.Key, Value: pstr(*kv.Value.StringValue)})
		}
		var scope *metricspb.ScopeMetrics
		var metric *metricspb.Metric
		var lastScope, lastMetric string
		for _, p := range res.Points {
			if scope == nil || p.Scope != lastScope {
				scope = &metricspb.ScopeMetrics{Scope: &commonpb.InstrumentationScope{Name: p.Scope}}
				rm.ScopeMetrics = append(rm.ScopeMetrics, scope)
				metric = nil
			}
			if metric == nil || p.Metric != lastMetric {
				temporality := metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED
				switch p.Temporality {
				case otlpmetrics.Delta:
					temporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
				case otlpmetrics.Cumulative:
					temporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
				}
				metric = &metricspb.Metric{Name: p.Metric, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{AggregationTemporality: temporality, IsMonotonic: p.Monotonic}}}
				scope.Metrics = append(scope.Metrics, metric)
			}
			lastScope, lastMetric = p.Scope, p.Metric
			dp := &metricspb.NumberDataPoint{
				StartTimeUnixNano: p.StartTimeUnixNano, TimeUnixNano: p.TimeUnixNano,
				Value: &metricspb.NumberDataPoint_AsInt{AsInt: int64(p.Value)},
			}
			if float64(int64(p.Value)) != p.Value {
				t.Fatalf("value %v is not a whole number", p.Value)
			}
			for _, kv := range p.Attributes {
				dp.Attributes = append(dp.Attributes, &commonpb.KeyValue{Key: kv.Key, Value: pstr(*kv.Value.StringValue)})
			}
			sum := metric.GetSum()
			sum.DataPoints = append(sum.DataPoints, dp)
		}
		out.ResourceMetrics = append(out.ResourceMetrics, rm)
	}
	return out
}

// sink records what the receiver's handler hands the sequencer.
type sink struct {
	mu  sync.Mutex
	got []otlpmetrics.Data
}

func (s *sink) AcceptMetrics(_ context.Context, d otlpmetrics.Data) (otlphttp.Partial, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, d)
	return otlphttp.Partial{}, nil
}

// Every metrics request of the first two hours, written as the protobuf a collector
// sends and posted to the receiver's handler, comes out of it as the data the demo
// sends: so each is a body the receiver could have produced. It catches a point
// whose time the converter would read differently, a sum that is not monotonic, a
// value or an attribute the converter would not make, and a nil slice where it makes
// an empty one, or the other way. It cannot catch a wrong temporality or order of
// attributes, which the encoder here maps one to one; the translators' own rules do.
func TestEveryMetricsRequestIsOneTheReceiverCouldHaveProduced(t *testing.T) {
	w := NewWorld(DefaultSeed)
	var rec sink
	h := otlphttp.MetricsHandler(&rec, otlphttp.Options{})
	n := 0
	for _, r := range metricsRequests(w) {
		if r.At.After(anchor.Add(2 * time.Hour)) {
			break
		}
		want := r.Metrics()
		body, err := proto.Marshal(toProto(t, want))
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.MetricsPath, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s at %s: status %d: %s", r.What, r.At, rr.Code, rr.Body)
		}
		n++
		rec.mu.Lock()
		got := rec.got[len(rec.got)-1]
		rec.mu.Unlock()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s at %s: the receiver decodes\n%+v\nthe demo sends\n%+v", r.What, r.At, got, want)
		}
	}
	if n != 240 {
		t.Errorf("%d requests posted, want 240 (two hours of two requests a minute)", n)
	}
}

// ---- what is printed ----

// The text says what the services are and how a call ends, names no service id and
// no pod in the instructions, builds the ledger's id from its namespace and its name
// as a request for it would, and claims no call fails.
func TestWhatIsPrintedAboutServices(t *testing.T) {
	b := canonicalBuild(t)
	ins, banner := b.Instructions(), b.Banner()
	t.Logf("the instructions are %d bytes", len(ins))
	if len(ins) > 1024 {
		t.Errorf("the instructions are %d bytes, want at most 1 KiB", len(ins))
	}
	id, err := resolver.Resolve(catalog.Service, []identity.Attr{
		{Key: catalog.ServiceNamespace, Value: "payments"}, {Key: catalog.ServiceName, Value: "ledger"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := id.Fingerprint().String()
	nameOnly, err := resolver.Resolve(catalog.Service, []identity.Attr{{Key: catalog.ServiceName, Value: "ledger"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(banner, "entity "+ledger+",") {
		t.Errorf("the banner does not give the ledger's id %s:\n%s", ledger, banner)
	}
	if strings.Contains(banner, nameOnly.Fingerprint().String()) {
		t.Errorf("the banner gives the id of a ledger with no namespace")
	}
	for _, s := range b.Story.Services {
		if strings.Contains(ins, s.ID) {
			t.Errorf("the instructions print the id of %s/%s", s.Namespace, s.Name)
		}
	}
	for _, want := range []string{"15 minutes", "6 hours", "type service", "blast_radius from a service", "service instances"} {
		if !strings.Contains(ins, want) {
			t.Errorf("the instructions lack %q:\n%s", want, ins)
		}
	}
	for _, want := range []string{"02:00:00Z", "02:10:00Z", "02:10:20Z", "02:25:20Z", "04:00:00Z", "04:20:00Z", "checkout starts calling payments-api"} {
		if !strings.Contains(banner, want) {
			t.Errorf("the banner lacks %q:\n%s", want, banner)
		}
	}
	for name, text := range map[string]string{"instructions": ins, "banner": banner} {
		for _, bad := range []string{"failed", "fails", "error", "unhealthy", "outage"} {
			if strings.Contains(strings.ToLower(text), bad) {
				t.Errorf("the %s says %q, but no health is stored", name, bad)
			}
		}
	}
}
