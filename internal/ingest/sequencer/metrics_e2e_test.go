package sequencer_test

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// toProto writes a request of the narrow model as the protobuf the shipped
// collector sends: one metric for each point, in a scope of its own. Only string
// attributes are written.
func toProto(t testing.TB, d otlpmetrics.Data) []byte {
	t.Helper()
	attrs := func(kvs []capture.KeyValue) []*commonpb.KeyValue {
		var out []*commonpb.KeyValue
		for _, kv := range kvs {
			out = append(out, &commonpb.KeyValue{Key: kv.Key, Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: *kv.Value.StringValue},
			}})
		}
		return out
	}
	var msg metricspb.MetricsData
	for _, r := range d.Resources {
		rm := &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{Attributes: attrs(r.Attributes)}}
		for _, p := range r.Points {
			temporality := metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
			if p.Temporality == otlpmetrics.Cumulative {
				temporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
			}
			pt := &metricspb.NumberDataPoint{
				Attributes: attrs(p.Attributes), StartTimeUnixNano: p.StartTimeUnixNano, TimeUnixNano: p.TimeUnixNano,
			}
			if p.Value == math.Trunc(p.Value) {
				pt.Value = &metricspb.NumberDataPoint_AsInt{AsInt: int64(p.Value)}
			} else {
				pt.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: p.Value}
			}
			rm.ScopeMetrics = append(rm.ScopeMetrics, &metricspb.ScopeMetrics{
				Scope: &commonpb.InstrumentationScope{Name: p.Scope},
				Metrics: []*metricspb.Metric{{Name: p.Metric, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: temporality, IsMonotonic: p.Monotonic, DataPoints: []*metricspb.NumberDataPoint{pt},
				}}}},
			})
		}
		msg.ResourceMetrics = append(msg.ResourceMetrics, rm)
	}
	b, err := proto.Marshal(&msg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// postMetrics sends the request through the receiver's handler, as protobuf, and
// fails the test unless it is answered 200.
func (h *harness) postMetrics(handler http.Handler, d otlpmetrics.Data) {
	h.t.Helper()
	req := httptest.NewRequestWithContext(h.t.Context(), http.MethodPost, otlphttp.MetricsPath, bytes.NewReader(toProto(h.t, d)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		h.t.Fatalf("POST /v1/metrics at %s: status %d, body %q", h.clock.now().Sub(t0), rec.Code, rec.Body.String())
	}
}

// The e2e world: three services, four pods and two nodes. web calls api and api
// calls db. api runs two instances, one on each node, and web and db one each.
var (
	e2eN1 = node{"node-1", "n1", t0.Add(-3 * time.Hour)}
	e2eN2 = node{"node-2", "n2", t0.Add(-3 * time.Hour)}

	e2eWebPod = pod{"uid-web-1", "web-1", "n1", t0.Add(-2 * time.Hour), t0.Add(-2 * time.Hour), "Running"}
	e2eAPIPod = pod{"uid-api-1", "api-1", "n1", t0.Add(-2 * time.Hour), t0.Add(-2 * time.Hour), "Running"}
	e2eAPI2   = pod{"uid-api-2", "api-2", "n2", t0.Add(-2 * time.Hour), t0.Add(-2 * time.Hour), "Running"}
	e2eDBPod  = pod{"uid-db-1", "db-1", "n2", t0.Add(-2 * time.Hour), t0.Add(-2 * time.Hour), "Running"}

	webAPI = edge{"web", "shop", "api", "backend"}
	apiDB  = edge{"api", "backend", "db", "data"}
)

// e2e runs the world through the receiver: the pods and nodes as logs requests,
// the calls and the span counts as protobuf metrics requests through
// otlphttp.MetricsHandler, at the given minutes. It returns the harness, stopped,
// and the store.
func e2e(t *testing.T, calls ...time.Duration) (*harness, *memstore.Store, *fakeStore) {
	t.Helper()
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st).start()
	handler := otlphttp.MetricsHandler(h.seq, otlphttp.Options{})
	h.setNow(0)
	h.sendOK(logs("", pullNode(t0, e2eN1), pullNode(t0, e2eN2),
		pullPod(t0, e2eWebPod), pullPod(t0, e2eAPIPod), pullPod(t0, e2eAPI2), pullPod(t0, e2eDBPod)))
	// The first points of the two series, which a new translator sets aside.
	h.postMetrics(handler, otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		mres(nil, webAPI.cum(t0, 1), apiDB.cum(t0, 1)),
	}})
	for i, d := range calls {
		h.setNow(d)
		h.sendOK(logs("", pullPod(at(d), e2eWebPod), pullPod(at(d), e2eAPIPod), pullPod(at(d), e2eAPI2), pullPod(at(d), e2eDBPod)))
		h.postMetrics(handler, otlpmetrics.Data{Resources: []otlpmetrics.Resource{
			mres(nil, webAPI.cum(at(d), float64(2+i)), apiDB.cum(at(d), float64(2+i))),
			spanRes("", "shop", "web", "web-i1", e2eWebPod.uid, spanPoint(at(d), 5)),
			spanRes("", "backend", "api", "api-i1", e2eAPIPod.uid, spanPoint(at(d), 5)),
			spanRes("", "backend", "api", "api-i2", e2eAPI2.uid, spanPoint(at(d), 5)),
			spanRes("", "data", "db", "db-i1", e2eDBPod.uid, spanPoint(at(d), 5)),
		}})
	}
	h.stop()
	return h, mem, st
}

func memberRoles(res extract.Result) map[identity.Fingerprint]extract.Role {
	out := map[identity.Fingerprint]extract.Role{}
	for _, m := range res.Members {
		out[m.Service] = m.Role
	}
	return out
}

// pairs are the service pairs of an extraction, as the set of the two services
// and the mechanism.
func pairSet(res extract.Result) map[[3]string]bool {
	out := map[[3]string]bool{}
	for _, p := range res.Pairs {
		a, b := p.Member.String(), p.Peer.String()
		if a > b {
			a, b = b, a
		}
		out[[3]string{a, b, p.Mechanism.String()}] = true
	}
	return out
}

func pairKey(a, b identity.Fingerprint, mech string) [3]string {
	x, y := a.String(), b.String()
	if x > y {
		x, y = y, x
	}
	return [3]string{x, y, mech}
}

// The whole path: protobuf over HTTP, the receiver's handler, the sequencer,
// a store. The pods and nodes arrive as logs, the calls and span counts as
// metrics; the extraction from a service seed lists exactly the services that
// call it and that it calls, and the pairs that shared a node.
func TestMetricsEndToEnd(t *testing.T) {
	_, mem, _ := e2e(t, time.Minute, 2*time.Minute)
	web, api, db := svcFP(t, "shop", "web"), svcFP(t, "backend", "api"), svcFP(t, "data", "db")
	w := interval.Window{From: at(150 * time.Second), To: at(3 * time.Minute), AsOf: store.Latest}
	ex := extract.NewExtractor(mem)

	res, err := ex.Extract(context.Background(), extract.Exploratory{Service: web}, w, extract.Options{NodeOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := memberRoles(res), map[identity.Fingerprint]extract.Role{web: extract.RoleSeed, api: extract.RoleCallee}; !mapsEqual(got, want) {
		t.Errorf("from web: members %v, want web as the seed and api as a callee", got)
	}
	if got, want := pairSet(res), map[[3]string]bool{
		pairKey(web, api, "shared_node"): true, pairKey(api, db, "shared_node"): true,
	}; !mapsEqual(got, want) {
		t.Errorf("from web: pairs %v, want web and api on n1, api and db on n2", got)
	}

	res, err = ex.Extract(context.Background(), extract.Exploratory{Service: db}, w, extract.Options{NodeOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := memberRoles(res), map[identity.Fingerprint]extract.Role{db: extract.RoleSeed, api: extract.RoleCaller}; !mapsEqual(got, want) {
		t.Errorf("from db: members %v, want db as the seed and api as a caller", got)
	}
	if got, want := pairSet(res), map[[3]string]bool{
		pairKey(db, api, "shared_node"): true, pairKey(api, web, "shared_node"): true,
	}; !mapsEqual(got, want) {
		t.Errorf("from db: pairs %v", got)
	}

	// The dependency, read with the cascade, is cut by both services' existence.
	r := interval.NewReader(mem, interval.Options{})
	long := interval.Window{From: at(0), To: at(24 * time.Hour), AsOf: store.Latest}
	edges, err := r.Edges(context.Background(), web, store.Forward, long)
	if err != nil {
		t.Fatal(err)
	}
	var dep *interval.Element
	for i, el := range edges.Elements {
		if el.Subject == store.EdgeSubject(web, api, catalog.DependsOn) {
			dep = &edges.Elements[i]
		}
	}
	if dep == nil || len(dep.Pieces) != 1 {
		t.Fatalf("the dependency web to api: %+v", edges.Elements)
	}
	for _, s := range []identity.Fingerprint{web, api} {
		ents, err := r.Entity(context.Background(), s, long)
		if err != nil {
			t.Fatal(err)
		}
		inside := false
		for _, p := range ents.Elements[0].Pieces {
			inside = inside || (!p.Start.After(dep.Pieces[0].Start) && !p.End.Before(dep.Pieces[0].End))
		}
		if !inside {
			t.Errorf("the dependency's piece %v is not inside the existence of %s: %v", dep.Pieces[0].Span, s, ents.Elements[0].Pieces)
		}
	}
}

func mapsEqual[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// A steady edge, whose counter stays where it was, is not sighted again: no
// record follows the last increase, and the edge ends at that increase plus the
// dependency expiry, as a liveness expiry.
func TestMetricsSteadyEdgeEndsAtTheLastIncreasePlusTheExpiry(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st, withConfig(func(c *sequencer.Config) { c.DependencyExpiry = 6 * time.Hour })).start()
	h.setNow(0)
	h.sendMOK(mreq("", webAPI.cum(at(0), 1)))
	h.setNow(time.Minute)
	h.sendMOK(mreq("", webAPI.cum(at(time.Minute), 2)))
	h.setNow(2 * time.Minute)
	h.sendMOK(mreq("", webAPI.cum(at(2*time.Minute), 3)))
	before := len(st.records())
	// The connector re-exports a series at each flush for fifteen minutes after its
	// last update, and drops it after that.
	for m := 3; m <= 15; m++ {
		d := time.Duration(m) * time.Minute
		h.setNow(d)
		h.sendMOK(mreq("", webAPI.cum(at(d), 3)))
		if m%5 == 0 {
			h.tick()
		}
	}
	h.stop()
	for _, r := range st.records()[before:] {
		if r.EventTime.After(at(2 * time.Minute)) {
			t.Fatalf("a record after the last increase: %s", show(r))
		}
	}
	web, api := svcFP(t, "shop", "web"), svcFP(t, "backend", "api")
	r := interval.NewReader(mem, interval.Options{})
	res, err := r.Edges(context.Background(), web, store.Forward, interval.Window{From: at(0), To: at(24 * time.Hour), AsOf: store.Latest})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Elements) != 1 || res.Elements[0].Subject != store.EdgeSubject(web, api, catalog.DependsOn) || len(res.Elements[0].Pieces) != 1 {
		t.Fatalf("edges %+v", res.Elements)
	}
	p := res.Elements[0].Pieces[0]
	if !p.Start.Equal(at(time.Minute)) || !p.End.Equal(at(2*time.Minute+6*time.Hour)) || p.EndSource != lifecycle.EndLivenessExpiry {
		t.Errorf("the edge is [%s, %s) ending by %s, want [1m, 2m+6h) ending by liveness_expiry",
			p.Start.Sub(t0), p.End.Sub(t0), p.EndSource)
	}
}

// The writers of metrics assert services, their instances, and the edges
// between them and to the pod: no record names a network endpoint, so no answer,
// refusal or status of this path can carry an endpoint id.
func TestMetricsRecordsNameNoEndpoint(t *testing.T) {
	_, _, st := e2e(t, time.Minute, 2*time.Minute)
	entity := []catalog.EntityType{catalog.Service, catalog.ServiceInstance}
	seen := 0
	for _, r := range st.records() {
		if !isMetricsProducer(r.Producer) {
			continue
		}
		seen++
		a := r.Subject.A.Type()
		switch r.Subject.Kind {
		case store.SubjectEntity:
			if !slices.Contains(entity, a) {
				t.Errorf("an entity of type %s: %s", a, show(r))
			}
		case store.SubjectEdge:
			b := r.Subject.B.Type()
			ok := (r.Subject.Relation == catalog.DependsOn && a == catalog.Service && b == catalog.Service) ||
				(r.Subject.Relation == catalog.InstanceOf && a == catalog.ServiceInstance && b == catalog.Service) ||
				(r.Subject.Relation == catalog.PartOf && a == catalog.ServiceInstance && b == catalog.K8sPod)
			if !ok {
				t.Errorf("an edge %s of %s to %s: %s", r.Subject.Relation, a, b, show(r))
			}
		}
		if a == catalog.NetworkEndpoint || (r.Subject.Kind == store.SubjectEdge && r.Subject.B.Type() == catalog.NetworkEndpoint) {
			t.Errorf("a record names an endpoint: %s", show(r))
		}
	}
	if seen == 0 {
		t.Fatal("no record of a metrics producer")
	}
}

func isMetricsProducer(p lifecycle.Producer) bool {
	return strings.HasPrefix(string(p), "svcgraph/") || strings.HasPrefix(string(p), "spancount/")
}
