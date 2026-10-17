package otlphttp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// metricsRecorder is a metrics sink that keeps what it was given and returns a
// set partial success and error.
type metricsRecorder struct {
	mu  sync.Mutex
	got []otlpmetrics.Data
	p   otlphttp.Partial
	err error
}

func (r *metricsRecorder) AcceptMetrics(_ context.Context, d otlpmetrics.Data) (otlphttp.Partial, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, d)
	return r.p, r.err
}

func (r *metricsRecorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// holdMetricsSink holds its first call until release is closed.
type holdMetricsSink struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func newHoldMetricsSink() *holdMetricsSink {
	return &holdMetricsSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *holdMetricsSink) AcceptMetrics(context.Context, otlpmetrics.Data) (otlphttp.Partial, error) {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		<-s.release
	}
	return otlphttp.Partial{}, nil
}

func newMetricsHandler(o otlphttp.Options) (http.Handler, *metricsRecorder) {
	r := &metricsRecorder{}
	return otlphttp.MetricsHandler(r, o), r
}

// doMetrics serves a request to /v1/metrics.
func doMetrics(t testing.TB, h http.Handler, q request) *httptest.ResponseRecorder {
	t.Helper()
	if q.path == "" {
		q.path = otlphttp.MetricsPath
	}
	return do(t, h, q)
}

// postPath serves one protobuf request whose body is read through a counting
// reader.
func postPath(t testing.TB, h http.Handler, path string, length int64, body []byte) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	return postTyped(t, h, path, protoType, length, body)
}

// postTyped is postPath with a Content-Type.
func postTyped(t testing.TB, h http.Handler, path, ctype string, length int64, body []byte) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	cr := &countingReader{r: bytes.NewReader(body)}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, cr)
	req.ContentLength = length
	req.Header.Set("Content-Type", ctype)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, cr.n
}

// ---- the sample: a metrics batch written out by hand ----

func skv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: pstr(v)}
}

func intPoint(start, tm uint64, v int64, attrs ...*commonpb.KeyValue) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes: attrs, StartTimeUnixNano: start, TimeUnixNano: tm,
		Value: &metricspb.NumberDataPoint_AsInt{AsInt: v},
	}
}

func doublePoint(start, tm uint64, v float64, attrs ...*commonpb.KeyValue) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes: attrs, StartTimeUnixNano: start, TimeUnixNano: tm,
		Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v},
	}
}

func sumMetric(name string, temporality metricspb.AggregationTemporality, monotonic bool, pts ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		AggregationTemporality: temporality, IsMonotonic: monotonic, DataPoints: pts,
	}}}
}

func scopeOf(name string, ms ...*metricspb.Metric) *metricspb.ScopeMetrics {
	return &metricspb.ScopeMetrics{Scope: &commonpb.InstrumentationScope{Name: name}, Metrics: ms}
}

func resourceOf(attrs []*commonpb.KeyValue, scopes ...*metricspb.ScopeMetrics) *metricspb.ResourceMetrics {
	return &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{Attributes: attrs}, ScopeMetrics: scopes}
}

const (
	cumulative = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	delta      = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
)

// sampleMetricsProto has two resources: the first holds a cumulative sum of
// integers and of doubles, a delta sum, and one metric of each kind that is
// dropped; the second holds a delta sum only.
func sampleMetricsProto() *metricspb.MetricsData {
	return &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
		resourceOf([]*commonpb.KeyValue{skv("k8s.cluster.name", "c1")},
			scopeOf("traces_service_graph",
				sumMetric("traces_service_graph_request_total", cumulative, true,
					intPoint(100, 1_000, 5, skv("client", "a"), skv("server", "b")),
					doublePoint(100, 2_000, 2.5, skv("client", "b"), skv("server", "c"))),
				sumMetric("not_monotonic", cumulative, false, intPoint(1, 9_000, 1), intPoint(1, 9_500, 2)),
				&metricspb.Metric{Name: "g", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{intPoint(0, 3_000, 1), intPoint(0, 10_000, 1)},
				}}},
				&metricspb.Metric{Name: "h", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					DataPoints: []*metricspb.HistogramDataPoint{{TimeUnixNano: 4_000}, {TimeUnixNano: 4_500}},
				}}},
				&metricspb.Metric{Name: "e", Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
					DataPoints: []*metricspb.ExponentialHistogramDataPoint{{TimeUnixNano: 5_000}},
				}}},
				&metricspb.Metric{Name: "s", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
					DataPoints: []*metricspb.SummaryDataPoint{{TimeUnixNano: 6_000}},
				}}},
			),
			scopeOf("other", sumMetric("trace.span.count", delta, true, intPoint(10, 7_000, 3))),
		),
		resourceOf(nil, scopeOf("x", sumMetric("m", delta, true, doublePoint(0, 8_000, 1)))),
	}}
}

// sampleMetricsData is the model the sample converts to, written out by hand.
func sampleMetricsData() otlpmetrics.Data {
	return otlpmetrics.Data{Resources: []otlpmetrics.Resource{{
		Attributes:         []capture.KeyValue{{Key: "k8s.cluster.name", Value: cstr("c1")}},
		NewestTimeUnixNano: 10_000,
		Points: []otlpmetrics.SumPoint{
			{
				Scope: "traces_service_graph", Metric: "traces_service_graph_request_total", Temporality: otlpmetrics.Cumulative, Monotonic: true,
				StartTimeUnixNano: 100, TimeUnixNano: 1_000, Value: 5,
				Attributes: []capture.KeyValue{{Key: "client", Value: cstr("a")}, {Key: "server", Value: cstr("b")}},
			},
			{
				Scope: "traces_service_graph", Metric: "traces_service_graph_request_total", Temporality: otlpmetrics.Cumulative, Monotonic: true,
				StartTimeUnixNano: 100, TimeUnixNano: 2_000, Value: 2.5,
				Attributes: []capture.KeyValue{{Key: "client", Value: cstr("b")}, {Key: "server", Value: cstr("c")}},
			},
			{
				Scope: "other", Metric: "trace.span.count", Temporality: otlpmetrics.Delta, Monotonic: true,
				StartTimeUnixNano: 10, TimeUnixNano: 7_000, Value: 3,
			},
		},
		DroppedNotSum:       6,
		DroppedNonMonotonic: 2,
	}, {
		NewestTimeUnixNano: 8_000,
		Points: []otlpmetrics.SumPoint{
			{Scope: "x", Metric: "m", Temporality: otlpmetrics.Delta, Monotonic: true, TimeUnixNano: 8_000, Value: 1},
		},
	}}}
}
