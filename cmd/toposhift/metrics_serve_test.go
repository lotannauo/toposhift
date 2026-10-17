package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// metricsBody is GET /status's metrics object.
type metricsBody struct {
	Metrics struct {
		Mode             string           `json:"mode"`
		DependencyExpiry string           `json:"dependency_expiry"`
		InstanceTTL      string           `json:"instance_ttl"`
		Counters         map[string]int64 `json:"counters"`
		Clusters         map[string]struct {
			LastServiceGraphPoint time.Time `json:"last_service_graph_point"`
			LastSpanCountPoint    time.Time `json:"last_span_count_point"`
			Silent                bool      `json:"silent"`
			Series                int       `json:"series"`
		} `json:"clusters"`
	} `json:"metrics"`
}

func getMetrics(t *testing.T, base string) metricsBody {
	t.Helper()
	code, body := getBody(t, base+"/status")
	var r metricsBody
	if err := json.Unmarshal([]byte(body), &r); code != http.StatusOK || err != nil {
		t.Fatalf("GET /status = %d %q (%v)", code, body, err)
	}
	return r
}

// stringKV is a string attribute.
func stringKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// serviceGraphRequest is a protobuf metrics request with one point of the service
// graph connector's counter, cumulative, at the given time.
func serviceGraphRequest(t testing.TB, at time.Time, value int64) []byte {
	t.Helper()
	point := &metricspb.NumberDataPoint{
		Attributes: []*commonpb.KeyValue{
			stringKV("client", "web"), stringKV("server", "api"), stringKV("connection_type", ""), stringKV("failed", "false"),
			stringKV("virtual_node", ""),
		},
		StartTimeUnixNano: uint64(at.Add(-time.Hour).UnixNano()),
		TimeUnixNano:      uint64(at.UnixNano()),
		Value:             &metricspb.NumberDataPoint_AsInt{AsInt: value},
	}
	b, err := proto.Marshal(&metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{Name: "traces_service_graph"},
			Metrics: []*metricspb.Metric{{Name: "traces_service_graph_request_total", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				IsMonotonic:            true,
				DataPoints:             []*metricspb.NumberDataPoint{point},
			}}}},
		}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// postMetrics sends a body to /v1/metrics with a Content-Type.
func postMetrics(t testing.TB, base, ctype string, body []byte) (status int, retryAfter, text string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/v1/metrics", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ctype)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var st struct {
		Message *string `json:"message"`
	}
	text = string(b)
	if json.Unmarshal(b, &st) == nil && st.Message != nil {
		text = *st.Message
	}
	return resp.StatusCode, resp.Header.Get("Retry-After"), text
}

// The dependency expiry and the instance TTL default to six hours, the
// variables set them, a flag beats a variable, and a value that is not a duration,
// has a part of a second or is outside two minutes to 8784 hours is a mistake in how
// serve was called: exit 2 before anything listens.
func TestServeDependencyExpiryAndInstanceTTL(t *testing.T) {
	for _, tt := range []struct {
		name         string
		env          map[string]string
		args         []string
		wantExpiry   string
		wantInstance string
	}{
		{"defaults", nil, nil, "6h0m0s", "6h0m0s"},
		{"empty variables", map[string]string{"TOPOSHIFT_DEPENDENCY_EXPIRY": "", "TOPOSHIFT_INSTANCE_TTL": ""}, nil, "6h0m0s", "6h0m0s"},
		{"variables", map[string]string{"TOPOSHIFT_DEPENDENCY_EXPIRY": "90m", "TOPOSHIFT_INSTANCE_TTL": "45m"}, nil, "1h30m0s", "45m0s"},
		{
			"flags beat variables",
			map[string]string{"TOPOSHIFT_DEPENDENCY_EXPIRY": "90m", "TOPOSHIFT_INSTANCE_TTL": "45m"},
			[]string{"--dependency-expiry", "15m", "--instance-ttl", "2m"},
			"15m0s", "2m0s",
		},
		{"the bounds", nil, []string{"--dependency-expiry", "8784h", "--instance-ttl", "120s"}, "8784h0m0s", "2m0s"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"TOPOSHIFT_DATA_DIR": t.TempDir()}
			for k, v := range tt.env {
				env[k] = v
			}
			s := startServe(t, testEnv(t, env), append([]string{"--retain", "off"}, tt.args...)...)
			waitIngestOn(t, s.base)
			got := getMetrics(t, s.base).Metrics
			if got.Mode != "on" || got.DependencyExpiry != tt.wantExpiry || got.InstanceTTL != tt.wantInstance {
				t.Errorf("metrics status %+v, want on with %s and %s", got, tt.wantExpiry, tt.wantInstance)
			}
			stopOK(t, s)
		})
	}

	for _, tt := range []struct {
		name    string
		env     map[string]string
		args    []string
		wantErr string
	}{
		{"expiry under two minutes", nil, []string{"--dependency-expiry", "90s"}, "--dependency-expiry 1m30s is outside 2m0s to 8784h0m0s"},
		{"expiry over 366 days", nil, []string{"--dependency-expiry", "9000h"}, "--dependency-expiry 9000h0m0s is outside 2m0s to 8784h0m0s"},
		{"expiry with a part of a second", nil, []string{"--dependency-expiry", "1.5s"}, "--dependency-expiry 1.5s is not a whole number of seconds"},
		{"expiry of zero", nil, []string{"--dependency-expiry", "0s"}, "--dependency-expiry"},
		{"expiry in the variable out of bounds", map[string]string{"TOPOSHIFT_DEPENDENCY_EXPIRY": "10s"}, nil, "--dependency-expiry"},
		{"expiry in the variable that is not a duration", map[string]string{"TOPOSHIFT_DEPENDENCY_EXPIRY": "soon"}, nil, `TOPOSHIFT_DEPENDENCY_EXPIRY "soon"`},
		{"ttl under two minutes", nil, []string{"--instance-ttl", "90s"}, "--instance-ttl 1m30s is outside 2m0s to 8784h0m0s"},
		{"ttl over 366 days", nil, []string{"--instance-ttl", "9000h"}, "--instance-ttl"},
		{"ttl with a part of a second", nil, []string{"--instance-ttl", "1.5s"}, "whole number of seconds"},
		{"ttl in the variable that is not a duration", map[string]string{"TOPOSHIFT_INSTANCE_TTL": "soon"}, nil, `TOPOSHIFT_INSTANCE_TTL "soon"`},
		{"ttl in the variable out of bounds", map[string]string{"TOPOSHIFT_INSTANCE_TTL": "9000h"}, nil, "--instance-ttl"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			// A serve that wrongly starts runs until the deadline, and then stops.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			code := run(ctx, append([]string{"serve", "--listen", "127.0.0.1:0"}, tt.args...), testEnv(t, tt.env), io.Discard, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), tt.wantErr) || strings.Contains(stderr.String(), "listening") {
				t.Errorf("exit %d, stderr %q, want exit 2 and %q", code, &stderr, tt.wantErr)
			}
		})
	}
}

// holdSink holds its first call until released, for logs and for metrics alike;
// a later call does not wait.
type holdSink struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newHoldSink() *holdSink {
	return &holdSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *holdSink) hold() {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-time.After(30 * time.Second):
		}
	}
}

func (s *holdSink) Accept(context.Context, capture.LogsData) error {
	s.hold()
	return nil
}

func (s *holdSink) AcceptMetrics(context.Context, otlpmetrics.Data) (otlphttp.Partial, error) {
	s.hold()
	return otlphttp.Partial{}, nil
}

// The two routes count their requests against one budget: a logs request
// held in the sink makes the metrics route answer 503 with Retry-After: 1 at
// once, and the reverse. Nothing listens: the routes are served in process.
func TestReceiverRoutesShareOneBudget(t *testing.T) {
	const n = 64
	// An empty request in n bytes: an unknown field, which both messages drop.
	body := protowire.AppendBytes(protowire.AppendTag(nil, 15, protowire.BytesType), make([]byte, n-2))
	mux := func(sink *switchSink, budget *otlphttp.Budget) *http.ServeMux {
		m := http.NewServeMux()
		for _, r := range receiverRoutes(sink, budget) {
			m.Handle(r.Pattern, r.Handler)
		}
		return m
	}
	post := func(h http.Handler, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	for _, c := range []struct{ held, other string }{{"/v1/logs", "/v1/metrics"}, {"/v1/metrics", "/v1/logs"}} {
		t.Run("held "+c.held, func(t *testing.T) {
			hold := newHoldSink()
			sink := &switchSink{}
			sink.set(hold)
			h := mux(sink, otlphttp.NewBudget(n))
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- post(h, c.held) }()
			select {
			case <-hold.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the request was not held")
			}
			rec := post(h, c.other)
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Errorf("%s while %s is held: status %d Retry-After %q, want 503 and 1", c.other, c.held, rec.Code, rec.Header().Get("Retry-After"))
			}
			close(hold.release)
			select {
			case rec := <-done:
				if rec.Code != http.StatusOK {
					t.Errorf("the held request: status %d", rec.Code)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the held request was not answered")
			}
			if rec := post(h, c.other); rec.Code != http.StatusOK {
				t.Errorf("%s after the release: status %d", c.other, rec.Code)
			}
		})
	}

	t.Run("the routes", func(t *testing.T) {
		var patterns []string
		for _, r := range receiverRoutes(&switchSink{}, nil) {
			patterns = append(patterns, r.Pattern)
		}
		if strings.Join(patterns, ",") != "POST /v1/logs,POST /v1/metrics" {
			t.Errorf("routes %v", patterns)
		}
	})
}

// serve -h documents the metrics receiver, the two flags and the status.
func TestServeHelpDocumentsServiceGraph(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{
		"-dependency-expiry", "TOPOSHIFT_DEPENDENCY_EXPIRY", "-instance-ttl", "TOPOSHIFT_INSTANCE_TTL",
		"POST /v1/metrics", "application/x-protobuf", "liveness_expiry", "svcgraph/", "spancount/",
		"Services and their calls", `"metrics"`, "tens of MB per cluster name, not measured", "no authentication",
		"JSON is answered 415", "L3 (services and their calls) is retained with L2",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("serve -h does not mention %q", want)
		}
	}
}

// serve with ingest on takes a protobuf metrics request and counts its point
// in GET /status; with no data directory, the metrics route is off in the way the
// logs route is, and JSON metrics are refused first.
func TestServeTakesMetrics(t *testing.T) {
	t.Run("ingest on", func(t *testing.T) {
		s := startServe(t, dirEnv(t, t.TempDir()), "--retain", "off")
		waitIngestOn(t, s.base)
		if got := getMetrics(t, s.base).Metrics; got.Mode != "on" || len(got.Counters) != 0 {
			t.Fatalf("before any request: %+v", got)
		}
		now := time.Now().Truncate(time.Second)
		if status, _, text := postMetrics(t, s.base, "application/x-protobuf", serviceGraphRequest(t, now, 1)); status != http.StatusOK || text != "" {
			t.Fatalf("POST /v1/metrics = %d %q, want 200 and an empty body", status, text)
		}
		got := getMetrics(t, s.base).Metrics
		if got.Counters["svcgraph.points"] != 1 || got.Counters["metrics.points"] != 1 || got.Counters["svcgraph.skipped_warm_up"] != 1 {
			t.Errorf("counters %v, want the one point, set aside by the warm-up", got.Counters)
		}
		c, ok := got.Clusters["default"]
		if !ok || c.LastServiceGraphPoint.Before(now) || c.Silent || c.Series != 1 || !c.LastSpanCountPoint.IsZero() {
			t.Errorf("clusters %+v, want the default cluster with a point since %s, one series, not silent", got.Clusters, now)
		}
		if status, _, text := postMetrics(t, s.base, "application/json", []byte(`{"resourceMetrics":[]}`)); status != http.StatusUnsupportedMediaType ||
			!strings.Contains(text, "application/x-protobuf only") {
			t.Errorf("JSON metrics = %d %q, want 415", status, text)
		}
		stopOK(t, s)
	})

	t.Run("no data directory", func(t *testing.T) {
		s := startServe(t, testEnv(t, nil))
		status, retryAfter, text := postMetrics(t, s.base, "application/x-protobuf", serviceGraphRequest(t, time.Now(), 1))
		if status != http.StatusServiceUnavailable || retryAfter != "30" || !strings.Contains(text, datadir.NoDirectoryMessage) ||
			!strings.Contains(text, "--data-dir") || !strings.Contains(text, "TOPOSHIFT_DATA_DIR") {
			t.Errorf("POST /v1/metrics = %d Retry-After %q %q, want 503, 30 and the sentence", status, retryAfter, text)
		}
		if status, _, text := postMetrics(t, s.base, "application/json", []byte(`{"resourceMetrics":[]}`)); status != http.StatusUnsupportedMediaType {
			t.Errorf("JSON metrics = %d %q, want 415 before the 503", status, text)
		}
		if got := getMetrics(t, s.base).Metrics; got.Mode != "off" {
			t.Errorf("metrics status %+v, want off", got)
		}
		stopOK(t, s)
	})
}
