package otlphttp_test

import (
	"errors"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// exportMetricsRequestDescriptor describes ExportMetricsServiceRequest as the
// OTLP collector's metrics service defines it: one repeated field 1,
// resource_metrics. It is written out because the package that defines it
// carries gRPC.
func exportMetricsRequestDescriptor(t testing.TB) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("otlphttp_export_metrics_test.proto"),
		Package:    proto.String("otlphttp.test"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto.Path()},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("ExportMetricsServiceRequest"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("resource_metrics"),
				JsonName: proto.String("resourceMetrics"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".opentelemetry.proto.metrics.v1.ResourceMetrics"),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Messages().ByName("ExportMetricsServiceRequest")
}

// exportMetricsResponseDescriptor describes ExportMetricsServiceResponse as the
// collector's metrics service defines it: partial_success at field 1, an
// ExportMetricsPartialSuccess whose field 1 is rejected_data_points and field 2
// error_message.
func exportMetricsResponseDescriptor(t testing.TB) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("otlphttp_export_metrics_response_test.proto"),
		Package: proto.String("otlphttp.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("ExportMetricsPartialSuccess"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("rejected_data_points"), JsonName: proto.String("rejectedDataPoints"), Number: proto.Int32(1),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				},
				{
					Name: proto.String("error_message"), JsonName: proto.String("errorMessage"), Number: proto.Int32(2),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				},
			},
		}, {
			Name: proto.String("ExportMetricsServiceResponse"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("partial_success"), JsonName: proto.String("partialSuccess"), Number: proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".otlphttp.test.ExportMetricsPartialSuccess"),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Messages().ByName("ExportMetricsServiceResponse")
}

// MetricsData and ExportMetricsServiceRequest are the same message on the
// wire: a single repeated field 1 of ResourceMetrics. The handler decodes the
// first where the protocol sends the second.
func TestMetricsDataIsWireCompatibleWithExportMetricsServiceRequest(t *testing.T) {
	sample := sampleMetricsProto()

	f := (&metricspb.MetricsData{}).ProtoReflect().Descriptor().Fields().ByName("resource_metrics")
	if f == nil || f.Number() != 1 || !f.IsList() || f.Message().FullName() != "opentelemetry.proto.metrics.v1.ResourceMetrics" {
		t.Fatalf("MetricsData.resource_metrics is not repeated ResourceMetrics at field 1: %v", f)
	}
	asData := marshalProto(t, sample)

	var byHand []byte
	for _, rm := range sample.ResourceMetrics {
		byHand = protowire.AppendTag(byHand, 1, protowire.BytesType)
		byHand = protowire.AppendBytes(byHand, marshalProto(t, rm))
	}

	desc := exportMetricsRequestDescriptor(t)
	req := dynamicpb.NewMessage(desc)
	list := req.Mutable(desc.Fields().ByName("resource_metrics")).List()
	for _, rm := range sample.ResourceMetrics {
		list.Append(protoreflect.ValueOfMessage(rm.ProtoReflect()))
	}
	asRequest := marshalProto(t, req)

	for name, b := range map[string][]byte{"by hand": byHand, "from a descriptor": asRequest} {
		if !reflect.DeepEqual(b, asData) {
			t.Errorf("%s: the request bytes differ from the MetricsData bytes", name)
		}
		var got metricspb.MetricsData
		if err := proto.Unmarshal(b, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !proto.Equal(&got, sample) {
			t.Errorf("%s: decoded as MetricsData, the request is a different value", name)
		}
		h, sink := newMetricsHandler(otlphttp.Options{})
		if rec := doMetrics(t, h, request{ctype: protoType, body: b}); rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", name, rec.Code)
		}
		if sink.calls() != 1 || !reflect.DeepEqual(sink.got[0], sampleMetricsData()) {
			t.Errorf("%s: the sink did not receive the sample", name)
		}
	}

	back := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(asData, back); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, req) {
		t.Error("MetricsData bytes decode to a different request")
	}
}

// deepValue is a value nested n levels deep (a scalar is 1, each array adds 1).
func deepValue(n int) *commonpb.AnyValue {
	v := pstr("x")
	for i := 1; i < n; i++ {
		v = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{v}}}}
	}
	return v
}

// metricsOf serves a message to a fresh handler and returns the response and
// the sink.
func metricsOf(t testing.TB, m *metricspb.MetricsData) (int, *metricsRecorder) {
	t.Helper()
	h, sink := newMetricsHandler(otlphttp.Options{})
	rec := doMetrics(t, h, request{ctype: protoType, body: marshalProto(t, m)})
	return rec.Code, sink
}

// The conversion, field by field.
func TestConvertMetrics(t *testing.T) {
	t.Run("the sample, with the scope kept and the dropped points counted", func(t *testing.T) {
		code, sink := metricsOf(t, sampleMetricsProto())
		if code != http.StatusOK || sink.calls() != 1 {
			t.Fatalf("status %d, %d calls", code, sink.calls())
		}
		got, want := sink.got[0], sampleMetricsData()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("the newest time covers the dropped points of each kind", func(t *testing.T) {
		kinds := map[string]*metricspb.Metric{
			"non-monotonic sum": sumMetric("m", cumulative, false, intPoint(0, 99, 1)),
			"gauge": {Name: "m", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{intPoint(0, 99, 1)},
			}}},
			"histogram": {Name: "m", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints: []*metricspb.HistogramDataPoint{{TimeUnixNano: 99}},
			}}},
			"exponential histogram": {Name: "m", Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints: []*metricspb.ExponentialHistogramDataPoint{{TimeUnixNano: 99}},
			}}},
			"summary": {Name: "m", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
				DataPoints: []*metricspb.SummaryDataPoint{{TimeUnixNano: 99}},
			}}},
		}
		for name, m := range kinds {
			_, sink := metricsOf(t, &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				resourceOf(nil, scopeOf("s", sumMetric("kept", cumulative, true, intPoint(0, 5, 1)), m)),
			}})
			r := sink.got[0].Resources[0]
			if r.NewestTimeUnixNano != 99 || len(r.Points) != 1 || r.DroppedNotSum+r.DroppedNonMonotonic != 1 {
				t.Errorf("%s: %+v", name, r)
			}
		}
	})

	t.Run("an integer and a double", func(t *testing.T) {
		_, sink := metricsOf(t, &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true,
				intPoint(0, 1, 1<<40), intPoint(0, 2, -3), doublePoint(0, 3, 0.25)))),
		}})
		pts := sink.got[0].Resources[0].Points
		if len(pts) != 3 || pts[0].Value != float64(1<<40) || pts[1].Value != -3 || pts[2].Value != 0.25 {
			t.Errorf("points %+v", pts)
		}
	})

	t.Run("delta, cumulative and unspecified", func(t *testing.T) {
		_, sink := metricsOf(t, &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf(nil, scopeOf("s",
				sumMetric("d", delta, true, intPoint(0, 1, 1)),
				sumMetric("c", cumulative, true, intPoint(0, 1, 1)),
				sumMetric("u", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED, true, intPoint(0, 1, 1)),
			)),
		}})
		pts := sink.got[0].Resources[0].Points
		if len(pts) != 3 || pts[0].Temporality.String() != "delta" || pts[1].Temporality.String() != "cumulative" || pts[2].Temporality.String() != "unspecified" {
			t.Errorf("points %+v", pts)
		}
	})

	t.Run("no value and no recorded value are not a number", func(t *testing.T) {
		noValue := &metricspb.NumberDataPoint{TimeUnixNano: 1}
		flagged := intPoint(0, 2, 7)
		flagged.Flags = uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK)
		flaggedDouble := doublePoint(0, 3, 7)
		flaggedDouble.Flags = 1
		_, sink := metricsOf(t, &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true, noValue, flagged, flaggedDouble, intPoint(0, 4, 0)))),
		}})
		pts := sink.got[0].Resources[0].Points
		if len(pts) != 4 || !math.IsNaN(pts[0].Value) || !math.IsNaN(pts[1].Value) || !math.IsNaN(pts[2].Value) || pts[3].Value != 0 {
			t.Errorf("points %+v", pts)
		}
	})

	t.Run("a resource with no points, and a metric with no data", func(t *testing.T) {
		_, sink := metricsOf(t, &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf([]*commonpb.KeyValue{skv("a", "b")}, scopeOf("s", &metricspb.Metric{Name: "m"})),
			{},
		}})
		rs := sink.got[0].Resources
		if len(rs) != 2 || len(rs[0].Points) != 0 || rs[0].NewestTimeUnixNano != 0 || rs[0].DroppedNotSum != 0 || len(rs[1].Attributes) != 0 {
			t.Errorf("resources %+v", rs)
		}
	})

	t.Run("an empty body is an empty request", func(t *testing.T) {
		h, sink := newMetricsHandler(otlphttp.Options{})
		if rec := doMetrics(t, h, request{ctype: protoType}); rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if sink.calls() != 1 || len(sink.got[0].Resources) != 0 {
			t.Errorf("got %+v", sink.got)
		}
	})

	t.Run("a string table index is refused", func(t *testing.T) {
		m := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf([]*commonpb.KeyValue{{Key: "k", KeyStrindex: 1, Value: pstr("v")}}),
		}}
		if code, sink := metricsOf(t, m); code != http.StatusBadRequest || sink.calls() != 0 {
			t.Errorf("status %d, %d calls", code, sink.calls())
		}
		m = &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
			resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true, intPoint(0, 1, 1, &commonpb.KeyValue{Key: "k", KeyStrindex: 2})))),
		}}
		if code, sink := metricsOf(t, m); code != http.StatusBadRequest || sink.calls() != 0 {
			t.Errorf("point: status %d, %d calls", code, sink.calls())
		}
	})

	t.Run("attribute nesting is bounded in the resource and the points", func(t *testing.T) {
		at := func(n int) []*commonpb.KeyValue { return []*commonpb.KeyValue{{Key: "k", Value: deepValue(n)}} }
		for _, c := range []struct {
			name string
			m    *metricspb.MetricsData
			ok   bool
		}{
			{"resource 32", &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{resourceOf(at(32))}}, true},
			{"resource 33", &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{resourceOf(at(33))}}, false},
			{"point 32", &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true, intPoint(0, 1, 1, at(32)...)))),
			}}, true},
			{"point 33", &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true, intPoint(0, 1, 1, at(33)...)))),
			}}, false},
		} {
			code, _ := metricsOf(t, c.m)
			if want := map[bool]int{true: http.StatusOK, false: http.StatusBadRequest}[c.ok]; code != want {
				t.Errorf("%s: status %d, want %d", c.name, code, want)
			}
		}
	})
}

// partialOf decodes the response of a metrics request with the hand-written
// descriptor of ExportMetricsServiceResponse.
func partialOf(t testing.TB, body []byte) (present bool, rejected int64, msg string) {
	t.Helper()
	desc := exportMetricsResponseDescriptor(t)
	resp := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(body, resp); err != nil {
		t.Fatalf("the response is not an ExportMetricsServiceResponse: %v", err)
	}
	f := desc.Fields().ByName("partial_success")
	if !resp.Has(f) {
		return false, 0, ""
	}
	ps := resp.Get(f).Message()
	return true, ps.Get(ps.Descriptor().Fields().ByName("rejected_data_points")).Int(),
		ps.Get(ps.Descriptor().Fields().ByName("error_message")).String()
}

// The statuses of /v1/metrics.
func TestMetricsStatuses(t *testing.T) {
	good := marshalProto(t, sampleMetricsProto())
	gzGood := gz(t, good)

	t.Run("JSON is 415 before the admission and before the body is read", func(t *testing.T) {
		sink := &metricsRecorder{}
		budget := otlphttp.NewBudget(10)
		h := otlphttp.MetricsHandler(sink, otlphttp.Options{Budget: budget})
		// Fill the budget with a request that waits: a JSON request is not
		// refused as busy, so it was not counted against the budget.
		hold := newHoldMetricsSink()
		h2 := otlphttp.MetricsHandler(hold, otlphttp.Options{Budget: budget})
		release := make(chan struct{})
		go func() {
			defer close(release)
			postPath(t, h2, otlphttp.MetricsPath, 10, good)
		}()
		<-hold.entered
		defer func() {
			close(hold.release)
			<-release
		}()
		jsonBody := []byte(`{"resourceMetrics":[]}`)
		rec, read := postTyped(t, h, otlphttp.MetricsPath, jsonType, int64(len(jsonBody)), jsonBody)
		checkStatus(t, rec, jsonType, http.StatusUnsupportedMediaType, 3,
			"unsupported Content-Type: metrics are received as application/x-protobuf only")
		if sink.calls() != 0 {
			t.Error("the sink was called")
		}
		if read != 0 {
			t.Errorf("%d bytes of the body were read", read)
		}
	})

	cases := []struct {
		name   string
		q      request
		status int
		code   int32
	}{
		{"another content type", request{ctype: "text/plain", body: good}, http.StatusUnsupportedMediaType, 3},
		{"no content type", request{body: good}, http.StatusUnsupportedMediaType, 3},
		{"bad encoding", request{ctype: protoType, encoding: "br", body: good}, http.StatusUnsupportedMediaType, 3},
		{"the logs path", request{path: otlphttp.Path, ctype: protoType, body: good}, http.StatusNotFound, 0},
		{"a longer path", request{path: "/v1/metrics/", ctype: protoType, body: good}, http.StatusNotFound, 0},
		{"malformed", request{ctype: protoType, body: []byte("not protobuf at all")}, http.StatusBadRequest, 3},
		{"cut short", request{ctype: protoType, body: good[:len(good)-1]}, http.StatusBadRequest, 3},
		{"bad gzip", request{ctype: protoType, encoding: "gzip", body: good}, http.StatusBadRequest, 3},
		{"gzip", request{ctype: protoType, encoding: "gzip", body: gzGood}, http.StatusOK, 0},
		{"plain", request{ctype: protoType, body: good}, http.StatusOK, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, sink := newMetricsHandler(otlphttp.Options{})
			rec := doMetrics(t, h, c.q)
			if rec.Code != c.status {
				t.Fatalf("status %d, want %d, body %q", rec.Code, c.status, rec.Body.String())
			}
			if c.status >= 400 && c.status != 404 {
				if st := statusOf(t, rec); st.Code != c.code {
					t.Errorf("code %d, want %d", st.Code, c.code)
				}
			}
			if (c.status == http.StatusOK) != (sink.calls() == 1) {
				t.Errorf("sink called %d times", sink.calls())
			}
		})
	}

	t.Run("the logs path refuses the metrics path", func(t *testing.T) {
		h, _ := newHandler(otlphttp.Options{})
		if rec := do(t, h, request{path: otlphttp.MetricsPath, ctype: protoType, body: good}); rec.Code != http.StatusNotFound {
			t.Errorf("status %d, want 404", rec.Code)
		}
	})

	t.Run("GET is 405 with Allow", func(t *testing.T) {
		h, _ := newMetricsHandler(otlphttp.Options{})
		rec := doMetrics(t, h, request{method: http.MethodGet, ctype: protoType})
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
			t.Errorf("status %d, Allow %q", rec.Code, rec.Header().Get("Allow"))
		}
	})

	t.Run("413 on the wire and after gzip", func(t *testing.T) {
		h, sink := newMetricsHandler(otlphttp.Options{MaxBodyBytes: 1 << 12})
		if rec := doMetrics(t, h, request{ctype: protoType, body: make([]byte, 2<<12)}); rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("wire: status %d", rec.Code)
		}
		if rec := doMetrics(t, h, request{ctype: protoType, chunked: true, body: make([]byte, 2<<12)}); rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("chunked: status %d", rec.Code)
		}
		if rec := doMetrics(t, h, request{ctype: protoType, encoding: "gzip", body: gzFast(t, make([]byte, 8<<12))}); rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("gzip: status %d", rec.Code)
		}
		if sink.calls() != 0 {
			t.Error("the sink was called")
		}
	})

	t.Run("sink errors", func(t *testing.T) {
		for _, c := range []struct {
			name       string
			err        error
			status     int
			retryAfter string
		}{
			{"unavailable", &otlphttp.UnavailableError{Message: "ingest is off"}, http.StatusServiceUnavailable, "30"},
			{"busy", otlphttp.ErrBusy, http.StatusServiceUnavailable, "1"},
			{"wrapped busy", errors.Join(errors.New("x"), otlphttp.ErrBusy), http.StatusServiceUnavailable, "1"},
			{"refused", &otlphttp.RefusedError{Message: "no"}, http.StatusBadRequest, ""},
			{"other", errors.New("disk"), http.StatusInternalServerError, ""},
		} {
			t.Run(c.name, func(t *testing.T) {
				h, sink := newMetricsHandler(otlphttp.Options{})
				sink.err = c.err
				rec := doMetrics(t, h, request{ctype: protoType, body: good})
				if rec.Code != c.status || rec.Header().Get("Retry-After") != c.retryAfter {
					t.Errorf("status %d Retry-After %q, want %d %q", rec.Code, rec.Header().Get("Retry-After"), c.status, c.retryAfter)
				}
			})
		}
	})

	t.Run("a partial success is the same bytes as for logs", func(t *testing.T) {
		h, sink := newMetricsHandler(otlphttp.Options{})
		sink.p = otlphttp.Partial{Rejected: 3, Message: "3 data points rejected"}
		rec := doMetrics(t, h, request{ctype: protoType, body: good})
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != protoType {
			t.Fatalf("status %d, Content-Type %q", rec.Code, rec.Header().Get("Content-Type"))
		}
		present, rejected, msg := partialOf(t, rec.Body.Bytes())
		if !present || rejected != 3 || msg != "3 data points rejected" {
			t.Errorf("partial success %v %d %q", present, rejected, msg)
		}
		// Nothing rejected: the empty response.
		sink.p = otlphttp.Partial{}
		rec = doMetrics(t, h, request{ctype: protoType, body: good})
		if rec.Body.Len() != 0 {
			t.Errorf("body %q, want empty", rec.Body.String())
		}
		// A message cut like a status message.
		sink.p = otlphttp.Partial{Rejected: 1, Message: strings.Repeat("é", 2000)}
		rec = doMetrics(t, h, request{ctype: protoType, body: good})
		if _, _, msg := partialOf(t, rec.Body.Bytes()); len(msg) > otlphttp.MaxStatusMessageBytes {
			t.Errorf("message of %d bytes", len(msg))
		}
	})
}
