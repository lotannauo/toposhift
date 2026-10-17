package otlphttp_test

import (
	"net/http"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// Whatever the body, the metrics handler answers 200 or 400 and does not
// panic. Under go test the seeds run; the fuzzer is run by hand.
func FuzzMetricsHandler(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not protobuf"))
	f.Add([]byte{0x0a, 0x00})                         // one empty ResourceMetrics: no resource, no scope
	f.Add([]byte{0x0a, 0x02, 0x1a, 0x00})             // an empty ScopeMetrics list entry without a resource
	f.Add([]byte{0x0a, 0x04, 0x12, 0x02, 0x1a, 0x00}) // a scope with an empty metric
	f.Add(mustBytes(sampleMetricsProto()))
	// Every kind of data that holds no message.
	for _, field := range []protowire.Number{5, 7, 9, 10, 11} {
		metric := protowire.AppendTag(nil, field, protowire.BytesType)
		metric = protowire.AppendBytes(metric, nil)
		scope := protowire.AppendTag(nil, 2, protowire.BytesType)
		scope = protowire.AppendBytes(scope, metric)
		rm := protowire.AppendTag(nil, 2, protowire.BytesType)
		rm = protowire.AppendBytes(rm, scope)
		body := protowire.AppendTag(nil, 1, protowire.BytesType)
		f.Add(protowire.AppendBytes(body, rm))
	}
	// A key index, a value of an unknown kind, and a deep value.
	m := &metricspb.MetricsData{}
	if err := proto.Unmarshal(mustBytes(sampleMetricsProto()), m); err == nil {
		m.ResourceMetrics[0].Resource.Attributes[0].KeyStrindex = 3
		f.Add(mustBytes(m))
	}
	deep := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
		resourceOf(nil, scopeOf("s", sumMetric("m", cumulative, true, intPoint(0, 1, 1, skv("k", "v"))))),
	}}
	deep.ResourceMetrics[0].Resource.Attributes = append(deep.ResourceMetrics[0].Resource.Attributes, &commonpb.KeyValue{Key: "k", Value: deepValue(40)})
	f.Add(mustBytes(deep))

	f.Fuzz(func(t *testing.T, b []byte) {
		h, _ := newMetricsHandler(otlphttp.Options{})
		rec := doMetrics(t, h, request{ctype: protoType, body: b})
		if rec.Code != http.StatusOK && rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d for a body of %d bytes", rec.Code, len(b))
		}
	})
}

func mustBytes(m proto.Message) []byte {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}
