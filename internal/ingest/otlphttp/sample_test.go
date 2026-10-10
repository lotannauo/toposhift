package otlphttp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

const (
	protoType = "application/x-protobuf"
	jsonType  = "application/json"
)

// recorder is a sink that keeps what it was given and returns a set error.
type recorder struct {
	mu  sync.Mutex
	got []capture.LogsData
	err error
}

func (r *recorder) Accept(_ context.Context, d capture.LogsData) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, d)
	return r.err
}

func (r *recorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// request is one POST to the handler.
type request struct {
	method   string
	path     string
	ctype    string
	encoding string
	chunked  bool // the length of the body is not declared
	body     []byte
}

// do serves a request with the handler and returns the recorded response.
func do(t testing.TB, h http.Handler, q request) *httptest.ResponseRecorder {
	t.Helper()
	if q.method == "" {
		q.method = http.MethodPost
	}
	if q.path == "" {
		q.path = otlphttp.Path
	}
	req := httptest.NewRequestWithContext(t.Context(), q.method, q.path, bytes.NewReader(q.body))
	if q.ctype != "" {
		req.Header.Set("Content-Type", q.ctype)
	}
	if q.chunked {
		req.ContentLength = -1
	}
	if q.encoding != "" {
		req.Header.Set("Content-Encoding", q.encoding)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func gz(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// gzFast is gz at the fastest level, for bodies of tens of MiB of zeros, which
// the default level takes long to compress under the race detector.
func gzFast(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func bodyOf(t testing.TB, rec *httptest.ResponseRecorder) []byte {
	t.Helper()
	b, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The sample is one batch in both models, written out by hand so that the
// conversion is checked field by field and not against itself.

const sampleEntityRefs = `[{"schemaUrl":"https://example.test/entity","type":"k8s.pod","idKeys":["k8s.pod.uid"],"descriptionKeys":["k8s.pod.name"]}]`

var (
	sampleTraceID = []byte{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c}
	sampleSpanID  = []byte{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74}
)

func ptr[T any](v T) *T { return &v }

func cstr(v string) capture.AnyValue { return capture.AnyValue{StringValue: &v} }
func cint(v int64) capture.AnyValue  { i := capture.Int64(v); return capture.AnyValue{IntValue: &i} }

func pstr(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

func pint(v int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
}

// sampleModel is the sample as the sink must receive it.
func sampleModel() capture.LogsData {
	bytesValue := []byte{1, 2, 3}
	emptyBytes := []byte{}
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{
		{
			Resource: capture.Resource{
				Attributes: []capture.KeyValue{
					{Key: "service.name", Value: cstr("checkout")},
					{Key: "ok", Value: capture.AnyValue{BoolValue: ptr(true)}},
					{Key: "n", Value: cint(-42)},
					{Key: "pi", Value: capture.AnyValue{DoubleValue: ptr(3.5)}},
					{Key: "raw", Value: capture.AnyValue{BytesValue: &bytesValue}},
					{Key: "list", Value: capture.AnyValue{ArrayValue: &capture.ArrayValue{
						Values: []capture.AnyValue{cstr("a"), cint(7), {}},
					}}},
					{Key: "map", Value: capture.AnyValue{KVListValue: &capture.KeyValueList{
						Values: []capture.KeyValue{{Key: "k", Value: cstr("v")}, {Key: "k", Value: cstr("again")}},
					}}},
					{Key: "empty", Value: capture.AnyValue{}},
					{Key: "empty string", Value: cstr("")},
					{Key: "empty bytes", Value: capture.AnyValue{BytesValue: &emptyBytes}},
				},
				DroppedAttributesCount: 3,
				EntityRefs:             []byte(sampleEntityRefs),
			},
			ScopeLogs: []capture.ScopeLogs{
				{
					Scope: capture.InstrumentationScope{
						Name:                   "my.library",
						Version:                "1.0.0",
						Attributes:             []capture.KeyValue{{Key: "scope.attr", Value: cstr("s")}},
						DroppedAttributesCount: 1,
					},
					LogRecords: []capture.LogRecord{
						{
							TimeUnixNano:           capture.Uint64(1<<63 + 1),
							ObservedTimeUnixNano:   1544712660300000000,
							SeverityNumber:         9,
							SeverityText:           "INFO",
							Body:                   &capture.AnyValue{StringValue: ptr("Example log record")},
							Attributes:             []capture.KeyValue{{Key: "http.status", Value: cint(200)}},
							DroppedAttributesCount: 2,
							Flags:                  1,
							TraceID:                sampleTraceID,
							SpanID:                 sampleSpanID,
							EventName:              "example.event",
						},
						{
							ObservedTimeUnixNano: 1544712661000000500,
							Body: &capture.AnyValue{KVListValue: &capture.KeyValueList{Values: []capture.KeyValue{
								{Key: "type", Value: cstr("ADDED")},
							}}},
						},
						{ObservedTimeUnixNano: 5},
					},
					SchemaURL: "https://opentelemetry.io/schemas/1.27.0",
				},
				{},
			},
			SchemaURL: "https://opentelemetry.io/schemas/1.26.0",
		},
		{},
	}}
}

// sampleProto is the same batch as protobuf.
func sampleProto() *logspb.LogsData {
	return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: pstr("checkout")},
					{Key: "ok", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
					{Key: "n", Value: pint(-42)},
					{Key: "pi", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.5}}},
					{Key: "raw", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3}}}},
					{Key: "list", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
						Values: []*commonpb.AnyValue{pstr("a"), pint(7), {}},
					}}}},
					{Key: "map", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
						Values: []*commonpb.KeyValue{{Key: "k", Value: pstr("v")}, {Key: "k", Value: pstr("again")}},
					}}}},
					{Key: "empty", Value: &commonpb.AnyValue{}},
					{Key: "empty string", Value: pstr("")},
					{Key: "empty bytes", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{}}}},
				},
				DroppedAttributesCount: 3,
				EntityRefs: []*commonpb.EntityRef{{
					SchemaUrl:       "https://example.test/entity",
					Type:            "k8s.pod",
					IdKeys:          []string{"k8s.pod.uid"},
					DescriptionKeys: []string{"k8s.pod.name"},
				}},
			},
			ScopeLogs: []*logspb.ScopeLogs{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:                   "my.library",
						Version:                "1.0.0",
						Attributes:             []*commonpb.KeyValue{{Key: "scope.attr", Value: pstr("s")}},
						DroppedAttributesCount: 1,
					},
					LogRecords: []*logspb.LogRecord{
						{
							TimeUnixNano:           1<<63 + 1,
							ObservedTimeUnixNano:   1544712660300000000,
							SeverityNumber:         logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
							SeverityText:           "INFO",
							Body:                   pstr("Example log record"),
							Attributes:             []*commonpb.KeyValue{{Key: "http.status", Value: pint(200)}},
							DroppedAttributesCount: 2,
							Flags:                  1,
							TraceId:                sampleTraceID,
							SpanId:                 sampleSpanID,
							EventName:              "example.event",
						},
						{
							ObservedTimeUnixNano: 1544712661000000500,
							Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
								Values: []*commonpb.KeyValue{{Key: "type", Value: pstr("ADDED")}},
							}}},
						},
						{ObservedTimeUnixNano: 5},
					},
					SchemaUrl: "https://opentelemetry.io/schemas/1.27.0",
				},
				{},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.26.0",
		},
		{},
	}}
}

func marshalProto(t testing.TB, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func encodeJSON(t testing.TB, d capture.LogsData) []byte {
	t.Helper()
	b, err := capture.EncodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// newHandler returns a handler over a fresh recorder.
func newHandler(o otlphttp.Options) (http.Handler, *recorder) {
	r := &recorder{}
	return otlphttp.Handler(r, o), r
}

// gzWithExtra compresses b into a gzip stream whose header has an extra field of n
// bytes, so the stream is longer than b whatever b is.
func gzWithExtra(t testing.TB, b []byte, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Extra = make([]byte, n)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
