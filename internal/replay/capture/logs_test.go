package capture_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

func readGolden(t *testing.T) []capture.Line {
	t.Helper()
	data, err := os.ReadFile("testdata/logs_example.jsonl")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines, err := readAll(capture.NewReader(bytes.NewReader(data), capture.ReaderOptions{}))
	if err != nil || len(lines) != 2 {
		t.Fatalf("read %d lines, error %v", len(lines), err)
	}
	return lines
}

func str(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func TestDecodeLogsExample(t *testing.T) {
	t.Parallel()
	got, err := capture.DecodeLogs(readGolden(t)[0].Raw)
	if err != nil {
		t.Fatalf("DecodeLogs: %v", err)
	}
	trace, _ := hex.DecodeString("5b8efff798038103d269b633813fc60c")
	span, _ := hex.DecodeString("eee19b7ec3c1b174")
	yes, ten, pi := true, capture.Int64(10), 637.704
	want := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource: capture.Resource{Attributes: []capture.KeyValue{{Key: "service.name", Value: str("my.service")}}},
		ScopeLogs: []capture.ScopeLogs{{
			Scope: capture.InstrumentationScope{
				Name: "my.library", Version: "1.0.0",
				Attributes: []capture.KeyValue{{Key: "my.scope.attribute", Value: str("some scope attribute")}},
			},
			LogRecords: []capture.LogRecord{{
				TimeUnixNano:         1544712660300000000,
				ObservedTimeUnixNano: 1544712660300000000,
				SeverityNumber:       10,
				SeverityText:         "Information",
				TraceID:              trace,
				SpanID:               span,
				Body:                 &capture.AnyValue{StringValue: new(string)},
				Attributes: []capture.KeyValue{
					{Key: "string.attribute", Value: str("some string")},
					{Key: "boolean.attribute", Value: capture.AnyValue{BoolValue: &yes}},
					{Key: "int.attribute", Value: capture.AnyValue{IntValue: &ten}},
					{Key: "double.attribute", Value: capture.AnyValue{DoubleValue: &pi}},
					{Key: "array.attribute", Value: capture.AnyValue{ArrayValue: &capture.ArrayValue{
						Values: []capture.AnyValue{str("many"), str("values")},
					}}},
					{Key: "map.attribute", Value: capture.AnyValue{KVListValue: &capture.KeyValueList{
						Values: []capture.KeyValue{{Key: "some.map.key", Value: str("some value")}},
					}}},
				},
			}},
		}},
	}}}
	*want.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.StringValue = "Example log record"
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DecodeLogs mismatch:\n got %+v\nwant %+v", got, want)
	}
	rec := got.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if rec.TimeUnixNano != 1544712660300000000 || rec.SeverityNumber != 10 {
		t.Errorf("time %d, severity %d", rec.TimeUnixNano, rec.SeverityNumber)
	}

	// Encoding writes the same message, with IDs in lowercase.
	enc, err := capture.EncodeLogs(got)
	if err != nil {
		t.Fatalf("EncodeLogs: %v", err)
	}
	for _, want := range []string{
		`"timeUnixNano":"1544712660300000000"`, `"severityNumber":10`,
		`"traceId":"5b8efff798038103d269b633813fc60c"`, `"spanId":"eee19b7ec3c1b174"`,
		`"intValue":"10"`, `"doubleValue":637.704`, `"boolValue":true`,
	} {
		if !strings.Contains(string(enc), want) {
			t.Errorf("encoding lacks %s: %s", want, enc)
		}
	}
	again, err := capture.DecodeLogs(enc)
	if err != nil || !reflect.DeepEqual(again, got) {
		t.Errorf("re-decoding the encoding: %v, equal %v", err, reflect.DeepEqual(again, got))
	}
}

func TestDecodeLogsKubernetesObject(t *testing.T) {
	t.Parallel()
	d, err := capture.DecodeLogs(readGolden(t)[1].Raw)
	if err != nil {
		t.Fatalf("DecodeLogs: %v", err)
	}
	rec := d.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if rec.Body == nil || rec.Body.KVListValue == nil {
		t.Fatalf("body = %+v, want a kvlist", rec.Body)
	}
	body := rec.Body.KVListValue.Values
	if len(body) != 2 || body[0].Key != "type" || body[1].Key != "object" {
		t.Fatalf("body keys = %+v, want type and object", body)
	}
	if s := body[0].Value.StringValue; s == nil || *s != "ADDED" {
		t.Errorf("type = %v, want ADDED", s)
	}
	object := body[1].Value.KVListValue
	if object == nil || object.Values[0].Key != "kind" || *object.Values[0].Value.StringValue != "Pod" {
		t.Errorf("object = %+v", object)
	}
	meta := object.Values[1].Value.KVListValue
	if meta == nil || len(meta.Values) != 3 || meta.Values[0].Key != "name" || *meta.Values[0].Value.StringValue != "web-0" {
		t.Errorf("metadata = %+v", meta)
	}
}

func TestDecodeLogs32Bit(t *testing.T) {
	t.Parallel()
	record := func(fields string) string {
		return `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{` + fields + `}]}]}]}`
	}
	tests := []struct {
		name    string
		in      string
		check   func(capture.LogsData) bool
		wantErr bool
	}{
		{"severity number", record(`"severityNumber":17`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == 17 }, false},
		{"severity as string", record(`"severityNumber":"17"`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == 17 }, false},
		{"severity min", record(`"severityNumber":-2147483648`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == math.MinInt32 }, false},
		{"severity name", record(`"severityNumber":"SEVERITY_NUMBER_INFO"`), nil, true},
		{"severity fraction", record(`"severityNumber":1.5`), nil, true},
		{"severity integral exponent", record(`"severityNumber":1e1`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == 10 }, false},
		{"severity integral fraction", record(`"severityNumber":"17.0"`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == 17 }, false},
		{"severity fractional exponent", record(`"severityNumber":1e-1`), nil, true},
		{"severity exponent too large", record(`"severityNumber":1e41`), nil, true},
		{"flags integral exponent", record(`"flags":"4e0"`), func(d capture.LogsData) bool { return rec(d).Flags == 4 }, false},
		{"flags too large by exponent", record(`"flags":43e8`), nil, true},
		{"severity too large", record(`"severityNumber":2147483648`), nil, true},
		{"severity plus", record(`"severityNumber":"+1"`), nil, true},
		{"severity null", record(`"severityNumber":null`), func(d capture.LogsData) bool { return rec(d).SeverityNumber == 0 }, false},
		{"flags", record(`"flags":1`), func(d capture.LogsData) bool { return rec(d).Flags == 1 }, false},
		{"flags as string max", record(`"flags":"4294967295"`), func(d capture.LogsData) bool { return rec(d).Flags == math.MaxUint32 }, false},
		{"flags negative", record(`"flags":-1`), nil, true},
		{"flags too large", record(`"flags":"4294967296"`), nil, true},
		{"flags bool", record(`"flags":true`), nil, true},
		{"dropped as string", record(`"droppedAttributesCount":"3"`), func(d capture.LogsData) bool { return rec(d).DroppedAttributesCount == 3 }, false},
		{"dropped fraction", record(`"droppedAttributesCount":3.5`), nil, true},
		{"time as number", record(`"timeUnixNano":5`), func(d capture.LogsData) bool { return rec(d).TimeUnixNano == 5 }, false},
		{"time negative", record(`"timeUnixNano":"-5"`), nil, true},
		{"unknown record field", record(`"futureField":{"a":1},"flags":2`), func(d capture.LogsData) bool { return rec(d).Flags == 2 }, false},
		{
			"resource dropped as string",
			`{"resourceLogs":[{"resource":{"droppedAttributesCount":"4"}}]}`,
			func(d capture.LogsData) bool { return d.ResourceLogs[0].Resource.DroppedAttributesCount == 4 }, false,
		},
		{"resource dropped bad", `{"resourceLogs":[{"resource":{"droppedAttributesCount":"x"}}]}`, nil, true},
		{
			"scope dropped as string",
			`{"resourceLogs":[{"scopeLogs":[{"scope":{"name":"n","droppedAttributesCount":"5"}}]}]}`,
			func(d capture.LogsData) bool {
				s := d.ResourceLogs[0].ScopeLogs[0].Scope
				return s.DroppedAttributesCount == 5 && s.Name == "n"
			}, false,
		},
		{"scope dropped bad", `{"resourceLogs":[{"scopeLogs":[{"scope":{"droppedAttributesCount":-1}}]}]}`, nil, true},
		{"trace id odd length", record(`"traceId":"abc"`), nil, true},
		{"trace id 16 bytes upper case", record(`"traceId":"5B8EFFF798038103D269B633813FC60C"`), func(d capture.LogsData) bool { return len(rec(d).TraceID) == 16 }, false},
		{"trace id empty", record(`"traceId":""`), func(d capture.LogsData) bool { return len(rec(d).TraceID) == 0 }, false},
		{"trace id 15 bytes", record(`"traceId":"5b8efff798038103d269b633813fc6"`), nil, true},
		{"trace id 17 bytes", record(`"traceId":"5b8efff798038103d269b633813fc60c00"`), nil, true},
		{"trace id 8 bytes", record(`"traceId":"eee19b7ec3c1b174"`), nil, true},
		{"span id 8 bytes", record(`"spanId":"EEE19B7EC3C1B174"`), func(d capture.LogsData) bool { return len(rec(d).SpanID) == 8 }, false},
		{"span id empty", record(`"spanId":""`), func(d capture.LogsData) bool { return len(rec(d).SpanID) == 0 }, false},
		{"span id 7 bytes", record(`"spanId":"eee19b7ec3c1b1"`), nil, true},
		{"span id 9 bytes", record(`"spanId":"eee19b7ec3c1b17400"`), nil, true},
		{"span id 16 bytes", record(`"spanId":"5b8efff798038103d269b633813fc60c"`), nil, true},
		{"span id not hex", record(`"spanId":"zz"`), nil, true},
		{
			"schema url", `{"resourceLogs":[{"schemaUrl":"https://example.test/1","scopeLogs":[{"schemaUrl":"https://example.test/2"}]}]}`,
			func(d capture.LogsData) bool {
				return d.ResourceLogs[0].SchemaURL == "https://example.test/1" && d.ResourceLogs[0].ScopeLogs[0].SchemaURL == "https://example.test/2"
			}, false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, err := capture.DecodeLogs([]byte(tc.in))
			if (err != nil) != tc.wantErr {
				t.Fatalf("DecodeLogs error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				if !errors.Is(err, capture.ErrFormat) {
					t.Errorf("error %q does not wrap ErrFormat", err)
				}
				return
			}
			if !tc.check(d) {
				t.Errorf("unexpected value: %+v", d)
			}
		})
	}
}

func rec(d capture.LogsData) capture.LogRecord { return d.ResourceLogs[0].ScopeLogs[0].LogRecords[0] }

func TestEncodeLogs32Bit(t *testing.T) {
	t.Parallel()
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource: capture.Resource{DroppedAttributesCount: 1},
		ScopeLogs: []capture.ScopeLogs{{
			Scope:      capture.InstrumentationScope{DroppedAttributesCount: 2},
			LogRecords: []capture.LogRecord{{SeverityNumber: 9, Flags: 1, DroppedAttributesCount: 3}},
		}},
	}}}
	got, err := capture.EncodeLogs(d)
	if err != nil {
		t.Fatalf("EncodeLogs: %v", err)
	}
	want := `{"resourceLogs":[{"resource":{"droppedAttributesCount":1},"scopeLogs":[{"scope":{"droppedAttributesCount":2},` +
		`"logRecords":[{"severityNumber":9,"droppedAttributesCount":3,"flags":1}]}]}]}`
	if string(got) != want {
		t.Errorf("EncodeLogs =\n%s\nwant\n%s", got, want)
	}
}

func TestEncodeLogsOmitsDefaults(t *testing.T) {
	t.Parallel()
	got, err := capture.EncodeLogs(capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{LogRecords: []capture.LogRecord{{}}}}}}})
	if err != nil {
		t.Fatalf("EncodeLogs: %v", err)
	}
	// Resource and scope are structs: written even when empty.
	if want := `{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{}]}]}]}`; string(got) != want {
		t.Errorf("EncodeLogs = %s, want %s", got, want)
	}
}

func TestDecodeLogsRefusals(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in string }{
		{"metrics line", metricsLine},
		{"traces line", tracesLine},
		{"no signal", `{}`},
		{"not an object", `[]`},
		{"empty", ``},
		{"wrong type", `{"resourceLogs":{}}`},
		{"bad nested value", `{"resourceLogs":[{"resource":{"attributes":[{"key":"k","value":{"stringValue":"a","intValue":"1"}}]}}]}`},
		{"invalid UTF-8", "{\"resourceLogs\":[{\"schemaUrl\":\"\xff\"}]}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := capture.DecodeLogs([]byte(tc.in)); err == nil || !errors.Is(err, capture.ErrFormat) {
				t.Errorf("DecodeLogs(%q) error = %v, want one wrapping ErrFormat", tc.in, err)
			}
		})
	}
}

// TestDecodeLogsKeyCase pins a quirk the package documents: encoding/json
// matches keys case-insensitively, so typed decoding reads "TimeUnixNano" as
// "timeUnixNano", but the signal key is matched exactly.
func TestDecodeLogsKeyCase(t *testing.T) {
	t.Parallel()
	d, err := capture.DecodeLogs([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"TimeUnixNano":"9"}]}]}]}`))
	if err != nil || rec(d).TimeUnixNano != 9 {
		t.Errorf("DecodeLogs = %+v, %v; want time 9", d, err)
	}
	if _, err := capture.DecodeLogs([]byte(`{"ResourceLogs":[]}`)); err == nil {
		t.Errorf("DecodeLogs accepted a signal key in the wrong case")
	}
}

// TestEntityRefsRoundTrip checks entity references survive a decode and an
// encode, as the JSON they were.
func TestEntityRefsRoundTrip(t *testing.T) {
	t.Parallel()
	const refs = `[{"schemaUrl":"https://example.test/s","type":"k8s.pod","idKeys":["k8s.pod.uid"],"descriptionKeys":["k8s.pod.name"]}]`
	in := `{"resourceLogs":[{"resource":{"attributes":[{"key":"k8s.pod.uid","value":{"stringValue":"u-1"}}],"entityRefs":` + refs + `}}]}`
	d, err := capture.DecodeLogs([]byte(in))
	if err != nil {
		t.Fatalf("DecodeLogs: %v", err)
	}
	if got := string(d.ResourceLogs[0].Resource.EntityRefs); got != refs {
		t.Errorf("EntityRefs = %s, want %s", got, refs)
	}
	out, err := capture.EncodeLogs(d)
	if err != nil {
		t.Fatalf("EncodeLogs: %v", err)
	}
	if !strings.Contains(string(out), `"entityRefs":`+refs) {
		t.Errorf("encoding lost the entity references: %s", out)
	}
	// Re-encoding the decoded encoding changes nothing.
	again, err := capture.DecodeLogs(out)
	if err != nil || !reflect.DeepEqual(again, d) {
		t.Errorf("second round trip: %v", err)
	}
	// Spaces in the input are compacted, and an absent field stays absent.
	d, err = capture.DecodeLogs([]byte(`{"resourceLogs":[{"resource":{"entityRefs": [ {"type": "x"} ]}}]}`))
	if err != nil || string(d.ResourceLogs[0].Resource.EntityRefs) == "" {
		t.Fatalf("DecodeLogs: %v, %+v", err, d)
	}
	out, _ = capture.EncodeLogs(d)
	if !strings.Contains(string(out), `"entityRefs":[{"type":"x"}]`) {
		t.Errorf("encoding = %s", out)
	}
	d, _ = capture.DecodeLogs([]byte(`{"resourceLogs":[{"resource":{}}]}`))
	if out, _ := capture.EncodeLogs(d); strings.Contains(string(out), "entityRefs") {
		t.Errorf("encoding invents entityRefs: %s", out)
	}
}

// TestEntityRefsNull checks a null entityRefs is the same as an absent one.
func TestEntityRefsNull(t *testing.T) {
	t.Parallel()
	d, err := capture.DecodeLogs([]byte(`{"resourceLogs":[{"resource":{"entityRefs":null,"droppedAttributesCount":1}}]}`))
	if err != nil {
		t.Fatalf("DecodeLogs: %v", err)
	}
	if got := d.ResourceLogs[0].Resource.EntityRefs; got != nil {
		t.Errorf("EntityRefs = %s, want nil", got)
	}
	out, err := capture.EncodeLogs(d)
	if err != nil || strings.Contains(string(out), "entityRefs") {
		t.Errorf("encoding = %s, %v; want no entityRefs key", out, err)
	}
	if !strings.Contains(string(out), `"droppedAttributesCount":1`) {
		t.Errorf("encoding lost the other fields: %s", out)
	}
}
