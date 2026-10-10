package otlphttp

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// FuzzDecodeBody feeds arbitrary bytes to the decoder of each encoding. Neither
// may panic. What a decoder accepts must be a value the capture model can
// write, and writing it, reading it back and writing it again must give the
// same bytes: a protobuf body that decodes is therefore a value of the same
// model JSON decoding gives, not some other shape.
func FuzzDecodeBody(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"resourceLogs":[]}`))
	f.Add([]byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"k","value":{"stringValue":"v"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1","traceId":"5b8efff798038103d269b633813fc60c","body":{"intValue":"7"}}]}]}]}`))
	// A protobuf message with one resource, one scope and one record with a body.
	f.Add([]byte{0x0a, 0x0c, 0x12, 0x0a, 0x12, 0x08, 0x2a, 0x06, 0x0a, 0x04, 'b', 'o', 'd', 'y'})
	f.Add([]byte{0x0a, 0x02, 0x0a, 0x00})
	f.Add([]byte{0x7b, 0x22})
	// Deep nesting, at and either side of the limits.
	for _, n := range []int{maxValueDepth, maxValueDepth + 1, 50, 3000} {
		f.Add(deepJSONKVList(n))
		f.Add(deepJSONArray(n))
		f.Add(deepProtoArray(n))
	}
	f.Add([]byte(`{"resourceLogs":[],"x":` + strings.Repeat("[", maxJSONDepth) + strings.Repeat("]", maxJSONDepth) + `}`))
	f.Add([]byte(`{"x":"` + strings.Repeat(`[{\"`, 100) + `"}`))
	f.Add([]byte("{ }"))

	f.Fuzz(func(t *testing.T, b []byte) {
		for _, k := range []kind{kindProto, kindJSON} {
			d, err := decodeBody(k, bytes.Clone(b))
			if err != nil {
				continue
			}
			if err := checkDepth(d); err != nil {
				t.Fatalf("kind %d: an accepted body is too deep: %v", k, err)
			}
			first, err := capture.EncodeLogs(d)
			if err != nil {
				t.Fatalf("kind %d: an accepted body does not encode: %v", k, err)
			}
			again, err := capture.DecodeLogs(first)
			if err != nil {
				t.Fatalf("kind %d: the encoding of an accepted body does not decode: %v\n%s", k, err, first)
			}
			second, err := capture.EncodeLogs(again)
			if err != nil {
				t.Fatalf("kind %d: %v", k, err)
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("kind %d: encode, decode, encode is not stable:\n%s\n%s", k, first, second)
			}
		}
	})
}

// deepJSONKVList is a logs line with a body that is a key-value list nested n
// values deep (a scalar is 1).
func deepJSONKVList(n int) []byte {
	return []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":` +
		strings.Repeat(`{"kvlistValue":{"values":[{"key":"k","value":`, n-1) + `{"stringValue":"x"}` +
		strings.Repeat(`}]}}`, n-1) + `}]}]}]}`)
}

// deepJSONArray is the same with arrays.
func deepJSONArray(n int) []byte {
	return []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":` +
		strings.Repeat(`{"arrayValue":{"values":[`, n-1) + `{"stringValue":"x"}` +
		strings.Repeat(`]}}`, n-1) + `}]}]}]}`)
}

// deepProtoArray is a request whose first record has a body that is an array
// nested n values deep, written by hand.
func deepProtoArray(n int) []byte {
	msg := func(field protowire.Number, b []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, field, protowire.BytesType), b)
	}
	v := msg(1, []byte("x")) // AnyValue.string_value
	for i := 1; i < n; i++ {
		v = msg(5, msg(1, v)) // AnyValue.array_value { values: [v] }
	}
	return msg(1, msg(2, msg(2, msg(5, v)))) // resource_logs, scope_logs, log_records, body
}
