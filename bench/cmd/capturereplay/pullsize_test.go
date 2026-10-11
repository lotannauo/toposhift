package main

import (
	"bytes"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// threePodLine is a pull of three pods whose records are as small as the receiver's are
// in shape: a body that holds the pod's uid, and the resource's name as an attribute.
func threePodLine(t *testing.T) []byte {
	t.Helper()
	var recs []capture.LogRecord
	for _, uid := range []string{"u1", "u2", "u3"} {
		body := obj(kv("metadata", obj(kv("uid", str(uid)))))
		recs = append(recs, capture.LogRecord{
			ObservedTimeUnixNano: nanos(t0),
			Body:                 &body,
			Attributes:           []capture.KeyValue{kv("k8s.resource.name", str("pods"))},
		})
	}
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		ScopeLogs: []capture.ScopeLogs{{Scope: capture.InstrumentationScope{Name: "s"}, LogRecords: recs}},
	}}}
	raw, err := capture.EncodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMeasurePullOfThreePods(t *testing.T) {
	raw := threePodLine(t)
	ps, err := measurePull(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ps.Records != 3 || ps.JSONBytes != len(raw) || !ps.Fits {
		t.Errorf("%+v", ps)
	}
	// By hand, inside out, for one record: the uid "u1" is 4 bytes as a value, 11 as a
	// pair with its key, 13 as the list of one, 15 as a value that holds it, 27 as the pair
	// "metadata", 29 as the list of one, and 31 as the body value; with the body's tag and
	// length that is 33. The attribute is 29 (the key's 19, the value's 8, and 2 for the
	// tag and length of the pair), and the observed time 9. A record is 71 bytes, 73 with
	// its tag and length, and three of them 219. The scope adds 5, so a scope's logs are
	// 224 bytes and 227 with a tag and a two-byte length; the empty resource adds 2, so a
	// resource's logs are 229 bytes and the message 1 + 2 + 229 = 232.
	if ps.ProtoBytes != 232 {
		t.Errorf("protobuf %d bytes, want 232", ps.ProtoBytes)
	}
	if ps.JSONGzip <= 0 || ps.ProtoGzip <= 0 {
		t.Errorf("gzip: %+v", ps)
	}
	// The same line, written out by hand, byte for byte, for the first record.
	pb, err := encodeLogs(mustDecode(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	obs := []byte{0x59, 0, 0, 0, 0, 0, 0, 0, 0}
	ns := uint64(t0.UnixNano())
	for i := range 8 {
		obs[1+i] = byte(ns >> (8 * i))
	}
	first := append([]byte{}, 0x2a, 31, 0x32, 29, 0x0a, 27)
	first = append(first, 0x0a, 8)
	first = append(first, "metadata"...)
	first = append(first, 0x12, 15, 0x32, 13, 0x0a, 11, 0x0a, 3)
	first = append(first, "uid"...)
	first = append(first, 0x12, 4, 0x0a, 2)
	first = append(first, "u1"...)
	first = append(first, 0x32, 27, 0x0a, 17)
	first = append(first, "k8s.resource.name"...)
	first = append(first, 0x12, 6, 0x0a, 4)
	first = append(first, "pods"...)
	first = append(first, obs...)
	if !bytes.Contains(pb, first) {
		t.Errorf("the first record is not in the encoding as written by hand:\n% x\n% x", first, pb)
	}
}

func mustDecode(t *testing.T, raw []byte) capture.LogsData {
	t.Helper()
	d, err := capture.DecodeLogs(raw)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestProtobufOfEveryKindOfValue(t *testing.T) {
	s, f, i, bl := "x", 1.5, capture.Int64(-1), true
	bs := []byte{1, 2}
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource: capture.Resource{
			Attributes: []capture.KeyValue{kv("a", capture.AnyValue{BoolValue: &bl})}, DroppedAttributesCount: 2,
			EntityRefs: []byte(`[{"type":"k8s.pod","idKeys":["k8s.pod.uid"]}]`),
		},
		SchemaURL: "u",
		ScopeLogs: []capture.ScopeLogs{{
			Scope: capture.InstrumentationScope{Name: "n", Version: "v", DroppedAttributesCount: 1},
			LogRecords: []capture.LogRecord{{
				TimeUnixNano: 1, SeverityNumber: 9, SeverityText: "INFO", Flags: 1, EventName: "e",
				TraceID: bytes.Repeat([]byte{1}, 16), SpanID: bytes.Repeat([]byte{2}, 8),
				Body: &capture.AnyValue{ArrayValue: &capture.ArrayValue{Values: []capture.AnyValue{
					{StringValue: &s}, {DoubleValue: &f}, {IntValue: &i}, {BytesValue: &bs}, {},
				}}},
			}},
		}},
	}}}
	pb, err := encodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	// A message is a series of fields; reading it back checks that the lengths nest.
	if n, ok := walkFields(pb); !ok || n == 0 {
		t.Errorf("the encoding does not parse as protobuf fields: %d fields, ok %v", n, ok)
	}
	// An empty value of a set kind is still written; an unset one is an empty message.
	empty := ""
	only := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{LogRecords: []capture.LogRecord{{Body: &capture.AnyValue{StringValue: &empty}}}}}}}}
	withString, _ := encodeLogs(only)
	only.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body = &capture.AnyValue{}
	withNone, _ := encodeLogs(only)
	if len(withString) != len(withNone)+2 {
		t.Errorf("an empty string value is %d bytes against an empty value's %d; want 2 more", len(withString), len(withNone))
	}
}

// walkFields reads a message's fields to the end, descending into those that are
// messages by trying them; it reports how many top-level fields it read and whether
// they all fit.
func walkFields(b []byte) (n int, ok bool) {
	for len(b) > 0 {
		tag, k := uvarint(b)
		if k == 0 {
			return n, false
		}
		b = b[k:]
		switch tag & 7 {
		case wireVarint:
			_, k = uvarint(b)
			if k == 0 {
				return n, false
			}
			b = b[k:]
		case wireFixed64:
			if len(b) < 8 {
				return n, false
			}
			b = b[8:]
		case wireFixed32:
			if len(b) < 4 {
				return n, false
			}
			b = b[4:]
		case wireBytes:
			l, k := uvarint(b)
			if k == 0 || uint64(len(b)-k) < l {
				return n, false
			}
			b = b[k+int(l):]
		default:
			return n, false
		}
		n++
	}
	return n, true
}

func uvarint(b []byte) (uint64, int) {
	var x uint64
	for i, c := range b {
		if i == 10 {
			return 0, 0
		}
		x |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return x, i + 1
		}
	}
	return 0, 0
}
