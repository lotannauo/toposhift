package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// receiverLimit is the body size the receiver accepts: 64 MiB, on the wire and after
// decompression.
const receiverLimit = 64 << 20

// pullSize is what one line of the capture would weigh as a request to the receiver.
type pullSize struct {
	JSONBytes int `json:"json_bytes"`
	// ProtoBytes is the line as an OTLP protobuf LogsData.
	ProtoBytes int `json:"proto_bytes"`
	// JSONGzip and ProtoGzip are the same after gzip at its default level.
	JSONGzip  int  `json:"json_gzip_bytes"`
	ProtoGzip int  `json:"proto_gzip_bytes"`
	Records   int  `json:"records"`
	Fits      bool `json:"fits_receiver_limit"`
}

// measurePull decodes one line and measures it as JSON and as protobuf, both plain and
// gzipped. It fits if neither form is over the receiver's limit.
func measurePull(raw []byte) (pullSize, error) {
	d, err := capture.DecodeLogs(raw)
	if err != nil {
		return pullSize{}, err
	}
	pb, err := encodeLogs(d)
	if err != nil {
		return pullSize{}, err
	}
	ps := pullSize{JSONBytes: len(raw), ProtoBytes: len(pb)}
	for _, rl := range d.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			ps.Records += len(sl.LogRecords)
		}
	}
	if ps.JSONGzip, err = gzipSize(raw); err != nil {
		return pullSize{}, err
	}
	if ps.ProtoGzip, err = gzipSize(pb); err != nil {
		return pullSize{}, err
	}
	ps.Fits = ps.JSONBytes <= receiverLimit && ps.ProtoBytes <= receiverLimit
	return ps, nil
}

type countWriter struct{ n int }

func (c *countWriter) Write(p []byte) (int, error) { c.n += len(p); return len(p), nil }

func gzipSize(b []byte) (int, error) {
	var c countWriter
	zw := gzip.NewWriter(&c)
	if _, err := io.Copy(zw, bytes.NewReader(b)); err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return c.n, nil
}

// The OTLP logs messages in the protobuf wire format, written by hand from the
// types of the capture package: this module does not require the generated protobuf types, and
// the size of a line is a count of what a standard encoder would write. Fields come in
// field-number order and proto3 zero values are left out, which is what the standard
// encoder does; a field that is a message is written if the capture's types have it (a resource
// and a scope always, a body only if set), and a member of a value's union is written
// when it is set, whatever its value.

const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
	wireFixed32 = 5
)

func appendTag(b []byte, field, wire int) []byte {
	return binary.AppendUvarint(b, uint64(field)<<3|uint64(wire))
}

func appendString(b []byte, field int, s string) []byte {
	b = appendTag(b, field, wireBytes)
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBytesField(b []byte, field int, p []byte) []byte {
	b = appendTag(b, field, wireBytes)
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func appendMessage(b []byte, field int, msg []byte) []byte {
	return appendBytesField(b, field, msg)
}

func appendFixed64(b []byte, field int, v uint64) []byte {
	b = appendTag(b, field, wireFixed64)
	return binary.LittleEndian.AppendUint64(b, v)
}

func appendFixed32(b []byte, field int, v uint32) []byte {
	b = appendTag(b, field, wireFixed32)
	return binary.LittleEndian.AppendUint32(b, v)
}

func appendVarintField(b []byte, field int, v uint64) []byte {
	b = appendTag(b, field, wireVarint)
	return binary.AppendUvarint(b, v)
}

func encodeAnyValue(v capture.AnyValue) []byte {
	var b []byte
	switch {
	case v.StringValue != nil:
		b = appendString(b, 1, *v.StringValue)
	case v.BoolValue != nil:
		x := uint64(0)
		if *v.BoolValue {
			x = 1
		}
		b = appendVarintField(b, 2, x)
	case v.IntValue != nil:
		b = appendVarintField(b, 3, uint64(*v.IntValue))
	case v.DoubleValue != nil:
		b = appendFixed64(b, 4, math.Float64bits(*v.DoubleValue))
	case v.ArrayValue != nil:
		var arr []byte
		for _, x := range v.ArrayValue.Values {
			arr = appendMessage(arr, 1, encodeAnyValue(x))
		}
		b = appendMessage(b, 5, arr)
	case v.KVListValue != nil:
		b = appendMessage(b, 6, encodeKVs(v.KVListValue.Values, 1))
	case v.BytesValue != nil:
		b = appendBytesField(b, 7, *v.BytesValue)
	}
	return b
}

// encodeKVs writes key-value pairs as the repeated field of a message.
func encodeKVs(kvs []capture.KeyValue, field int) []byte {
	var b []byte
	for _, kv := range kvs {
		var m []byte
		if kv.Key != "" {
			m = appendString(m, 1, kv.Key)
		}
		m = appendMessage(m, 2, encodeAnyValue(kv.Value))
		b = appendMessage(b, field, m)
	}
	return b
}

func encodeLogRecord(l capture.LogRecord) []byte {
	var b []byte
	if l.TimeUnixNano != 0 {
		b = appendFixed64(b, 1, uint64(l.TimeUnixNano))
	}
	if l.SeverityNumber != 0 {
		b = appendVarintField(b, 2, uint64(int64(l.SeverityNumber)))
	}
	if l.SeverityText != "" {
		b = appendString(b, 3, l.SeverityText)
	}
	if l.Body != nil {
		b = appendMessage(b, 5, encodeAnyValue(*l.Body))
	}
	b = append(b, encodeKVs(l.Attributes, 6)...)
	if l.DroppedAttributesCount != 0 {
		b = appendVarintField(b, 7, uint64(l.DroppedAttributesCount))
	}
	if l.Flags != 0 {
		b = appendFixed32(b, 8, l.Flags)
	}
	if len(l.TraceID) != 0 {
		b = appendBytesField(b, 9, l.TraceID)
	}
	if len(l.SpanID) != 0 {
		b = appendBytesField(b, 10, l.SpanID)
	}
	if l.ObservedTimeUnixNano != 0 {
		b = appendFixed64(b, 11, uint64(l.ObservedTimeUnixNano))
	}
	if l.EventName != "" {
		b = appendString(b, 12, l.EventName)
	}
	return b
}

// entityRefJSON is the OTLP/JSON form of an entity reference, as the capture keeps it.
type entityRefJSON struct {
	SchemaURL       string   `json:"schemaUrl"`
	Type            string   `json:"type"`
	IDKeys          []string `json:"idKeys"`
	DescriptionKeys []string `json:"descriptionKeys"`
}

func encodeResource(r capture.Resource) ([]byte, error) {
	b := encodeKVs(r.Attributes, 1)
	if r.DroppedAttributesCount != 0 {
		b = appendVarintField(b, 2, uint64(r.DroppedAttributesCount))
	}
	if len(r.EntityRefs) != 0 {
		var refs []entityRefJSON
		if err := json.Unmarshal(r.EntityRefs, &refs); err != nil {
			return nil, fmt.Errorf("entity refs: %w", err)
		}
		for _, ref := range refs {
			var m []byte
			if ref.SchemaURL != "" {
				m = appendString(m, 1, ref.SchemaURL)
			}
			if ref.Type != "" {
				m = appendString(m, 2, ref.Type)
			}
			for _, k := range ref.IDKeys {
				m = appendString(m, 3, k)
			}
			for _, k := range ref.DescriptionKeys {
				m = appendString(m, 4, k)
			}
			b = appendMessage(b, 3, m)
		}
	}
	return b, nil
}

func encodeScope(s capture.InstrumentationScope) []byte {
	var b []byte
	if s.Name != "" {
		b = appendString(b, 1, s.Name)
	}
	if s.Version != "" {
		b = appendString(b, 2, s.Version)
	}
	b = append(b, encodeKVs(s.Attributes, 3)...)
	if s.DroppedAttributesCount != 0 {
		b = appendVarintField(b, 4, uint64(s.DroppedAttributesCount))
	}
	return b
}

// encodeLogs writes a LogsData as the protobuf bytes an exporter would send.
func encodeLogs(d capture.LogsData) ([]byte, error) {
	var b []byte
	for _, rl := range d.ResourceLogs {
		res, err := encodeResource(rl.Resource)
		if err != nil {
			return nil, err
		}
		m := appendMessage(nil, 1, res)
		for _, sl := range rl.ScopeLogs {
			var s []byte
			s = appendMessage(s, 1, encodeScope(sl.Scope))
			for _, lr := range sl.LogRecords {
				s = appendMessage(s, 2, encodeLogRecord(lr))
			}
			if sl.SchemaURL != "" {
				s = appendString(s, 3, sl.SchemaURL)
			}
			m = appendMessage(m, 2, s)
		}
		if rl.SchemaURL != "" {
			m = appendString(m, 3, rl.SchemaURL)
		}
		b = appendMessage(b, 1, m)
	}
	return b, nil
}
