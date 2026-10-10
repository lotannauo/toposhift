package otlphttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// snippet quotes the start of s, for an error message: what a client sends is
// not echoed back in full.
func snippet(s string) string {
	const limit = 32
	if len(s) > limit {
		return strconv.Quote(s[:limit]) + "..."
	}
	return strconv.Quote(s)
}

// errUnsupported is wrapped by the errors of a protobuf message the model cannot
// hold without losing or guessing at something.
var errUnsupported = errors.New("unsupported")

// The sizes of trace and span IDs; an ID is absent or exactly this long. The
// JSON decoder holds the same rule.
const (
	traceIDSize = 16
	spanIDSize  = 8
)

// convertLogs turns a protobuf LogsData into the OTLP JSON model. An empty
// repeated field becomes a nil slice and an empty ID becomes nil, which is
// what decoding the JSON of the same data gives.
func convertLogs(d *logspb.LogsData) (capture.LogsData, error) {
	var out capture.LogsData
	for _, rl := range d.GetResourceLogs() {
		res, err := convertResource(rl.GetResource())
		if err != nil {
			return capture.LogsData{}, err
		}
		o := capture.ResourceLogs{Resource: res, SchemaURL: rl.GetSchemaUrl()}
		for _, sl := range rl.GetScopeLogs() {
			scope, err := convertScope(sl.GetScope())
			if err != nil {
				return capture.LogsData{}, err
			}
			so := capture.ScopeLogs{Scope: scope, SchemaURL: sl.GetSchemaUrl()}
			for _, lr := range sl.GetLogRecords() {
				rec, err := convertRecord(lr)
				if err != nil {
					return capture.LogsData{}, err
				}
				so.LogRecords = append(so.LogRecords, rec)
			}
			o.ScopeLogs = append(o.ScopeLogs, so)
		}
		out.ResourceLogs = append(out.ResourceLogs, o)
	}
	return out, nil
}

func convertResource(r *resourcepb.Resource) (capture.Resource, error) {
	attrs, err := convertAttributes(r.GetAttributes())
	if err != nil {
		return capture.Resource{}, fmt.Errorf("resource attributes: %w", err)
	}
	refs, err := convertEntityRefs(r.GetEntityRefs())
	if err != nil {
		return capture.Resource{}, err
	}
	return capture.Resource{
		Attributes:             attrs,
		DroppedAttributesCount: r.GetDroppedAttributesCount(),
		EntityRefs:             refs,
	}, nil
}

func convertScope(s *commonpb.InstrumentationScope) (capture.InstrumentationScope, error) {
	attrs, err := convertAttributes(s.GetAttributes())
	if err != nil {
		return capture.InstrumentationScope{}, fmt.Errorf("scope attributes: %w", err)
	}
	return capture.InstrumentationScope{
		Name:                   s.GetName(),
		Version:                s.GetVersion(),
		Attributes:             attrs,
		DroppedAttributesCount: s.GetDroppedAttributesCount(),
	}, nil
}

func convertRecord(l *logspb.LogRecord) (capture.LogRecord, error) {
	if n := len(l.GetTraceId()); n != 0 && n != traceIDSize {
		return capture.LogRecord{}, fmt.Errorf("traceId is %d bytes, want %d", n, traceIDSize)
	}
	if n := len(l.GetSpanId()); n != 0 && n != spanIDSize {
		return capture.LogRecord{}, fmt.Errorf("spanId is %d bytes, want %d", n, spanIDSize)
	}
	attrs, err := convertAttributes(l.GetAttributes())
	if err != nil {
		return capture.LogRecord{}, fmt.Errorf("log record attributes: %w", err)
	}
	var body *capture.AnyValue
	if l.GetBody() != nil {
		v, err := convertValue(l.GetBody())
		if err != nil {
			return capture.LogRecord{}, fmt.Errorf("log record body: %w", err)
		}
		body = &v
	}
	return capture.LogRecord{
		TimeUnixNano:           capture.Uint64(l.GetTimeUnixNano()),
		ObservedTimeUnixNano:   capture.Uint64(l.GetObservedTimeUnixNano()),
		SeverityNumber:         int32(l.GetSeverityNumber()),
		SeverityText:           l.GetSeverityText(),
		Body:                   body,
		Attributes:             attrs,
		DroppedAttributesCount: l.GetDroppedAttributesCount(),
		Flags:                  l.GetFlags(),
		TraceID:                idBytes(l.GetTraceId()),
		SpanID:                 idBytes(l.GetSpanId()),
		EventName:              l.GetEventName(),
	}, nil
}

// idBytes copies an ID, and returns nil for an empty one, as decoding "" does.
func idBytes(b []byte) capture.HexBytes {
	if len(b) == 0 {
		return nil
	}
	return capture.HexBytes(bytes.Clone(b))
}

func convertAttributes(kvs []*commonpb.KeyValue) ([]capture.KeyValue, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	out := make([]capture.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		if kv.GetKeyStrindex() != 0 {
			return nil, fmt.Errorf("key %s: key_strindex needs a string table, which logs do not carry: %w", snippet(kv.GetKey()), errUnsupported)
		}
		var v capture.AnyValue
		if kv.GetValue() != nil {
			var err error
			if v, err = convertValue(kv.GetValue()); err != nil {
				return nil, fmt.Errorf("key %s: %w", snippet(kv.GetKey()), err)
			}
		}
		out = append(out, capture.KeyValue{Key: kv.GetKey(), Value: v})
	}
	return out, nil
}

// convertValue converts one value. An unset oneof is the empty value, as {} is
// in JSON.
func convertValue(v *commonpb.AnyValue) (capture.AnyValue, error) {
	var out capture.AnyValue
	switch x := v.GetValue().(type) {
	case nil:
	case *commonpb.AnyValue_StringValue:
		out.StringValue = &x.StringValue
	case *commonpb.AnyValue_BoolValue:
		out.BoolValue = &x.BoolValue
	case *commonpb.AnyValue_IntValue:
		i := capture.Int64(x.IntValue)
		out.IntValue = &i
	case *commonpb.AnyValue_DoubleValue:
		out.DoubleValue = &x.DoubleValue
	case *commonpb.AnyValue_ArrayValue:
		arr := new(capture.ArrayValue)
		for _, e := range x.ArrayValue.GetValues() {
			c, err := convertValue(e)
			if err != nil {
				return capture.AnyValue{}, err
			}
			arr.Values = append(arr.Values, c)
		}
		out.ArrayValue = arr
	case *commonpb.AnyValue_KvlistValue:
		kvs, err := convertAttributes(x.KvlistValue.GetValues())
		if err != nil {
			return capture.AnyValue{}, err
		}
		out.KVListValue = &capture.KeyValueList{Values: kvs}
	case *commonpb.AnyValue_BytesValue:
		b := bytes.Clone(x.BytesValue)
		if b == nil {
			b = []byte{} // a set bytes value is never nil, as in JSON
		}
		out.BytesValue = &b
	case *commonpb.AnyValue_StringValueStrindex:
		return capture.AnyValue{}, fmt.Errorf("string_value_strindex needs a string table, which logs do not carry: %w", errUnsupported)
	default:
		return capture.AnyValue{}, fmt.Errorf("value of an unknown kind %T: %w", x, errUnsupported)
	}
	return out, nil
}

// entityRef is the OTLP/JSON form of an EntityRef.
type entityRef struct {
	SchemaURL       string   `json:"schemaUrl,omitempty"`
	Type            string   `json:"type,omitempty"`
	IDKeys          []string `json:"idKeys,omitempty"`
	DescriptionKeys []string `json:"descriptionKeys,omitempty"`
}

// convertEntityRefs writes the entity references as the JSON array the model
// keeps them as, compact and with the fields in the order of the proto
// message. Nothing in it needs a typed model here.
func convertEntityRefs(refs []*commonpb.EntityRef) (json.RawMessage, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	list := make([]entityRef, 0, len(refs))
	for _, r := range refs {
		list = append(list, entityRef{
			SchemaURL:       r.GetSchemaUrl(),
			Type:            r.GetType(),
			IDKeys:          r.GetIdKeys(),
			DescriptionKeys: r.GetDescriptionKeys(),
		})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(list); err != nil {
		return nil, fmt.Errorf("entity refs: %w", err)
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}
