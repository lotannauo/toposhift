package capture

import (
	"encoding/json"
	"fmt"
)

// LogsData is the OTLP LogsData message: the logs of one collector batch.
//
// Decoding ignores fields the model does not name, as the OTLP/JSON
// specification requires, so a decoded and re-encoded line can differ from
// the original. Use [Line.Raw] to keep a line exactly.
type LogsData struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs,omitempty"`
}

// ResourceLogs holds the logs of one resource.
type ResourceLogs struct {
	Resource  Resource    `json:"resource"`
	ScopeLogs []ScopeLogs `json:"scopeLogs,omitempty"`
	SchemaURL string      `json:"schemaUrl,omitempty"`
}

// Resource describes the entity that produced telemetry.
type Resource struct {
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`

	// EntityRefs is the entityRefs array, kept as the JSON it was so that
	// entity references survive a decode and an encode without a typed model.
	EntityRefs json.RawMessage `json:"entityRefs,omitempty"`
}

// ScopeLogs holds the logs one instrumentation scope produced.
type ScopeLogs struct {
	Scope      InstrumentationScope `json:"scope"`
	LogRecords []LogRecord          `json:"logRecords,omitempty"`
	SchemaURL  string               `json:"schemaUrl,omitempty"`
}

// InstrumentationScope names the library that produced telemetry.
type InstrumentationScope struct {
	Name                   string     `json:"name,omitempty"`
	Version                string     `json:"version,omitempty"`
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`
}

// LogRecord is one log record. A zero time is unset in OTLP. Decoding refuses
// a TraceID that is not 16 bytes and a SpanID that is not 8, unless it is
// empty.
type LogRecord struct {
	TimeUnixNano           Uint64     `json:"timeUnixNano,omitempty"`
	ObservedTimeUnixNano   Uint64     `json:"observedTimeUnixNano,omitempty"`
	SeverityNumber         int32      `json:"severityNumber,omitempty"`
	SeverityText           string     `json:"severityText,omitempty"`
	Body                   *AnyValue  `json:"body,omitempty"`
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`
	Flags                  uint32     `json:"flags,omitempty"`
	TraceID                HexBytes   `json:"traceId,omitempty"`
	SpanID                 HexBytes   `json:"spanId,omitempty"`
	EventName              string     `json:"eventName,omitempty"`
}

// KeyValue is one attribute.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// MarshalJSON always writes resourceLogs, as an empty array if there are no
// resources, so that every encoded value is a line a [Reader] accepts.
func (d LogsData) MarshalJSON() ([]byte, error) {
	rl := d.ResourceLogs
	if rl == nil {
		rl = []ResourceLogs{}
	}
	return marshalNoEscape(struct {
		ResourceLogs []ResourceLogs `json:"resourceLogs"`
	}{rl})
}

// DecodeLogs decodes one line of a logs capture. The line must be a valid
// logs line: a JSON object with a resourceLogs key.
//
// The signal key is matched exactly; the other keys are matched by
// [encoding/json], which ignores case, so "TimeUnixNano" is read as
// "timeUnixNano" although the OTLP/JSON specification does not allow it.
func DecodeLogs(raw json.RawMessage) (LogsData, error) {
	sig, err := parseLine(raw)
	if err != nil {
		return LogsData{}, err
	}
	if sig != Logs {
		return LogsData{}, formatErrorf(0, nil, "%s line, not logs", sig)
	}
	var d LogsData
	if err := json.Unmarshal(raw, &d); err != nil {
		return LogsData{}, formatErrorf(0, err, "logs: %v", err)
	}
	return d, nil
}

// EncodeLogs encodes d as one line without a trailing newline. 64-bit
// integers are decimal strings, enumerations are numbers, trace and span IDs
// are lowercase hex, other bytes are base64, fields with default values are
// omitted, and <, > and & are not escaped.
func EncodeLogs(d LogsData) ([]byte, error) {
	return marshalNoEscape(d)
}

// The 32-bit fields are JSON numbers on output and a number or a decimal
// string on input. encoding/json cannot do the second on its own, so the
// types that have such fields decode through a shadow struct whose fields
// shadow the 32-bit ones with raw messages.

// UnmarshalJSON implements [json.Unmarshaler].
func (r *Resource) UnmarshalJSON(b []byte) error {
	type plain Resource
	var out Resource
	aux := struct {
		*plain
		DroppedAttributesCount json.RawMessage `json:"droppedAttributesCount"`
	}{plain: (*plain)(&out)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var err error
	if out.DroppedAttributesCount, err = parseUint32(aux.DroppedAttributesCount); err != nil {
		return fmt.Errorf("droppedAttributesCount: %w", err)
	}
	if string(out.EntityRefs) == "null" {
		out.EntityRefs = nil // JSON null is absent, so it is not written back
	}
	*r = out
	return nil
}

// UnmarshalJSON implements [json.Unmarshaler].
func (s *InstrumentationScope) UnmarshalJSON(b []byte) error {
	type plain InstrumentationScope
	var out InstrumentationScope
	aux := struct {
		*plain
		DroppedAttributesCount json.RawMessage `json:"droppedAttributesCount"`
	}{plain: (*plain)(&out)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var err error
	if out.DroppedAttributesCount, err = parseUint32(aux.DroppedAttributesCount); err != nil {
		return fmt.Errorf("droppedAttributesCount: %w", err)
	}
	*s = out
	return nil
}

// UnmarshalJSON implements [json.Unmarshaler].
func (l *LogRecord) UnmarshalJSON(b []byte) error {
	type plain LogRecord
	var out LogRecord
	aux := struct {
		*plain
		SeverityNumber         json.RawMessage `json:"severityNumber"`
		DroppedAttributesCount json.RawMessage `json:"droppedAttributesCount"`
		Flags                  json.RawMessage `json:"flags"`
	}{plain: (*plain)(&out)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var err error
	if out.SeverityNumber, err = parseInt32(aux.SeverityNumber); err != nil {
		return fmt.Errorf("severityNumber: %w", err)
	}
	if out.DroppedAttributesCount, err = parseUint32(aux.DroppedAttributesCount); err != nil {
		return fmt.Errorf("droppedAttributesCount: %w", err)
	}
	if out.Flags, err = parseUint32(aux.Flags); err != nil {
		return fmt.Errorf("flags: %w", err)
	}
	if n := len(out.TraceID); n != 0 && n != traceIDSize {
		return formatErrorf(0, nil, "traceId is %d bytes, want %d", n, traceIDSize)
	}
	if n := len(out.SpanID); n != 0 && n != spanIDSize {
		return formatErrorf(0, nil, "spanId is %d bytes, want %d", n, spanIDSize)
	}
	*l = out
	return nil
}

// The sizes of trace and span IDs; an ID is absent or exactly this long.
const (
	traceIDSize = 16
	spanIDSize  = 8
)
