package k8sobjects

import (
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// object is a decoded Kubernetes object, or any mapping inside one.
type object = map[string]any

// plain converts an OTLP value to plain Go values: strings, int64, float64,
// bool, []byte, []any and object. The receiver encodes a Kubernetes object as
// nested key-value lists, so this recovers the object. An empty value is nil.
// When a key repeats in a list, the last one wins.
func plain(v capture.AnyValue) any {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return int64(*v.IntValue)
	case v.BoolValue != nil:
		return *v.BoolValue
	case v.DoubleValue != nil:
		return *v.DoubleValue
	case v.BytesValue != nil:
		return *v.BytesValue
	case v.ArrayValue != nil:
		out := make([]any, len(v.ArrayValue.Values))
		for i, e := range v.ArrayValue.Values {
			out[i] = plain(e)
		}
		return out
	case v.KVListValue != nil:
		out := make(object, len(v.KVListValue.Values))
		for _, kv := range v.KVListValue.Values {
			out[kv.Key] = plain(kv.Value)
		}
		return out
	}
	return nil
}

// walk follows a path of keys through nested objects and returns what it finds, or
// nil.
func walk(o object, path ...string) any {
	var cur any = o
	for _, k := range path {
		m, ok := cur.(object)
		if !ok {
			return nil
		}
		cur = m[k]
	}
	return cur
}

// str returns the string at the path, or "" if there is none or it is not a
// string.
func str(o object, path ...string) string {
	s, _ := walk(o, path...).(string)
	return s
}

// obj returns the object at the path, or nil.
func obj(o object, path ...string) object {
	m, _ := walk(o, path...).(object)
	return m
}

// list returns the objects in the array at the path, skipping any element that
// is not an object.
func list(o object, path ...string) []object {
	a, _ := walk(o, path...).([]any)
	out := make([]object, 0, len(a))
	for _, e := range a {
		if m, ok := e.(object); ok {
			out = append(out, m)
		}
	}
	return out
}

// integer returns the whole number at the path. A collector may write a count
// as an integer or as a floating-point number.
func integer(o object, path ...string) int64 {
	switch n := walk(o, path...).(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

// parseTime reads a Kubernetes timestamp (RFC 3339, whole seconds, UTC).
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}
