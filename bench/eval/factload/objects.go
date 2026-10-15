package factload

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// Object is one listing of a Kubernetes object in a pull as the collector's objects
// receiver emits it in pull mode: a log record whose body is the object, with the
// resource it was listed from named in an attribute, and whose observed time is
// when it was listed.
type Object struct {
	Resource capture.Resource
	Record   capture.LogRecord
}

// resourceNameKey is the log attribute that names the listed resource, such as
// "pods", which the translator reads when the object does not carry its kind.
const resourceNameKey = "k8s.resource.name"

// NewObject builds the pull-mode log record of one object: its JSON as the body,
// the resource name as the attribute, and the time it was observed.
func NewObject(resourceName string, body []byte, observed time.Time) (Object, error) {
	v, err := BodyFromJSON(body)
	if err != nil {
		return Object{}, err
	}
	if observed.IsZero() || observed.UnixNano() <= 0 {
		return Object{}, errors.New("factload: an object needs an observed time")
	}
	rec := capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()),
		Body:                 &v,
	}
	if resourceName != "" {
		name := resourceName
		rec.Attributes = []capture.KeyValue{{Key: resourceNameKey, Value: capture.AnyValue{StringValue: &name}}}
	}
	return Object{Record: rec}, nil
}

// BodyFromJSON converts a JSON object to the nested key-value lists the objects
// receiver encodes a Kubernetes object as: strings, booleans, integers (a number
// that is a whole int64), doubles, arrays and key-value lists with the keys
// sorted. A null is an empty value.
func BodyFromJSON(raw []byte) (capture.AnyValue, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return capture.AnyValue{}, fmt.Errorf("factload: object body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return capture.AnyValue{}, errors.New("factload: object body: data after the object")
	}
	if _, ok := v.(map[string]any); !ok {
		return capture.AnyValue{}, errors.New("factload: object body is not a JSON object")
	}
	return anyOf(v), nil
}

func anyOf(v any) capture.AnyValue {
	switch x := v.(type) {
	case string:
		return capture.AnyValue{StringValue: &x}
	case bool:
		return capture.AnyValue{BoolValue: &x}
	case json.Number:
		if i, err := strconv.ParseInt(string(x), 10, 64); err == nil {
			n := capture.Int64(i)
			return capture.AnyValue{IntValue: &n}
		}
		f, _ := x.Float64()
		return capture.AnyValue{DoubleValue: &f}
	case []any:
		arr := &capture.ArrayValue{Values: make([]capture.AnyValue, len(x))}
		for i, e := range x {
			arr.Values[i] = anyOf(e)
		}
		return capture.AnyValue{ArrayValue: arr}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		kv := &capture.KeyValueList{Values: make([]capture.KeyValue, len(keys))}
		for i, k := range keys {
			kv.Values[i] = capture.KeyValue{Key: k, Value: anyOf(x[k])}
		}
		return capture.AnyValue{KVListValue: kv}
	}
	return capture.AnyValue{}
}
