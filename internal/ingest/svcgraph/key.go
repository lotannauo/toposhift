package svcgraph

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// encodeSet is the canonical bytes of a set of attributes: the number of
// attributes, then each one as its key and value, in byte order of that encoding.
// The order the attributes were received in does not matter; the number of times a
// key is given does.
func encodeSet(kvs []capture.KeyValue) []byte {
	parts := make([][]byte, len(kvs))
	for i, kv := range kvs {
		parts[i] = appendAny(appendString(nil, kv.Key), kv.Value)
	}
	slices.SortFunc(parts, bytes.Compare)
	out := binary.BigEndian.AppendUint32(nil, uint32(len(parts)))
	for _, p := range parts {
		out = binary.BigEndian.AppendUint32(out, uint32(len(p)))
		out = append(out, p...)
	}
	return out
}

func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

// appendAny encodes a value with a tag for its kind, so that the string "1", the
// integer 1 and the double 1 are three different values.
func appendAny(dst []byte, v capture.AnyValue) []byte {
	switch {
	case v.StringValue != nil:
		return appendString(append(dst, 's'), *v.StringValue)
	case v.BoolValue != nil:
		if *v.BoolValue {
			return append(dst, 'b', 1)
		}
		return append(dst, 'b', 0)
	case v.IntValue != nil:
		return binary.BigEndian.AppendUint64(append(dst, 'i'), uint64(*v.IntValue))
	case v.DoubleValue != nil:
		return binary.BigEndian.AppendUint64(append(dst, 'd'), math.Float64bits(*v.DoubleValue))
	case v.ArrayValue != nil:
		dst = binary.BigEndian.AppendUint32(append(dst, 'a'), uint32(len(v.ArrayValue.Values)))
		for _, e := range v.ArrayValue.Values {
			dst = appendAny(dst, e)
		}
		return dst
	case v.KVListValue != nil:
		dst = binary.BigEndian.AppendUint32(append(dst, 'k'), uint32(len(v.KVListValue.Values)))
		for _, kv := range v.KVListValue.Values {
			dst = appendAny(appendString(dst, kv.Key), kv.Value)
		}
		return dst
	case v.BytesValue != nil:
		return appendString(append(dst, 'y'), string(*v.BytesValue))
	}
	return append(dst, 'n')
}

// seriesKey identifies a series: the resource's attributes, the scope, the metric
// and the point's attributes. The cluster is the translator's own.
func seriesKey(resource []byte, scope, metric string, point []capture.KeyValue) string {
	b := make([]byte, 0, len(resource)+len(scope)+len(metric)+64)
	b = append(b, resource...)
	b = appendString(b, scope)
	b = appendString(b, metric)
	b = append(b, encodeSet(point)...)
	return string(b)
}
