package demo

import (
	"fmt"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The OTLP values a collector sends, built from plain Go values. Map keys are
// written sorted, so a request is the same bytes whenever it is built.

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

func bv(b bool) capture.AnyValue { return capture.AnyValue{BoolValue: &b} }

// val converts plain Go values to an OTLP value.
func val(v any) capture.AnyValue {
	switch x := v.(type) {
	case string:
		return sv(x)
	case int:
		return iv(int64(x))
	case int64:
		return iv(x)
	case bool:
		return bv(x)
	case []any:
		a := &capture.ArrayValue{}
		for _, e := range x {
			a.Values = append(a.Values, val(e))
		}
		return capture.AnyValue{ArrayValue: a}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		l := &capture.KeyValueList{}
		for _, k := range keys {
			l.Values = append(l.Values, capture.KeyValue{Key: k, Value: val(x[k])})
		}
		return capture.AnyValue{KVListValue: l}
	}
	panic(fmt.Sprintf("demo: no OTLP value for a %T", v))
}

// kvlist is a flat list of string attributes, keys sorted.
func kvlist(m map[string]string) capture.AnyValue {
	a := make(map[string]any, len(m))
	for k, v := range m {
		a[k] = v
	}
	return val(a)
}

// stamp is a Kubernetes timestamp: RFC 3339 in whole seconds.
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// The scope the Kubernetes objects receiver writes its records under.
const objectsScope = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sobjectsreceiver"

// objectsRecord is a record of the objects receiver: the body is the object (a
// pull) or a watch event (a watch), and only the observed time is set. attrs are
// the record's attributes beyond k8s.resource.name.
func objectsRecord(observed time.Time, body map[string]any, resource string, attrs ...capture.KeyValue) capture.LogRecord {
	b := val(body)
	lr := capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()),
		Body:                 &b,
		Attributes:           []capture.KeyValue{{Key: "k8s.resource.name", Value: sv(resource)}},
	}
	lr.Attributes = append(lr.Attributes, attrs...)
	return lr
}

// entityEvent is an entity event of the cluster receiver with a report
// interval of five minutes. A delete carries the type and the id alone.
func entityEvent(event, typ, idKey, id string, tm time.Time, desc map[string]string) capture.LogRecord {
	lr := capture.LogRecord{EventName: event, TimeUnixNano: capture.Uint64(tm.UnixNano())}
	lr.Attributes = append(lr.Attributes,
		capture.KeyValue{Key: "entity.id", Value: kvlist(map[string]string{idKey: id})},
		capture.KeyValue{Key: "entity.type", Value: sv(typ)},
	)
	if event == "entity.state" {
		lr.Attributes = append(lr.Attributes,
			capture.KeyValue{Key: "entity.report.interval", Value: iv(int64(ReportEvery / time.Second))},
			capture.KeyValue{Key: "entity.description", Value: kvlist(desc)},
		)
	}
	return lr
}

// entityScope is the scope the receivers write entity events as logs under.
func entityScope() capture.InstrumentationScope {
	return capture.InstrumentationScope{Attributes: []capture.KeyValue{
		{Key: "otel.entity.event_as_log", Value: bv(true)},
	}}
}

// request is one resource with one scope: what a collector sends in one export.
func request(scope capture.InstrumentationScope, res []capture.KeyValue, recs ...capture.LogRecord) capture.LogsData {
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource:  capture.Resource{Attributes: res},
		ScopeLogs: []capture.ScopeLogs{{Scope: scope, LogRecords: recs}},
	}}}
}

// clusterAttr is the attribute the collector's resource processor adds to every
// resource.
func clusterAttr() capture.KeyValue {
	return capture.KeyValue{Key: "k8s.cluster.name", Value: sv(ClusterName)}
}
