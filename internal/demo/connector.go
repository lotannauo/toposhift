package demo

import (
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The trace collector of the shipped configuration (deploy/otel-collector) turns
// the spans of the services into two metrics, and sends them as OTLP metrics: the
// service graph connector's request counter, and the count connector's span
// counter. This file is that collector as a pure function of the world and an
// instant, like the emitters of the cluster's collectors: it counts nothing, draws
// nothing and keeps no state, so that a flush is built from the calls and the pods
// alone.
//
// The model is this. The collector started two hours before the anchor. Its
// service graph connector flushes once a minute, at 20 seconds past, from the
// anchor on; a flush sends every series that was called in the last fifteen
// minutes, each as a cumulative sum from the collector's start whose value counts
// the calls up to the flush, and removes at the first flush after that a series
// that was not called for longer. Every point of a flush is stamped with the
// flush (the real connector stamps each a millisecond apart; one instant keeps the
// answers readable and changes no rule). The count connector sends once a minute
// too, at 50 seconds past, one delta point for each instance whose pod runs on a
// node that is ready: the spans of the minute. The real connector sends about once
// for each batch of spans; one point a minute writes the same sightings, which the
// coalescer folds. A request arrives a second after the instant of its points.

// FlushEvery is how often the trace collector sends: once a minute, the
// connector's flush interval in the shipped configuration.
const FlushEvery = time.Minute

const (
	// flushPast and spanPast are where in the minute the two connectors send, and
	// arrives how long after its points a request is received.
	flushPast = 20 * time.Second
	spanPast  = 50 * time.Second
	arrives   = time.Second
	// idleLife is how long the connector keeps a series that is not called.
	idleLife = 15 * time.Minute
	// flushes is how many times each connector sends in the window.
	flushes = int(Span / FlushEvery)
	// spansPerMinute is the value of a span count point: any count above zero says
	// the instance ran.
	spansPerMinute = 40
)

// scopeSpanCount is the scope the count connector's points are in; the route reads
// the metric and not the scope.
const scopeSpanCount = "countconnector"

// flushTime and spanTime are the instants of the kth points.
func flushTime(k int) time.Time { return anchor.Add(flushPast + time.Duration(k)*FlushEvery) }
func spanTime(k int) time.Time  { return anchor.Add(spanPast + time.Duration(k)*FlushEvery) }

// flushAfter is the first flush at or after t.
func flushAfter(t time.Time) time.Time {
	d := t.Sub(anchor.Add(flushPast))
	k := 0
	if d > 0 {
		k = int((d + FlushEvery - 1) / FlushEvery)
	}
	return flushTime(k)
}

func collectorResource(attrs ...capture.KeyValue) []capture.KeyValue {
	return append([]capture.KeyValue{clusterAttr()}, attrs...)
}

// newest sets the time of the newest point of the resource, as the converter does.
func newest(r otlpmetrics.Resource) otlpmetrics.Resource {
	for _, p := range r.Points {
		r.NewestTimeUnixNano = max(r.NewestTimeUnixNano, p.TimeUnixNano)
	}
	return r
}

func nano(t time.Time) uint64 { return uint64(t.UnixNano()) }

// order is the key the series of a flush are sorted by: the client's namespace and
// name, then the server's.
func (c Call) order() string {
	return strings.Join([]string{c.ClientNamespace, c.Client, c.ServerNamespace, c.Server}, "/")
}

// serviceGraph is the kth flush of the service graph connector: one point for each
// call that was made in the last fifteen minutes, in the order of the series.
func (w *World) serviceGraph(k int) otlpmetrics.Data {
	at := flushTime(k)
	start := nano(anchor.Add(-collectorLead))
	calls := slices.Clone(w.Calls)
	slices.SortStableFunc(calls, func(a, b Call) int {
		return strings.Compare(a.order(), b.order())
	})
	res := otlpmetrics.Resource{Attributes: collectorResource()}
	for _, c := range calls {
		n, last := c.firedBy(at)
		if n == 0 || at.Sub(last) > idleLife {
			continue
		}
		connection := ""
		if c.Database {
			connection = "database"
		}
		res.Points = append(res.Points, otlpmetrics.SumPoint{
			Scope: svcgraph.Scope, Metric: svcgraph.Metric,
			Temporality: otlpmetrics.Cumulative, Monotonic: true,
			StartTimeUnixNano: start, TimeUnixNano: nano(at), Value: float64(n),
			Attributes: []capture.KeyValue{
				{Key: "client", Value: sv(c.Client)},
				{Key: "server", Value: sv(c.Server)},
				{Key: "connection_type", Value: sv(connection)},
				{Key: "failed", Value: sv("false")},
				{Key: "client_service.namespace", Value: sv(c.ClientNamespace)},
				{Key: "server_service.namespace", Value: sv(c.ServerNamespace)},
				{Key: "virtual_node", Value: sv("")},
			},
		})
	}
	return otlpmetrics.Data{Resources: []otlpmetrics.Resource{newest(res)}}
}

// sendsSpans says whether the pod's instance sends a span count at t: its pod
// runs, and its node is ready, because a node whose runtime is down runs nothing.
func (w *World) sendsSpans(p *Pod, t time.Time) bool {
	if _, ok := w.Instance(p); !ok || p.phaseAt(t) != phaseRunning {
		return false
	}
	ready, _ := w.readyAt(p.Node, t)
	return ready
}

// spanCounts is the kth span count request of the count connector: a resource for
// each instance that sends one, in the order of the pods, each with one delta
// point of the minute that ends at the instant.
func (w *World) spanCounts(k int) otlpmetrics.Data {
	at := spanTime(k)
	var out otlpmetrics.Data
	for _, p := range w.Pods {
		if !w.sendsSpans(p, at) {
			continue
		}
		in, _ := w.Instance(p)
		out.Resources = append(out.Resources, newest(otlpmetrics.Resource{
			Attributes: collectorResource(
				capture.KeyValue{Key: "service.name", Value: sv(in.Service)},
				capture.KeyValue{Key: "service.namespace", Value: sv(in.Namespace)},
				capture.KeyValue{Key: "service.instance.id", Value: sv(in.ID)},
				capture.KeyValue{Key: "service.version", Value: sv(in.Version)},
				capture.KeyValue{Key: "k8s.pod.uid", Value: sv(in.PodUID)},
			),
			Points: []otlpmetrics.SumPoint{{
				Scope: scopeSpanCount, Metric: spancount.Metric,
				Temporality: otlpmetrics.Delta, Monotonic: true,
				StartTimeUnixNano: nano(at.Add(-FlushEvery)), TimeUnixNano: nano(at), Value: spansPerMinute,
			}},
		}))
	}
	return out
}

// isDatabasePoint says whether a service graph point counts calls to a database.
func isDatabasePoint(p otlpmetrics.SumPoint) bool {
	for _, kv := range p.Attributes {
		if kv.Key == "connection_type" && kv.Value.StringValue != nil {
			return *kv.Value.StringValue == "database"
		}
	}
	return false
}
