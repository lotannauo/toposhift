package main

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/replay/capture"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// The truth gives the latest sighting at or before an instant that the token knows
// of; a sighting that arrived late is placed by its instant and known from its own
// stamp.
func TestTruthLastBy(t *testing.T) {
	k := tkey{sub: edgeSub, prod: producer}
	tr := newTruth()
	at := func(m float64) time.Time { return epoch.Add(minutes(m)) }
	tr.add(k, at(1), 10)
	tr.add(k, at(3), 11)
	tr.add(k, at(2), 20) // arrived late: after the one at 3, stamped later
	tr.add(k, at(3), 21) // the same instant again
	for _, c := range []struct {
		at, token float64
		want      float64
		ok        bool
	}{
		{0, 99, 0, false},
		{1, 10, 1, true},
		{1, 9, 0, false},
		{2.5, 15, 1, true}, // the late one is not known yet
		{2.5, 20, 2, true}, // known
		{3, 15, 3, true},   // the one at 3 known at 11
		{3, 10, 1, true},   // before it was known
		{100, 99, 3, true}, // after all of them
		{100, 20, 3, true}, // the first of the two at 3
		{100, 11, 3, true}, // idem
		{100, 10, 1, true}, // only the first
		{100, 9, 0, false}, // nothing known
	} {
		got, ok := tr.lastBy(k, at(c.at), uint64(c.token))
		if ok != c.ok || (ok && !got.Equal(at(c.want))) {
			t.Errorf("lastBy(%v, token %v) = %s, %v; want %v, %v", c.at, c.token, got.Sub(epoch), ok, at(c.want).Sub(epoch), c.ok)
		}
	}
	if tr.n != 4 || tr.sightings(edgeSub) != 4 || len(tr.edges()) != 1 {
		t.Errorf("n %d, sightings %d, edges %d", tr.n, tr.sightings(edgeSub), len(tr.edges()))
	}
	if _, ok := tr.lastBy(tkey{sub: edgeSub, prod: "other"}, at(100), 99); ok {
		t.Errorf("another producer has the sightings")
	}
}

// The cluster name is the sequencer's: the last k8s.cluster.name that is a string,
// when it is not empty, else the default.
func TestClusterName(t *testing.T) {
	str := func(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }
	i := int64(5)
	for _, c := range []struct {
		attrs []capture.KeyValue
		want  string
	}{
		{nil, "default"},
		{[]capture.KeyValue{{Key: "k8s.cluster.name", Value: str("a")}}, "a"},
		{[]capture.KeyValue{{Key: "k8s.cluster.name", Value: str("a")}, {Key: "k8s.cluster.name", Value: str("b")}}, "b"},
		{[]capture.KeyValue{{Key: "k8s.cluster.name", Value: str("a")}, {Key: "k8s.cluster.name", Value: str("")}}, "default"},
		{[]capture.KeyValue{{Key: "k8s.cluster.name", Value: str("a")}, {Key: "k8s.cluster.name", Value: capture.AnyValue{IntValue: (*capture.Int64)(&i)}}}, "a"},
		{[]capture.KeyValue{{Key: "service.name", Value: str("x")}}, "default"},
	} {
		if got := clusterName(otlpmetrics.Resource{Attributes: c.attrs}); got != c.want {
			t.Errorf("clusterName(%v) = %q, want %q", c.attrs, got, c.want)
		}
	}
}

// A line's time is the newest time of any data point of any kind of metric.
func TestNewestTime(t *testing.T) {
	m := &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
		{Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: 5e9}, {TimeUnixNano: 9e9}}}}},
		{Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: 12e9}}}}},
		{Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{DataPoints: []*metricspb.HistogramDataPoint{{TimeUnixNano: 7e9}}}}},
	}}}}}}
	if got := newestTime(m); !got.Equal(time.Unix(12, 0)) {
		t.Errorf("newestTime = %s, want 12s", got)
	}
	if got := newestTime(&metricspb.MetricsData{}); !got.IsZero() {
		t.Errorf("newestTime of nothing = %s", got)
	}
	m.ResourceMetrics[0].ScopeMetrics[0].Metrics = append(m.ResourceMetrics[0].ScopeMetrics[0].Metrics,
		&metricspb.Metric{Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: []*metricspb.SummaryDataPoint{{TimeUnixNano: 30e9}}}}},
		&metricspb.Metric{Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{DataPoints: []*metricspb.ExponentialHistogramDataPoint{{TimeUnixNano: 20e9}}}}})
	if got := newestTime(m); !got.Equal(time.Unix(30, 0)) {
		t.Errorf("newestTime = %s, want 30s", got)
	}
}
