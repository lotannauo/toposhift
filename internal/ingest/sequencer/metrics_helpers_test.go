package sequencer_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

func kv(k, v string) capture.KeyValue { return capture.KeyValue{Key: k, Value: sv(v)} }

// connStart is the start time of the connector in these tests, an hour before t0.
var connStart = t0.Add(-time.Hour)

func nano(t time.Time) uint64 { return uint64(t.UnixNano()) }

// edgeAttrs are the labels the service graph connector gives a call between two
// services, as the first spike found them: the two namespaces, the failed label,
// the empty connection type, and the empty virtual node label.
func edgeAttrs(client, clientNS, server, serverNS string) []capture.KeyValue {
	return []capture.KeyValue{
		kv("client", client), kv("server", server), kv("connection_type", ""), kv("failed", "false"),
		kv("client_service.namespace", clientNS), kv("server_service.namespace", serverNS), kv("virtual_node", ""),
	}
}

// edge is a series of the connector's counter.
type edge struct{ client, clientNS, server, serverNS string }

var (
	ab = edge{"a", "shop", "b", "shop"}
	cd = edge{"c", "shop", "d", "shop"}
	ef = edge{"e", "shop", "f", "shop"}
)

func (e edge) attrs() []capture.KeyValue {
	return edgeAttrs(e.client, e.clientNS, e.server, e.serverNS)
}

// cum is a cumulative point of the counter of an edge, whose connector started at
// connStart.
func (e edge) cum(tm time.Time, v float64) otlpmetrics.SumPoint {
	return otlpmetrics.SumPoint{
		Scope: svcgraph.Scope, Metric: svcgraph.Metric, Temporality: otlpmetrics.Cumulative, Monotonic: true,
		StartTimeUnixNano: nano(connStart), TimeUnixNano: nano(tm), Value: v, Attributes: e.attrs(),
	}
}

// delta is a delta point, which is a sighting when it is above zero.
func (e edge) delta(tm time.Time, v float64) otlpmetrics.SumPoint {
	p := e.cum(tm, v)
	p.Temporality = otlpmetrics.Delta
	return p
}

// spanPoint is a point of the count connector's span counter.
func spanPoint(tm time.Time, v float64) otlpmetrics.SumPoint {
	return otlpmetrics.SumPoint{
		Scope: "countconnector", Metric: spancount.Metric, Temporality: otlpmetrics.Delta, Monotonic: true,
		StartTimeUnixNano: nano(tm.Add(-time.Minute)), TimeUnixNano: nano(tm), Value: v,
	}
}

// clusterAttrs are the resource attributes of a cluster name, none for none.
func clusterAttrs(cluster string, more ...capture.KeyValue) []capture.KeyValue {
	var out []capture.KeyValue
	if cluster != "" {
		out = append(out, kv("k8s.cluster.name", cluster))
	}
	return append(out, more...)
}

// mres is a resource of a metrics request.
func mres(attrs []capture.KeyValue, pts ...otlpmetrics.SumPoint) otlpmetrics.Resource {
	r := otlpmetrics.Resource{Attributes: attrs, Points: pts}
	for _, p := range pts {
		r.NewestTimeUnixNano = max(r.NewestTimeUnixNano, p.TimeUnixNano)
	}
	return r
}

// mreq is a metrics request of one resource of a cluster name.
func mreq(cluster string, pts ...otlpmetrics.SumPoint) otlpmetrics.Data {
	return otlpmetrics.Data{Resources: []otlpmetrics.Resource{mres(clusterAttrs(cluster), pts...)}}
}

// spanRes is the resource of an application instance with span counts.
func spanRes(cluster, ns, service, instance, podUID string, pts ...otlpmetrics.SumPoint) otlpmetrics.Resource {
	attrs := clusterAttrs(cluster, kv("service.name", service))
	if ns != "" {
		attrs = append(attrs, kv("service.namespace", ns))
	}
	if instance != "" {
		attrs = append(attrs, kv("service.instance.id", instance))
	}
	if podUID != "" {
		attrs = append(attrs, kv("k8s.pod.uid", podUID))
	}
	return mres(attrs, pts...)
}

// sendM sends a metrics request and waits for its answer.
func (h *harness) sendM(d otlpmetrics.Data) (otlphttp.Partial, error) {
	h.t.Helper()
	return h.seq.AcceptMetrics(context.Background(), d)
}

// sendMOK sends a metrics request at the clock's instant and fails the test
// unless it is answered 200 with nothing rejected.
func (h *harness) sendMOK(d otlpmetrics.Data) {
	h.t.Helper()
	p, err := h.sendM(d)
	if err != nil || p.Rejected != 0 {
		h.t.Fatalf("at %s: partial %+v, error %v", h.clock.now().Sub(t0), p, err)
	}
}

// sendMAsync sends a metrics request on its own goroutine.
func (h *harness) sendMAsync(d otlpmetrics.Data) <-chan answer {
	ch := make(chan answer, 1)
	go func() {
		p, err := h.seq.AcceptMetrics(context.Background(), d)
		ch <- answer{p, err}
	}()
	return ch
}

// at sets the ingest clock to t0 plus d.
func (h *harness) setNow(d time.Duration) { h.clock.set(at(d)) }

// ---- the records ----

func isDependsOn(r store.Record) bool {
	return r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.DependsOn
}

// fromProducer filters the records of the producers with the prefix.
func fromProducer(recs []store.Record, prefix string) []store.Record {
	var out []store.Record
	for _, r := range recs {
		if strings.HasPrefix(string(r.Producer), prefix) {
			out = append(out, r)
		}
	}
	return out
}

func dependsOn(recs []store.Record) []store.Record {
	var out []store.Record
	for _, r := range recs {
		if isDependsOn(r) {
			out = append(out, r)
		}
	}
	return out
}

func svcFP(t testing.TB, ns, name string) identity.Fingerprint {
	t.Helper()
	fp, err := otlpmetrics.NewFingerprints(identity.NewResolver(catalog.Default())).Service(ns, name)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

func instFP(t testing.TB, ns, name, id string) identity.Fingerprint {
	t.Helper()
	fp, err := otlpmetrics.NewFingerprints(identity.NewResolver(catalog.Default())).Instance(ns, name, id)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// edgeRecords are the depends_on records of an edge series, from client to server.
func edgeRecords(t testing.TB, recs []store.Record, e edge) []store.Record {
	t.Helper()
	want := store.EdgeSubject(svcFP(t, e.clientNS, e.client), svcFP(t, e.serverNS, e.server), catalog.DependsOn)
	var out []store.Record
	for _, r := range recs {
		if r.Subject == want {
			out = append(out, r)
		}
	}
	return out
}

// ---- the log ----

// syncBuffer is a log destination that tests can read while the loop writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// count is how many lines hold s.
func (b *syncBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}

// withLog sends the sequencer's log to a buffer that the test reads.
func withLog(b *syncBuffer) option {
	return withConfig(func(c *sequencer.Config) {
		c.Logger = slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
}

// counter is a counter of the status by name, zero for an absent one.
func counter(h *harness, name string) int64 { return h.seq.MetricsStatus().Counters[name] }
