package svcgraph_test

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

const (
	testProducer = lifecycle.Producer("svcgraph/test")
	// expiry is the TTL of every Observe in these tests: E, six hours.
	expiry = 6 * time.Hour
	flush  = 60 * time.Second
	// idle is the connector's idle life of a series.
	idle = 15 * time.Minute
)

// t0 is the instant the scripts begin.
var t0 = time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

// connStart is the start time of the first connector in these tests, an hour before
// the scripts begin.
var connStart = t0.Add(-time.Hour)

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

// tb is what the helpers need of a test, so that a rapid test can use them too.
type tb interface {
	Helper()
	Fatal(args ...any)
}

func newTranslator(t tb, opts ...func(*svcgraph.Config)) *svcgraph.Translator {
	t.Helper()
	c := svcgraph.Config{Producer: testProducer, Resolver: resolver(), Expiry: expiry}
	for _, o := range opts {
		o(&c)
	}
	tr, err := svcgraph.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func maxSeries(n int) func(*svcgraph.Config) { return func(c *svcgraph.Config) { c.MaxSeries = n } }

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func kv(k, v string) capture.KeyValue { return capture.KeyValue{Key: k, Value: sv(v)} }

// edgeID names one series of the connector: an edge, and whether the calls failed.
type edgeID struct {
	client, server     string
	clientNS, serverNS string
	failed             bool
}

func (e edgeID) String() string {
	return fmt.Sprintf("%s/%s>%s/%s/%v", e.clientNS, e.client, e.serverNS, e.server, e.failed)
}

// attrs are the attributes the connector gives the series, for a call between two
// services.
func (e edgeID) attrs() []capture.KeyValue {
	out := []capture.KeyValue{kv("client", e.client), kv("server", e.server), kv("connection_type", ""), kv("failed", fmt.Sprint(e.failed))}
	if e.clientNS != "" {
		out = append(out, kv("client_service.namespace", e.clientNS))
	}
	if e.serverNS != "" {
		out = append(out, kv("server_service.namespace", e.serverNS))
	}
	return out
}

func edge(client, server string) edgeID { return edgeID{client: client, server: server} }

// counter is a cumulative point of the request counter.
func counter(e edgeID, start, at time.Time, v float64) otlpmetrics.SumPoint {
	return otlpmetrics.SumPoint{
		Scope: svcgraph.Scope, Metric: svcgraph.Metric,
		Temporality: otlpmetrics.Cumulative, Monotonic: true,
		StartTimeUnixNano: uint64(start.UnixNano()), TimeUnixNano: uint64(at.UnixNano()),
		Value: v, Attributes: e.attrs(),
	}
}

// withAttrs replaces the attributes of a point.
func withAttrs(p otlpmetrics.SumPoint, kvs ...capture.KeyValue) otlpmetrics.SumPoint {
	p.Attributes = kvs
	return p
}

// resource is a resource with the cluster stamped on it, as the collector does.
func resource(points ...otlpmetrics.SumPoint) otlpmetrics.Resource {
	return otlpmetrics.Resource{
		Attributes: []capture.KeyValue{kv("k8s.cluster.name", "test")},
		Points:     points,
	}
}

// send translates a request, and commits it.
func send(tr *svcgraph.Translator, received time.Time, points ...otlpmetrics.SumPoint) svcgraph.Result {
	r := tr.Translate(resource(points...), received)
	tr.Commit()
	return r
}

// rcv is the time a request made at the time at is received: a second later.
func rcv(at time.Time) time.Time { return at.Add(time.Second) }

func skipped(r svcgraph.Result, reason string) int {
	for _, s := range r.Skipped {
		if s.Reason == reason {
			return s.Count
		}
	}
	return 0
}

// ---- the connector ----

// conn simulates the service graph connector: cumulative counters that start at the
// connector's start, and a cleanup that removes a series that has had no update for
// fifteen minutes. A series that is removed and called again is made again with the
// same start time.
type conn struct {
	start  time.Time
	series map[edgeID]*connSeries
}

type connSeries struct {
	value   float64
	updated time.Time
}

func newConn(start time.Time) *conn { return &conn{start: start, series: map[edgeID]*connSeries{}} }

// call counts n calls of an edge at the time at.
func (c *conn) call(e edgeID, n int, at time.Time) {
	s := c.series[e]
	if s == nil {
		s = &connSeries{}
		c.series[e] = s
	}
	s.value += float64(n)
	s.updated = at
}

// restart is a new connector: no series, and a later start time.
func (c *conn) restart(start time.Time) {
	c.start = start
	c.series = map[edgeID]*connSeries{}
}

// flush cleans the cache and returns every series' point at the time at, in order of
// the series, each point stamped a millisecond after the one before it as the real
// connector stamps each with the time it is made. offset moves the whole flush.
func (c *conn) flush(at time.Time, offset time.Duration) []otlpmetrics.SumPoint {
	for e, s := range c.series {
		if at.Sub(s.updated) > idle {
			delete(c.series, e)
		}
	}
	ids := make([]edgeID, 0, len(c.series))
	for e := range c.series {
		ids = append(ids, e)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	pts := make([]otlpmetrics.SumPoint, len(ids))
	for i, e := range ids {
		pts[i] = counter(e, c.start, at.Add(offset+time.Duration(i)*time.Millisecond), c.series[e].value)
	}
	return pts
}

// ---- reading results ----

// callsOf are the (client, server, instant) of every depends_on record, in order.
type call struct {
	from, to identity.Fingerprint
	at       time.Time
}

func calls(recs []store.Record) []call {
	var out []call
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.DependsOn {
			out = append(out, call{r.Subject.A, r.Subject.B, r.EventTime})
		}
	}
	return out
}

func serviceFP(t tb, ns, name string) identity.Fingerprint {
	t.Helper()
	attrs := []identity.Attr{{Key: catalog.ServiceName, Value: name}}
	if ns != "" {
		attrs = append([]identity.Attr{{Key: catalog.ServiceNamespace, Value: ns}}, attrs...)
	}
	id, err := resolver().Resolve(catalog.Service, attrs)
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// entities are the fingerprints of the entity records, in order.
func entities(recs []store.Record) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEntity {
			out = append(out, r.Subject.A)
		}
	}
	return out
}

func describe(recs []store.Record) string {
	s := ""
	for _, r := range recs {
		s += fmt.Sprintf("\n  %v %s %s ttl=%s", r.Kind, r.Layer, r.EventTime.Format("15:04:05.000"), r.TTL)
		if r.Subject.Kind == store.SubjectEdge {
			s += fmt.Sprintf(" edge %s", r.Subject.Relation)
		} else {
			s += fmt.Sprintf(" entity %s", r.Subject.A.Type())
		}
	}
	return s
}

// existence folds the records of one subject, giving each a sequence number in
// order, and returns the intervals the subject exists in.
func existence(t tb, subject store.Subject, recs []store.Record) []lifecycle.Interval {
	t.Helper()
	var as []lifecycle.Assertion
	for _, r := range recs {
		if r.Subject == subject {
			r.Seq = uint64(len(as) + 1)
			as = append(as, r.Assertion())
		}
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	return slices.Clone(tl.Existence())
}
