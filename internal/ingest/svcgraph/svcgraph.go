// Package svcgraph translates the request counter of the OpenTelemetry service
// graph connector into services and the calls between them.
//
// # Input
//
// The connector pairs the client span and the server span of a call by trace
// context, never by address, and counts the calls in a cumulative sum:
// traces_service_graph_request_total, in the scope traces_service_graph, with the
// attributes
//
//   - client and server, the service.name of each side;
//   - connection_type, which is always there: empty for a call between two
//     services, and messaging_system, database or virtual_node otherwise;
//   - virtual_node, which is there on every point when the connector is configured
//     with virtual_node_extra_label: empty for a call between two services, and
//     client or server for an edge the connector turned into a virtual node when it
//     expired, which also sets connection_type to virtual_node;
//   - failed, whether the call failed, which makes up to two series of one edge;
//   - client_service.namespace and server_service.namespace, which are there only
//     when the collector's connector is configured with the dimension and the span
//     has the attribute.
//
// The connector's resource has no attributes of its own. The collector stamps the
// cluster on it, and the series is keyed by whatever the resource carries.
//
// # A point is not a sighting
//
// The connector re-exports every series at each flush until the series has been
// idle for fifteen minutes. A point's presence is therefore no evidence of a call:
// treating it as one would add up to sixteen minutes of false liveness to every
// edge. What this package writes is a sighting, one Observe of the services and the
// edge, only for a point that is evidence of a new call.
//
// The cumulative counters are diffed here, not by a processor in front, for two
// reasons: a series the connector removed and created again keeps its start time,
// so a processor that drops a value below the previous one as a reset would lose
// it; and a processor that drops a first point whose start time is before its own
// start would lose every first point.
//
// # The rule
//
// R is the longest a series the connector keeps can go without an update: fifteen
// minutes, plus the connector's cache loop, plus one flush. A series is keyed by
// the resource's attributes, the scope, the metric and the point's attributes
// (failed among them); the cluster is the translator's. Its state is the start time
// and the value of the latest point used, that point's time, and the time of the
// last increase.
//
// A delta point is a sighting if its value is above zero. A cumulative point of a
// series that has state takes the first case that applies:
//
//  1. Its time is not after the state's last point: a retry or a duplicate. Not a
//     sighting.
//  2. Its start time is before the state's. Within one connector start times only
//     go forward, so this is a second connector behind the same producer. Counted,
//     not a sighting.
//  3. The start time is the same and the value is larger: a sighting.
//  4. The start time is later, or the value fell: the connector made the series
//     again, which it does only for a new request. A sighting if the value is above
//     zero.
//  5. Its time is more than R after the last increase: a sighting if the value is
//     above zero. The series is still there after its idle life ran out, so it was
//     updated or made again since. This catches a series made again with the same
//     start time and an equal value: called once, idle for twenty minutes, called
//     once again.
//  6. Otherwise: not a sighting.
//
// What each case writes to the state. Cases 1 and 2 write nothing. Every other case
// sets the start time, the value and the last point time, and a sighting also sets
// the last increase to the point's time. A state made without a sighting (in
// warm-up, or after an overflow) sets the last increase to the point's time too, or
// case 5 would fire on its next point.
//
// A cumulative point of a series with no state is a sighting if its value is above
// zero and the translator is past its warm-up. The warm-up begins at the first point
// used, and again at every point set aside for want of room in the table, and ends
// two flushes after that, or two times the largest gap seen between two
// consecutive points of one series if that is longer (a gap of more than R is not
// counted: the connector removed the series and made it again): a translator cannot count
// flushes from the points' times, because the connector stamps each point with the
// time it was made. The connector re-exports every live series at every flush, so
// once the warm-up has ended every series that existed already has state.
//
// A state is evicted when the newest point time is later than its last point by
// more than R and two flushes: a lookup ignores such a state, and a commit removes
// it, at most once for each flush of newest point time, so until then it holds its
// room. The table holds at most [Config.MaxSeries] series. A series that has no state and finds no room is set aside, counted with
// [SkipSeriesOverflow], and the warm-up begins again, so a series that later gains
// room is never taken for a call on its unchanged count.
//
// # Not written
//
// Only edges with connection_type empty are written. A call through a broker, to a
// database or to a virtual node ([SkipConnectionMessaging], [SkipConnectionDatabase],
// [SkipConnectionVirtualNode]), a point with no connection_type, and a point whose
// virtual_node label is non-empty or unreadable ([SkipVirtualNodeLabel]) are counted
// and take no part in the series' state. The connection_type is checked first. A
// virtual_node label that is absent or empty is no reason to set a point aside: the
// connector puts it on every point when it is configured to, empty for a call between
// two services. A self-call is written like any other.
//
// A gauge, a sum that is not monotonic and a sum with no temporality are counted. So
// is a point whose time is zero or lies more than [otlpmetrics.MaxClockAhead] after
// the request was received: it is skipped, never rejected, and never changes any
// state. The translator does not substitute the time it received the request for such
// a point's time, which would end runs and move a series' state.
//
// # What is asserted
//
// The producer is "svcgraph/<cluster>", given in [Config.Producer], and it is never
// renamed. A sighting is written as Observes with the expiry as TTL, at the point's
// time, with basis observed (the collector's clock):
//
//   - the service of each side, in layer L3, whose payload is its description,
//     {"name":...,"namespace":...} with the namespace left out when it has none (see
//     [otlpmetrics.ServiceDescription]);
//   - depends_on from the client service to the server service, in the layer the
//     catalog records for it (L3), with no payload.
//
// The services are asserted because an edge is reported only while both its
// endpoints exist. This package never writes a Delete, never a Through, and never a
// count, a rate or a latency: the evidence is the sighting, and the edge ends at the
// last sighting plus the expiry.
//
// # Identity
//
// A service is (the namespace or none, the name). The name is the client or server
// label, the namespace the client_service.namespace or server_service.namespace
// label; an empty namespace is none. A value is used as received if it is a string,
// given once or repeated with the same value, and not blank. A point whose client or
// server, or whose namespace, is not usable writes nothing and is counted. The
// namespace of one side is never used for the other. Nothing else identifies anything.
// The description of a service is made of the same two values and adds nothing to
// its identity.
//
// # State across a failed group
//
// The translator keeps a committed state and a pending state. [Translator.Translate]
// changes the pending state, which later calls see. The caller promotes it with
// [Translator.Commit] when the group that held the request has been answered with
// success, whether its write returned or returned an error whose batch was stored,
// and drops it with [Translator.Rollback] when the group failed, so that the
// collector's retry of the request is diffed against the same state and is sighted.
// A caller that makes the translator anew, as it does after a panic, has no state
// and accepts a new warm-up.
//
// # Times
//
// This package never reads a clock. Every time it returns is a point's time, and
// the one comparison with the present uses the time the caller received the request.
//
// A Translator is not safe for concurrent use.
package svcgraph

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Source is the source name of the producer: the producer of a cluster is
// "svcgraph/<cluster>". It is never renamed.
const Source = "svcgraph"

// The connector's scope and counter.
const (
	Scope  = "traces_service_graph"
	Metric = "traces_service_graph_request_total"
)

// The connector's settings that bound a series' life, and what this package assumes
// of them when a [Config] does not say.
const (
	// DefaultFlush is the connector's metrics_flush_interval.
	DefaultFlush = 60 * time.Second
	// DefaultCacheLoop is the connector's cache_loop.
	DefaultCacheLoop = time.Minute
	// DefaultMaxSeries is the number of series the translator keeps: about twice the
	// edges of the supported scale, because of the failed attribute.
	DefaultMaxSeries = 200_000
	// IdleLife is how long the connector keeps a series that has no update before it
	// removes it, at its next cache loop.
	IdleLife = 15 * time.Minute
)

// maxSetting bounds the expiry and the connector's settings, which keep a TTL and
// the times derived from them far from the end of the representable times.
const maxSetting = 366 * 24 * time.Hour

// The reasons a translator gives for a point or part of one it does not turn into a
// record. All of them write nothing for the point, and none is an error: a caller
// that exposes them as counters names them for that, not as dropped data.
const (
	// SkipNotCounter is a point of another scope or metric.
	SkipNotCounter = "not the service graph request counter"
	// SkipNoTemporality is a sum with no temporality.
	SkipNoTemporality = "sum has no temporality"
	// SkipNoConnectionType is a point with no usable connection_type attribute: the
	// connector always sets it, so the point is malformed.
	SkipNoConnectionType = "no connection type"
	// SkipConnectionDatabase is a call to a database, which is an endpoint and not a
	// service.
	SkipConnectionDatabase = "connection type database"
	// SkipConnectionMessaging is a call through a messaging system, whose direction
	// is not decided.
	SkipConnectionMessaging = "connection type messaging_system"
	// SkipConnectionVirtualNode is a call to a virtual node.
	SkipConnectionVirtualNode = "connection type virtual_node"
	// SkipConnectionUnknown is a connection_type this package does not know.
	SkipConnectionUnknown = "connection type unknown"
	// SkipVirtualNodeLabel is a point whose virtual_node label names a virtual node (a
	// non-empty value) or cannot be read (it is not a string, or is given twice with
	// different values).
	SkipVirtualNodeLabel = "point has a virtual node label"
	// SkipNoEndpoint is a point whose client or server is absent, not a string,
	// given more than once with different values, or not usable as a service name.
	SkipNoEndpoint = "client or server unusable"
	// SkipBadNamespace is a point whose client or server namespace is not a string,
	// is given more than once with different values, or is blank.
	SkipBadNamespace = "client or server namespace unusable"
	// SkipBadIdentity is a point whose services the identity encoding refused.
	SkipBadIdentity = "services refused by the identity encoding"
	// SkipSeriesOverflow is a series with no state for which the table has no room.
	SkipSeriesOverflow = "series table full"
	// SkipStale is a point not later than the last point of its series: a retry or a
	// duplicate (case 1).
	SkipStale = "point not later than the series' last"
	// SkipSecondConnector is a point whose start time is before the series': a
	// second connector behind the producer (case 2).
	SkipSecondConnector = "start time before the series': second connector"
	// SkipWarmUp is a point of a series with no state, with a value above zero,
	// before the warm-up has ended.
	SkipWarmUp = "series new during warm-up"
)

// Config configures a [Translator].
type Config struct {
	// Producer names the source of the assertions: "svcgraph/<cluster>". The caller
	// builds it; this package does not check its format. Required.
	Producer lifecycle.Producer
	// Resolver turns the identifying attributes into fingerprints. Required;
	// usually identity.NewResolver(catalog.Default()).
	Resolver *identity.Resolver
	// Expiry is the TTL of every Observe, and so how long an edge outlives its last
	// sighting. Required: it must be above zero and at most a year.
	Expiry time.Duration
	// Flush is the connector's metrics_flush_interval, and CacheLoop its cache_loop.
	// The zero value takes [DefaultFlush] and [DefaultCacheLoop]. Each must be at
	// most a year. Changing either of the connector's settings needs the same change
	// here, because R and the warm-up depend on them.
	Flush, CacheLoop time.Duration
	// MaxSeries bounds the series kept. The zero value takes [DefaultMaxSeries].
	MaxSeries int
}

// Result is what one resource stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced: for each
	// sighting, the client's service, the server's, and the edge. They have no Seq,
	// no Through and no Delete.
	Records []store.Record
	// Skipped says what was set aside or not used, and why, sorted by reason.
	Skipped []otlpmetrics.Count
}

// Translator turns the points of the service graph counter into store records.
type Translator struct {
	producer lifecycle.Producer
	fp       otlpmetrics.Fingerprints
	expiry   time.Duration
	l        *ledger
}

// New returns a translator with no state.
func New(c Config) (*Translator, error) {
	if c.Flush == 0 {
		c.Flush = DefaultFlush
	}
	if c.CacheLoop == 0 {
		c.CacheLoop = DefaultCacheLoop
	}
	if c.MaxSeries == 0 {
		c.MaxSeries = DefaultMaxSeries
	}
	switch {
	case c.Producer == "":
		return nil, errors.New("svcgraph: producer is required")
	case c.Resolver == nil:
		return nil, errors.New("svcgraph: resolver is required")
	case c.Expiry <= 0:
		return nil, errors.New("svcgraph: expiry is required and must be above zero")
	case c.Expiry > maxSetting:
		return nil, fmt.Errorf("svcgraph: expiry %s is over %s", c.Expiry, maxSetting)
	case c.Flush < 0 || c.Flush > maxSetting:
		return nil, fmt.Errorf("svcgraph: flush %s is not between zero and %s", c.Flush, maxSetting)
	case c.CacheLoop < 0 || c.CacheLoop > maxSetting:
		return nil, fmt.Errorf("svcgraph: cache loop %s is not between zero and %s", c.CacheLoop, maxSetting)
	case c.MaxSeries < 0:
		return nil, errors.New("svcgraph: max series must not be negative")
	}
	return &Translator{
		producer: c.Producer,
		fp:       otlpmetrics.NewFingerprints(c.Resolver),
		expiry:   c.Expiry,
		l:        newLedger(c.MaxSeries, c.Flush, IdleLife+c.CacheLoop+c.Flush),
	}, nil
}

// TTL is the TTL of every Observe this translator writes.
func (t *Translator) TTL() time.Duration { return t.expiry }

// Series is the number of series the translator holds, the pending ones included.
func (t *Translator) Series() int { return t.l.size() }

// Commit promotes the pending state to committed. See "State across a failed
// group" in the package documentation.
func (t *Translator) Commit() { t.l.commit() }

// Rollback drops the pending state: the translator is as it was after the last
// Commit.
func (t *Translator) Rollback() { t.l.rollback() }

// edge is the endpoints of a point's call.
type edge struct {
	client, server     string
	clientNS, serverNS string
}

// Translate turns the points of one resource into records, changing the pending
// state. received is the time the caller received the request; it may be zero, and
// then no point is judged to be ahead of it.
func (t *Translator) Translate(res otlpmetrics.Resource, received time.Time) Result {
	var tally otlpmetrics.Tally
	otlpmetrics.Dropped(&tally, res)
	var recs []store.Record
	var resKey []byte
	for _, p := range res.Points {
		if p.Scope != Scope || p.Metric != Metric {
			tally.Add(SkipNotCounter)
			continue
		}
		at, reason := otlpmetrics.Check(p, received)
		if reason != "" {
			tally.Add(reason)
			continue
		}
		if p.Temporality != otlpmetrics.Delta && p.Temporality != otlpmetrics.Cumulative {
			tally.Add(SkipNoTemporality)
			continue
		}
		attrs := otlpmetrics.Collect(p.Attributes)
		if reason := connection(attrs); reason != "" {
			tally.Add(reason)
			continue
		}
		e, reason := endpoints(attrs)
		if reason != "" {
			tally.Add(reason)
			continue
		}
		sight := false
		if p.Temporality == otlpmetrics.Delta {
			sight = p.Value > 0
		} else {
			if resKey == nil {
				resKey = encodeSet(res.Attributes)
			}
			if p.StartTimeUnixNano > math.MaxInt64 {
				tally.Add(otlpmetrics.SkipNoTime)
				continue
			}
			key := seriesKey(resKey, p.Scope, p.Metric, p.Attributes)
			switch t.l.cumulative(key, int64(p.StartTimeUnixNano), at.UnixNano(), p.Value) {
			case sighted:
				sight = true
			case warmingUp:
				tally.Add(SkipWarmUp)
			case stale:
				tally.Add(SkipStale)
			case secondConnector:
				tally.Add(SkipSecondConnector)
			case overflowed:
				tally.Add(SkipSeriesOverflow)
			}
		}
		if !sight {
			continue
		}
		out, err := t.sighting(e, at)
		if err != nil {
			tally.Add(SkipBadIdentity)
			continue
		}
		recs = append(recs, out...)
	}
	return Result{Records: recs, Skipped: tally.Counts()}
}

// connection says why a point is not an edge between two services, or "" if it is.
func connection(attrs otlpmetrics.Attributes) string {
	ct, st := attrs.Get("connection_type")
	if st != otlpmetrics.StatusOK {
		return SkipNoConnectionType
	}
	switch ct {
	case "":
	case "database":
		return SkipConnectionDatabase
	case "messaging_system":
		return SkipConnectionMessaging
	case "virtual_node":
		return SkipConnectionVirtualNode
	default:
		return SkipConnectionUnknown
	}
	// The connector puts the label on every point when it is configured to: empty for
	// a call between two services. A value names a virtual node; a label that cannot
	// be read is never taken for a call.
	switch v, st := attrs.Get("virtual_node"); {
	case st == otlpmetrics.StatusBad, st == otlpmetrics.StatusOK && v != "":
		return SkipVirtualNodeLabel
	}
	return ""
}

// endpoints reads the two services of a point.
func endpoints(attrs otlpmetrics.Attributes) (edge, string) {
	var e edge
	var st otlpmetrics.Status
	if e.client, st = attrs.Get("client"); st != otlpmetrics.StatusOK || !otlpmetrics.UsableID(e.client) {
		return edge{}, SkipNoEndpoint
	}
	if e.server, st = attrs.Get("server"); st != otlpmetrics.StatusOK || !otlpmetrics.UsableID(e.server) {
		return edge{}, SkipNoEndpoint
	}
	var ok bool
	if e.clientNS, ok = namespace(attrs, "client_"+otlpmetrics.KeyServiceNamespace); !ok {
		return edge{}, SkipBadNamespace
	}
	if e.serverNS, ok = namespace(attrs, "server_"+otlpmetrics.KeyServiceNamespace); !ok {
		return edge{}, SkipBadNamespace
	}
	return e, ""
}

// namespace reads a namespace label: absent and empty are none, and a label that is
// not a string, is given twice with different values or is blank is unusable.
func namespace(attrs otlpmetrics.Attributes, key string) (string, bool) {
	v, st := attrs.Get(key)
	switch st {
	case otlpmetrics.StatusAbsent:
		return "", true
	case otlpmetrics.StatusBad:
		return "", false
	}
	return v, otlpmetrics.UsableNamespace(v)
}

// sighting is the records of one call: the two services, then the edge.
func (t *Translator) sighting(e edge, at time.Time) ([]store.Record, error) {
	client, err := t.fp.Service(e.clientNS, e.client)
	if err != nil {
		return nil, err
	}
	server, err := t.fp.Service(e.serverNS, e.server)
	if err != nil {
		return nil, err
	}
	obs := func(s store.Subject, payload []byte) store.Record {
		return otlpmetrics.Observe(t.producer, s, at, store.BasisObserved, t.expiry, payload)
	}
	recs := []store.Record{obs(store.EntitySubject(client), otlpmetrics.ServiceDescription(e.clientNS, e.client))}
	if server != client {
		recs = append(recs, obs(store.EntitySubject(server), otlpmetrics.ServiceDescription(e.serverNS, e.server)))
	}
	return append(recs, obs(store.EdgeSubject(client, server, catalog.DependsOn), nil)), nil
}
