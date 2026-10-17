package sequencer

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// The counters of the metrics path, as GET /status names them under "metrics".
// metrics.* count the data points of the requests answered without error;
// svcgraph.* and spancount.* what the two translators did with the points that
// were given to them. A name says what was done, not that data was lost:
// skipped_* is a point set aside, refused_* a part of a record that was not
// written.
const (
	counterMetricsPoints         = "metrics.points"
	counterSkippedOtherMetric    = "metrics.skipped_other_metric"
	counterDroppedNotSum         = "metrics.dropped_not_sum"
	counterDroppedNonMonotonic   = "metrics.dropped_non_monotonic"
	counterSvcgraphPoints        = "svcgraph.points"
	counterSvcgraphSightings     = "svcgraph.sightings"
	counterSpancountPoints       = "spancount.points"
	counterSpancountSightings    = "spancount.sightings"
	counterSeriesOverflow        = "svcgraph.skipped_series_overflow"
	counterNoInstanceID          = "spancount.skipped_no_instance_id"
	counterSvcgraphSkippedOther  = "svcgraph.skipped_other"
	counterSpancountSkippedOther = "spancount.skipped_other"
)

// svcgraphCounters and spancountCounters name each reason a translator gives as a
// counter. The reasons the two share with package otlpmetrics are named in both.
var (
	svcgraphCounters = map[string]string{
		svcgraph.SkipNotCounter:            "svcgraph.skipped_not_counter",
		svcgraph.SkipNoTemporality:         "svcgraph.skipped_no_temporality",
		svcgraph.SkipNoConnectionType:      "svcgraph.skipped_no_connection_type",
		svcgraph.SkipConnectionDatabase:    "svcgraph.skipped_connection_database",
		svcgraph.SkipConnectionMessaging:   "svcgraph.skipped_connection_messaging",
		svcgraph.SkipConnectionVirtualNode: "svcgraph.skipped_connection_virtual_node",
		svcgraph.SkipConnectionUnknown:     "svcgraph.skipped_connection_unknown",
		svcgraph.SkipVirtualNodeLabel:      "svcgraph.skipped_virtual_node_label",
		svcgraph.SkipNoEndpoint:            "svcgraph.skipped_no_endpoint",
		svcgraph.SkipBadNamespace:          "svcgraph.skipped_bad_namespace",
		svcgraph.SkipBadIdentity:           "svcgraph.skipped_bad_identity",
		svcgraph.SkipSeriesOverflow:        counterSeriesOverflow,
		svcgraph.SkipStale:                 "svcgraph.skipped_stale",
		svcgraph.SkipSecondConnector:       "svcgraph.skipped_second_connector",
		svcgraph.SkipWarmUp:                "svcgraph.skipped_warm_up",
		otlpmetrics.SkipNotSum:             "svcgraph.skipped_not_sum",
		otlpmetrics.SkipNonMonotonic:       "svcgraph.skipped_non_monotonic",
		otlpmetrics.SkipNoTime:             "svcgraph.skipped_no_time",
		otlpmetrics.SkipBadValue:           "svcgraph.skipped_bad_value",
		otlpmetrics.SkipClockAhead:         "svcgraph.skipped_clock_ahead",
	}
	spancountCounters = map[string]string{
		spancount.SkipNotCounter:     "spancount.skipped_not_counter",
		spancount.SkipNotDelta:       "spancount.skipped_not_delta",
		spancount.SkipNoSpans:        "spancount.skipped_no_spans",
		spancount.SkipNoServiceName:  "spancount.skipped_no_service_name",
		spancount.SkipNoInstanceID:   counterNoInstanceID,
		spancount.SkipBadNamespace:   "spancount.skipped_bad_namespace",
		spancount.SkipBadIdentity:    "spancount.skipped_bad_identity",
		spancount.UnplacedNoPodUID:   "spancount.refused_bridge_no_pod_uid",
		spancount.UnplacedBadPodUID:  "spancount.refused_bridge_bad_pod_uid",
		spancount.RefusedVersion:     "spancount.refused_version",
		otlpmetrics.SkipNotSum:       "spancount.skipped_not_sum",
		otlpmetrics.SkipNonMonotonic: "spancount.skipped_non_monotonic",
		otlpmetrics.SkipNoTime:       "spancount.skipped_no_time",
		otlpmetrics.SkipBadValue:     "spancount.skipped_bad_value",
		otlpmetrics.SkipClockAhead:   "spancount.skipped_clock_ahead",
	}
)

// metricsWork is what the metrics path counted for one request, by cluster name
// and counter name, and which kinds of point each cluster name had. It is kept
// only when the request is answered without error.
type metricsWork struct {
	counts    map[string]map[string]int64
	svcgraph  map[string]bool
	spancount map[string]bool
}

func (m *metricsWork) add(cluster, counter string, n int64) {
	if n <= 0 {
		return
	}
	if m.counts == nil {
		m.counts = make(map[string]map[string]int64)
	}
	c := m.counts[cluster]
	if c == nil {
		c = make(map[string]int64)
		m.counts[cluster] = c
	}
	c[counter] += n
}

func (m *metricsWork) saw(set *map[string]bool, cluster string) {
	if *set == nil {
		*set = make(map[string]bool)
	}
	(*set)[cluster] = true
}

// route says which translator takes a point.
type route uint8

const (
	routeOther route = iota
	routeServiceGraph
	routeSpanCount
)

// routeOf routes a point by its scope and metric: the service graph connector's
// request counter, and the count connector's span counter. Everything else is
// counted by the sequencer and given to nobody.
func routeOf(p otlpmetrics.SumPoint) route {
	switch {
	case p.Scope == svcgraph.Scope && p.Metric == svcgraph.Metric:
		return routeServiceGraph
	case p.Metric == spancount.Metric:
		return routeSpanCount
	}
	return routeOther
}

// countPoints is the weight and the slots of a metrics request: its data points,
// the sum points and the dropped ones, and the slots its translation fills, which
// are one for each service graph point (an upper bound on its sightings; the
// unused stay nil and are never rejected) and one for each resource with span
// count points.
func countPoints(d otlpmetrics.Data) (records, slots int) {
	for _, res := range d.Resources {
		records += len(res.Points) + max(res.DroppedNotSum, 0) + max(res.DroppedNonMonotonic, 0)
		sg, sc := 0, 0
		for _, p := range res.Points {
			switch routeOf(p) {
			case routeServiceGraph:
				sg++
			case routeSpanCount:
				sc++
			}
		}
		slots += sg
		if sc > 0 {
			slots++
		}
	}
	return records, slots
}

// translateMetrics gives the points of each resource of a metrics request to the
// translators of its cluster name, by metric, and puts what they returned in the
// request's slots: within a resource the service graph's sightings first, one
// slot each, and then the span counts' one slot. A refused slot rejects the
// points it stands for (see "Metrics" in the package documentation). received is
// the instant the request was claimed at, so a rerun from memory translates alike.
func (s *Sequencer) translateMetrics(w *work) {
	r := w.r
	w.weights = make([]int64, r.slots)
	slot := 0
	for i, res := range r.points.Resources {
		name := w.names[i]
		c := s.clusters[name]
		var sg, sc []otlpmetrics.SumPoint
		other := 0
		for _, p := range res.Points {
			switch routeOf(p) {
			case routeServiceGraph:
				sg = append(sg, p)
			case routeSpanCount:
				sc = append(sc, p)
			default:
				other++
			}
		}
		w.metrics.add(name, counterMetricsPoints, int64(len(res.Points)+max(res.DroppedNotSum, 0)+max(res.DroppedNonMonotonic, 0)))
		w.metrics.add(name, counterSkippedOtherMetric, int64(other))
		w.metrics.add(name, counterDroppedNotSum, int64(res.DroppedNotSum))
		w.metrics.add(name, counterDroppedNonMonotonic, int64(res.DroppedNonMonotonic))
		if s.hooks.beforeTranslateMetrics != nil {
			s.hooks.beforeTranslateMetrics(name, res)
		}
		part := func(pts []otlpmetrics.SumPoint) otlpmetrics.Resource {
			return otlpmetrics.Resource{Attributes: res.Attributes, NewestTimeUnixNano: res.NewestTimeUnixNano, Points: pts}
		}
		if len(sg) > 0 {
			w.metrics.saw(&w.metrics.svcgraph, name)
			w.metrics.add(name, counterSvcgraphPoints, int64(len(sg)))
			out := c.svcgraph.Translate(part(sg), r.received)
			producer := svcgraph.Source + "/" + name
			for _, k := range out.Skipped {
				w.count(&w.skips, producer, k.Reason, "", k.Count)
				w.metrics.add(name, counterOf(svcgraphCounters, svcgraph.Source, k.Reason), int64(k.Count))
			}
			sightings := splitSightings(out.Records)
			if len(sightings) > len(sg) {
				// Each sighting stands for a point.
				panic("sequencer: the service graph translator gave more sightings than points")
			}
			for k, recs := range sightings {
				w.out[slot+k], w.weights[slot+k] = recs, 1
			}
			w.metrics.add(name, counterSvcgraphSightings, int64(len(sightings)))
		}
		slot += len(sg)
		if len(sc) > 0 {
			w.metrics.saw(&w.metrics.spancount, name)
			w.metrics.add(name, counterSpancountPoints, int64(len(sc)))
			out := c.spancount.Translate(part(sc), r.received)
			producer := spancount.Source + "/" + name
			for _, k := range out.Skipped {
				w.count(&w.skips, producer, k.Reason, "", k.Count)
				w.metrics.add(name, counterOf(spancountCounters, spancount.Source, k.Reason), int64(k.Count))
			}
			w.out[slot], w.weights[slot] = out.Records, int64(len(sc))
			for _, rec := range out.Records {
				if rec.Subject.Kind == store.SubjectEntity && rec.Subject.A.Type() == catalog.ServiceInstance {
					w.metrics.add(name, counterSpancountSightings, 1)
				}
			}
			slot++
		}
	}
}

// counterOf is the counter a translator's reason is counted under.
func counterOf(table map[string]string, source, reason string) string {
	if c, ok := table[reason]; ok {
		return c
	}
	return source + ".skipped_other"
}

// splitSightings splits the records of the service graph translator by sighting:
// for each, the client's service, the server's unless it is the client, and the
// depends_on edge, which ends it.
func splitSightings(recs []store.Record) [][]store.Record {
	var out [][]store.Record
	start := 0
	for i, r := range recs {
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.DependsOn {
			out = append(out, recs[start:i+1:i+1])
			start = i + 1
		}
	}
	if start < len(recs) {
		out = append(out, recs[start:len(recs):len(recs)])
	}
	return out
}

// isSpanCount says whether a record is the span count translators': its time is
// an application's clock.
func isSpanCount(r store.Record) bool {
	return strings.HasPrefix(string(r.Producer), spancount.Source+"/")
}

// clockRecords is the records that take part in retention's agreement of clocks:
// those of a batch except the span counts', whose times are the clocks of many
// applications, any of which being behind would make a batch disagree and pause
// retention. The service graph's times are the collector's, one clock. See
// "Retention" in the package documentation. The batch is returned itself when it
// holds no span count record.
func clockRecords(recs []store.Record) []store.Record {
	i := slices.IndexFunc(recs, isSpanCount)
	if i < 0 {
		return recs
	}
	out := slices.Clone(recs[:i])
	for _, r := range recs[i+1:] {
		if !isSpanCount(r) {
			out = append(out, r)
		}
	}
	return out
}

// metricsCluster is what the sequencer knows of the metrics of one cluster name.
type metricsCluster struct {
	// lastSG and lastSC are the ingest instants of the latest group that held a
	// service graph point, and a span count point, of the cluster. silent says three
	// connector flushes have passed since lastSG; warned says the silence has been
	// warned about. series is the loop's last snapshot of the service graph
	// translator's series.
	lastSG, lastSC time.Time
	silent, warned bool
	series         int
}

// MetricsStatus is a snapshot of the metrics path's counters and of the latest
// points of each cluster that has had any. The counters are those of every group
// answered without error since New.
type MetricsStatus struct {
	// DependencyExpiry and InstanceTTL are the TTLs of the two producers.
	DependencyExpiry, InstanceTTL time.Duration
	// Counters are named as the constants of this file say.
	Counters map[string]int64
	// Clusters are the clusters that have had a service graph or span count point,
	// by name.
	Clusters map[string]MetricsCluster
}

// MetricsCluster is the latest metrics of a cluster.
type MetricsCluster struct {
	// LastServiceGraphPoint and LastSpanCountPoint are the ingest instants of the
	// latest group that held a point of the kind for the cluster, never a point's
	// own time, which is a sender's clock. Zero for none.
	LastServiceGraphPoint, LastSpanCountPoint time.Time
	// Silent says that more than three connector flushes have passed since the
	// latest service graph point, as of the latest flush tick.
	Silent bool
	// Series is the number of series the cluster's service graph translator held
	// after the latest group or flush tick, which the loop snapshots: the status
	// never calls a translator.
	Series int
}

// MetricsStatus returns the metrics path's counters and last points. It may be
// called from any goroutine.
func (s *Sequencer) MetricsStatus() MetricsStatus {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	st := MetricsStatus{
		DependencyExpiry: s.cfg.DependencyExpiry,
		InstanceTTL:      s.cfg.InstanceTTL,
		Counters:         maps.Clone(s.metricsCount),
		Clusters:         make(map[string]MetricsCluster, len(s.metricsSeen)),
	}
	for name, c := range s.metricsSeen {
		st.Clusters[name] = MetricsCluster{
			LastServiceGraphPoint: c.lastSG, LastSpanCountPoint: c.lastSC, Silent: c.silent, Series: c.series,
		}
	}
	return st
}

// noteMetrics is called for a group answered with success: it promotes the
// pending state of every service graph translator to committed, adds the counts of
// the group's requests that were not refused, notes the instant of each cluster's
// points, and logs what the operator has to act on, at most once a minute for each
// kind and cluster: a series table that is full, and span counts with no instance
// id.
func (s *Sequencer) noteMetrics(ws []*work, now time.Time) {
	for _, c := range s.clusters {
		c.svcgraph.Commit()
	}
	byCluster := map[string]map[string]int64{}
	sg, sc := map[string]bool{}, map[string]bool{}
	for _, w := range ws {
		if w.refused != "" {
			continue
		}
		for name, m := range w.metrics.counts {
			t := byCluster[name]
			if t == nil {
				t = make(map[string]int64, len(m))
				byCluster[name] = t
			}
			for k, v := range m {
				t[k] += v
			}
		}
		maps.Copy(sg, w.metrics.svcgraph)
		maps.Copy(sc, w.metrics.spancount)
	}
	names := slices.Sorted(maps.Keys(byCluster))
	s.metricsMu.Lock()
	for _, name := range names {
		for k, v := range byCluster[name] {
			s.metricsCount[k] += v
		}
	}
	for _, name := range slices.Sorted(maps.Keys(sg)) {
		c := s.seenMetrics(name)
		c.lastSG, c.silent, c.warned = now, false, false
	}
	for _, name := range slices.Sorted(maps.Keys(sc)) {
		s.seenMetrics(name).lastSC = now
	}
	s.metricsMu.Unlock()
	for _, name := range names {
		m := byCluster[name]
		if n := m[counterSeriesOverflow]; n > 0 && s.resumeLogDue("svcgraph_overflow/"+name, now) {
			s.log.Warn("sequencer: the service graph translator's series table is full; new series are not sighted until there is room (a sender can cause this on purpose; restrict who reaches the receiver)",
				"cluster", name, "count", n, "max_series", s.cfg.ServiceGraphMaxSeries)
		}
		if n := m[counterNoInstanceID]; n > 0 && s.resumeLogDue("no_instance_id/"+name, now) {
			s.log.Warn("sequencer: span counts with no usable service.instance.id write no instance and no service; set service.instance.id in the application's SDK or OTEL_RESOURCE_ATTRIBUTES",
				"cluster", name, "count", n)
		}
	}
	s.snapshotSeries()
}

// seenMetrics returns the entry of a cluster name, made if there is none. The
// caller holds metricsMu.
func (s *Sequencer) seenMetrics(name string) *metricsCluster {
	c := s.metricsSeen[name]
	if c == nil {
		c = &metricsCluster{}
		s.metricsSeen[name] = c
	}
	return c
}

// snapshotSeries copies the series count of each cluster's service graph
// translator into the status. Only the loop calls a translator; the status reads
// the copy. A name whose translators were freed holds none.
func (s *Sequencer) snapshotSeries() {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	for name, m := range s.metricsSeen {
		m.series = 0
		if c := s.clusters[name]; c != nil {
			m.series = c.svcgraph.Series()
		}
	}
}

// metricsKeep is how long a cluster's latest metrics are kept in the status after
// them, and with it the throttles of its warnings, so that a cluster that went
// silent stays in the status (marked silent) for a day, past the freeing of its
// slot, and the memory of client-chosen names is bounded.
const metricsKeep = 24 * time.Hour

// silentAfter is how long a cluster may go without a service graph point before it
// is silent: three of the connector's flushes.
const silentAfter = 3 * svcgraph.DefaultFlush

// checkMetricsSilent is called at each flush tick. A cluster that had a service
// graph point within the last day and has had none for more than three connector
// flushes is marked silent, and warned about, once for each time it goes silent:
// the calls between its services stop being sighted, and lapse after the
// dependency expiry. The ingest clock's present is the comparison, so a stopped
// collector and a stopped connector read alike. A cluster whose latest points are
// more than [metricsKeep] old is forgotten.
func (s *Sequencer) checkMetricsSilent(now time.Time) {
	type silent struct {
		name string
		last time.Time
	}
	var warn []silent
	var forget []string
	s.metricsMu.Lock()
	for name, c := range s.metricsSeen {
		if now.Sub(c.lastSG) > metricsKeep && now.Sub(c.lastSC) > metricsKeep {
			delete(s.metricsSeen, name)
			forget = append(forget, name)
			continue
		}
		c.silent = !c.lastSG.IsZero() && now.Sub(c.lastSG) <= metricsKeep && now.Sub(c.lastSG) > silentAfter
		if c.silent && !c.warned {
			c.warned = true
			warn = append(warn, silent{name, c.lastSG})
		}
	}
	s.metricsMu.Unlock()
	for _, name := range forget {
		s.forgetWarned(name)
	}
	slices.SortFunc(warn, func(a, b silent) int { return strings.Compare(a.name, b.name) })
	for _, w := range warn {
		s.log.Warn("sequencer: no service graph point for more than three flushes; calls between services stop being sighted and lapse after the dependency expiry (the trace collector or its pipeline is down, the shipped collector's metrics pipeline is off, or no call happened for about 16 minutes)",
			"cluster", w.name, "last_service_graph_point", w.last, "dependency_expiry", s.cfg.DependencyExpiry)
	}
}
