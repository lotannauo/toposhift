package sequencer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/detector"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The names the receivers give their records and attributes.
const (
	attrClusterName = "k8s.cluster.name"
	attrEntityType  = "entity.type"
	eventState      = "entity.state"
	eventDelete     = "entity.delete"
	typeNamespace   = "k8s.namespace"
	typeHost        = "host"
	// attrLegacyPrefix starts the name of every attribute of the older form of the
	// entity events, and scopeEventAsLog is the scope attribute the receivers set
	// on the entity events they write as logs, in both forms.
	attrLegacyPrefix = "otel.entity."
	scopeEventAsLog  = "otel.entity.event_as_log"
)

// translatorKind says which of a cluster name's translators takes a log record.
type translatorKind uint8

const (
	kindObjects   translatorKind = iota // the Kubernetes objects receiver's records
	kindCluster                         // the cluster receiver's entity events
	kindNodeAgent                       // a node collector's host entity events
)

// cluster is the translators of one cluster name.
type cluster struct {
	objects   *k8sobjects.Translator
	cluster   *k8scluster.Translator
	nodeagent *nodeagent.Translator
	// svcgraph keeps the state of the service graph connector's series, committed
	// and pending, and spancount keeps none (see metrics.go).
	svcgraph  *svcgraph.Translator
	spancount *spancount.Translator
	// lastUsed is the ingest instant of the latest request that used the name.
	lastUsed time.Time
}

// newCluster makes the translators of a cluster name, with no state.
func (s *Sequencer) newCluster(name string) (*cluster, error) {
	objects, err := k8sobjects.New(k8sobjects.Config{
		Producer:     lifecycle.Producer("k8sobjects/" + name),
		Resolver:     s.resolver,
		PullInterval: s.cfg.PullInterval,
	})
	if err != nil {
		return nil, err
	}
	cl, err := k8scluster.New(k8scluster.Config{
		Producer:           lifecycle.Producer("k8scluster/" + name),
		Resolver:           s.resolver,
		CollectionInterval: s.cfg.CollectionInterval,
	})
	if err != nil {
		return nil, err
	}
	na, err := nodeagent.New(nodeagent.Config{
		Producer: lifecycle.Producer(nodeagent.Source + "/" + name),
		Resolver: s.resolver,
		Interval: s.cfg.NodeInterval,
	})
	if err != nil {
		return nil, err
	}
	sg, err := svcgraph.New(svcgraph.Config{
		Producer:  lifecycle.Producer(svcgraph.Source + "/" + name),
		Resolver:  s.resolver,
		Expiry:    s.cfg.DependencyExpiry,
		MaxSeries: s.cfg.ServiceGraphMaxSeries,
	})
	if err != nil {
		return nil, err
	}
	sc, err := spancount.New(spancount.Config{
		Producer: lifecycle.Producer(spancount.Source + "/" + name),
		Resolver: s.resolver,
		TTL:      s.cfg.InstanceTTL,
	})
	if err != nil {
		return nil, err
	}
	return &cluster{objects: objects, cluster: cl, nodeagent: na, svcgraph: sg, spancount: sc}, nil
}

// renewCluster makes the translators of a cluster name anew, keeping its slot:
// as a restart does, and as a panic while translating a request of the name
// must, so the service graph translator loses its state and warms up again.
func (s *Sequencer) renewCluster(name string) { s.replaceCluster(name, false) }

// resetCluster is renewCluster for a group that failed: the k8s translators are
// made anew, but the name's service graph translator is kept, rolled back to its
// committed state. Its state is the connector's series, which nothing but the
// metrics the collector sends again can rebuild, and a failed group must not lose
// it (see "Failed writes" and "Metrics" in the package documentation).
func (s *Sequencer) resetCluster(name string) { s.replaceCluster(name, true) }

func (s *Sequencer) replaceCluster(name string, keepGraph bool) {
	c, err := s.newCluster(name)
	if err != nil {
		// New made a pair with the same configuration.
		panic("sequencer: " + err.Error())
	}
	if old := s.clusters[name]; old != nil {
		c.lastUsed = old.lastUsed
		if keepGraph {
			old.svcgraph.Rollback()
			c.svcgraph = old.svcgraph
		}
	}
	s.clusters[name] = c
}

// work is one attempt at one request: what its translation gave, and its verdict.
// A rerun starts from a new one.
type work struct {
	r *request
	// names is the cluster name of each resource, and touched the distinct names,
	// in order of first appearance. Both are set once the names are resolved.
	names   []string
	touched []string
	// refused is the message of a refusal of the whole request; empty otherwise.
	refused string
	// out holds, for each slot in the order of the request (a log record, or for a
	// metrics request what metrics.go says), the store records it gave that passed
	// the checks, or nil if it was rejected. weights, when set, is what rejecting
	// each slot rejects, in data points; none is 1.
	out         [][]store.Record
	weights     []int64
	rejected    int64
	firstReject string
	// refusedClock is the newest event time stamped by another machine's clock
	// among the records refused as before a horizon; zero for none.
	refusedClock time.Time
	// notes and skips count what the translators said, by producer and reason.
	notes, skips map[[3]string]int
	// agent counts, by cluster name and counter name, what the node agents'
	// records and the legacy entity events of the request stood for (see
	// nodeagent.go). The counts are kept only when the request is answered
	// without error.
	agent map[string]map[string]int64
	// resumed counts what the resume step decided for the request's records. It is
	// added to the sequencer's counts only when the group is answered without
	// error.
	resumed resume.Stats
	// metrics counts what the metrics path did with the request (see metrics.go).
	// Like the node agents' counts, they are kept only when the request is answered
	// without error.
	metrics metricsWork
}

// call is one translator call of a request: the slot of its log record, the
// cluster name, which translator, and the records it marked backdated (by their
// index in the slot's records).
type call struct {
	slot  int
	name  string
	kind  translatorKind
	marks []resume.Mark
}

// coalesce adds the records that passed to the coalescer, in order, and appends
// what it returns to batch. A refused request gives nothing.
func (w *work) coalesce(co *coalesce.Coalescer, batch []store.Record) []store.Record {
	if w.refused != "" {
		return batch
	}
	for _, recs := range w.out {
		for _, r := range recs {
			batch = co.Add(r, batch)
		}
	}
	return batch
}

// reject rejects the slot, with the weight it has in the request's partial
// success, into that partial success.
func (w *work) reject(slot int, err error, weight int64) {
	w.out[slot] = nil
	if w.rejected == 0 {
		w.firstReject = err.Error()
	}
	w.rejected += weight
}

// weight is what rejecting the slot adds to the partial success: a log record
// is 1, and a slot of a metrics request the data points it stands for.
func (w *work) weight(slot int) int64 {
	if w.weights == nil {
		return 1
	}
	return w.weights[slot]
}

func (w *work) count(m *map[[3]string]int, producer, reason, detail string, n int) {
	if *m == nil {
		*m = make(map[[3]string]int)
	}
	(*m)[[3]string{producer, reason, detail}] += n
}

func (w *work) logNotes(log *slog.Logger) {
	for _, k := range slices.SortedFunc(maps.Keys(w.notes), compareKeys) {
		log.Info("sequencer: translator note", "producer", k[0], "reason", k[1], "detail", k[2], "count", w.notes[k])
	}
	for _, k := range slices.SortedFunc(maps.Keys(w.skips), compareKeys) {
		log.Debug("sequencer: translator skip", "producer", k[0], "reason", k[1], "count", w.skips[k])
	}
}

func compareKeys(a, b [3]string) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// translateAll runs each request through the cluster names, the namespace pass
// and the detector, the translation, the resume step and the checks, in arrival
// order. A request whose translation panics is refused, the translators of the
// names it touched are made anew, and the requests before it that share a name
// with those are translated again from memory: nothing has reached the coalescer
// yet. lk is the group's lookup, nil when resume is off. An error from the resume
// step that aborts (see [resume.IsAbort]) is returned at once: the group is not
// written.
func (s *Sequencer) translateAll(ctx context.Context, reqs []*request, now time.Time, lk resume.Lookup) ([]*work, error) {
	ws := make([]*work, len(reqs))
	for i, r := range reqs {
		ws[i] = &work{r: r}
	}
	done := make([]bool, len(ws))
	for i := 0; i < len(ws); i++ {
		if done[i] {
			continue
		}
		done[i] = true
		w := ws[i]
		if w.r.panicked != "" {
			w.refused = w.r.panicked
			continue
		}
		p, err := s.translate(ctx, w, now, lk)
		if err != nil {
			return ws, err
		}
		if p == nil {
			continue
		}
		w.r.panicked = fmt.Sprintf("translating a record of this request failed (%s); the request is refused", bounded(fmt.Sprint(p), maxQuoted))
		w.refused, w.out, w.resumed = w.r.panicked, nil, resume.Stats{}
		// The names to make anew: those the request touched, and those of every
		// earlier request that shares one, until no more join.
		renew := make(map[string]bool)
		for _, n := range w.touched {
			renew[n] = true
		}
		redo := make([]bool, i)
		for grew := true; grew; {
			grew = false
			for j := range i {
				if redo[j] || ws[j].r.panicked != "" || !sharesName(ws[j].touched, renew) {
					continue
				}
				redo[j] = true
				for _, n := range ws[j].touched {
					if !renew[n] {
						renew[n], grew = true, true
					}
				}
			}
		}
		for _, n := range slices.Sorted(maps.Keys(renew)) {
			if _, ok := s.clusters[n]; ok {
				s.renewCluster(n)
			}
		}
		s.log.Warn("sequencer: the translators of the request's cluster names are made anew", "clusters", slices.Sorted(maps.Keys(renew)))
		for j := range i {
			if redo[j] {
				ws[j], done[j] = &work{r: ws[j].r}, false
			}
		}
		i = -1 // translate the reruns, in arrival order, then go on
	}
	return ws, nil
}

func sharesName(names []string, set map[string]bool) bool {
	for _, n := range names {
		if set[n] {
			return true
		}
	}
	return false
}

// translate takes one request through the cluster names, the namespace pass and
// the detector, the translation of the rest, the resume step, and the checks. It
// returns the value of a panic it recovered, or nil, and the error of a resume
// step that aborts.
func (s *Sequencer) translate(ctx context.Context, w *work, now time.Time, lk resume.Lookup) (panicked any, abort error) {
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("sequencer: translating a request panicked", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			panicked, abort = p, nil
		}
	}()
	r := w.r
	if r.metrics {
		attrs := make([][]capture.KeyValue, len(r.points.Resources))
		for i, res := range r.points.Resources {
			attrs[i] = res.Attributes
		}
		if !s.resolveNames(w, now, attrs) {
			return nil, nil
		}
		w.out = make([][]store.Record, r.slots)
		if s.detectorRefuses(w, now) {
			return nil, nil
		}
		s.translateMetrics(w)
		s.checkSlots(w)
		return nil, nil
	}
	attrs := make([][]capture.KeyValue, len(r.data.ResourceLogs))
	for i, rl := range r.data.ResourceLogs {
		attrs[i] = rl.Resource.Attributes
	}
	if !s.resolveNames(w, now, attrs) {
		return nil, nil
	}
	w.out = make([][]store.Record, r.records)
	selected := make([]bool, r.records)
	var calls []call

	// The namespace pass: only the namespace events go through the translator
	// first, so that the kube-system set follows every sighting, refused
	// requests included, and the detector sees them before it decides.
	slot := 0
	for i, rl := range r.data.ResourceLogs {
		name := w.names[i]
		c := s.clusters[name]
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				// A legacy event is counted in the second pass and never translated.
				if namespaceEvent(lr) && !legacyEntityEvent(sl.Scope, lr) {
					selected[slot] = true
					res, err := s.translateCluster(c, name, rl.Resource, lr, r.received, w)
					if err != nil {
						w.reject(slot, err, 1)
					} else {
						w.out[slot] = res.Records
						calls = append(calls, call{slot: slot, name: name, kind: kindCluster, marks: res.Backdated})
						for _, ns := range res.Namespaces {
							s.det.Observe(detector.FromK8sCluster(name, ns))
						}
					}
				}
				slot++
			}
		}
	}
	if s.detectorRefuses(w, now) {
		// The rest of the request is never translated.
		return nil, nil
	}

	// The rest, by what the record is. A legacy entity event, the older form the
	// receivers write when the feature gate is off, is counted and translated by
	// nobody. Of the entity events, those of a host go to the node agents'
	// translator and the others to the cluster receiver's; everything else goes to
	// the objects translator.
	slot = 0
	for i, rl := range r.data.ResourceLogs {
		name := w.names[i]
		c := s.clusters[name]
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				switch {
				case selected[slot]:
				case legacyEntityEvent(sl.Scope, lr):
					w.countAgent(name, counterLegacyEvents, 1)
				default:
					var recs []store.Record
					var marks []resume.Mark
					var err error
					kind := kindOf(lr)
					switch kind {
					case kindNodeAgent:
						var res nodeagent.Result
						res, err = s.translateNodeAgent(c, name, rl.Resource, lr, r.received, w)
						recs, marks = res.Records, res.Backdated
					case kindCluster:
						var res k8scluster.Result
						res, err = s.translateCluster(c, name, rl.Resource, lr, r.received, w)
						recs, marks = res.Records, res.Backdated
					default:
						var res k8sobjects.Result
						res, err = s.translateObjects(c, name, rl.Resource, lr, w)
						recs, marks = res.Records, res.Backdated
					}
					if err != nil {
						w.reject(slot, err, 1)
					} else {
						w.out[slot] = recs
						calls = append(calls, call{slot: slot, name: name, kind: kind, marks: marks})
					}
				}
				slot++
			}
		}
	}

	// The resume step, before the checks and the coalescer: see "Resume" in the
	// package documentation.
	if lk != nil {
		if err := s.resume(ctx, w, lk, calls); err != nil {
			return nil, err
		}
	}

	s.checkSlots(w)
	return nil, nil
}

// detectorRefuses checks each cluster name the request touched with the detector,
// and, when one is refused, refuses the whole request with its message.
func (s *Sequencer) detectorRefuses(w *work, now time.Time) bool {
	for _, name := range w.touched {
		if v := s.det.Check(name, now); v.Refuse {
			w.refused = v.Message
			return true
		}
	}
	return false
}

// checkSlots runs the checks: a slot (a log record, or what a sighting or a
// resource of a metrics request gave) is rejected if any of its store records
// would be refused by the store on any retry, and none of its records reaches the
// coalescer.
func (s *Sequencer) checkSlots(w *work) {
	for slot, recs := range w.out {
		for _, rec := range recs {
			if err := s.check(rec); err != nil {
				if errors.Is(err, store.ErrBeforeHorizon) && qualifies(rec.EventTimeBasis) && !isSpanCount(rec) && rec.EventTime.After(w.refusedClock) {
					// A refusal says what the producer's clock reads as much as a
					// commit does. See "Retention" in the package documentation. The
					// span counts' clocks are the applications', and do not count.
					w.refusedClock = rec.EventTime
				}
				w.reject(slot, err, w.weight(slot))
				break
			}
		}
	}
}

// resumerOf is the translator of a cluster name's kind as the resume step wants
// it, with the function that gives what describes a subject in its payloads.
func (s *Sequencer) resumerOf(name string, kind translatorKind) (resume.Resumer, func(store.Subject, []byte) []byte) {
	c := s.clusters[name]
	switch kind {
	case kindCluster:
		return c.cluster, k8scluster.Comparable
	case kindNodeAgent:
		return c.nodeagent, nodeagent.Comparable
	}
	return c.objects, k8sobjects.Comparable
}

// resume takes the records of each translator of the request through
// [resume.Apply], one producer at a time in the order the producers first appear,
// each with its records in the order they were translated (the namespace pass
// first), and puts back what it kept in their slots.
func (s *Sequencer) resume(ctx context.Context, w *work, lk resume.Lookup, calls []call) error {
	type group struct {
		name string
		kind translatorKind
	}
	var order []group
	byGroup := map[group][]call{}
	for _, c := range calls {
		g := group{c.name, c.kind}
		if _, ok := byGroup[g]; !ok {
			order = append(order, g)
		}
		byGroup[g] = append(byGroup[g], c)
	}
	horizon := func(l catalog.Layer) time.Time {
		if l < catalog.L0 || l > catalog.L3 {
			return time.Time{}
		}
		return s.horizons[l-catalog.L0]
	}
	for _, g := range order {
		cs := byGroup[g]
		var res resume.Result
		marked := false
		for _, c := range cs {
			base := len(res.Records)
			res.Records = append(res.Records, w.out[c.slot]...)
			for _, m := range c.marks {
				m.Index += base
				res.Marks = append(res.Marks, m)
				marked = true
			}
		}
		if !marked {
			continue
		}
		tr, described := s.resumerOf(g.name, g.kind)
		st, err := resume.Apply(ctx, lk, horizon, &res, tr, described)
		if err != nil {
			return err
		}
		w.resumed.Add(st)
		// Put the records back in their slots, without those Apply dropped.
		dropped := res.Dropped
		i, k := 0, 0 // the position in the records as given, and in what Apply kept
		for _, c := range cs {
			n := len(w.out[c.slot])
			var kept []store.Record
			for range n {
				if len(dropped) > 0 && dropped[0] == i {
					dropped = dropped[1:]
				} else {
					kept = append(kept, res.Records[k])
					k++
				}
				i++
			}
			w.out[c.slot] = kept
		}
	}
	return nil
}

func (s *Sequencer) translateCluster(c *cluster, name string, res capture.Resource, lr capture.LogRecord, received time.Time, w *work) (k8scluster.Result, error) {
	if s.hooks.beforeTranslate != nil {
		s.hooks.beforeTranslate(name, lr)
	}
	out, err := c.cluster.Translate(res, lr, received)
	producer := "k8scluster/" + name
	for _, n := range out.Notes {
		w.count(&w.notes, producer, n.Reason, n.Detail, n.Count)
	}
	for _, k := range out.Skipped {
		w.count(&w.skips, producer, k.Reason, "", k.Count)
	}
	return out, err
}

// translateNodeAgent gives a host entity event to the cluster name's node agent
// translator, with the ingest instant of the request as the time it was received:
// the translator checks the agent's clock against it.
func (s *Sequencer) translateNodeAgent(c *cluster, name string, res capture.Resource, lr capture.LogRecord, received time.Time, w *work) (nodeagent.Result, error) {
	if s.hooks.beforeTranslate != nil {
		s.hooks.beforeTranslate(name, lr)
	}
	w.countAgent(name, counterHostEvents, 1)
	out, err := c.nodeagent.Translate(res, lr, received)
	producer := nodeagent.Source + "/" + name
	for _, n := range out.Notes {
		w.count(&w.notes, producer, n.Reason, n.Detail, n.Count)
	}
	for _, k := range out.Skipped {
		w.count(&w.skips, producer, k.Reason, "", k.Count)
		w.countAgent(name, agentCounter(k.Reason), int64(k.Count))
	}
	return out, err
}

func (s *Sequencer) translateObjects(c *cluster, name string, res capture.Resource, lr capture.LogRecord, w *work) (k8sobjects.Result, error) {
	if s.hooks.beforeTranslate != nil {
		s.hooks.beforeTranslate(name, lr)
	}
	out, err := c.objects.Translate(res, lr)
	producer := "k8sobjects/" + name
	for _, n := range out.Notes {
		w.count(&w.notes, producer, n.Reason, "", n.Count)
	}
	for _, k := range out.Skipped {
		w.count(&w.skips, producer, k.Reason, "", k.Count)
	}
	return out, err
}

// entityType is the entity.type of a log record, read as the cluster translator
// reads it: the last attribute of that key with a string value.
func entityType(lr capture.LogRecord) string {
	typ := ""
	for _, kv := range lr.Attributes {
		if kv.Key == attrEntityType && kv.Value.StringValue != nil {
			typ = *kv.Value.StringValue
		}
	}
	return typ
}

// entityEvent says whether a log record is an entity event of the current form.
func entityEvent(lr capture.LogRecord) bool {
	return lr.EventName == eventState || lr.EventName == eventDelete
}

// namespaceEvent says whether a log record is an entity event of a namespace. It
// reads entity.type as the cluster translator does.
func namespaceEvent(lr capture.LogRecord) bool {
	return entityEvent(lr) && entityType(lr) == typeNamespace
}

// kindOf is the translator that takes a log record: a host entity event goes to
// the node agents' translator, any other entity event to the cluster receiver's,
// and everything else to the objects receiver's.
func kindOf(lr capture.LogRecord) translatorKind {
	switch {
	case !entityEvent(lr):
		return kindObjects
	case entityType(lr) == typeHost:
		return kindNodeAgent
	}
	return kindCluster
}

// legacyEntityEvent says whether a log record is an entity event in the older
// form, which the receivers write when the entity-events feature gate is off.
// That form has no event name and carries its entity in attributes named
// otel.entity.*; the scope attribute otel.entity.event_as_log marks it too, but
// the current form carries that attribute as well, so it counts only on a record
// that has no entity event name. A record of the current form with an
// otel.entity.* attribute of its own is legacy as the translators read it.
func legacyEntityEvent(scope capture.InstrumentationScope, lr capture.LogRecord) bool {
	for _, kv := range lr.Attributes {
		if strings.HasPrefix(kv.Key, attrLegacyPrefix) {
			return true
		}
	}
	if entityEvent(lr) {
		return false
	}
	for _, kv := range scope.Attributes {
		if kv.Key == scopeEventAsLog {
			return true
		}
	}
	return false
}

// check reports why the store would refuse a translated record on any retry: it
// is invalid, its producer is over the write limit, or it is before its layer's
// horizon in a way the coalescer does not mend. A record that carries a Through
// reaching the horizon is re-timed to it by the coalescer, and is not refused.
func (s *Sequencer) check(r store.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := store.WritableProducer(r.Producer); err != nil {
		return err
	}
	if r.Layer < catalog.L0 || r.Layer > catalog.L3 {
		return fmt.Errorf("layer %s: %w", r.Layer, store.ErrInvalid)
	}
	h := s.horizons[r.Layer-catalog.L0]
	if !h.IsZero() && r.EventTime.Before(h) && (r.Through.IsZero() || r.Through.Before(h)) {
		return fmt.Errorf("event time %s is before the retention horizon %s of layer %s: %w",
			r.EventTime.Format(time.RFC3339Nano), h.Format(time.RFC3339Nano), r.Layer, store.ErrBeforeHorizon)
	}
	return nil
}

// resolveNames gives each resource of the request, by its attributes, its cluster name, and makes
// the translators of a name not seen before. It refuses the whole request, and
// returns false, for a k8s.cluster.name that differs from the configured name,
// for a name a store cannot write, and for a name beyond MaxClusters.
func (s *Sequencer) resolveNames(w *work, now time.Time, resources [][]capture.KeyValue) bool {
	names := make([]string, len(resources))
	var fresh []string
	for i, attrs := range resources {
		v, ok := clusterAttr(attrs)
		if s.cfg.ClusterNameSet {
			if ok && v != s.cfg.ClusterName {
				w.refused = fmt.Sprintf("k8s.cluster.name %s differs from the configured cluster name %s: set k8s.cluster.name to %s in the collector, or remove it",
					quote(v), quote(s.cfg.ClusterName), quote(s.cfg.ClusterName))
				return false
			}
			names[i] = s.cfg.ClusterName
			continue
		}
		name := DefaultClusterName
		if ok {
			name = v
		}
		if store.CheckClusterNameLimit(name) != nil {
			w.refused = fmt.Sprintf("cluster name %s cannot be stored: a cluster name is 1 to %d bytes of [A-Za-z0-9._:/-]; set a shorter k8s.cluster.name in the collector, or configure cluster_name",
				quote(name), store.MaxClusterNameLen)
			return false
		}
		names[i] = name
		if _, known := s.clusters[name]; !known && !slices.Contains(fresh, name) {
			fresh = append(fresh, name)
		}
	}
	if len(fresh) > 0 && len(s.clusters)+len(fresh) > s.cfg.MaxClusters {
		w.refused = fmt.Sprintf("cluster name %s is one more than the %d this server takes at once; a name is freed after %s without a request",
			quote(fresh[0]), s.cfg.MaxClusters, idleMultiple*max(s.cfg.PullInterval, s.cfg.CollectionInterval, s.cfg.NodeInterval))
		return false
	}
	w.names = names
	for _, n := range names {
		if !slices.Contains(w.touched, n) {
			w.touched = append(w.touched, n)
		}
	}
	for _, n := range w.touched {
		if _, ok := s.clusters[n]; !ok {
			s.renewCluster(n)
		}
		s.clusters[n].lastUsed = now
	}
	return true
}

// clusterAttr is the resource's k8s.cluster.name: the last attribute of that key
// with a string value, when it is not empty.
func clusterAttr(attrs []capture.KeyValue) (string, bool) {
	v := ""
	for _, kv := range attrs {
		if kv.Key == attrClusterName && kv.Value.StringValue != nil {
			v = *kv.Value.StringValue
		}
	}
	return v, v != ""
}

// maxQuoted bounds a value a client chose, in bytes, where a message shows it.
const maxQuoted = 64

// quote returns s quoted, cut to maxQuoted bytes on a rune boundary, with "..."
// after the closing quote when it was cut.
func quote(s string) string {
	if len(s) <= maxQuoted {
		return strconv.Quote(s)
	}
	return strconv.Quote(bounded(s, maxQuoted)) + "..."
}

// bounded cuts s to at most n bytes on a rune boundary.
func bounded(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
