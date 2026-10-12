package sequencer

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/detector"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
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
)

// cluster is the translators of one cluster name.
type cluster struct {
	objects *k8sobjects.Translator
	cluster *k8scluster.Translator
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
	return &cluster{objects: objects, cluster: cl}, nil
}

// renewCluster makes the translators of a cluster name anew, keeping its slot.
func (s *Sequencer) renewCluster(name string) {
	c, err := s.newCluster(name)
	if err != nil {
		// New made a pair with the same configuration.
		panic("sequencer: " + err.Error())
	}
	if old := s.clusters[name]; old != nil {
		c.lastUsed = old.lastUsed
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
	// out holds, for each log record in the order of the request, the store
	// records it gave that passed the checks, or nil if it was rejected.
	out         [][]store.Record
	rejected    int64
	firstReject string
	// refusedClock is the newest event time stamped by another machine's clock
	// among the records refused as before a horizon; zero for none.
	refusedClock time.Time
	// notes and skips count what the translators said, by producer and reason.
	notes, skips map[[3]string]int
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

// reject rejects the log record at slot into the request's partial success.
func (w *work) reject(slot int, err error) {
	w.out[slot] = nil
	if w.rejected == 0 {
		w.firstReject = err.Error()
	}
	w.rejected++
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
// and the detector, the translation and the checks, in arrival order. A request
// whose translation panics is refused, the translators of the names it touched
// are made anew, and the requests before it that share a name with those are
// translated again from memory: nothing has reached the coalescer yet.
func (s *Sequencer) translateAll(reqs []*request, now time.Time) []*work {
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
		p := s.translate(w, now)
		if p == nil {
			continue
		}
		w.r.panicked = fmt.Sprintf("translating a record of this request failed (%s); the request is refused", bounded(fmt.Sprint(p), maxQuoted))
		w.refused, w.out = w.r.panicked, nil
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
	return ws
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
// the detector, the translation of the rest, and the checks. It returns the
// value of a panic it recovered, or nil.
func (s *Sequencer) translate(w *work, now time.Time) (panicked any) {
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("sequencer: translating a request panicked", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			panicked = p
		}
	}()
	if !s.resolveNames(w, now) {
		return nil
	}
	r := w.r
	w.out = make([][]store.Record, r.records)
	selected := make([]bool, r.records)

	// The namespace pass: only the namespace events go through the translator
	// first, so that the kube-system set follows every sighting, refused
	// requests included, and the detector sees them before it decides.
	slot := 0
	for i, rl := range r.data.ResourceLogs {
		name := w.names[i]
		c := s.clusters[name]
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				if namespaceEvent(lr) {
					selected[slot] = true
					res, err := s.translateCluster(c, name, rl.Resource, lr, r.received, w)
					if err != nil {
						w.reject(slot, err)
					} else {
						w.out[slot] = res.Records
						for _, ns := range res.Namespaces {
							s.det.Observe(detector.FromK8sCluster(name, ns))
						}
					}
				}
				slot++
			}
		}
	}
	for _, name := range w.touched {
		if v := s.det.Check(name, now); v.Refuse {
			// The rest of the request is never translated.
			w.refused = v.Message
			return nil
		}
	}

	// The rest, by event name: the cluster receiver's entity events to its
	// translator, everything else to the objects translator.
	slot = 0
	for i, rl := range r.data.ResourceLogs {
		name := w.names[i]
		c := s.clusters[name]
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				if !selected[slot] {
					var recs []store.Record
					var err error
					if lr.EventName == eventState || lr.EventName == eventDelete {
						var res k8scluster.Result
						res, err = s.translateCluster(c, name, rl.Resource, lr, r.received, w)
						recs = res.Records
					} else {
						var res k8sobjects.Result
						res, err = s.translateObjects(c, name, rl.Resource, lr, w)
						recs = res.Records
					}
					if err != nil {
						w.reject(slot, err)
					} else {
						w.out[slot] = recs
					}
				}
				slot++
			}
		}
	}

	// The checks: a log record is rejected if any of its store records would be
	// refused by the store on any retry, and none of its records reaches the
	// coalescer.
	for slot, recs := range w.out {
		for _, rec := range recs {
			if err := s.check(rec); err != nil {
				if errors.Is(err, store.ErrBeforeHorizon) && qualifies(rec.EventTimeBasis) && rec.EventTime.After(w.refusedClock) {
					// A refusal says what the producer's clock reads as much as a
					// commit does. See "Retention" in the package documentation.
					w.refusedClock = rec.EventTime
				}
				w.reject(slot, err)
				break
			}
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

// namespaceEvent says whether a log record is an entity event of a namespace. It
// reads entity.type as the cluster translator does: the last attribute of that
// key with a string value.
func namespaceEvent(lr capture.LogRecord) bool {
	if lr.EventName != eventState && lr.EventName != eventDelete {
		return false
	}
	typ := ""
	for _, kv := range lr.Attributes {
		if kv.Key == attrEntityType && kv.Value.StringValue != nil {
			typ = *kv.Value.StringValue
		}
	}
	return typ == typeNamespace
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

// resolveNames gives each resource of the request its cluster name, and makes
// the translators of a name not seen before. It refuses the whole request, and
// returns false, for a k8s.cluster.name that differs from the configured name,
// for a name a store cannot write, and for a name beyond MaxClusters.
func (s *Sequencer) resolveNames(w *work, now time.Time) bool {
	rls := w.r.data.ResourceLogs
	names := make([]string, len(rls))
	var fresh []string
	for i, rl := range rls {
		v, ok := clusterAttr(rl.Resource)
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
			quote(fresh[0]), s.cfg.MaxClusters, idleMultiple*max(s.cfg.PullInterval, s.cfg.CollectionInterval))
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
func clusterAttr(res capture.Resource) (string, bool) {
	v := ""
	for _, kv := range res.Attributes {
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
