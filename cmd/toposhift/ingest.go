package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/clustername"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/server"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// envClusterName is the environment twin of --cluster-name.
const envClusterName = "TOPOSHIFT_CLUSTER_NAME"

// envRetain is the environment twin of --retain.
const envRetain = "TOPOSHIFT_RETAIN"

// defaultRetain is --retain when neither the flag nor its variable is given.
// Releases made before background retention passes its latency check on CI ship
// with off, each on its own release commit, and v0.1.0 is the first; main and
// every other build have on. It is a variable only so tests can run serve with the
// other default.
var defaultRetain = "on"

// Where serve's retention mode came from, as GET /status says always, and as the
// boot log says on the line that retention is off, and on the line that it is on
// (logged once ingest is on).
const (
	retainByDefault = "default"
	retainByFlag    = "flag"
	retainByEnv     = "environment"
)

// envNodeInterval is the environment twin of --node-interval.
const envNodeInterval = "TOPOSHIFT_NODE_INTERVAL"

// envDependencyExpiry and envInstanceTTL are the environment twins of
// --dependency-expiry and --instance-ttl.
const (
	envDependencyExpiry = "TOPOSHIFT_DEPENDENCY_EXPIRY"
	envInstanceTTL      = "TOPOSHIFT_INSTANCE_TTL"
)

// The bounds of --dependency-expiry and --instance-ttl: a whole number of seconds,
// at least two connector flushes, below which a call that is made steadily would
// lapse between two sightings, and at most the translators' own bound of 366 days.
const (
	minTTLFlag = 2 * time.Minute
	maxTTLFlag = 366 * 24 * time.Hour
)

// envDuration reads a duration from the environment: an empty variable is unset,
// and gives def.
func envDuration(getenv func(string) string, name string, def time.Duration) (time.Duration, error) {
	v := getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", name, v, err)
	}
	return d, nil
}

// checkTTLFlag refuses a TTL outside the bounds, or with a part of a second.
func checkTTLFlag(name string, d time.Duration) error {
	switch {
	case d%time.Second != 0:
		return usagef("%s %s is not a whole number of seconds", name, d)
	case d < minTTLFlag || d > maxTTLFlag:
		return usagef("%s %s is outside %s to %s", name, d, minTTLFlag, maxTTLFlag)
	}
	return nil
}

// The bounds of --node-interval: a whole number of seconds, at least a minute and
// at most an hour.
const (
	minNodeInterval = time.Minute
	maxNodeInterval = time.Hour
)

// checkNodeInterval refuses a node interval outside the bounds, or with a part of
// a second.
func checkNodeInterval(d time.Duration) error {
	switch {
	case d%time.Second != 0:
		return usagef("--node-interval %s is not a whole number of seconds", d)
	case d < minNodeInterval || d > maxNodeInterval:
		return usagef("--node-interval %s is outside %s to %s", d, minNodeInterval, maxNodeInterval)
	}
	return nil
}

// retainEvery is the interval of the retention check, a constant of serve. It is
// a variable only so tests can lower it.
var retainEvery = sequencer.DefaultRetainEvery

// ingestStarting is what the receiver answers while the store opens.
const ingestStarting = "ingest is starting: the store is opening; send the request again later"

// The bounds of a shutdown after the server's own: the sequencer has this long
// to answer what it has claimed and write the coalescer's absorbed refreshes. It
// is an estimate. It is a variable only so tests can lower it.
var sequencerStopTimeout = 5 * time.Second

// The seams of the boot, for tests: how the store is opened, what the sequencer
// writes through, how it opens the lookup of its resume step, what the read-only
// tools read through, and a hook called with the open store once the cluster name
// is recorded and before the receiver is switched to the sequencer.
var (
	openStore    = pebblestore.Open
	writerFor    = func(st *pebblestore.Store) sequencer.Writer { return st }
	lookupFor    = storeLookup
	readerFor    = func(st *pebblestore.Store) store.Store { return st }
	beforeSwitch func(*pebblestore.Store)
)

// storeLookup is the opener of the store's own lookup, for the sequencer's resume
// step.
func storeLookup(st *pebblestore.Store) func() (resume.Lookup, error) {
	return func() (resume.Lookup, error) {
		lk, err := st.NewLookup()
		if err != nil {
			return nil, err
		}
		return lk, nil
	}
}

// ingestConfig is what serve was told about ingest.
type ingestConfig struct {
	// clusterName is the configured cluster name when set; with set false, none
	// is configured.
	clusterName string
	set         bool
	// accept is --accept-new-cluster-name, and acceptGiven says it was given:
	// given and empty accepts that no name is set.
	accept      string
	acceptGiven bool
	// pullInterval is the objects receiver's pull interval.
	pullInterval time.Duration
	// nodeInterval is the report interval the node agents are configured with.
	nodeInterval time.Duration
	// dependencyExpiry and instanceTTL are the TTLs of the service graph and the
	// span count producers.
	dependencyExpiry, instanceTTL time.Duration
	// retain is --retain on: the sequencer retains the store once a day.
	retain bool
	// backup is what serve was told about the backup.
	backup backupConfig
}

// serveStoreOptions are the options serve opens its store with: those of the
// store every command opens ([storeOptions]), with the authority of the
// producers of the configured cluster name, or of "default" when none is set:
// the objects receiver's descriptions rank above the cluster receiver's. Rank is
// applied when the store folds, so it can change between boots without rewriting
// anything. With no name configured, the producers of the other cluster names a
// client sends are not ranked, and for them the cluster receiver's producer
// wins by the order of the names; that shows only in descriptions, and goes
// when ranking follows the producer's prefix.
//
// Retention runs in the background, so that a retention does not hold the
// writer for its whole pass, and obs, when not nil, is told of its phases. The
// layers' retention is storeOptions', whether or not serve retains: a pass an
// earlier boot left is resumed all the same.
func serveStoreOptions(clusterName string, set bool, obs pebblestore.RetainObserver) pebblestore.Options {
	o := storeOptions()
	o.Retention = pebblestore.Background
	o.Observer = obs
	o.Policy.Rank = serveRank(clusterName, set)
	return o
}

// serveRank is the authority of the producers of the configured cluster name, or
// of "default" when none is set, that serve opens its store with. The read-only
// tools carry a digest of it, so they are given the same map.
func serveRank(clusterName string, set bool) map[lifecycle.Producer]int {
	c := sequencer.DefaultClusterName
	if set {
		c = clusterName
	}
	return map[lifecycle.Producer]int{
		lifecycle.Producer("k8sobjects/" + c): 2,
		lifecycle.Producer("k8scluster/" + c): 1,
	}
}

// switchSink is the receiver's sink, which boot switches: a gate that answers
// 503 while the store opens or when ingest is off, and then the sequencer.
type switchSink struct{ cur atomic.Pointer[sinkOf] }

type sinkOf struct{ otlphttp.Sink }

func (w *switchSink) set(s otlphttp.Sink) { w.cur.Store(&sinkOf{s}) }

func (w *switchSink) Accept(ctx context.Context, d capture.LogsData) error {
	return w.cur.Load().Accept(ctx, d)
}

func (w *switchSink) AcceptPartial(ctx context.Context, d capture.LogsData) (otlphttp.Partial, error) {
	s := w.cur.Load().Sink
	if p, ok := s.(otlphttp.PartialSink); ok {
		return p.AcceptPartial(ctx, d)
	}
	return otlphttp.Partial{}, s.Accept(ctx, d)
}

// AcceptMetrics hands the metrics to the current sink when it takes them: the gate
// and the sequencer both do. A sink that does not is reachable only in tests.
func (w *switchSink) AcceptMetrics(ctx context.Context, d otlpmetrics.Data) (otlphttp.Partial, error) {
	if m, ok := w.cur.Load().Sink.(otlphttp.MetricsSink); ok {
		return m.AcceptMetrics(ctx, d)
	}
	return otlphttp.Partial{}, errors.New("the current sink takes no metrics")
}

// receiverRoutes are the two routes of the receiver, both on the one in-flight
// budget: OTLP logs at POST /v1/logs and OTLP metrics at POST /v1/metrics. They
// are served on every listen address, as the receiver has no authentication on
// any: a non-loopback address is warned about by the server.
func receiverRoutes(sink *switchSink, budget *otlphttp.Budget) []server.Route {
	return []server.Route{{
		Pattern: "POST " + otlphttp.Path,
		Handler: otlphttp.Handler(sink, otlphttp.Options{Budget: budget}),
	}, {
		Pattern: "POST " + otlphttp.MetricsPath,
		Handler: otlphttp.MetricsHandler(sink, otlphttp.Options{Budget: budget}),
	}}
}

// retainLog is the store's retention observer, which logs the phases of each
// pass, and the retention check's store, which notes the horizon each call asks
// for so that the publication can name the layers' horizons. Its methods are
// called by the store's goroutines and the sequencer's loop.
type retainLog struct {
	log     *slog.Logger
	offsets [4]time.Duration
	keep    [4]bool
	st      sequencer.Retainer // set once the store is open

	mu        sync.Mutex
	asked     time.Time
	published map[uint64]time.Time
	rewritten map[uint64]time.Time
}

func newRetainLog(log *slog.Logger, o pebblestore.Options) *retainLog {
	return &retainLog{
		log: log, offsets: o.Offsets, keep: o.Keep,
		published: map[uint64]time.Time{}, rewritten: map[uint64]time.Time{},
	}
}

// Retain asks the store for a retention to horizon.
func (r *retainLog) Retain(ctx context.Context, horizon time.Time) error {
	r.mu.Lock()
	r.asked = horizon
	r.mu.Unlock()
	return r.st.Retain(ctx, horizon)
}

// layer is the horizon a retention to h asks of layer l, or "kept".
func (r *retainLog) layer(l catalog.Layer, h time.Time) any {
	if r.keep[l-catalog.L0] {
		return "kept"
	}
	return h.Add(-r.offsets[l-catalog.L0])
}

func (r *retainLog) Published(gen uint64, at time.Time) {
	r.mu.Lock()
	h := r.asked
	for g := range r.published {
		if g < gen {
			delete(r.published, g)
			delete(r.rewritten, g)
		}
	}
	r.published[gen] = at
	r.mu.Unlock()
	r.log.Info("retention published", "gen", gen, "L1", r.layer(catalog.L1, h), "L2", r.layer(catalog.L2, h), "at", at)
}

func (r *retainLog) RewriteEnded(gen uint64, at time.Time) {
	r.mu.Lock()
	r.rewritten[gen] = at
	pub, ok := r.published[gen]
	r.mu.Unlock()
	if !ok {
		r.log.Info("rewrite ended", "gen", gen, "resumed", true)
		return
	}
	r.log.Info("rewrite ended", "gen", gen, "after", at.Sub(pub))
}

func (r *retainLog) SettleEnded(gen uint64, at time.Time) {
	r.mu.Lock()
	pub, published := r.published[gen]
	rew, rewritten := r.rewritten[gen]
	delete(r.published, gen)
	delete(r.rewritten, gen)
	r.mu.Unlock()
	attrs := []any{"gen", gen}
	if published {
		attrs = append(attrs, "after", at.Sub(pub))
	} else {
		attrs = append(attrs, "resumed", true)
	}
	if rewritten {
		attrs = append(attrs, "settle", at.Sub(rew))
	}
	r.log.Info("settle ended", attrs...)
}

// serveRetainConfig is the retention check of serve: L2's window, the offset of
// L2 in the options the store was opened with, the wait for a pass, and the
// fixed interval.
func serveRetainConfig(st *pebblestore.Store, o pebblestore.Options, rl *retainLog) *sequencer.RetainConfig {
	return &sequencer.RetainConfig{
		Store:  rl,
		Wait:   st.Instrument().WaitRetained,
		Window: retainWindow,
		Offset: o.Offsets[catalog.L2-catalog.L0],
		Every:  retainEvery,
	}
}

// statusRoute answers GET /status with the retention check's last snapshot,
// which the sequencer published, and the resume step's counts: the handler never
// calls the store.
type statusRoute struct {
	retain bool
	setBy  string // retainByDefault, retainByFlag or retainByEnv
	seq    atomic.Pointer[sequencer.Sequencer]
	backup *backupReporter // nil: the backup is off
}

// nodeAgentStatus is the node agents' status: on with the interval expected of the
// agents, the counters, and the latest host event of each cluster that has had
// one, or off while ingest is not running.
type nodeAgentStatus struct {
	Mode     string                        `json:"mode"`
	Interval string                        `json:"interval,omitempty"`
	Counters map[string]int64              `json:"counters,omitempty"`
	Clusters map[string]nodeAgentClusterOf `json:"clusters,omitempty"`
}

// nodeAgentClusterOf is one cluster's latest host event. Silent is true once more
// than three intervals have passed since, as of the latest flush tick.
type nodeAgentClusterOf struct {
	LastHostEvent time.Time `json:"last_host_event"`
	Silent        bool      `json:"silent"`
}

// nodeAgentOf is the node agents' status of the sequencer, off when ingest is not
// running.
func nodeAgentOf(seq *sequencer.Sequencer) nodeAgentStatus {
	if seq == nil {
		return nodeAgentStatus{Mode: "off"}
	}
	st := seq.NodeAgentStatus()
	out := nodeAgentStatus{
		Mode: "on", Interval: st.Interval.String(), Counters: st.Counters,
		Clusters: make(map[string]nodeAgentClusterOf, len(st.Clusters)),
	}
	for name, c := range st.Clusters {
		out.Clusters[name] = nodeAgentClusterOf{LastHostEvent: c.LastHostEvent, Silent: c.Silent}
	}
	return out
}

// metricsStatus is the metrics path's status: on with the TTLs of the two
// producers, the counters, and the latest points of each cluster that has had
// any, or off while ingest is not running.
type metricsStatus struct {
	Mode             string                      `json:"mode"`
	DependencyExpiry string                      `json:"dependency_expiry,omitempty"`
	InstanceTTL      string                      `json:"instance_ttl,omitempty"`
	Counters         map[string]int64            `json:"counters,omitempty"`
	Clusters         map[string]metricsClusterOf `json:"clusters,omitempty"`
}

// metricsClusterOf is one cluster's latest service graph and span count points,
// as ingest instants. Silent is true once more than three connector flushes have
// passed since the latest service graph point, as of the latest flush tick, and
// Series is the number of series the cluster's service graph translator held.
type metricsClusterOf struct {
	LastServiceGraphPoint *time.Time `json:"last_service_graph_point,omitempty"`
	LastSpanCountPoint    *time.Time `json:"last_span_count_point,omitempty"`
	Silent                bool       `json:"silent"`
	Series                int        `json:"series"`
}

// metricsOf is the metrics status of the sequencer, off when ingest is not
// running.
func metricsOf(seq *sequencer.Sequencer) metricsStatus {
	if seq == nil {
		return metricsStatus{Mode: "off"}
	}
	st := seq.MetricsStatus()
	out := metricsStatus{
		Mode: "on", DependencyExpiry: st.DependencyExpiry.String(), InstanceTTL: st.InstanceTTL.String(),
		Counters: st.Counters, Clusters: make(map[string]metricsClusterOf, len(st.Clusters)),
	}
	for name, c := range st.Clusters {
		out.Clusters[name] = metricsClusterOf{
			LastServiceGraphPoint: orNull(c.LastServiceGraphPoint), LastSpanCountPoint: orNull(c.LastSpanCountPoint),
			Silent: c.Silent, Series: c.Series,
		}
	}
	return out
}

// resumeStatus is the resume step's status: on with its counters, disabled when
// the sequencer was given no lookup, or off while ingest is not running.
type resumeStatus struct {
	Mode     string           `json:"mode"`
	Counters map[string]int64 `json:"counters,omitempty"`
}

// resumeOf is the resume status of the sequencer, nil when ingest is not running.
func resumeOf(seq *sequencer.Sequencer) resumeStatus {
	if seq == nil {
		return resumeStatus{Mode: "off"}
	}
	st, on := seq.ResumeStats()
	if !on {
		return resumeStatus{Mode: "disabled"}
	}
	out := resumeStatus{Mode: "on", Counters: map[string]int64{}}
	for k, v := range st.Counters() {
		out.Counters["sequencer.resume_"+strings.ReplaceAll(k, ".", "_")] = v
	}
	return out
}

// retentionOff is the retention status of serve --retain off.
type retentionOff struct {
	Mode  string `json:"mode"`
	SetBy string `json:"set_by"`
}

// retentionOn is the retention status of serve --retain on.
type retentionOn struct {
	Mode          string                `json:"mode"`
	SetBy         string                `json:"set_by"`
	CheckedAt     *time.Time            `json:"checked_at"`
	Horizons      map[string]*time.Time `json:"horizons"`
	Agreement     string                `json:"agreement"`
	PendingSince  *time.Time            `json:"pending_since"`
	LastSettleEnd *time.Time            `json:"last_settle_end"`
	StoppedBy     string                `json:"stopped_by"`
	Counters      map[string]uint64     `json:"counters"`
}

func orNull(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (s *statusRoute) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	var body any = retentionOff{Mode: "off", SetBy: s.setBy}
	if s.retain {
		on := retentionOn{Mode: "on", SetBy: s.setBy, StoppedBy: "ingest is not running"}
		if seq := s.seq.Load(); seq != nil {
			st, _ := seq.RetentionStatus()
			on = retentionOn{
				Mode:          "on",
				SetBy:         s.setBy,
				CheckedAt:     orNull(st.CheckedAt),
				Horizons:      map[string]*time.Time{},
				Agreement:     st.Agreement.String(),
				PendingSince:  orNull(st.PendingSince),
				LastSettleEnd: orNull(st.LastSettleEnd),
				StoppedBy:     st.StoppedBy,
				Counters: map[string]uint64{
					"serve.retain_calls": st.Calls, "serve.retain_skipped": st.Skipped,
					"serve.retain_capped": st.Capped, "serve.retain_catchups": st.CatchUps,
				},
			}
			for l := catalog.L0; l <= catalog.L3; l++ {
				on.Horizons[l.String()] = orNull(st.Horizons[l-catalog.L0])
			}
		}
		body = on
	}
	w.Header().Set("Content-Type", "application/json")
	seq := s.seq.Load()
	_ = json.NewEncoder(w).Encode(map[string]any{"retention": body, "resume": resumeOf(seq), "nodeagent": nodeAgentOf(seq), "metrics": metricsOf(seq), "backup": s.backup.status()})
}

// ingest is what boot opened for ingest: the store, and the sequencer with its
// own context once it runs.
type ingest struct {
	st      *pebblestore.Store
	seq     *sequencer.Sequencer
	cancel  context.CancelFunc
	runDone chan error
	backup  *backupRun // nil when the backup is off
}

// startIngest opens the store in the data directory, decides from the cluster
// name it records whether ingest may start, records the name when it should,
// starts the sequencer, and switches the receiver to it. It returns what must be
// closed at shutdown, nil when nothing is open. Whatever keeps ingest off is
// logged and answered by the receiver with 503 and Retry-After: 30; the server
// stays up. stopped is closed when the server has ended; boot that finds it
// ended, or ctx done, after the store opened closes the store and starts
// nothing.
//
// The read-only tools are handed the store as soon as it is open, whatever
// keeps ingest off after that: reading needs no cluster name. When the store
// cannot be opened they answer with the receiver's sentence.
func startIngest(ctx context.Context, dir *datadir.Dir, ic ingestConfig, sink *switchSink, status *statusRoute, reads *mcphttp.Server, log *slog.Logger, stopped <-chan struct{}) *ingest {
	start := time.Now()
	rl := newRetainLog(log, storeOptions())
	opts := serveStoreOptions(ic.clusterName, ic.set, rl)
	st, err := openStore(dir.Store, opts)
	if err != nil {
		log.Error("ingest is off: the store could not be opened", "path", dir.Store, "err", err)
		msg := "ingest is off: the store could not be opened: " + err.Error() + "; see the server's log"
		sink.set(gateSink{message: msg})
		reads.Unavailable(msg)
		return nil
	}
	log.Info("store opened", "path", dir.Store, "took", time.Since(start))
	in := &ingest{st: st}
	if err := reads.Attach(readerFor(st)); err != nil {
		log.Error("the tools cannot read the store", "err", err)
	}
	select {
	case <-ctx.Done():
		return in // stopping: the store is closed with nothing started
	case <-stopped:
		return in
	default:
	}

	name, recordedSet, present, err := st.ClusterName()
	if err != nil {
		log.Error("ingest is off: the store's cluster name could not be read", "err", err)
		sink.set(gateSink{message: "ingest is off: the store's cluster name could not be read: " + err.Error()})
		return in
	}
	d := clustername.Decide(clustername.Input{
		Recorded: name, RecordedSet: recordedSet, Present: present, LastSeq: st.LastSeq(),
		Configured: ic.clusterName, ConfiguredSet: ic.set,
		Accept: ic.accept, AcceptGiven: ic.acceptGiven,
	})
	if d.Warning != "" {
		log.Warn(d.Warning)
	}
	if d.IngestOff {
		log.Error(d.Message)
		sink.set(gateSink{message: d.Message})
		return in
	}
	if d.Message != "" {
		log.Warn(d.Message)
	}
	if d.Record {
		// Synced, and before anything is written under the name.
		if err := st.RecordClusterName(context.Background(), ic.clusterName, ic.set); err != nil {
			log.Error("ingest is off: the cluster name could not be recorded", "err", err)
			sink.set(gateSink{message: "ingest is off: the cluster name could not be recorded: " + err.Error()})
			return in
		}
		log.Info("recorded the cluster name", "cluster_name", showName(ic))
	}

	// The backup, when asked for, takes what the store commits and the retentions
	// it logs, and is stopped last.
	in.backup = startBackup(ctx, dir, st, ic, opts, status, log)
	var retain *sequencer.RetainConfig
	if ic.retain {
		rl.st = in.backup.retainer(st, st)
		retain = serveRetainConfig(st, opts, rl)
	}
	lookup := lookupFor(st)
	if lookup == nil {
		log.Error("resume is disabled: the sequencer has no lookup of the store; after a restart every live object is asserted again from its creation")
	}
	seq, err := sequencer.New(sequencer.Config{
		Store:          in.backup.writer(st, writerFor(st)),
		ClusterName:    ic.clusterName,
		ClusterNameSet: ic.set,
		PullInterval:   ic.pullInterval,
		NodeInterval:   ic.nodeInterval,

		DependencyExpiry: ic.dependencyExpiry,
		InstanceTTL:      ic.instanceTTL,
		Logger:           log,
		Retain:           retain,
		Lookup:           lookup,
	})
	if err != nil {
		log.Error("ingest is off: the sequencer could not be made", "err", err)
		sink.set(gateSink{message: "ingest is off: " + err.Error()})
		return in
	}
	// The sequencer's context is its own: the signal that stops the server must
	// not stop it before the server has answered what it is serving.
	seqCtx, cancel := context.WithCancel(context.Background())
	in.seq, in.cancel, in.runDone = seq, cancel, make(chan error, 1)
	go func() { in.runDone <- seq.Run(seqCtx) }()
	status.seq.Store(seq)
	if beforeSwitch != nil {
		beforeSwitch(st)
	}
	sink.set(seq)
	log.Info("ingest is on", "cluster_name", showName(ic), "took", time.Since(start))
	if ic.retain {
		log.Info("retention is on: once a day, L2 keeps 30 days, L1 90 days, and L0 is kept",
			"check_every", retainEvery, "set_by", status.setBy)
	}
	return in
}

func showName(ic ingestConfig) string {
	if !ic.set {
		return "(none set: " + sequencer.DefaultClusterName + ")"
	}
	return ic.clusterName
}

// stop ends ingest after the server has stopped: the sequencer answers what it
// has claimed and writes the coalescer's absorbed refreshes, within
// sequencerStopTimeout; past it, its context is cancelled and Run is waited
// for. Run returns only once its retention waiter has, so no Write, Retain or
// wait is in flight when the store closes. Then the store is closed, which
// stops a retention pass at a chunk boundary, or between two ranges of its
// settle, and leaves its marker for the next open to resume. The wait for Run
// is bounded by one commit, one retention chunk, or one settle sub-range
// (seconds) [estimate]. Each step's time is logged. A stop that timed out is
// not an error: the stored data is valid, and only absorbed refreshes may be
// unwritten.
//
// closeStore is false when a call of the read-only tools may still be reading
// the store: the store must not be closed under a read. It is then left open,
// with a line in the log, for the process to end with.
func (in *ingest) stop(log *slog.Logger, closeStore bool) {
	if in == nil {
		return
	}
	if in.seq != nil {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), sequencerStopTimeout)
		err := in.seq.Stop(ctx)
		cancel()
		if err != nil {
			log.Warn("the sequencer did not stop in time; it is cancelled, and absorbed refreshes may be unwritten",
				"timeout", sequencerStopTimeout, "err", err)
			in.cancel()
		}
		if err := <-in.runDone; err != nil && !errors.Is(err, context.Canceled) {
			log.Error("the sequencer ended with an error", "err", err)
		}
		in.cancel()
		log.Info("sequencer stopped", "took", time.Since(start))
	}
	if !closeStore {
		log.Warn("the store was not closed: a call of the tools may still be reading it")
		in.backup.stop()
		return
	}
	start := time.Now()
	if err := in.st.Close(); err != nil {
		log.Error("the store did not close cleanly", "err", err)
	}
	log.Info("store closed", "took", time.Since(start))
	// The backup is cut once more after the store closes: the cut reads only the
	// backup's own files, and a kill during it leaves a closed store.
	in.backup.stop()
}

const ingestHelp = `
Ingest

  With a data directory, serve opens the store in it and takes OpenTelemetry
  logs at POST /v1/logs: the Kubernetes objects receiver's pods and nodes, and
  the cluster receiver's entity events. It takes metrics at POST /v1/metrics
  too (see Services and their calls). A request is answered 200 only once
  its records are committed to disk. While the store opens, and whenever
  ingest is off, the receiver answers 503 with Retry-After and says why; health
  is served all the same.

  --cluster-name (or $TOPOSHIFT_CLUSTER_NAME) names the cluster the logs come
  from. With a name, a request whose k8s.cluster.name differs is refused. With
  none, each request's k8s.cluster.name names its cluster, or "default". An
  empty value is no name. The store records the name it was started with.
  When the configured name differs from the recorded one, ingest stays off:
  records under the old name stay, and their references lapse by their TTL.
  To accept the change, start once with
  --accept-new-cluster-name=<the configured name> (empty when no name is set).
  The flag is for one boot, and has no environment twin; a value naming
  another name is ignored with a warning.

  A store that keeps failing every write stays so until serve is restarted:
  the receiver answers 503 and the log says why. On a stop, serve finishes the
  requests in flight, writes what the coalescer holds, and closes the store.

Retention

  The default of this build is --retain {default}; $TOPOSHIFT_RETAIN sets it
  too, and the flag wins. GET /status says which was used, and so does the boot
  log, on its retention line (with retention on, once ingest is on).

  --retain on discards old history once a day, in the background: L2 (pods,
  nodes) keeps 30 days, L1 (hosts) 90 days, and L0 is kept. A retention runs
  once a day, so the store holds up to a day more:
  L2 up to 31 days, L1 up to 91. The L1 default passed its growth check, a
  bench run at 20 hosts; none at full size has been run.
  --retain off asks for no retention, and nothing more is discarded, though a
  pass an earlier boot started still completes below the horizon it already
  published. The store then grows for as long as serve runs. Any other value
  is refused.
  L3 (services and their calls) is retained with L2.

  This server's clock decides what is old, so it is checked against the
  clocks of the producers on other hosts: the times their records carry must
  agree with it within a day. While they disagree (a clock that jumped, or a
  collector replaying old records), nothing is discarded, and the log warns,
  then errs after a day. A store never retained waits for that agreement
  before its first retention; with no producer clock to compare (only records
  stamped on receipt), a store that fell behind catches up two days per day
  of uptime. A collector on this host shares its clock, and confirms nothing.

  A store filled by replay, or by serve --retain off, loses its records older
  than 30 days (90 for L1) at its first retention, once the clocks agree.

  An error from a retention stops retention until serve is restarted, and the
  log says so at every check. A store that has latched a failed commit also
  refuses ingest (503) until the restart; a retention refused as invalid does
  not, and ingest goes on.

  GET /status answers the retention's state as JSON: its mode and what set it
  (set_by: default, flag or environment), each layer's horizon, the clock
  agreement, the pass pending, the last settle, and why retention stopped, if
  it did.

Backup

  --backup on (or $TOPOSHIFT_BACKUP=on) keeps a backup of the records the store
  commits in backup/ of the data directory, and --backup off, the default,
  keeps none: nothing is written, started or logged for it. It copies the
  committed records, not the store's files, and nothing derived from them. It
  is cut every --backup-every (or $TOPOSHIFT_BACKUP_EVERY; default 1h, a whole
  number of minutes from 5m to 24h), and once more at a stop, after the store
  closes, within 5 seconds. A cut writes only when a record or a retention was
  committed since the last one. A commit waits while a cut is made.

  A backup that fails never fails a commit or a read: it is logged, at start
  and then every hour, and GET /status answers "backup": its mode (off, on,
  broken or refused), what set it, the reason, the interval, and, for the
  local backup, the generation and the Seq it stands at, the time of its last
  cut and the error of its last attempt, its files and the size of its
  manifest, and what it retired last and the classes it does not retire.

  A backup starts with the store's first record, so --backup on must be given
  from the first boot of a data directory: a store that already holds records
  is refused, and so is a store that ends before its backup, and a store whose
  cluster name differs from the backup's. A store that ends after its backup
  (a stop whose last cut failed, or a crash), or whose retention was applied
  after the last cut, leaves the backup broken: it ends where it stood, and this
  build does not repair it.

  With retention on, after each retention the backup drops the files whose
  records lie more than a day behind their layer's horizon. L0 and the hosts
  are kept, and so is a class that holds a record with no TTL or a TTL above
  23h55m, which /status reports. The backup is in the data directory, on the
  volume of the store, so it does not survive the loss of the volume, and this
  build has no command that restores from it.

Resume

  After a restart, a reset or a long silence, ingest times each object it sees
  again from the store's last record of it, so that history is not rewritten
  from the object's creation. GET /status also answers "resume": its mode (on,
  or disabled, which the log reports as an error) and its counters: records
  new, raised to the stored record, changed (timed when observed), dropped
  after a stored delete, unknown (by reason: walk bound, baseline only, below
  the horizon, read error), and later records raised with them.

Node agents

  The cluster collector's pipeline may also carry the host entity events of
  node-level collectors (deploy/otel-collector/nodeagent.yaml), which place each
  Kubernetes node on its host. The agents send only to the cluster collector,
  never to serve. The producer is nodeagent/<cluster name>, and every event's
  record lives for three times --node-interval (or $TOPOSHIFT_NODE_INTERVAL;
  default 5m, a whole number of seconds from 1m to 1h), which must be the
  interval the agents report at: an event that reports another is stored all the
  same and counted. An event with no usable host.id is not stored.

  GET /status also answers "nodeagent": its counters, and for each cluster the
  time of its latest host event. A counter named refused_* is a record, or a part
  of one, that was not stored; substituted_* is a record that was stored with
  something put in place of what it gave (the receipt time for the agent's clock,
  no boot id); skipped_* is a record set aside. legacy_entity_events counts
  entity events in the older form, from either receiver, which are never
  translated: the collector was started without the feature gate. The log warns
  of the refused and legacy events at most once a minute for each cluster, and
  once each time a cluster's host events stop for three intervals.

Services and their calls

  The trace collector (deploy/otel-collector/README.md) sends two metrics to
  POST /v1/metrics: the service graph connector's request counter, which says
  that one service called another, and the count connector's span counter,
  which says that an instance of a service ran. The body is protobuf
  (application/x-protobuf), gzip or not; JSON is answered 415, because JSON
  metrics are not received. The same request limits, the same cluster name
  rules, and the same single in-flight budget apply as for logs. The producers
  are svcgraph/<cluster name> and spancount/<cluster name>.

  A call between two services stays until its last sighting plus
  --dependency-expiry (or $TOPOSHIFT_DEPENDENCY_EXPIRY; default 6h), and an
  instance until its last span count plus --instance-ttl (or
  $TOPOSHIFT_INSTANCE_TTL; default 6h); each is a whole number of seconds from
  2m to 8784h, and an end shows as liveness_expiry. A counter that does not
  rise is not a call. No count, rate or latency is stored.

  GET /status also answers "metrics": the counters (skipped_* is a point set
  aside, refused_* a part of a record that was not stored), and for each cluster
  when its latest service graph point and span count point arrived, whether it is
  silent, and the series held. The log warns, at most once a minute for each
  cluster, when the table of call series is full and of span counts with no
  service.instance.id (which write nothing), and once each time a cluster's
  service graph points stop for three minutes.

  Memory: each cluster name holds up to 200,000 call series, which is
  tens of MB per cluster name, not measured.
  Without --cluster-name, a sender that reaches the receiver directly, or
  through a collector whose resource processor was deleted, chooses up to 16
  cluster names, which also take the slots the logs route needs; set
  --cluster-name where senders are not trusted.

  The receiver has no authentication: anything that reaches it can add services,
  instances and calls, and bridge an instance to any pod whose uid it knows, so
  restrict who reaches it with the network. It serves both routes on every
  listen address; the read-only tools are served only when it listens on
  loopback.
`
