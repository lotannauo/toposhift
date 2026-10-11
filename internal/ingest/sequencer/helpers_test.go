package sequencer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// t0 is the start of every scenario's ingest clock.
var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

// ---- OTLP values ----

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

// val converts plain Go values to an OTLP value, with map keys sorted.
func val(v any) capture.AnyValue {
	switch x := v.(type) {
	case string:
		return sv(x)
	case int:
		return iv(int64(x))
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
	panic(fmt.Sprintf("val: %T", v))
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ---- objects receiver records ----

type pod struct {
	uid, name, node string
	created         time.Time
	scheduled       time.Time // the PodScheduled time; zero for none
	phase           string
}

func (p pod) object() map[string]any {
	status := map[string]any{"phase": p.phase}
	if !p.scheduled.IsZero() {
		status["conditions"] = []any{map[string]any{"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(p.scheduled)}}
	}
	spec := map[string]any{}
	if p.node != "" {
		spec["nodeName"] = p.node
	}
	meta := map[string]any{"uid": p.uid, "name": p.name, "namespace": "default"}
	if !p.created.IsZero() {
		meta["creationTimestamp"] = ts(p.created)
	}
	return map[string]any{"kind": "Pod", "apiVersion": "v1", "metadata": meta, "spec": spec, "status": status}
}

type node struct {
	uid, name string
	created   time.Time
}

func (n node) object() map[string]any {
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": n.uid, "name": n.name, "creationTimestamp": ts(n.created)},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "lastTransitionTime": ts(n.created),
		}}},
	}
}

func objRec(observed time.Time, body any, resource string) capture.LogRecord {
	b := val(body)
	return capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()),
		Body:                 &b,
		Attributes:           []capture.KeyValue{{Key: "k8s.resource.name", Value: sv(resource)}},
	}
}

func pullPod(observed time.Time, p pod) capture.LogRecord {
	return objRec(observed, p.object(), "pods")
}

func pullNode(observed time.Time, n node) capture.LogRecord {
	return objRec(observed, n.object(), "nodes")
}

func watchPod(observed time.Time, typ string, p pod) capture.LogRecord {
	return objRec(observed, map[string]any{"type": typ, "object": p.object()}, "pods")
}

func watchNode(observed time.Time, typ string, n node) capture.LogRecord {
	return objRec(observed, map[string]any{"type": typ, "object": n.object()}, "nodes")
}

// ---- cluster receiver entity events ----

func kvlist(m map[string]string) capture.AnyValue {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	l := &capture.KeyValueList{}
	for _, k := range keys {
		l.Values = append(l.Values, capture.KeyValue{Key: k, Value: sv(m[k])})
	}
	return capture.AnyValue{KVListValue: l}
}

// entity is an entity event with a report interval of five minutes.
func entity(event, typ, idKey, id string, tm time.Time, desc map[string]string) capture.LogRecord {
	lr := capture.LogRecord{EventName: event, TimeUnixNano: capture.Uint64(tm.UnixNano())}
	lr.Attributes = append(lr.Attributes,
		capture.KeyValue{Key: "entity.id", Value: kvlist(map[string]string{idKey: id})},
		capture.KeyValue{Key: "entity.type", Value: sv(typ)},
	)
	if event == "entity.state" {
		lr.Attributes = append(lr.Attributes,
			capture.KeyValue{Key: "entity.report.interval", Value: iv(300)},
			capture.KeyValue{Key: "entity.description", Value: kvlist(desc)},
		)
	}
	return lr
}

func nsState(uid, name string, tm time.Time) capture.LogRecord {
	return entity("entity.state", "k8s.namespace", "k8s.namespace.uid", uid, tm, map[string]string{"k8s.namespace.name": name})
}

func nsDelete(uid string, tm time.Time) capture.LogRecord {
	return entity("entity.delete", "k8s.namespace", "k8s.namespace.uid", uid, tm, nil)
}

func podState(p pod, tm time.Time) capture.LogRecord {
	return entity("entity.state", "k8s.pod", "k8s.pod.uid", p.uid, tm, map[string]string{
		"k8s.pod.name": p.name, "k8s.namespace.name": "default", "k8s.node.name": p.node,
		"k8s.pod.phase": p.phase, "pod.creation_timestamp": ts(p.created),
	})
}

func nodeState(n node, tm time.Time) capture.LogRecord {
	return entity("entity.state", "k8s.node", "k8s.node.uid", n.uid, tm, map[string]string{
		"k8s.node.name": n.name, "node.creation_timestamp": ts(n.created), "k8s.node.condition_ready": "true",
	})
}

// ---- requests ----

// logs is a request of one resource; a non-empty cluster sets its
// k8s.cluster.name.
func logs(cluster string, recs ...capture.LogRecord) capture.LogsData {
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{resource(cluster, recs...)}}
}

func resource(cluster string, recs ...capture.LogRecord) capture.ResourceLogs {
	var res capture.Resource
	if cluster != "" {
		res.Attributes = []capture.KeyValue{{Key: "k8s.cluster.name", Value: sv(cluster)}}
	}
	return capture.ResourceLogs{Resource: res, ScopeLogs: []capture.ScopeLogs{{LogRecords: recs}}}
}

// ---- the clock and the tick ----

type fakeClock struct{ ns atomic.Int64 }

func newClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.set(t)
	return c
}

func (c *fakeClock) set(t time.Time) { c.ns.Store(t.UnixNano()) }
func (c *fakeClock) now() time.Time  { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Now() time.Time  { return c.now() }

// ---- the store ----

// fakeStore writes to an inner store and keeps what each successful write
// committed. inject, when set, decides each write before it reaches the inner
// store: whether to write it, and the error to return.
type fakeStore struct {
	inner interface {
		sequencer.Writer
	}
	mu        sync.Mutex
	writes    int
	committed [][]store.Record
	inject    func(i int, batch []store.Record) (write bool, err error)
}

func newMem(t testing.TB) *memstore.Store {
	t.Helper()
	m, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *fakeStore) Write(ctx context.Context, batch []store.Record) error {
	f.mu.Lock()
	i := f.writes
	f.writes++
	inject := f.inject
	f.mu.Unlock()
	write, injected := true, error(nil)
	if inject != nil {
		write, injected = inject(i, batch)
	}
	var err error
	if write {
		err = f.inner.Write(ctx, batch)
		if err == nil && len(batch) > 0 {
			f.mu.Lock()
			f.committed = append(f.committed, slices.Clone(batch))
			f.mu.Unlock()
		}
	}
	if injected != nil {
		return injected
	}
	return err
}

func (f *fakeStore) LastSeq() uint64 { return f.inner.LastSeq() }

func (f *fakeStore) LayerHorizon(l catalog.Layer) store.Horizon { return f.inner.LayerHorizon(l) }

func (f *fakeStore) records() []store.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.Record
	for _, b := range f.committed {
		out = append(out, b...)
	}
	return out
}

func (f *fakeStore) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

// ---- the harness ----

type harness struct {
	t      *testing.T
	clock  *fakeClock
	st     *fakeStore
	seq    *sequencer.Sequencer
	tickCh chan time.Time
	ticked chan struct{}
	runErr chan error
	cancel context.CancelFunc
	hooks  sequencer.Hooks
}

type option func(*sequencer.Config, *harness)

func withConfig(f func(*sequencer.Config)) option {
	return func(c *sequencer.Config, _ *harness) { f(c) }
}

func withHooks(h sequencer.Hooks) option {
	return func(_ *sequencer.Config, hs *harness) { hs.hooks = h }
}

// newHarness makes a sequencer over st at t0, with the fake clock and tick, and
// does not start it.
func newHarness(t *testing.T, st *fakeStore, opts ...option) *harness {
	t.Helper()
	h := &harness{
		t: t, clock: newClock(t0), st: st,
		tickCh: make(chan time.Time), ticked: make(chan struct{}),
	}
	cfg := sequencer.Config{
		Store:  st,
		Now:    h.clock.Now,
		Tick:   func(time.Duration) <-chan time.Time { return h.tickCh },
		Logger: slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, o := range opts {
		o(&cfg, h)
	}
	s, err := sequencer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.seq = s
	after := h.hooks.AfterTick
	h.hooks.AfterTick = func() {
		if after != nil {
			after()
		}
		h.ticked <- struct{}{}
	}
	sequencer.SetHooks(s, h.hooks)
	// Registered first, so that it runs last: a gate made after the harness is
	// opened before the loop is waited for.
	t.Cleanup(func() {
		if h.cancel != nil {
			h.cancel()
			<-h.runErr
		}
	})
	return h
}

// start runs the loop; it is stopped when the test ends.
func (h *harness) start() *harness {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.runErr = make(chan error, 1)
	go func() { h.runErr <- h.seq.Run(ctx) }()
	return h
}

// send sends a request at the clock's instant and waits for its answer.
func (h *harness) send(d capture.LogsData) (otlphttp.Partial, error) {
	h.t.Helper()
	return h.seq.AcceptPartial(context.Background(), d)
}

// sendOK sends a request and fails the test unless it is answered 200 with
// nothing rejected.
func (h *harness) sendOK(d capture.LogsData) {
	h.t.Helper()
	p, err := h.send(d)
	if err != nil || p.Rejected != 0 {
		h.t.Fatalf("at %s: partial %+v, error %v", h.clock.now().Sub(t0), p, err)
	}
}

// tick sends a flush tick at the clock's instant and waits until it is handled.
func (h *harness) tick() {
	h.t.Helper()
	h.tickCh <- h.clock.now()
	<-h.ticked
}

// stop stops the sequencer and waits for Run to return.
func (h *harness) stop() {
	h.t.Helper()
	start := time.Now()
	if err := h.seq.Stop(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	if err := <-h.runErr; err != nil {
		h.t.Fatalf("Run: %v", err)
	}
	h.t.Logf("Stop and the end of Run took %s", time.Since(start))
	h.runErr <- nil // for the cleanup
}

// sendAsync sends a request on its own goroutine and returns its answer's
// channel.
func (h *harness) sendAsync(ctx context.Context, d capture.LogsData) <-chan answer {
	ch := make(chan answer, 1)
	go func() {
		p, err := h.seq.AcceptPartial(ctx, d)
		ch <- answer{p, err}
	}()
	return ch
}

type answer struct {
	p   otlphttp.Partial
	err error
}

// waitQueued waits until n requests wait on the queue.
func (h *harness) waitQueued(n int) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for sequencer.Queued(h.seq) != n {
		if time.Now().After(deadline) {
			h.t.Fatalf("%d requests queued, want %d", sequencer.Queued(h.seq), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func recv[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
	}
	var zero T
	return zero
}

// ---- the reference path ----

// reference is the path the sequencer is checked against, written apart from it:
// translate (the namespace events of a request first, then the rest by event
// name), check, coalesce in the same order, number, and keep. It has no
// detector: the scenarios it is used with refuse nothing.
type reference struct {
	t        testing.TB
	resolver *identity.Resolver
	objects  map[string]*k8sobjects.Translator
	cluster  map[string]*k8scluster.Translator
	co       *coalesce.Coalescer
	cfg      coalesce.Config
	seq      uint64
	written  []store.Record
}

func newReference(t testing.TB) *reference {
	r := &reference{t: t, resolver: identity.NewResolver(catalog.Default()), cfg: coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}}
	r.restart()
	return r
}

// restart makes the translators and the coalescer anew, as a restart does.
func (r *reference) restart() {
	r.objects = map[string]*k8sobjects.Translator{}
	r.cluster = map[string]*k8scluster.Translator{}
	co, err := coalesce.New(r.cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	r.co = co
}

func (r *reference) translators(name string) (*k8sobjects.Translator, *k8scluster.Translator) {
	if r.objects[name] == nil {
		o, err := k8sobjects.New(k8sobjects.Config{Producer: lifecycle.Producer("k8sobjects/" + name), Resolver: r.resolver, PullInterval: 5 * time.Minute})
		if err != nil {
			r.t.Fatal(err)
		}
		c, err := k8scluster.New(k8scluster.Config{Producer: lifecycle.Producer("k8scluster/" + name), Resolver: r.resolver, CollectionInterval: 5 * time.Minute})
		if err != nil {
			r.t.Fatal(err)
		}
		r.objects[name], r.cluster[name] = o, c
	}
	return r.objects[name], r.cluster[name]
}

func isNamespace(lr capture.LogRecord) bool {
	if lr.EventName != "entity.state" && lr.EventName != "entity.delete" {
		return false
	}
	typ := ""
	for _, kv := range lr.Attributes {
		if kv.Key == "entity.type" && kv.Value.StringValue != nil {
			typ = *kv.Value.StringValue
		}
	}
	return typ == "k8s.namespace"
}

func clusterOf(res capture.Resource) string {
	name := ""
	for _, kv := range res.Attributes {
		if kv.Key == "k8s.cluster.name" && kv.Value.StringValue != nil {
			name = *kv.Value.StringValue
		}
	}
	if name == "" {
		return "default"
	}
	return name
}

// request takes one request, received at the given instant.
func (r *reference) request(d capture.LogsData, received time.Time) {
	type slot struct {
		recs []store.Record
		bad  bool
	}
	var slots []*slot
	type item struct {
		res  capture.Resource
		lr   capture.LogRecord
		name string
	}
	var items []item
	for _, rl := range d.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				items = append(items, item{rl.Resource, lr, clusterOf(rl.Resource)})
				slots = append(slots, &slot{})
			}
		}
	}
	for pass := range 2 {
		for i, it := range items {
			if isNamespace(it.lr) != (pass == 0) {
				continue
			}
			o, c := r.translators(it.name)
			var recs []store.Record
			var err error
			if it.lr.EventName == "entity.state" || it.lr.EventName == "entity.delete" {
				var res k8scluster.Result
				res, err = c.Translate(it.res, it.lr, received)
				recs = res.Records
			} else {
				var res k8sobjects.Result
				res, err = o.Translate(it.res, it.lr)
				recs = res.Records
			}
			slots[i].recs, slots[i].bad = recs, err != nil
		}
	}
	var out []store.Record
	for _, s := range slots {
		if s.bad {
			continue
		}
		ok := true
		for _, rec := range s.recs {
			if rec.Validate() != nil || store.WritableProducer(rec.Producer) != nil {
				ok = false
			}
		}
		if !ok {
			continue
		}
		for _, rec := range s.recs {
			out = r.co.Add(rec, out)
		}
	}
	r.write(out)
}

func (r *reference) flush(now time.Time) {
	r.write(r.co.Flush(now, nil))
	r.co.Prune(now)
}

func (r *reference) flushAll() { r.write(r.co.FlushAll(nil)) }

func (r *reference) write(out []store.Record) {
	for i := range out {
		r.seq++
		out[i].Seq = r.seq
	}
	r.written = append(r.written, out...)
}

// ---- comparison ----

// diffRecords reports the first difference between two record sequences, field
// by field, or "" if they are equal.
func diffRecords(got, want []store.Record) string {
	for i := range min(len(got), len(want)) {
		if d := diffRecord(got[i], want[i]); d != "" {
			return fmt.Sprintf("record %d: %s\n got  %s\n want %s", i, d, show(got[i]), show(want[i]))
		}
	}
	if len(got) != len(want) {
		return fmt.Sprintf("%d records, want %d", len(got), len(want))
	}
	return ""
}

func diffRecord(a, b store.Record) string {
	switch {
	case a.Layer != b.Layer:
		return "layer"
	case a.Subject != b.Subject:
		return "subject"
	case a.Producer != b.Producer:
		return "producer"
	case !a.EventTime.Equal(b.EventTime):
		return "event time"
	case a.Seq != b.Seq:
		return "seq"
	case a.Kind != b.Kind:
		return "kind"
	case a.TTL != b.TTL:
		return "ttl"
	case !a.Through.Equal(b.Through):
		return "through"
	case string(a.Payload) != string(b.Payload):
		return "payload"
	case a.Boot != b.Boot:
		return "boot"
	case a.EventTimeBasis != b.EventTimeBasis:
		return "basis"
	}
	return ""
}

func show(r store.Record) string {
	sub := r.Subject.A.Type()
	if r.Subject.Kind == store.SubjectEdge {
		sub = catalog.EntityType(fmt.Sprintf("%s-%s->%s", r.Subject.A.Type(), r.Subject.Relation, r.Subject.B.Type()))
	}
	return fmt.Sprintf("{seq %d %s %s %s at %s through %s ttl %s basis %s}", r.Seq, r.Producer, sub, r.Kind,
		r.EventTime.Sub(t0), throughOf(r), r.TTL, r.EventTimeBasis)
}

func throughOf(r store.Record) string {
	if r.Through.IsZero() {
		return "-"
	}
	return r.Through.Sub(t0).String()
}

// errInjected is a failure of a write that is not a validation error.
var errInjected = errors.New("injected I/O failure")

// fixtureRecords are the log records of a capture file of the translators'
// fixtures.
func fixtureRecords(t testing.TB, path string) []capture.LogRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []capture.LogRecord
	for _, line := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		d, err := capture.DecodeLogs(line)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, rl := range d.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				out = append(out, sl.LogRecords...)
			}
		}
	}
	return out
}

func newClusterTranslator(t testing.TB) *k8scluster.Translator {
	t.Helper()
	tr, err := k8scluster.New(k8scluster.Config{
		Producer: "k8scluster/default", Resolver: identity.NewResolver(catalog.Default()), CollectionInterval: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// gate holds a goroutine at a seam until it is opened. It is opened when the
// test ends too, so that a failing test does not leave the loop held.
type gate struct {
	entered, release chan struct{}
	enterOnce, once  sync.Once
	released         atomic.Bool
}

func newGate(t testing.TB) *gate {
	g := &gate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

// hold is called at the seam: it says it was reached, and waits.
func (g *gate) hold() {
	g.enterOnce.Do(func() { close(g.entered) })
	<-g.release
}

func (g *gate) open() {
	g.once.Do(func() {
		g.released.Store(true)
		close(g.release)
	})
}
