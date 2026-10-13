package sequencer_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The resume step on the real path: the real translators, Sequencer, resume.Apply
// and a pebblestore (or a memstore, for parity), driven on the fake clock with a
// pull of the objects receiver and a report of the cluster receiver every five
// minutes and a flush tick every 30 seconds.

// ---- the cluster the receivers report ----

type rsNode struct {
	uid, name string
	created   time.Time // zero: the object has no creationTimestamp
}

type rsPod struct {
	uid, name, node string
	created         time.Time
	scheduled       time.Time
	phase           string
	ctrID           string    // a runtime id, "" for none
	ctrStarted      time.Time // the container's startedAt
	conds           []any     // more conditions
}

// rsWorld is what the cluster holds at a time: its nodes and pods, which a test
// changes between pulls.
type rsWorld struct {
	nodes []rsNode
	pods  []*rsPod
	// podsFirst lists the pods before the nodes in a pull.
	podsFirst bool
	// noCluster leaves out the cluster receiver's reports.
	noCluster bool
	// cluster is the k8s.cluster.name of every resource ("" for none).
	cluster string
}

func (n rsNode) object() map[string]any {
	meta := map[string]any{"uid": n.uid, "name": n.name}
	if !n.created.IsZero() {
		meta["creationTimestamp"] = ts(n.created)
	}
	return map[string]any{
		"kind": "Node", "apiVersion": "v1", "metadata": meta,
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(t0.Add(-24 * time.Hour))}}},
	}
}

func (p *rsPod) object() map[string]any {
	conds := []any{map[string]any{"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(p.scheduled)}}
	conds = append(conds, p.conds...)
	status := map[string]any{"phase": p.phase, "conditions": conds}
	if p.ctrID != "" {
		status["containerStatuses"] = []any{map[string]any{
			"name": "c", "containerID": "containerd://" + p.ctrID, "restartCount": 0,
			"state": map[string]any{"running": map[string]any{"startedAt": ts(p.ctrStarted)}},
		}}
	}
	return map[string]any{
		"kind": "Pod", "apiVersion": "v1",
		"metadata": map[string]any{"uid": p.uid, "name": p.name, "namespace": "default", "creationTimestamp": ts(p.created)},
		"spec":     map[string]any{"nodeName": p.node},
		"status":   status,
	}
}

// pull is one list of the objects receiver at the time.
func (w *rsWorld) pull(tm time.Time) capture.LogsData {
	var nodes, pods []capture.LogRecord
	for _, n := range w.nodes {
		nodes = append(nodes, pullNodeObj(tm, n.object()))
	}
	for _, p := range w.pods {
		pods = append(pods, objRec(tm, p.object(), "pods"))
	}
	recs := append(nodes, pods...)
	if w.podsFirst {
		recs = append(pods, nodes...)
	}
	return logs(w.cluster, recs...)
}

func pullNodeObj(tm time.Time, o map[string]any) capture.LogRecord { return objRec(tm, o, "nodes") }

// report is one report of the cluster receiver at the time: a state of every
// node, pod and container.
func (w *rsWorld) report(tm time.Time) capture.LogsData {
	var recs []capture.LogRecord
	for _, n := range w.nodes {
		desc := map[string]string{"k8s.node.name": n.name, "k8s.node.condition_ready": "true"}
		if !n.created.IsZero() {
			desc["node.creation_timestamp"] = ts(n.created)
		}
		recs = append(recs, entity("entity.state", "k8s.node", "k8s.node.uid", n.uid, tm, desc))
	}
	for _, p := range w.pods {
		recs = append(recs, entity("entity.state", "k8s.pod", "k8s.pod.uid", p.uid, tm, map[string]string{
			"k8s.pod.name": p.name, "k8s.namespace.name": "default", "k8s.node.name": p.node,
			"k8s.pod.phase": p.phase, "pod.creation_timestamp": ts(p.created),
		}))
		if p.ctrID != "" {
			recs = append(recs, entity("entity.state", "container", "container.id", "containerd://"+p.ctrID, tm, map[string]string{
				"k8s.container.name": "c", "k8s.pod.uid": p.uid, "container.status": "running",
				"container.creation_timestamp": ts(p.ctrStarted),
			}))
		}
	}
	return logs(w.cluster, recs...)
}

// newWorld is nNodes nodes and nPods pods spread over them, created a day before
// t0, scheduled a minute after, each with a container.
func newWorld(nNodes, nPods int) *rsWorld {
	w := &rsWorld{}
	for i := range nNodes {
		w.nodes = append(w.nodes, rsNode{uid: fmt.Sprintf("node-%d", i), name: fmt.Sprintf("n%d", i), created: t0.Add(-48 * time.Hour)})
	}
	for i := range nPods {
		w.pods = append(w.pods, &rsPod{
			uid: fmt.Sprintf("pod-%02d", i), name: fmt.Sprintf("p%02d", i), node: fmt.Sprintf("n%d", i%nNodes),
			created: t0.Add(-24 * time.Hour), scheduled: t0.Add(-24*time.Hour + time.Minute), phase: "Running",
			ctrID: fmt.Sprintf("ctr-%02d", i), ctrStarted: t0.Add(-24*time.Hour + 2*time.Minute),
		})
	}
	return w
}

// ---- fingerprints ----

var rsResolver = identity.NewResolver(catalog.Default())

func fpOf(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := rsResolver.Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func nodeFP(t testing.TB, uid string) identity.Fingerprint {
	return fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, uid)
}

func ctrFP(t testing.TB, id string) identity.Fingerprint {
	return fpOf(t, catalog.Container, catalog.ContainerID, id)
}

// ---- the harness ----

type rsOptions struct {
	// lag is the checkpoint lag; kmin the records between checkpoints (0: the
	// default policy's).
	lag  time.Duration
	kmin int
	// mem uses a memstore, and off runs with no lookup.
	mem, off bool
	// configured sets the cluster name "c1"; collection is the collection
	// interval (0: the default).
	configured bool
	collection time.Duration
	// beforeTranslate is a seam of the loop.
	beforeTranslate func(cluster string, lr capture.LogRecord)
}

// rs is a sequencer over a store that a test can restart, with what the store
// counted.
type rs struct {
	t     *testing.T
	o     rsOptions
	clock *fakeClock
	dir   string
	peb   *pebblestore.Store
	mem   *memstore.Store
	rec   *countRecorder
	fs    *fakeStore
	h     *harness
	logs  *logCapture

	mu sync.Mutex
	// writes counts the sequencer's writes; inval holds, from the write the test
	// armed it at, the checkpoints each write deleted, by prefix.
	writes int
	armed  bool
	inval  []map[string]int
	// prior are the records the sequencers before the current one wrote, and
	// priorStats their resume counts.
	prior      []store.Record
	priorStats resume.Stats
}

func newRS(t *testing.T, o rsOptions) *rs {
	t.Helper()
	r := &rs{t: t, o: o, clock: newClock(t0), rec: newCountRecorder(), logs: newLogCapture(t.Output())}
	if o.mem {
		r.mem = newMem(t)
		t.Cleanup(func() { _ = r.mem.Close() })
	} else {
		r.dir = t.TempDir()
		r.open()
	}
	r.startSequencer()
	return r
}

func (r *rs) open() {
	r.t.Helper()
	opts := pebblestore.DefaultOptions()
	ck := pebblestore.DefaultCheckpoints()
	ck.Lag = r.o.lag
	if r.o.kmin > 0 {
		ck.KMin = r.o.kmin
	}
	opts.Checkpoints = &ck
	opts.Recorder = r.rec
	st, err := pebblestore.Open(r.dir, opts)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { _ = st.Close() })
	st.Instrument().OnInvalidate(func(prefix []byte, n int) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.armed {
			r.inval[len(r.inval)-1][string(prefix)] += n
		}
	})
	r.peb = st
}

func (r *rs) store() interface {
	sequencer.Writer
	store.Store
} {
	if r.mem != nil {
		return r.mem
	}
	return r.peb
}

func (r *rs) opener() func() (resume.Lookup, error) {
	if r.o.off {
		return nil
	}
	if r.mem != nil {
		m := r.mem
		return func() (resume.Lookup, error) { return m.NewLookup() }
	}
	p := r.peb
	return func() (resume.Lookup, error) { return p.NewLookup() }
}

func (r *rs) startSequencer() {
	r.t.Helper()
	r.fs = &fakeStore{inner: r.store()}
	logger := slog.New(r.logs)
	r.h = newHarness(r.t, r.fs,
		withConfig(func(c *sequencer.Config) {
			c.Now = r.clock.Now
			c.Logger = logger
			c.Lookup = r.opener()
			if r.o.configured {
				c.ClusterName, c.ClusterNameSet = "c1", true
			}
			if r.o.collection > 0 {
				c.CollectionInterval = r.o.collection
			}
		}),
		withHooks(sequencer.Hooks{
			BeforeWrite: func([]store.Record) {
				r.mu.Lock()
				r.writes++
				if r.armed {
					r.inval = append(r.inval, map[string]int{})
				}
				r.mu.Unlock()
			},
			BeforeTranslate: r.o.beforeTranslate,
		}))
	r.h.clock = r.clock
	r.h.start()
}

// restart stops the sequencer, closes and opens the store again (a memstore is
// kept), and starts a new sequencer: a restart of the process.
func (r *rs) restart() {
	r.t.Helper()
	r.h.stop()
	r.prior = append(r.prior, r.fs.records()...)
	s, _ := r.h.seq.ResumeStats()
	r.priorStats.Add(s)
	if r.peb != nil {
		if err := r.peb.Close(); err != nil {
			r.t.Fatal(err)
		}
		r.open()
	}
	r.startSequencer()
}

// arm starts keeping the checkpoints each write deletes.
func (r *rs) arm() {
	r.mu.Lock()
	r.armed = true
	r.mu.Unlock()
}

// invalidations are, since arm, the checkpoints deleted by each write, by prefix.
func (r *rs) invalidations() []map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.inval)
}

// maxPerPrefix is the most checkpoints one write deleted from one prefix since
// arm, and total all of them.
func (r *rs) maxPerPrefix() (most, total int) {
	for _, w := range r.invalidations() {
		for _, n := range w {
			most = max(most, n)
			total += n
		}
	}
	return most, total
}

// tick sends a flush tick at the clock's instant and waits for it.
func (r *rs) tick() {
	r.h.tickCh <- r.clock.now()
	<-r.h.ticked
}

// send sends a request at the clock's instant and fails the test unless it is
// answered 200 with nothing rejected.
func (r *rs) send(d capture.LogsData) {
	r.t.Helper()
	p, err := r.h.seq.AcceptPartial(context.Background(), d)
	if err != nil || p.Rejected != 0 {
		r.t.Fatalf("at %s: partial %+v, error %v", r.clock.now().Sub(t0), p, err)
	}
}

// pullAt sends the world's pull and report at the clock's instant.
func (r *rs) pullAt(w *rsWorld) {
	r.t.Helper()
	tm := r.clock.now()
	r.send(w.pull(tm))
	if !w.noCluster {
		r.send(w.report(tm))
	}
}

// run moves the clock forward in steps of 30 seconds to end, with a tick at each
// step and the world's pull and report every five minutes after t0, unless skip
// says to leave one out. Every pull is made before the tick of its instant.
func (r *rs) run(w *rsWorld, end time.Time, skip func(time.Time) bool) {
	r.t.Helper()
	for tm := r.clock.now().Add(30 * time.Second); !tm.After(end); tm = tm.Add(30 * time.Second) {
		r.clock.set(tm)
		if tm.Sub(t0)%(5*time.Minute) == 0 && (skip == nil || !skip(tm)) {
			r.pullAt(w)
		}
		r.tick()
	}
}

// stats is the resume counts so far, of every sequencer.
func (r *rs) stats() resume.Stats {
	s, _ := r.h.seq.ResumeStats()
	s.Add(r.priorStats)
	return s
}

// keysWalked is the samples of lookup.keys_walked: one per lookup that walked.
func (r *rs) keysWalked() []int64 {
	r.rec.mu.Lock()
	defer r.rec.mu.Unlock()
	return slices.Clone(r.rec.samples["lookup.keys_walked"])
}

// ---- reads ----

// probe is one read: what it answered and the records it stepped, and how many
// records the run wrote in all to the prefix the read walks.
type probe struct {
	what    string
	answer  string
	steps   int64
	records int
	// before says the instant is before the reset, and edge that the read walks a
	// prefix of edges.
	before, edge bool
}

// reads are the answers, at each instant, of the reads the acceptance names: the
// reverse neighbors of the first node (node<-), a pod's forward neighbors and its
// existence (its entity prefix holds both producers' records), a container's
// forward neighbors (part_of), and the first node's existence.
func (r *rs) reads(w *rsWorld, instants []time.Time) []probe {
	r.t.Helper()
	ctx := context.Background()
	st := r.store()
	node := nodeFP(r.t, w.nodes[0].uid)
	pod := podFP(r.t, w.pods[0].uid)
	ctr := ctrFP(r.t, w.pods[0].ctrID)
	all := r.allRecords()
	held := func(in func(store.Subject) bool) int {
		n := 0
		for _, rec := range all {
			if in(rec.Subject) {
				n++
			}
		}
		return n
	}
	var out []probe
	for _, tm := range instants {
		for _, q := range []struct {
			what string
			edge bool
			in   func(store.Subject) bool
			do   func() (string, error)
		}{
			{
				"node<-", true, func(s store.Subject) bool { return s.Kind == store.SubjectEdge && s.B == node },
				func() (string, error) {
					return neighbors(st.Neighbors(ctx, node, store.Reverse, tm, store.Current(catalog.L2)))
				},
			},
			{
				"pod->", true, func(s store.Subject) bool { return s.Kind == store.SubjectEdge && s.A == pod },
				func() (string, error) {
					return neighbors(st.Neighbors(ctx, pod, store.Forward, tm, store.Current(catalog.L2)))
				},
			},
			{
				"pod", false, func(s store.Subject) bool { return s == store.EntitySubject(pod) },
				func() (string, error) { return alive(st.Alive(ctx, pod, tm, store.Current(catalog.L2))) },
			},
			{
				"container->", true, func(s store.Subject) bool { return s.Kind == store.SubjectEdge && s.A == ctr },
				func() (string, error) {
					return neighbors(st.Neighbors(ctx, ctr, store.Forward, tm, store.Current(catalog.L2)))
				},
			},
			{
				"node", false, func(s store.Subject) bool { return s == store.EntitySubject(node) },
				func() (string, error) { return alive(st.Alive(ctx, node, tm, store.Current(catalog.L2))) },
			},
		} {
			before := r.rec.count("read.records_stepped")
			a, err := q.do()
			if err != nil {
				r.t.Fatalf("%s at %s: %v", q.what, tm.Sub(t0), err)
			}
			out = append(out, probe{
				what: fmt.Sprintf("%s at %s", q.what, tm.Sub(t0)), answer: a, steps: r.rec.count("read.records_stepped") - before,
				records: held(q.in), before: tm.Before(at(rsReset)), edge: q.edge,
			})
		}
	}
	return out
}

func neighbors(ns []store.Neighbor, err error) (string, error) {
	var parts []string
	for _, n := range ns {
		parts = append(parts, string(n.Relation)+":"+n.Peer.String())
	}
	slices.Sort(parts)
	return strings.Join(parts, ","), err
}

func alive(a bool, err error) (string, error) { return fmt.Sprint(a), err }

// compareReads compares the reads with the control's. It returns, as failures, the
// reads whose answers differ; the reads before the reset of an edge prefix whose
// steps differ (a checkpoint the reset deleted would show there); and the reads
// before the reset of an entity, which walk the whole prefix, whose steps exceed
// the control's by more than the records the run holds in the prefix beyond the
// control's (the coalescer's gap after a reset, one extra record per run). It
// returns as notes every other difference in steps: after the reset the
// checkpoints of the two runs are placed apart, counted from different records.
func compareReads(got, want []probe) (failures, notes []string) {
	for i := range min(len(got), len(want)) {
		g, w := got[i], want[i]
		if g.answer != w.answer {
			failures = append(failures, fmt.Sprintf("%s: answered %q, the control %q", g.what, g.answer, w.answer))
		}
		if g.steps == w.steps {
			continue
		}
		msg := fmt.Sprintf("%s: stepped %d records, the control %d (the prefix holds %d records, the control's %d)", g.what, g.steps, w.steps, g.records, w.records)
		switch {
		case g.before && g.edge:
			failures = append(failures, msg)
		case g.before && g.steps-w.steps > int64(max(g.records-w.records, 0)):
			failures = append(failures, msg)
		default:
			notes = append(notes, msg)
		}
	}
	return failures, notes
}

// lastOf is the store's last assertion of a reference, read through a new lookup.
func (r *rs) lastOf(ref store.LookupRef) store.LastAssertion {
	r.t.Helper()
	open := r.opener()
	if open == nil {
		if r.mem != nil {
			open = func() (resume.Lookup, error) { return r.mem.NewLookup() }
		} else {
			open = func() (resume.Lookup, error) { return r.peb.NewLookup() }
		}
	}
	lk, err := open()
	if err != nil {
		r.t.Fatal(err)
	}
	defer lk.Close()
	a, err := lk.Last(context.Background(), ref)
	if err != nil {
		r.t.Fatal(err)
	}
	return a
}

func objectsRef(t testing.TB, sub store.Subject, cluster string) store.LookupRef {
	if cluster == "" {
		cluster = "default"
	}
	return store.LookupRef{Layer: catalog.L2, Subject: sub, Producer: lifecycle.Producer("k8sobjects/" + cluster)}
}

// recordsOf are the records of a subject and producer the sequencer wrote, in
// order.
func (r *rs) recordsOf(sub store.Subject, producer lifecycle.Producer) []store.Record {
	var out []store.Record
	for _, rec := range r.allRecords() {
		if rec.Subject == sub && rec.Producer == producer {
			out = append(out, rec)
		}
	}
	return out
}

// allRecords are the records every sequencer so far wrote, in order.
func (r *rs) allRecords() []store.Record {
	return append(slices.Clone(r.prior), r.fs.records()...)
}
