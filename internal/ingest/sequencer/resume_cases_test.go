package sequencer_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// lags are the two checkpoint lags every case runs at: the nanosecond at which a
// checkpoint sits at its prefix's newest key, and the default five minutes, set
// explicitly.
var lags = []time.Duration{time.Nanosecond, 5 * time.Minute}

// Case a. A pod changed phase while toposhift was down, for less than the TTL:
// the post-reset record of its entity takes the observed time, with no Through;
// the records written for its entity prefix over the next three pulls are exactly
// those fixed before the code was written.
func TestResumeCaseAChangedPhaseWhileDown(t *testing.T) {
	for _, o := range []rsOptions{{lag: time.Nanosecond}, {lag: 5 * time.Minute}, {lag: time.Nanosecond, mem: true}} {
		lag := o.lag
		t.Run(fmt.Sprintf("lag %s memstore %v", o.lag, o.mem), func(t *testing.T) {
			r := newRS(t, o)
			w := &rsWorld{noCluster: true}
			w.nodes = []rsNode{{uid: "node-0", name: "n", created: t0.Add(-48 * time.Hour)}}
			p := &rsPod{uid: "pod-p", name: "p", node: "n", created: t0, scheduled: t0, phase: "Pending"}
			w.pods = []*rsPod{p}
			pod, node := podFP(t, "pod-p"), nodeFP(t, "node-0")
			podSub := store.EntitySubject(pod)
			edgeSub := store.EdgeSubject(pod, node, catalog.ScheduledOn)
			nodeSub := store.EntitySubject(node)
			producer := lifecycle.Producer("k8sobjects/default")

			// Pulls at 00:00 to 00:30, then a restart at 00:32.
			r.pullAt(w)
			r.tick()
			r.run(w, at(32*time.Minute), nil)
			r.restart()

			// The precondition, derived from the coalescer's rules.
			pre := r.lastOf(objectsRef(t, podSub, ""))
			if pre.Outcome != store.LookupFound || pre.Kind != lifecycle.Observe || !pre.Last.Equal(at(30*time.Minute)) ||
				string(pre.Payload) == "" || !containsPhase(pre.Payload, "Pending") {
				t.Fatalf("precondition: the pod's last stored assertion is %+v (%s), want Found, Observe, Pending, last 00:30", pre, pre.Payload)
			}
			var stored []string
			for _, rec := range r.recordsOf(podSub, producer) {
				stored = append(stored, fmt.Sprintf("%s through %s", rec.EventTime.Sub(t0), throughOf(rec)))
			}
			// The derivation of the precondition gave stored records at 00:00, 00:10,
			// 00:20 and 00:30 with no Through. The coalescer starts a run where the basis
			// changes too, and the first sighting's basis is the object's field: the
			// records are at 00:00, 00:05, 00:15 and 00:25, and the flush at the stop
			// extends the last run through 00:30. The last instant is 00:30 either way.
			if fmt.Sprint(stored) != "[0s through - 5m0s through - 15m0s through - 25m0s through - 25m0s through 30m0s]" {
				t.Errorf("the pod's stored records are %v", stored)
			}
			t.Logf("lag %s: the pod's stored records before the restart: %v", lag, stored)
			lastEdge := r.lastOf(objectsRef(t, edgeSub, ""))
			lastNode := r.lastOf(objectsRef(t, nodeSub, ""))

			// The pull at 00:40: Running, with a container started at 00:36.
			p.phase, p.ctrID, p.ctrStarted = "Running", "ctr-c", at(36*time.Minute)
			n0 := len(r.recordsOf(podSub, producer))
			before := r.stats()
			r.clock.set(at(40 * time.Minute))
			r.pullAt(w)
			r.tick()
			st := since(r.stats(), before)
			if st.Changed != 1 || st.ChangedGap != 0 || st.Raised != 2 || st.New != 2 || st.Unknown != 0 || st.Dropped != 0 {
				t.Errorf("counters after the 00:40 group: %v; want changed 1, gap 0, raised 2, new 2, unknown 0, dropped 0", counters(st))
			}
			for _, x := range []struct {
				sub  store.Subject
				last store.LastAssertion
			}{{edgeSub, lastEdge}, {nodeSub, lastNode}} {
				recs := r.recordsOf(x.sub, producer)
				var in []store.Record
				for _, rec := range recs {
					if rec.Through.Equal(at(40*time.Minute)) || rec.EventTime.Equal(at(40*time.Minute)) {
						in = append(in, rec)
					}
				}
				basis := x.last.Basis
				if x.last.LastFromThrough {
					basis = store.BasisObserved
				}
				if len(in) != 1 || !in[0].EventTime.Equal(x.last.Last) || !in[0].Through.Equal(at(40*time.Minute)) || in[0].EventTimeBasis != basis {
					t.Errorf("%v in the 00:40 group: %v; want one record at %s through 00:40, basis %s", x.sub.Relation, showAll(in), x.last.Last.Sub(t0), basis)
				}
			}
			r.clock.set(at(40*time.Minute + 30*time.Second))
			r.run(w, at(50*time.Minute), nil)

			got := r.recordsOf(podSub, producer)[n0:]
			want := []string{
				"observe at 40m0s through - basis observed Running",
				"observe at 50m0s through - basis observed Running",
			}
			var gotS []string
			for _, rec := range got {
				phase := "?"
				for _, ph := range []string{"Running", "Pending"} {
					if containsPhase(rec.Payload, ph) {
						phase = ph
					}
				}
				gotS = append(gotS, fmt.Sprintf("%s at %s through %s basis %s %s", rec.Kind, rec.EventTime.Sub(t0), throughOf(rec), rec.EventTimeBasis, phase))
			}
			if fmt.Sprint(gotS) != fmt.Sprint(want) {
				t.Errorf("the pod's records over the groups of 00:40, 00:45 and 00:50:\n got  %v\n want %v", gotS, want)
			}
			t.Logf("lag %s: records %v; the 00:40 group's counters %v", lag, gotS, counters(st))
		})
	}
}

func containsPhase(payload []byte, phase string) bool {
	return strings.Contains(string(payload), `"phase":"`+phase+`"`)
}

func showAll(recs []store.Record) []string {
	var out []string
	for _, r := range recs {
		out = append(out, show(r))
	}
	return out
}

// onePod is a world of one node and one pod on it, reported by the objects
// receiver alone.
func onePod() (*rsWorld, *rsPod) {
	p := &rsPod{
		uid: "pod-p", name: "p", node: "n", created: t0.Add(-time.Hour), scheduled: t0.Add(-59 * time.Minute),
		phase: "Running", ctrID: "ctr-p", ctrStarted: t0.Add(-58 * time.Minute),
	}
	return &rsWorld{noCluster: true, nodes: []rsNode{{uid: "node-0", name: "n", created: t0.Add(-48 * time.Hour)}}, pods: []*rsPod{p}}, p
}

const objectsDefault = lifecycle.Producer("k8sobjects/default")

// pullOnce moves the clock to tm, sends the world's pull and report, and ticks.
func (r *rs) pullOnce(w *rsWorld, tm time.Time) {
	r.t.Helper()
	r.clock.set(tm)
	r.pullAt(w)
	r.tick()
}

// Case b. A description changed during a silence longer than the TTL: the record
// takes the observed time, counted as changed and as a gap, with one warning.
func TestResumeCaseBChangedAfterTheTTL(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond})
	w, p := onePod()
	r.pullAt(w)
	r.tick()
	r.run(w, at(30*time.Minute), nil)
	r.clock.set(at(32 * time.Minute))
	r.restart()
	r.logs.take()
	p.phase = "Failed"
	before := r.stats()
	r.pullOnce(w, at(60*time.Minute))
	st := since(r.stats(), before)
	if st.Changed != 1 || st.ChangedGap != 1 || st.GapExample.Subject != store.EntitySubject(podFP(t, "pod-p")) {
		t.Errorf("%v (gap example %v), want the pod changed, with a gap", counters(st), st.GapExample.Subject)
	}
	if n := count(r.logs.take(), slog.LevelWarn, "a description changed during a silence longer than its TTL"); n != 1 {
		t.Errorf("%d warnings of the gap, want 1", n)
	}
	recs := r.recordsOf(store.EntitySubject(podFP(t, "pod-p")), objectsDefault)
	if last := recs[len(recs)-1]; !last.EventTime.Equal(at(60*time.Minute)) || !last.Through.IsZero() || last.EventTimeBasis != store.BasisObserved {
		t.Errorf("the changed pod's record: %s", show(last))
	}
}

// Case c. A node with no usable creationTimestamp, first seen again after a reset,
// by a list and by a watch: its interval starts when it is first seen again,
// after every instant the store holds, so its pods' edges, whose PodScheduled time
// is before that, wait and begin at a later record of the pod. No edge is changed:
// none is resumed against an interval that started after it. (That an edge is
// compared on its node_name alone, so that a new node_interval_start is no change,
// is shown by each translator's Comparable test.)
func TestResumeCaseCANodeWithoutACreationTime(t *testing.T) {
	for _, viaWatch := range []bool{false, true} {
		t.Run(fmt.Sprint("by a watch ", viaWatch), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: time.Nanosecond})
			w, p := onePod()
			w.nodes[0].created = time.Time{}
			p.scheduled = at(10 * time.Minute)
			r.pullAt(w)
			r.tick()
			r.run(w, at(30*time.Minute), nil)
			r.clock.set(at(32 * time.Minute))
			r.restart()
			edge := store.EdgeSubject(podFP(t, "pod-p"), nodeFP(t, "node-0"), catalog.ScheduledOn)
			stored := r.lastOf(objectsRef(t, edge, ""))
			before := r.stats()
			r.clock.set(at(35 * time.Minute))
			if viaWatch {
				r.send(logs("", objRec(at(35*time.Minute), map[string]any{"type": "MODIFIED", "object": w.nodes[0].object()}, "nodes")))
				r.send(logs("", objRec(at(35*time.Minute), w.pods[0].object(), "pods")))
			} else {
				r.pullAt(w)
			}
			r.tick()
			r.run(w, at(45*time.Minute), nil)
			st := since(r.stats(), before)
			if st.Changed != 0 || st.Unknown != 0 {
				t.Errorf("%v: want no change", counters(st))
			}
			recs := r.recordsOf(edge, objectsDefault)
			got := recs[len(recs)-1]
			if !got.EventTime.Equal(at(40*time.Minute)) || !got.Through.IsZero() {
				t.Errorf("the edge after the reset: %s, want begun at the pod's next record, 00:40", show(got))
			}
			if string(got.Payload) == string(stored.Payload) || nodeNameOf(t, got.Payload) != nodeNameOf(t, stored.Payload) {
				t.Errorf("payload %s, stored %s: want a new node_interval_start and the same node_name", got.Payload, stored.Payload)
			}
		})
	}
}

func nodeNameOf(t *testing.T, payload []byte) string {
	t.Helper()
	var p struct {
		NodeName string `json:"node_name"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("%s: %v", payload, err)
	}
	return p.NodeName
}

// Case e. A pod whose last pull was not stored (the write failed, and the
// translators were made anew): its next record is raised to its own stored last
// instant, and through the observation the store answers as a store that took the
// failed pull answers.
func TestResumeCaseELastRecordNotStored(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("memstore ", mem), func(t *testing.T) { caseE(t, mem) })
	}
}

func caseE(t *testing.T, mem bool) {
	var lastBefore time.Time
	run := func(fail bool) *rs {
		r := newRS(t, rsOptions{lag: time.Nanosecond, mem: mem})
		w, _ := onePod()
		r.pullAt(w)
		r.tick()
		r.run(w, at(30*time.Minute), nil)
		r.clock.set(at(35 * time.Minute))
		if fail {
			var once atomic.Bool
			r.fs.inject = func(int, []store.Record) (bool, error) {
				if once.CompareAndSwap(false, true) {
					return false, errInjected
				}
				return true, nil
			}
			if _, err := r.h.seq.AcceptPartial(t.Context(), w.pull(at(35*time.Minute))); !errors.Is(err, otlphttp.ErrBusy) {
				t.Fatalf("the failed write answered %v, want 503", err)
			}
			r.fs.inject = nil
			r.tick()
			lastBefore = r.lastOf(objectsRef(t, store.EntitySubject(podFP(t, "pod-p")), "")).Last
		} else {
			r.pullAt(w)
			r.tick()
		}
		r.run(w, at(60*time.Minute), nil)
		return r
	}
	oracle, r := run(false), run(true)
	pod := store.EntitySubject(podFP(t, "pod-p"))
	if st := r.stats(); st.Raised < 1 || st.Changed != 0 || st.Unknown != 0 {
		t.Errorf("%v: want the pod raised", counters(st))
	}
	recs := r.recordsOf(pod, objectsDefault)
	var resumed *store.Record
	for i := range recs {
		if recs[i].Through.Equal(at(40*time.Minute)) && recs[i].EventTimeBasis == store.BasisObserved {
			resumed = &recs[i]
		}
	}
	// The coalescer that absorbed the refresh of 00:30 was made anew with the
	// translators, so the last stored instant is the record of 00:25.
	if resumed == nil || !resumed.EventTime.Equal(lastBefore) {
		t.Errorf("records %v: want the pull at 00:40 raised to %s, its last stored instant", showAll(recs), lastBefore.Sub(t0))
	}
	for m := 0; m <= 60; m++ {
		tm := at(time.Duration(m) * time.Minute)
		a, err := r.store().Alive(t.Context(), podFP(t, "pod-p"), tm, store.Current(catalog.L2))
		b, err2 := oracle.store().Alive(t.Context(), podFP(t, "pod-p"), tm, store.Current(catalog.L2))
		if err != nil || err2 != nil || a != b {
			t.Errorf("alive at %dm: %v, the oracle %v (%v %v)", m, a, b, err, err2)
		}
	}
}

// Case f. An edge never stored, pending across a reset (the node was never
// listed): once the node is listed it begins at PodScheduled, through the pod's
// observation, as a genuine first sighting, and deletes checkpoints of its own
// two prefixes at most.
func TestResumeCaseFAnEdgeNeverStored(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond, kmin: 4})
	w, _ := onePod()
	node := w.nodes
	w.nodes = nil
	r.pullAt(w)
	r.tick()
	r.run(w, at(2*time.Hour), nil)
	r.clock.set(at(2*time.Hour + 2*time.Minute))
	r.restart()
	r.arm()
	w.nodes = node
	r.pullOnce(w, at(2*time.Hour+5*time.Minute))
	edge := store.EdgeSubject(podFP(t, "pod-p"), nodeFP(t, "node-0"), catalog.ScheduledOn)
	recs := r.recordsOf(edge, objectsDefault)
	if len(recs) != 1 || !recs[0].EventTime.Equal(w.pods[0].scheduled) || !recs[0].Through.Equal(at(2*time.Hour+5*time.Minute)) {
		t.Errorf("the edge: %v, want one record at PodScheduled through the observation", showAll(recs))
	}
	prefixes := map[string]bool{}
	for _, inv := range r.invalidations() {
		for p := range inv {
			prefixes[p] = true
		}
	}
	if len(prefixes) > 2 {
		t.Errorf("checkpoints deleted in %d prefixes, want at most the edge's two", len(prefixes))
	}
	t.Logf("resume %v; prefixes with deleted checkpoints %d", counters(r.stats()), len(prefixes))
}

// Case f′. The first pull after a reset lists the pods before the nodes, and the
// edges were stored before: each edge waits for its node, is resolved when the
// node is listed, and is raised to its stored last instant; no checkpoint is
// deleted.
func TestResumeCaseFPrimePodsBeforeNodes(t *testing.T) {
	for _, lag := range lags {
		t.Run(fmt.Sprint("lag ", lag), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: lag})
			w := newWorld(2, 10)
			w.noCluster = true
			r.pullAt(w)
			r.tick()
			r.run(w, at(6*time.Hour), nil)
			r.clock.set(at(6*time.Hour + 2*time.Minute))
			r.restart()
			var lasts []time.Time
			for _, p := range w.pods {
				edge := store.EdgeSubject(podFP(t, p.uid), nodeFP(t, "node-"+p.node[1:]), catalog.ScheduledOn)
				lasts = append(lasts, r.lastOf(objectsRef(t, edge, "")).Last)
			}
			r.arm()
			w.podsFirst = true
			r.pullOnce(w, at(6*time.Hour+5*time.Minute))
			for i, p := range w.pods {
				edge := store.EdgeSubject(podFP(t, p.uid), nodeFP(t, "node-"+p.node[1:]), catalog.ScheduledOn)
				recs := r.recordsOf(edge, objectsDefault)
				if got := recs[len(recs)-1]; !got.EventTime.Equal(lasts[i]) || !got.Through.Equal(at(6*time.Hour+5*time.Minute)) {
					t.Errorf("%s's edge: %s, want raised to %s", p.name, show(got), lasts[i].Sub(t0))
				}
			}
			most, total := r.maxPerPrefix()
			if most > 1 {
				t.Errorf("a write deleted %d checkpoints of one prefix", most)
			}
			t.Logf("deleted %d checkpoints, at most %d of one prefix; resume %v", total, most, counters(r.stats()))
		})
	}
}

// Case l. After a raise, a watch event whose change is timed between the pod's
// creation and its resumed instant is held at that instant by the translator's
// own state, and deletes no checkpoint.
func TestResumeCaseLAWatchAfterARaise(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond, kmin: 4})
	w, p := onePod()
	r.pullAt(w)
	r.tick()
	r.run(w, at(2*time.Hour), nil)
	r.clock.set(at(2*time.Hour + 2*time.Minute))
	r.restart()
	pod := store.EntitySubject(podFP(t, "pod-p"))
	stored := r.lastOf(objectsRef(t, pod, ""))
	r.pullOnce(w, at(2*time.Hour+5*time.Minute))
	r.arm()
	p.conds = []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(at(30 * time.Minute))}}
	r.clock.set(at(2*time.Hour + 6*time.Minute))
	r.send(logs("", objRec(r.clock.now(), map[string]any{"type": "MODIFIED", "object": p.object()}, "pods")))
	r.tick()
	recs := r.recordsOf(pod, objectsDefault)
	got := recs[len(recs)-1]
	if !got.EventTime.Equal(stored.Last) {
		t.Errorf("the watch event's record: %s, want held at the resumed instant %s", show(got), stored.Last.Sub(t0))
	}
	if _, total := r.maxPerPrefix(); total != 0 {
		t.Errorf("the watch event deleted %d checkpoints", total)
	}
}

// Case m. In steady state, with watch events timed by their changes, the step
// looks up only the subjects seen for the first time: a new pod, its container,
// its part_of edge and its scheduled_on edge.
func TestResumeCaseMSteadyStateLooksUpNewSubjectsOnly(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond})
	w := newWorld(2, 4)
	w.noCluster = true
	r.pullAt(w)
	r.tick()
	first := len(r.keysWalked())
	// 2 nodes, and 4 pods with a container, a part_of edge and an edge each.
	if first != 2+4*4 {
		t.Errorf("%d lookups for the first pull, want %d", first, 2+4*4)
	}
	for m := 1; m <= 12; m++ {
		tm := at(time.Duration(m) * 5 * time.Minute)
		w.pods[m%4].conds = []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(tm.Add(-2 * time.Minute))}}
		r.clock.set(tm.Add(-time.Minute))
		r.send(logs("", objRec(r.clock.now(), map[string]any{"type": "MODIFIED", "object": w.pods[m%4].object()}, "pods")))
		r.tick()
		r.clock.set(tm)
		r.pullAt(w)
		r.tick()
	}
	if n := len(r.keysWalked()); n != first {
		t.Errorf("%d lookups after an hour of steady state, want %d", n, first)
	}
	w.pods = append(w.pods, &rsPod{
		uid: "pod-new", name: "new", node: "n0", created: at(61 * time.Minute), scheduled: at(61 * time.Minute),
		phase: "Running", ctrID: "ctr-new", ctrStarted: at(62 * time.Minute),
	})
	r.pullOnce(w, at(65*time.Minute))
	if n := len(r.keysWalked()); n != first+4 {
		t.Errorf("%d lookups after a new pod, want %d", n, first+4)
	}
	if st := r.stats(); st.Raised != 0 || st.Changed != 0 || st.Unknown != 0 {
		t.Errorf("%v, want only new subjects", counters(st))
	}
}

// Case p. One lookup per group: the store opens one iterator for each group the
// sequencer commits, and none for a flush.
func TestResumeCasePOneIteratorPerGroup(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond})
	w := newWorld(2, 4)
	r.pullAt(w)
	r.tick()
	r.run(w, at(time.Hour), nil)
	groups := 2 * 13 // a pull and a report every five minutes, 00:00 to 01:00
	if n := r.rec.count("lookup.iterators"); n != int64(groups) {
		t.Errorf("%d iterators, want %d", n, groups)
	}
}

// since is what the step counted between two snapshots of the counts.
func since(after, before resume.Stats) resume.Stats {
	return resume.Stats{
		New: after.New - before.New, Raised: after.Raised - before.Raised, Changed: after.Changed - before.Changed,
		ChangedGap: after.ChangedGap - before.ChangedGap, Dropped: after.Dropped - before.Dropped,
		Unknown: after.Unknown - before.Unknown, UnknownExhausted: after.UnknownExhausted - before.UnknownExhausted,
		UnknownBaseline: after.UnknownBaseline - before.UnknownBaseline, UnknownBelowHorizon: after.UnknownBelowHorizon - before.UnknownBelowHorizon,
		UnknownError: after.UnknownError - before.UnknownError, MirrorRaised: after.MirrorRaised - before.MirrorRaised,
		ExhaustedExample: after.ExhaustedExample, GapExample: after.GapExample, ErrorExample: after.ErrorExample, FirstError: after.FirstError,
	}
}

// payloadOf is the payload a translator of the objects receiver with no state
// writes for a subject, given the log records.
func payloadOf(t *testing.T, sub store.Subject, lrs ...capture.LogRecord) []byte {
	t.Helper()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: objectsDefault, Resolver: rsResolver, PullInterval: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, lr := range lrs {
		res, err := tr.Translate(capture.Resource{}, lr)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res.Records {
			if r.Subject == sub {
				return r.Payload
			}
		}
	}
	t.Fatalf("no record of %v", sub)
	return nil
}

// write writes records straight to the store, numbered after its last.
func (r *rs) write(recs ...store.Record) {
	r.t.Helper()
	st := r.store()
	seq := st.LastSeq()
	for i := range recs {
		seq++
		recs[i].Seq = seq
	}
	if err := st.Write(r.t.Context(), recs); err != nil {
		r.t.Fatal(err)
	}
}

func observe(sub store.Subject, producer lifecycle.Producer, event, through time.Time, payload []byte) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: sub, Producer: producer, EventTime: event, Through: through,
		Kind: lifecycle.Observe, TTL: 15 * time.Minute, Payload: payload, EventTimeBasis: store.BasisObserved,
	}
}

const clusterDefault = lifecycle.Producer("k8scluster/default")

// Case g. The objects producer's newest record of a pod's scheduled_on edge is a
// long run (an Observe at 00:00 through 00:30, as a coalescer with a 30-minute age
// bound writes), and the other producer's records of the edge put at least three
// checkpoints in the pod's forward prefix after its start. (An entity's own
// prefix holds no checkpoint under a boot key, so the case is an edge's.) The
// edge's first record after a reset is raised to the run's last instant exactly,
// and deletes at most one checkpoint of a prefix; and so is the next one, after a
// second reset in a row, to the last instant the first left.
func TestResumeCaseGALongRun(t *testing.T) {
	for _, lag := range lags {
		for _, mem := range []bool{false, true} {
			t.Run(fmt.Sprintf("lag %s memstore %v", lag, mem), func(t *testing.T) {
				r := newRS(t, rsOptions{lag: lag, kmin: 4, mem: mem})
				w, p := onePod()
				w.pods[0].ctrID = ""
				edge := store.EdgeSubject(podFP(t, p.uid), nodeFP(t, "node-0"), catalog.ScheduledOn)
				payload := payloadOf(t, edge, objRec(at(0), w.nodes[0].object(), "nodes"), objRec(at(0), p.object(), "pods"))
				r.write(observe(edge, objectsDefault, at(0), at(30*time.Minute), payload))
				written := r.rec.count("checkpoint.written")
				for s := 20; s <= 30*60; s += 20 {
					r.write(observe(edge, clusterDefault, at(time.Duration(s)*time.Second), time.Time{}, []byte(`{"node_name":"n"}`)))
				}
				if n := r.rec.count("checkpoint.written") - written; !mem && n < 3 {
					t.Fatalf("%d checkpoints after the run's start, want at least 3", n)
				}
				r.arm()
				for i, tm := range []time.Time{at(40 * time.Minute), at(45 * time.Minute)} {
					if i == 1 {
						r.clock.set(at(42 * time.Minute))
						r.restart()
					}
					last := r.lastOf(objectsRef(t, edge, ""))
					r.pullOnce(w, tm)
					recs := r.recordsOf(edge, objectsDefault)
					got := recs[len(recs)-1]
					if !got.EventTime.Equal(last.Last) || !got.Through.Equal(tm) {
						t.Errorf("reset %d: the edge's record %s, want raised to %s exactly", i+1, show(got), last.Last.Sub(t0))
					}
				}
				most, total := r.maxPerPrefix()
				if most > 1 {
					t.Errorf("a write deleted %d checkpoints of one prefix", most)
				}
				if st := r.stats(); st.Raised < 2 || st.Changed != 0 || st.Unknown != 0 {
					t.Errorf("%v", counters(st))
				}
				t.Logf("deleted %d checkpoints, at most %d of one prefix by one write; resume %v", total, most, counters(r.stats()))
			})
		}
	}
}

// Case i. A stored Delete of the objects producer: a first sighting that starts at
// or before it is dropped; one that starts after it is a new life, left as it is.
func TestResumeCaseIAStoredDelete(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("memstore ", mem), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: time.Nanosecond, mem: mem})
			w, _ := onePod()
			w.pods[0].ctrID = ""
			later := &rsPod{uid: "pod-q", name: "q", node: "n", created: at(20 * time.Minute), scheduled: at(21 * time.Minute), phase: "Running"}
			w.pods = append(w.pods, later)
			for _, uid := range []string{"pod-p", "pod-q"} {
				r.write(store.Record{
					Layer: catalog.L2, Subject: store.EntitySubject(podFP(t, uid)), Producer: objectsDefault, EventTime: at(10 * time.Minute),
					Kind: lifecycle.Delete, EventTimeBasis: store.BasisObserved,
				})
			}
			r.pullOnce(w, at(30*time.Minute))
			st := r.stats()
			if st.Dropped != 1 {
				t.Errorf("%v, want one dropped", counters(st))
			}
			if recs := r.recordsOf(store.EntitySubject(podFP(t, "pod-p")), objectsDefault); len(recs) != 0 {
				t.Errorf("the pod created before the delete: %v, want its first sighting dropped", showAll(recs))
			}
			recs := r.recordsOf(store.EntitySubject(podFP(t, "pod-q")), objectsDefault)
			if len(recs) != 1 || !recs[0].EventTime.Equal(at(20*time.Minute)) || !recs[0].Through.Equal(at(30*time.Minute)) {
				t.Errorf("the pod created after the delete: %v, want its first sighting as it is", showAll(recs))
			}
		})
	}
}

// Case h. After a retention: a pod whose newest record is folded into the
// baseline and still alive at the horizon is unknown (baseline only), and takes
// the observed time; one that lapsed before the horizon finds nothing, and its
// record is re-timed to the horizon by the coalescer as before.
func TestResumeCaseHAfterARetention(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("memstore ", mem), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: time.Nanosecond, mem: mem})
			w := &rsWorld{noCluster: true}
			alive := &rsPod{uid: "pod-alive", name: "a", node: "", created: t0.Add(-time.Hour), scheduled: t0.Add(-time.Hour), phase: "Running"}
			lapsed := &rsPod{uid: "pod-lapsed", name: "l", node: "", created: t0.Add(-time.Hour), scheduled: t0.Add(-time.Hour), phase: "Running"}
			w.pods = []*rsPod{alive, lapsed}
			h := at(2 * time.Hour)
			for _, x := range []struct {
				p  *rsPod
				at time.Time
			}{{alive, h.Add(-10 * time.Minute)}, {lapsed, h.Add(-30 * time.Minute)}} {
				sub := store.EntitySubject(podFP(t, x.p.uid))
				r.write(observe(sub, objectsDefault, x.at, time.Time{}, payloadOf(t, sub, objRec(x.at, x.p.object(), "pods"))))
			}
			if err := r.store().Retain(t.Context(), h); err != nil {
				t.Fatal(err)
			}
			if got := r.store().LayerHorizon(catalog.L2).Time; !got.Equal(h) {
				t.Fatalf("L2 horizon %s, want %s", got, h)
			}
			r.pullOnce(w, h.Add(5*time.Minute))
			st := r.stats()
			if st.UnknownBaseline != 1 || st.Unknown != 1 || st.New != 1 {
				t.Errorf("%v, want one baseline only and one new", counters(st))
			}
			a := r.recordsOf(store.EntitySubject(podFP(t, alive.uid)), objectsDefault)
			if len(a) != 1 || !a[0].EventTime.Equal(h.Add(5*time.Minute)) || !a[0].Through.IsZero() {
				t.Errorf("the pod alive at the horizon: %v, want the observed time", showAll(a))
			}
			l := r.recordsOf(store.EntitySubject(podFP(t, lapsed.uid)), objectsDefault)
			if len(l) != 1 || !l[0].EventTime.Equal(h) || !l[0].Through.Equal(h.Add(5*time.Minute)) {
				t.Errorf("the pod that lapsed before the horizon: %v, want re-timed to the horizon", showAll(l))
			}
		})
	}
}

// Case j. A walk past 1,024 keys: the other producer holds more records of the
// pod, newer than the objects producer's, than a lookup walks. The answer is
// unknown, the record takes the observed time (never its creation), counted, with
// one warning naming the pod.
func TestResumeCaseJAWalkPastItsBound(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond})
	w, p := onePod()
	w.pods[0].ctrID = ""
	pod := store.EntitySubject(podFP(t, p.uid))
	r.write(observe(pod, objectsDefault, at(0), time.Time{}, payloadOf(t, pod, objRec(at(0), p.object(), "pods"))))
	var recs []store.Record
	for i := range 1100 {
		recs = append(recs, observe(pod, clusterDefault, at(time.Duration(i+1)*time.Second), time.Time{}, []byte(`{"k8s.pod.name":"p"}`)))
	}
	r.write(recs...)
	r.logs.take()
	r.pullOnce(w, at(30*time.Minute))
	st := r.stats()
	if st.UnknownExhausted != 1 || st.ExhaustedExample.Subject != pod {
		t.Errorf("%v, example %v: want the pod exhausted", counters(st), st.ExhaustedExample.Subject)
	}
	got := r.recordsOf(pod, objectsDefault)
	if last := got[len(got)-1]; !last.EventTime.Equal(at(30*time.Minute)) || !last.Through.IsZero() || last.EventTimeBasis != store.BasisObserved {
		t.Errorf("the pod's record: %s, want the observed time", show(last))
	}
	if n := count(r.logs.take(), slog.LevelWarn, "a lookup reached its walk bound"); n != 1 {
		t.Errorf("%d warnings, want 1", n)
	}
	if n := r.rec.count("lookup.exhausted"); n != 1 {
		t.Errorf("lookup.exhausted %d, want 1", n)
	}
}

// Case k. node<- holds more than 1,024 records newer than a pod's edge: the edge's
// lookup reads the pod's own side, and walks no more than 8 keys.
func TestResumeCaseKABusyNodeIsNotWalked(t *testing.T) {
	r := newRS(t, rsOptions{lag: time.Nanosecond})
	w, p := onePod()
	w.pods[0].ctrID = ""
	node := nodeFP(t, "node-0")
	edge := store.EdgeSubject(podFP(t, p.uid), node, catalog.ScheduledOn)
	r.pullAt(w)
	r.tick()
	r.run(w, at(30*time.Minute), nil)
	r.clock.set(at(32 * time.Minute))
	r.restart()
	stored := r.lastOf(objectsRef(t, edge, ""))
	var recs []store.Record
	for i := range 1100 {
		other := podFP(t, fmt.Sprintf("other-%04d", i))
		recs = append(recs, observe(store.EdgeSubject(other, node, catalog.ScheduledOn), objectsDefault, at(33*time.Minute), time.Time{}, []byte(`{"node_name":"n"}`)))
	}
	r.write(recs...)
	walked := len(r.keysWalked())
	r.pullOnce(w, at(35*time.Minute))
	for _, k := range r.keysWalked()[walked:] {
		if k > 8 {
			t.Errorf("a lookup walked %d keys, more than 8", k)
		}
	}
	got := r.recordsOf(edge, objectsDefault)
	if last := got[len(got)-1]; !last.EventTime.Equal(stored.Last) || !last.Through.Equal(at(35*time.Minute)) {
		t.Errorf("the edge: %s, want raised to %s", show(last), stored.Last.Sub(t0))
	}
	if st := r.stats(); st.Unknown != 0 {
		t.Errorf("%v", counters(st))
	}
}

// Case d. Two producers on one pod, and a checkpoint of the pod's entity prefix
// above the objects producer's newest record: the objects producer's first
// sighting after a reset is raised to that record's last instant, neither to the
// checkpoint nor to the other producer's records.
func TestResumeCaseDTwoProducersAndACheckpoint(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("memstore ", mem), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: time.Nanosecond, kmin: 4, mem: mem})
			w, p := onePod()
			w.pods[0].ctrID = ""
			pod := store.EntitySubject(podFP(t, p.uid))
			r.pullAt(w)
			r.tick()
			r.run(w, at(30*time.Minute), nil)
			// The objects receiver goes quiet; the cluster receiver goes on.
			w.noCluster = false
			written := r.rec.count("checkpoint.written")
			for tm := at(30*time.Minute + 30*time.Second); !tm.After(at(90 * time.Minute)); tm = tm.Add(30 * time.Second) {
				r.clock.set(tm)
				if tm.Sub(t0)%(5*time.Minute) == 0 {
					r.send(w.report(tm))
				}
				r.tick()
			}
			if n := r.rec.count("checkpoint.written") - written; !mem && n == 0 {
				t.Fatal("no checkpoint above the objects producer's newest record")
			}
			r.clock.set(at(92 * time.Minute))
			r.restart()
			stored := r.lastOf(objectsRef(t, pod, ""))
			if stored.Last.After(at(31 * time.Minute)) {
				t.Fatalf("the objects producer's last instant %s, want its own, by 00:30", stored.Last.Sub(t0))
			}
			w.noCluster = true
			r.pullOnce(w, at(95*time.Minute))
			got := r.recordsOf(pod, objectsDefault)
			if last := got[len(got)-1]; !last.EventTime.Equal(stored.Last) || !last.Through.Equal(at(95*time.Minute)) {
				t.Errorf("the objects producer's pod record: %s, want raised to its own last instant %s", show(last), stored.Last.Sub(t0))
			}
		})
	}
}

// Case l for a container. The coalescer starts each subject's runs on its own
// phase, so a container's last stored instant can be later than its pod's. After
// a reset each is raised to its own, and a later watch event whose change is timed
// between the two holds the container at its own resumed instant, not its pod's.
func TestResumeCaseLAContainerAfterARaise(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("memstore ", mem), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: time.Nanosecond, mem: mem})
			w, p := onePod()
			pod, ctr := store.EntitySubject(podFP(t, p.uid)), store.EntitySubject(ctrFP(t, p.ctrID))
			pull := objRec(at(0), p.object(), "pods")
			r.write(
				observe(pod, objectsDefault, at(20*time.Minute), time.Time{}, payloadOf(t, pod, pull)),
				observe(ctr, objectsDefault, at(30*time.Minute), time.Time{}, payloadOf(t, ctr, pull)),
			)
			r.pullOnce(w, at(40*time.Minute))
			last := func(sub store.Subject) store.Record {
				recs := r.recordsOf(sub, objectsDefault)
				return recs[len(recs)-1]
			}
			if !last(pod).EventTime.Equal(at(20*time.Minute)) || !last(ctr).EventTime.Equal(at(30*time.Minute)) {
				t.Fatalf("after the reset: pod %s, container %s; want each raised to its own last instant", show(last(pod)), show(last(ctr)))
			}
			p.conds = []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(at(25 * time.Minute))}}
			r.clock.set(at(41 * time.Minute))
			r.send(logs("", objRec(r.clock.now(), map[string]any{"type": "MODIFIED", "object": p.object()}, "pods")))
			r.tick()
			if got := last(pod); !got.EventTime.Equal(at(25 * time.Minute)) {
				t.Errorf("the pod's change: %s, want at its own time", show(got))
			}
			if got := last(ctr); !got.EventTime.Equal(at(30 * time.Minute)) {
				t.Errorf("the container with the pod's change: %s, want held at its resumed instant 00:30", show(got))
			}
		})
	}
}
