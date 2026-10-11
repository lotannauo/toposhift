package sequencer_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The scenario's cluster: nodes n1 and n2 known from the start, n3 created at
// 90 s, and n4, which exists from the start but is first seen by a watch event
// at 12 min. Pods p1 to p3 and p5 are in the first pull; p3 names n3 before it
// exists; p5 names n4, so its scheduled_on edge is pending until 12 min; p4 is
// added at 3 min; p2 is deleted at 7 min.
var (
	n1 = node{"node-1", "n1", t0.Add(-time.Hour)}
	n2 = node{"node-2", "n2", t0.Add(-time.Hour)}
	n3 = node{"node-3", "n3", t0.Add(90 * time.Second)}
	n4 = node{"node-4", "n4", t0.Add(-time.Hour)}

	p1Pending = pod{"pod-1", "p1", "n1", t0.Add(-30 * time.Minute), t0.Add(-30 * time.Minute), "Pending"}
	p1        = pod{"pod-1", "p1", "n1", t0.Add(-30 * time.Minute), t0.Add(-30 * time.Minute), "Running"}
	p2        = pod{"pod-2", "p2", "n2", t0.Add(-30 * time.Minute), t0.Add(-30 * time.Minute), "Running"}
	p3        = pod{"pod-3", "p3", "n3", t0.Add(-10 * time.Minute), time.Time{}, "Pending"}
	p4        = pod{"pod-4", "p4", "n1", t0.Add(3 * time.Minute), t0.Add(3 * time.Minute), "Running"}
	p5        = pod{"pod-5", "p5", "n4", t0.Add(-20 * time.Minute), t0.Add(-20 * time.Minute), "Running"}
)

const (
	kubeSystemUID = "ns-kube-system-1"
	defaultNSUID  = "ns-default-1"
)

// step is a request at an instant, or a flush tick when tick is set.
type step struct {
	at    time.Duration
	tick  bool
	label string
	data  capture.LogsData
}

// clusterEvents are the cluster receiver's events at an instant: the two
// namespaces (the namespace pass's records), a node and a pod.
func clusterEvents(tm time.Time, pods ...pod) []capture.LogRecord {
	recs := []capture.LogRecord{nsState(kubeSystemUID, "kube-system", tm), nsState(defaultNSUID, "default", tm)}
	for _, p := range pods {
		recs = append(recs, podState(p, tm))
	}
	return recs
}

func pullPods(d time.Duration, pods ...pod) capture.LogsData {
	var recs []capture.LogRecord
	for _, p := range pods {
		recs = append(recs, pullPod(at(d), p))
	}
	recs = append(recs, clusterEvents(at(d), pods[0])...)
	return logs("", recs...)
}

func pullNodes(d time.Duration, nodes ...node) capture.LogsData {
	var recs []capture.LogRecord
	for _, n := range nodes {
		recs = append(recs, pullNode(at(d), n))
	}
	recs = append(recs, nodeState(nodes[0], at(d)))
	return logs("", recs...)
}

// scenario is one pull of pods and nodes every five minutes from 0 to 20 min, a
// watch stream between, and flush ticks after most steps and every 30 s from
// 20 min to 40 min.
func scenario() []step {
	s := []step{
		{at: 0, label: "A0 pods", data: pullPods(0, p1Pending, p2, p3, p5)},
		{at: time.Second, label: "B0 nodes", data: pullNodes(time.Second, n1, n2)},
		{at: 30 * time.Second, tick: true},
		{at: time.Minute, label: "p1 modified", data: logs("", watchPod(at(time.Minute), "MODIFIED", p1))},
		{at: 2 * time.Minute, label: "n3 added", data: logs("", watchNode(at(2*time.Minute), "ADDED", n3))},
		{at: 3 * time.Minute, label: "p4 added", data: logs("", watchPod(at(3*time.Minute), "ADDED", p4))},
		{at: 3*time.Minute + 30*time.Second, tick: true},
	}
	for _, m := range []time.Duration{5, 10, 15, 20} {
		pods := []pod{p1, p2, p3, p4, p5}
		if m > 5 {
			pods = []pod{p1, p3, p4, p5}
		}
		nodes := []node{n1, n2, n3}
		if m > 12 {
			nodes = append(nodes, n4)
		}
		d := m * time.Minute
		s = append(s,
			step{at: d, label: fmt.Sprintf("A%d pods", m), data: pullPods(d, pods...)},
			step{at: d + time.Second, label: fmt.Sprintf("B%d nodes", m), data: pullNodes(d+time.Second, nodes...)},
			step{at: d + 30*time.Second, tick: true},
		)
		switch m {
		case 5:
			s = append(s, step{at: 7 * time.Minute, label: "p2 deleted", data: logs("", watchPod(at(7*time.Minute), "DELETED", p2))})
		case 10:
			s = append(s, step{at: 12 * time.Minute, label: "n4 modified", data: logs("", watchNode(at(12*time.Minute), "MODIFIED", n4))})
		}
	}
	for d := 21 * time.Minute; d <= 40*time.Minute; d += 30 * time.Second {
		s = append(s, step{at: d, tick: true})
	}
	return s
}

// runScenario drives the sequencer through the steps and stops it. failAt is the
// label of a request whose first attempt fails with errInjected, which is sent
// again at once, as a collector does, after a 503.
func runScenario(t *testing.T, h *harness, steps []step, failAt string) {
	t.Helper()
	for _, st := range steps {
		h.clock.set(at(st.at))
		if st.tick {
			h.tick()
			continue
		}
		p, err := h.send(st.data)
		if st.label == failAt {
			if !errors.Is(err, otlphttp.ErrBusy) {
				t.Fatalf("%s: the failed write was answered %v, want a 503 (ErrBusy)", st.label, err)
			}
			p, err = h.send(st.data)
		}
		if err != nil || p.Rejected != 0 {
			t.Fatalf("%s: partial %+v, error %v", st.label, p, err)
		}
	}
	h.stop()
}

// referenceRun is the reference path over the steps. restartAt is the label of
// the request before which it restarts. It returns the records and, by step
// label, what each step wrote.
func referenceRun(t *testing.T, steps []step, restartAt string) ([]store.Record, map[string][]store.Record) {
	ref := newReference(t)
	byStep := map[string][]store.Record{}
	for _, st := range steps {
		n := len(ref.written)
		if st.tick {
			ref.flush(at(st.at))
			continue
		}
		if st.label == restartAt {
			ref.restart()
		}
		ref.request(st.data, at(st.at))
		byStep[st.label] = slices.Clone(ref.written[n:])
	}
	ref.flushAll()
	return ref.written, byStep
}

// S1. One pull and a watch stream through Accept, flush ticks at fixed instants,
// then Stop: the store holds what the reference path stores, every record field
// compared, in the order of the commits.
func TestS1MatchesTheReference(t *testing.T) {
	steps := scenario()
	want, _ := referenceRun(t, steps, "")
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	runScenario(t, h, steps, "")
	got := st.records()
	if d := diffRecords(got, want); d != "" {
		t.Fatal(d)
	}
	t.Logf("%d records match the reference, in %d commits", len(got), len(st.committed))
	// The scenario exercises what it is meant to: both producers, a pending edge
	// resolved by a later node, refreshes absorbed and written by a flush, and the
	// flush on stop.
	checkScenarioCoverage(t, got)
}

// checkScenarioCoverage fails unless the records show the paths the scenario is
// built for.
func checkScenarioCoverage(t *testing.T, recs []store.Record) {
	t.Helper()
	var objects, cluster, edges, extensions int
	for _, r := range recs {
		switch r.Producer {
		case "k8sobjects/default":
			objects++
		case "k8scluster/default":
			cluster++
		}
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn {
			edges++
		}
		if r.Kind == lifecycle.Observe && !r.Through.IsZero() && r.EventTime.After(t0) {
			extensions++ // a flush's extension: a run's start, asserted through a later refresh
		}
	}
	if objects == 0 || cluster == 0 || edges == 0 || extensions == 0 {
		t.Errorf("objects %d, cluster %d, scheduled_on %d, extensions %d: the scenario does not cover its paths", objects, cluster, edges, extensions)
	}
}

// S1 on pebblestore: the digest of the records read back from the store equals
// the digest of the reference's, read back from a store it was written to. The
// checkpoints are left out, because they can depend on the commit boundaries.
func TestS1PebbleDigest(t *testing.T) {
	steps := scenario()
	want, byStep := referenceRun(t, steps, "")
	open := func() *pebblestore.Store {
		s, err := pebblestore.Open(t.TempDir(), pebblestore.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	seqStore, refStore := open(), open()
	h := newHarness(t, &fakeStore{inner: seqStore}).start()
	runScenario(t, h, steps, "")

	// The reference's records, written in the batches its steps made.
	ctx := context.Background()
	n := 0
	for _, st := range steps {
		if b := byStep[st.label]; !st.tick && len(b) > 0 {
			if err := refStore.Write(ctx, b); err != nil {
				t.Fatal(err)
			}
			n += len(b)
		}
	}
	if err := refStore.Write(ctx, want[n:]); err != nil { // the flushes and the stop
		t.Fatal(err)
	}
	got, wantDigest := readBack(t, seqStore, want), readBack(t, refStore, want)
	if got != wantDigest {
		t.Fatalf("digest of the records %s, want %s", got, wantDigest)
	}
	if direct := digest(want); got != direct {
		t.Fatalf("digest of the records read back %s, of the reference's records %s", got, direct)
	}
	t.Logf("records digest %s (%d records)", got[:16], len(want))
}

// readBack reads every record of the subjects of recs from s, through the
// store's reads, and returns their digest.
func readBack(t *testing.T, s store.Store, recs []store.Record) string {
	t.Helper()
	ctx := context.Background()
	seen := map[store.Subject]bool{}
	var out []store.Record
	for _, r := range recs {
		if seen[r.Subject] {
			continue
		}
		seen[r.Subject] = true
		sc := store.Current(r.Layer)
		var got []store.Record
		var err error
		if r.Subject.Kind == store.SubjectEntity {
			got, err = s.EntityWindow(ctx, r.Subject.A, store.MinEventTime, store.MaxEventTime, sc)
		} else {
			got, err = s.Window(ctx, r.Subject.A, store.Forward, store.MinEventTime, store.MaxEventTime, sc)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range got {
			if g.Subject == r.Subject {
				out = append(out, g)
			}
		}
	}
	return digest(out)
}

// digest is a SHA-256 of every field of the records, in Seq order.
func digest(recs []store.Record) string {
	recs = slices.Clone(recs)
	slices.SortFunc(recs, func(a, b store.Record) int { return int(a.Seq) - int(b.Seq) })
	h := sha256.New()
	for _, r := range recs {
		fmt.Fprintf(h, "%d|%v|%s|%d|%d|%d|%d|%d|%x|%s|%d\n", r.Layer, r.Subject, r.Producer, r.EventTime.UnixNano(),
			r.Seq, r.Kind, r.TTL, r.Through.UnixNano(), r.Payload, r.Boot, r.EventTimeBasis)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// S2. A write that fails before landing, in the group of a pull whose refreshes
// the coalescer absorbs: every request in it is answered 503, the translators and
// the coalescer are made anew, and the request sent again stores what the
// reference stores after a restart at that request. Against S1 the difference
// is the restart effects, each of which the test finds.
func TestS2FailedWriteResets(t *testing.T) {
	steps := scenario()
	const failAt = "B10 nodes"
	normal, normalBy := referenceRun(t, steps, "")
	want, restartBy := referenceRun(t, steps, failAt)

	st := &fakeStore{inner: newMem(t)}
	var failed bool
	h := newHarness(t, st)
	h.st.inject = func(_ int, batch []store.Record) (bool, error) {
		if !failed && h.clock.now().Equal(at(10*time.Minute+time.Second)) {
			failed = true
			return false, errInjected
		}
		return true, nil
	}
	h.start()
	runScenario(t, h, steps, failAt)
	if !failed {
		t.Fatal("the write was not failed")
	}
	if d := diffRecords(st.records(), want); d != "" {
		t.Fatal(d)
	}

	// The restart effects, against the run without the failure.
	if slices.EqualFunc(normal, want, func(a, b store.Record) bool { return diffRecord(a, b) == "" }) {
		t.Fatal("the restart changed nothing")
	}
	objects := lifecycle.Producer("k8sobjects/default")
	// The coalescer's gap: the failed group's refreshes were absorbed without the
	// failure, and start new runs after it.
	if n := countBy(normalBy[failAt], objects, catalog.K8sNode); n != 0 {
		t.Errorf("without the failure the node refreshes at 10 min wrote %d records, want them absorbed", n)
	}
	// The first records of each UID, asserted again at creation with Through.
	reasserted := false
	for _, r := range restartBy[failAt] {
		if r.Producer == objects && r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sNode &&
			r.EventTime.Equal(n1.created) && r.Through.Equal(at(10*time.Minute+time.Second)) {
			reasserted = true
		}
	}
	if !reasserted {
		t.Error("node n1 was not asserted again at its creation, through the retry")
	}
	// The pending scheduled_on edge of p5 is lost: n4's arrival at 12 min resolves
	// it without the failure, and only the next pull of p5 (15 min) after it.
	if n := countEdges(normalBy["n4 modified"], objects); n != 1 {
		t.Errorf("without the failure n4's arrival wrote %d scheduled_on edges, want 1", n)
	}
	if n := countEdges(restartBy["n4 modified"], objects); n != 0 {
		t.Errorf("after the failure n4's arrival wrote %d scheduled_on edges, want 0: the pending edge is lost", n)
	}
	if n := countEdges(restartBy["A15 pods"], objects); n == 0 {
		t.Error("after the failure the next pull of p5 did not bring its edge back")
	}
}

func countBy(recs []store.Record, p lifecycle.Producer, typ catalog.EntityType) int {
	n := 0
	for _, r := range recs {
		if r.Producer == p && r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == typ {
			n++
		}
	}
	return n
}

func countEdges(recs []store.Record, p lifecycle.Producer) int {
	n := 0
	for _, r := range recs {
		if r.Producer == p && r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn && r.Kind == lifecycle.Observe {
			n++
		}
	}
	return n
}

// S2, the last restart effect: the kube-system set is forgotten. A second
// kube-system UID seen during the warmup, a failed write, and then the delete of
// that UID: the delete gives no sighting, so once the warmup ends the name is
// refused until the UID lapses, three collection intervals after its state.
// Without the failure the delete ends it, and nothing is refused.
func TestS2ForgetsTheKubeSystemSet(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure %v", fail), func(t *testing.T) {
			st := &fakeStore{inner: newMem(t)}
			h := newHarness(t, st)
			failed := false
			h.st.inject = func(int, []store.Record) (bool, error) {
				if fail && !failed && h.clock.now().Equal(at(2*time.Minute)) {
					failed = true
					return false, errInjected
				}
				return true, nil
			}
			h.start()
			h.clock.set(at(time.Minute))
			h.sendOK(logs("", nsState("ks-1", "kube-system", at(time.Minute)), nsState("ks-2", "kube-system", at(time.Minute)), pullPod(at(time.Minute), p1)))
			h.clock.set(at(2 * time.Minute))
			req := logs("", pullPod(at(2*time.Minute), p2))
			if _, err := h.send(req); fail != (err != nil) {
				t.Fatalf("the request at 2 min: %v", err)
			}
			if fail {
				h.sendOK(req)
			}
			h.clock.set(at(3 * time.Minute))
			h.sendOK(logs("", nsDelete("ks-2", at(3*time.Minute))))
			// ks-1 stays alive: its state is repeated.
			h.clock.set(at(6 * time.Minute))
			_, err := h.send(logs("", nsState("ks-1", "kube-system", at(6*time.Minute)), pullPod(at(6*time.Minute), p3)))
			var refused *otlphttp.RefusedError
			if fail != errors.As(err, &refused) {
				t.Fatalf("after the warmup: %v (refusal expected: %v)", err, fail)
			}
			if fail {
				// ks-2 vouches until 1 min plus three report intervals of 5 min.
				h.clock.set(at(16 * time.Minute))
				h.sendOK(logs("", nsState("ks-1", "kube-system", at(16*time.Minute)), pullPod(at(16*time.Minute), p3)))
			}
		})
	}
}

// S3. A write that fails but landed (the last sequence number advanced): every
// request is answered 200, nothing is reset, and the next group follows without
// a duplicate: the store equals S1's.
func TestS3FailedWriteThatLanded(t *testing.T) {
	steps := scenario()
	want, _ := referenceRun(t, steps, "")
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st)
	landed := false
	h.st.inject = func(_ int, batch []store.Record) (bool, error) {
		if !landed && len(batch) > 0 && h.clock.now().Equal(at(5*time.Minute)) {
			landed = true
			return true, errInjected // stored, and an error all the same
		}
		return true, nil
	}
	h.start()
	runScenario(t, h, steps, "")
	if !landed {
		t.Fatal("no write landed with an error")
	}
	if d := diffRecords(st.records(), want); d != "" {
		t.Fatal(d)
	}
}
