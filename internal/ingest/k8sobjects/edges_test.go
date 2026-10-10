package k8sobjects_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func at(min, sec int) time.Time {
	return t0.Add(time.Duration(min)*time.Minute + time.Duration(sec)*time.Second)
}

func edgeOf(t testing.TB, r k8sobjects.Result) store.Record {
	t.Helper()
	for _, rec := range r.Records {
		if rec.Subject.Kind == store.SubjectEdge && rec.Subject.Relation == catalog.ScheduledOn {
			return rec
		}
	}
	t.Fatalf("no scheduled_on record in %s", joinSubjects(r.Records))
	return store.Record{}
}

// A pod created before its node, with no scheduling time to say otherwise, is
// pending. The next record of the pod that finds a covering node begins the edge,
// at that record's own time: later than the truth, never guessed.
func TestPendingEdgeIsRetriedAtLaterRecords(t *testing.T) {
	tr := newTranslator(t, 0)
	translate(t, tr, pullRec(at(7, 0), nodeObject("n-1", node, at(5, 0), "1")))
	p := podSpec{UID: "p1", Name: "p", Node: node, Created: at(0, 0), Phase: "Pending"}
	r := translate(t, tr, pullRec(at(8, 0), p.object()))
	if len(r.Pending) != 1 || len(tr.Pending()) != 1 {
		t.Fatalf("got %d pending, want the pod created before its node to wait", len(r.Pending))
	}

	// Nothing changed, so the pod has no record, but its edge now resolves.
	r = translate(t, tr, pullRec(at(8, 30), p.object()))
	edge := only(t, r)
	if edge.Subject != store.EdgeSubject(podFP(t, "p1"), nodeFP(t, "n-1"), catalog.ScheduledOn) || edge.Kind != lifecycle.Observe {
		t.Fatalf("got %+v, want the scheduled_on edge", edge)
	}
	if !edge.EventTime.Equal(at(8, 30)) || edge.EventTimeBasis != store.BasisObserved || len(tr.Pending()) != 0 {
		t.Errorf("edge at %s basis %s, pending %d; want the record's time, observed, none pending", edge.EventTime, edge.EventTimeBasis, len(tr.Pending()))
	}
	if r := translate(t, tr, pullRec(at(9, 0), p.object())); len(r.Records) != 0 {
		t.Errorf("a later record gave %v, want nothing", r.Records)
	}

	// While no node covers the record's time, the edge stays pending.
	tr = newTranslator(t, 0)
	translate(t, tr, pullRec(at(7, 0), nodeObject("n-1", node, at(10, 0), "1")))
	translate(t, tr, pullRec(at(8, 0), p.object()))
	if r := translate(t, tr, pullRec(at(9, 0), p.object())); len(r.Records) != 0 || len(tr.Pending()) != 1 {
		t.Errorf("got %v, pending %d; want a node not yet created to leave it pending", r.Records, len(tr.Pending()))
	}
}

// A pod first seen already scheduled has its edge begin when it was scheduled, not
// when it was created.
func TestEdgeBeginsWhenThePodWasScheduled(t *testing.T) {
	scheduled := func(status string, when time.Time) podSpec {
		return podSpec{
			UID: "p1", Name: "p", Node: node, Created: at(0, 0), Phase: "Running",
			Conds: []map[string]any{cond("PodScheduled", status, when)},
		}
	}
	// The node was created after the pod and before it was scheduled.
	nodeObj := nodeObject("n-1", node, at(5, 0), "1")

	tr := newTranslator(t, 0)
	translate(t, tr, pullRec(at(7, 0), nodeObj))
	r := translate(t, tr, pullRec(at(8, 0), scheduled("True", at(6, 0)).object()))
	if len(r.Pending) != 0 || len(r.Records) != 2 {
		t.Fatalf("got %d records, %d pending, want the pod and its edge", len(r.Records), len(r.Pending))
	}
	if pod, edge := r.Records[0], edgeOf(t, r); !pod.EventTime.Equal(at(0, 0)) || !edge.EventTime.Equal(at(6, 0)) || edge.EventTimeBasis != store.BasisObjectField {
		t.Errorf("pod at %s, edge at %s basis %s; want creation and the scheduled time, object field", pod.EventTime, edge.EventTime, edge.EventTimeBasis)
	}

	// A pending edge carries the same time.
	tr = newTranslator(t, 0)
	r = translate(t, tr, pullRec(at(8, 0), scheduled("True", at(6, 0)).object()))
	if len(r.Pending) != 1 || !r.Pending[0].Time.Equal(at(6, 0)) || r.Pending[0].Basis != store.BasisObjectField {
		t.Fatalf("pending = %+v, want the scheduled time, object field", r.Pending)
	}
	nr := translate(t, tr, pullRec(at(9, 0), nodeObj))
	if edge := edgeOf(t, nr); !edge.EventTime.Equal(at(6, 0)) || edge.EventTimeBasis != store.BasisObjectField {
		t.Errorf("late edge at %s basis %s, want the scheduled time", edge.EventTime, edge.EventTimeBasis)
	}

	// A condition that is not True, or a time that cannot be trusted, is not used.
	for name, p := range map[string]podSpec{
		"not true":          scheduled("False", at(6, 0)),
		"before creation":   scheduled("True", at(-1, 0)),
		"far in the future": scheduled("True", at(90, 0)),
	} {
		tr = newTranslator(t, 0)
		translate(t, tr, pullRec(at(7, 0), nodeObj))
		// Created at 5:30, after the node, so the creation time resolves.
		p.Created = at(5, 30)
		if name == "before creation" {
			p.Conds = []map[string]any{cond("PodScheduled", "True", at(5, 0))}
		}
		r := translate(t, tr, pullRec(at(8, 0), p.object()))
		if edge := edgeOf(t, r); !edge.EventTime.Equal(at(5, 30)) || edge.EventTimeBasis != store.BasisObjectField {
			t.Errorf("%s: edge at %s basis %s, want the record's own time (creation), object field", name, edge.EventTime, edge.EventTimeBasis)
		}
		wantSkip := 1
		if name == "not true" {
			wantSkip = 0
		}
		if got := skips(r)[k8sobjects.SkipImplausibleTime]; got != wantSkip {
			t.Errorf("%s: %d implausible times counted, want %d", name, got, wantSkip)
		}
	}
}

// An event time raised to the previous record's keeps that record's basis, and is
// counted. This holds for a changed record and for a removal, for pods and nodes.
func TestClampedTimeKeepsTheBasisOfTheRecordItFollows(t *testing.T) {
	check := func(t *testing.T, what string, rec store.Record, wantAt time.Time, wantBasis store.EventTimeBasis, r k8sobjects.Result) {
		t.Helper()
		if !rec.EventTime.Equal(wantAt) || rec.EventTimeBasis != wantBasis || skips(r)[k8sobjects.SkipClampedTime] != 1 {
			t.Errorf("%s: %s at %s basis %s, skips %v; want %s, %s, one %q", what, rec.Kind, rec.EventTime, rec.EventTimeBasis, skips(r), wantAt, wantBasis, k8sobjects.SkipClampedTime)
		}
	}

	t.Run("pod changed", func(t *testing.T) {
		tr := newTranslator(t, 0)
		p := podSpec{UID: "p1", Name: "p", Created: at(0, 0), Phase: "Pending"}
		translate(t, tr, watchRec(at(0, 1), "ADDED", p.object()))
		p.Phase = "Running" // no time of its own: observed
		last := at(1, 0)
		if rec := only(t, translate(t, tr, watchRec(last, "MODIFIED", p.object()))); rec.EventTimeBasis != store.BasisObserved {
			t.Fatalf("setup: %+v", rec)
		}
		// A new condition whose time is older than the previous record.
		p.Conds = []map[string]any{cond("Ready", "True", at(0, 30))}
		r := translate(t, tr, watchRec(at(1, 30), "MODIFIED", p.object()))
		check(t, "pod", only(t, r), last, store.BasisObserved, r)
	})

	t.Run("pod removal", func(t *testing.T) {
		tr := newTranslator(t, 0)
		p := podSpec{UID: "p1", Name: "p", Created: at(0, 0), Phase: "Pending", Conds: []map[string]any{cond("Ready", "True", at(0, 20))}}
		// Created at 0:00, but the first record is at 0:20: no, a first record is at
		// creation. A later one takes an object time ahead of its observed time.
		translate(t, tr, watchRec(at(0, 1), "ADDED", podSpec{UID: "p1", Name: "p", Created: at(0, 0), Phase: "Pending"}.object()))
		ahead := at(0, 20)
		if rec := only(t, translate(t, tr, watchRec(at(0, 10), "MODIFIED", p.object()))); !rec.EventTime.Equal(ahead) || rec.EventTimeBasis != store.BasisObjectField {
			t.Fatalf("setup: %+v", rec)
		}
		r := translate(t, tr, watchRec(at(0, 15), "DELETED", p.object()))
		check(t, "pod", only(t, r), ahead, store.BasisObjectField, r)
	})

	t.Run("node changed and removed", func(t *testing.T) {
		tr := newTranslator(t, 0)
		n := nodeObject("n-1", node, at(0, 0), "1")
		translate(t, tr, watchRec(at(0, 1), "ADDED", n))
		// A new condition ahead of its observed time.
		n["status"].(map[string]any)["conditions"] = []any{
			map[string]any{"type": "Ready", "status": "True", "reason": "KubeletReady", "lastTransitionTime": ts(at(0, 0))},
			map[string]any{"type": "DiskPressure", "status": "False", "lastTransitionTime": ts(at(0, 40))},
		}
		ahead := at(0, 40)
		if rec := only(t, translate(t, tr, watchRec(at(0, 10), "MODIFIED", n))); !rec.EventTime.Equal(ahead) || rec.EventTimeBasis != store.BasisObjectField {
			t.Fatalf("setup: %+v", rec)
		}
		// A change with no time of its own, observed before that.
		n["metadata"].(map[string]any)["labels"] = map[string]any{"kubernetes.io/hostname": "renamed"}
		r := translate(t, tr, watchRec(at(0, 20), "MODIFIED", n))
		check(t, "node changed", only(t, r), ahead, store.BasisObjectField, r)

		r = translate(t, tr, watchRec(at(0, 30), "DELETED", n))
		check(t, "node removal", only(t, r), ahead, store.BasisObjectField, r)
	})
}

// After a restart the state is gone. A pod the translator saw before is unseen,
// and its DELETED must still end what this producer asserted for it: its
// containers, their part_of edges, and its edge to the node.
func TestDeleteOfAnUnseenPodEndsItsContainersAndEdge(t *testing.T) {
	p := podSpec{
		UID: "p1", Name: "p", Node: node, Created: at(1, 0), Phase: "Running",
		Containers: []map[string]any{
			{"name": "app", "restartCount": 0, "containerID": "containerd://abc"},
			{"name": "side", "restartCount": 0, "containerID": "containerd://def"},
			{"name": "bare", "restartCount": 0},
		},
	}
	nodeObj := nodeObject("n-1", node, at(0, 0), "1")

	before := newTranslator(t, 0)
	translate(t, before, pullRec(at(2, 0), nodeObj))
	translate(t, before, pullRec(at(2, 1), p.object()))
	want := translate(t, before, watchRec(at(10, 0), "DELETED", p.object()))

	after := newTranslator(t, 0) // a restart
	translate(t, after, pullRec(at(8, 0), nodeObj))
	got := translate(t, after, watchRec(at(10, 0), "DELETED", p.object()))
	if len(got.Skipped) != 0 {
		t.Errorf("skips = %v, want none", got.Skipped)
	}
	subjects := func(r k8sobjects.Result) []store.Subject {
		var out []store.Subject
		for _, rec := range r.Records {
			if rec.Kind != lifecycle.Delete || !rec.EventTime.Equal(at(10, 0)) || rec.EventTimeBasis != store.BasisObserved {
				t.Errorf("record %+v is not a delete at the observed time", rec)
			}
			out = append(out, rec.Subject)
		}
		return out
	}
	gs, ws := subjects(got), subjects(want)
	if len(gs) != 6 { // two part_of edges, two containers, the edge, the pod
		t.Fatalf("got %d deletes (%s), want 6", len(gs), joinSubjects(got.Records))
	}
	for _, s := range ws {
		if !slices.Contains(gs, s) {
			t.Errorf("the restarted translator did not delete %+v", s)
		}
	}

	// A node name that does not resolve leaves the edge, and says so.
	lone := newTranslator(t, 0)
	r := translate(t, lone, watchRec(at(10, 0), "DELETED", p.object()))
	if len(r.Records) != 5 || skips(r)[k8sobjects.SkipUnseenDelete] != 1 {
		t.Errorf("got %d records, skips %v; want the containers and the pod, and one %q", len(r.Records), skips(r), k8sobjects.SkipUnseenDelete)
	}
}

// With a TTL a container is refreshed with its pod, so both lapse when the pulls
// stop; the edges have no TTL.
func TestContainerLapsesWithItsPod(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	p := podSpec{
		UID: "p1", Name: "p", Created: at(0, 0), Phase: "Running",
		Containers: []map[string]any{{"name": "app", "restartCount": 0, "containerID": "containerd://abc"}},
	}
	var batches [][]store.Record
	for i := 0; i < 3; i++ {
		p.Heartbeat = string(rune('a' + i))
		batches = append(batches, translate(t, tr, pullRec(at(10*i, 0), p.object())).Records)
	}
	s := storeFrom(t, batches...)
	ctx := context.Background()
	last := at(20, 0)
	pod, ctr := podFP(t, "p1"), containerFP(t, "abc")
	for _, c := range []struct {
		at   time.Time
		want bool
	}{{at(0, 0), true}, {last, true}, {last.Add(ttl - time.Second), true}, {last.Add(ttl + time.Minute), false}} {
		for name, fp := range map[string]store.Subject{"pod": store.EntitySubject(pod), "container": store.EntitySubject(ctr)} {
			got, err := s.Alive(ctx, fp.A, c.at, store.Current(catalog.L2))
			if err != nil || got != c.want {
				t.Errorf("%s alive at +%s = %v, %v, want %v", name, c.at.Sub(t0), got, err, c.want)
			}
		}
	}
}

// A pod whose records stop for longer than the TTL lapses, and returns when they
// resume with no DELETED in between. Its existence and its container's show the
// gap. The edges have no TTL: they stay stored throughout, nothing re-asserts
// or deletes them, and none is emitted on the return.
func TestLapsedPodReturnsWithoutNewEdges(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	nodeObj := nodeObject("n-1", node, at(-10, 0), "1")
	p := podSpec{
		UID: "p1", Name: "p", Node: node, Created: at(0, 0), Phase: "Running",
		Conds:      []map[string]any{cond("PodScheduled", "True", at(0, 0))},
		Containers: []map[string]any{{"name": "app", "restartCount": 0, "containerID": "containerd://abc"}},
	}
	// The node is refreshed throughout, so only the pod's lapse is under test.
	var batches [][]store.Record
	pull := func(min int, i int) k8sobjects.Result {
		p.Heartbeat = string(rune('a' + i))
		nodeObj["metadata"].(map[string]any)["resourceVersion"] = p.Heartbeat
		r := translate(t, tr, pullRec(at(min, 0), nodeObj))
		pr := translate(t, tr, pullRec(at(min, 1), p.object()))
		batches = append(batches, r.Records, pr.Records)
		return pr
	}
	first := pull(0, 0)
	if len(first.Records) != 4 {
		t.Fatalf("first sight gave %s, want pod, container, part_of, scheduled_on", joinSubjects(first.Records))
	}
	pull(10, 1)
	for i, min := range []int{70, 80} {
		r := pull(min, i+2)
		for _, rec := range r.Records {
			if rec.Subject.Kind == store.SubjectEdge {
				t.Errorf("the pull at +%dm emitted an edge record %+v; the edges are not re-asserted", min, rec)
			}
		}
		if len(r.Records) != 2 {
			t.Errorf("the pull at +%dm gave %s, want the pod and the container", min, joinSubjects(r.Records))
		}
	}

	s := storeFrom(t, batches...)
	ctx, scope := context.Background(), store.Current(catalog.L2)
	pod, ctr, nodeRef := podFP(t, "p1"), containerFP(t, "abc"), nodeFP(t, "n-1")
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{at(5, 0), true},
		{at(10, 0).Add(ttl - time.Second), true},
		{at(10, 0).Add(ttl + time.Second), false},
		{at(40, 0), false},
		{at(69, 59), false},
		{at(70, 0), false},
		{at(70, 2), true},
		{at(80, 1).Add(ttl - time.Second), true},
	} {
		for name, f := range map[string]identity.Fingerprint{"pod": pod, "container": ctr} {
			got, err := s.Alive(ctx, f, c.at, scope)
			if err != nil || got != c.want {
				t.Errorf("%s alive at +%s = %v, %v, want %v", name, c.at.Sub(t0), got, err, c.want)
			}
		}
	}
	// The references are stored throughout, lapse included.
	for _, when := range []time.Time{at(5, 0), at(40, 0), at(75, 0)} {
		ns, err := s.Neighbors(ctx, pod, store.Forward, when, scope)
		if err != nil || len(ns) != 1 || ns[0].Peer != nodeRef || ns[0].Relation != catalog.ScheduledOn {
			t.Errorf("scheduled_on at +%s = %v, %v, want one edge to the node", when.Sub(t0), ns, err)
		}
		ns, err = s.Neighbors(ctx, ctr, store.Forward, when, scope)
		if err != nil || len(ns) != 1 || ns[0].Peer != pod || ns[0].Relation != catalog.PartOf {
			t.Errorf("part_of at +%s = %v, %v, want one edge to the pod", when.Sub(t0), ns, err)
		}
	}
}

// A pod failed by the garbage collector outlives its node. After a restart the
// translator learns the node only from its DELETED, and the pod's own DELETED
// comes after that: the edge still resolves, at the time the pod was scheduled.
func TestUnseenDeleteOfAPodOutlivingItsNode(t *testing.T) {
	p := podSpec{
		UID: "p1", Name: "p", Node: node, Created: at(1, 0), Phase: "Failed",
		Conds: []map[string]any{cond("PodScheduled", "True", at(2, 0))},
	}
	// The node was created after the pod, so the pod's creation time is not in its
	// interval; the scheduling time is.
	nodeObj := nodeObject("n-1", node, at(1, 30), "1")

	tr := newTranslator(t, 0) // a restart: no state
	nodeRef := translate(t, tr, watchRec(at(10, 0), "DELETED", nodeObj))
	if rec := only(t, nodeRef); rec.Kind != lifecycle.Delete {
		t.Fatalf("node DELETED gave %+v", rec)
	}
	r := translate(t, tr, watchRec(at(11, 0), "DELETED", p.object()))
	if len(r.Records) != 2 || len(r.Skipped) != 0 {
		t.Fatalf("got %s, skips %v; want the edge's delete and the pod's", joinSubjects(r.Records), r.Skipped)
	}
	edge := r.Records[0]
	if edge.Subject != store.EdgeSubject(podFP(t, "p1"), nodeFP(t, "n-1"), catalog.ScheduledOn) || edge.Kind != lifecycle.Delete ||
		!edge.EventTime.Equal(at(11, 0)) || edge.EventTimeBasis != store.BasisObserved {
		t.Errorf("edge record = %+v, want a delete at the observed time", edge)
	}

	// With no scheduling time to go by, it is still counted, not guessed.
	p.Conds = nil
	tr = newTranslator(t, 0)
	translate(t, tr, watchRec(at(10, 0), "DELETED", nodeObj))
	r = translate(t, tr, watchRec(at(11, 0), "DELETED", p.object()))
	if len(r.Records) != 1 || skips(r)[k8sobjects.SkipUnseenDelete] != 1 {
		t.Errorf("got %s, skips %v; want the pod's delete and one %q", joinSubjects(r.Records), r.Skipped, k8sobjects.SkipUnseenDelete)
	}
}
