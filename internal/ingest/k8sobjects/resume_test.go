package k8sobjects_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

var r0 = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

func rm(m int) time.Time { return r0.Add(time.Duration(m) * time.Minute) }

// resumePod is a pod on node n1, created an hour before r0 and scheduled 30
// minutes before it, with one container that has a runtime id.
func resumePod(conds ...map[string]any) podSpec {
	return podSpec{
		UID: "p1", Name: "p1", Node: "n1", Created: rm(-60), Phase: "Running",
		Conds:      append([]map[string]any{cond("PodScheduled", "True", rm(-30))}, conds...),
		Containers: []map[string]any{{"name": "c", "containerID": "containerd://abc", "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": ts(rm(-29))}}}},
	}
}

// markedSubjects lists the subjects of a result's marked records, and checks that
// each mark names a backdated Observe.
func markedSubjects(t *testing.T, r k8sobjects.Result) []store.Subject {
	t.Helper()
	var out []store.Subject
	for _, m := range r.Backdated {
		rec := r.Records[m.Index]
		if rec.Kind != lifecycle.Observe || !rec.Through.After(rec.EventTime) {
			t.Errorf("mark on a record that is not a backdated Observe: %+v", rec)
		}
		if m.ObservedBasis != store.BasisObserved {
			t.Errorf("mark with observed basis %s, want observed", m.ObservedBasis)
		}
		out = append(out, rec.Subject)
	}
	return out
}

func sameSubjects(a, b []store.Subject) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Every first sighting the translator backdates is marked: a node, a pod, its
// container and part_of edge, and its scheduled_on edge at PodScheduled. A later
// list item is timed at its observation and marks nothing, and neither does a
// later watch event timed by its own change, though it carries a Through.
func TestFirstSightingsAreMarked(t *testing.T) {
	tr := newTranslator(t, 15*time.Minute)
	pod, node, ctr := podFP(t, "p1"), nodeFP(t, "n1-uid"), containerFP(t, "abc")
	rn := translate(t, tr, pullRec(r0, nodeObject("n1-uid", "n1", rm(-120), "1")))
	if got, want := markedSubjects(t, rn), []store.Subject{store.EntitySubject(node)}; !sameSubjects(got, want) {
		t.Errorf("node's first record: marked %v, want the node", got)
	}
	rp := translate(t, tr, pullRec(r0, resumePod().object()))
	want := []store.Subject{
		store.EntitySubject(pod), store.EntitySubject(ctr),
		store.EdgeSubject(ctr, pod, catalog.PartOf), store.EdgeSubject(pod, node, catalog.ScheduledOn),
	}
	if got := markedSubjects(t, rp); !sameSubjects(got, want) {
		t.Errorf("pod's first record: marked %v, want %v", got, want)
	}
	if r := translate(t, tr, pullRec(rm(5), resumePod().object())); len(r.Backdated) != 0 {
		t.Errorf("a later list item marks %v", markedSubjects(t, r))
	}
	r := translate(t, tr, watchRec(rm(6), "MODIFIED", resumePod(cond("Ready", "True", rm(3))).object()))
	if len(r.Backdated) != 0 {
		t.Errorf("a later watch event timed by its change marks %v", markedSubjects(t, r))
	}
	if rec := recordOf(t, r, store.EntitySubject(pod)); !rec.Through.After(rec.EventTime) {
		t.Errorf("the watch event's pod record has no Through, so it does not show that a Through alone marks nothing: %+v", rec)
	}
}

// A pod listed before its node (as the first pull after a reset can) leaves its
// edge pending; the node's record resolves it at PodScheduled, through the pod's
// observation, and that edge is marked in the node's result.
func TestAnEdgeResolvedFromPendingIsMarked(t *testing.T) {
	tr := newTranslator(t, 15*time.Minute)
	pod, node := podFP(t, "p1"), nodeFP(t, "n1-uid")
	rp := translate(t, tr, pullRec(r0, resumePod().object()))
	if len(rp.Pending) != 1 {
		t.Fatalf("pending %v, want the pod's edge", rp.Pending)
	}
	for _, s := range markedSubjects(t, rp) {
		if s.Kind == store.SubjectEdge && s.Relation == catalog.ScheduledOn {
			t.Errorf("a pending edge is marked before it is emitted")
		}
	}
	rn := translate(t, tr, pullRec(r0.Add(time.Second), nodeObject("n1-uid", "n1", rm(-120), "1")))
	want := []store.Subject{store.EntitySubject(node), store.EdgeSubject(pod, node, catalog.ScheduledOn)}
	if got := markedSubjects(t, rn); !sameSubjects(got, want) {
		t.Fatalf("node's record: marked %v, want the node and the resolved edge", got)
	}
	edge := rn.Records[rn.Backdated[1].Index]
	if !edge.EventTime.Equal(rm(-30)) || !edge.Through.Equal(r0) {
		t.Errorf("resolved edge at %s through %s, want PodScheduled through the pod's observation", edge.EventTime, edge.Through)
	}
}

// handleOf is the handle of the mark of a subject in a result.
func handleOf(t *testing.T, r k8sobjects.Result, sub store.Subject) any {
	t.Helper()
	for _, m := range r.Backdated {
		if r.Records[m.Index].Subject == sub {
			return m.Handle
		}
	}
	t.Fatalf("no mark of %v", sub)
	return nil
}

// recordOf is the record of a subject in a result.
func recordOf(t *testing.T, r k8sobjects.Result, sub store.Subject) store.Record {
	t.Helper()
	for _, rec := range r.Records {
		if rec.Subject == sub {
			return rec
		}
	}
	t.Fatalf("no record of %v in %s", sub, joinSubjects(r.Records))
	return store.Record{}
}

// Resumed raises the state that times a subject's later records, and never lowers
// it: after a raise, a watch event whose change is timed between the creation and
// the resumed instant is held at that instant, with its basis; a second, lower
// Resumed changes nothing.
func TestResumedRaisesAndNeverLowers(t *testing.T) {
	tr := newTranslator(t, 15*time.Minute)
	pod, node, ctr := podFP(t, "p1"), nodeFP(t, "n1-uid"), containerFP(t, "abc")
	rn := translate(t, tr, pullRec(r0, nodeObject("n1-uid", "n1", rm(-120), "1")))
	rp := translate(t, tr, pullRec(r0, resumePod().object()))
	edge, partOf := store.EdgeSubject(pod, node, catalog.ScheduledOn), store.EdgeSubject(ctr, pod, catalog.PartOf)
	for _, s := range []store.Subject{store.EntitySubject(pod), edge, partOf} {
		tr.Resumed(handleOf(t, rp, s), rm(-10), store.BasisObserved)
		tr.Resumed(handleOf(t, rp, s), rm(-40), store.BasisObjectField) // lower: ignored
	}
	tr.Resumed(handleOf(t, rn, store.EntitySubject(node)), rm(-5), store.BasisObserved)
	tr.Resumed(nil, rm(100), store.BasisObserved)            // no handle: nothing
	tr.Resumed("not a handle", rm(100), store.BasisObserved) // a foreign one: nothing

	// The pod's change is timed 20 minutes before r0, after its creation and before
	// the resumed instant.
	r := translate(t, tr, watchRec(rm(1), "MODIFIED", resumePod(cond("Ready", "True", rm(-20))).object()))
	for _, s := range []store.Subject{store.EntitySubject(pod), store.EntitySubject(ctr), edge, partOf} {
		rec := recordOf(t, r, s)
		if !rec.EventTime.Equal(rm(-10)) || rec.EventTimeBasis != store.BasisObserved {
			t.Errorf("%v: at %s basis %s, want held at the resumed instant, basis observed", s, rec.EventTime.Sub(r0), rec.EventTimeBasis)
		}
	}
	if !hasSkip(r, k8sobjects.SkipClampedTime) {
		t.Error("the hold is not counted")
	}
	// The node's next change, timed before its resumed instant, is held too.
	n := translate(t, tr, watchRec(rm(3), "MODIFIED", map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "n1-uid", "name": "n1", "creationTimestamp": ts(rm(-120))},
		"status":   map[string]any{"conditions": []any{cond("Ready", "False", rm(-8))}},
	}))
	if rec := only(t, n); !rec.EventTime.Equal(rm(-5)) || rec.EventTimeBasis != store.BasisObserved {
		t.Errorf("node change at %s basis %s, want held at its resumed instant", rec.EventTime.Sub(r0), rec.EventTimeBasis)
	}
}

// A container's entity has a state of its own: resumed to a time later than its
// pod's, its later records are held at that time, though the pod's are held at
// the pod's own.
func TestAContainerIsHeldAtItsOwnResumedTime(t *testing.T) {
	tr := newTranslator(t, 15*time.Minute)
	pod, ctr := podFP(t, "p1"), containerFP(t, "abc")
	translate(t, tr, pullRec(r0, nodeObject("n1-uid", "n1", rm(-120), "1")))
	rp := translate(t, tr, pullRec(r0, resumePod().object()))
	tr.Resumed(handleOf(t, rp, store.EntitySubject(pod)), rm(-20), store.BasisObserved)
	tr.Resumed(handleOf(t, rp, store.EntitySubject(ctr)), rm(-5), store.BasisObserved)
	tr.Resumed(handleOf(t, rp, store.EntitySubject(ctr)), rm(-30), store.BasisObjectField) // lower: ignored
	r := translate(t, tr, watchRec(rm(1), "MODIFIED", resumePod(cond("Ready", "True", rm(-10))).object()))
	if rec := recordOf(t, r, store.EntitySubject(pod)); !rec.EventTime.Equal(rm(-10)) {
		t.Errorf("pod at %s, want its change's time", rec.EventTime.Sub(r0))
	}
	if rec := recordOf(t, r, store.EntitySubject(ctr)); !rec.EventTime.Equal(rm(-5)) || rec.EventTimeBasis != store.BasisObserved {
		t.Errorf("container at %s basis %s, want held at its own resumed time", rec.EventTime.Sub(r0), rec.EventTimeBasis)
	}
	// Its DELETED is held there too, and so is its part_of edge's at the edge's.
	del := translate(t, tr, watchRec(rm(-6), "DELETED", resumePod().object()))
	if rec := recordOf(t, del, store.EntitySubject(ctr)); rec.EventTime.Before(rm(-5)) {
		t.Errorf("container's Delete at %s, before its resumed time", rec.EventTime.Sub(r0))
	}
}

// Comparable leaves out what a scheduled_on edge's payload says of its resolution,
// which the translator derives from its own node index, and keeps every other
// payload whole.
func TestComparableIsTheDescription(t *testing.T) {
	pod, node := podFP(t, "p1"), nodeFP(t, "n1-uid")
	edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
	a := []byte(`{"node_name":"n1","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-01T08:00:00Z"}}`)
	b := []byte(`{"node_name":"n1","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-01T10:00:00Z"}}`)
	c := []byte(`{"node_name":"n2","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-01T08:00:00Z"}}`)
	if !bytes.Equal(k8sobjects.Comparable(edge, a), k8sobjects.Comparable(edge, b)) {
		t.Error("two resolutions of one node name differ")
	}
	if bytes.Equal(k8sobjects.Comparable(edge, a), k8sobjects.Comparable(edge, c)) {
		t.Error("two node names compare equal")
	}
	for _, s := range []store.Subject{store.EntitySubject(pod), store.EdgeSubject(containerFP(t, "abc"), pod, catalog.PartOf)} {
		if got := k8sobjects.Comparable(s, a); !bytes.Equal(got, a) {
			t.Errorf("%v: Comparable %s, want the payload whole", s, got)
		}
	}
	if got := k8sobjects.Comparable(edge, []byte("not json")); string(got) != "not json" {
		t.Errorf("an unreadable payload: %s, want it whole", got)
	}
}

// The translator's handles are those of resume.Mark, and the translator is a
// resume.Resumer.
var _ resume.Resumer = (*k8sobjects.Translator)(nil)
