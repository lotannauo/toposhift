package k8scluster_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func marked(t *testing.T, r k8scluster.Result, basis store.EventTimeBasis) []store.Subject {
	t.Helper()
	var out []store.Subject
	for _, m := range r.Backdated {
		rec := r.Records[m.Index]
		if rec.Kind != lifecycle.Observe || !rec.Through.After(rec.EventTime) {
			t.Errorf("mark on a record that is not a backdated Observe: %+v", rec)
		}
		if m.ObservedBasis != basis {
			t.Errorf("mark with observed basis %s, want %s", m.ObservedBasis, basis)
		}
		out = append(out, rec.Subject)
	}
	return out
}

func subjectsEqual(a, b []store.Subject) bool {
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

// The first record of a node, a pod and a container, and a first container's
// part_of edge, timed at their creation, are marked, with the basis of the
// record's instant. A later state is not, nor a scheduled_on edge begun at the
// instant of the record that names its node.
func TestFirstRecordsAreMarked(t *testing.T) {
	tr := newTranslator(t)
	node, pod, ctr := nodeFP(t, "n1-uid"), podFP(t, "p1-uid"), ctrFP(t, "abc")
	created := t0.Add(-time.Hour)
	rn := translate(t, tr, nodeEv("n1-uid", "n1", t0, created))
	if got := marked(t, rn, store.BasisProducerEvent); !subjectsEqual(got, []store.Subject{store.EntitySubject(node)}) {
		t.Errorf("node: marked %v", got)
	}
	rp := translate(t, tr, podEv("p1-uid", "p1", "n1", t0, created))
	if got := marked(t, rp, store.BasisProducerEvent); !subjectsEqual(got, []store.Subject{store.EntitySubject(pod)}) {
		t.Errorf("pod with a known node: marked %v, want the pod alone", got)
	}
	if e := edgesOf(rp.Records, catalog.ScheduledOn); len(e) != 1 || !e[0].Through.IsZero() {
		t.Errorf("the pod's edge: %+v, want one, at its instant", e)
	}
	rc := translate(t, tr, ctrEv("containerd://abc", "p1-uid", "c", t0, created))
	want := []store.Subject{store.EntitySubject(ctr), store.EdgeSubject(ctr, pod, catalog.PartOf)}
	if got := marked(t, rc, store.BasisProducerEvent); !subjectsEqual(got, want) {
		t.Errorf("container: marked %v, want %v", got, want)
	}
	for _, e := range []ent{nodeEv("n1-uid", "n1", t0.Add(time.Minute), created), podEv("p1-uid", "p1", "n1", t0.Add(time.Minute), created), ctrEv("containerd://abc", "p1-uid", "c", t0.Add(time.Minute), created)} {
		if r := translate(t, tr, e); len(r.Backdated) != 0 {
			t.Errorf("a later %s state marks %v", e.Type, marked(t, r, store.BasisProducerEvent))
		}
	}
	// On the receipt clock the mark takes the receipt basis.
	tr2 := newTranslator(t, distrust)
	r := translateAt(t, tr2, nodeEv("n1-uid", "n1", t0, created), t0)
	if got := marked(t, r, store.BasisReceipt); len(got) != 1 {
		t.Errorf("node on the receipt clock: marked %v", got)
	}
}

// A scheduled_on edge that waited for its node is asserted through its pod's
// latest instant, and is marked in the node's result.
func TestAnEdgeThatWaitedForItsNodeIsMarked(t *testing.T) {
	tr := newTranslator(t)
	node, pod := nodeFP(t, "n1-uid"), podFP(t, "p1-uid")
	created := t0.Add(-time.Hour)
	translate(t, tr, podEv("p1-uid", "p1", "n1", t0, created))
	translate(t, tr, podEv("p1-uid", "p1", "n1", t0.Add(time.Minute), created))
	rn := translate(t, tr, nodeEv("n1-uid", "n1", t0.Add(2*time.Minute), created))
	want := []store.Subject{store.EntitySubject(node), store.EdgeSubject(pod, node, catalog.ScheduledOn)}
	if got := marked(t, rn, store.BasisProducerEvent); !subjectsEqual(got, want) {
		t.Fatalf("node's result: marked %v, want the node and the edge", got)
	}
	edge := rn.Records[rn.Backdated[1].Index]
	wantTimes(t, "resolved edge", edge, t0, store.BasisProducerEvent, t0.Add(time.Minute))
}

func handle(t *testing.T, r k8scluster.Result, sub store.Subject) any {
	t.Helper()
	for _, m := range r.Backdated {
		if r.Records[m.Index].Subject == sub {
			return m.Handle
		}
	}
	t.Fatalf("no mark of %v", sub)
	return nil
}

// Resumed raises the state that times a subject's later records, and never lowers
// it: a later state whose instant is before the resumed one is held at it, with
// its basis, for the node, the pod, the container and the part_of edge.
func TestResumedRaisesTheStateAndNeverLowersIt(t *testing.T) {
	tr := newTranslator(t)
	node, pod, ctr := nodeFP(t, "n1-uid"), podFP(t, "p1-uid"), ctrFP(t, "abc")
	created := t0.Add(-time.Hour)
	rn := translate(t, tr, nodeEv("n1-uid", "n1", t0, created))
	rp := translate(t, tr, podEv("p1-uid", "p1", "", t0, created))
	rc := translate(t, tr, ctrEv("containerd://abc", "p1-uid", "c", t0, created))
	partOf := store.EdgeSubject(ctr, pod, catalog.PartOf)
	resumed := t0.Add(30 * time.Second)
	for _, x := range []struct {
		r   k8scluster.Result
		sub store.Subject
	}{{rn, store.EntitySubject(node)}, {rp, store.EntitySubject(pod)}, {rc, store.EntitySubject(ctr)}, {rc, partOf}} {
		h := handle(t, x.r, x.sub)
		tr.Resumed(h, resumed, store.BasisObserved)
		tr.Resumed(h, t0.Add(-30*time.Minute), store.BasisObjectField) // lower: ignored
	}
	tr.Resumed(nil, t0.Add(time.Hour), store.BasisObserved)
	tr.Resumed(42, t0.Add(time.Hour), store.BasisObserved)

	at := t0.Add(10 * time.Second) // before the resumed instant
	for _, e := range []ent{nodeEv("n1-uid", "n1", at, created), podEv("p1-uid", "p1", "", at, created), ctrEv("containerd://abc", "p1-uid", "c", at, created)} {
		r := translate(t, tr, e)
		for _, rec := range r.Records {
			if !rec.EventTime.Equal(resumed) || rec.EventTimeBasis != store.BasisObserved {
				t.Errorf("%s state: record of %v at %s basis %s, want held at the resumed instant", e.Type, rec.Subject.Relation, rec.EventTime.Sub(t0), rec.EventTimeBasis)
			}
		}
	}
}

// The description of an unchanged object is the same in two reports, whatever
// else the reports carry (labels, owner uids, the interval), so that Comparable
// finds it unchanged; a scheduled_on edge is compared on its node_name alone.
func TestDescriptionsAreStableAcrossReports(t *testing.T) {
	created := t0.Add(-time.Hour)
	first := func(e ent) store.Record {
		t.Helper()
		r := translate(t, newTranslator(t), e)
		for _, rec := range r.Records {
			if rec.Subject.Kind == store.SubjectEntity {
				return rec
			}
		}
		t.Fatalf("no entity record: %s", describeRecords(r.Records))
		return store.Record{}
	}
	for _, x := range []struct{ a, b ent }{
		{nodeEv("n1-uid", "n1", t0, created), nodeEv("n1-uid", "n1", t0.Add(time.Minute), created).with("k8s.node.label.zone", "a").interval(300)},
		{podEv("p1-uid", "p1", "n1", t0, created), podEv("p1-uid", "p1", "n1", t0.Add(time.Minute), created).with("k8s.pod.label.app", "x").with("k8s.replicaset.uid", "rs")},
		{ctrEv("containerd://abc", "p1-uid", "c", t0, created), ctrEv("containerd://abc", "p1-uid", "c", t0.Add(time.Minute), created).with("container.label.x", "y")},
	} {
		a, b := first(x.a), first(x.b)
		if !bytes.Equal(k8scluster.Comparable(a.Subject, a.Payload), k8scluster.Comparable(b.Subject, b.Payload)) {
			t.Errorf("%s: two reports of an unchanged object differ: %s and %s", x.a.Type, a.Payload, b.Payload)
		}
	}
	changed := podEv("p1-uid", "p1", "n1", t0, created).with("k8s.pod.phase", "Pending")
	a, b := first(podEv("p1-uid", "p1", "n1", t0, created)), first(changed)
	if bytes.Equal(k8scluster.Comparable(a.Subject, a.Payload), k8scluster.Comparable(b.Subject, b.Payload)) {
		t.Error("a changed phase compares equal")
	}

	edge := store.EdgeSubject(podFP(t, "p1-uid"), nodeFP(t, "n1-uid"), catalog.ScheduledOn)
	p1 := []byte(`{"node_name":"n1","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-09T15:00:00Z"}}`)
	p2 := []byte(`{"node_name":"n1","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-09T16:00:00Z"}}`)
	p3 := []byte(`{"node_name":"n2","resolution":{"basis":"node-name-index","node_interval_start":"2026-10-09T15:00:00Z"}}`)
	if !bytes.Equal(k8scluster.Comparable(edge, p1), k8scluster.Comparable(edge, p2)) || bytes.Equal(k8scluster.Comparable(edge, p1), k8scluster.Comparable(edge, p3)) {
		t.Error("a scheduled_on edge is not compared on its node_name alone")
	}
}

var _ resume.Resumer = (*k8scluster.Translator)(nil)
