package k8scluster_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestPodScheduledOnByName(t *testing.T) {
	podAt, podCreated := t0, t0.Add(-time.Hour)
	nodeAt, nodeCreated := t0.Add(30*time.Second), t0.Add(-3*time.Hour)

	t.Run("pending until the node arrives", func(t *testing.T) {
		tr := newTranslator(t)
		r1 := translate(t, tr, podEv("p-1", "p", "node-a", podAt, podCreated))
		if rec := only(t, r1); rec.Subject.A != podFP(t, "p-1") || rec.Kind != lifecycle.Observe {
			t.Errorf("the first state of a pod whose node is unknown gave %s, want the pod only", describeRecords(r1.Records))
		}
		if len(r1.Pending) != 1 || len(tr.Pending()) != 1 {
			t.Fatalf("pending = %v / %v, want one", r1.Pending, tr.Pending())
		}
		p := r1.Pending[0]
		if p.NodeName != "node-a" || p.PodUID != "p-1" || p.Pod != podFP(t, "p-1") || !p.Time.Equal(podAt) || p.Basis != store.BasisProducerEvent {
			t.Errorf("pending edge = %+v, want node-a for p-1 at the instant of the record that named it", p)
		}
		// A later record of the pod, with the node still unknown, keeps it pending
		// from the first.
		translate(t, tr, podEv("p-1", "p", "node-a", podAt.Add(20*time.Second), podCreated))
		if ps := tr.Pending(); len(ps) != 1 || !ps[0].Time.Equal(podAt) {
			t.Errorf("pending after a second record = %+v, want the first", ps)
		}

		// The node arrives, and the edge is written at the time it was pending
		// from, through the latest instant of its pod: not the node's.
		r2 := translate(t, tr, nodeEv("n-1", "node-a", nodeAt, nodeCreated))
		if len(r2.Records) != 2 {
			t.Fatalf("node arrival gave %s, want the node and the edge", describeRecords(r2.Records))
		}
		wantTimes(t, "node", r2.Records[0], nodeCreated, store.BasisObjectField, nodeAt)
		edge := r2.Records[1]
		if edge.Subject.Kind != store.SubjectEdge || edge.Subject.Relation != catalog.ScheduledOn || edge.Subject.A != podFP(t, "p-1") || edge.Subject.B != nodeFP(t, "n-1") {
			t.Fatalf("edge subject = %+v", edge.Subject)
		}
		wantTimes(t, "late-resolved edge", edge, podAt, store.BasisProducerEvent, podAt.Add(20*time.Second))
		if edge.Kind != lifecycle.Observe || edge.TTL != ttl60 {
			t.Errorf("edge kind %s TTL %s, want Observe with %s", edge.Kind, edge.TTL, ttl60)
		}
		if !bytes.Contains(edge.Payload, []byte(`"node_name":"node-a"`)) {
			t.Errorf("edge payload = %s, want the raw node name", edge.Payload)
		}
		if len(tr.Pending()) != 0 {
			t.Errorf("pending after the node arrived = %v", tr.Pending())
		}

		// With a TTL, the edge is re-asserted with each pod state, as it was
		// established.
		for i := 1; i <= 2; i++ {
			at := podAt.Add(time.Duration(i) * time.Minute)
			r := translate(t, tr, podEv("p-1", "p", "node-a", at, podCreated))
			if len(r.Records) != 2 {
				t.Fatalf("pod state %d gave %s, want the pod and the edge", i, describeRecords(r.Records))
			}
			again := r.Records[1]
			if again.Subject != edge.Subject || again.Kind != lifecycle.Observe || again.TTL != ttl60 {
				t.Errorf("re-assertion %d = %+v, want the same edge, Observe, %s", i, again, ttl60)
			}
			wantTimes(t, "re-assertion", again, at, store.BasisProducerEvent, time.Time{})
			if !bytes.Equal(again.Payload, edge.Payload) {
				t.Errorf("re-assertion payload %s, want %s", again.Payload, edge.Payload)
			}
		}
	})

	t.Run("node known first", func(t *testing.T) {
		tr := newTranslator(t)
		translate(t, tr, nodeEv("n-1", "node-a", nodeAt, nodeCreated))
		r := translate(t, tr, podEv("p-1", "p", "node-a", podAt.Add(time.Minute), podCreated))
		if len(r.Records) != 2 || len(r.Pending) != 0 {
			t.Fatalf("gave %s and pending %v, want the pod and the edge", describeRecords(r.Records), r.Pending)
		}
		wantTimes(t, "edge", r.Records[1], podAt.Add(time.Minute), store.BasisProducerEvent, time.Time{})
	})

	t.Run("a pod that names its node later has the edge from then", func(t *testing.T) {
		tr := newTranslator(t)
		translate(t, tr, nodeEv("n-1", "node-a", nodeAt, nodeCreated))
		r := translate(t, tr, podEv("p-1", "p", "", podAt, podCreated))
		if len(edgesOf(r.Records, catalog.ScheduledOn)) != 0 {
			t.Fatalf("a pod with no node gave an edge")
		}
		at := podAt.Add(time.Minute)
		r = translate(t, tr, podEv("p-1", "p", "node-a", at, podCreated))
		edges := edgesOf(r.Records, catalog.ScheduledOn)
		if len(edges) != 1 {
			t.Fatalf("gave %s", describeRecords(r.Records))
		}
		wantTimes(t, "edge", edges[0], at, store.BasisProducerEvent, time.Time{})
	})

	t.Run("a re-assertion is never timed before the edge", func(t *testing.T) {
		tr := newTranslator(t)
		pc, nc := t0.Add(-2*time.Hour), t0.Add(5*time.Second)
		translate(t, tr, podEv("p-1", "p", "n", t0, pc).interval(0))
		r := translate(t, tr, nodeEv("n-1", "n", t0.Add(10*time.Second), nc).interval(0))
		if len(edgesOf(r.Records, catalog.ScheduledOn)) != 0 || len(tr.Pending()) != 1 {
			t.Fatalf("a node created after the pod's time resolved: %s", describeRecords(r.Records))
		}
		// Without a TTL an unchanged pod gives no record, but the pending edge is
		// retried and begins at this record's time.
		t2 := t0.Add(10 * time.Minute)
		r = translate(t, tr, podEv("p-1", "p", "n", t2, pc).interval(0))
		edge := only(t, r)
		if edge.Subject.Relation != catalog.ScheduledOn {
			t.Fatalf("gave %s, want the edge only", describeRecords(r.Records))
		}
		wantTimes(t, "edge", edge, t2, store.BasisProducerEvent, time.Time{})
		if edge.TTL != 0 {
			t.Errorf("edge TTL = %s with no report interval, want 0", edge.TTL)
		}
		// A record from before the edge, now with a TTL, re-asserts the edge at its
		// own time.
		t3 := t0.Add(5 * time.Minute)
		r = translate(t, tr, podEv("p-1", "p", "n", t3, pc).interval(60))
		if len(r.Records) != 2 {
			t.Fatalf("gave %s, want the pod and the edge", describeRecords(r.Records))
		}
		wantTimes(t, "pod", r.Records[0], t3, store.BasisProducerEvent, time.Time{})
		wantTimes(t, "re-assertion", r.Records[1], t2, store.BasisProducerEvent, time.Time{})
		if skipCount(r, k8scluster.SkipClampedTime) != 1 {
			t.Errorf("skips = %v, want the raised time counted once", r.Skipped)
		}
	})
}

func TestTwoLiveNodesUnderOneName(t *testing.T) {
	for _, keep := range []string{"n-A", "n-B"} {
		t.Run("keep "+keep, func(t *testing.T) {
			tr := newTranslator(t)
			translate(t, tr, nodeEv("n-A", "dup", t0, t0.Add(-3*time.Hour)))
			translate(t, tr, nodeEv("n-B", "dup", t0.Add(10*time.Second), t0.Add(-2*time.Hour)))
			podCreated := t0.Add(-time.Hour)

			r := translate(t, tr, podEv("p-1", "p", "dup", t0.Add(20*time.Second), podCreated))
			if got := only(t, r); got.Subject.A != podFP(t, "p-1") {
				t.Fatalf("gave %s, want the pod and no edge", describeRecords(r.Records))
			}
			if skipCount(r, k8scluster.SkipAmbiguousNodeName) < 1 {
				t.Errorf("skips = %v, want %q", r.Skipped, k8scluster.SkipAmbiguousNodeName)
			}
			if len(r.Pending) != 1 {
				t.Errorf("pending = %v, want the edge kept pending", r.Pending)
			}

			// Once one of the two is deleted, the next pod state resolves to the
			// other.
			gone := "n-A"
			if keep == "n-A" {
				gone = "n-B"
			}
			translate(t, tr, deleteEv("k8s.node", gone, t0.Add(30*time.Second)))
			r = translate(t, tr, podEv("p-1", "p", "dup", t0.Add(40*time.Second), podCreated))
			edges := edgesOf(r.Records, catalog.ScheduledOn)
			if len(edges) != 1 {
				t.Fatalf("gave %s, want the pod and the edge", describeRecords(r.Records))
			}
			if edges[0].Subject.B != nodeFP(t, keep) {
				t.Errorf("edge goes to %v, want the node that is left (%s)", edges[0].Subject.B, keep)
			}
			if len(tr.Pending()) != 0 {
				t.Errorf("pending = %v", tr.Pending())
			}
		})
	}
}

func TestLapsedNodeFreesItsName(t *testing.T) {
	tr := newTranslator(t)
	created := t0.Add(-time.Hour)
	translate(t, tr, nodeEv("n-A", "n", t0, created))
	translate(t, tr, nodeEv("n-A", "n", t0.Add(time.Minute), created))

	// A is the one node bearing the name.
	r := translate(t, tr, podEv("p-0", "p0", "n", t0.Add(80*time.Second), time.Time{}))
	if edges := edgesOf(r.Records, catalog.ScheduledOn); len(edges) != 1 || edges[0].Subject.B != nodeFP(t, "n-A") {
		t.Fatalf("with one node: gave %s", describeRecords(r.Records))
	}

	// B bears it too, and A's delete is lost.
	bAt := t0.Add(90 * time.Second)
	translate(t, tr, nodeEv("n-B", "n", bAt, bAt))
	r = translate(t, tr, podEv("p-1", "p", "n", t0.Add(120*time.Second), time.Time{}))
	if len(edgesOf(r.Records, catalog.ScheduledOn)) != 0 || skipCount(r, k8scluster.SkipAmbiguousNodeName) < 1 {
		t.Fatalf("two live nodes: gave %s and skips %v", describeRecords(r.Records), r.Skipped)
	}
	translate(t, tr, nodeEv("n-B", "n", t0.Add(150*time.Second), bAt))

	// A was last asserted at +60s; its interval ends a TTL (3 minutes) after, at
	// +240s, which is no longer inside it.
	r = translate(t, tr, podEv("p-1", "p", "n", t0.Add(239*time.Second), time.Time{}))
	if len(edgesOf(r.Records, catalog.ScheduledOn)) != 0 {
		t.Fatalf("just before A lapses: gave %s, want no edge", describeRecords(r.Records))
	}
	r = translate(t, tr, podEv("p-1", "p", "n", t0.Add(240*time.Second), time.Time{}))
	edges := edgesOf(r.Records, catalog.ScheduledOn)
	if len(edges) != 1 || edges[0].Subject.B != nodeFP(t, "n-B") {
		t.Fatalf("when A has lapsed: gave %s, want the edge to n-B", describeRecords(r.Records))
	}
	wantTimes(t, "edge", edges[0], t0.Add(240*time.Second), store.BasisProducerEvent, time.Time{})
}

func TestContainerPartOfPod(t *testing.T) {
	const id = "containerd://abc123"
	created := t0.Add(-time.Hour)
	mk := func(at time.Time) ent {
		e := ctrEv(id, "p-1", "web", at, created)
		e.Desc["container.image.name"] = "nginx"
		e.Desc["container.image.tag"] = "1.27"
		return e
	}
	tr := newTranslator(t)

	r := translate(t, tr, mk(t0))
	if len(r.Records) != 2 {
		t.Fatalf("first state gave %s, want the container and its edge", describeRecords(r.Records))
	}
	c, e := r.Records[0], r.Records[1]
	if c.Subject.Kind != store.SubjectEntity || c.Subject.A != ctrFP(t, "abc123") || c.Layer != catalog.L2 {
		t.Errorf("container subject = %+v layer %s", c.Subject, c.Layer)
	}
	if e.Subject.Kind != store.SubjectEdge || e.Subject.Relation != catalog.PartOf || e.Subject.A != ctrFP(t, "abc123") || e.Subject.B != podFP(t, "p-1") {
		t.Errorf("edge subject = %+v, want part_of from the container to the pod named by k8s.pod.uid", e.Subject)
	}
	wantPayload := `{"container.creation_timestamp":"` + ts(created) + `","container.image.name":"nginx","container.image.tag":"1.27","container.status":"running","k8s.container.name":"web"}`
	if string(c.Payload) != wantPayload {
		t.Errorf("container payload = %s, want %s", c.Payload, wantPayload)
	}
	for _, rec := range r.Records {
		wantTimes(t, "first state of "+string(rec.Subject.Relation), rec, created, store.BasisObjectField, t0)
		if rec.TTL != ttl60 {
			t.Errorf("TTL = %s, want %s", rec.TTL, ttl60)
		}
	}

	// Both are re-asserted with each record of the container.
	for i := 1; i <= 2; i++ {
		at := t0.Add(time.Duration(i) * time.Minute)
		r = translate(t, tr, mk(at))
		if len(r.Records) != 2 || r.Records[0].Subject != c.Subject || r.Records[1].Subject != e.Subject {
			t.Fatalf("state %d gave %s, want the container and its edge again", i, describeRecords(r.Records))
		}
		for _, rec := range r.Records {
			wantTimes(t, "re-assertion", rec, at, store.BasisProducerEvent, time.Time{})
			if rec.TTL != ttl60 || rec.Kind != lifecycle.Observe {
				t.Errorf("re-assertion %s TTL %s", rec.Kind, rec.TTL)
			}
		}
	}

	// The delete ends both, under either spelling of the id.
	at := t0.Add(3 * time.Minute)
	r = translate(t, tr, deleteEv("container", "abc123", at))
	if len(r.Records) != 2 {
		t.Fatalf("delete gave %s, want the edge and the container", describeRecords(r.Records))
	}
	if r.Records[0].Subject != e.Subject || r.Records[1].Subject != c.Subject {
		t.Errorf("delete subjects = %+v, %+v", r.Records[0].Subject, r.Records[1].Subject)
	}
	for _, rec := range r.Records {
		if rec.Kind != lifecycle.Delete || rec.TTL != 0 || !rec.Through.IsZero() || len(rec.Payload) != 0 {
			t.Errorf("delete record = %+v", rec)
		}
		wantTimes(t, "delete", rec, at, store.BasisProducerEvent, time.Time{})
	}
	if hasSkip(r, k8scluster.SkipUnseenDelete) {
		t.Errorf("delete of a known container counted as unseen")
	}

	// A container that names no pod is an entity with no edge.
	tr = newTranslator(t)
	r = translate(t, tr, ctrEv(id, "", "web", t0, created))
	if got := only(t, r); got.Subject.A != ctrFP(t, "abc123") {
		t.Errorf("gave %s", describeRecords(r.Records))
	}
	if skipCount(r, k8scluster.SkipContainerNoPod) != 1 {
		t.Errorf("skips = %v, want %q", r.Skipped, k8scluster.SkipContainerNoPod)
	}
	r = translate(t, tr, ctrEv(id, "", "web", t0.Add(time.Minute), created).with("k8s.pod.uid", "  "))
	if len(edgesOf(r.Records, catalog.PartOf)) != 0 || skipCount(r, k8scluster.SkipContainerNoPod) != 1 {
		t.Errorf("a blank pod uid: gave %s and skips %v", describeRecords(r.Records), r.Skipped)
	}

	// A delete of a container this translator never saw is written, and counted.
	r = translate(t, newTranslator(t), deleteEv("container", "zzz", t0))
	if got := only(t, r); got.Kind != lifecycle.Delete || !hasSkip(r, k8scluster.SkipUnseenDelete) {
		t.Errorf("delete of an unseen container: %s, skips %v", describeRecords(r.Records), r.Skipped)
	}
}

func TestPodDeleteEndsItsContainers(t *testing.T) {
	tr := newTranslator(t)
	created := t0.Add(-time.Hour)
	translate(t, tr, nodeEv("n-1", "node-a", t0, created))
	translate(t, tr, podEv("p-1", "p", "node-a", t0, created))
	translate(t, tr, ctrEv("containerd://c1", "p-1", "one", t0, created))
	translate(t, tr, ctrEv("containerd://c2", "p-1", "two", t0, created))
	translate(t, tr, ctrEv("containerd://other", "p-2", "three", t0, created))

	at := t0.Add(time.Minute)
	r := translate(t, tr, deleteEv("k8s.pod", "p-1", at))
	want := []struct {
		rel catalog.RelationType
		a   string
	}{
		{catalog.PartOf, "c1"}, {"", "c1"}, {catalog.PartOf, "c2"}, {"", "c2"}, {catalog.ScheduledOn, "p-1"}, {"", "p-1"},
	}
	if len(r.Records) != len(want) {
		t.Fatalf("pod delete gave %s", describeRecords(r.Records))
	}
	for i, w := range want {
		rec := r.Records[i]
		isEdge := w.rel != ""
		if rec.Kind != lifecycle.Delete || rec.TTL != 0 || !rec.Through.IsZero() || (rec.Subject.Kind == store.SubjectEdge) != isEdge || (isEdge && rec.Subject.Relation != w.rel) {
			t.Errorf("record %d = %+v, want a Delete (edge %q)", i, rec, w.rel)
		}
		wantA := podFP(t, w.a)
		if strings.HasPrefix(w.a, "c") {
			wantA = ctrFP(t, w.a)
		}
		if rec.Subject.A != wantA {
			t.Errorf("record %d subject %v, want %s", i, rec.Subject.A, w.a)
		}
		wantTimes(t, "delete", rec, at, store.BasisProducerEvent, time.Time{})
	}
	if hasSkip(r, k8scluster.SkipUnseenDelete) {
		t.Errorf("skips = %v", r.Skipped)
	}

	// The other pod's container is untouched, and the deleted ones are gone.
	r = translate(t, tr, deleteEv("k8s.pod", "p-2", at.Add(time.Minute)))
	if len(r.Records) != 3 || !hasSkip(r, k8scluster.SkipUnseenDelete) {
		t.Errorf("delete of p-2 (never seen, one container) gave %s, skips %v", describeRecords(r.Records), r.Skipped)
	}
	r = translate(t, tr, ctrEv("containerd://c1", "p-1", "one", at.Add(2*time.Minute), created))
	if len(r.Records) != 0 || !hasSkip(r, k8scluster.SkipAfterDelete) {
		t.Errorf("container state after its pod's delete gave %s", describeRecords(r.Records))
	}
}

func TestPendingEdgeIsDroppedWithItsPod(t *testing.T) {
	tr := newTranslator(t)
	translate(t, tr, podEv("p-1", "p", "nowhere", t0, time.Time{}))
	if len(tr.Pending()) != 1 {
		t.Fatalf("pending = %v", tr.Pending())
	}
	r := translate(t, tr, deleteEv("k8s.pod", "p-1", t0.Add(time.Minute)))
	if got := only(t, r); got.Kind != lifecycle.Delete {
		t.Errorf("gave %s", describeRecords(r.Records))
	}
	if skipCount(r, k8scluster.SkipPendingDroppedDeleted) != 1 || len(tr.Pending()) != 0 {
		t.Errorf("skips = %v, pending = %v, want the pending edge dropped and counted", r.Skipped, tr.Pending())
	}
}

func TestEmptyContainerIDIsRefused(t *testing.T) {
	events, refused := 0, 0
	for _, name := range []string{"run1.jsonl", "run2.jsonl"} {
		in := loadFixture(t, name)
		for _, x := range in {
			if typeOf(x.Rec) == "container" {
				events++
			}
		}
		for _, r := range translateAll(t, newTranslator(t), in, false) {
			refused += skipCount(r, k8scluster.SkipContainerNoID)
			for _, rec := range r.Records {
				if rec.Subject.A.Type() == catalog.Container || rec.Subject.Relation == catalog.PartOf {
					t.Errorf("%s: a container record was written: %+v", name, rec.Subject)
				}
			}
		}
	}
	if events == 0 {
		t.Fatal("the fixtures have no container events")
	}
	if refused != events {
		t.Errorf("%d container events in the fixtures, %d refused with %q", events, refused, k8scluster.SkipContainerNoID)
	}

	// An id that is empty once its prefix is stripped is empty too.
	tr := newTranslator(t)
	for _, id := range []string{"", " ", "containerd://", "docker://  "} {
		for _, name := range []string{"entity.state", "entity.delete"} {
			e := ctrEv(id, "p-1", "c", t0, time.Time{}).event(name)
			r := translate(t, tr, e)
			if len(r.Records) != 0 || skipCount(r, k8scluster.SkipContainerNoID) != 1 {
				t.Errorf("container id %q (%s): %d records and skips %v", id, name, len(r.Records), r.Skipped)
			}
		}
	}
}

func TestUnchangedStateWithoutTTLGivesNoRecord(t *testing.T) {
	tr := newTranslator(t)
	first := only(t, translate(t, tr, podEv("p-1", "p", "", t0, time.Time{}).interval(0)))
	if first.TTL != 0 {
		t.Errorf("TTL = %s, want 0 for an interval of 0", first.TTL)
	}
	r := translate(t, tr, podEv("p-1", "p", "", t0.Add(time.Minute), time.Time{}).interval(0))
	if len(r.Records) != 0 {
		t.Errorf("an unchanged state without a TTL gave %s", describeRecords(r.Records))
	}
	r = translate(t, tr, podEv("p-1", "p", "", t0.Add(2*time.Minute), time.Time{}).interval(0).with("k8s.pod.phase", "Failed"))
	if got := only(t, r); got.Kind != lifecycle.Observe || got.TTL != 0 {
		t.Errorf("a changed state gave %+v", got)
	}
	// With a TTL every state is a refresh.
	r = translate(t, tr, podEv("p-1", "p", "", t0.Add(3*time.Minute), time.Time{}).with("k8s.pod.phase", "Failed"))
	if got := only(t, r); got.TTL != ttl60 {
		t.Errorf("an unchanged state with a TTL gave %+v, want a refresh", got)
	}
}

func TestPodRebound(t *testing.T) {
	tr := newTranslator(t)
	translate(t, tr, nodeEv("n-a", "a", t0, t0.Add(-time.Hour)))
	translate(t, tr, nodeEv("n-b", "b", t0, t0.Add(-time.Hour)))
	r := translate(t, tr, podEv("p-1", "p", "a", t0, t0.Add(-30*time.Minute)))
	if len(edgesOf(r.Records, catalog.ScheduledOn)) != 1 {
		t.Fatalf("gave %s", describeRecords(r.Records))
	}

	// The pod now names another node: the old edge ends, a new one begins.
	at := t0.Add(time.Minute)
	r = translate(t, tr, podEv("p-1", "p", "b", at, t0.Add(-30*time.Minute)))
	if len(r.Records) != 3 {
		t.Fatalf("gave %s, want the pod, the old edge's Delete and the new edge", describeRecords(r.Records))
	}
	if r.Records[1].Kind != lifecycle.Delete || r.Records[1].Subject.B != nodeFP(t, "n-a") || r.Records[2].Subject.B != nodeFP(t, "n-b") || r.Records[2].Kind != lifecycle.Observe {
		t.Errorf("records = %+v", r.Records)
	}

	// A pending edge whose node name changes is dropped and counted.
	translate(t, tr, podEv("p-2", "q", "nowhere", at, time.Time{}))
	r = translate(t, tr, podEv("p-2", "q", "elsewhere", at.Add(time.Minute), time.Time{}))
	if skipCount(r, k8scluster.SkipPendingDroppedRebound) != 1 || len(tr.Pending()) != 1 || tr.Pending()[0].NodeName != "elsewhere" {
		t.Errorf("skips = %v, pending = %v", r.Skipped, tr.Pending())
	}
}

func TestContainerMovedToAnotherPod(t *testing.T) {
	tr := newTranslator(t)
	translate(t, tr, ctrEv("containerd://c1", "p-1", "web", t0, time.Time{}))
	at := t0.Add(time.Minute)
	r := translate(t, tr, ctrEv("containerd://c1", "p-2", "web", at, time.Time{}))
	if len(r.Records) != 3 {
		t.Fatalf("gave %s, want the container, the old edge's Delete and the new edge", describeRecords(r.Records))
	}
	if r.Records[1].Kind != lifecycle.Delete || r.Records[1].Subject.B != podFP(t, "p-1") || r.Records[2].Subject.B != podFP(t, "p-2") {
		t.Errorf("records = %+v", r.Records)
	}
	// The delete of the first pod no longer reaches the container.
	r = translate(t, tr, deleteEv("k8s.pod", "p-1", at.Add(time.Minute)))
	if got := only(t, r); got.Subject.A != podFP(t, "p-1") {
		t.Errorf("gave %s", describeRecords(r.Records))
	}
	r = translate(t, tr, deleteEv("k8s.pod", "p-2", at.Add(2*time.Minute)))
	if len(r.Records) != 3 {
		t.Errorf("delete of the second pod gave %s, want its container and edge too", describeRecords(r.Records))
	}
}

func TestScheduledOnBeginsAtFirstNamingRecord(t *testing.T) {
	created := t0.Add(-2 * time.Hour)
	nodeCreated := t0.Add(-3 * time.Hour)

	t.Run("a known node", func(t *testing.T) {
		tr := newTranslator(t)
		translate(t, tr, nodeEv("n-1", "node-a", t0, nodeCreated))
		r := translate(t, tr, podEv("p-1", "p", "node-a", t0, created))
		if len(r.Records) != 2 {
			t.Fatalf("gave %s, want the pod and the edge", describeRecords(r.Records))
		}
		// The pod is at its creation, through the instant; the edge is at the
		// instant, and has no Through: nothing says it was bound before.
		wantTimes(t, "pod", r.Records[0], created, store.BasisObjectField, t0)
		if r.Records[1].Subject.Relation != catalog.ScheduledOn {
			t.Fatalf("second record = %+v", r.Records[1].Subject)
		}
		wantTimes(t, "edge", r.Records[1], t0, store.BasisProducerEvent, time.Time{})
		if r.Records[1].TTL != ttl60 {
			t.Errorf("edge TTL = %s, want %s", r.Records[1].TTL, ttl60)
		}
	})

	t.Run("a creation ahead of the instant", func(t *testing.T) {
		tr := newTranslator(t)
		translate(t, tr, nodeEv("n-1", "node-a", t0, nodeCreated))
		ahead := t0.Add(2 * time.Minute)
		r := translate(t, tr, podEv("p-1", "p", "node-a", t0, ahead))
		wantTimes(t, "pod", r.Records[0], ahead, store.BasisObjectField, time.Time{})
		wantTimes(t, "edge", r.Records[1], t0, store.BasisProducerEvent, time.Time{})
	})

	t.Run("a node that arrives later", func(t *testing.T) {
		tr := newTranslator(t)
		r := translate(t, tr, podEv("p-1", "p", "node-a", t0, created))
		if len(r.Pending) != 1 || !r.Pending[0].Time.Equal(t0) || r.Pending[0].Basis != store.BasisProducerEvent {
			t.Fatalf("pending = %+v, want the instant of the first record", r.Pending)
		}
		r = translate(t, tr, nodeEv("n-1", "node-a", t0.Add(time.Minute), nodeCreated))
		edges := edgesOf(r.Records, catalog.ScheduledOn)
		if len(edges) != 1 {
			t.Fatalf("gave %s", describeRecords(r.Records))
		}
		wantTimes(t, "edge", edges[0], t0, store.BasisProducerEvent, time.Time{})
	})
}
