package k8sobjects_test

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

func newPulledTranslator(t testing.TB, interval, ttl time.Duration) *k8sobjects.Translator {
	t.Helper()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: testProducer, Resolver: resolver(), PullInterval: interval, EntityTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func subjectsOf(rs []store.Record) []store.Subject {
	var out []store.Subject
	for _, r := range rs {
		out = append(out, r.Subject)
	}
	return out
}

// scheduledPod is a running pod with one container (runtime id "<uid>-c"), bound to
// node by the given time.
func scheduledPod(uid string, created, scheduled time.Time) podSpec {
	return podSpec{
		UID: uid, Name: uid, Node: node, Created: created, Phase: "Running",
		Conds:      []map[string]any{cond("PodScheduled", "True", scheduled)},
		Containers: []map[string]any{{"name": "app", "restartCount": 0, "containerID": "containerd://" + uid + "-c"}},
	}
}

// isPlainDelete checks that a record is a Delete with no TTL, no payload and no
// Through.
func isPlainDelete(r store.Record) bool {
	return r.Kind == lifecycle.Delete && r.TTL == 0 && len(r.Payload) == 0 && r.Through.IsZero()
}

func TestEdgesTakeTheEntityTTL(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	translate(t, tr, pullRec(at(1, 0), nodeObject("n-1", node, at(-10, 0), "1")))
	p := scheduledPod("p1", at(0, 0), at(0, 5))
	pod, ctr, nodeFp := podFP(t, "p1"), containerFP(t, "p1-c"), nodeFP(t, "n-1")

	r := translate(t, tr, pullRec(at(1, 0), p.object()))
	want := []store.Subject{
		store.EntitySubject(pod),
		store.EntitySubject(ctr),
		store.EdgeSubject(ctr, pod, catalog.PartOf),
		store.EdgeSubject(pod, nodeFp, catalog.ScheduledOn),
	}
	if got := subjectsOf(r.Records); !slices.Equal(got, want) {
		t.Fatalf("first sight gave %s, want pod, container, part_of, scheduled_on", joinSubjects(r.Records))
	}
	for _, rec := range r.Records {
		if rec.Kind != lifecycle.Observe || rec.TTL != ttl {
			t.Errorf("record %+v, want an Observe with TTL %s", rec, ttl)
		}
	}

	del := translate(t, tr, watchRec(at(5, 0), "DELETED", p.object()))
	wantDel := []store.Subject{
		store.EdgeSubject(ctr, pod, catalog.PartOf),
		store.EntitySubject(ctr),
		store.EdgeSubject(pod, nodeFp, catalog.ScheduledOn),
		store.EntitySubject(pod),
	}
	if got := subjectsOf(del.Records); !slices.Equal(got, wantDel) {
		t.Fatalf("DELETED gave %s, want the part_of, container, scheduled_on and pod deletes", joinSubjects(del.Records))
	}
	for _, rec := range del.Records {
		if !isPlainDelete(rec) {
			t.Errorf("record %+v, want a Delete with no TTL, payload or Through", rec)
		}
	}
}

func TestEdgesAreReassertedWithTheirEntity(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	translate(t, tr, pullRec(at(1, 0), nodeObject("n-old", node, at(-10, 0), "1")))
	p := scheduledPod("p1", at(0, 0), at(0, 5))
	first := translate(t, tr, pullRec(at(1, 0), p.object()))
	est := edgeOf(t, first)
	if !est.EventTime.Equal(at(0, 5)) || est.EventTimeBasis != store.BasisObjectField {
		t.Fatalf("setup: the edge was established at %s basis %s", est.EventTime, est.EventTimeBasis)
	}
	want := subjectsOf(first.Records)

	check := func(what string, r k8sobjects.Result, when time.Time) {
		t.Helper()
		if got := subjectsOf(r.Records); !slices.Equal(got, want) {
			t.Errorf("%s gave %s, want pod, container, part_of, scheduled_on", what, joinSubjects(r.Records))
			return
		}
		for _, rec := range r.Records {
			if rec.Kind != lifecycle.Observe || rec.TTL != ttl || !rec.EventTime.Equal(when) || rec.EventTimeBasis != store.BasisObserved || !rec.Through.IsZero() {
				t.Errorf("%s: %+v, want an Observe at %s with basis observed and the TTL", what, rec, when)
			}
		}
		if e := edgeOf(t, r); !bytes.Equal(e.Payload, est.Payload) {
			t.Errorf("%s: scheduled_on payload %s, want the establishing one %s", what, e.Payload, est.Payload)
		}
		if part := r.Records[2]; string(part.Payload) != "{}" {
			t.Errorf("%s: part_of payload %s, want {}", what, part.Payload)
		}
	}

	p.Heartbeat = "2"
	check("an unchanged pull", translate(t, tr, pullRec(at(6, 0), p.object())), at(6, 0))
	check("an unchanged MODIFIED", translate(t, tr, watchRec(at(7, 0), "MODIFIED", p.object())), at(7, 0))

	// The node is deleted and created again under its name. The pod keeps the edge
	// it was established with, and its payload does not follow the name.
	translate(t, tr, watchRec(at(8, 0), "DELETED", nodeObject("n-old", node, at(-10, 0), "1")))
	translate(t, tr, watchRec(at(8, 30), "ADDED", nodeObject("n-new", node, at(8, 0), "1")))
	r := translate(t, tr, pullRec(at(9, 0), p.object()))
	check("a pull after the node was replaced", r, at(9, 0))
	if e := edgeOf(t, r); e.Subject.B != nodeFP(t, "n-old") {
		t.Errorf("the edge points at %v, want the node it was established with", e.Subject.B)
	}
}

// The scheduled_on edge begins at the PodScheduled time, which a clock ahead can
// place after the next record's time. A re-assertion is never earlier than the
// edge's own latest assertion.
func TestReassertionNeverPrecedesTheEdge(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	translate(t, tr, pullRec(at(1, 0), nodeObject("n-1", node, at(-10, 0), "1")))
	p := scheduledPod("p1", at(0, 0), at(3, 0)) // two minutes after the next record
	first := translate(t, tr, pullRec(at(1, 0), p.object()))
	if e := edgeOf(t, first); !e.EventTime.Equal(at(3, 0)) || e.EventTimeBasis != store.BasisObjectField || !e.Through.IsZero() {
		t.Fatalf("setup: the edge = %+v", e)
	}

	r := translate(t, tr, pullRec(at(2, 0), p.object()))
	e := edgeOf(t, r)
	if !e.EventTime.Equal(at(3, 0)) || e.EventTimeBasis != store.BasisObjectField {
		t.Errorf("re-asserted edge at %s basis %s, want the edge's own time %s, object field", e.EventTime, e.EventTimeBasis, at(3, 0))
	}
	if got := skips(r)[k8sobjects.SkipClampedTime]; got != 1 {
		t.Errorf("counted %d clamped times, want 1", got)
	}
	for _, rec := range r.Records {
		if rec.Subject.Relation != catalog.ScheduledOn && !rec.EventTime.Equal(at(2, 0)) {
			t.Errorf("record %+v, want the observed time %s", rec, at(2, 0))
		}
	}

	r = translate(t, tr, pullRec(at(4, 0), p.object()))
	e = edgeOf(t, r)
	if !e.EventTime.Equal(at(4, 0)) || e.EventTimeBasis != store.BasisObserved || len(r.Skipped) != 0 {
		t.Errorf("the next record gave the edge at %s basis %s, skips %v; want %s, observed, none", e.EventTime, e.EventTimeBasis, r.Skipped, at(4, 0))
	}
}

// The TTL is the pull interval's, whatever the kind of record.
func TestWatchRecordsCarryThePullTTL(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newPulledTranslator(t, 5*time.Minute, 0)
	n := nodeObject("n-1", node, at(-10, 0), "1")
	p := scheduledPod("p1", at(0, 0), at(0, 5))
	var recs []store.Record
	add := func(lr capture.LogRecord) { recs = append(recs, translate(t, tr, lr).Records...) }
	add(watchRec(at(1, 0), "ADDED", n))
	add(watchRec(at(1, 1), "ADDED", p.object()))
	p.Phase = "Failed"
	add(watchRec(at(2, 0), "MODIFIED", p.object()))
	add(pullRec(at(6, 0), n))
	add(pullRec(at(6, 1), p.object()))
	if len(recs) < 10 {
		t.Fatalf("got %d records, want ten or more", len(recs))
	}
	var edges int
	for _, rec := range recs {
		if rec.Kind != lifecycle.Observe || rec.TTL != ttl {
			t.Errorf("record %+v, want an Observe with TTL %s", rec, ttl)
		}
		if rec.Subject.Kind == store.SubjectEdge {
			edges++
		}
	}
	if edges < 4 {
		t.Errorf("got %d edge records, want the edges of both pod records", edges)
	}
	for _, rec := range translate(t, tr, watchRec(at(7, 0), "DELETED", p.object())).Records {
		if !isPlainDelete(rec) {
			t.Errorf("record %+v, want a plain Delete", rec)
		}
	}
}

func TestPullGapNote(t *testing.T) {
	const interval = 5 * time.Minute
	tr := newPulledTranslator(t, interval, 0)
	n := nodeObject("n-1", node, at(-10, 0), "1")
	p := podSpec{UID: "p1", Name: "p1", Created: at(0, 0), Phase: "Running"}
	p2 := podSpec{UID: "p2", Name: "p2", Created: at(0, 0), Phase: "Running"}
	for _, s := range []struct {
		name string
		lr   capture.LogRecord
		want int
	}{
		{"the first node generation", pullRec(at(0, 0), n), 0},
		{"the first pod generation", pullRec(at(0, 0), p.object()), 0},
		{"pods at +5", pullRec(at(5, 0), p.object()), 0},
		{"pods at +10", pullRec(at(10, 0), p.object()), 0},
		{"a watch record between generations", watchRec(at(12, 0), "MODIFIED", p.object()), 0},
		{"pods at +17:30, exactly 1.5 intervals", pullRec(at(17, 30), p.object()), 0},
		// The nodes' own gap, 20 minutes. It is the nodes' and not the pods'.
		{"nodes at +20", pullRec(at(20, 0), n), 1},
		{"a watch record that would shorten the pods' gap", watchRec(at(26, 0), "MODIFIED", p.object()), 0},
		{"pods at +27:31, over 1.5 intervals", pullRec(at(27, 31), p.object()), 1},
		{"another pod of the same generation", pullRec(at(27, 31), p2.object()), 0},
		{"pods at +32:31", pullRec(at(32, 31), p.object()), 0},
	} {
		r := translate(t, tr, s.lr)
		var got int
		for _, n := range r.Notes {
			if n.Reason != k8sobjects.NotePullGap {
				t.Errorf("%s: note %q", s.name, n.Reason)
			}
			got += n.Count
		}
		if got != s.want || len(r.Notes) > 1 {
			t.Errorf("%s: notes %v, want %d of %q", s.name, r.Notes, s.want, k8sobjects.NotePullGap)
		}
	}

	// With no interval there is nothing to compare with.
	tr = newTranslator(t, 0)
	translate(t, tr, pullRec(at(0, 0), p.object()))
	if r := translate(t, tr, pullRec(at(120, 0), p.object())); len(r.Notes) != 0 {
		t.Errorf("a translator without an interval gave notes %v", r.Notes)
	}
}

// A name borne by two live node UIDs resolves to neither, is counted each time it
// is looked up, and resolves again when only one is left.
func TestNodeNameUnderTwoLiveUIDsResolvesToNeither(t *testing.T) {
	tr := newTranslator(t, 0)
	nb := nodeObject("n-b", node, at(0, 0), "1")
	translate(t, tr, pullRec(at(1, 0), nodeObject("n-a", node, at(0, 0), "1")))
	translate(t, tr, pullRec(at(1, 1), nb))
	p := podSpec{UID: "p1", Name: "p", Node: node, Created: at(0, 10), Phase: "Pending"}

	r := translate(t, tr, pullRec(at(2, 0), p.object()))
	if len(r.Records) != 1 || len(r.Pending) != 1 || len(tr.Pending()) != 1 || skips(r)[k8sobjects.SkipAmbiguousNodeName] != 1 {
		t.Fatalf("got %d records, %d pending, skips %v; want the pod, one pending edge, one ambiguous name", len(r.Records), len(r.Pending), skips(r))
	}

	p.Phase = "Running"
	r = translate(t, tr, watchRec(at(3, 0), "MODIFIED", p.object()))
	if len(r.Records) != 1 || len(tr.Pending()) != 1 || skips(r)[k8sobjects.SkipAmbiguousNodeName] != 1 {
		t.Fatalf("a later record gave %d records, %d pending, skips %v; want the pod, still pending, counted again", len(r.Records), len(tr.Pending()), skips(r))
	}

	translate(t, tr, watchRec(at(4, 0), "DELETED", nb))
	r = translate(t, tr, pullRec(at(5, 0), p.object()))
	edge := only(t, r)
	if edge.Subject != store.EdgeSubject(podFP(t, "p1"), nodeFP(t, "n-a"), catalog.ScheduledOn) || edge.Kind != lifecycle.Observe {
		t.Fatalf("got %+v, want the scheduled_on edge to n-a", edge)
	}
	if !edge.EventTime.Equal(at(5, 0)) || edge.EventTimeBasis != store.BasisObserved || len(tr.Pending()) != 0 || len(r.Skipped) != 0 {
		t.Errorf("edge at %s basis %s, pending %d, skips %v; want the record's time, observed, none", edge.EventTime, edge.EventTimeBasis, len(tr.Pending()), r.Skipped)
	}
}

// A node whose DELETED was lost does not hold its name for ever: with a TTL its
// interval ends a TTL after the last record that asserted it.
func TestLapsedNodeFreesItsName(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	// n-a was created before it was first seen, and is never seen again.
	translate(t, tr, pullRec(at(0, 0), nodeObject("n-a", node, at(-10, 0), "1")))

	podAt := func(uid string, when time.Time) k8sobjects.Result {
		p := podSpec{UID: uid, Name: uid, Node: node, Created: when, Phase: "Running"}
		return translate(t, tr, pullRec(when, p.object()))
	}
	edgeTo := func(what string, r k8sobjects.Result, uid, nodeUID string) {
		t.Helper()
		if len(r.Records) != 2 || len(r.Pending) != 0 {
			t.Fatalf("%s: got %s, %d pending; want the pod and its edge", what, joinSubjects(r.Records), len(r.Pending))
		}
		if e := edgeOf(t, r); e.Subject != store.EdgeSubject(podFP(t, uid), nodeFP(t, nodeUID), catalog.ScheduledOn) {
			t.Errorf("%s: the edge is %+v, want one to %s", what, e.Subject, nodeUID)
		}
	}
	// n-a was last asserted at +0, so it bears the name until +15:00.
	edgeTo("a pod inside n-a's TTL", podAt("p1", at(10, 0)), "p1", "n-a")
	edgeTo("a pod just inside n-a's TTL", podAt("p3", at(14, 59)), "p3", "n-a")
	if r := podAt("p4", at(15, 0)); len(r.Pending) != 1 || skips(r)[k8sobjects.SkipAmbiguousNodeName] != 0 {
		t.Fatalf("a pod after n-a's TTL gave %d pending, skips %v; want it pending, with no node", len(r.Pending), skips(r))
	}

	// n-b appears under the same name. It does not make the name ambiguous.
	nr := translate(t, tr, pullRec(at(30, 1), nodeObject("n-b", node, at(30, 0), "1")))
	if len(nr.Records) != 1 || skips(nr)[k8sobjects.SkipAmbiguousNodeName] != 0 {
		t.Errorf("n-b gave %s, skips %v; want its own record and no pending edge resolved to it", joinSubjects(nr.Records), skips(nr))
	}
	edgeTo("a pod after n-b appeared", podAt("p2", at(31, 0)), "p2", "n-b")
}

// An edge that waited for its node is re-asserted by the pod's next record like any
// other, at that record's time, or at the edge's own latest assertion if that is
// later.
func TestResolvedPendingEdgeIsReasserted(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	a := podSpec{UID: "pa", Name: "pa", Node: node, Created: at(0, 0), Phase: "Running"}
	b := podSpec{
		UID: "pb", Name: "pb", Node: node, Created: at(0, 0), Phase: "Running",
		Conds: []map[string]any{cond("PodScheduled", "True", at(7, 0))}, // ahead of the observed time
	}
	translate(t, tr, pullRec(at(5, 0), a.object()))
	translate(t, tr, pullRec(at(5, 0), b.object()))
	if len(tr.Pending()) != 2 {
		t.Fatalf("got %d pending, want both pods", len(tr.Pending()))
	}

	nr := translate(t, tr, pullRec(at(6, 0), nodeObject("n-1", node, at(-1, 0), "1")))
	if len(nr.Records) != 3 {
		t.Fatalf("the node gave %s, want itself and two edges", joinSubjects(nr.Records))
	}
	edgeA, edgeB := nr.Records[1], nr.Records[2]
	if edgeA.Subject != store.EdgeSubject(podFP(t, "pa"), nodeFP(t, "n-1"), catalog.ScheduledOn) || !edgeA.EventTime.Equal(at(0, 0)) || !edgeA.Through.Equal(at(5, 0)) {
		t.Fatalf("edge of pa = %+v, want the pod's time, with the pod's latest observed time as Through, not the node's", edgeA)
	}
	if !edgeB.EventTime.Equal(at(7, 0)) || edgeB.EventTimeBasis != store.BasisObjectField || !edgeB.Through.IsZero() {
		t.Fatalf("edge of pb = %+v, want its scheduling time, no Through", edgeB)
	}

	ra := translate(t, tr, pullRec(at(10, 0), a.object()))
	e := edgeOf(t, ra)
	if len(ra.Records) != 2 || !e.EventTime.Equal(at(10, 0)) || e.EventTimeBasis != store.BasisObserved || e.TTL != ttl || !bytes.Equal(e.Payload, edgeA.Payload) || !e.Through.IsZero() {
		t.Errorf("pa's next record gave %s, edge %+v; want the pod and the edge at %s, observed, same payload", joinSubjects(ra.Records), e, at(10, 0))
	}

	rb := translate(t, tr, pullRec(at(6, 30), b.object()))
	e = edgeOf(t, rb)
	if !e.EventTime.Equal(at(7, 0)) || e.EventTimeBasis != store.BasisObjectField || skips(rb)[k8sobjects.SkipClampedTime] != 1 || !bytes.Equal(e.Payload, edgeB.Payload) {
		t.Errorf("pb's next record gave the edge %+v, skips %v; want it held at its own time %s, object field", e, skips(rb), at(7, 0))
	}
}

// A pod created long before it was first seen exists, as far as the producer
// knows, from its creation: UIDs are never reused. The first Observe says so.
func TestFirstSightingInAPullCarriesThrough(t *testing.T) {
	const ttl = 15 * time.Minute
	created, seen := t0, t0.Add(2*time.Hour)
	tr := newTranslator(t, ttl)
	p := podSpec{UID: "p1", Name: "p", Created: created, Phase: "Running"}
	r := translate(t, tr, pullRec(seen, p.object()))
	rec := only(t, r)
	if rec.Kind != lifecycle.Observe || !rec.EventTime.Equal(created) || !rec.Through.Equal(seen) || rec.TTL != ttl || rec.EventTimeBasis != store.BasisObjectField {
		t.Fatalf("first sight = %+v, want an Observe at %s through %s, TTL %s, object field", rec, created, seen, ttl)
	}

	s := storeFrom(t, r.Records)
	ctx, scope, pod := context.Background(), store.Current(catalog.L2), podFP(t, "p1")
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{created.Add(-time.Nanosecond), false},
		{created.Add(time.Minute), true},
		{created.Add(time.Hour), true},
		{seen, true},
		{seen.Add(ttl - time.Nanosecond), true},
		{seen.Add(ttl), false},
	} {
		got, err := s.Alive(ctx, pod, c.at, scope)
		if err != nil || got != c.want {
			t.Errorf("alive at %s = %v, %v, want %v", c.at.Sub(created), got, err, c.want)
		}
	}
}

func TestScheduledOnFromThePastCarriesThrough(t *testing.T) {
	const ttl = 15 * time.Minute
	created, scheduled, seen := t0, t0.Add(30*time.Second), t0.Add(2*time.Hour)
	tr := newTranslator(t, ttl)
	nr := translate(t, tr, pullRec(seen, nodeObject("n-1", node, created.Add(-10*time.Minute), "1")))
	p := scheduledPod("p1", created, scheduled)
	pr := translate(t, tr, pullRec(seen, p.object()))
	e := edgeOf(t, pr)
	if !e.EventTime.Equal(scheduled) || !e.Through.Equal(seen) || e.TTL != ttl || e.EventTimeBasis != store.BasisObjectField {
		t.Fatalf("scheduled_on = %+v, want an Observe at %s through %s, TTL %s, object field", e, scheduled, seen, ttl)
	}

	s := storeFrom(t, nr.Records, pr.Records)
	ctx, scope, pod := context.Background(), store.Current(catalog.L2), podFP(t, "p1")
	for _, c := range []struct {
		at   time.Time
		want int
	}{
		{created.Add(10 * time.Second), 0},
		{scheduled.Add(-time.Nanosecond), 0},
		{scheduled.Add(time.Minute), 1},
		{created.Add(time.Hour), 1},
		{seen, 1},
		{seen.Add(ttl - time.Nanosecond), 1},
		{seen.Add(ttl), 0},
	} {
		ns, err := s.Neighbors(ctx, pod, store.Forward, c.at, scope)
		got := 0
		for _, n := range ns {
			if n.Relation == catalog.ScheduledOn && n.Peer == nodeFP(t, "n-1") {
				got++
			}
		}
		if err != nil || got != c.want {
			t.Errorf("scheduled_on edges at %s = %d, %v, want %d", c.at.Sub(created), got, err, c.want)
		}
	}
}

// A record whose event time is its observed time has no Through, so it is the
// record there has always been.
func TestTimelyAddedHasNoThrough(t *testing.T) {
	for _, ttl := range ttls {
		tr := newTranslator(t, ttl)
		p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending"}
		rec := only(t, translate(t, tr, watchRec(t0, "ADDED", p.object())))
		want := store.Record{
			Layer: rec.Layer, Subject: store.EntitySubject(podFP(t, "p1")), Producer: testProducer,
			EventTime: rec.EventTime, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisObjectField, Payload: rec.Payload,
		}
		if !rec.EventTime.Equal(t0) || !reflect.DeepEqual(rec, want) {
			t.Errorf("ttl %s: ADDED gave %+v, want %+v", ttl, rec, want)
		}
	}
}

// A change timed by the object two hours before it was observed is asserted with
// the observed time as Through, so the pod exists from the change to the
// observation and for a TTL after.
func TestModifiedTimedInThePastCarriesThrough(t *testing.T) {
	const ttl = 15 * time.Minute
	observed, changed := t0.Add(3*time.Hour), t0.Add(time.Hour)
	for _, withTTL := range ttls {
		tr := newTranslator(t, withTTL)
		p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending"}
		added := translate(t, tr, watchRec(t0, "ADDED", p.object()))
		p.Phase = "Running"
		p.Conds = []map[string]any{cond("Ready", "True", changed)}
		modified := translate(t, tr, watchRec(observed, "MODIFIED", p.object()))
		rec := only(t, modified)
		if !rec.EventTime.Equal(changed) || rec.EventTimeBasis != store.BasisObjectField || !rec.Through.Equal(observed) || rec.TTL != withTTL {
			t.Fatalf("ttl %s: MODIFIED gave %+v, want an Observe at %s through %s, object field", withTTL, rec, changed, observed)
		}
		if withTTL == 0 {
			continue
		}
		s := storeFrom(t, added.Records, modified.Records)
		for _, c := range []struct {
			at   time.Time
			want bool
		}{
			{changed, true},
			{changed.Add(time.Hour), true},
			{observed, true},
			{observed.Add(ttl - time.Nanosecond), true},
			{observed.Add(ttl), false},
		} {
			got, err := s.Alive(context.Background(), podFP(t, "p1"), c.at, store.Current(catalog.L2))
			if err != nil || got != c.want {
				t.Errorf("alive at +%s = %v, %v, want %v", c.at.Sub(t0), got, err, c.want)
			}
		}
	}
}

// A time two minutes ahead of the observed time is plausible and is used, and it
// needs no Through: the record is not before its observation.
func TestClockAheadHasNoThrough(t *testing.T) {
	observed := t0.Add(10 * time.Minute)
	ahead := observed.Add(2 * time.Minute)
	for _, ttl := range ttls {
		tr := newTranslator(t, ttl)
		p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending"}
		translate(t, tr, watchRec(t0, "ADDED", p.object()))
		p.Phase = "Running"
		p.Conds = []map[string]any{cond("Ready", "True", ahead)}
		rec := only(t, translate(t, tr, watchRec(observed, "MODIFIED", p.object())))
		if !rec.EventTime.Equal(ahead) || rec.EventTimeBasis != store.BasisObjectField || !rec.Through.IsZero() {
			t.Errorf("ttl %s: MODIFIED gave %+v, want an Observe at %s, object field, no Through", ttl, rec, ahead)
		}

		// The same for an edge bound two minutes after the record that sees it.
		tr = newTranslator(t, ttl)
		translate(t, tr, pullRec(observed, nodeObject("n-1", node, t0.Add(-time.Minute), "1")))
		sp := scheduledPod("p2", t0, ahead)
		e := edgeOf(t, translate(t, tr, pullRec(observed, sp.object())))
		if !e.EventTime.Equal(ahead) || !e.Through.IsZero() {
			t.Errorf("ttl %s: scheduled_on = %+v, want it at %s with no Through", ttl, e, ahead)
		}
	}
}

// No Delete carries a TTL, a payload or a Through, including a Delete timed before
// its observation: the Delete of a container replaced in a record timed in the
// past, and the end of an edge whose node name changed.
func TestDeleteHasNoThroughNorTTL(t *testing.T) {
	observed := t0.Add(3 * time.Hour)
	for _, ttl := range ttls {
		t.Run(ttl.String(), func(t *testing.T) {
			deletes := func(what string, rs []store.Record, want int) {
				t.Helper()
				n := 0
				for _, r := range rs {
					if r.Kind != lifecycle.Delete {
						continue
					}
					n++
					if !isPlainDelete(r) {
						t.Errorf("%s: %+v, want no TTL, payload or Through", what, r)
					}
				}
				if n != want {
					t.Errorf("%s: %d deletes (%s), want %d", what, n, joinSubjects(rs), want)
				}
			}

			// A seen pod.
			tr := newTranslator(t, ttl)
			translate(t, tr, pullRec(t0.Add(time.Minute), nodeObject("n-1", node, t0.Add(-time.Hour), "1")))
			p := scheduledPod("p1", t0, t0.Add(time.Second))
			translate(t, tr, watchRec(t0.Add(time.Minute), "ADDED", p.object()))

			// A restart of the container, and a change of node, both timed an hour
			// before they were observed: their Deletes are timed in the past.
			p.Node = "other-node"
			p.Containers = []map[string]any{{
				"name": "app", "restartCount": 1, "containerID": "containerd://p1-c2",
				"state": map[string]any{"running": map[string]any{"startedAt": ts(observed.Add(-time.Hour))}},
			}}
			mod := translate(t, tr, watchRec(observed, "MODIFIED", p.object()))
			for _, r := range mod.Records {
				if r.Kind == lifecycle.Delete && !r.EventTime.Before(observed) {
					t.Fatalf("setup: the Delete %+v is not timed before the observation", r)
				}
			}
			deletes("replaced container and edge", mod.Records, 3) // part_of, container, scheduled_on
			deletes("DELETED of a seen pod", translate(t, tr, watchRec(observed.Add(time.Hour), "DELETED", p.object())).Records, 3)

			// An unseen pod, as after a restart.
			tr = newTranslator(t, ttl)
			translate(t, tr, pullRec(observed, nodeObject("n-1", node, t0.Add(-time.Hour), "1")))
			q := scheduledPod("p2", t0, t0.Add(time.Second))
			deletes("DELETED of an unseen pod", translate(t, tr, watchRec(observed, "DELETED", q.object())).Records, 4)

			// A node.
			nodeDel := translate(t, tr, watchRec(observed.Add(time.Minute), "DELETED", nodeObject("n-1", node, t0.Add(-time.Hour), "1")))
			deletes("DELETED of a node", nodeDel.Records, 1)
		})
	}
}

// The coalescer passes a record that carries a Through on unchanged, and the
// refresh after it starts a run; the pod exists without a break across the
// observation.
func TestThroughRecordThroughTheCoalescer(t *testing.T) {
	const ttl = 15 * time.Minute
	created, seen := t0, t0.Add(2*time.Hour)
	tr := newTranslator(t, ttl)
	p := podSpec{UID: "p1", Name: "p", Created: created, Phase: "Running"}
	first := translate(t, tr, pullRec(seen, p.object())).Records
	next := translate(t, tr, pullRec(seen.Add(5*time.Minute), p.object())).Records
	if len(first) != 1 || len(next) != 1 || first[0].Through.IsZero() || !next[0].Through.IsZero() {
		t.Fatalf("setup: %v then %v", first, next)
	}

	co, err := coalesce.New(coalesce.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	out := co.Add(first[0], nil)
	if len(out) != 1 || !reflect.DeepEqual(out[0], first[0]) {
		t.Fatalf("the Through record came out as %v, want it unchanged", out)
	}
	out = co.Add(next[0], nil)
	if len(out) != 1 || !reflect.DeepEqual(out[0], next[0]) {
		t.Fatalf("the refresh came out as %v, want it unchanged, starting a run", out)
	}

	s := storeFrom(t, first, next)
	ctx, scope, pod := context.Background(), store.Current(catalog.L2), podFP(t, "p1")
	last := seen.Add(5 * time.Minute)
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{created.Add(-time.Nanosecond), false},
		{created, true},
		{seen.Add(-time.Nanosecond), true},
		{seen, true},
		{seen.Add(time.Nanosecond), true},
		{last, true},
		{last.Add(ttl - time.Nanosecond), true},
		{last.Add(ttl), false},
	} {
		got, err := s.Alive(ctx, pod, c.at, scope)
		if err != nil || got != c.want {
			t.Errorf("alive at %s = %v, %v, want %v", c.at.Sub(created), got, err, c.want)
		}
	}
}

// Over the captures, in both modes: every record validates, no Observe lapses
// before its observed time plus the TTL, and a Through is always the observed time
// and only on an Observe.
func TestNoObserveLapsesBeforeItsObservedTimePlusTTL(t *testing.T) {
	for _, ttl := range ttls {
		for _, name := range captures {
			in := loadCapture(t, name)
			seq, throughs := uint64(0), 0
			for i, res := range translateAll(t, newTranslator(t, ttl), in) {
				observed := observedOf(in[i])
				for _, rec := range res.Records {
					seq++
					rec.Seq = seq
					if err := rec.Validate(); err != nil {
						t.Errorf("ttl %s, %s: %v", ttl, in[i].Name, err)
					}
					if rec.Kind != lifecycle.Observe {
						if !rec.Through.IsZero() {
							t.Errorf("ttl %s, %s: a Delete with a Through: %+v", ttl, in[i].Name, rec)
						}
						continue
					}
					last := rec.EventTime
					if rec.Through.After(last) {
						last = rec.Through
						throughs++
						if !rec.Through.Equal(observed) {
							t.Errorf("ttl %s, %s: Through %s, want the observed time %s", ttl, in[i].Name, rec.Through, observed)
						}
					}
					if lapse := last.Add(rec.TTL); lapse.Before(observed.Add(rec.TTL)) {
						t.Errorf("ttl %s, %s: %+v lapses at %s, before the observed time plus the TTL %s", ttl, in[i].Name, rec.Subject, lapse, observed.Add(rec.TTL))
					}
				}
			}
			if throughs == 0 {
				t.Errorf("ttl %s, %s: no record carries a Through, so the test proves nothing", ttl, name)
			}
		}
	}
}

// The arrival of a node says nothing about whether a pod that named it still
// exists. An edge resolved late is asserted through the pod's last observation, so
// it does not outlive a pod that is no longer seen.
func TestLateResolvedEdgeDoesNotOutliveItsPod(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	p := podSpec{UID: "p1", Name: "p", Node: node, Created: at(0, 0), Phase: "Running"}
	pr := translate(t, tr, pullRec(at(5, 0), p.object()))
	if len(pr.Pending) != 1 {
		t.Fatalf("setup: %d pending, want the edge to wait for its node", len(pr.Pending))
	}
	// The pod is seen once. An hour later its node appears.
	nr := translate(t, tr, pullRec(at(60, 0), nodeObject("n-1", node, at(-1, 0), "1")))
	if len(nr.Records) != 2 {
		t.Fatalf("the node gave %s, want itself and the edge", joinSubjects(nr.Records))
	}
	edge := nr.Records[1]
	if !edge.EventTime.Equal(at(0, 0)) || !edge.Through.Equal(at(5, 0)) {
		t.Fatalf("edge = %+v, want it from the pod's time through the pod's last observation %s", edge, at(5, 0))
	}

	s := storeFrom(t, pr.Records, nr.Records)
	ctx, scope, pod := context.Background(), store.Current(catalog.L2), podFP(t, "p1")
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{at(1, 0), true},
		{at(5, 0).Add(ttl - time.Nanosecond), true},
		{at(70, 0), false},
	} {
		got, err := s.Alive(ctx, pod, c.at, scope)
		if err != nil || got != c.want {
			t.Errorf("pod alive at +%s = %v, %v, want %v", c.at.Sub(t0), got, err, c.want)
		}
	}
	// The node is alive at +70m, and the pod is not: the edge is not either.
	if got, err := s.Alive(ctx, nodeFP(t, "n-1"), at(70, 0), scope); err != nil || !got {
		t.Fatalf("setup: node alive at +70m = %v, %v, want true", got, err)
	}
	ns, err := s.Neighbors(ctx, pod, store.Forward, at(70, 0), scope)
	if err != nil || len(ns) != 0 {
		t.Errorf("neighbors of the pod at +70m = %v, %v, want none", ns, err)
	}
}

// The edge is asserted through the pod's newest record, whichever kind it was, and
// not through the first one.
func TestLateResolvedEdgeIsAssertedThroughThePodsLatestRecord(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	p := podSpec{UID: "p1", Name: "p", Node: node, Created: at(0, 0), Phase: "Running", Heartbeat: "1"}
	pr := translate(t, tr, pullRec(at(5, 0), p.object()))
	if len(pr.Pending) != 1 {
		t.Fatalf("setup: %d pending, want the edge to wait for its node", len(pr.Pending))
	}
	p.Heartbeat = "2"
	mod := translate(t, tr, watchRec(at(8, 0), "MODIFIED", p.object()))
	if len(tr.Pending()) != 1 || len(mod.Records) != 1 || !mod.Records[0].EventTime.Equal(at(8, 0)) {
		t.Fatalf("setup: the unchanged MODIFIED gave %v, pending %d; want the pod at +8m and the edge still waiting", mod.Records, len(tr.Pending()))
	}
	nr := translate(t, tr, pullRec(at(60, 0), nodeObject("n-1", node, at(-1, 0), "1")))
	if len(nr.Records) != 2 {
		t.Fatalf("the node gave %s, want itself and the edge", joinSubjects(nr.Records))
	}
	edge := nr.Records[1]
	if !edge.EventTime.Equal(at(0, 0)) || !edge.Through.Equal(at(8, 0)) {
		t.Fatalf("edge = %+v, want it from the pod's time through the pod's newest record %s", edge, at(8, 0))
	}

	s := storeFrom(t, pr.Records, mod.Records, nr.Records)
	ctx, scope, pod := context.Background(), store.Current(catalog.L2), podFP(t, "p1")
	last := at(8, 0)
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{at(1, 0), true},
		{last, true},
		{last.Add(ttl - time.Nanosecond), true},
		{last.Add(ttl), false},
		{at(70, 0), false},
	} {
		got, err := s.Alive(ctx, pod, c.at, scope)
		if err != nil || got != c.want {
			t.Errorf("pod alive at +%s = %v, %v, want %v", c.at.Sub(t0), got, err, c.want)
		}
		ns, err := s.Neighbors(ctx, pod, store.Forward, c.at, scope)
		if got := len(ns) == 1 && ns[0].Relation == catalog.ScheduledOn; err != nil || got != c.want {
			t.Errorf("scheduled_on at +%s = %v, %v, want present = %v", c.at.Sub(t0), ns, err, c.want)
		}
	}
}
