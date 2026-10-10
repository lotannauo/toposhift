package k8sobjects_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The UIDs of the objects in the fixtures (the first eight characters of each).
const (
	uidEvicted  = "4a9d1695" // pod 42s2c, patched to Failed with reason Evicted
	uidRestart  = "fcc7cb19" // pod 4gjhs, patched to restartCount 3
	uidPodGC    = "c5ad95aa" // pod wr85m, failed by PodGC when its node was deleted
	uidSbt4s    = "df1a0034" // pod sbt4s, deleted
	uidNode2Old = "f41e8e62" // kwok-node-2, deleted
	uidNode2New = "4edef4ef" // kwok-node-2, re-created
	uidNode4    = "70f60f5f" // kwok-node-4, heartbeat only
	uidGap      = "1f037c43" // a pod created while the collector was down
)

// ofUID returns the inputs about the object whose UID starts with prefix, in order.
func ofUID(t testing.TB, in []input, prefix string) []input {
	t.Helper()
	var out []input
	for _, x := range in {
		if _, o := objectOf(x.Rec); strings.HasPrefix(subStr(o, "metadata", "uid"), prefix) {
			out = append(out, x)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no record of uid %s", prefix)
	}
	return out
}

func observedOf(x input) time.Time { return time.Unix(0, int64(x.Rec.ObservedTimeUnixNano)).UTC() }

func uidOf(x input) string {
	_, o := objectOf(x.Rec)
	return subStr(o, "metadata", "uid")
}

// payloadOf decodes the payload of a record.
func payloadOf(t testing.TB, r store.Record) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Payload, &m); err != nil {
		t.Fatalf("payload %q: %v", r.Payload, err)
	}
	return m
}

var (
	t0   = time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	node = "kwok-node-2"
)

func TestConfig(t *testing.T) {
	for name, c := range map[string]k8sobjects.Config{
		"no producer": {Resolver: resolver()},
		"no resolver": {Producer: testProducer},
		"negative":    {Producer: testProducer, Resolver: resolver(), EntityTTL: -time.Second},
	} {
		if _, err := k8sobjects.New(c); err == nil {
			t.Errorf("%s: New returned no error", name)
		}
	}
}

func TestNoObservedTimeIsAnError(t *testing.T) {
	tr := newTranslator(t, 0)
	lr := pullRec(t0, nodeObject("n1", "n", t0, ""))
	lr.ObservedTimeUnixNano = 0
	if _, err := tr.Translate(noResource, lr); !errors.Is(err, k8sobjects.ErrNoObservedTime) {
		t.Fatalf("err = %v, want ErrNoObservedTime", err)
	}
}

func TestEntityEventsAreSkipped(t *testing.T) {
	in := loadCapture(t, "run1.jsonl")
	tr := newTranslator(t, 0)
	n := 0
	for _, x := range in {
		if !strings.HasPrefix(x.Rec.EventName, "entity.") {
			continue
		}
		n++
		r, err := tr.Translate(x.Res, x.Rec)
		if err != nil {
			t.Fatalf("%s: %v", x.Name, err)
		}
		if len(r.Records) != 0 || len(r.Pending) != 0 || len(r.Skipped) != 1 || r.Skipped[0] != (k8sobjects.Skip{Reason: k8sobjects.SkipEntityEvent, Count: 1}) {
			t.Errorf("%s (%s): %+v, want one skip %q", x.Name, x.Rec.EventName, r, k8sobjects.SkipEntityEvent)
		}
	}
	if n < 2 {
		t.Fatalf("the fixture has %d entity events, want a state and a delete", n)
	}
}

func TestOtherKindsAreSkipped(t *testing.T) {
	in := loadCapture(t, "run1.jsonl")
	tr := newTranslator(t, 0)
	n := 0
	for _, x := range in {
		if _, o := objectOf(x.Rec); subStr(o, "kind") != "Event" {
			continue
		}
		n++
		r := translate(t, tr, x.Rec)
		if len(r.Records) != 0 || !hasSkip(r, k8sobjects.SkipUnsupportedKind) {
			t.Errorf("%s: %+v, want a skip %q and no records", x.Name, r, k8sobjects.SkipUnsupportedKind)
		}
	}
	if n == 0 {
		t.Fatal("the fixture has no Event object")
	}
}

// A watch ADDED followed by a MODIFIED that changes nothing the description
// lists gives one record, and a node's heartbeat gives none.
func TestAddedThenUnchangedModifiedGivesOneRecord(t *testing.T) {
	in := ofUID(t, loadCapture(t, "run1.jsonl"), uidNode2New)
	if len(in) != 2 {
		t.Fatalf("fixture has %d records of the node, want ADDED and MODIFIED", len(in))
	}
	rs := translateAll(t, newTranslator(t, 0), in)
	if len(rs[0].Records) != 1 || len(rs[1].Records) != 0 {
		t.Fatalf("got %d then %d records, want 1 then 0", len(rs[0].Records), len(rs[1].Records))
	}
	r := rs[0].Records[0]
	created := time.Date(2026, 10, 9, 16, 41, 25, 0, time.UTC)
	if r.Kind != lifecycle.Observe || !r.EventTime.Equal(created) || r.EventTimeBasis != store.BasisObjectField || r.TTL != 0 {
		t.Errorf("ADDED gave %+v, want an Observe at the creation time, basis object field, TTL 0", r)
	}
}

func TestSyntheticAddedThenUnchangedModified(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending", Heartbeat: "1"}
	added := translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
	p.Heartbeat = "2" // resourceVersion
	o := p.object()
	o["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "x"}}
	modified := translate(t, tr, watchRec(t0.Add(2*time.Second), "MODIFIED", o))
	if len(added.Records) != 1 || len(modified.Records) != 0 {
		t.Fatalf("got %d then %d records, want 1 then 0", len(added.Records), len(modified.Records))
	}
}

// The heartbeat of a node changes every ten minutes and is not a change. The
// fixture's last record is such a MODIFIED.
func TestHeartbeatOnlyChangeGivesNoRecord(t *testing.T) {
	in := ofUID(t, loadCapture(t, "run1.jsonl"), uidNode4)
	if len(in) != 2 {
		t.Fatalf("fixture has %d records of the node, want a pull and a MODIFIED", len(in))
	}
	rs := translateAll(t, newTranslator(t, 0), in)
	if len(rs[0].Records) != 1 || len(rs[1].Records) != 0 {
		t.Fatalf("got %d then %d records, want 1 then 0", len(rs[0].Records), len(rs[1].Records))
	}
	// The only differences between the two objects are the excluded fields.
	_, a := objectOf(in[0].Rec)
	_, b := objectOf(in[1].Rec)
	ca, _ := sub(a, "status", "conditions").([]any)
	cb, _ := sub(b, "status", "conditions").([]any)
	moved := false
	for i := range ca {
		if subStr(ca[i].(map[string]any), "lastHeartbeatTime") != subStr(cb[i].(map[string]any), "lastHeartbeatTime") {
			moved = true
		}
	}
	if !moved {
		t.Fatal("the fixture's lastHeartbeatTime did not move, so the test proves nothing")
	}
}

// No payload carries a field that changes with every write or heartbeat.
func TestPayloadsExcludeVolatileFields(t *testing.T) {
	for _, name := range []string{"run1.jsonl", "run2.jsonl"} {
		in := loadCapture(t, name)
		for i, r := range translateAll(t, newTranslator(t, 0), in) {
			for _, rec := range r.Records {
				p := strings.ToLower(string(rec.Payload))
				for _, bad := range []string{"heartbeat", "resourceversion", "managedfields", "lastprobe", "resource_version", "observed"} {
					if strings.Contains(p, bad) {
						t.Errorf("%s: payload %s contains %q", in[i].Name, rec.Payload, bad)
					}
				}
			}
		}
	}
}

func TestFailedEvictedPatch(t *testing.T) {
	in := ofUID(t, loadCapture(t, "run1.jsonl"), uidEvicted)
	rs := translateAll(t, newTranslator(t, 0), in)
	patch, x := only(t, rs[len(rs)-1]), in[len(in)-1]
	if patch.Kind != lifecycle.Observe || patch.Subject.A != podFP(t, uidOf(x)) {
		t.Fatalf("patch gave %+v", patch)
	}
	// A phase and a reason carry no time of their own.
	if !patch.EventTime.Equal(observedOf(x)) || patch.EventTimeBasis != store.BasisObserved {
		t.Errorf("event time %s basis %s, want the observed time %s, observed", patch.EventTime, patch.EventTimeBasis, observedOf(x))
	}
	p := payloadOf(t, patch)
	if p["phase"] != "Failed" || p["reason"] != "Evicted" {
		t.Errorf("payload phase %v reason %v, want Failed and Evicted", p["phase"], p["reason"])
	}
}

// restartCount 3 with a lastState time in January: lastState is never read, so
// the change takes the observed time.
func TestRestartCountThree(t *testing.T) {
	in := ofUID(t, loadCapture(t, "run1.jsonl"), uidRestart)
	_, last := objectOf(in[len(in)-1].Rec)
	cs := sub(last, "status", "containerStatuses").([]any)
	ls := cs[0].(map[string]any)["lastState"].(map[string]any)["terminated"].(map[string]any)
	if subStr(ls, "startedAt") != "2026-01-01T00:00:00Z" {
		t.Fatalf("the fixture's lastState.terminated.startedAt is %q, want January", subStr(ls, "startedAt"))
	}
	rs := translateAll(t, newTranslator(t, 0), in)
	r, x := only(t, rs[len(rs)-1]), in[len(in)-1]
	if !r.EventTime.Equal(observedOf(x)) || r.EventTimeBasis != store.BasisObserved {
		t.Errorf("event time %s basis %s, want %s, observed", r.EventTime, r.EventTimeBasis, observedOf(x))
	}
	c := payloadOf(t, r)["containers"].([]any)[0].(map[string]any)
	if c["restart_count"] != float64(3) {
		t.Errorf("restart_count = %v, want 3", c["restart_count"])
	}
	if len(skips(rs[len(rs)-1])) != 1 || skips(rs[len(rs)-1])[k8sobjects.SkipContainerNoID] != 1 {
		t.Errorf("skips = %v, want only the container without an id", skips(rs[len(rs)-1]))
	}
}

// A node deleted and created again under its name is two entities.
func TestNodeRecreatedUnderNewUID(t *testing.T) {
	all := loadCapture(t, "run1.jsonl")
	oldIn, newIn := ofUID(t, all, uidNode2Old), ofUID(t, all, uidNode2New)
	tr := newTranslator(t, 0)
	rs := translateAll(t, tr, oldIn)
	del := only(t, rs[len(rs)-1])
	if del.Kind != lifecycle.Delete || del.Subject.A != nodeFP(t, uidOf(oldIn[0])) || del.EventTimeBasis != store.BasisObserved || !del.EventTime.Equal(observedOf(oldIn[len(oldIn)-1])) {
		t.Fatalf("DELETED gave %+v", del)
	}
	rs = translateAll(t, tr, newIn)
	add := only(t, rs[0])
	if add.Kind != lifecycle.Observe || add.Subject.A != nodeFP(t, uidOf(newIn[0])) || add.Subject.A == del.Subject.A {
		t.Fatalf("ADDED gave %+v, want an Observe of a new entity", add)
	}
	if add.EventTimeBasis != store.BasisObjectField || add.EventTime.After(observedOf(newIn[0])) {
		t.Errorf("ADDED event time %s basis %s, want the creation time, object field", add.EventTime, add.EventTimeBasis)
	}
	// The old UID is gone for good: a late record about it is set aside.
	late := translate(t, tr, oldIn[0].Rec)
	if len(late.Records) != 0 || !hasSkip(late, k8sobjects.SkipAfterDelete) {
		t.Errorf("a record after DELETED gave %+v", late)
	}
}

// A pod's end is its own DELETED, never its node's.
func TestPodEndsAtItsOwnDeleted(t *testing.T) {
	in := loadCapture(t, "run1.jsonl")
	rs := translateAll(t, newTranslator(t, 0), in)
	podUID := ""
	for _, x := range in {
		if strings.HasPrefix(uidOf(x), uidPodGC) {
			podUID = uidOf(x)
		}
	}
	pod := podFP(t, podUID)
	var nodeDeletedAt, podDeletedAt time.Time
	for i, x := range in {
		typ, o := objectOf(x.Rec)
		if typ != "DELETED" {
			continue
		}
		switch subStr(o, "kind") {
		case "Node":
			nodeDeletedAt = observedOf(x)
			for _, r := range rs[i].Records {
				if r.Subject.A == pod || r.Subject.B == pod {
					t.Errorf("the node's DELETED ended the pod: %+v", r)
				}
			}
			if len(rs[i].Records) != 1 {
				t.Errorf("the node's DELETED gave %d records, want only the node's own", len(rs[i].Records))
			}
		case "Pod":
			if uidOf(x) != podUID {
				continue
			}
			podDeletedAt = observedOf(x)
			var ends []store.Record
			for _, r := range rs[i].Records {
				if r.Kind == lifecycle.Delete {
					ends = append(ends, r)
				}
			}
			if len(ends) != 2 { // the edge and the pod
				t.Fatalf("the pod's DELETED gave %d deletes, want 2 (edge, pod): %v", len(ends), rs[i].Records)
			}
			for _, r := range ends {
				if !r.EventTime.Equal(podDeletedAt) || r.EventTimeBasis != store.BasisObserved {
					t.Errorf("end %+v, want the pod's observed time, observed", r)
				}
			}
		}
	}
	if !nodeDeletedAt.Before(podDeletedAt) {
		t.Fatalf("the node was deleted at %s, the pod at %s: the fixture does not show PodGC", nodeDeletedAt, podDeletedAt)
	}
}

// A pod created while the collector was down has no ADDED. It is first seen in a
// pull and is born at its creation time.
func TestGapPodFirstSeenInPull(t *testing.T) {
	in := loadCapture(t, "run2.jsonl")
	tr := newTranslator(t, 0)
	first := -1
	for i, x := range in {
		if strings.HasPrefix(uidOf(x), uidGap) {
			first = i
			break
		}
	}
	if typ, _ := objectOf(in[first].Rec); typ != "" {
		t.Fatalf("the first record of the pod is a %s, want a list item", typ)
	}
	translateAll(t, tr, in[:first])
	r := translate(t, tr, in[first].Rec).Records
	if len(r) != 2 {
		t.Fatalf("first sight gave %d records, want the pod and its edge: %v", len(r), r)
	}
	_, o := objectOf(in[first].Rec)
	created, _ := time.Parse(time.RFC3339, subStr(o, "metadata", "creationTimestamp"))
	if !created.Before(observedOf(in[first])) {
		t.Fatal("the creation time is not before the observed time, so the test proves nothing")
	}
	for _, rec := range r {
		if !rec.EventTime.Equal(created) || rec.EventTimeBasis != store.BasisObjectField {
			t.Errorf("%+v: want the creation time %s, object field", rec, created)
		}
	}

	// A list item of a UID already seen takes the observed time, changed or not.
	changed := in[first].Rec
	changed.ObservedTimeUnixNano += capture.Uint64(time.Minute)
	_, obj := objectOf(changed)
	obj["status"].(map[string]any)["phase"] = "Failed"
	// A condition that carries a newer time of its own, which a watch event would use.
	obj["status"].(map[string]any)["conditions"] = append(sub(obj, "status", "conditions").([]any), map[string]any{
		"type": "DisruptionTarget", "status": "True", "lastTransitionTime": ts(observedOf(in[first]).Add(30 * time.Second)),
	})
	changed.Body = bodyOf(obj)
	rec := only(t, translate(t, tr, changed))
	if !rec.EventTime.Equal(observedOf(input{Rec: changed})) || rec.EventTimeBasis != store.BasisObserved {
		t.Errorf("a changed list item of a known UID gave %+v, want the observed time, observed", rec)
	}
}

func bodyOf(o map[string]any) *capture.AnyValue {
	v := val(o)
	return &v
}

// A pod that names a node the index does not know yet is pending, and is resolved
// when a node whose interval covers the pod's time arrives.
func TestPendingEdgeResolvedWhenNodeArrives(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{UID: "pod-1", Name: "p1", Node: node, Created: t0.Add(10 * time.Second), Phase: "Running"}
	r := translate(t, tr, pullRec(t0.Add(5*time.Minute), p.object()))
	if len(r.Records) != 1 || len(r.Pending) != 1 {
		t.Fatalf("got %d records and %d pending, want the pod and one pending edge", len(r.Records), len(r.Pending))
	}
	want := k8sobjects.PendingEdge{
		Pod: podFP(t, "pod-1"), PodUID: "pod-1", NodeName: node,
		Time: p.Created, Basis: store.BasisObjectField,
	}
	if r.Pending[0] != want {
		t.Errorf("pending = %+v, want %+v", r.Pending[0], want)
	}
	if got := tr.Pending(); len(got) != 1 || got[0] != want {
		t.Errorf("Pending() = %v", got)
	}

	// An unrelated node does not resolve it.
	other := translate(t, tr, pullRec(t0.Add(5*time.Minute+time.Second), nodeObject("n-other", "other", t0, "")))
	if len(other.Records) != 1 || len(tr.Pending()) != 1 {
		t.Fatalf("an unrelated node gave %d records, %d pending", len(other.Records), len(tr.Pending()))
	}

	nr := translate(t, tr, pullRec(t0.Add(5*time.Minute+2*time.Second), nodeObject("n-2", node, t0, "")))
	if len(nr.Records) != 2 || len(tr.Pending()) != 0 {
		t.Fatalf("the node gave %d records and left %d pending, want the node and the edge", len(nr.Records), len(tr.Pending()))
	}
	edge := nr.Records[1]
	if edge.Subject != store.EdgeSubject(podFP(t, "pod-1"), nodeFP(t, "n-2"), catalog.ScheduledOn) || edge.Kind != lifecycle.Observe || edge.TTL != 0 {
		t.Fatalf("edge = %+v", edge)
	}
	if !edge.EventTime.Equal(p.Created) || edge.EventTimeBasis != store.BasisObjectField {
		t.Errorf("edge at %s basis %s, want the pod record's time %s, object field", edge.EventTime, edge.EventTimeBasis, p.Created)
	}
	var payload struct {
		NodeName   string `json:"node_name"`
		Resolution struct {
			Basis string `json:"basis"`
			Start string `json:"node_interval_start"`
		} `json:"resolution"`
	}
	if err := json.Unmarshal(edge.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.NodeName != node || payload.Resolution.Basis != "node-name-index" || payload.Resolution.Start != t0.Format(time.RFC3339Nano) {
		t.Errorf("edge payload = %s", edge.Payload)
	}

	// The pod's DELETED ends the edge that was resolved late.
	del := translate(t, tr, watchRec(t0.Add(time.Hour), "DELETED", p.object()))
	if len(del.Records) != 2 || del.Records[0].Subject != edge.Subject || del.Records[0].Kind != lifecycle.Delete {
		t.Errorf("DELETED gave %v, want the edge's delete and the pod's", del.Records)
	}
}

// A node whose interval starts after the pod's time does not cover it.
func TestNodeCreatedAfterPodTimeDoesNotResolve(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{UID: "pod-1", Name: "p1", Node: node, Created: t0, Phase: "Running"}
	translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	r := translate(t, tr, pullRec(t0.Add(2*time.Minute), nodeObject("n-2", node, t0.Add(30*time.Second), "")))
	if len(r.Records) != 1 || len(tr.Pending()) != 1 {
		t.Fatalf("got %d records, %d pending, want the node and still one pending", len(r.Records), len(tr.Pending()))
	}
}

// Between the DELETED of a node and its re-creation under the same name, a pod
// that names it stays pending, and resolves to the new UID only if its time is
// after the new node's creation.
func TestPendingAcrossNodeRecreation(t *testing.T) {
	tr := newTranslator(t, 0)
	oldNode := nodeObject("n-old", node, t0, "")
	translate(t, tr, pullRec(t0.Add(time.Minute), oldNode))
	deleted := t0.Add(10 * time.Minute)
	del := translate(t, tr, watchRec(deleted, "DELETED", oldNode))
	if len(del.Records) != 1 || del.Records[0].Kind != lifecycle.Delete {
		t.Fatalf("node DELETED gave %v", del.Records)
	}

	inGap := podSpec{UID: "pod-gap", Name: "gap", Node: node, Created: deleted.Add(30 * time.Second), Phase: "Running"}
	r := translate(t, tr, watchRec(deleted.Add(time.Minute), "ADDED", inGap.object()))
	if len(r.Records) != 1 || len(r.Pending) != 1 {
		t.Fatalf("a pod in the gap gave %d records, %d pending, want its own record and a pending edge", len(r.Records), len(r.Pending))
	}

	recreated := t0.Add(11 * time.Minute)
	nr := translate(t, tr, watchRec(recreated.Add(2*time.Second), "ADDED", nodeObject("n-new", node, recreated, "")))
	if len(nr.Records) != 1 {
		t.Fatalf("the new node gave %d records, want only its own: %v", len(nr.Records), nr.Records)
	}
	if got := tr.Pending(); len(got) != 1 || got[0].PodUID != "pod-gap" {
		t.Fatalf("the pod from the gap resolved to the new node: pending = %v", got)
	}

	after := podSpec{UID: "pod-after", Name: "after", Node: node, Created: recreated.Add(30 * time.Second), Phase: "Running"}
	ar := translate(t, tr, watchRec(recreated.Add(time.Minute), "ADDED", after.object()))
	if len(ar.Records) != 2 || len(ar.Pending) != 0 {
		t.Fatalf("a pod after the re-creation gave %d records, %d pending, want the pod and its edge", len(ar.Records), len(ar.Pending))
	}
	if e := ar.Records[1]; e.Subject.B != nodeFP(t, "n-new") {
		t.Errorf("the edge points at %s, want the new node", e.Subject.B)
	}
	// And a pod in the first node's lifetime, seen late, resolves to the old UID.
	before := podSpec{UID: "pod-before", Name: "before", Node: node, Created: t0.Add(5 * time.Minute), Phase: "Running"}
	br := translate(t, tr, pullRec(recreated.Add(2*time.Minute), before.object()))
	if len(br.Records) != 2 || br.Records[1].Subject.B != nodeFP(t, "n-old") {
		t.Errorf("a pod from the first node's lifetime gave %v, want an edge to the old UID", br.Records)
	}

	// A pending edge whose pod is deleted first is dropped, and counted.
	gone := translate(t, tr, watchRec(recreated.Add(3*time.Minute), "DELETED", inGap.object()))
	if len(gone.Records) != 1 || len(tr.Pending()) != 0 || skips(gone)[k8sobjects.SkipPendingDroppedDeleted] != 1 {
		t.Errorf("DELETED of a pending pod gave %v, pending %v, skips %v", gone.Records, tr.Pending(), skips(gone))
	}
}

// Two live nodes under one name cannot be told apart, so nothing is guessed.
func TestAmbiguousNodeNameStaysPending(t *testing.T) {
	tr := newTranslator(t, 0)
	translate(t, tr, pullRec(t0.Add(time.Minute), nodeObject("n-a", node, t0, "")))
	translate(t, tr, pullRec(t0.Add(time.Minute), nodeObject("n-b", node, t0, "")))
	p := podSpec{UID: "pod-1", Name: "p", Node: node, Created: t0.Add(10 * time.Second), Phase: "Running"}
	r := translate(t, tr, pullRec(t0.Add(2*time.Minute), p.object()))
	if len(r.Records) != 1 || len(r.Pending) != 1 {
		t.Fatalf("got %d records, %d pending, want the pod and a pending edge", len(r.Records), len(r.Pending))
	}
}

func TestContainerWithoutIDIsSkipped(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{
		UID: "p1", Name: "p", Created: t0, Phase: "Running",
		Containers: []map[string]any{
			{"name": "a", "restartCount": 0},
			{"name": "b", "restartCount": 0, "containerID": ""},
		},
	}
	r := translate(t, tr, pullRec(t0.Add(time.Second), p.object()))
	if len(r.Records) != 1 || r.Records[0].Subject.A.Type() != catalog.K8sPod {
		t.Fatalf("got %v, want only the pod", r.Records)
	}
	if got := skips(r)[k8sobjects.SkipContainerNoID]; got != 2 {
		t.Errorf("skipped %d containers without an id, want 2", got)
	}
}

// A container with a runtime id is an entity, part of its pod; a restart gives it
// a new id, which ends the old container; the pod's DELETED ends what is left.
func TestContainerWithIDIsEntityPartOfPod(t *testing.T) {
	tr := newTranslator(t, 0)
	spec := func(ids ...string) podSpec {
		p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running"}
		for i, id := range ids {
			c := map[string]any{"name": []string{"app", "side"}[i], "restartCount": 0}
			if id != "" {
				c["containerID"] = id
			}
			p.Containers = append(p.Containers, c)
		}
		return p
	}
	pod := podFP(t, "p1")
	r := translate(t, tr, pullRec(t0.Add(time.Second), spec("containerd://abc", "").object()))
	if len(r.Records) != 3 {
		t.Fatalf("got %d records (%s), want the pod, a container, a part_of edge", len(r.Records), joinSubjects(r.Records))
	}
	c, e := r.Records[1], r.Records[2]
	if c.Subject != store.EntitySubject(containerFP(t, "abc")) || c.Layer != catalog.L2 || c.TTL != 0 {
		t.Errorf("container record = %+v", c)
	}
	if e.Subject != store.EdgeSubject(containerFP(t, "abc"), pod, catalog.PartOf) || e.Kind != lifecycle.Observe || e.TTL != 0 {
		t.Errorf("edge record = %+v", e)
	}
	if skips(r)[k8sobjects.SkipContainerNoID] != 1 {
		t.Errorf("skips = %v, want one container without an id", skips(r))
	}

	// The same containers again: nothing new.
	if r := translate(t, tr, pullRec(t0.Add(2*time.Second), spec("containerd://abc", "").object())); len(r.Records) != 0 {
		t.Errorf("an unchanged pod gave %v", r.Records)
	}

	// A restart: a new id, and the pod's description changes with the count.
	restarted := spec("containerd://def", "")
	restarted.Containers[0]["restartCount"] = 1
	r = translate(t, tr, watchRec(t0.Add(time.Minute), "MODIFIED", restarted.object()))
	var got []string
	for _, rec := range r.Records {
		got = append(got, rec.Kind.String()+" "+string(rec.Subject.Relation)+" "+string(rec.Subject.A.Type()))
	}
	want := []string{"observe  k8s.pod", "delete part_of container", "delete  container", "observe  container", "observe part_of container"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("restart gave %q, want %q", got, want)
	}

	// The pod's DELETED ends the new container, its edge, and the pod.
	r = translate(t, tr, watchRec(t0.Add(time.Hour), "DELETED", restarted.object()))
	if len(r.Records) != 3 {
		t.Fatalf("DELETED gave %v", r.Records)
	}
	for _, rec := range r.Records {
		if rec.Kind != lifecycle.Delete || rec.TTL != 0 || len(rec.Payload) != 0 {
			t.Errorf("record %+v is not a plain delete", rec)
		}
	}
	if r.Records[2].Subject != store.EntitySubject(pod) {
		t.Errorf("the last record is %+v, want the pod", r.Records[2])
	}

	// An id with no runtime prefix is not trusted.
	bad := translate(t, newTranslator(t, 0), pullRec(t0, spec("abc", "").object()))
	if len(bad.Records) != 1 || skips(bad)[k8sobjects.SkipBadContainerID] != 1 {
		t.Errorf("an id without a prefix gave %v, skips %v", bad.Records, skips(bad))
	}
}

// deletionTimestamp says deletion was requested. It is an attribute, and nothing
// ends until the object's own DELETED.
func TestDeletionTimestampIsAnAttributeNeverAnEnd(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{UID: "p1", Name: "p", Node: "", Created: t0, Phase: "Running", Conds: []map[string]any{cond("Ready", "True", t0)}}
	translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))

	p.Extra = map[string]any{"deletionTimestamp": ts(t0.Add(time.Minute)), "deletionGracePeriodSeconds": 30}
	obs := t0.Add(time.Minute + 500*time.Millisecond)
	r := translate(t, tr, watchRec(obs, "MODIFIED", p.object()))
	rec := only(t, r)
	if rec.Kind != lifecycle.Observe {
		t.Fatalf("deletionTimestamp gave a %s", rec.Kind)
	}
	if got := payloadOf(t, rec)["deletion_timestamp"]; got != ts(t0.Add(time.Minute)) {
		t.Errorf("deletion_timestamp = %v, want it kept as an attribute", got)
	}
	// It is not a time the translator takes for the change: the change is at the
	// observed time.
	if !rec.EventTime.Equal(obs) || rec.EventTimeBasis != store.BasisObserved {
		t.Errorf("event time %s basis %s, want the observed time, observed", rec.EventTime, rec.EventTimeBasis)
	}

	end := t0.Add(2 * time.Minute)
	r = translate(t, tr, watchRec(end, "DELETED", p.object()))
	rec = only(t, r)
	if rec.Kind != lifecycle.Delete || !rec.EventTime.Equal(end) || rec.EventTimeBasis != store.BasisObserved {
		t.Errorf("DELETED gave %+v, want a delete at the observed time", rec)
	}
}

// A list that omits an object proves nothing. The translator ends an object only
// at its own DELETED.
func TestAbsentPullItemInfersNoDelete(t *testing.T) {
	tr := newTranslator(t, 0)
	mk := func(uid string) map[string]any {
		return podSpec{UID: uid, Name: uid, Created: t0, Phase: "Running"}.object()
	}
	pulls := [][]string{{"p1", "p2", "p3"}, {"p1", "p3"}, {"p1"}, {"p1"}}
	var all []store.Record
	for i, pull := range pulls {
		at := t0.Add(time.Duration(i+1) * 10 * time.Minute)
		for _, uid := range pull {
			all = append(all, translate(t, tr, pullRec(at, mk(uid))).Records...)
		}
	}
	// Only the first pull's three pods are records; nothing is a Delete.
	if len(all) != 3 {
		t.Fatalf("got %d records, want the 3 observations of the first pull: %v", len(all), all)
	}
	for _, r := range all {
		if r.Kind != lifecycle.Observe {
			t.Errorf("record %+v is not an observation", r)
		}
	}
}

// Identity is the UID. Two pods with one name are two entities.
func TestKeyedByUIDNotName(t *testing.T) {
	tr := newTranslator(t, 0)
	a := podSpec{UID: "uid-a", Name: "same", Created: t0, Phase: "Running"}
	b := podSpec{UID: "uid-b", Name: "same", Created: t0.Add(time.Hour), Phase: "Running"}
	ra := only(t, translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", a.object())))
	end := translate(t, tr, watchRec(t0.Add(time.Minute), "DELETED", a.object()))
	rb := only(t, translate(t, tr, watchRec(t0.Add(time.Hour+time.Second), "ADDED", b.object())))
	if ra.Subject.A == rb.Subject.A || ra.Subject.A != podFP(t, "uid-a") || rb.Subject.A != podFP(t, "uid-b") {
		t.Fatalf("subjects %v and %v, want the fingerprints of the two UIDs", ra.Subject.A, rb.Subject.A)
	}
	if only(t, end).Subject.A != ra.Subject.A {
		t.Errorf("DELETED ended %v, want the pod uid-a", end.Records)
	}
	if !rb.EventTime.Equal(b.Created) || rb.EventTimeBasis != store.BasisObjectField {
		t.Errorf("the second pod was born at %s basis %s, want its own creation time, object field", rb.EventTime, rb.EventTimeBasis)
	}
	// The same name alive at once, under two UIDs, with the same description.
	tr = newTranslator(t, 0)
	b.Created = a.Created
	r1 := translate(t, tr, pullRec(t0.Add(time.Second), a.object()))
	r2 := translate(t, tr, pullRec(t0.Add(time.Second), b.object()))
	if len(r1.Records) != 1 || len(r2.Records) != 1 || r1.Records[0].Subject.A == r2.Records[0].Subject.A {
		t.Errorf("pods of one name and two UIDs gave %v and %v", r1.Records, r2.Records)
	}
}

// A time taken from an object is used only between its creation and the observed
// time plus five minutes.
func TestImplausibleFieldTimes(t *testing.T) {
	obs := t0.Add(time.Hour)
	base := func() podSpec {
		return podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending", Conds: []map[string]any{cond("PodScheduled", "True", t0)}}
	}
	for _, tc := range []struct {
		name      string
		at        time.Time
		wantField bool
	}{
		{"just before the limit", obs.Add(k8sobjects.MaxClockAhead), true},
		{"past the limit", obs.Add(k8sobjects.MaxClockAhead + time.Second), false},
		{"before creation", t0.Add(-time.Second), false},
		{"far in the past", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"inside", t0.Add(time.Minute), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTranslator(t, 0)
			p := base()
			translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
			p.Phase = "Running"
			p.Conds = append(p.Conds, cond("Ready", "True", tc.at))
			r := translate(t, tr, watchRec(obs, "MODIFIED", p.object()))
			rec := only(t, r)
			if tc.wantField {
				if !rec.EventTime.Equal(tc.at) || rec.EventTimeBasis != store.BasisObjectField || len(r.Skipped) != 0 {
					t.Errorf("got %+v skips %v, want the field time %s", rec, r.Skipped, tc.at)
				}
				return
			}
			if !rec.EventTime.Equal(obs) || rec.EventTimeBasis != store.BasisObserved || skips(r)[k8sobjects.SkipImplausibleTime] != 1 {
				t.Errorf("got %+v skips %v, want the observed time and one %q", rec, r.Skipped, k8sobjects.SkipImplausibleTime)
			}
		})
	}

	t.Run("lastState is never read", func(t *testing.T) {
		tr := newTranslator(t, 0)
		p := base()
		p.Containers = []map[string]any{{"name": "app", "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": ts(t0)}}}}
		translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
		p.Containers = []map[string]any{{
			"name": "app", "restartCount": 3,
			"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
			"lastState": map[string]any{"terminated": map[string]any{"startedAt": "2026-01-01T00:00:00Z", "finishedAt": "2026-01-01T00:00:05Z"}},
		}}
		r := translate(t, tr, watchRec(obs, "MODIFIED", p.object()))
		rec := only(t, r)
		if !rec.EventTime.Equal(obs) || rec.EventTimeBasis != store.BasisObserved || skips(r)[k8sobjects.SkipImplausibleTime] != 0 {
			t.Errorf("got %+v skips %v, want the observed time and no implausible time", rec, r.Skipped)
		}
	})

	t.Run("an unreadable time", func(t *testing.T) {
		tr := newTranslator(t, 0)
		p := base()
		translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
		p.Phase = "Running"
		bad := cond("Ready", "True", t0)
		bad["lastTransitionTime"] = "yesterday"
		p.Conds = append(p.Conds, bad)
		r := translate(t, tr, watchRec(obs, "MODIFIED", p.object()))
		rec := only(t, r)
		if !rec.EventTime.Equal(obs) || rec.EventTimeBasis != store.BasisObserved || skips(r)[k8sobjects.SkipImplausibleTime] != 1 {
			t.Errorf("got %+v skips %v", rec, r.Skipped)
		}
	})

	t.Run("a creation time ahead of the observed time", func(t *testing.T) {
		tr := newTranslator(t, 0)
		p := base()
		p.Created = obs.Add(time.Hour)
		rec := only(t, translate(t, tr, watchRec(obs, "ADDED", p.object())))
		if !rec.EventTime.Equal(obs) || rec.EventTimeBasis != store.BasisObserved {
			t.Errorf("got %+v, want the observed time", rec)
		}
	})
}

// The newest changed time wins, and a record never goes back before the
// previous one of its UID.
func TestChangedTimesAndMonotonic(t *testing.T) {
	tr := newTranslator(t, 0)
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending", Conds: []map[string]any{cond("PodScheduled", "True", t0)}}
	translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))

	p.Conds = []map[string]any{
		cond("PodScheduled", "True", t0),
		cond("Initialized", "True", t0.Add(10*time.Second)),
		cond("Ready", "True", t0.Add(20*time.Second)),
	}
	p.Containers = []map[string]any{{"name": "app", "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": ts(t0.Add(15 * time.Second))}}}}
	rec := only(t, translate(t, tr, watchRec(t0.Add(time.Minute), "MODIFIED", p.object())))
	if !rec.EventTime.Equal(t0.Add(20*time.Second)) || rec.EventTimeBasis != store.BasisObjectField {
		t.Errorf("event time %s basis %s, want the newest changed time %s", rec.EventTime, rec.EventTimeBasis, t0.Add(20*time.Second))
	}

	// A change without a time of its own, observed before the last record's time
	// (clocks differ), does not move the pod back.
	p.Phase = "Running"
	rec = only(t, translate(t, tr, watchRec(t0.Add(10*time.Second), "MODIFIED", p.object())))
	if !rec.EventTime.Equal(t0.Add(20 * time.Second)) {
		t.Errorf("event time %s, want it held at the previous record's %s", rec.EventTime, t0.Add(20*time.Second))
	}
}

// With a TTL, every pod and node record is an observation with that TTL, so each
// pull refreshes it; edges and containers have none.
func TestEntityTTL(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	n := nodeObject("n-1", node, t0, "1")
	p := podSpec{
		UID: "p1", Name: "p", Node: node, Created: t0.Add(time.Second), Phase: "Running",
		Containers: []map[string]any{{"name": "app", "restartCount": 0, "containerID": "containerd://abc"}},
	}
	nr := translate(t, tr, pullRec(t0.Add(time.Minute), n))
	if rec := only(t, nr); rec.TTL != ttl {
		t.Errorf("node TTL = %s, want %s", rec.TTL, ttl)
	}
	pr := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	if len(pr.Records) != 4 {
		t.Fatalf("pod gave %d records (%s), want pod, container, part_of, scheduled_on", len(pr.Records), joinSubjects(pr.Records))
	}
	for _, r := range pr.Records {
		want := time.Duration(0)
		if r.Subject.Kind == store.SubjectEntity {
			want = ttl // the pod and its container; the edges have none
		}
		if r.TTL != want {
			t.Errorf("%s TTL = %s, want %s", r.Subject.A.Type(), r.TTL, want)
		}
	}

	// A refresh: nothing changed but the heartbeat.
	n2 := nodeObject("n-1", node, t0, "2")
	p.Heartbeat = "2"
	at := t0.Add(10 * time.Minute)
	nr = translate(t, tr, pullRec(at, n2))
	pr = translate(t, tr, pullRec(at, p.object()))
	// The pod and its container are refreshed; the edges are not re-asserted.
	if len(nr.Records) != 1 || len(pr.Records) != 2 {
		t.Fatalf("refresh gave %d node and %d pod records, want 1 and 2 (pod, container)", len(nr.Records), len(pr.Records))
	}
	for _, rec := range append(nr.Records, pr.Records...) {
		if rec.Kind != lifecycle.Observe || rec.Subject.Kind != store.SubjectEntity || rec.TTL != ttl || !rec.EventTime.Equal(at) || rec.EventTimeBasis != store.BasisObserved {
			t.Errorf("refresh = %+v, want an entity observation at the observed time with the TTL", rec)
		}
	}
	// A watch event that changed nothing is a refresh too.
	rs := translate(t, tr, watchRec(at.Add(time.Minute), "MODIFIED", p.object())).Records
	if len(rs) != 2 || rs[0].TTL != ttl || rs[0].EventTimeBasis != store.BasisObserved {
		t.Errorf("unchanged MODIFIED = %v", rs)
	}
	// A removal has none.
	for _, r := range translate(t, tr, watchRec(at.Add(time.Hour), "DELETED", p.object())).Records {
		if r.Kind != lifecycle.Delete || r.TTL != 0 {
			t.Errorf("removal record %+v", r)
		}
	}
}

// The payload holds the allow-listed fields and nothing the observer adds.
func TestPayloadAllowList(t *testing.T) {
	tr := newTranslator(t, 0)
	n := nodeObject("n-1", node, t0, "1")
	n["spec"] = map[string]any{"providerID": "kwok://n-1"}
	n["status"].(map[string]any)["addresses"] = []any{
		map[string]any{"type": "InternalIP", "address": "10.0.0.2"},
		map[string]any{"type": "Hostname", "address": node},
	}
	n["status"].(map[string]any)["nodeInfo"] = map[string]any{"machineID": "m", "systemUUID": "s", "bootID": "b", "kubeletVersion": "v1"}
	rec := only(t, translate(t, tr, pullRec(t0.Add(time.Minute), n)))
	if rec.Boot != "" || rec.Seq != 0 || !rec.Through.IsZero() || rec.Producer != testProducer {
		t.Errorf("record envelope = %+v", rec)
	}
	want := `{"addresses":[{"address":"kwok-node-2","type":"Hostname"},{"address":"10.0.0.2","type":"InternalIP"}],` +
		`"conditions":[{"last_transition_time":"2026-10-09T10:00:00Z","reason":"KubeletReady","status":"True","type":"Ready"}],` +
		`"labels":{"kubernetes.io/hostname":"kwok-node-2"},"name":"kwok-node-2",` +
		`"node_info":{"boot_id":"b","machine_id":"m","system_uuid":"s"},"provider_id":"kwok://n-1"}`
	if string(rec.Payload) != want {
		t.Errorf("node payload\n got %s\nwant %s", rec.Payload, want)
	}

	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running"}
	o := p.object()
	o["status"].(map[string]any)["hostIP"] = "192.168.0.1"
	o["status"].(map[string]any)["podIPs"] = []any{map[string]any{"ip": "10.1.0.1"}, map[string]any{"ip": "fd00::1"}}
	o["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{"kind": "Other", "name": "x"},
		map[string]any{"kind": "ReplicaSet", "name": "rs", "controller": true},
	}
	rec = only(t, translate(t, tr, pullRec(t0.Add(time.Minute), o)))
	want = `{"host_ip":"192.168.0.1","name":"p","namespace":"default","owner":{"kind":"ReplicaSet","name":"rs"},"phase":"Running","pod_ips":["10.1.0.1","fd00::1"]}`
	if string(rec.Payload) != want {
		t.Errorf("pod payload\n got %s\nwant %s", rec.Payload, want)
	}
}

func TestRecordsCarryNoSeqAndTheProducer(t *testing.T) {
	for _, r := range records(translateAll(t, newTranslator(t, 0), loadCapture(t, "run1.jsonl"))) {
		if r.Seq != 0 || r.Boot != "" || !r.Through.IsZero() || r.Producer != testProducer || r.TTL != 0 {
			t.Fatalf("record %+v: want no Seq, Boot or Through, the producer, TTL 0", r)
		}
		if r.Kind == lifecycle.Delete && len(r.Payload) != 0 {
			t.Fatalf("delete with a payload: %+v", r)
		}
		if r.EventTimeBasis != store.BasisObjectField && r.EventTimeBasis != store.BasisObserved {
			t.Fatalf("record %+v: basis must be object field or observed", r)
		}
	}
}

// With a TTL, every pod and node record of the fixtures is an observation with
// it, whether the description changed or not, and nothing else has one.
func TestEntityTTLOnFixtures(t *testing.T) {
	const ttl = 15 * time.Minute
	for _, name := range captures {
		in := loadCapture(t, name)
		rs := translateAll(t, newTranslator(t, ttl), in)
		for i, r := range rs {
			typ, o := objectOf(in[i].Rec)
			kind := subStr(o, "kind")
			entity := (kind == "Pod" || kind == "Node") && typ != "DELETED"
			var own []store.Record
			for _, rec := range r.Records {
				isOwn := rec.Subject.Kind == store.SubjectEntity && (rec.Subject.A.Type() == catalog.K8sPod || rec.Subject.A.Type() == catalog.K8sNode) && rec.Kind == lifecycle.Observe
				if isOwn {
					own = append(own, rec)
				}
				if want := map[bool]time.Duration{true: ttl, false: 0}[isOwn]; rec.TTL != want {
					t.Errorf("%s: record %+v has TTL %s, want %s", in[i].Name, rec.Subject, rec.TTL, want)
				}
			}
			if entity && len(own) != 1 {
				t.Errorf("%s: %d pod or node observations, want 1", in[i].Name, len(own))
			}
		}
	}
}

// Records that are not a pod or a node, or that are unusable, are set aside with a
// reason and never an error.
func TestOddRecords(t *testing.T) {
	tr := newTranslator(t, 0)
	good := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running"}.object()

	bookmark := translate(t, tr, logRec(t0, map[string]any{"type": "BOOKMARK", "object": good}, "pods"))
	if len(bookmark.Records) != 0 || !hasSkip(bookmark, k8sobjects.SkipUnsupportedWatchType) {
		t.Errorf("BOOKMARK gave %+v", bookmark)
	}
	empty := capture.LogRecord{ObservedTimeUnixNano: capture.Uint64(t0.UnixNano())}
	if r := translate(t, tr, empty); !hasSkip(r, k8sobjects.SkipUnsupportedBody) {
		t.Errorf("a record with no body gave %+v", r)
	}
	nouid := podSpec{Name: "p", Created: t0, Phase: "Running"}.object()
	delete(nouid["metadata"].(map[string]any), "uid")
	if r := translate(t, tr, pullRec(t0, nouid)); len(r.Records) != 0 || !hasSkip(r, k8sobjects.SkipMissingUID) {
		t.Errorf("an object with no uid gave %+v", r)
	}
	// The kind may be missing from a list item; the resource name then says it.
	nokind := podSpec{UID: "p2", Name: "p2", Created: t0, Phase: "Running"}.object()
	delete(nokind, "kind")
	if r := translate(t, tr, pullRec(t0, nokind)); len(r.Records) != 1 {
		t.Errorf("a list item with no kind gave %+v", r)
	}

	// The namespace of the resource stands in for one the object lacks.
	nons := podSpec{UID: "p3", Name: "p3", Created: t0, Phase: "Running"}.object()
	delete(nons["metadata"].(map[string]any), "namespace")
	res := capture.Resource{Attributes: []capture.KeyValue{{Key: "k8s.namespace.name", Value: sv("team-a")}}}
	r, err := tr.Translate(res, pullRec(t0, nons))
	if err != nil {
		t.Fatal(err)
	}
	if got := payloadOf(t, only(t, r))["namespace"]; got != "team-a" {
		t.Errorf("namespace = %v, want the resource's", got)
	}

	// A DELETED for a pod never seen ends the pod only, and says what it could not
	// address.
	withNode := podSpec{UID: "p4", Name: "p4", Node: node, Created: t0, Phase: "Running"}.object()
	r = translate(t, tr, watchRec(t0.Add(time.Hour), "DELETED", withNode))
	rec := only(t, r)
	if rec.Kind != lifecycle.Delete || rec.Subject.A != podFP(t, "p4") || skips(r)[k8sobjects.SkipUnseenDelete] != 1 {
		t.Errorf("DELETED of an unseen pod gave %+v, skips %v", r.Records, r.Skipped)
	}
}
