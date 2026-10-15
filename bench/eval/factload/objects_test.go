package factload

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// A snapshot of an invented cluster: two nodes, three pods, and a pod that names
// a node the snapshot never lists. The pods are listed before their nodes, as a
// list of objects need not be ordered.
func snapshot(t testing.TB, observed time.Time) []Object {
	t.Helper()
	created := "2026-03-04T08:00:00Z"
	return []Object{
		mustObject(t, "pods", podJSON("pod-uid-1", "shop-front-1", "shop", "node-a", created, "aaa1", "aaa2"), observed),
		mustObject(t, "pods", podJSON("pod-uid-2", "shop-cart-1", "shop", "node-a", created, "bbb1"), observed),
		mustObject(t, "pods", podJSON("pod-uid-3", "shop-db-1", "shop", "node-b", created), observed),
		mustObject(t, "pods", podJSON("pod-uid-4", "shop-lost-1", "shop", "node-ghost", created), observed),
		mustObject(t, "nodes", nodeJSON("node-uid-a", "node-a", "2026-03-01T00:00:00Z"), observed),
		mustObject(t, "nodes", nodeJSON("node-uid-b", "node-b", "2026-03-01T00:00:00Z"), observed),
		// An object of a kind the translator does not make facts of is set aside.
		mustObject(t, "configmaps", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"uid":"cm-uid","name":"cfg","namespace":"shop","creationTimestamp":"2026-03-04T08:00:00Z"}}`, observed),
	}
}

// reference runs the same objects through the translator and the resume step
// with nothing but the product's own calls, and returns what it would write.
func reference(t testing.TB, objs []Object) ([]store.Record, resume.Stats) {
	t.Helper()
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: ProducerObjects, Resolver: resolver(), EntityTTL: ObjectTTL})
	if err != nil {
		t.Fatal(err)
	}
	var out []store.Record
	var total resume.Stats
	var seq uint64
	for _, o := range objs {
		res, err := tr.Translate(o.Resource, o.Record)
		if err != nil {
			t.Fatal(err)
		}
		rr := resume.Result{Records: res.Records, Marks: res.Backdated}
		lk, err := st.NewLookup()
		if err != nil {
			t.Fatal(err)
		}
		s, err := resume.Apply(ctx, lk, nil, &rr, tr, k8sobjects.Comparable)
		_ = lk.Close()
		if err != nil {
			t.Fatal(err)
		}
		total.Add(s)
		for i := range rr.Records {
			seq++
			rr.Records[i].Seq = seq
		}
		if len(rr.Records) > 0 {
			if err := st.Write(ctx, rr.Records); err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, rr.Records...)
	}
	return out, total
}

// Every record of the snapshot is the translator's, as the resume step leaves
// it: the loader adds none, changes none and drops none.
func TestEveryL2RecordIsTheTranslatorsAfterTheResumeStep(t *testing.T) {
	observed := time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC)
	objs := snapshot(t, observed)
	st, stats := load(t, Scenario{Objects: objs, Source: "set@rev"})

	want, wantStats := reference(t, objs)
	got := st.records()
	if len(got) != len(want) || len(got) == 0 {
		t.Fatalf("wrote %d records, the translator gives %d", len(got), len(want))
	}
	for i := range want {
		g := got[i]
		g.Seq = 0
		w := want[i]
		w.Seq = 0
		if !reflect.DeepEqual(g, w) {
			t.Fatalf("record %d is %+v, the translator's is %+v", i, g, w)
		}
	}
	if stats.ObjectRecords != len(want) {
		t.Errorf("ObjectRecords = %d, want %d", stats.ObjectRecords, len(want))
	}
	// The resume step ran on every record the translator backdated: its counters
	// are the reference's, and not zero.
	if stats.Resume.New == 0 || stats.Resume != wantStats {
		t.Errorf("resume stats %+v, want %+v (non-zero)", stats.Resume, wantStats)
	}
	if stats.Skipped[k8sobjects.SkipUnsupportedKind] != 1 {
		t.Errorf("skipped %v, want one unsupported kind", stats.Skipped)
	}
}

// A first sighting is backdated to the object's creation and asserted through the
// time it was listed, with the evaluation's TTL: the object exists from its
// creation to the snapshot plus the TTL.
func TestASnapshotAssertsFromCreationThroughTheListingPlusTheTTL(t *testing.T) {
	observed := time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC)
	st, _ := load(t, Scenario{Objects: snapshot(t, observed)})
	pod := store.EntitySubject(podFP(t, "pod-uid-1"))
	var rec *store.Record
	for _, r := range st.records() {
		if r.Subject == pod {
			rec = &r
			break
		}
	}
	if rec == nil {
		t.Fatal("no record of the pod")
	}
	if want := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC); !rec.EventTime.Equal(want) {
		t.Errorf("event time %s, want the creation time %s", rec.EventTime, want)
	}
	if !rec.Through.Equal(observed) || rec.TTL != 5*time.Minute {
		t.Errorf("through %s, TTL %s; want %s, 5m", rec.Through, rec.TTL, observed)
	}
	if rec.EventTimeBasis != store.BasisObjectField || rec.Producer != ProducerObjects || rec.Kind != lifecycle.Observe {
		t.Errorf("basis %s, producer %s, kind %s", rec.EventTimeBasis, rec.Producer, rec.Kind)
	}

	// Alive from the creation to the listing plus the TTL, and not before or after.
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{time.Date(2026, 3, 4, 7, 59, 59, 0, time.UTC), false},
		{time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC), true},
		{observed.Add(ObjectTTL - time.Nanosecond), true},
		{observed.Add(ObjectTTL), false},
	} {
		alive, err := st.Alive(ctx, podFP(t, "pod-uid-1"), c.at, store.Scope{Layer: catalog.L2, AsOf: store.Latest})
		if err != nil {
			t.Fatal(err)
		}
		if alive != c.want {
			t.Errorf("pod alive at %s = %t, want %t", c.at.Format(time.RFC3339Nano), alive, c.want)
		}
	}
}

// A pod that names a node the snapshot never lists stays unplaced: it has no
// scheduled_on edge, no node is made from its node name, and it is reported. The
// pods that name a listed node are placed even though they were listed first.
func TestAPodWhoseNodeIsMissingStaysUnplaced(t *testing.T) {
	st, stats := load(t, Scenario{Objects: snapshot(t, time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC))})

	lost := podFP(t, "pod-uid-4")
	if !slices.Equal(stats.Unplaced, []identity.Fingerprint{lost}) {
		t.Errorf("unplaced = %v, want the one pod", stats.Unplaced)
	}
	nodes := map[string]bool{}
	var scheduled []store.Subject
	for _, r := range st.records() {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sNode {
			nodes[r.Subject.A.String()] = true
		}
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn {
			if r.Subject.A == lost {
				t.Errorf("the unplaced pod has a scheduled_on edge: %+v", r.Subject)
			}
			scheduled = append(scheduled, r.Subject)
		}
	}
	if len(nodes) != 2 || !nodes[nodeFP(t, "node-uid-a").String()] || !nodes[nodeFP(t, "node-uid-b").String()] {
		t.Errorf("node entities %v, want the two listed nodes only", nodes)
	}
	placed := map[string]string{}
	for _, s := range scheduled {
		placed[s.A.String()] = s.B.String()
	}
	for uid, node := range map[string]string{"pod-uid-1": "node-uid-a", "pod-uid-2": "node-uid-a", "pod-uid-3": "node-uid-b"} {
		if placed[podFP(t, uid).String()] != nodeFP(t, node).String() {
			t.Errorf("pod %s is not scheduled on %s", uid, node)
		}
	}
}

// The loader refuses a store that holds anything: a scenario is one empty store.
func TestLoadRefusesANonEmptyStore(t *testing.T) {
	st, _ := load(t, Scenario{Objects: snapshot(t, time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC))})
	if _, err := Load(ctx, st, Scenario{}); err == nil {
		t.Error("Load into a store that holds records succeeded")
	}
}

func TestBodyFromJSON(t *testing.T) {
	v, err := BodyFromJSON([]byte(`{"b":1,"a":[true,"x",2.5,null],"c":{"n":9007199254740993}}`))
	if err != nil || v.KVListValue == nil {
		t.Fatalf("%v %+v", err, v)
	}
	keys := []string{}
	for _, kv := range v.KVListValue.Values {
		keys = append(keys, kv.Key)
	}
	if !slices.Equal(keys, []string{"a", "b", "c"}) {
		t.Errorf("keys %v, want sorted", keys)
	}
	c := v.KVListValue.Values[2].Value.KVListValue.Values[0].Value
	if c.IntValue == nil || int64(*c.IntValue) != 9007199254740993 {
		t.Errorf("a large integer is not kept exact: %+v", c)
	}
	arr := v.KVListValue.Values[0].Value.ArrayValue.Values
	if arr[0].BoolValue == nil || arr[1].StringValue == nil || arr[2].DoubleValue == nil || arr[3] != (capture.AnyValue{}) {
		t.Errorf("array %+v", arr)
	}
	for _, bad := range []string{``, `[]`, `"x"`, `{"a":1} {"b":2}`, `{`} {
		if _, err := BodyFromJSON([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
