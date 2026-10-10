package k8scluster_test

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests in this file compare what this package writes with what the
// Kubernetes objects translator writes for the same objects. They are the only
// place the two packages meet.

// val converts plain Go values (string, int, []any, map[string]any) to an OTLP
// value, with map keys in sorted order.
func val(v any) capture.AnyValue {
	switch x := v.(type) {
	case string:
		return sv(x)
	case int:
		return iv(int64(x))
	case []any:
		a := &capture.ArrayValue{}
		for _, e := range x {
			a.Values = append(a.Values, val(e))
		}
		return capture.AnyValue{ArrayValue: a}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		l := &capture.KeyValueList{}
		for _, k := range keys {
			l.Values = append(l.Values, capture.KeyValue{Key: k, Value: val(x[k])})
		}
		return capture.AnyValue{KVListValue: l}
	}
	panic(fmt.Sprintf("val: %T", v))
}

// objectsPull is a list item of the objects receiver.
func objectsPull(observed time.Time, resource string, o map[string]any) capture.LogRecord {
	b := val(o)
	return capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()),
		Body:                 &b,
		Attributes:           []capture.KeyValue{{Key: "k8s.resource.name", Value: sv(resource)}},
	}
}

func objectsTranslator(t testing.TB) *k8sobjects.Translator {
	t.Helper()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: "k8sobjects/test", Resolver: resolver()})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func podObject(uid, name, node, containerID string) map[string]any {
	status := map[string]any{"phase": "Running"}
	if containerID != "" {
		status["containerStatuses"] = []any{map[string]any{
			"name": "web", "containerID": containerID, "restartCount": 0,
			"state": map[string]any{"running": map[string]any{"startedAt": ts(t0.Add(-time.Hour))}},
		}}
	}
	return map[string]any{
		"kind": "Pod", "apiVersion": "v1",
		"metadata": map[string]any{"uid": uid, "name": name, "namespace": "default", "creationTimestamp": ts(t0.Add(-time.Hour))},
		"spec":     map[string]any{"nodeName": node},
		"status":   status,
	}
}

func nodeObject(uid, name string) map[string]any {
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": uid, "name": name, "creationTimestamp": ts(t0.Add(-time.Hour))},
	}
}

func objectsRecords(t testing.TB, lr capture.LogRecord) []store.Record {
	t.Helper()
	r, err := objectsTranslator(t).Translate(noResource, lr)
	if err != nil {
		t.Fatal(err)
	}
	return r.Records
}

func TestContainerIDPrefix(t *testing.T) {
	// With and without the runtime prefix, the container is the same one.
	var fps []identity.Fingerprint
	for _, id := range []string{"containerd://abc", "abc", "docker://abc", "cri-o://abc"} {
		r := translate(t, newTranslator(t), ctrEv(id, "p-1", "web", t0, time.Time{}))
		ctr := entitiesOf(r.Records, catalog.Container)
		if len(ctr) != 1 {
			t.Fatalf("id %q gave %s", id, describeRecords(r.Records))
		}
		fps = append(fps, ctr[0].Subject.A)
	}
	for i, f := range fps {
		if f != ctrFP(t, "abc") {
			t.Errorf("fingerprint %d = %v, want that of the bare id", i, f)
		}
	}

	// And it is the one the objects translator gives the same container, from
	// the "<runtime>://<id>" of the pod status.
	recs := objectsRecords(t, objectsPull(t0, "pods", podObject("p-1", "p", "", "containerd://abc")))
	ctr := entitiesOf(recs, catalog.Container)
	if len(ctr) != 1 {
		t.Fatalf("objects translator gave %s", describeRecords(recs))
	}
	if ctr[0].Subject.A != fps[0] {
		t.Errorf("objects translator's container %v, this package's %v", ctr[0].Subject.A, fps[0])
	}
}

func TestPodFingerprintMatchesK8sObjects(t *testing.T) {
	pod := entitiesOf(objectsRecords(t, objectsPull(t0, "pods", podObject("p-uid-1", "p", "", ""))), catalog.K8sPod)
	node := entitiesOf(objectsRecords(t, objectsPull(t0, "nodes", nodeObject("n-uid-1", "a"))), catalog.K8sNode)
	if len(pod) != 1 || len(node) != 1 {
		t.Fatalf("objects translator gave %d pods and %d nodes", len(pod), len(node))
	}

	tr := newTranslator(t)
	translate(t, tr, nodeEv("n-uid-1", "a", t0, time.Time{}))
	r := translate(t, tr, podEv("p-uid-1", "p", "a", t0, time.Time{}))
	mine := entitiesOf(r.Records, catalog.K8sPod)
	if len(mine) != 1 || mine[0].Subject.A != pod[0].Subject.A {
		t.Errorf("pod fingerprint %v, objects translator's %v", mine, pod[0].Subject.A)
	}
	edges := edgesOf(r.Records, catalog.ScheduledOn)
	if len(edges) != 1 || edges[0].Subject.B != node[0].Subject.A {
		t.Errorf("scheduled_on goes to %v, want the objects translator's node %v", edges, node[0].Subject.A)
	}
	if len(mine) == 1 && mine[0].Layer != pod[0].Layer {
		t.Errorf("layer %s, objects translator's %s", mine[0].Layer, pod[0].Layer)
	}
}

// fingerprintsOf returns the pods and nodes among the entities records write.
func fingerprintsOf(rs []store.Record) []string {
	seen := map[string]struct{}{}
	for _, r := range rs {
		if r.Subject.Kind != store.SubjectEntity {
			continue
		}
		if typ := r.Subject.A.Type(); typ == catalog.K8sPod || typ == catalog.K8sNode {
			seen[r.Subject.A.String()] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// For the same objects, every pod and node UID the cluster events give is one
// the objects translator gives, and the reverse. The fixtures hold the events of
// the cluster receiver for a set of nodes and pods in run1.jsonl and run2.jsonl,
// and the records of the objects receiver for the same set in objects.jsonl. The
// set was cut so that each side names each UID at least once.
func TestCrossProducerUIDs(t *testing.T) {
	var mine []store.Record
	for _, name := range []string{"run1.jsonl", "run2.jsonl"} {
		in := loadFixture(t, name)
		mine = append(mine, records(translateAll(t, newTranslator(t), in, false))...)
	}

	objs := loadFixture(t, "objects.jsonl")
	otr := objectsTranslator(t)
	var theirs []store.Record
	for _, x := range objs {
		r, err := otr.Translate(x.Res, x.Rec)
		if err != nil {
			t.Fatalf("%s: %v", x.Name, err)
		}
		theirs = append(theirs, r.Records...)
	}

	a, b := fingerprintsOf(mine), fingerprintsOf(theirs)
	if len(a) < 10 {
		t.Fatalf("the cluster events name %d pods and nodes, want at least 10", len(a))
	}
	if !slices.Equal(a, b) {
		t.Errorf("cluster events name %d pods and nodes, the objects records %d; only in one of them: %v", len(a), len(b), symmetric(a, b))
	}

	// And by UID: the nodes and pods the fixtures name.
	want := map[catalog.EntityType][]string{
		catalog.K8sNode: {"36ee1190", "4edef4ef", "70f60f5f", "f41e8e62"},
		catalog.K8sPod:  {"16414d46", "1f037c43", "4a9d1695", "9ab1d6d5", "a19305d2", "c5ad95aa"},
	}
	got := map[catalog.EntityType][]string{}
	for _, x := range loadAll(t) {
		var typ catalog.EntityType
		switch typeOf(x.Rec) {
		case "k8s.node":
			typ = catalog.K8sNode
		case "k8s.pod":
			typ = catalog.K8sPod
		default:
			continue
		}
		uid := uidOf(x.Rec)[:8]
		if !slices.Contains(got[typ], uid) {
			got[typ] = append(got[typ], uid)
		}
	}
	for typ, uids := range got {
		slices.Sort(uids)
		if !slices.Equal(uids, want[typ]) {
			t.Errorf("%s UIDs in the cluster fixtures = %v, want %v", typ, uids, want[typ])
		}
	}
}

func loadAll(t testing.TB) []input {
	return append(loadFixture(t, "run1.jsonl"), loadFixture(t, "run2.jsonl")...)
}

func symmetric(a, b []string) []string {
	var out []string
	for _, s := range a {
		if !slices.Contains(b, s) {
			out = append(out, "cluster only: "+s)
		}
	}
	for _, s := range b {
		if !slices.Contains(a, s) {
			out = append(out, "objects only: "+s)
		}
	}
	return out
}

// The two packages bound the clock the same way.
func TestMaxClockAheadMatchesK8sObjects(t *testing.T) {
	if k8scluster.MaxClockAhead != k8sobjects.MaxClockAhead {
		t.Errorf("MaxClockAhead = %s, objects translator's %s", k8scluster.MaxClockAhead, k8sobjects.MaxClockAhead)
	}
}
