package k8scluster_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The keys of a description that are kept, per type. They are written out here
// and not taken from the package, so that a change to either is noticed.
var wantAllowed = map[string][]string{
	"k8s.pod": {
		"k8s.pod.name", "k8s.namespace.name", "k8s.node.name", "k8s.pod.phase", "k8s.pod.status_reason",
		"k8s.workload.kind", "k8s.workload.name", "k8s.deployment.name", "k8s.replicaset.name",
		"k8s.statefulset.name", "pod.creation_timestamp",
	},
	"k8s.node": {
		"k8s.node.name", "node.creation_timestamp", "k8s.node.condition_ready",
		"k8s.node.condition_memory_pressure", "k8s.node.condition_disk_pressure",
		"k8s.node.condition_pid_pressure", "k8s.node.condition_network_unavailable",
	},
	"container": {
		"k8s.container.name", "k8s.pod.name", "k8s.namespace.name", "k8s.node.name",
		"container.image.name", "container.image.tag", "container.status",
		"container.status.reason", "container.creation_timestamp",
	},
}

func payloadKeys(t testing.TB, payload []byte) []string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("payload %s: %v", payload, err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func TestDescriptionAllowList(t *testing.T) {
	in := loadFixture(t, "run1.jsonl")

	t.Run("the pod of the capture", func(t *testing.T) {
		tr := newTranslator(t)
		i, first := find(t, in, "of pod 262tj", func(x input) bool { return isEvent(x, "entity.state", "k8s.pod", "9ab1d6d5") })
		r, err := tr.Translate(first.Res, first.Rec, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		want := `{"k8s.deployment.name":"pre","k8s.namespace.name":"default","k8s.node.name":"kwok-node-4",` +
			`"k8s.pod.name":"pre-5984778487-262tj","k8s.pod.phase":"Running","k8s.replicaset.name":"pre-5984778487",` +
			`"k8s.workload.kind":"Deployment","k8s.workload.name":"pre","pod.creation_timestamp":"2026-10-09T16:32:08Z"}`
		rec := entitiesOf(r.Records, catalog.K8sPod)
		if len(rec) != 1 {
			t.Fatalf("gave %s, want the pod", describeRecords(r.Records))
		}
		if string(rec[0].Payload) != want {
			t.Errorf("payload = %s, want %s", rec[0].Payload, want)
		}
		wantDropped := []string{
			"k8s.deployment.uid", "k8s.pod.label.app", "k8s.pod.label.pod-template-hash",
			"k8s.replicaset.uid", "k8s.service.kubernetes",
		}
		if got := noteDetails(r, k8scluster.NoteDescriptionKeyDropped); !slices.Equal(got, wantDropped) {
			t.Errorf("dropped-key notes = %v, want %v", got, wantDropped)
		}
		for _, n := range r.Notes {
			if n.Count != 1 {
				t.Errorf("note %+v counted more than once", n)
			}
		}

		// The next state of the same pod drops the same keys, and says nothing.
		_, next := find(t, in[i+1:], "of the next state of 262tj", func(x input) bool { return isEvent(x, "entity.state", "k8s.pod", "9ab1d6d5") })
		r, err = tr.Translate(next.Res, next.Rec, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Notes) != 0 || len(r.Records) != 1 {
			t.Errorf("next state: notes %+v, %d records, want none and the pod", r.Notes, len(r.Records))
		}
	})

	t.Run("the node of the capture", func(t *testing.T) {
		tr := newTranslator(t)
		_, x := find(t, in, "of kwok-node-0", func(x input) bool { return isEvent(x, "entity.state", "k8s.node", "36ee1190") })
		r, err := tr.Translate(x.Res, x.Rec, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		wantKeys := []string{
			"k8s.node.condition_disk_pressure", "k8s.node.condition_memory_pressure",
			"k8s.node.condition_network_unavailable", "k8s.node.condition_pid_pressure",
			"k8s.node.condition_ready", "k8s.node.name", "node.creation_timestamp",
		}
		if got := payloadKeys(t, only(t, r).Payload); !slices.Equal(got, wantKeys) {
			t.Errorf("node payload keys = %v, want %v", got, wantKeys)
		}
		labels := 0
		v, _ := attr(x.Rec, "entity.description")
		for _, kv := range v.KVListValue.Values {
			if strings.Contains(kv.Key, ".label.") {
				labels++
			}
		}
		got := noteDetails(r, k8scluster.NoteDescriptionKeyDropped)
		if labels == 0 || len(got) != labels {
			t.Errorf("%d label keys in the node, %d dropped keys noted: %v", labels, len(got), got)
		}
	})

	for typ, keys := range wantAllowed {
		t.Run("every kept key of "+typ, func(t *testing.T) {
			desc := map[string]string{}
			for _, k := range keys {
				desc[k] = "v:" + k
				if strings.HasSuffix(k, "creation_timestamp") {
					desc[k] = ts(t0.Add(-time.Hour))
				}
			}
			// What is not kept: a key nobody listed, two labels, and the pod uid,
			// which a container reads for its edge and the others have no use for.
			desc["extra.key"] = "x"
			desc[typ+".label.team"] = "private"
			desc["k8s.pod.label.zz"] = "private"
			desc["k8s.pod.uid"] = "p-1"
			e := map[string]ent{
				"k8s.pod":   {Type: "k8s.pod", IDKey: "k8s.pod.uid", ID: "p-1"},
				"k8s.node":  {Type: "k8s.node", IDKey: "k8s.node.uid", ID: "n-1"},
				"container": {Type: "container", IDKey: "container.id", ID: "containerd://c1"},
			}[typ]
			e.Time, e.Desc = t0, desc
			r := translate(t, newTranslator(t), e)

			recs := entitiesOf(r.Records, catalog.EntityType(typ))
			if len(recs) != 1 {
				t.Fatalf("gave %s", describeRecords(r.Records))
			}
			want := slices.Clone(keys)
			slices.Sort(want)
			if got := payloadKeys(t, recs[0].Payload); !slices.Equal(got, want) {
				t.Errorf("payload keys = %v, want %v", got, want)
			}
			wantNotes := []string{"extra.key", typ + ".label.team", "k8s.pod.label.zz"}
			if typ != "container" {
				wantNotes = append(wantNotes, "k8s.pod.uid")
			}
			slices.Sort(wantNotes)
			wantNotes = slices.Compact(wantNotes)
			if got := noteDetails(r, k8scluster.NoteDescriptionKeyDropped); !slices.Equal(got, wantNotes) {
				t.Errorf("dropped-key notes = %v, want %v", got, wantNotes)
			}
		})
	}

	t.Run("each name is noted once for the translator", func(t *testing.T) {
		tr := newTranslator(t)
		a := translate(t, tr, nodeEv("n-1", "a", t0, time.Time{}).with("shared.extra", "1"))
		b := translate(t, tr, podEv("p-1", "p", "", t0, time.Time{}).with("shared.extra", "2"))
		if noteCount(a, k8scluster.NoteDescriptionKeyDropped) != 1 || noteCount(b, k8scluster.NoteDescriptionKeyDropped) != 0 {
			t.Errorf("notes = %v then %v, want the name once, at the first drop", a.Notes, b.Notes)
		}
	})

	t.Run("a kept key with a value that is not a string is dropped", func(t *testing.T) {
		tr := newTranslator(t)
		lr := nodeEv("n-1", "a", t0, time.Time{}).rec()
		for i, kv := range lr.Attributes {
			if kv.Key == "entity.description" {
				kv.Value.KVListValue.Values = append(kv.Value.KVListValue.Values,
					capture.KeyValue{Key: "k8s.node.condition_disk_pressure", Value: bv(false)})
				lr.Attributes[i] = kv
			}
		}
		r, err := tr.Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if got := payloadKeys(t, only(t, r).Payload); slices.Contains(got, "k8s.node.condition_disk_pressure") {
			t.Errorf("payload keys = %v", got)
		}
		if got := noteDetails(r, k8scluster.NoteDescriptionKeyDropped); !slices.Equal(got, []string{"k8s.node.condition_disk_pressure"}) {
			t.Errorf("notes = %v", got)
		}
	})

	t.Run("the names remembered are bounded", func(t *testing.T) {
		tr := newTranslator(t)
		e := podEv("p-1", "p", "", t0, time.Time{})
		noted, full := 0, 0
		for i := 0; i < 5000; i += 100 {
			for j := i; j < i+100; j++ {
				e = e.with(fmt.Sprintf("k8s.pod.label.l%04d", j), "x")
			}
			r := translate(t, tr, e.at(t0.Add(time.Duration(i)*time.Second)))
			noted += noteCount(r, k8scluster.NoteDescriptionKeyDropped)
			full += noteCount(r, k8scluster.NoteDroppedKeysFull)
		}
		if noted != 4096 || full != 1 {
			t.Errorf("%d names noted and %d full notes, want 4096 and 1", noted, full)
		}
	})
}
