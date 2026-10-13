package resume_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func val(v any) capture.AnyValue {
	switch x := v.(type) {
	case string:
		return capture.AnyValue{StringValue: &x}
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

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func objectRecord(observed time.Time, body map[string]any, resource string) capture.LogRecord {
	b := val(body)
	s := resource
	return capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()), Body: &b,
		Attributes: []capture.KeyValue{{Key: "k8s.resource.name", Value: capture.AnyValue{StringValue: &s}}},
	}
}

func condition(typ string, at time.Time) any {
	return map[string]any{"type": typ, "status": "True", "lastTransitionTime": ts(at)}
}

func podObject(conds ...any) map[string]any {
	return map[string]any{
		"kind": "Pod", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "p1", "name": "p1", "namespace": "default", "creationTimestamp": ts(at(0))},
		"spec":     map[string]any{"nodeName": "n1"},
		"status":   map[string]any{"phase": "Running", "conditions": append([]any{condition("PodScheduled", at(1))}, conds...)},
	}
}

func nodeObject() map[string]any {
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "n1-uid", "name": "n1", "creationTimestamp": ts(at(-60))},
		"status":   map[string]any{"conditions": []any{condition("Ready", at(-60))}},
	}
}

func newObjects(t *testing.T) *k8sobjects.Translator {
	t.Helper()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: producer, Resolver: identity.NewResolver(catalog.Default()), PullInterval: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// l′. The mirror-raise against the translator: a translator never reset, whose
// state for a pod and its edge is the instant the store holds, clamps a later
// record of the pod exactly as the mirror-raise clamps that record from a
// translator that lost its state and was resumed: the same times, bases, Throughs
// and payloads.
func TestTheMirrorRaiseIsTheTranslatorsClamp(t *testing.T) {
	ctx := context.Background()
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seq := uint64(0)
	write := func(recs []store.Record) {
		t.Helper()
		for i := range recs {
			seq++
			recs[i].Seq = seq
		}
		if err := st.Write(ctx, recs); err != nil {
			t.Fatal(err)
		}
	}
	translate := func(tr *k8sobjects.Translator, lr capture.LogRecord) k8sobjects.Result {
		t.Helper()
		r, err := tr.Translate(capture.Resource{}, lr)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// The translator that is never reset: the node, the pod's first sighting, and a
	// list at 00:30 that leaves its state for the pod and its edge at 00:30,
	// observed. What it gave is stored.
	never := newObjects(t)
	for _, lr := range []capture.LogRecord{
		objectRecord(at(27), nodeObject(), "nodes"),
		objectRecord(at(27), map[string]any{"type": "ADDED", "object": podObject()}, "pods"),
		objectRecord(at(30), podObject(), "pods"),
	} {
		write(translate(never, lr).Records)
	}

	// The request after a reset: the node and the pod listed at 00:40, unchanged, and
	// then a watch event of the pod whose change is timed at 00:20.
	later := objectRecord(at(41), map[string]any{"type": "MODIFIED", "object": podObject(condition("Ready", at(20)))}, "pods")
	fresh := newObjects(t)
	var res resume.Result
	var laterFrom int
	for i, lr := range []capture.LogRecord{objectRecord(at(40), nodeObject(), "nodes"), objectRecord(at(40), podObject(), "pods"), later} {
		r := translate(fresh, lr)
		if i == 2 {
			laterFrom = len(res.Records)
		}
		for _, m := range r.Backdated {
			m.Index += len(res.Records)
			res.Marks = append(res.Marks, m)
		}
		res.Records = append(res.Records, r.Records...)
	}
	lk, err := st.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := resume.Apply(ctx, lk, func(catalog.Layer) time.Time { return time.Time{} }, &res, fresh, k8sobjects.Comparable)
	if err != nil {
		t.Fatal(err)
	}
	_ = lk.Close()
	if stats.Raised != 3 || stats.MirrorRaised != 2 || stats.Unknown != 0 || stats.Changed != 0 {
		t.Fatalf("%+v, want the node, the pod and its edge raised, and the watch event's two records mirrored", stats)
	}

	// The translator never reset, given the watch event alone.
	want := translate(never, later).Records
	got := res.Records[laterFrom:]
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Subject != w.Subject || !g.EventTime.Equal(w.EventTime) || g.EventTimeBasis != w.EventTimeBasis ||
			!g.Through.Equal(w.Through) || string(g.Payload) != string(w.Payload) {
			t.Errorf("record %d: mirrored at %s basis %s through %s; the translator's clamp gives %s basis %s through %s",
				i, g.EventTime.Sub(t0), g.EventTimeBasis, g.Through.Sub(t0), w.EventTime.Sub(t0), w.EventTimeBasis, w.Through.Sub(t0))
		}
	}
	if !want[0].EventTime.Equal(at(30)) || want[0].EventTimeBasis != store.BasisObserved {
		t.Errorf("the case does not exercise the clamp: the translator gives %s basis %s", want[0].EventTime.Sub(t0), want[0].EventTimeBasis)
	}
	// And the resumed translator's own state now times the next record alike.
	next := objectRecord(at(42), map[string]any{"type": "MODIFIED", "object": podObject(condition("Ready", at(20)), condition("ContainersReady", at(25)))}, "pods")
	g, w := translate(fresh, next).Records, translate(never, next).Records
	for i := range w {
		if !g[i].EventTime.Equal(w[i].EventTime) || g[i].EventTimeBasis != w[i].EventTimeBasis {
			t.Errorf("next record %d: %s basis %s, the never-reset translator %s basis %s", i, g[i].EventTime.Sub(t0), g[i].EventTimeBasis, w[i].EventTime.Sub(t0), w[i].EventTimeBasis)
		}
	}
}
