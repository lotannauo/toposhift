package k8scluster_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestConfig(t *testing.T) {
	good := k8scluster.Config{Producer: testProducer, Resolver: resolver()}
	for _, c := range []struct {
		name string
		mod  func(*k8scluster.Config)
		ok   bool
	}{
		{"valid", func(*k8scluster.Config) {}, true},
		{"no producer", func(c *k8scluster.Config) { c.Producer = "" }, false},
		{"no resolver", func(c *k8scluster.Config) { c.Resolver = nil }, false},
		{"negative collection interval", func(c *k8scluster.Config) { c.CollectionInterval = -time.Second }, false},
		{"collection interval over a year", func(c *k8scluster.Config) { c.CollectionInterval = 367 * 24 * time.Hour }, false},
		{"collection interval of a year", func(c *k8scluster.Config) { c.CollectionInterval = 366 * 24 * time.Hour }, true},
		{"collection interval", func(c *k8scluster.Config) { c.CollectionInterval = 5 * time.Minute }, true},
		{"distrust", func(c *k8scluster.Config) { c.DistrustProducerClock = true }, true},
	} {
		cfg := good
		c.mod(&cfg)
		tr, err := k8scluster.New(cfg)
		if (err == nil) != c.ok {
			t.Errorf("%s: error = %v, want ok = %v", c.name, err, c.ok)
		}
		if err == nil && tr == nil {
			t.Errorf("%s: nil translator without an error", c.name)
		}
	}

	// The defaults: no interval for an event that has none, so no TTL, and the
	// producer's clock is trusted.
	r := translate(t, newTranslator(t), nodeEv("n-1", "a", t0, time.Time{}).noInterval())
	rec := only(t, r)
	if rec.TTL != 0 {
		t.Errorf("default TTL of an event with no interval = %s, want 0", rec.TTL)
	}
	if rec.EventTimeBasis != store.BasisProducerEvent || !rec.EventTime.Equal(t0) {
		t.Errorf("default clock: %s at %s, want the producer's time", rec.EventTimeBasis, rec.EventTime)
	}
	if rec.Producer != testProducer {
		t.Errorf("producer = %q, want %q", rec.Producer, testProducer)
	}
	r = translate(t, newTranslator(t, collection(5*time.Minute)), nodeEv("n-1", "a", t0, time.Time{}).noInterval())
	if got := only(t, r).TTL; got != 15*time.Minute {
		t.Errorf("TTL of an event with no interval at a collection interval of 5m = %s, want 15m", got)
	}
}

func TestEventNameDispatch(t *testing.T) {
	tr := newTranslator(t)
	state := only(t, translate(t, tr, nodeEv("n-1", "a", t0, time.Time{})))
	if state.Kind != lifecycle.Observe {
		t.Errorf("entity.state gave %s, want Observe", state.Kind)
	}
	del := only(t, translate(t, tr, deleteEv("k8s.node", "n-1", t0.Add(time.Minute))))
	if del.Kind != lifecycle.Delete {
		t.Errorf("entity.delete gave %s, want Delete", del.Kind)
	}

	// Records that carry every attribute of an entity event but not its name are
	// not entity events. Only the name says so.
	for _, name := range []string{"", "entity.other", "entity.state ", "Entity.State", "k8s.event"} {
		lr := nodeEv("n-2", "b", t0, time.Time{}).rec()
		lr.EventName = name
		r, err := tr.Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipNotEntityEvent) != 1 {
			t.Errorf("event name %q: %d records and skips %v, want none and one %q", name, len(r.Records), r.Skipped, k8scluster.SkipNotEntityEvent)
		}
	}

	// A record of the Kubernetes objects receiver, in a watch and in a pull form.
	body := capture.AnyValue{KVListValue: &capture.KeyValueList{Values: []capture.KeyValue{
		{Key: "type", Value: sv("ADDED")},
		{Key: "object", Value: capture.AnyValue{KVListValue: &capture.KeyValueList{Values: []capture.KeyValue{
			{Key: "kind", Value: sv("Pod")},
		}}}},
	}}}
	for _, lr := range []capture.LogRecord{
		{ObservedTimeUnixNano: capture.Uint64(t0.UnixNano()), Body: &body, Attributes: []capture.KeyValue{{Key: "k8s.resource.name", Value: sv("pods")}}},
		{ObservedTimeUnixNano: capture.Uint64(t0.UnixNano()), Attributes: []capture.KeyValue{{Key: "k8s.resource.name", Value: sv("nodes")}}},
		{},
	} {
		r, err := tr.Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipNotEntityEvent) != 1 {
			t.Errorf("objects record: %d records and skips %v, want none and one %q", len(r.Records), r.Skipped, k8scluster.SkipNotEntityEvent)
		}
	}

	// A refused record changed nothing: the first state of n-2 is still its first.
	created := t0.Add(-time.Hour)
	first := only(t, translate(t, tr, nodeEv("n-2", "b", t0, created)))
	wantTimes(t, "first state of n-2", first, created, store.BasisObjectField, t0)
}

func TestLegacyEntityEventsAreRefused(t *testing.T) {
	tr := newTranslator(t)
	for _, name := range []string{"", "entity.state"} {
		lr := capture.LogRecord{
			EventName:            name,
			TimeUnixNano:         capture.Uint64(t0.UnixNano()),
			ObservedTimeUnixNano: capture.Uint64(t0.UnixNano()),
			Attributes: []capture.KeyValue{
				{Key: "otel.entity.event.type", Value: sv("entity_state")},
				{Key: "otel.entity.type", Value: sv("k8s.node")},
				{Key: "otel.entity.id", Value: kvlist(map[string]string{"k8s.node.uid": "n-1"})},
				{Key: "otel.entity.attributes", Value: kvlist(map[string]string{"k8s.node.name": "a"})},
				{Key: "otel.entity.interval", Value: iv(60000)},
			},
		}
		r, err := tr.Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipLegacyEntityEvent) != 1 {
			t.Errorf("legacy event named %q: %d records and skips %v, want none and one %q", name, len(r.Records), r.Skipped, k8scluster.SkipLegacyEntityEvent)
		}
	}
}

func TestMissingIDAndRecordsAfterDelete(t *testing.T) {
	tr := newTranslator(t)
	for _, e := range []ent{
		{Type: "k8s.node", IDKey: "something.else", ID: "x", Time: t0, Desc: map[string]string{"k8s.node.name": "a"}},
		nodeEv("", "a", t0, time.Time{}),
		nodeEv("  ", "a", t0, time.Time{}),
		podEv("", "p", "", t0, time.Time{}),
		{Event: "entity.delete", Type: "k8s.pod", IDKey: "k8s.pod.uid", ID: "", Time: t0},
		{Type: "container", IDKey: "other", ID: "abc", Time: t0},
	} {
		r := translate(t, tr, e)
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipMissingID) != 1 {
			t.Errorf("%s %q (key %q): %d records and skips %v, want none and one %q", e.Type, e.ID, e.IDKey, len(r.Records), r.Skipped, k8scluster.SkipMissingID)
		}
	}

	// A UID is never reused: a state after its delete is not a new entity.
	for _, c := range []struct {
		typ   string
		state ent
	}{
		{"k8s.node", nodeEv("n-1", "a", t0.Add(2*time.Minute), time.Time{})},
		{"k8s.pod", podEv("p-1", "p", "", t0.Add(2*time.Minute), time.Time{})},
		{"container", ctrEv("abc", "p-1", "c", t0.Add(2*time.Minute), time.Time{})},
	} {
		translate(t, tr, c.state.at(t0))
		translate(t, tr, deleteEv(c.typ, c.state.ID, t0.Add(time.Minute)))
		r := translate(t, tr, c.state)
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipAfterDelete) != 1 {
			t.Errorf("%s state after its delete: %d records and skips %v, want none and one %q", c.typ, len(r.Records), r.Skipped, k8scluster.SkipAfterDelete)
		}
	}
}

func TestTypesNotInCatalogAreSkipped(t *testing.T) {
	tr := newTranslator(t)
	for _, c := range []struct {
		typ, key string
	}{
		{"k8s.deployment", "k8s.deployment.uid"},
		{"k8s.replicaset", "k8s.replicaset.uid"},
		{"k8s.statefulset", "k8s.statefulset.uid"},
		{"k8s.service", "k8s.service.uid"},
		{"k8s.namespace", "k8s.namespace.uid"},
	} {
		for _, name := range []string{"entity.state", "entity.delete"} {
			e := ent{Event: name, Type: c.typ, IDKey: c.key, ID: "u-1", Time: t0, Desc: map[string]string{"k8s.namespace.name": "default"}}
			// No time is needed for a record that gives nothing.
			e.Time = time.Time{}
			r := translate(t, tr, e)
			if len(r.Records) != 0 || skipCount(r, k8scluster.SkipTypeNotInCatalog) != 1 {
				t.Errorf("%s %s: %d records and skips %v, want none and one %q", c.typ, name, len(r.Records), r.Skipped, k8scluster.SkipTypeNotInCatalog)
			}
		}
	}
	for _, typ := range []string{"k8s.cronjob", "host", "", "K8S.NODE"} {
		r := translate(t, tr, ent{Type: typ, IDKey: "k8s.node.uid", ID: "u-1", Time: t0})
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipUnknownEntityType) != 1 {
			t.Errorf("type %q: %d records and skips %v, want none and one %q", typ, len(r.Records), r.Skipped, k8scluster.SkipUnknownEntityType)
		}
	}
}

func TestNodeStateAndDelete(t *testing.T) {
	tr := newTranslator(t)
	created := t0.Add(-2 * time.Hour)

	first := only(t, translate(t, tr, nodeEv("n-1", "node-a", t0, created)))
	if first.Subject.Kind != store.SubjectEntity || first.Subject.A != nodeFP(t, "n-1") {
		t.Errorf("first state subject = %v, want the node keyed by its UID", first.Subject)
	}
	if first.Kind != lifecycle.Observe || first.Producer != testProducer || first.Layer != catalog.L2 {
		t.Errorf("first state: kind %s producer %q layer %s", first.Kind, first.Producer, first.Layer)
	}
	// The first record of a UID is at its creation, through the instant.
	wantTimes(t, "first state", first, created, store.BasisObjectField, t0)
	if first.TTL != ttl60 {
		t.Errorf("first state TTL = %s, want %s", first.TTL, ttl60)
	}
	wantPayload := `{"k8s.node.condition_ready":"true","k8s.node.name":"node-a","node.creation_timestamp":"` + ts(created) + `"}`
	if string(first.Payload) != wantPayload {
		t.Errorf("payload = %s, want %s", first.Payload, wantPayload)
	}

	// A later state is at the instant, with no Through.
	later := only(t, translate(t, tr, nodeEv("n-1", "node-a", t0.Add(time.Minute), created)))
	wantTimes(t, "later state", later, t0.Add(time.Minute), store.BasisProducerEvent, time.Time{})
	if later.TTL != ttl60 {
		t.Errorf("later state TTL = %s, want %s", later.TTL, ttl60)
	}

	// The delete carries no TTL, no Through and no payload.
	del := only(t, translate(t, tr, deleteEv("k8s.node", "n-1", t0.Add(2*time.Minute))))
	if del.Kind != lifecycle.Delete || del.Subject.A != nodeFP(t, "n-1") {
		t.Errorf("delete: kind %s subject %v", del.Kind, del.Subject)
	}
	wantTimes(t, "delete", del, t0.Add(2*time.Minute), store.BasisProducerEvent, time.Time{})
	if del.TTL != 0 || len(del.Payload) != 0 {
		t.Errorf("delete carries TTL %s and payload %q, want neither", del.TTL, del.Payload)
	}
	for _, r := range []store.Record{first, later, del} {
		r.Seq = 1
		if err := r.Validate(); err != nil {
			t.Errorf("%s record does not validate: %v", r.Kind, err)
		}
	}
}

func TestUnseenPodDelete(t *testing.T) {
	tr := newTranslator(t)
	r := translate(t, tr, deleteEv("k8s.pod", "p-unseen", t0))
	del := only(t, r)
	if del.Kind != lifecycle.Delete || del.Subject.A != podFP(t, "p-unseen") || del.TTL != 0 || !del.Through.IsZero() {
		t.Errorf("delete of an unseen pod = %+v", del)
	}
	if skipCount(r, k8scluster.SkipUnseenDelete) != 1 {
		t.Errorf("skips = %v, want one %q", r.Skipped, k8scluster.SkipUnseenDelete)
	}

	// The delete of a pod this translator has seen is not counted so.
	translate(t, tr, podEv("p-seen", "p", "", t0, time.Time{}))
	r = translate(t, tr, deleteEv("k8s.pod", "p-seen", t0.Add(time.Minute)))
	if hasSkip(r, k8scluster.SkipUnseenDelete) {
		t.Errorf("delete of a seen pod counted as unseen: %v", r.Skipped)
	}
	if got := only(t, r); got.Kind != lifecycle.Delete {
		t.Errorf("delete of a seen pod = %s", got.Kind)
	}
}

// A refused record must leave no trace: the failing record below would, if it
// changed anything, have changed the report interval, the dropped-key names and
// the first-record state of its UID.
func TestUntrustedMode(t *testing.T) {
	tr := newTranslator(t, distrust)
	rcv := t0.Add(time.Second)
	created := t0.Add(-time.Hour)

	// The time the producer wrote is not used for the instant.
	r := translateAt(t, tr, nodeEv("n-1", "a", t0.Add(-24*time.Hour), created), rcv)
	rec := only(t, r)
	wantTimes(t, "first state", rec, created, store.BasisObjectField, rcv)
	r = translateAt(t, tr, nodeEv("n-1", "a", t0.Add(24*time.Hour), created), rcv.Add(time.Minute))
	wantTimes(t, "later state", only(t, r), rcv.Add(time.Minute), store.BasisReceipt, time.Time{})
	r = translateAt(t, tr, deleteEv("k8s.node", "n-1", t0), rcv.Add(2*time.Minute))
	wantTimes(t, "delete", only(t, r), rcv.Add(2*time.Minute), store.BasisReceipt, time.Time{})
	if skips(r)[k8scluster.SkipImplausibleTime] != 0 {
		t.Errorf("untrusted mode counted %v", r.Skipped)
	}

	// Without a receipt time there is no instant.
	for _, e := range []ent{
		nodeEv("n-9", "z", t0, created),
		deleteEv("k8s.node", "n-1", t0),
	} {
		if _, err := tr.Translate(noResource, e.rec(), time.Time{}); !errors.Is(err, k8scluster.ErrNoTime) {
			t.Errorf("untrusted record with no receipt time: error = %v, want ErrNoTime", err)
		}
	}

	// A refused record leaves the translator unchanged. This one would switch the
	// interval to 120 seconds, note its dropped label, and make n-2 known.
	tr = newTranslator(t, distrust)
	a := podEv("p-1", "p", "", t0, created).interval(60)
	translateAt(t, tr, a, rcv)
	failing := podEv("p-2", "q", "", t0, created).interval(120).with("k8s.pod.label.tier", "web")
	if _, err := tr.Translate(noResource, failing.rec(), time.Time{}); !errors.Is(err, k8scluster.ErrNoTime) {
		t.Fatalf("error = %v, want ErrNoTime", err)
	}
	next := translateAt(t, tr, podEv("p-2", "q", "", t0, created).interval(60).with("k8s.pod.label.tier", "web"), rcv.Add(time.Minute))
	if n := noteCount(next, k8scluster.NoteReportIntervalChanged); n != 0 {
		t.Errorf("the record after a refused one noted %d interval changes, want 0", n)
	}
	if got := noteDetails(next, k8scluster.NoteDescriptionKeyDropped); len(got) != 1 || got[0] != "k8s.pod.label.tier" {
		t.Errorf("dropped-key notes = %v, want the label once, as if the refused record had never come", got)
	}
	podRec := entitiesOf(next.Records, catalog.K8sPod)
	if len(podRec) != 1 {
		t.Fatalf("records = %s", describeRecords(next.Records))
	}
	// It is still the first record of p-2: at its creation, through the instant.
	wantTimes(t, "p-2 after a refused record", podRec[0], created, store.BasisObjectField, rcv.Add(time.Minute))
	if podRec[0].TTL != ttl60 {
		t.Errorf("TTL = %s, want %s", podRec[0].TTL, ttl60)
	}
}
