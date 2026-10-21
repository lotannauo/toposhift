package pgengine

import (
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func testFP(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// TestACopiedRowHasAValueForEveryColumnOfTheLog: the COPY names the columns of
// pgschema.LogColumns, and a column that was added there and not here would be copied
// as nothing.
func TestACopiedRowHasAValueForEveryColumnOfTheLog(t *testing.T) {
	pod, node := testFP(catalog.K8sPod, 1), testFP(catalog.K8sNode, 2)
	at := time.Unix(100, 0).UTC()
	edge := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s", EventTime: at, Seq: 7,
		Kind: lifecycle.Observe, TTL: time.Minute, Through: at.Add(time.Hour), Payload: []byte("p"),
	}
	ent := engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(pod), Producer: "k8s", EventTime: at, Seq: 8, Kind: lifecycle.Delete}
	names := pgschema.Names(pgschema.LogColumns)
	if !slices.Equal(names, []string{"seq", "event_ns", "layer", "subject_kind", "src", "dst", "rel", "producer", "kind", "ttl_ns", "through_ns", "payload"}) {
		t.Fatalf("the log has columns %v: the row of a record must be changed with them", names)
	}

	l := &Loader{}
	row := l.logRow(edge)
	if len(row) != len(names) {
		t.Fatalf("%d values for %d columns", len(row), len(names))
	}
	want := []any{
		seqToDB(7), pgmodel.ToNanos(at), int16(catalog.L2), byte('g'), pgmodel.EncodeFingerprint(pod), pgmodel.EncodeFingerprint(node),
		string(catalog.ScheduledOn), "k8s", byte(lifecycle.Observe), int64(time.Minute), pgmodel.ToNanos(at.Add(time.Hour)), []byte("p"),
	}
	for i := range want {
		if !equalValue(row[i], want[i]) {
			t.Errorf("%s: %#v, want %#v", names[i], row[i], want[i])
		}
	}

	// An entity has no target and no relation, a delete has no through time, and a
	// payload that is nil is empty: the column is not null.
	row = l.logRow(ent)
	if row[3] != byte('e') || row[5] != nil || row[6] != nil || row[10] != nil || row[8] != byte(lifecycle.Delete) {
		t.Errorf("an entity delete: %#v", row)
	}
	if p, ok := row[11].([]byte); !ok || p == nil || len(p) != 0 {
		t.Errorf("an empty payload: %#v", row[11])
	}
}

func equalValue(a, b any) bool {
	if x, ok := a.([]byte); ok {
		y, ok := b.([]byte)
		return ok && slices.Equal(x, y)
	}
	return a == b
}

// TestTheIdsOfTheDictionaryAreGivenInTheOrderEntitiesFirstAppear: the same stream makes
// the same dictionary, an edge's source before its target.
func TestTheIdsOfTheDictionaryAreGivenInTheOrderEntitiesFirstAppear(t *testing.T) {
	pod, node, pod2 := testFP(catalog.K8sPod, 1), testFP(catalog.K8sNode, 2), testFP(catalog.K8sPod, 3)
	at := time.Unix(100, 0).UTC()
	rec := func(seq uint64, s engine.Subject) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: s, Producer: "k8s", EventTime: at, Seq: seq, Kind: lifecycle.Observe}
	}
	l := &Loader{ids: true, dict: map[identity.Fingerprint]int64{}}
	for _, r := range []engine.Record{
		rec(1, engine.EdgeSubject(pod, node, catalog.ScheduledOn)),
		rec(2, engine.EntitySubject(pod2)),
		rec(3, engine.EdgeSubject(pod2, node, catalog.ScheduledOn)),
	} {
		row := l.logRow(r)
		if _, ok := row[4].(int64); !ok {
			t.Fatalf("a key that is not an id: %#v", row[4])
		}
	}
	for fp, id := range map[identity.Fingerprint]int64{pod: 1, node: 2, pod2: 3} {
		if l.dict[fp] != id {
			t.Errorf("%s has id %d, want %d", fp, l.dict[fp], id)
		}
	}
	if len(l.dictOrder) != 3 || l.dictOrder[0] != pod || l.dictOrder[2] != pod2 {
		t.Errorf("dictionary order %v", l.dictOrder)
	}
}
