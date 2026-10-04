package runner

import (
	"bytes"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func TestCacheBytes(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		payload  uint64
		fraction float64
		want     int64
	}{
		{0, 0.25, 1 << 20},
		{1 << 20, 0.25, 1 << 20},
		{16 << 20, 0.25, 4 << 20},
		{16<<20 + 12345, 0.25, 4 << 20},
		{100 << 20, 0.5, 50 << 20},
		{(1 << 30) + (1 << 29), 1, 1<<30 + 1<<29},
	} {
		if got := cacheBytes(c.payload, c.fraction); got != c.want {
			t.Errorf("cacheBytes(%d, %g) = %d, want %d", c.payload, c.fraction, got, c.want)
		}
	}
}

func testFingerprint(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// The digest of a stream covers every field a store is given: changing any one
// of them changes the bytes a record is hashed as, and equal records hash as
// equal bytes.
func TestAppendRecordCoversEveryField(t *testing.T) {
	t.Parallel()

	pod := testFingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	other := testFingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "q")
	node := testFingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "a",
		EventTime: at, Seq: 7, Kind: lifecycle.Observe, TTL: time.Minute, Through: at.Add(time.Second), Payload: []byte("xy"),
	}
	want := appendRecord(nil, base)
	if !bytes.Equal(want, appendRecord(nil, base)) {
		t.Fatal("the same record hashed as different bytes")
	}
	// The buffer is appended to, not replaced.
	if got := appendRecord([]byte("pre"), base); !bytes.Equal(got, append([]byte("pre"), want...)) {
		t.Error("appendRecord did not append")
	}
	for name, mod := range map[string]func(*engine.Record){
		"layer":       func(r *engine.Record) { r.Layer = catalog.L1 },
		"seq":         func(r *engine.Record) { r.Seq++ },
		"kind":        func(r *engine.Record) { r.Kind = lifecycle.Delete },
		"subject":     func(r *engine.Record) { r.Subject = engine.EntitySubject(pod) },
		"source":      func(r *engine.Record) { r.Subject.A = other },
		"target":      func(r *engine.Record) { r.Subject.B = pod },
		"relation":    func(r *engine.Record) { r.Subject.Relation = catalog.RunsOn },
		"producer":    func(r *engine.Record) { r.Producer = "b" },
		"event time":  func(r *engine.Record) { r.EventTime = r.EventTime.Add(time.Nanosecond) },
		"ttl":         func(r *engine.Record) { r.TTL++ },
		"through":     func(r *engine.Record) { r.Through = r.Through.Add(time.Nanosecond) },
		"no through":  func(r *engine.Record) { r.Through = time.Time{} },
		"payload":     func(r *engine.Record) { r.Payload = []byte("xz") },
		"payload len": func(r *engine.Record) { r.Payload = []byte("x") },
	} {
		r := base
		mod(&r)
		if bytes.Equal(want, appendRecord(nil, r)) {
			t.Errorf("changing the %s did not change what the record hashes as", name)
		}
	}
	// A field moving into its neighbour cannot make two records hash alike.
	a, b := base, base
	a.Subject.Relation, a.Producer = "x", "yz"
	b.Subject.Relation, b.Producer = "xy", "z"
	if bytes.Equal(appendRecord(nil, a), appendRecord(nil, b)) {
		t.Error("two records that differ in where a string ends hash alike")
	}
	// An entity with no second end is not an edge to the zero entity.
	if bytes.Equal(appendFingerprint(nil, identity.Fingerprint{}), appendFingerprint(nil, pod)) {
		t.Error("the zero fingerprint hashes like a real one")
	}
	if bytes.Equal(appendRetention(nil, 1, at), appendRetention(nil, 2, at)) || bytes.Equal(appendRetention(nil, 1, at), appendRetention(nil, 1, at.Add(1))) {
		t.Error("a retention hashes alike at another record count or horizon")
	}
}
