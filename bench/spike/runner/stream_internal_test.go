package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
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

// writtenSink records what Drive writes and when it retains.
type writtenSink struct {
	records []engine.Record
	retains []int
}

func (s *writtenSink) Write(b []engine.Record) error {
	s.records = append(s.records, b...)
	return nil
}

func (s *writtenSink) Retain(time.Time) error {
	s.retains = append(s.retains, len(s.records))
	return nil
}

// What a stream with pins says it wrote is what its sinks were given: the records and
// the last sequence number are the projected ones, and the digest covers exactly them
// and the retentions, by the number of records written when each came.
func TestDriveWithPinsReportsExactlyWhatItWrote(t *testing.T) {
	t.Parallel()

	for _, batch := range []int{1, 7, 64, 100, 333} { // the last batch of the stream ends on a pinned record or not
		driveWithPins(t, batch)
	}
}

func driveWithPins(t *testing.T, batch int) {
	t.Helper()
	w := workload.Small() // many entities, so that most records touch no pin
	w.Duration = 8 * time.Minute
	spec := DefaultSpec(w)
	spec.BatchSize = batch
	spec.MinNonEmpty = 0
	spec.Retentions = []Retention{{At: 5 * time.Minute, Keep: 3 * time.Minute}}
	pins, err := MakePins(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Pins = pins
	var sink writtenSink
	info, err := Drive(context.Background(), spec, &sink)
	if err != nil {
		t.Fatal(err)
	}
	if info.Records != uint64(len(sink.records)) || len(sink.records) == 0 {
		t.Fatalf("%d records reported, %d written", info.Records, len(sink.records))
	}
	if want := sink.records[len(sink.records)-1].Seq; info.LastSeq != want {
		t.Errorf("last seq %d, the last record written has %d", info.LastSeq, want)
	}
	var written uint64
	for _, r := range sink.records {
		written += uint64(len(r.Payload))
	}
	if info.PayloadBytes != written {
		t.Errorf("%d payload bytes reported, %d written", info.PayloadBytes, written)
	}
	if len(sink.retains) != 1 {
		t.Fatalf("%d retentions", len(sink.retains))
	}
	// Replay the digest from what was written.
	h := sha256.New()
	var buf []byte
	for i, r := range sink.records {
		if i == sink.retains[0] {
			h.Write(appendRetention(buf[:0], uint64(i), info.Retentions[0].Horizon))
		}
		buf = appendRecord(buf[:0], r)
		h.Write(buf)
	}
	if sink.retains[0] == len(sink.records) { // a retention after the last record
		h.Write(appendRetention(buf[:0], uint64(len(sink.records)), info.Retentions[0].Horizon))
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.Digest {
		t.Errorf("the digest is %s, what was written has %s", info.Digest, got)
	}
	for _, r := range sink.records {
		if !touches(pins.entities(), r) {
			t.Fatalf("a record that touches no pin was written: %+v", r.Subject)
		}
	}
}
