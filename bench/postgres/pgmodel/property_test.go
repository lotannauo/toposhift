package pgmodel_test

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

const sec = int64(time.Second)

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// The subjects of the random streams: two entities and four edges between them (two
// of them the same pair in the other direction, and two relations), all in layer L2,
// so that they share endpoints and a reader sees several peers.
var (
	pod1, pod2, node1 = fp("k8s.pod", 1), fp("k8s.pod", 2), fp("k8s.node", 3)

	subjects = []engine.Subject{
		engine.EntitySubject(pod1),
		engine.EntitySubject(node1),
		engine.EdgeSubject(pod1, node1, "scheduled_on"),
		engine.EdgeSubject(pod2, node1, "scheduled_on"),
		engine.EdgeSubject(node1, pod1, "scheduled_on"),
		engine.EdgeSubject(pod1, node1, "part_of"),
	}
	producers = []lifecycle.Producer{"a", "b", "c"}
)

// genRecord draws a record about a random subject. Event times fall in a minute
// starting one second past the epoch, so that many records share instants and many
// intervals touch; TTLs are zero (watch mode) or a few seconds; a delete carries none.
func genRecord(t *rapid.T, label string) engine.Record {
	s := rapid.SampledFrom(subjects).Draw(t, label+" subject")
	r := engine.Record{
		Layer: catalog.L2, Subject: s,
		Producer:  rapid.SampledFrom(producers).Draw(t, label+" producer"),
		EventTime: time.Unix(0, (1+rapid.Int64Range(0, 60).Draw(t, label+" at"))*sec).UTC(),
	}
	if rapid.IntRange(0, 3).Draw(t, label+" delete") == 0 {
		r.Kind = lifecycle.Delete
		return r
	}
	r.Kind = lifecycle.Observe
	r.Payload = []byte{byte(rapid.IntRange(0, 3).Draw(t, label+" payload"))}
	if rapid.Bool().Draw(t, label+" has ttl") {
		r.TTL = time.Duration(rapid.Int64Range(1, 8).Draw(t, label+" ttl")) * time.Second
	}
	if rapid.IntRange(0, 3).Draw(t, label+" run") == 0 {
		r.Through = r.EventTime.Add(time.Duration(rapid.Int64Range(0, 5).Draw(t, label+" through")) * time.Second)
	}
	return r
}

// TestRandomStreamsEqualBulkAndOracle writes random batches, with late events, shared
// instants, overridden records, several producers, runs and deletes, and retentions at
// random points, to a model and to the reference engine. After every batch the
// model's rows, clipped to the horizon, are the rows of a bulk load of everything
// written, and every kind of read at every instant of interest is the reference
// engine's.
func TestRandomStreamsEqualBulkAndOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m, ora := pgmodel.New(), oracle.New()
		var written []engine.Record
		var horizon time.Time
		seq := uint64(rapid.IntRange(1, 5).Draw(t, "first seq"))
		steps := rapid.IntRange(1, 25).Draw(t, "steps")
		for step := range steps {
			var batch []engine.Record
			for i := range rapid.IntRange(1, 6).Draw(t, "batch size") {
				r := genRecord(t, fmt.Sprintf("record %d.%d", step, i))
				if r.EventTime.Before(horizon) {
					continue
				}
				r.Seq = seq
				seq += uint64(rapid.IntRange(1, 3).Draw(t, "seq gap"))
				batch = append(batch, r)
			}
			if len(batch) > 0 {
				if err := m.Write(batch); err != nil {
					t.Fatal(err)
				}
				if err := ora.Write(batch); err != nil {
					t.Fatal(err)
				}
				written = append(written, batch...)
			}
			if rapid.IntRange(0, 4).Draw(t, "retain") == 0 {
				h := time.Unix(0, (1+rapid.Int64Range(0, 64).Draw(t, "horizon"))*sec).UTC()
				if err := m.Retain(h); err != nil {
					t.Fatal(err)
				}
				if err := ora.Retain(h); err != nil {
					t.Fatal(err)
				}
				if h.After(horizon) {
					horizon = h
				}
			}
			checkAgainstBulk(t, m, written, horizon)
			checkReads(t, m, ora, written, horizon)
		}
	})
}

func nanos(horizon time.Time) int64 {
	if horizon.IsZero() {
		return 0
	}
	return pgmodel.ToNanos(horizon)
}

func checkAgainstBulk(t *rapid.T, m *pgmodel.Model, written []engine.Record, horizon time.Time) {
	t.Helper()
	bulk, err := pgmodel.Bulk(written)
	if err != nil {
		t.Fatal(err)
	}
	got, want := m.Tables().Clipped(nanos(horizon)), bulk.Clipped(nanos(horizon))
	if !got.Equal(want) {
		t.Fatalf("horizon %s: the rows written batch by batch are not a bulk load's: %s", horizon.Format(time.RFC3339), got.FirstDifference(want))
	}
}

// checkReads asks the model and the reference engine the same questions.
func checkReads(t *rapid.T, m *pgmodel.Model, ora *oracle.Oracle, written []engine.Record, horizon time.Time) {
	t.Helper()
	floor := engine.MinEventTime
	if !horizon.IsZero() {
		floor = horizon
	}
	var instants []time.Time
	for _, r := range written {
		for _, x := range []time.Time{r.EventTime, r.Through, r.EventTime.Add(r.TTL), r.Through.Add(r.TTL)} {
			if x.IsZero() {
				continue
			}
			for _, d := range []time.Duration{-1, 0, 1} {
				if at := x.Add(d); !at.Before(floor) {
					instants = append(instants, at)
				}
			}
		}
	}
	instants = append(instants, floor, engine.MaxEventTime, engine.MaxEventTime.Add(time.Hour))
	slices.SortFunc(instants, time.Time.Compare)
	instants = slices.CompactFunc(instants, time.Time.Equal)

	sc := engine.Current(catalog.L2)
	other := engine.Current(catalog.L1)
	for _, at := range instants {
		for _, e := range []identity.Fingerprint{pod1, pod2, node1} {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				for _, s := range []engine.Scope{sc, other} {
					a, errA := m.Neighbors(e, dir, at, s)
					b, errB := ora.Neighbors(e, dir, at, s)
					if errA != nil || errB != nil || !equalNeighbors(a, b) {
						t.Fatalf("Neighbors(%s, %s, %s, %s) = %v (%v), the reference engine %v (%v)", e, dir, at.Format(time.RFC3339Nano), s.Layer, a, errA, b, errB)
					}
				}
			}
			for _, s := range []engine.Scope{sc, other} {
				a, errA := m.Alive(e, at, s)
				b, errB := ora.Alive(e, at, s)
				if errA != nil || errB != nil || a != b {
					t.Fatalf("Alive(%s, %s, %s) = %v (%v), the reference engine %v (%v)", e, at.Format(time.RFC3339Nano), s.Layer, a, errA, b, errB)
				}
			}
		}
		fps := []identity.Fingerprint{pod1, node1, pod2, pod1}
		for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
			a, errA := m.NeighborsBatch(fps, dir, at, sc)
			b, errB := ora.NeighborsBatch(fps, dir, at, sc)
			if errA != nil || errB != nil || !equalBatches(a, b) {
				t.Fatalf("NeighborsBatch(%s, %s) = %v (%v), the reference engine %v (%v)", dir, at.Format(time.RFC3339Nano), a, errA, b, errB)
			}
		}
	}
	// The reads as of an earlier token are the client's fold of the log; before a
	// retention they can be asked about at every token.
	if horizon.IsZero() {
		last := m.LastSeq()
		for _, asOf := range []uint64{last / 2, last - last/3, last} {
			s := engine.Scope{Layer: catalog.L2, AsOf: asOf}
			for _, at := range instants {
				for _, e := range []identity.Fingerprint{pod1, node1} {
					a, errA := m.Neighbors(e, engine.Reverse, at, s)
					b, errB := ora.Neighbors(e, engine.Reverse, at, s)
					if errA != nil || errB != nil || !equalNeighbors(a, b) {
						t.Fatalf("Neighbors(%s, reverse, %s, as of %d) = %v (%v), the reference engine %v (%v)", e, at.Format(time.RFC3339Nano), asOf, a, errA, b, errB)
					}
					okA, _ := m.Alive(e, at, s)
					okB, _ := ora.Alive(e, at, s)
					if okA != okB {
						t.Fatalf("Alive(%s, %s, as of %d) = %v, the reference engine %v", e, at.Format(time.RFC3339Nano), asOf, okA, okB)
					}
				}
			}
		}
	}
	for i := 0; i+1 < len(instants) && i < 40; i++ {
		for _, e := range []identity.Fingerprint{pod1, node1} {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				a, errA := m.Window(e, dir, instants[i], instants[i+1], sc)
				b, errB := ora.Window(e, dir, instants[i], instants[i+1], sc)
				if errA != nil || errB != nil || !equalRecords(a, b) {
					t.Fatalf("Window(%s, %s, %s, %s) differs: %v (%v) / %v (%v)", e, dir, instants[i].Format(time.RFC3339Nano), instants[i+1].Format(time.RFC3339Nano), a, errA, b, errB)
				}
			}
		}
	}
	// A window to the end of time holds a record at the last representable instant.
	a, _ := m.Window(pod1, engine.Forward, floor, engine.MaxEventTime.Add(time.Hour), sc)
	b, _ := ora.Window(pod1, engine.Forward, floor, engine.MaxEventTime.Add(time.Hour), sc)
	if !equalRecords(a, b) {
		t.Fatalf("Window to the end of time differs: %v / %v", a, b)
	}
}

func equalNeighbors(a, b []engine.Neighbor) bool { return slices.Equal(a, b) }

func equalBatches(a, b [][]engine.Neighbor) bool {
	return slices.EqualFunc(a, b, slices.Equal[[]engine.Neighbor])
}

func equalRecords(a, b []engine.Record) bool {
	return slices.EqualFunc(a, b, func(x, y engine.Record) bool {
		return reflect.DeepEqual(normalize(x), normalize(y))
	})
}

func normalize(r engine.Record) engine.Record {
	if len(r.Payload) == 0 {
		r.Payload = nil
	}
	return r
}
