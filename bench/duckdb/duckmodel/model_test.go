package duckmodel

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestWriteRefusesWholeBatches covers what the contract says a refused batch leaves:
// nothing, and its sequence numbers unused.
func TestWriteRefusesWholeBatches(t *testing.T) {
	sub := fixtureEdge(t)
	for _, arm := range arms {
		m := New(arm)
		if err := m.Write([]engine.Record{rec1(sub, 1, "a", lifecycle.Observe, 10, 0)}); err != nil {
			t.Fatal(err)
		}
		invalid := rec1(sub, 3, "a", lifecycle.Observe, 11, 0)
		invalid.Producer = ""
		otherLayer := rec1(sub, 3, "a", lifecycle.Observe, 11, 0)
		otherLayer.Layer = catalog.L3
		for name, batch := range map[string][]engine.Record{
			"an invalid record":       {rec1(sub, 2, "a", lifecycle.Observe, 11, 0), invalid},
			"a sequence not above":    {rec1(sub, 2, "a", lifecycle.Observe, 11, 0), rec1(sub, 2, "a", lifecycle.Observe, 12, 0)},
			"a sequence at the last":  {rec1(sub, 1, "a", lifecycle.Observe, 12, 0)},
			"a subject in two layers": {rec1(sub, 2, "a", lifecycle.Observe, 11, 0), otherLayer},
		} {
			if err := m.Write(batch); !errors.Is(err, engine.ErrInvalid) {
				t.Errorf("%s, %s: %v", arm, name, err)
			}
			if m.LastSeq() != 1 || len(m.Records()) != 1 {
				t.Errorf("%s, %s: the model moved to seq %d with %d records", arm, name, m.LastSeq(), len(m.Records()))
			}
		}
		if err := m.Write([]engine.Record{rec1(sub, 2, "a", lifecycle.Observe, 11, 0)}); err != nil {
			t.Fatalf("%s: seq 2 was used by a refused batch: %v", arm, err)
		}
	}
}

// TestARecordBeforeTheHorizonRefusesItsBatch: after a retention a batch with one record
// before the horizon is refused whole.
func TestARecordBeforeTheHorizonRefusesItsBatch(t *testing.T) {
	sub := fixtureEdge(t)
	m := New(ArmRecords)
	if err := m.Write([]engine.Record{rec1(sub, 1, "a", lifecycle.Observe, 10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Retain(time.Unix(14, 0)); err != nil {
		t.Fatal(err)
	}
	err := m.Write([]engine.Record{rec1(sub, 2, "a", lifecycle.Observe, 30, 1), rec1(sub, 3, "a", lifecycle.Observe, 13, 1)})
	if !errors.Is(err, engine.ErrBeforeHorizon) || !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a record before the horizon: %v", err)
	}
	if m.LastSeq() != 1 || len(m.Records()) != 1 {
		t.Fatalf("a refused batch changed the model: seq %d, %d records", m.LastSeq(), len(m.Records()))
	}
}

// TestRetainingPastTheEndRefusesEveryRecord: a horizon after the last representable
// instant is after every record, that instant's included, as the reference engine has it,
// and what stands for the records stays readable.
func TestRetainingPastTheEndRefusesEveryRecord(t *testing.T) {
	sub := fixtureEdge(t)
	for _, arm := range arms {
		m, ora := New(arm), oracle.New()
		first := rec1(sub, 1, "a", lifecycle.Observe, 10, 0)
		for _, e := range []engine.Engine{m, ora} {
			if err := e.Write([]engine.Record{first}); err != nil {
				t.Fatal(err)
			}
			if err := e.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		atEnd := rec1(sub, 2, "a", lifecycle.Observe, 0, 0)
		atEnd.EventTime = engine.MaxEventTime
		for name, e := range map[string]engine.Engine{"model": m, "oracle": ora} {
			if err := e.Write([]engine.Record{atEnd}); !errors.Is(err, engine.ErrBeforeHorizon) {
				t.Errorf("%s %s: a write at the last instant: %v", arm, name, err)
			}
		}
		got, err1 := m.Neighbors(sub.A, engine.Forward, engine.MaxEventTime, engine.Current(catalog.L2))
		want, err2 := ora.Neighbors(sub.A, engine.Forward, engine.MaxEventTime, engine.Current(catalog.L2))
		if err1 != nil || err2 != nil || !slices.Equal(got, want) || len(got) != 1 {
			t.Errorf("%s: after the end: %v, %v; the reference: %v, %v", arm, got, err1, want, err2)
		}
		// And the same Retain again, or an earlier one, changes nothing.
		if err := m.Retain(engine.MaxEventTime.Add(2 * time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := m.Retain(time.Unix(5, 0)); err != nil {
			t.Fatal(err)
		}
		if m.LastSeq() != 1 || len(m.Records()) != 1 {
			t.Errorf("%s: seq %d, %d records", arm, m.LastSeq(), len(m.Records()))
		}
	}
}

// TestOpenReturnsWhatWasLeft is the reopen the conformance factory relies on, and an
// arm's model is not another arm's.
func TestOpenReturnsWhatWasLeft(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { Forget(dir, ArmRecords); Forget(dir, ArmIntervals) })
	m := Open(dir, ArmRecords)
	if err := m.Write([]engine.Record{rec1(fixtureEdge(t), 1, "a", lifecycle.Observe, 10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if again := Open(dir, ArmRecords); again.LastSeq() != 1 || again.Arm() != ArmRecords {
		t.Fatalf("reopened at seq %d", again.LastSeq())
	}
	if other := Open(dir, ArmIntervals); other.LastSeq() != 0 || other.Arm() != ArmIntervals {
		t.Fatalf("the other arm opened at seq %d", other.LastSeq())
	}
	Forget(dir, ArmRecords)
	if fresh := Open(dir, ArmRecords); fresh.LastSeq() != 0 {
		t.Fatalf("after Forget, seq %d", fresh.LastSeq())
	}
}

// TestWhatTheModelHoldsIsAFunctionOfTheStream runs a stream twice and gets the same
// counts, rows and size.
func TestWhatTheModelHoldsIsAFunctionOfTheStream(t *testing.T) {
	cfg := conformance.Configs()[1]
	run := func(arm Arm) (Stats, int64, []uint64) {
		m := New(arm)
		stream(t, cfg, 40, []float64{0.4}, []engine.Engine{m})
		st, err := m.Stats()
		if err != nil {
			t.Fatal(err)
		}
		size, err := m.Size()
		if err != nil {
			t.Fatal(err)
		}
		return st, size, seqsOf(m.Records())
	}
	for _, arm := range arms {
		s1, z1, r1 := run(arm)
		s2, z2, r2 := run(arm)
		if s1 != s2 || z1 != z2 || !slices.Equal(r1, r2) {
			t.Fatalf("%s: two runs differ: %+v %d / %+v %d", arm, s1, z1, s2, z2)
		}
		if s1.Records == 0 || s1.Edges == 0 || s1.Subjects <= s1.Edges || z1 <= 0 {
			t.Fatalf("%s: a stream with entities, edges and a retention should hold rows of each: %+v, size %d", arm, s1, z1)
		}
		// Only the intervals arm holds an interval file.
		if (arm == ArmIntervals) != (s1.Intervals > 0) {
			t.Fatalf("%s: %d intervals", arm, s1.Intervals)
		}
	}
	// The intervals arm costs the intervals in its size.
	_, recordsSize, _ := run(ArmRecords)
	_, intervalsSize, _ := run(ArmIntervals)
	if intervalsSize <= recordsSize {
		t.Errorf("sizes: records arm %d, intervals arm %d", recordsSize, intervalsSize)
	}
}

// TestPayloadsAreCopied: a caller that changes the bytes it wrote, or the bytes it was
// given by a read, does not change the model.
func TestPayloadsAreCopied(t *testing.T) {
	sub := fixtureEdge(t)
	m := New(ArmRecords)
	r := rec1(sub, 1, "a", lifecycle.Observe, 10, 0)
	r.Payload = []byte{1, 2, 3}
	if err := m.Write([]engine.Record{r}); err != nil {
		t.Fatal(err)
	}
	r.Payload[0] = 9
	win := func() []engine.Record {
		rs, err := m.Window(sub.A, engine.Forward, time.Unix(0, 0), time.Unix(20, 0), engine.Current(catalog.L2))
		if err != nil || len(rs) != 1 {
			t.Fatalf("window: %v, %v", rs, err)
		}
		return rs
	}
	got := win()
	if !slices.Equal(got[0].Payload, []byte{1, 2, 3}) {
		t.Fatalf("the model holds %v after the writer changed its bytes", got[0].Payload)
	}
	got[0].Payload[1] = 9
	if !slices.Equal(win()[0].Payload, []byte{1, 2, 3}) {
		t.Fatal("a reader changed the model's bytes")
	}
}
