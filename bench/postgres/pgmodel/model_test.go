package pgmodel_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var edge = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)

func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(seq uint64, p lifecycle.Producer, s int64, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: edge, Producer: p, EventTime: at(s), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(seq uint64, p lifecycle.Producer, s int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: edge, Producer: p, EventTime: at(s), Seq: seq, Kind: lifecycle.Delete}
}

func edgeRows(t *testing.T, m *pgmodel.Model) []pgmodel.EdgeRow {
	t.Helper()
	return m.Tables().Edges
}

func span(row pgmodel.EdgeRow) [2]int64 {
	end, open := row.EffectiveEnd()
	if open {
		end = -1
	}
	return [2]int64{row.Start / sec, end / sec}
}

// TestRowsFollowARefreshAGapALateDeleteAndARetention walks one edge through the cases
// the planner exists for, and counts the rows each costs: a first interval is an
// insert, an extension an update of the one row, a touching observation by another
// producer joins two intervals, a late delete splits one in two (an update and an
// insert), and a retention deletes the rows that ended and keeps the one that spans the
// horizon with its old start.
func TestRowsFollowARefreshAGapALateDeleteAndARetention(t *testing.T) {
	m := pgmodel.New()
	write := func(rs ...engine.Record) {
		t.Helper()
		if err := m.Write(rs); err != nil {
			t.Fatal(err)
		}
	}
	rows := func(want ...[2]int64) {
		t.Helper()
		var got [][2]int64
		for _, r := range edgeRows(t, m) {
			got = append(got, span(r))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("rows %v, want %v", got, want)
		}
	}
	counts := func(inserted, updated, deleted uint64) {
		t.Helper()
		c := m.Counts()
		if c.RowsInserted != inserted || c.RowsUpdated != updated || c.RowsDeleted != deleted {
			t.Fatalf("rows inserted %d, updated %d, deleted %d; want %d, %d, %d", c.RowsInserted, c.RowsUpdated, c.RowsDeleted, inserted, updated, deleted)
		}
	}

	write(observe(1, "a", 10, 5))
	rows([2]int64{10, 15})
	counts(1, 0, 0)

	write(observe(2, "a", 13, 5)) // a refresh before the deadline extends the interval
	rows([2]int64{10, 18})
	counts(1, 1, 0)

	write(observe(3, "b", 18, 2)) // another producer, starting where the last ended: one interval
	rows([2]int64{10, 20})
	counts(1, 2, 0)

	write(del(4, "a", 12)) // a late delete splits the interval: a is out from 12 to 13
	rows([2]int64{10, 12}, [2]int64{13, 20})
	counts(2, 3, 0)
	if rs := edgeRows(t, m); rs[0].EndSource != lifecycle.EndProducer || rs[1].EndSource != lifecycle.EndLivenessExpiry {
		t.Fatalf("end sources %v, %v", rs[0].EndSource, rs[1].EndSource)
	}

	if err := m.Retain(at(14)); err != nil {
		t.Fatal(err)
	}
	rows([2]int64{13, 20}) // [10, 12) ended before the horizon; [13, 20) spans it and keeps its start
	counts(2, 3, 1)
	clipped := m.Tables().Clipped(14 * sec)
	if len(clipped.Edges) != 1 || clipped.Edges[0].Start != 14*sec {
		t.Fatalf("clipped rows %+v", clipped.Edges)
	}

	// A record after the retention extends the row that spans the horizon from the
	// baseline of a, the last record of a before the horizon, which the log no longer
	// holds.
	write(observe(5, "c", 20, 3))
	rows([2]int64{13, 23})
	counts(2, 4, 1)

	// A record before the horizon is refused, and the batch with it.
	before := m.Tables().Digest()
	err := m.Write([]engine.Record{observe(6, "a", 30, 1), observe(7, "a", 13, 1)})
	if !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Fatalf("a record before the horizon: %v", err)
	}
	if m.Tables().Digest() != before || m.LastSeq() != 5 {
		t.Fatal("a refused batch changed the model")
	}
}

// TestWriteRefusesWholeBatches covers what the contract says a refused batch leaves:
// nothing, and its sequence numbers unused.
func TestWriteRefusesWholeBatches(t *testing.T) {
	m := pgmodel.New()
	if err := m.Write([]engine.Record{observe(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	invalid := observe(3, "a", 11, 0)
	invalid.Producer = ""
	otherLayer := observe(3, "a", 11, 0)
	otherLayer.Layer = catalog.L3
	for name, batch := range map[string][]engine.Record{
		"an invalid record":       {observe(2, "a", 11, 0), invalid},
		"a sequence not above":    {observe(2, "a", 11, 0), observe(2, "a", 12, 0)},
		"a sequence at the last":  {observe(1, "a", 12, 0)},
		"a subject in two layers": {observe(2, "a", 11, 0), otherLayer},
	} {
		if err := m.Write(batch); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
		if m.LastSeq() != 1 || m.Counts().Records != 1 {
			t.Errorf("%s: the model moved to seq %d, %d records", name, m.LastSeq(), m.Counts().Records)
		}
	}
	if err := m.Write([]engine.Record{observe(2, "a", 11, 0)}); err != nil {
		t.Fatalf("seq 2 was used by a refused batch: %v", err)
	}
}

// TestCountsAreAFunctionOfTheStream runs a stream twice and gets the same counts and
// the same rows.
func TestCountsAreAFunctionOfTheStream(t *testing.T) {
	run := func() (pgmodel.Counts, string) {
		m := pgmodel.New()
		streamInto(t, m, configForCounts(), 40, []float64{0.4}, func([]engine.Record, time.Time) {})
		return m.Counts(), m.Tables().Digest()
	}
	c1, d1 := run()
	c2, d2 := run()
	if c1 != c2 || d1 != d2 {
		t.Fatalf("two runs differ: %+v %s / %+v %s", c1, d1, c2, d2)
	}
	if c1.Records == 0 || c1.RowsInserted == 0 || c1.RowsUpdated == 0 || c1.RowsDeleted == 0 || c1.Refolds == 0 {
		t.Fatalf("a stream with refreshes and a retention should insert, update and delete rows: %+v", c1)
	}
}

// TestOpenReturnsWhatWasLeft is the reopen the conformance factory relies on.
func TestOpenReturnsWhatWasLeft(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { pgmodel.Forget(dir) })
	m := pgmodel.Open(dir)
	if err := m.Write([]engine.Record{observe(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if again := pgmodel.Open(dir); again.LastSeq() != 1 {
		t.Fatalf("reopened at seq %d", again.LastSeq())
	}
	pgmodel.Forget(dir)
	if fresh := pgmodel.Open(dir); fresh.LastSeq() != 0 {
		t.Fatalf("after Forget, seq %d", fresh.LastSeq())
	}
}

// TestUnchangedRowsAndRowsBeforeTheHorizonAreNotWritten: a record inside an interval
// that changes nothing writes nothing; and a re-fold after a retention does not write
// a row that ended before the horizon, and restarts a straddling row at its baseline.
func TestUnchangedRowsAndRowsBeforeTheHorizonAreNotWritten(t *testing.T) {
	m := pgmodel.New()
	write := func(r engine.Record) {
		t.Helper()
		if err := m.Write([]engine.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	rows := func(want ...[2]int64) {
		t.Helper()
		var got [][2]int64
		for _, r := range edgeRows(t, m) {
			got = append(got, span(r))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("rows %v, want %v", got, want)
		}
	}
	counts := func(inserted, updated, deleted uint64) {
		t.Helper()
		c := m.Counts()
		if c.RowsInserted != inserted || c.RowsUpdated != updated || c.RowsDeleted != deleted {
			t.Fatalf("rows inserted %d, updated %d, deleted %d; want %d, %d, %d", c.RowsInserted, c.RowsUpdated, c.RowsDeleted, inserted, updated, deleted)
		}
	}

	write(observe(1, "a", 10, 10))
	rows([2]int64{10, 20})
	counts(1, 0, 0)

	write(observe(2, "b", 11, 1)) // inside the interval: nothing changes
	rows([2]int64{10, 20})
	counts(1, 0, 0)

	write(observe(3, "a", 13, 10))
	rows([2]int64{10, 23})
	counts(1, 1, 0)

	if err := m.Retain(at(14)); err != nil {
		t.Fatal(err)
	}
	// The re-fold sees the baselines of a (13) and b (11): b's [11, 12) ended before
	// the horizon and is not written, and the straddling row restarts at a's baseline.
	write(observe(4, "c", 25, 0))
	rows([2]int64{13, 23}, [2]int64{25, 0}) // an open row has end 0
	counts(3, 1, 1)
}

// TestRetainingPastTheEndRefusesEveryRecord: a horizon after the last representable
// instant is after every record, that instant's included, as the reference engine has it.
func TestRetainingPastTheEndRefusesEveryRecord(t *testing.T) {
	m, ora := pgmodel.New(), oracle.New()
	last := observe(1, "a", 10, 0)
	for _, e := range []engine.Engine{m, ora} {
		if err := e.Write([]engine.Record{last}); err != nil {
			t.Fatal(err)
		}
		if err := e.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	atEnd := observe(2, "a", 0, 0)
	atEnd.EventTime = engine.MaxEventTime
	for name, e := range map[string]engine.Engine{"model": m, "oracle": ora} {
		if err := e.Write([]engine.Record{atEnd}); !errors.Is(err, engine.ErrBeforeHorizon) {
			t.Errorf("%s: a write at the last instant: %v", name, err)
		}
	}
	// And the same Retain again, or an earlier one, changes nothing.
	if err := m.Retain(engine.MaxEventTime.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.Retain(at(5)); err != nil {
		t.Fatal(err)
	}
	if m.LastSeq() != 1 {
		t.Errorf("last seq %d", m.LastSeq())
	}
}
