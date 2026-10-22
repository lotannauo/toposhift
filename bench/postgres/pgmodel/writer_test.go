package pgmodel_test

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// scripted writes records one batch at a time to a model and says what each cost.
type scripted struct {
	t    *testing.T
	m    *pgmodel.Model
	last pgmodel.Counts
	seq  uint64
}

func newScripted(t *testing.T, cfg pgmodel.ModelConfig) *scripted {
	return &scripted{t: t, m: pgmodel.NewWith(cfg)}
}

// write stores the records, numbering them, and returns what the batch added to the
// counts.
func (s *scripted) write(rs ...engine.Record) pgmodel.Counts {
	s.t.Helper()
	for i := range rs {
		s.seq++
		rs[i].Seq = s.seq
	}
	if err := s.m.Write(rs); err != nil {
		s.t.Fatal(err)
	}
	c := s.m.Counts()
	d := pgmodel.Counts{
		FastSubjects: c.FastSubjects - s.last.FastSubjects, Refolds: c.Refolds - s.last.Refolds,
		RowsInserted: c.RowsInserted - s.last.RowsInserted, RowsUpdated: c.RowsUpdated - s.last.RowsUpdated,
		RowsHeapOnly: c.RowsHeapOnly - s.last.RowsHeapOnly, RowsFinalized: c.RowsFinalized - s.last.RowsFinalized,
		RowsDeleted: c.RowsDeleted - s.last.RowsDeleted,
	}
	s.last = c
	return d
}

type cost struct{ fast, slow, inserted, updated, heapOnly, finalized, deleted uint64 }

func (s *scripted) cost(got pgmodel.Counts, want cost) {
	s.t.Helper()
	g := cost{got.FastSubjects, got.Refolds, got.RowsInserted, got.RowsUpdated, got.RowsHeapOnly, got.RowsFinalized, got.RowsDeleted}
	if g != want {
		s.t.Fatalf("cost %+v, want %+v", g, want)
	}
}

// row is the edge's only row as stored: [start, deadline-or-end), whether it is
// provisional, and its end source.
func (s *scripted) rows(want ...pgmodel.Row) {
	s.t.Helper()
	var got []pgmodel.Row
	for _, e := range s.m.Tables().Edges {
		got = append(got, e.Row)
	}
	if !slices.Equal(got, want) {
		s.t.Fatalf("rows %+v, want %+v", got, want)
	}
}

func prov(start, deadline int64) pgmodel.Row {
	return pgmodel.Provisional(start*sec, deadline*sec, lifecycle.EndLivenessExpiry)
}

func fin(start, end int64, src lifecycle.EndSource) pgmodel.Row {
	return pgmodel.Final(start*sec, end*sec, src)
}

// TestAHeartbeatIsAnUpdateOfADeadlineAndAnExpiryIsOneUpdate: the writer keeps the one
// interval of a refreshed subject as a provisional row, moves its deadline with updates
// that touch no index, and closes it once the newest event time has passed the deadline,
// which costs one update that does.
func TestAHeartbeatIsAnUpdateOfADeadlineAndAnExpiryIsOneUpdate(t *testing.T) {
	s := newScripted(t, pgmodel.ModelConfig{})
	// The first record of a subject is an insert of a row that is provisional, because
	// its deadline is after every event time written.
	s.cost(s.write(observe(0, "a", 10, 5)), cost{fast: 1, inserted: 1})
	s.rows(prov(10, 15))
	// Each refresh before the deadline moves it: a heap-only update, planned with no read.
	for i := range int64(4) {
		s.cost(s.write(observe(0, "a", 13+3*i, 5)), cost{fast: 1, updated: 1, heapOnly: 1})
	}
	s.rows(prov(10, 27))
	// Another producer's observation inside the interval, whose deadline is inside it
	// too, changes nothing and writes nothing.
	s.cost(s.write(observe(0, "b", 23, 1)), cost{fast: 1})
	s.rows(prov(10, 27))
	// Another subject's record at a later time passes the deadline: the finalizer closes
	// the row, an update of the range, which no index spares.
	other := engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod2, node1, catalog.ScheduledOn), Producer: "a", EventTime: at(40), Kind: lifecycle.Observe, TTL: 5 * time.Second}
	s.cost(s.write(other), cost{fast: 1, inserted: 1, updated: 1, finalized: 1})
	var closed pgmodel.EdgeRow
	for _, e := range s.m.Tables().Edges {
		if e.Src == pod1 {
			closed = e
		}
	}
	if closed.HasDeadline || closed.Open || closed.End != 27*sec || closed.EndSource != lifecycle.EndLivenessExpiry {
		t.Fatalf("the closed row %+v", closed.Row)
	}
}

// TestWhatTakesTheFastPath is each rule of the fast path, one case at a time: a record
// that could do more than lengthen the last interval is re-folded, and one that cannot
// is not.
func TestWhatTakesTheFastPath(t *testing.T) {
	type step struct {
		r    engine.Record
		want cost
	}
	for _, c := range []struct {
		name  string
		steps []step
		rows  []pgmodel.Row
	}{
		{"an equal deadline is a re-fold, where a tie between sources is broken", []step{
			{observe(0, "a", 10, 10), cost{fast: 1, inserted: 1}},
			{observe(0, "b", 15, 5), cost{slow: 1}},
		}, []pgmodel.Row{prov(10, 20)}},
		{"a deadline that shrinks is a re-fold", []step{
			{observe(0, "a", 10, 10), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 11, 2), cost{slow: 1, updated: 1, heapOnly: 1}},
		}, []pgmodel.Row{prov(10, 13)}},
		{"a delete is a re-fold", []step{
			{observe(0, "a", 10, 10), cost{fast: 1, inserted: 1}},
			{del(0, "a", 12), cost{slow: 1, updated: 1}},
		}, []pgmodel.Row{fin(10, 12, lifecycle.EndProducer)}},
		{"a record earlier than its producer's latest is a re-fold", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 13, 5), cost{fast: 1, updated: 1, heapOnly: 1}},
			{observe(0, "a", 11, 5), cost{slow: 1}},
		}, []pgmodel.Row{prov(10, 18)}},
		{"a record before the last interval starts is a re-fold", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "b", 8, 1), cost{slow: 1, inserted: 1}},
		}, []pgmodel.Row{fin(8, 9, lifecycle.EndLivenessExpiry), prov(10, 15)}},
		{"another producer's record where the interval ends joins them", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "b", 15, 3), cost{fast: 1, updated: 1, heapOnly: 1}},
		}, []pgmodel.Row{prov(10, 18)}},
		{"a record after the interval is a new interval, and the old one is closed", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 20, 5), cost{fast: 1, inserted: 1, updated: 1, finalized: 0}},
		}, []pgmodel.Row{fin(10, 15, lifecycle.EndLivenessExpiry), prov(20, 25)}},
		{"a record with no TTL makes the interval open, and nothing lengthens an open one", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 12, 0), cost{fast: 1, updated: 1, heapOnly: 1}},
			{observe(0, "a", 14, 0), cost{fast: 1}},
			{observe(0, "b", 15, 5), cost{fast: 1}},
		}, []pgmodel.Row{pgmodel.OpenRow(10 * sec)}},
		{"an observation that ends an open interval's producer by a TTL is a re-fold", []step{
			{observe(0, "a", 10, 0), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 12, 5), cost{slow: 1, updated: 1, heapOnly: 1}},
		}, []pgmodel.Row{prov(10, 17)}},
		{"a record at its producer's instant replaces it", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{observe(0, "a", 10, 8), cost{fast: 1, updated: 1, heapOnly: 1}},
		}, []pgmodel.Row{prov(10, 18)}},
		{"a delete at its producer's instant is a re-fold and removes the row", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{del(0, "a", 10), cost{slow: 1, deleted: 1}},
		}, nil},
		{"a record after a delete by its producer is a new row", []step{
			{observe(0, "a", 10, 5), cost{fast: 1, inserted: 1}},
			{del(0, "a", 12), cost{slow: 1, updated: 1}},
			{observe(0, "a", 14, 5), cost{fast: 1, inserted: 1}},
		}, []pgmodel.Row{fin(10, 12, lifecycle.EndProducer), prov(14, 19)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newScripted(t, pgmodel.ModelConfig{})
			for i, st := range c.steps {
				got := s.write(st.r)
				s.t.Helper()
				if g := (cost{got.FastSubjects, got.Refolds, got.RowsInserted, got.RowsUpdated, got.RowsHeapOnly, got.RowsFinalized, got.RowsDeleted}); g != st.want {
					t.Fatalf("step %d: cost %+v, want %+v", i, g, st.want)
				}
			}
			s.rows(c.rows...)
		})
	}
}

// TestAFinalRowIsNotExtendedAndIsProvisionalAgainAfterAReFold: a record that reaches a
// row that was closed reopens it through a re-fold, and the row the re-fold leaves is
// provisional again if its deadline is still ahead, so the next refresh is fast.
func TestAFinalRowIsNotExtendedAndIsProvisionalAgainAfterAReFold(t *testing.T) {
	s := newScripted(t, pgmodel.ModelConfig{})
	s.write(observe(0, "a", 10, 5))
	// Another subject's record at 12 does not close it; one at 15 does.
	filler := func(at int64) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod2, node1, catalog.ScheduledOn), Producer: "z", EventTime: pgmodelAt(at), Kind: lifecycle.Observe, TTL: time.Hour}
	}
	s.write(filler(12))
	s.rows(prov(10, 15), prov(12, 12+3600))
	s.write(filler(16))
	s.rows(fin(10, 15, lifecycle.EndLivenessExpiry), prov(12, 16+3600))
	// b observes at 15, where the closed row ends: the re-fold joins them and the result
	// is provisional (its deadline, 20, is after the newest event time written, 16).
	c := s.write(observe(0, "b", 15, 5))
	s.cost(c, cost{slow: 1, updated: 1})
	s.rows(prov(10, 20), prov(12, 16+3600))
	// The next refresh is on the fast path.
	s.cost(s.write(observe(0, "b", 17, 5)), cost{fast: 1, updated: 1, heapOnly: 1})
}

func pgmodelAt(s int64) time.Time { return time.Unix(s, 0).UTC() }

// TestNaiveWriterRewritesTheRangeOfEveryExtension: the diagnostic writer keeps no
// provisional row, so every extension is an update of the range, which an index sees.
func TestNaiveWriterRewritesTheRangeOfEveryExtension(t *testing.T) {
	s := newScripted(t, pgmodel.ModelConfig{WriterConfig: pgmodel.WriterConfig{Naive: true}})
	s.cost(s.write(observe(0, "a", 10, 5)), cost{fast: 1, inserted: 1})
	s.rows(fin(10, 15, lifecycle.EndLivenessExpiry))
	for i := range int64(3) {
		s.cost(s.write(observe(0, "a", 13+3*i, 5)), cost{fast: 1, updated: 1})
	}
	s.rows(fin(10, 24, lifecycle.EndLivenessExpiry))
	// An open interval is as it is in the skilled writer's tables; reaching it costs the
	// naive writer a range that was bounded.
	s.cost(s.write(observe(0, "a", 20, 0)), cost{fast: 1, updated: 1})
	s.rows(pgmodel.OpenRow(10 * sec))
	// A late delete is a re-fold in either: it splits the interval.
	s.cost(s.write(del(0, "a", 12)), cost{slow: 1, inserted: 1, updated: 1})
	// The two writers' tables are one in every form that is not a deadline.
	for _, e := range s.m.Tables().Edges {
		if e.HasDeadline {
			t.Fatalf("a naive writer kept a provisional row: %+v", e)
		}
	}
}

// checkForm holds the tables to what the stream decides about their form, whatever
// batches it was written in: a row has a deadline only if it is the last row of its
// subject and the deadline is after the newest event time written; a closed last row
// that ended by a deadline after that time has one; and no row has ended at or before the
// horizon, which a retention deletes and a re-fold does not write.
func checkForm(t fataler, tb pgmodel.Tables, newest, horizon int64, naive bool) {
	t.Helper()
	type key struct {
		edge bool
		a, b string
	}
	last := make(map[key]pgmodel.Row)
	var order []key
	note := func(k key, r pgmodel.Row) {
		if prev, ok := last[k]; ok && prev.Start >= r.Start {
			return
		}
		last[k] = r
		order = append(order, k)
	}
	for _, e := range tb.Edges {
		note(key{true, e.Src.String() + string(e.Rel), e.Dst.String()}, e.Row)
	}
	for _, e := range tb.Entities {
		note(key{false, e.Fp.String(), ""}, e.Row)
	}
	check := func(k key, r pgmodel.Row, isLast bool) {
		if r.EndsAtOrBefore(horizon) {
			t.Fatalf("a row %+v that ended at or before the horizon %d is stored", r, horizon)
		}
		if r.HasDeadline {
			switch {
			case naive:
				t.Fatalf("a naive writer's row has a deadline: %+v", r)
			case !isLast:
				t.Fatalf("a row that is not the last of its subject is provisional: %+v", r)
			case r.Deadline <= newest:
				t.Fatalf("a provisional row %+v with a deadline at or before the newest event time %d: the finalizer missed it", r, newest)
			case !r.Open:
				t.Fatalf("a provisional row with a bounded range: %+v", r)
			}
		}
		if !naive && isLast && !r.HasDeadline && !r.Open && r.EndSource == lifecycle.EndLivenessExpiry && r.End > newest {
			t.Fatalf("a closed row %+v that ends after the newest event time %d and is not provisional", r, newest)
		}
	}
	for _, e := range tb.Edges {
		k := key{true, e.Src.String() + string(e.Rel), e.Dst.String()}
		check(k, e.Row, last[k] == e.Row)
	}
	for _, e := range tb.Entities {
		k := key{false, e.Fp.String(), ""}
		check(k, e.Row, last[k] == e.Row)
	}
}

// genHeartbeat draws a record in a stream that is mostly refreshes: few subjects, a
// producer that refreshes ahead of its deadline, and now and then something else (a
// late record, a delete, another producer, a shorter TTL, a different layer of silence).
func genHeartbeat(t *rapid.T, label string, now *int64) engine.Record {
	s := rapid.SampledFrom(subjects).Draw(t, label+" subject")
	r := engine.Record{Layer: catalog.L2, Subject: s}
	r.Producer = rapid.SampledFrom([]lifecycle.Producer{"a", "a", "a", "b"}).Draw(t, label+" producer")
	switch rapid.IntRange(0, 9).Draw(t, label+" shape") {
	case 0: // late
		r.EventTime = time.Unix(0, max(1, *now-rapid.Int64Range(0, 20).Draw(t, label+" late"))*sec).UTC()
	case 1: // a delete
		r.EventTime = time.Unix(0, (*now+rapid.Int64Range(0, 3).Draw(t, label+" dat"))*sec).UTC()
		r.Kind = lifecycle.Delete
		return r
	default:
		*now += rapid.Int64Range(0, 4).Draw(t, label+" step")
		r.EventTime = time.Unix(0, *now*sec).UTC()
	}
	r.Kind = lifecycle.Observe
	if rapid.IntRange(0, 11).Draw(t, label+" open") != 0 {
		r.TTL = time.Duration(rapid.SampledFrom([]int64{1, 3, 5, 5, 5, 8, 12}).Draw(t, label+" ttl")) * time.Second
	}
	if rapid.IntRange(0, 5).Draw(t, label+" run") == 0 {
		r.Through = r.EventTime.Add(time.Duration(rapid.Int64Range(0, 6).Draw(t, label+" through")) * time.Second)
	}
	r.Payload = []byte{byte(rapid.IntRange(0, 3).Draw(t, label+" payload"))}
	return r
}

// TestStoredRowsAreTheFoldOfTheStreamInTheirForm: whatever batches a stream is written
// in, the rows are those of one fold of everything written, each in the form the newest
// event time decides, with a retention anywhere clipping them (a re-fold after one
// restarts a row that spans the horizon, so after a retention only the clipped rows are
// the same). The fast path, the re-fold and the finalizer are all that is between them.
func TestStoredRowsAreTheFoldOfTheStreamInTheirForm(t *testing.T) {
	for _, naive := range []bool{false, true} {
		t.Run(fmt.Sprintf("naive %v", naive), func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				m := pgmodel.NewWith(pgmodel.ModelConfig{WriterConfig: pgmodel.WriterConfig{Naive: naive}})
				var written []engine.Record
				var horizon time.Time
				seq, now := uint64(1), int64(5)
				newest, retained := int64(math.MinInt64), false
				for step := range rapid.IntRange(1, 30).Draw(t, "steps") {
					var batch []engine.Record
					for i := range rapid.IntRange(1, 5).Draw(t, "batch size") {
						r := genHeartbeat(t, fmt.Sprintf("record %d.%d", step, i), &now)
						if r.EventTime.Before(horizon) {
							continue
						}
						r.Seq = seq
						seq++
						batch = append(batch, r)
					}
					if len(batch) > 0 {
						if err := m.Write(batch); err != nil {
							t.Fatal(err)
						}
						written = append(written, batch...)
						for _, r := range batch {
							newest = max(newest, pgmodel.ToNanos(r.EventTime))
						}
					}
					if rapid.IntRange(0, 9).Draw(t, "retain") == 0 {
						h := time.Unix(0, max(1, now-rapid.Int64Range(0, 25).Draw(t, "horizon"))*sec).UTC()
						if err := m.Retain(h); err != nil {
							t.Fatal(err)
						}
						if h.After(horizon) {
							horizon, retained = h, true
						}
					}
					bulk, err := pgmodel.Bulk(written)
					if err != nil {
						t.Fatal(err)
					}
					got := m.Tables()
					checkForm(t, got, newest, nanos(horizon), naive)
					if !retained {
						if want := bulk.AsStored(newest, naive); !got.Identical(want) {
							t.Fatalf("the rows are not one fold of the stream in the form it decides: %s", got.FirstDifference(want))
						}
					} else if g, w := got.Clipped(nanos(horizon)), bulk.Clipped(nanos(horizon)); !g.Equal(w) {
						t.Fatalf("horizon %s: %s", horizon.Format(time.RFC3339), g.FirstDifference(w))
					}
				}
			})
		})
	}
}

// TestAWriterRestoredFromWhatItStoredWritesWhatTheOneThatWroteItWould: the state a
// writer keeps between batches is rebuilt from the rows and the log, as an engine does
// when it opens a database. A writer rebuilt in the middle of a stream and the one that
// wrote it leave the same rows.
func TestAWriterRestoredFromWhatItStoredWritesWhatTheOneThatWroteItWould(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cfg := pgmodel.WriterConfig{}
		a, b := newDriver(t, cfg), newDriver(t, cfg)
		var horizon time.Time
		seq, now := uint64(1), int64(5)
		steps := rapid.IntRange(2, 30).Draw(t, "steps")
		switchAt := rapid.IntRange(1, steps-1).Draw(t, "switch at")
		for step := range steps {
			var batch []engine.Record
			for i := range rapid.IntRange(1, 5).Draw(t, "batch size") {
				r := genHeartbeat(t, fmt.Sprintf("record %d.%d", step, i), &now)
				if r.EventTime.Before(horizon) {
					continue
				}
				r.Seq = seq
				seq++
				batch = append(batch, r)
			}
			if step == switchAt {
				// b is the engine that opened the database a leaves.
				b.restoreFrom(a)
			}
			if len(batch) > 0 {
				a.write(batch)
				if step >= switchAt {
					b.write(batch)
				}
			}
			if rapid.IntRange(0, 9).Draw(t, "retain") == 0 {
				h := time.Unix(0, max(1, now-rapid.Int64Range(0, 25).Draw(t, "horizon"))*sec).UTC()
				a.retain(h)
				if step >= switchAt {
					b.retain(h)
				}
				horizon = time.Unix(0, max(a.horizon, nanos(horizon))).UTC()
			}
			if step >= switchAt {
				if x, y := a.tables(), b.tables(); !x.Identical(y) {
					t.Fatalf("after step %d the restored writer's rows differ: %s", step, x.FirstDifference(y))
				}
			}
		}
	})
}
