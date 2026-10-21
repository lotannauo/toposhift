package pgmodel_test

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// driver is a writer that keeps the log, the baselines and the rows itself and uses the
// planner only through its exported surface, as an engine over a database does: it
// finds the start of the interval a batch lands in from its rows, hands [pgmodel.Refold]
// the rows and the records from there, and applies the changes it is given by key.
type driver struct {
	t        fataler
	subjects map[engine.Subject]*driven
	horizon  int64
	counts   pgmodel.Counts
}

type driven struct {
	layer catalog.Layer
	log   []pgmodel.LogRow // ascending by (event time, seq)
	base  []baseline
	rows  []pgmodel.Row // ascending by start
}

type baseline struct {
	pgmodel.LogRow
}

// fataler is what the driver needs of a *testing.T or a *rapid.T.
type fataler interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func newDriver(t fataler) *driver {
	return &driver{t: t, subjects: make(map[engine.Subject]*driven)}
}

func lessLog(a, b pgmodel.LogRow) int {
	return cmp.Or(cmp.Compare(a.Event, b.Event), cmp.Compare(a.Seq, b.Seq))
}

func (d *driver) write(batch []engine.Record) {
	d.t.Helper()
	fresh := make(map[engine.Subject][]engine.Record)
	var order []engine.Subject
	for _, r := range batch {
		if _, ok := fresh[r.Subject]; !ok {
			order = append(order, r.Subject)
		}
		fresh[r.Subject] = append(fresh[r.Subject], r)
	}
	for _, s := range order {
		recs := fresh[s]
		st := d.subjects[s]
		if st == nil {
			st = &driven{layer: recs[0].Layer}
			d.subjects[s] = st
		}
		earliest := int64(math.MaxInt64)
		var news []pgmodel.LogRow
		for _, r := range recs {
			l := pgmodel.LogRowOf(r)
			earliest = min(earliest, l.Event)
			news = append(news, l)
		}
		// The start of the last row that begins at or before the earliest new event.
		from := sort.Search(len(st.rows), func(i int) bool { return st.rows[i].Start > earliest }) - 1
		start := int64(math.MinInt64)
		if from >= 0 {
			start = st.rows[from].Start
		} else {
			from = 0
		}
		var input []pgmodel.LogRow
		for _, b := range st.base {
			if b.Event >= start {
				input = append(input, b.LogRow)
			}
		}
		for _, l := range st.log {
			if l.Event >= start {
				input = append(input, l)
			}
		}
		input = append(input, news...)
		// The planner takes its input in any order.
		slices.Reverse(input)

		old := slices.Clone(st.rows[from:])
		want, ch, err := pgmodel.Refold(old, input, start, d.horizon)
		if err != nil {
			d.t.Fatal(err)
		}
		// Applying the changes by key to the rows from start on gives the rows it returned.
		got := applyChanges(d.t, old, ch)
		if !slices.Equal(got, want) {
			d.t.Fatalf("applying the changes to %v gives %v, the planner returned %v", old, got, want)
		}
		if !slices.IsSortedFunc(want, func(a, b pgmodel.Row) int { return cmp.Compare(a.Start, b.Start) }) {
			d.t.Fatalf("rows out of order: %v", want)
		}
		st.rows = append(slices.Clone(st.rows[:from]), want...)
		st.log = append(st.log, news...)
		slices.SortFunc(st.log, lessLog)
		d.counts.RowsInserted += uint64(len(ch.Inserted))
		d.counts.RowsUpdated += uint64(len(ch.Updated))
		d.counts.RowsDeleted += uint64(len(ch.Deleted))
	}
}

// applyChanges applies deletes, then updates, then inserts, each by start.
func applyChanges(t fataler, rows []pgmodel.Row, ch pgmodel.Changes) []pgmodel.Row {
	t.Helper()
	out := slices.Clone(rows)
	find := func(start int64) int {
		for i, r := range out {
			if r.Start == start {
				return i
			}
		}
		return -1
	}
	for _, s := range ch.Deleted {
		i := find(s)
		if i < 0 {
			t.Fatalf("a delete of start %d, which is not stored", s)
		}
		out = slices.Delete(out, i, i+1)
	}
	for _, r := range ch.Updated {
		i := find(r.Start)
		if i < 0 || out[i] == r {
			t.Fatalf("an update of start %d which is %s", r.Start, map[bool]string{true: "not stored", false: "unchanged"}[i < 0])
		}
		out[i] = r
	}
	for _, r := range ch.Inserted {
		if find(r.Start) >= 0 {
			t.Fatalf("an insert of start %d, which is stored", r.Start)
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b pgmodel.Row) int { return cmp.Compare(a.Start, b.Start) })
	return out
}

func (d *driver) retain(horizon time.Time) {
	h := pgmodel.ToNanos(horizon)
	if h <= d.horizon {
		return
	}
	d.horizon = h
	for _, st := range d.subjects {
		last := make(map[lifecycle.Producer]pgmodel.LogRow)
		var all []pgmodel.LogRow
		for _, b := range st.base {
			all = append(all, b.LogRow)
		}
		all = append(all, st.log...)
		for _, l := range all {
			if l.Event < h {
				if prev, ok := last[l.Producer]; !ok || lessLog(prev, l) < 0 {
					last[l.Producer] = l
				}
			}
		}
		if len(last) > 0 {
			st.base = st.base[:0]
			for _, l := range last {
				st.base = append(st.base, baseline{l})
			}
			slices.SortFunc(st.base, func(a, b baseline) int { return cmp.Compare(a.Producer, b.Producer) })
		}
		st.log = slices.DeleteFunc(st.log, func(l pgmodel.LogRow) bool { return l.Event < h })
		n := len(st.rows)
		st.rows = slices.DeleteFunc(st.rows, func(r pgmodel.Row) bool { return r.EndsAtOrBefore(h) })
		d.counts.RowsDeleted += uint64(n - len(st.rows))
	}
}

func (d *driver) tables() pgmodel.Tables {
	var t pgmodel.Tables
	for s, st := range d.subjects {
		for _, r := range st.rows {
			if s.Kind == engine.SubjectEdge {
				t.Edges = append(t.Edges, pgmodel.EdgeRow{Src: s.A, Dst: s.B, Rel: s.Relation, Layer: st.layer, Row: r})
			} else {
				t.Entities = append(t.Entities, pgmodel.EntityRow{Fp: s.A, Layer: st.layer, Row: r})
			}
		}
	}
	slices.SortFunc(t.Edges, func(a, b pgmodel.EdgeRow) int {
		return cmp.Or(engine.CompareFingerprints(a.Src, b.Src), engine.CompareFingerprints(a.Dst, b.Dst), cmp.Compare(a.Rel, b.Rel), cmp.Compare(a.Start, b.Start))
	})
	slices.SortFunc(t.Entities, func(a, b pgmodel.EntityRow) int {
		return cmp.Or(engine.CompareFingerprints(a.Fp, b.Fp), cmp.Compare(a.Start, b.Start))
	})
	return t
}

// aliveAsOf is a read as of a token by a reader of the log: the records of the subject
// the token saw, baselines among them, folded.
func (d *driver) aliveAsOf(s engine.Subject, t int64, asOf uint64) bool {
	d.t.Helper()
	st := d.subjects[s]
	if st == nil {
		return false
	}
	var seen []pgmodel.LogRow
	for _, b := range st.base {
		if b.Seq <= asOf {
			seen = append(seen, b.LogRow)
		}
	}
	for _, l := range st.log {
		if l.Seq <= asOf {
			seen = append(seen, l)
		}
	}
	alive, err := pgmodel.AliveAt(seen, t)
	if err != nil {
		d.t.Fatal(err)
	}
	return alive
}

// TestAWriterThatUsesOnlyTheExportedPlannerMakesTheModelsRows: the SQL engine does what
// this driver does, so what it relies on is checked with no server. Fed random streams,
// with late events, deletes, runs and retentions, the driver's rows and row changes are
// the model's, after every batch, and a read as of an earlier token that folds the log
// with AliveAt is the model's.
func TestAWriterThatUsesOnlyTheExportedPlannerMakesTheModelsRows(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m, d := pgmodel.New(), newDriver(t)
		seq := uint64(1)
		var horizon time.Time
		var floor uint64 // the last seq when the horizon last moved: a token below it is not asked about
		for step := range rapid.IntRange(1, 25).Draw(t, "steps") {
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
				d.write(batch)
			}
			if rapid.IntRange(0, 4).Draw(t, "retain") == 0 {
				h := time.Unix(0, (1+rapid.Int64Range(0, 64).Draw(t, "horizon"))*sec).UTC()
				if err := m.Retain(h); err != nil {
					t.Fatal(err)
				}
				d.retain(h)
				if h.After(horizon) {
					horizon, floor = h, seq-1
				}
			}
			if got, want := d.tables(), m.Tables(); !got.Equal(want) {
				t.Fatalf("after step %d the driver's rows differ from the model's: %s", step, got.FirstDifference(want))
			}
			c := m.Counts()
			if d.counts.RowsInserted != c.RowsInserted || d.counts.RowsUpdated != c.RowsUpdated || d.counts.RowsDeleted != c.RowsDeleted {
				t.Fatalf("after step %d the driver changed %+v rows, the model %+v", step, d.counts, c)
			}
			if seq > 1 {
				for _, s := range subjects[:2] {
					for _, asOf := range []uint64{max(seq/2, floor), seq - 1, engine.Latest} {
						// After a retention only instants at or after the horizon are asked
						// about, and only tokens at or above the floor.
						at := max((1+rapid.Int64Range(0, 64).Draw(t, "instant"))*sec, nanos(horizon))
						want, err := m.Alive(s.A, time.Unix(0, at).UTC(), engine.Scope{Layer: catalog.L2, AsOf: asOf})
						if err != nil {
							t.Fatal(err)
						}
						if got := d.aliveAsOf(s, at, asOf); got != want {
							t.Fatalf("alive at %d as of seq %d: %v, the model says %v", at/sec, asOf, got, want)
						}
					}
				}
			}
		}
	})
}

func TestRefoldOfNothingChangesNothing(t *testing.T) {
	rows, ch, err := pgmodel.Refold(nil, nil, math.MinInt64, 0)
	if err != nil || len(rows) != 0 || !ch.Empty() {
		t.Errorf("%v %+v %v", rows, ch, err)
	}
	// A refold of rows that are already right is no change, however the records are
	// ordered.
	l1 := pgmodel.LogRow{Producer: "a", Seq: 1, Event: 10 * sec, TTL: 5 * time.Second, Kind: lifecycle.Observe}
	l2 := pgmodel.LogRow{Producer: "a", Seq: 2, Event: 13 * sec, TTL: 5 * time.Second, Kind: lifecycle.Observe}
	for _, in := range [][]pgmodel.LogRow{{l1, l2}, {l2, l1}} {
		rows, ch, err := pgmodel.Refold(nil, in, math.MinInt64, 0)
		if err != nil || len(rows) != 1 || len(ch.Inserted) != 1 || ch.Inserted[0].Start != 10*sec {
			t.Fatalf("%v %+v %v", rows, ch, err)
		}
		if _, again, err := pgmodel.Refold(rows, in, math.MinInt64, 0); err != nil || !again.Empty() {
			t.Errorf("a second refold changed %+v (%v)", again, err)
		}
		// Past its horizon the row is neither kept nor inserted.
		if rows, ch, err := pgmodel.Refold(nil, in, math.MinInt64, 18*sec); err != nil || len(rows) != 0 || !ch.Empty() {
			t.Errorf("a row that ended before the horizon: %v %+v %v", rows, ch, err)
		}
	}
}
