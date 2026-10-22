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
// plans a batch, gives each slow subject the rows and the log it asks for, applies the
// changes it is handed to rows it finds by where the plan says they are (a ctid, in a
// database) and tells the plan where each row it wrote went. A change that names a row
// by a place where that row is not is a failure, so the places the planner carries
// between batches are checked here, with no server.
type driver struct {
	t        fataler
	w        *pgmodel.Writer
	subjects map[engine.Subject]*driven
	horizon  int64
	next     uint64
	counts   pgmodel.Counts
}

type driven struct {
	layer catalog.Layer
	log   []pgmodel.LogRow // ascending by (event time, seq)
	base  []pgmodel.LogRow
	rows  []placed // ascending by start
}

// placed is a stored row and its place.
type placed struct {
	pgmodel.Row
	loc pgmodel.Loc
}

// fataler is what the driver needs of a *testing.T or a *rapid.T.
type fataler interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func newDriver(t fataler, cfg pgmodel.WriterConfig) *driver {
	return &driver{t: t, w: pgmodel.NewWriter(cfg), subjects: make(map[engine.Subject]*driven)}
}

func lessLog(a, b pgmodel.LogRow) int {
	return cmp.Or(cmp.Compare(a.Event, b.Event), cmp.Compare(a.Seq, b.Seq))
}

// place makes a place no other row has had.
func (d *driver) place() pgmodel.Loc {
	d.next++
	var l pgmodel.Loc
	for i := range 6 {
		l[i] = byte(d.next >> (8 * i))
	}
	return l
}

func (d *driver) write(batch []engine.Record) {
	d.t.Helper()
	plan, err := d.w.Plan(batch)
	if err != nil {
		d.t.Fatal(err)
	}
	for _, s := range plan.Slow() {
		st := d.subjects[s]
		var old []pgmodel.Stored
		var input []pgmodel.LogRow
		if st != nil {
			// The rows from the last that begins at or before the earliest new event.
			earliest := plan.Earliest(s)
			from := sort.Search(len(st.rows), func(i int) bool { return st.rows[i].Start > earliest }) - 1
			start := int64(math.MinInt64)
			if from >= 0 {
				start = st.rows[from].Start
			} else {
				from = 0
			}
			for _, r := range st.rows[from:] {
				old = append(old, pgmodel.Stored{Row: r.Row, Loc: r.loc})
			}
			for _, b := range st.base {
				if b.Event >= start {
					input = append(input, b)
				}
			}
			for _, l := range st.log {
				if l.Event >= start {
					input = append(input, l)
				}
			}
			// The planner takes its input in any order.
			slices.Reverse(input)
		}
		if err := plan.Resolve(s, old, input); err != nil {
			d.t.Fatal(err)
		}
	}
	changes, err := plan.Finish()
	if err != nil {
		d.t.Fatal(err)
	}
	for _, r := range batch {
		st := d.subjects[r.Subject]
		if st == nil {
			st = &driven{layer: r.Layer}
			d.subjects[r.Subject] = st
		}
		st.log = append(st.log, pgmodel.LogRowOf(r))
		slices.SortFunc(st.log, lessLog)
	}
	type where struct {
		s     engine.Subject
		start int64
		loc   pgmodel.Loc
	}
	var wrote []where
	for _, sc := range changes {
		st := d.subjects[sc.Subject]
		if st == nil {
			d.t.Fatalf("changes for a subject that has no records: %v", sc.Subject.A)
		}
		find := func(c pgmodel.RowChange) int {
			i := slices.IndexFunc(st.rows, func(r placed) bool { return r.Start == c.Old.Start })
			if i < 0 {
				d.t.Fatalf("a %d of the row at %d, which is not stored: %+v", c.Kind, c.Old.Start, c)
			}
			if st.rows[i].loc != c.Old.Loc || st.rows[i].Row != c.Old.Row {
				d.t.Fatalf("a %d of the row at %d by the place %v and the row %+v; the row there is %v, %+v", c.Kind, c.Old.Start, c.Old.Loc, c.Old.Row, st.rows[i].loc, st.rows[i].Row)
			}
			return i
		}
		for _, c := range sc.Rows {
			if c.Kind == pgmodel.Delete {
				st.rows = slices.Delete(st.rows, find(c), find(c)+1)
			}
		}
		for _, c := range sc.Rows {
			switch c.Kind {
			case pgmodel.Update:
				i := find(c)
				if st.rows[i].Row == c.New || c.New.Start != c.Old.Start {
					d.t.Fatalf("an update that changes nothing or the start: %+v", c)
				}
				if c.Extension && (!c.HeapOnly || !c.Old.HasDeadline || !c.New.HasDeadline) {
					d.t.Fatalf("an extension that is not a move of a deadline: %+v", c)
				}
				st.rows[i] = placed{Row: c.New, loc: d.place()}
				wrote = append(wrote, where{sc.Subject, c.New.Start, st.rows[i].loc})
			case pgmodel.Insert:
				if slices.ContainsFunc(st.rows, func(r placed) bool { return r.Start == c.New.Start }) {
					d.t.Fatalf("an insert of start %d, which is stored", c.New.Start)
				}
				st.rows = append(st.rows, placed{Row: c.New, loc: d.place()})
				wrote = append(wrote, where{sc.Subject, c.New.Start, st.rows[len(st.rows)-1].loc})
			}
		}
		slices.SortFunc(st.rows, func(a, b placed) int { return cmp.Compare(a.Start, b.Start) })
	}
	d.counts.Records += uint64(len(batch))
	c := plan.Counts()
	c.Records = 0
	d.add(c)
	plan.Commit()
	for _, w := range wrote {
		d.w.SetLoc(w.s, w.start, w.loc)
	}
	// The rows the writer remembers are rows that are stored, where it says.
	for s, st := range d.subjects {
		if last, ok := d.w.LastRow(s); ok {
			if len(st.rows) == 0 || st.rows[len(st.rows)-1].Row != last.Row || st.rows[len(st.rows)-1].loc != last.Loc {
				d.t.Fatalf("the writer remembers %+v at %v as the last row of a subject whose rows are %+v", last.Row, last.Loc, st.rows)
			}
		}
	}
}

// restoreFrom makes d a driver that has opened the database of another: it has its rows
// and its log, and its writer is rebuilt from them with the last row of each subject
// and the latest record of each producer, as an engine rebuilds it.
func (d *driver) restoreFrom(o *driver) {
	d.subjects = make(map[engine.Subject]*driven)
	d.horizon, d.next = o.horizon, o.next
	d.w = pgmodel.NewWriter(pgmodel.WriterConfig{})
	newest := int64(math.MinInt64)
	for s, st := range o.subjects {
		c := &driven{layer: st.layer, log: slices.Clone(st.log), base: slices.Clone(st.base), rows: slices.Clone(st.rows)}
		d.subjects[s] = c
		latest := make(map[lifecycle.Producer]pgmodel.LogRow)
		for _, l := range slices.Concat(st.base, st.log) {
			newest = max(newest, l.Event)
			if prev, ok := latest[l.Producer]; !ok || lessLog(prev, l) < 0 {
				latest[l.Producer] = l
			}
		}
		var rows []pgmodel.LogRow
		for _, l := range latest {
			rows = append(rows, l)
		}
		var last *pgmodel.Stored
		if n := len(c.rows); n > 0 {
			last = &pgmodel.Stored{Row: c.rows[n-1].Row, Loc: c.rows[n-1].loc}
		}
		d.w.Restore(s, st.layer, last, rows)
	}
	d.w.SetState(d.horizon, newest)
}

func (d *driver) add(c pgmodel.Counts) {
	d.counts.FastSubjects += c.FastSubjects
	d.counts.Refolds += c.Refolds
	d.counts.RecordsFolded += c.RecordsFolded
	d.counts.RowsInserted += c.RowsInserted
	d.counts.RowsUpdated += c.RowsUpdated
	d.counts.RowsDeleted += c.RowsDeleted
	d.counts.RowsHeapOnly += c.RowsHeapOnly
	d.counts.RowsFinalized += c.RowsFinalized
}

func (d *driver) retain(horizon time.Time) {
	h := pgmodel.ToNanos(horizon)
	if h <= d.horizon {
		return
	}
	d.horizon = h
	// The provisional rows the horizon removes are the ones the plan names, by place.
	plan := d.w.PlanRetain(h)
	named := make(map[pgmodel.Loc]bool)
	for _, l := range plan.Deleted {
		if !l.HasDeadline {
			d.t.Fatalf("a retention names a row without a deadline: %+v", l)
		}
		named[l.Loc] = true
	}
	for _, st := range d.subjects {
		last := make(map[lifecycle.Producer]pgmodel.LogRow)
		all := slices.Concat(st.base, st.log)
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
				st.base = append(st.base, l)
			}
			slices.SortFunc(st.base, func(a, b pgmodel.LogRow) int { return cmp.Compare(a.Producer, b.Producer) })
		}
		st.log = slices.DeleteFunc(st.log, func(l pgmodel.LogRow) bool { return l.Event < h })
		n := len(st.rows)
		st.rows = slices.DeleteFunc(st.rows, func(r placed) bool {
			if !r.EndsAtOrBefore(h) {
				return false
			}
			if r.HasDeadline {
				if !named[r.loc] {
					d.t.Fatalf("a provisional row at %v that the horizon removes and the plan does not name", r.loc)
				}
				delete(named, r.loc)
			}
			return true
		})
		d.counts.RowsDeleted += uint64(n - len(st.rows))
	}
	if len(named) > 0 {
		d.t.Fatalf("a retention names %d rows that are not stored", len(named))
	}
	d.w.CommitRetain(h)
}

func (d *driver) tables() pgmodel.Tables {
	var t pgmodel.Tables
	for s, st := range d.subjects {
		for _, r := range st.rows {
			if s.Kind == engine.SubjectEdge {
				t.Edges = append(t.Edges, pgmodel.EdgeRow{Src: s.A, Dst: s.B, Rel: s.Relation, Layer: st.layer, Row: r.Row})
			} else {
				t.Entities = append(t.Entities, pgmodel.EntityRow{Fp: s.A, Layer: st.layer, Row: r.Row})
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
			seen = append(seen, b)
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
// the model's, in the same form, after every batch, whatever the places it was told; and a
// read as of an earlier token that folds the log with AliveAt is the model's.
func TestAWriterThatUsesOnlyTheExportedPlannerMakesTheModelsRows(t *testing.T) {
	for _, cfg := range []pgmodel.WriterConfig{{}, {Naive: true}} {
		t.Run(fmt.Sprintf("naive %v", cfg.Naive), func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				m, d := pgmodel.NewWith(pgmodel.ModelConfig{WriterConfig: cfg}), newDriver(t, cfg)
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
					if got, want := d.tables(), m.Tables(); !got.Identical(want) {
						t.Fatalf("after step %d the driver's rows differ from the model's: %s", step, got.FirstDifference(want))
					}
					c, dc := m.Counts(), d.counts
					if dc.Records != c.Records || dc.FastSubjects != c.FastSubjects || dc.Refolds != c.Refolds || dc.RecordsFolded != c.RecordsFolded ||
						dc.RowsInserted != c.RowsInserted || dc.RowsUpdated != c.RowsUpdated || dc.RowsDeleted != c.RowsDeleted ||
						dc.RowsHeapOnly != c.RowsHeapOnly || dc.RowsFinalized != c.RowsFinalized {
						t.Fatalf("after step %d the driver counts %+v, the model %+v", step, dc, c)
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
		})
	}
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
