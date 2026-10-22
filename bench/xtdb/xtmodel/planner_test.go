package xtmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// plans runs scripted batches through a rig and checks the statements each makes on
// seg_f (seg_r makes the same), then checks the rows the rig holds against the fold.
type script struct {
	t       *testing.T
	r       *rig
	written []engine.Record
	batches int
}

func newScript(t *testing.T) *script { return &script{t: t, r: newRig()} }

// batch writes the records as one batch and expects seg_f statements in the order
// planned (an empty string is no statement), then compares the picture with the fold.
func (s *script) batch(want string, recs ...engine.Record) *Plan {
	s.t.Helper()
	pl := s.r.write(recs...)
	s.written = append(s.written, recs...)
	s.batches++
	if got := statements(pl, SegF); got != want {
		s.t.Errorf("batch %d: seg_f statements %q, want %q", s.batches, got, want)
	}
	if f, r := statements(pl, SegF), statements(pl, SegR); f != r {
		s.t.Errorf("batch %d: seg_r statements %q differ from seg_f's %q", s.batches, r, f)
	}
	s.matchesFold()
	return pl
}

func (s *script) matchesFold() {
	s.t.Helper()
	got := (&Model{img: s.r.img}).existence(SegF, edgeA, view{latest: true})
	want, err := foldExistence(s.written, edgeA, ^uint64(0))
	if err != nil {
		s.t.Fatal(err)
	}
	if len(got) != len(want) {
		s.t.Fatalf("after batch %d: the table has %v, the fold %v", s.batches, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			s.t.Fatalf("after batch %d: the table has %v, the fold %v", s.batches, got, want)
		}
	}
}

func extend(r engine.Record, through int64) engine.Record {
	r.Through = at(through)
	return r
}

// TestStatementsFollowEachShapeOfRecord walks one edge through the cases the planner
// exists for and says what each costs. Times are seconds.
func TestStatementsFollowEachShapeOfRecord(t *testing.T) {
	t.Run("a first observation is one insert", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,15)", observe(1, "a", 10, 5))
	})
	t.Run("a heartbeat before the deadline extends with one insert", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,15)", observe(1, "a", 10, 5))
		s.batch("+[13,18)", observe(2, "a", 13, 5))
		s.batch("+[30,35)", observe(3, "a", 30, 5)) // after the gap: the same
	})
	t.Run("a watch-mode refresh is one insert to the end of time", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("+[20,inf)", observe(2, "a", 20, 0))
	})
	t.Run("a shrinking deadline deletes what the earlier deadline held", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[20,50)", observe(1, "a", 20, 30))
		s.batch("+[25,27) -[27,inf)", observe(2, "a", 25, 2))
	})
	t.Run("a delete after an open observation ends it, and a second one is nothing", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("-[30,inf)", del(2, "a", 30))
		s.batch("", del(3, "a", 40))
		s.batch("+[50,inf)", observe(4, "a", 50, 0))
	})
	t.Run("a late observation is bounded by the next assertion", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("+[50,inf)", observe(2, "a", 50, 0))
		// Late: the next assertion of a is at 50, and a's own deadline is 35.
		s.batch("+[30,35) -[35,50)", observe(3, "a", 30, 5))
	})
	t.Run("a late observation whose deadline passes the next assertion stops at it", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,12)", observe(1, "a", 10, 2))
		s.batch("+[50,55)", observe(2, "a", 50, 5))
		s.batch("+[30,50)", observe(3, "a", 30, 100))
	})
	t.Run("a late delete reaches the next assertion, not the end of time", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("+[50,inf)", observe(2, "a", 50, 0))
		s.batch("-[30,50)", del(3, "a", 30))
	})
	t.Run("a late record before every assertion of its producer", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[40,inf)", observe(1, "a", 40, 0))
		s.batch("+[10,15) -[15,40)", observe(2, "a", 10, 5))
		s.batch("-[5,10)", del(3, "a", 5)) // bounded by the assertion at 10, not the one at 40
	})
	t.Run("a record at an instant its producer already used overwrites it", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,110)", observe(1, "a", 10, 100))
		s.batch("+[10,15) -[15,inf)", observe(2, "a", 10, 5)) // the later write wins
		s.batch("-[10,inf)", del(3, "a", 10))
		s.batch("+[10,inf)", observe(4, "a", 10, 0))
	})
	t.Run("a late overwrite of an instant between two assertions", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("+[30,inf)", observe(2, "a", 30, 0))
		s.batch("+[50,inf)", observe(3, "a", 50, 0))
		s.batch("+[30,35) -[35,50)", observe(4, "a", 30, 5)) // overwrites the one at 30, late
		s.batch("-[30,50)", del(5, "a", 30))
	})
	t.Run("a run extended at its first event time is one insert", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,17)", extend(observe(1, "a", 10, 5), 12)) // a run 10..12, deadline 17
		s.batch("+[10,25)", extend(observe(2, "a", 10, 5), 20)) // the same run, extended to 20
		s.batch("+[10,22) -[22,inf)", extend(observe(3, "a", 10, 5), 17))
	})
	t.Run("other producers have their own documents", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,15)", observe(1, "a", 10, 5))
		s.batch("+[12,20)", observe(2, "b", 12, 8))
		s.batch("-[13,inf)", del(3, "b", 13)) // b's delete leaves a's segment alone
		s.matchesFold()
	})
}

// TestABatchMergesTheStatementsOfADocument: the statements a batch makes about one
// document are the fewest with the same effect, which depends on the order of the
// records.
func TestABatchMergesTheStatementsOfADocument(t *testing.T) {
	t.Run("heartbeats in one batch are one insert", func(t *testing.T) {
		s := newScript(t)
		pl := s.batch("+[10,inf)", observe(1, "a", 10, 0), observe(2, "a", 20, 0), observe(3, "a", 30, 0))
		if pl.Counts.MergedAway != 2 {
			t.Errorf("merged away %d statements, want 2", pl.Counts.MergedAway)
		}
	})
	t.Run("a later record overwrites an earlier one of the batch", func(t *testing.T) {
		s := newScript(t)
		s.batch("", observe(1, "a", 10, 0), del(2, "a", 10)) // nothing stored, nothing to delete
		s.batch("+[10,inf)", del(3, "a", 10), observe(4, "a", 10, 0))
		// The document holds [10, inf), so what two short segments leave out is deleted.
		s.batch("+[20,35) -[25,30) -[35,inf)", observe(5, "a", 20, 5), observe(6, "a", 30, 5))
	})
	t.Run("a late record's bound can be an earlier record of its batch", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[30,35) +[50,inf)", observe(1, "a", 50, 0), observe(2, "a", 30, 5))
	})
	t.Run("a stored document's deletes are kept", func(t *testing.T) {
		s := newScript(t)
		s.batch("+[10,inf)", observe(1, "a", 10, 0))
		s.batch("+[20,25) -[25,inf)", observe(2, "a", 20, 5), observe(3, "a", 22, 3))
	})
	t.Run("the entities of a batch each get a document", func(t *testing.T) {
		r := newRig()
		e := engine.Record{
			Layer: catalog.L2, Subject: engine.EntitySubject(pod1), Producer: "a", EventTime: at(10), Seq: 1,
			Kind: lifecycle.Observe, TTL: 5 * time.Second,
		}
		pl := r.write(e, observe(2, "a", 11, 5))
		if got := statements(pl, Ent); got != "+[10,15)" {
			t.Errorf("ent statements %q", got)
		}
		if got := statements(pl, SegF); got != "+[11,16)" {
			t.Errorf("seg_f statements %q", got)
		}
		if got := statements(pl, LogE); got != "+[0,inf)" {
			t.Errorf("log_e statements %q", got)
		}
	})
}

// TestCountsAreAFunctionOfTheStream: documents written per table, the share of
// records planned from the log, and records with a boundary off a whole microsecond.
func TestCountsAreAFunctionOfTheStream(t *testing.T) {
	r := newRig()
	r.write(observe(1, "a", 10, 0), observe(2, "a", 50, 0))
	pl := r.write(observe(3, "a", 30, 5), del(4, "b", 20))
	c := pl.Counts
	if c.Batches != 1 || c.Records != 2 || c.InOrder != 1 || c.Late != 1 {
		t.Errorf("counts %+v", c)
	}
	// seg: the late insert and its delete, in both directions; nothing for b's delete
	// of a document that does not exist is a statement either: b is fresh, so its
	// delete is merged away.
	if got := c.Tables[SegF]; got != (TableCounts{Inserts: 1, PortionDeletes: 1}) {
		t.Errorf("seg_f %+v", got)
	}
	if c.Tables[SegR] != c.Tables[SegF] {
		t.Errorf("seg_r %+v is not seg_f's %+v", c.Tables[SegR], c.Tables[SegF])
	}
	if got := c.Tables[LogF]; got.Inserts != 2 || c.Tables[LogR].Inserts != 2 || c.Tables[Meta].Inserts != 1 {
		t.Errorf("log_f %+v, log_r %+v, meta %+v", got, c.Tables[LogR], c.Tables[Meta])
	}
	if c.SegmentInserts() != 2 || c.SegmentPortionDeletes() != 2 || c.Statements() != 2+2+2+2+1 {
		t.Errorf("segment inserts %d, deletes %d, statements %d", c.SegmentInserts(), c.SegmentPortionDeletes(), c.Statements())
	}
}

func TestABoundaryOffAWholeMicrosecondIsCounted(t *testing.T) {
	r := newRig()
	whole := observe(1, "a", 10, 5)
	pl := r.write(whole)
	if pl.Counts.OffMicrosecond != 0 {
		t.Fatalf("a record on whole seconds counted %d", pl.Counts.OffMicrosecond)
	}
	for name, mk := range map[string]func(engine.Record) engine.Record{
		"event time":  func(x engine.Record) engine.Record { x.EventTime = x.EventTime.Add(1); return x },
		"deadline":    func(x engine.Record) engine.Record { x.TTL += 1; return x },
		"through":     func(x engine.Record) engine.Record { x.Through = x.EventTime.Add(1500); return x },
		"1 ns before": func(x engine.Record) engine.Record { x.EventTime = x.EventTime.Add(-1); return x },
	} {
		x := mk(observe(2, "b", 20, 5))
		x.Seq = r.n + 10
		if got := r.write(x).Counts.OffMicrosecond; got != 1 {
			t.Errorf("%s: counted %d, want 1", name, got)
		}
	}
	// A watch-mode observation has no deadline to be off.
	if got := r.write(observe(100, "c", 30, 0)).Counts.OffMicrosecond; got != 0 {
		t.Errorf("an open record counted %d", got)
	}
}

// TestAPlannerWithNoMemoryPlansTheSame: what the planner remembers is a function of
// the log, so a planner rebuilt from the log plans the next batch as the one that
// remembered.
func TestAPlannerWithNoMemoryPlansTheSame(t *testing.T) {
	for seed := uint64(1); seed <= seeds(20); seed++ {
		steps := genStream(seedDrawer(seed), 25, false)
		r := newRig()
		for _, st := range steps {
			if len(st.batch) == 0 {
				continue
			}
			rebuilt := r.img.planner()
			if len(rebuilt.state) != len(r.pl.state) {
				t.Fatalf("seed %d: the rebuilt planner knows %d keys, the other %d", seed, len(rebuilt.state), len(r.pl.state))
			}
			for k, v := range r.pl.state {
				if rebuilt.state[k] != v {
					t.Fatalf("seed %d: %v: rebuilt %+v, remembered %+v", seed, k, rebuilt.state[k], v)
				}
			}
			a, err := r.pl.Plan(r.n+1, st.batch)
			if err != nil {
				t.Fatal(err)
			}
			b, err := rebuilt.Plan(r.n+1, st.batch)
			if err != nil {
				t.Fatal(err)
			}
			if len(a.Txn.Ops) != len(b.Txn.Ops) {
				t.Fatalf("seed %d: %d statements against %d", seed, len(a.Txn.Ops), len(b.Txn.Ops))
			}
			for i := range a.Txn.Ops {
				if a.Txn.Ops[i].String() != b.Txn.Ops[i].String() {
					t.Fatalf("seed %d: statement %d: %s against %s", seed, i, a.Txn.Ops[i], b.Txn.Ops[i])
				}
			}
			r.write(st.batch...)
		}
	}
}

// TestPlanningChangesNothingUntilAccepted: a plan that is not accepted, because the
// server refused its transaction, leaves the planner as it was.
func TestPlanningChangesNothingUntilAccepted(t *testing.T) {
	r := newRig()
	r.write(observe(1, "a", 10, 5))
	before := len(r.pl.state)
	st := r.pl.state[segKey{edgeA, "a"}]
	if _, err := r.pl.Plan(2, []engine.Record{observe(2, "a", 12, 5), observe(3, "z", 12, 5)}); err != nil {
		t.Fatal(err)
	}
	if len(r.pl.state) != before || r.pl.state[segKey{edgeA, "a"}] != st {
		t.Fatal("planning changed the planner's memory")
	}
	if _, err := r.pl.Plan(2, nil); err == nil {
		t.Fatal("an empty batch was planned")
	}
}

// TestOffMicrosecondIsThePlannersOwnTest: the test on one record agrees with the planner's
// count on every record of generated streams and of the records the planner's test builds
// for each boundary.
func TestOffMicrosecondIsThePlannersOwnTest(t *testing.T) {
	check := func(name string, recs []engine.Record) {
		t.Helper()
		var want uint64
		for _, r := range recs {
			if OffMicrosecond(r) {
				want++
			}
		}
		pl := newRig().write(recs...)
		if pl.Counts.OffMicrosecond != want {
			t.Errorf("%s: the planner counted %d, OffMicrosecond %d", name, pl.Counts.OffMicrosecond, want)
		}
	}
	var odd []engine.Record
	seq := uint64(0)
	for _, mk := range []func(engine.Record) engine.Record{
		func(x engine.Record) engine.Record { return x },
		func(x engine.Record) engine.Record { x.EventTime = x.EventTime.Add(1); return x },
		func(x engine.Record) engine.Record { x.TTL += 1; return x },
		func(x engine.Record) engine.Record { x.Through = x.EventTime.Add(1500); return x },
		func(x engine.Record) engine.Record { x.EventTime = x.EventTime.Add(-1); return x },
		func(x engine.Record) engine.Record { x.EventTime = x.EventTime.Add(1000); return x },
	} {
		seq++
		odd = append(odd, mk(observe(seq, lifecycle.Producer(fmt.Sprintf("p%d", seq)), 20+int64(seq), 5)))
	}
	odd = append(odd, observe(100, "open", 30, 0), del(101, "open", 40))
	check("boundaries", odd)
	for seed := uint64(1); seed <= seeds(10); seed++ {
		for _, st := range genStream(seedDrawer(seed), 25, true) {
			if len(st.batch) > 0 {
				check("generated", st.batch)
			}
		}
	}
	if OffMicrosecond(observe(1, "a", 10, 5)) || !OffMicrosecond(engine.Record{EventTime: time.Unix(1, 1).UTC(), Kind: lifecycle.Observe}) {
		t.Error("whole seconds are off a microsecond, or a nanosecond is on one")
	}
}
