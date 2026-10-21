package xtmodel

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func write(t *testing.T, m *Model, rs ...engine.Record) {
	t.Helper()
	if err := m.Write(rs); err != nil {
		t.Fatal(err)
	}
}

// TestTheSystemTimeIsTheOrdinalAndAReopenedModelRebuildsItFromTheMetaDocuments: the
// n-th committed batch is at the base plus n microseconds; a model opened again over
// what the first left has the same last token, the same ordinals and the same planner
// memory, and its next batch takes the next ordinal.
func TestTheSystemTimeIsTheOrdinalAndAReopenedModelRebuildsItFromTheMetaDocuments(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { Forget(dir) })
	m := Open(dir)
	write(t, m, observe(1, "a", 10, 0), observe(2, "a", 20, 5))
	write(t, m, observe(1<<62, "b", 30, 5)) // sequence numbers near 2^63 are no clock
	write(t, m, del(1<<62+9, "a", 40))
	if want := BaseSystemTime + 3*1000; SystemTime(3) != want {
		t.Fatalf("system time of the third batch %d, want %d", SystemTime(3), want)
	}
	for n, last := range []uint64{2, 1 << 62, 1<<62 + 9} {
		got, ok := m.SystemTimeOf(last)
		if !ok || got != SystemTime(uint64(n)+1) {
			t.Errorf("batch %d ended at token %d: system time %d, %v", n+1, last, got, ok)
		}
	}
	if _, ok := m.SystemTimeOf(1); ok {
		t.Error("a token inside a batch has a system time")
	}

	re := Open(dir)
	if re.LastSeq() != m.LastSeq() || re.batches != m.batches || len(re.tokens) != len(m.tokens) {
		t.Fatalf("reopened: last seq %d, %d batches, %d tokens; before: %d, %d, %d", re.LastSeq(), re.batches, len(re.tokens), m.LastSeq(), m.batches, len(m.tokens))
	}
	for tok, n := range m.tokens {
		if re.tokens[tok] != n {
			t.Errorf("token %d: ordinal %d after reopening, %d before", tok, re.tokens[tok], n)
		}
	}
	if len(re.pl.state) != len(m.pl.state) {
		t.Fatalf("the reopened planner knows %d keys, the first %d", len(re.pl.state), len(m.pl.state))
	}
	write(t, re, observe(1<<62+10, "a", 50, 0))
	if d := re.img.metas[len(re.img.metas)-1]; d.n != 4 || d.sys != SystemTime(4) {
		t.Errorf("the batch after a reopening: ordinal %d at %d", d.n, d.sys)
	}
	// A record at the sequence number already written is refused, as before.
	if err := re.Write([]engine.Record{observe(1<<62+10, "a", 60, 0)}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a repeated seq: %v", err)
	}
}

// TestAReadAtATokenInsideABatchIsAClientFold: tokens that ended a batch, and the newest,
// are read at a system time; any other token is answered from the log.
func TestAReadAtATokenInsideABatchIsAClientFold(t *testing.T) {
	m := New()
	write(t, m, observe(1, "a", 10, 0), observe(2, "b", 20, 0), observe(3, "c", 30, 0))
	write(t, m, observe(5, "a", 40, 0), del(6, "b", 41))
	count := func(tok uint64) ReadCounts {
		before := m.ReadCounts()
		if _, err := m.Neighbors(pod1, engine.Forward, at(50), engine.Scope{Layer: catalog.L2, AsOf: tok}); err != nil {
			t.Fatal(err)
		}
		after := m.ReadCounts()
		return ReadCounts{Native: after.Native - before.Native, ClientFold: after.ClientFold - before.ClientFold}
	}
	for tok, want := range map[uint64]ReadCounts{
		3:             {Native: 1}, // the first batch's end
		6:             {Native: 1}, // the newest
		engine.Latest: {Native: 1},
		1:             {ClientFold: 1}, // inside the first batch
		2:             {ClientFold: 1},
		4:             {ClientFold: 1}, // between two batches, which no system time names
		5:             {ClientFold: 1}, // inside the second
		0:             {ClientFold: 1}, // before everything
	} {
		if got := count(tok); got != want {
			t.Errorf("token %d: %+v, want %+v", tok, got, want)
		}
	}
}

// TestRefusedBatchesChangeNothing: every way a write is refused leaves the ordinals,
// the token, the planner and the log as they were; an empty batch is no transaction.
func TestRefusedBatchesChangeNothing(t *testing.T) {
	m := New()
	write(t, m, observe(1, "a", 10, 5))
	if err := m.Retain(at(5)); err != nil {
		t.Fatal(err)
	}
	other := observe(5, "a", 12, 5)
	other.Layer = catalog.L1
	bad := observe(5, "a", 12, 5)
	bad.TTL = -1
	for name, c := range map[string]struct {
		batch []engine.Record
		is    error
	}{
		"an invalid record after a good one":  {[]engine.Record{observe(2, "a", 12, 5), bad}, engine.ErrInvalid},
		"a seq not above the last":            {[]engine.Record{observe(1, "a", 12, 5)}, engine.ErrInvalid},
		"seqs that do not ascend":             {[]engine.Record{observe(3, "a", 12, 5), observe(2, "a", 13, 5)}, engine.ErrInvalid},
		"a subject in another layer":          {[]engine.Record{other}, engine.ErrInvalid},
		"a record before the horizon":         {[]engine.Record{observe(2, "a", 12, 5), observe(3, "a", 4, 5)}, engine.ErrBeforeHorizon},
		"a record before the horizon, second": {[]engine.Record{observe(2, "a", 4, 5)}, engine.ErrBeforeHorizon},
	} {
		if err := m.Write(c.batch); !errors.Is(err, c.is) {
			t.Errorf("%s: %v", name, err)
		}
		if m.LastSeq() != 1 || m.batches != 1 || len(m.img.metas) != 1 || m.Counts().Records != 1 || len(m.img.logs[LogF][pod1]) != 1 {
			t.Fatalf("%s changed the model", name)
		}
	}
	if err := m.Write(nil); err != nil || m.batches != 1 || len(m.img.metas) != 1 {
		t.Fatalf("an empty batch: %v, %d batches", err, m.batches)
	}
	write(t, m, observe(2, "a", 12, 5))
	if m.img.metas[1].sys != SystemTime(2) {
		t.Fatalf("the batch after refusals is at system time %d", m.img.metas[1].sys)
	}
}

// TestRetainRecordsTheHorizonAndFreesNothing: XTDB's only removal erases a document for
// all time, so retention is not mapped; the horizon is kept across a reopening.
func TestRetainRecordsTheHorizonAndFreesNothing(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { Forget(dir) })
	m := Open(dir)
	write(t, m, observe(1, "a", 10, 5), observe(2, "a", 30, 5), observe(3, "b", 50, 0))
	before, _ := m.Size()
	docs := len(m.img.docs)
	if err := m.Retain(at(40)); err != nil {
		t.Fatal(err)
	}
	if after, _ := m.Size(); after != before || len(m.img.docs) != docs {
		t.Fatalf("retention changed what is held: %d bytes, %d documents (before %d, %d)", after, len(m.img.docs), before, docs)
	}
	// Earlier horizons change nothing; the horizon only moves forward.
	if err := m.Retain(at(20)); err != nil {
		t.Fatal(err)
	}
	if err := Open(dir).Write([]engine.Record{observe(4, "a", 30, 5)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Fatalf("a record at 30 after a horizon of 40, on a reopened model: %v", err)
	}
	if err := m.Write([]engine.Record{observe(4, "a", 40, 5)}); err != nil {
		t.Fatalf("a record at the horizon: %v", err)
	}
	// A horizon past the last representable instant refuses everything.
	if err := m.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.Write([]engine.Record{observe(5, "a", 50, 5)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Fatalf("a record after a horizon past the end: %v", err)
	}
}

// TestASegmentThatEndsAtTheLastInstantIsNotOpen: Forever is above the last
// representable instant, so a deadline there is a closed end, and a read after the end
// of the range sees the state after every record.
func TestASegmentThatEndsAtTheLastInstantIsNotOpen(t *testing.T) {
	last := engine.MaxEventTime
	rec := func(seq uint64, p lifecycle.Producer, e time.Time, ttl time.Duration) engine.Record {
		r := observe(seq, p, 0, 0)
		r.EventTime, r.TTL = e, ttl
		return r
	}
	c := newChecker(New())
	step1 := step{batch: []engine.Record{rec(1, "a", last.Add(-5*time.Second), 5*time.Second)}}
	if err := c.do(step1); err != nil {
		t.Fatal(err)
	}
	// a's deadline is the last instant exactly: it is alive just before and gone at it
	// and after, which would not be so if that end were the end of time.
	for at, want := range map[time.Time]int{last.Add(-time.Second): 1, last: 0, last.Add(time.Hour): 0} {
		ns, err := c.m.Neighbors(pod1, engine.Forward, at, engine.Current(catalog.L2))
		if err != nil || len(ns) != want {
			t.Errorf("at %s: %v (%v), want %d", at.Format(time.RFC3339Nano), ns, err, want)
		}
	}
	// An open record at the last instant is alive after the range.
	for i, st := range []step{
		{batch: []engine.Record{rec(2, "b", last.Add(-2*time.Second), 0)}},
		{batch: []engine.Record{rec(3, "c", last, 0), del(4, "b", 5)}},
	} {
		if err := c.do(st); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if ns, err := c.m.Neighbors(pod1, engine.Forward, last.Add(time.Hour), engine.Current(catalog.L2)); err != nil || len(ns) != 1 {
		t.Errorf("after the range: %v (%v)", ns, err)
	}
}
