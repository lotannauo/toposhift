package xtmodel

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

var testKey = Key{Table: SegF, Subject: edgeA, Producer: "a"}

func seg(kind OpKind, from, to Instant) Op {
	return Op{Kind: kind, Key: testKey, From: from, To: to, Layer: 2}
}

// TestADocumentKeepsItsHistoryInSystemTime: an insert over a stretch of valid time
// overwrites that stretch and leaves the rest; a read at a system time sees the
// document as the database knew it then.
func TestADocumentKeepsItsHistoryInSystemTime(t *testing.T) {
	im := newImage()
	commit := func(n uint64, ops ...Op) {
		t.Helper()
		if err := im.apply(Txn{System: SystemTime(n), Ops: ops}); err != nil {
			t.Fatal(err)
		}
	}
	commit(1, seg(Insert, 10, 20))
	commit(2, seg(Insert, 15, 30)) // overwrites [15, 20), extends
	commit(3, seg(DeletePortion, 12, 17))
	commit(4, seg(DeletePortion, 100, 200)) // nothing there
	d := im.docs[testKey]

	at := func(t Instant, n uint64) bool {
		v := view{sys: SystemTime(n)}
		if n == 0 {
			v = view{latest: true}
		}
		return d.at(t, v)
	}
	for _, c := range []struct {
		t Instant
		n uint64
		w bool
	}{
		// Before the first transaction nothing was known.
		{12, 0 + 1, true},
		{9, 1, false},
		{20, 1, false},
		{25, 1, false},
		// After the second: [10, 30).
		{10, 2, true},
		{29, 2, true},
		{30, 2, false},
		{9, 2, false},
		// After the third: [10, 12) and [17, 30).
		{11, 3, true},
		{12, 3, false},
		{16, 3, false},
		{17, 3, true},
		{29, 3, true},
		// The newest is the same as the last.
		{11, 0, true},
		{13, 0, false},
		{17, 0, true},
		{30, 0, false},
		{150, 0, false},
	} {
		if got := at(c.t, c.n); got != c.w {
			t.Errorf("valid time %d as of transaction %d: %v, want %v", c.t, c.n, got, c.w)
		}
	}
	if d.at(15, view{sys: BaseSystemTime}) {
		t.Error("the document was known before the first transaction")
	}
}

// TestStatementsOfOneTransactionLeaveNoHistoryBetweenThem: a version made and
// overwritten in one transaction was never visible at any system time.
func TestStatementsOfOneTransactionLeaveNoHistoryBetweenThem(t *testing.T) {
	im := newImage()
	if err := im.apply(Txn{System: SystemTime(1), Ops: []Op{seg(Insert, 10, 20), seg(DeletePortion, 10, 20), seg(Insert, 12, 14)}}); err != nil {
		t.Fatal(err)
	}
	d := im.docs[testKey]
	if len(d.closed) != 0 || len(d.live) != 1 || d.live[0] != (version{12, 14, SystemTime(1), Forever}) {
		t.Fatalf("live %+v, closed %+v", d.live, d.closed)
	}
}

// TestATransactionThatCannotCommitChangesNothing.
func TestATransactionThatCannotCommitChangesNothing(t *testing.T) {
	im := newImage()
	if err := im.apply(Txn{System: SystemTime(2), Ops: []Op{seg(Insert, 10, 20)}}); err != nil {
		t.Fatal(err)
	}
	rec := newRec(observe(1, "a", 10, 0))
	for name, tx := range map[string]Txn{
		"a system time that is not later":                     {System: SystemTime(2), Ops: []Op{seg(Insert, 30, 40)}},
		"a system time before the last":                       {System: SystemTime(1), Ops: []Op{seg(Insert, 30, 40)}},
		"an empty valid-time range, after one good statement": {System: SystemTime(3), Ops: []Op{seg(Insert, 30, 40), seg(Insert, 50, 50)}},
		"a log insert with no record":                         {System: SystemTime(3), Ops: []Op{seg(Insert, 30, 40), {Kind: Insert, Key: Key{Table: LogF, Seq: 1}}}},
		"a log delete":                                        {System: SystemTime(3), Ops: []Op{{Kind: DeletePortion, Key: Key{Table: LogF, Seq: 1}, Rec: rec}}},
		"a statement with no kind":                            {System: SystemTime(3), Ops: []Op{{Key: testKey, From: 1, To: 2}}},
		"a statement on a table that is none":                 {System: SystemTime(3), Ops: []Op{{Kind: Insert, Key: Key{Table: Table(99)}}}},
	} {
		if err := im.apply(tx); err == nil {
			t.Errorf("%s: committed", name)
		}
	}
	d := im.docs[testKey]
	if im.lastSys != SystemTime(2) || len(d.live) != 1 || d.live[0].vFrom != 10 || len(im.logs[LogF]) != 0 {
		t.Fatalf("a refused transaction changed the image: last system time %d, live %+v", im.lastSys, d.live)
	}
}

// TestADeleteOfADocumentThatIsNotThereIsNothing.
func TestADeleteOfADocumentThatIsNotThereIsNothing(t *testing.T) {
	im := newImage()
	if err := im.apply(Txn{System: SystemTime(1), Ops: []Op{seg(DeletePortion, 1, 2)}}); err != nil {
		t.Fatal(err)
	}
	if len(im.docs) != 0 {
		t.Fatal("a delete made a document")
	}
}

// TestTheSegmentTablesOfAnEdgeAreReadFromEachEnd: a document is found from the entity
// its table is read from.
func TestTheSegmentTablesOfAnEdgeAreReadFromEachEnd(t *testing.T) {
	r := newRig()
	r.write(observe(1, "a", 10, 0))
	if got := len(r.img.incident[SegF][pod1]); got != 1 {
		t.Errorf("seg_f from the source: %d documents", got)
	}
	if got := len(r.img.incident[SegR][node1]); got != 1 {
		t.Errorf("seg_r from the target: %d documents", got)
	}
	if len(r.img.incident[SegF][node1])+len(r.img.incident[SegR][pod1]) != 0 {
		t.Error("a document is found from the wrong end")
	}
	if got := len(r.img.logs[LogF][pod1]) + len(r.img.logs[LogR][node1]); got != 2 {
		t.Errorf("log documents: %d", got)
	}
	if !strings.Contains(Op{Kind: Insert, Key: testKey, From: 1, To: Forever}.String(), "inf") && !strings.Contains(Op{Kind: Insert, Key: testKey, From: 1, To: Forever}.String(), "forever") {
		t.Error("a statement does not say it reaches the end of time")
	}
	var _ engine.Engine = New()
}
