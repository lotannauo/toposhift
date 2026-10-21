package xtengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// failWhen makes the fake node fail the statements for which match is true, with err,
// until it is called again.
func failWhen(n *fakeNode, match func(conn, sql string) bool, err error) {
	n.failExec = func(conn, sql string) error {
		if match(conn, sql) {
			return err
		}
		return nil
	}
}

func isCommit(conn, sql string) bool { return conn == "w" && sql == "COMMIT" }

// stmtsOf are the statements of the writer's n-th transaction (from 1), between its begin
// and its commit.
func stmtsOf(t *testing.T, n *fakeNode, which int) []loggedStmt {
	t.Helper()
	var out []loggedStmt
	k := 0
	open := false
	for _, s := range n.statements["w"] {
		switch {
		case strings.HasPrefix(s.sql, "BEGIN READ WRITE"):
			k++
			open = k == which
		case s.sql == "COMMIT" || s.sql == "ROLLBACK":
			open = false
		case open:
			out = append(out, s)
		}
	}
	if k < which {
		t.Fatalf("the writer opened %d transactions, not %d", k, which)
	}
	return out
}

func countPrefix(stmts []loggedStmt, prefix string) int {
	n := 0
	for _, s := range stmts {
		if strings.HasPrefix(s.sql, prefix) {
			n++
		}
	}
	return n
}

// TestAWriteIsOneTransactionAtTheNextSystemTime: a batch is one transaction, opened at the
// system time of the next ordinal, with the planner's statements in order and a meta
// document last, and committed; nothing is sent on the reader's connection.
func TestAWriteIsOneTransactionAtTheNextSystemTime(t *testing.T) {
	node := newFakeNode()
	e := openFake(t, node, 0)
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0), podRec(2, 10)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(3, "a", 20, 60)}); err != nil {
		t.Fatal(err)
	}
	if got, want := beginTimes(node.statements["w"]), []string{sysLit(defaultBase, 1), sysLit(defaultBase, 2)}; !slices.Equal(got, want) {
		t.Errorf("transactions at %v, want %v", got, want)
	}
	all := node.statements["w"]
	if all[0].sql != "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2000-01-01T00:00:00.000001Z')" || all[len(all)-2].sql != "COMMIT" || !strings.HasPrefix(all[len(all)-1].sql, "SELECT last_seq FROM meta ") {
		t.Errorf("the transactions are not framed by a begin, a commit and a read of the meta document:\n%s", describe(all))
	}
	if len(node.statements["r"]) != 0 {
		t.Errorf("a write used the reader's connection:\n%s", describe(node.statements["r"]))
	}
	first := stmtsOf(t, node, 1)
	// An edge record: a segment in each direction and a log document in each; an entity
	// record: a segment and a log document; one meta document.
	for prefix, want := range map[string]int{
		"INSERT INTO seg_f ": 1, "INSERT INTO seg_r ": 1, "INSERT INTO log_f ": 1, "INSERT INTO log_r ": 1,
		"INSERT INTO ent ": 1, "INSERT INTO log_e ": 1, "INSERT INTO meta ": 1,
	} {
		if got := countPrefix(first, prefix); got != want {
			t.Errorf("%d statements %q, want %d:\n%s", got, prefix, want, describe(first))
		}
	}
	if len(first) != 7 || !strings.HasPrefix(first[len(first)-1].sql, "INSERT INTO meta ") {
		t.Errorf("the meta document is not last:\n%s", describe(first))
	}

	// The segment of an open observe: the segment of the edge, from its event time, with no end.
	segF := first[slices.IndexFunc(first, func(s loggedStmt) bool { return strings.HasPrefix(s.sql, "INSERT INTO seg_f ") })]
	wantSeg := []any{
		"k8s.pod:" + fpHex(pod1) + "|k8s.node:" + fpHex(node1) + "|scheduled_on|a", int64(catalog.L2), pod1.String(), node1.String(), "scheduled_on", "a",
		sec(10), nil,
	}
	if !reflect.DeepEqual(segF.args, wantSeg) {
		t.Errorf("the segment's arguments are %v, want %v", segF.args, wantSeg)
	}
	segR := first[slices.IndexFunc(first, func(s loggedStmt) bool { return strings.HasPrefix(s.sql, "INSERT INTO seg_r ") })]
	if segR.args[2] != node1.String() || segR.args[3] != pod1.String() {
		t.Errorf("the reverse segment is read from %v and its peer is %v: want the target and the source", segR.args[2], segR.args[3])
	}
	// A TTL ends the segment at its deadline.
	second := stmtsOf(t, node, 2)
	segF = second[slices.IndexFunc(second, func(s loggedStmt) bool { return strings.HasPrefix(s.sql, "INSERT INTO seg_f ") })]
	if segF.args[6] != sec(20) || segF.args[7] != sec(80) {
		t.Errorf("a record at 20s with a TTL of 60s has the valid time [%v, %v), want [20s, 80s)", segF.args[6], segF.args[7])
	}
	// The record's log document has all of its fields.
	logF := first[slices.IndexFunc(first, func(s loggedStmt) bool { return strings.HasPrefix(s.sql, "INSERT INTO log_f ") })]
	wantLog := []any{
		logID(1), int64(catalog.L2), pod1.String(), node1.String(), "scheduled_on", "a",
		seqToDB(1), sec(10).UnixNano(), int64(lifecycle.Observe), int64(0), nil,
		"01",
	}
	if !reflect.DeepEqual(logF.args, wantLog) {
		t.Errorf("the log document's arguments are %v, want %v", logF.args, wantLog)
	}
	meta := first[len(first)-1]
	wantMeta := []any{int64(1), "batch", seqToDB(2), time.Date(2000, 1, 1, 0, 0, 0, 1000, time.UTC).UnixMicro(), int64(0), int64(0)}
	if !reflect.DeepEqual(meta.args, wantMeta) {
		t.Errorf("the meta document's arguments are %v, want %v", meta.args, wantMeta)
	}
}

func fpHex(f interface{ String() string }) string {
	_, h, _ := strings.Cut(f.String(), ":")
	return h
}

// TestAShrinkingDeadlineDeletesWhatItLeaves: the statements of the planner reach the node as
// they are: a portion delete has its range as arguments, a null end where it reaches the end of
// time.
func TestAShrinkingDeadlineDeletesWhatItLeaves(t *testing.T) {
	node := newFakeNode()
	e := openFake(t, node, 0)
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(2, "a", 20, 5)}); err != nil {
		t.Fatal(err)
	}
	got := stmtsOf(t, node, 2)
	i := slices.IndexFunc(got, func(s loggedStmt) bool { return strings.HasPrefix(s.sql, "DELETE FROM seg_f ") })
	if i < 0 {
		t.Fatalf("no portion delete of seg_f:\n%s", describe(got))
	}
	want := "DELETE FROM seg_f FOR PORTION OF VALID_TIME FROM $1 TO $2 WHERE _id = $3"
	if got[i].sql != want || got[i].args[0] != sec(25) || got[i].args[1] != nil {
		t.Errorf("the portion delete is %q %v, want %q from 25s to the end of time", got[i].sql, got[i].args, want)
	}
}

// TestAFailedBatchChangesNothing: a statement, a begin or a commit that the node refuses
// leaves the engine as it was, the batch can be written again at the same system time, and
// a commit that fails with no answer from the node ends writes.
func TestAFailedBatchChangesNothing(t *testing.T) {
	pgErr := &pgconn.PgError{Message: "the node refused"}
	netErr := errors.New("connection reset")
	batch1 := []engine.Record{observeA(1, "a", 10, 0), observeA(2, "b", 11, 0)}
	batch2 := []engine.Record{observeA(3, "a", 12, 0), edgeRec(4, edgeB, "c", 13, 0)}
	for _, c := range []struct {
		name   string
		match  func(conn, sql string) bool
		err    error
		rolled bool // the engine sent a rollback
		broken bool
	}{
		{"the begin", func(conn, sql string) bool { return conn == "w" && strings.HasPrefix(sql, "BEGIN READ WRITE") }, pgErr, false, false},
		{"a statement", func(conn, sql string) bool { return conn == "w" && strings.HasPrefix(sql, "INSERT INTO log_r ") }, pgErr, true, false},
		{"the commit, with an abort from the node", isCommit, pgErr, true, false},
		{"the commit, with no answer", isCommit, netErr, false, true},
		{"a statement and then the rollback", func(conn, sql string) bool {
			return conn == "w" && (strings.HasPrefix(sql, "INSERT INTO log_r ") || sql == "ROLLBACK")
		}, pgErr, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			node := newFakeNode()
			e := openFake(t, node, 0)
			if err := e.Write(batch1); err != nil {
				t.Fatal(err)
			}
			failWhen(node, c.match, c.err)
			err := e.Write(batch2)
			if !errors.Is(err, c.err) {
				t.Fatalf("the failure of %s was reported as %v", c.name, orNil(err))
			}
			failWhen(node, func(string, string) bool { return false }, nil)
			if e.LastSeq() != 2 || e.Counts().Batches != 1 || e.Counts().Records != 2 {
				t.Errorf("a refused batch left last seq %d, %d batches, %d records", e.LastSeq(), e.Counts().Batches, e.Counts().Records)
			}
			rolledBack := slices.ContainsFunc(node.statements["w"], func(s loggedStmt) bool { return s.sql == "ROLLBACK" })
			if rolledBack != c.rolled {
				t.Errorf("a rollback was sent: %v, want %v", rolledBack, c.rolled)
			}
			err = e.Write(batch2)
			switch {
			case c.broken && err == nil:
				t.Fatal("the engine was written to after a transaction of unknown outcome")
			case c.broken:
				return
			case err != nil:
				t.Fatalf("the batch was refused when written again: %v", err)
			}
			// The retry is at the system time the refused attempt had: the batch took no ordinal.
			times := beginTimes(node.statements["w"])
			want := []string{sysLit(defaultBase, 1), sysLit(defaultBase, 2), sysLit(defaultBase, 2)}
			if !slices.Equal(times, want) {
				t.Errorf("transactions at %v, want %v", times, want)
			}
			if e.LastSeq() != 4 {
				t.Errorf("last seq %d after the batch was written", e.LastSeq())
			}
		})
	}
}

// TestTheSystemTimeOfABatchIsItsOrdinal: batches are at the base plus their ordinal in
// microseconds, strictly later each time, and a token that ended a batch names its system
// time.
func TestTheSystemTimeOfABatchIsItsOrdinal(t *testing.T) {
	for _, tc := range []struct {
		name string
		base time.Time
	}{{"the base of the baseline", time.Time{}}, {"a base of a test", time.Date(2031, 3, 4, 5, 6, 7, 8000, time.UTC)}} {
		t.Run(tc.name, func(t *testing.T) {
			node := newFakeNode()
			names, _ := namesFor("")
			e, err := openWith(context.Background(), Config{Base: tc.base}, names, node.conn("w"), node.conn("r"))
			if err != nil {
				t.Fatal(err)
			}
			base := tc.base
			if base.IsZero() {
				base = defaultBase
			}
			for i, batch := range [][]engine.Record{
				{observeA(1, "a", 11, 0), observeA(2, "a", 12, 0)},
				{observeA(3, "a", 13, 0)},
				{observeA(10, "a", 14, 0)},
			} {
				if err := e.Write(batch); err != nil {
					t.Fatal(err)
				}
				last := batch[len(batch)-1].Seq
				sys, ok := e.SystemTimeOf(last)
				if want := base.Add(time.Duration(i+1) * time.Microsecond); !ok || int64(sys) != want.UnixNano() {
					t.Errorf("token %d is at system time %d (ok %v), want %d", last, sys, ok, want.UnixNano())
				}
			}
			if _, ok := e.SystemTimeOf(1); ok {
				t.Error("a token inside a batch names a system time")
			}
			prev := ""
			for i, got := range beginTimes(node.statements["w"]) {
				if want := sysLit(base, i+1); got != want || got <= prev {
					t.Errorf("transaction %d at %s, want %s after %q", i+1, got, want, prev)
				}
				prev = got
			}
		})
	}
}

// TestARetainRefusesEarlierWritesAndMovesOnlyForward: a write before the horizon is refused
// whole, a horizon does not move back, and one after the range refuses everything.
func TestARetainRefusesEarlierWritesAndMovesOnlyForward(t *testing.T) {
	node := newFakeNode()
	e := openFake(t, node, 0)
	if err := retentionCheck(e); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(sec(5)); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(100, "a", 19, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("an earlier Retain moved the horizon back: %v", orNil(err))
	}
	if err := e.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(100, "a", 9_000_000_000, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a horizon after the range accepted a record: %v", orNil(err))
	}
}

// retentionCheck: after Retain a write before the horizon is refused whole.
func retentionCheck(e *Engine) error {
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		return err
	}
	if err := e.Retain(sec(20)); err != nil {
		return err
	}
	err := e.Write([]engine.Record{observeA(2, "a", 30, 0), observeA(3, "a", 19, 0)})
	if !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("a write before the horizon was not refused as such: %w", orNil(err))
	}
	if e.LastSeq() != 1 || e.Counts().Batches != 1 {
		return fmt.Errorf("a refused batch changed the engine: last seq %d, %d batches", e.LastSeq(), e.Counts().Batches)
	}
	return e.Write([]engine.Record{observeA(2, "a", 30, 0)})
}

// systemTimeCheck: the system time of the n-th committed batch is the base plus n
// microseconds, a batch the node refuses takes no ordinal and no sequence number, and
// times strictly increase.
func systemTimeCheck(node *fakeNode, e *Engine) error {
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0), observeA(2, "a", 11, 0)}); err != nil {
		return err
	}
	failWhen(node, isCommit, &pgconn.PgError{Message: "the node refused the transaction"})
	if err := e.Write([]engine.Record{observeA(3, "a", 12, 0), observeA(4, "b", 13, 0)}); err == nil {
		return errors.New("a refused transaction was reported as written")
	}
	failWhen(node, func(string, string) bool { return false }, nil)
	if e.LastSeq() != 2 || e.Counts().Batches != 1 || e.Counts().Records != 2 {
		return fmt.Errorf("a refused batch left last seq %d, %d batches, %d records", e.LastSeq(), e.Counts().Batches, e.Counts().Records)
	}
	for _, b := range [][]engine.Record{
		{observeA(3, "a", 12, 0), observeA(4, "b", 13, 0)},
		{observeA(5, "a", 14, 0)},
		{observeA(8, "c", 15, 0)},
	} {
		if err := e.Write(b); err != nil {
			return err
		}
	}
	if e.LastSeq() != 8 {
		return fmt.Errorf("last seq %d", e.LastSeq())
	}
	// The one refused attempt was at ordinal 2, as was the batch written after it.
	want := []string{sysLit(defaultBase, 1), sysLit(defaultBase, 2), sysLit(defaultBase, 2), sysLit(defaultBase, 3), sysLit(defaultBase, 4)}
	if got := beginTimes(node.statements["w"]); !slices.Equal(got, want) {
		return fmt.Errorf("transactions at %v, want %v", got, want)
	}
	for i, tok := range []uint64{2, 4, 5, 8} {
		if sys, ok := e.SystemTimeOf(tok); !ok || int64(sys) != defaultBase.Add(time.Duration(i+1)*time.Microsecond).UnixNano() {
			return fmt.Errorf("token %d is at system time %d (%v)", tok, sys, ok)
		}
	}
	return nil
}

func TestTheSystemTimeMappingHolds(t *testing.T) {
	node := newFakeNode()
	if err := systemTimeCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
}

// offMicrosecondCheck: a record with a boundary that is not a whole microsecond is refused
// whole with ErrInvalid, counted, and sends nothing; the same batch without it is written.
func offMicrosecondCheck(node *fakeNode, e *Engine) error {
	off := observeA(1, "a", 10, 0)
	off.EventTime = sec(10).Add(time.Nanosecond)
	for _, r := range []engine.Record{
		off,
		{Layer: catalog.L2, Subject: edgeA, Producer: "a", EventTime: sec(10), Seq: 1, Kind: lifecycle.Observe, TTL: time.Second + time.Nanosecond}, // the deadline is off
	} {
		err := e.Write([]engine.Record{r, observeA(2, "b", 11, 0)})
		if !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("a boundary off a whole microsecond was not refused with ErrInvalid: %w", orNil(err))
		}
	}
	if got := e.Refused().OffMicrosecond; got != 2 {
		return fmt.Errorf("%d records counted as off a microsecond, want 2", got)
	}
	if n := len(node.statements["w"]); n != 0 || e.LastSeq() != 0 || e.Counts().Batches != 0 {
		return fmt.Errorf("a refused batch sent %d statements, left last seq %d and %d batches", n, e.LastSeq(), e.Counts().Batches)
	}
	// A whole microsecond is written.
	whole := observeA(1, "a", 10, 0)
	whole.EventTime = sec(10).Add(time.Microsecond)
	if err := e.Write([]engine.Record{whole}); err != nil {
		return err
	}
	return nil
}

func TestABoundaryOffAWholeMicrosecondIsRefusedAndCounted(t *testing.T) {
	node := newFakeNode()
	if err := offMicrosecondCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
}

// batches is a stream with late records, shrinking deadlines, deletes and two producers,
// in four batches.
func lateStream() [][]engine.Record {
	del := observeA(5, "b", 25, 0)
	del.Kind, del.Payload = lifecycle.Delete, nil
	return [][]engine.Record{
		{observeA(1, "a", 10, 0), observeA(2, "b", 20, 30), podRec(3, 10)},
		{observeA(4, "a", 40, 5), del},
		{observeA(6, "a", 15, 10), podRec(7, 12)},
		{observeA(8, "a", 50, 0), observeA(9, "b", 45, 9)},
	}
}

// TestAReopenedEnginePlansAsOneThatWasNeverClosed: an engine opened over what another left
// sends, for the rest of a stream, the statements that the one that was never closed sends:
// its token, its ordinals, its horizon and what its planner remembers come back from the node.
func TestAReopenedEnginePlansAsOneThatWasNeverClosed(t *testing.T) {
	stream := lateStream()
	straight := newFakeNode()
	a := openFake(t, straight, 0)
	for _, b := range stream {
		if err := a.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	reopened := newFakeNode()
	b := openFake(t, reopened, 0)
	for _, batch := range stream[:2] {
		if err := b.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	c := openFake(t, reopened, 0)
	if c.LastSeq() != 5 {
		t.Fatalf("a reopened engine is at last seq %d, want 5", c.LastSeq())
	}
	for _, batch := range stream[2:] {
		if err := c.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	for n := 3; n <= 4; n++ {
		want, got := describe(stmtsOf(t, straight, n)), describe(stmtsOf(t, reopened, n-2))
		if got != want {
			t.Errorf("batch %d after a reopen:\n%s\nwant:\n%s", n, got, want)
		}
	}
	if got, want := beginTimes(reopened.statements["w"]), []string{sysLit(defaultBase, 3), sysLit(defaultBase, 4)}; !slices.Equal(got, want) {
		t.Errorf("the reopened engine wrote at %v, want %v", got, want)
	}
	// The tokens of the earlier batches name their system times again.
	for tok, n := range map[uint64]int{3: 1, 5: 2} {
		if sys, ok := c.SystemTimeOf(tok); !ok || int64(sys) != defaultBase.Add(time.Duration(n)*time.Microsecond).UnixNano() {
			t.Errorf("after a reopen, token %d is at %d (%v)", tok, sys, ok)
		}
	}
}

// TestTheBaseOfANodeIsKeptWithIt: a node written with a base is read back with it, an engine
// that names another is refused, and a base must be a whole microsecond.
func TestTheBaseOfANodeIsKeptWithIt(t *testing.T) {
	names, _ := namesFor("")
	node := newFakeNode()
	base := time.Date(2031, 3, 4, 5, 6, 7, 8000, time.UTC)
	open := func(cfg Config) (*Engine, error) {
		return openWith(context.Background(), cfg, names, node.conn("w"), node.conn("r"))
	}
	e, err := open(Config{Base: base})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()
	e, err = open(Config{})
	if err != nil {
		t.Fatalf("a node written with a base, opened with none: %v", err)
	}
	if sys, _ := e.SystemTimeOf(1); int64(sys) != base.Add(time.Microsecond).UnixNano() {
		t.Errorf("the base was not read back: token 1 at %d", sys)
	}
	_ = e.Close()
	if _, err := open(Config{Base: base}); err != nil {
		t.Errorf("the same base: %v", err)
	}
	if _, err := open(Config{Base: base.Add(time.Second)}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("another base: %v", orNil(err))
	}
	for name, b := range map[string]time.Time{
		"a base with nanoseconds": time.Date(2031, 1, 1, 0, 0, 0, 1, time.UTC),
		"a base before 1970":      time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC),
		"a base after 2262":       time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		if _, err := openWith(context.Background(), Config{Base: b}, names, newFakeNode().conn("w"), newFakeNode().conn("r")); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: %v", name, orNil(err))
		}
	}
}

// TestAnEngineOverTablesThatAreNotItsOwnIsRefused: meta documents that do not make one
// engine's history are an error, not a guess.
func TestAnEngineOverTablesThatAreNotItsOwnIsRefused(t *testing.T) {
	names, _ := namesFor("")
	for name, docs := range map[string][][]any{
		"a gap in the ordinals":            {{int64(1), "batch", seqToDB(2), int64(1), int64(0), int64(0)}, {int64(3), "batch", seqToDB(5), int64(3), int64(0), int64(0)}},
		"two batches ending at one number": {{int64(1), "batch", seqToDB(2), int64(1), int64(0), int64(0)}, {int64(2), "batch", seqToDB(2), int64(2), int64(0), int64(0)}},
		"a kind that is not a batch":       {{int64(1), "retain", seqToDB(2), int64(1), int64(0), int64(0)}},
		"an unknown kind":                  {{int64(1), "merge", seqToDB(2), int64(1), int64(0), int64(0)}},
		"a retention that moves the token": {{int64(1), "batch", seqToDB(2), int64(1), int64(0), int64(0)}, {int64(2), "retain", seqToDB(5), int64(2), int64(0), int64(0)}},
		"two bases":                        {{int64(1), "batch", seqToDB(2), int64(1), int64(0), int64(0)}, {int64(2), "batch", seqToDB(3), int64(5), int64(0), int64(0)}},
	} {
		node := newFakeNode()
		node.lastSys = 1
		for _, d := range docs {
			node.table("meta").paint(d[0], 0, forever, 1, map[string]any{"kind": d[1], "last_seq": d[2], "sys_us": d[3], "horizon_ns": d[4], "past_end": d[5]})
		}
		if e, err := openWith(context.Background(), Config{}, names, node.conn("w"), node.conn("r")); err == nil {
			_ = e.Close()
			t.Errorf("%s: opened", name)
		}
	}
}

// TestACountOfWhatWasWrittenEqualsTheModels: for one stream and one batching the counts of
// the engine are those of the model, which a server is not needed to know.
func TestACountOfWhatWasWrittenEqualsTheModels(t *testing.T) {
	g, err := workload.New(workload.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	e := openFake(t, newFakeNode(), 0)
	m := xtmodel.New()
	for range 30 {
		batch := g.Batch(17)
		if err := e.Write(slices.Clone(batch)); err != nil {
			t.Fatal(err)
		}
		if err := m.Write(slices.Clone(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := e.Counts(), m.Counts(); !reflect.DeepEqual(got, want) {
		t.Errorf("the engine counted %+v, the model %+v", got, want)
	}
	if e.Counts().Statements() == 0 || e.Counts().Late+e.Counts().InOrder != e.Counts().Records {
		t.Errorf("counts %+v", e.Counts())
	}
}

// TestSizeIsWhatTheNodeKeeps: the regular files under the data directories, and an error
// when there are none to measure.
func TestSizeIsWhatTheNodeKeeps(t *testing.T) {
	root := t.TempDir()
	write := func(path string, n int) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "log", "a"), 100)
	write(filepath.Join(root, "log", "sub", "b"), 20)
	write(filepath.Join(root, "storage", "c"), 3)
	write(filepath.Join(root, "other", "d"), 1000)
	if err := os.Symlink(filepath.Join(root, "other", "d"), filepath.Join(root, "storage", "link")); err != nil {
		t.Fatal(err)
	}
	names, _ := namesFor("")
	open := func(dirs ...string) *Engine {
		e, err := openWith(context.Background(), Config{DataDirs: dirs}, names, newFakeNode().conn("w"), newFakeNode().conn("r"))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	if n, err := open(filepath.Join(root, "log"), filepath.Join(root, "storage")).Size(); err != nil || n != 123 {
		t.Errorf("size %d, %v; want 123", n, err)
	}
	if _, err := open().Size(); !errors.Is(err, ErrNoDataDirs) {
		t.Errorf("no directories: %v", orNil(err))
	}
	if _, err := open(filepath.Join(root, "missing")).Size(); err == nil {
		t.Error("a directory that is not there was measured")
	}
}

// TestExtremesAgreeWithTheModelOnTheFake and TestTheLastNanosecondIsRefusedOnTheFake are the
// server's checks of the ends of the range, on the fake node.
func TestExtremesAgreeWithTheModelOnTheFake(t *testing.T) {
	checkExtremes(t, openFake(t, newFakeNode(), 0))
}

func TestTheLastNanosecondIsRefusedOnTheFake(t *testing.T) {
	checkRefusesLastNanosecond(t, openFake(t, newFakeNode(), 0))
}

// TestReadsSeeWholeBatchesWhileAWriterWrites: readers that run while batches are written see
// a number of edges that is a multiple of the batch's, never part of one, and the engine is
// free of data races (it is the test the race detector is for: it runs in a trimmed run too).
func TestReadsSeeWholeBatchesWhileAWriterWrites(t *testing.T) {
	const batches, perBatch = 30, 3
	e := openFake(t, newFakeNode(), 0)
	done := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				ns, err := e.Neighbors(node1, engine.Reverse, sec(100), engine.Current(catalog.L2))
				if err == nil && len(ns)%perBatch != 0 {
					err = fmt.Errorf("a read saw %d edges, which is part of a batch", len(ns))
				}
				if err != nil {
					errs <- err
					return
				}
				if _, err := e.Alive(pod1, sec(100), engine.Current(catalog.L2)); err != nil {
					errs <- err
					return
				}
				_ = e.LastSeq()
				_, _ = e.SystemTimeOf(e.LastSeq())
			}
		}()
	}
	seq := uint64(0)
	for b := range batches {
		var batch []engine.Record
		for i := range perBatch {
			seq++
			peer := fpOf("k8s.pod", byte(b*perBatch+i+10))
			batch = append(batch, edgeRec(seq, engine.EdgeSubject(peer, node1, catalog.ScheduledOn), "a", 10, 0))
		}
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
		if b == batches/2 {
			if err := e.Retain(sec(1)); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(done)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if ns, err := e.Neighbors(node1, engine.Reverse, sec(100), engine.Current(catalog.L2)); err != nil || len(ns) != batches*perBatch {
		t.Errorf("after the writes: %d edges, %v", len(ns), err)
	}
}
