package xtengine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// retainCheck: a Retain that moves the horizon is a transaction of its own at the next
// ordinal, that writes one meta document with the horizon and the sequence number the last
// batch ended at; the token of that number names it, a Retain that moves nothing writes
// nothing, a Retain before any batch takes the first ordinal, and an engine opened afterwards
// has the horizon, with no batch after the Retain.
func retainCheck(node *fakeNode, e *Engine) error {
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0), observeA(2, "b", 11, 0)}); err != nil {
		return err
	}
	if err := e.Retain(sec(8)); err != nil {
		return err
	}
	// The retention is transaction 2.
	times := beginTimes(node.statements["w"])
	if want := []string{sysLit(defaultBase, 1), sysLit(defaultBase, 2)}; !slices.Equal(times, want) {
		return fmt.Errorf("transactions at %v, want %v", times, want)
	}
	var meta []loggedStmt
	inRetain := false
	for _, s := range node.statements["w"] {
		switch {
		case strings.HasPrefix(s.sql, "BEGIN READ WRITE") && strings.Contains(s.sql, sysLit(defaultBase, 2)):
			inRetain = true
		case s.sql == "COMMIT":
			inRetain = false
		case inRetain && strings.HasPrefix(s.sql, "INSERT"):
			meta = append(meta, s)
		}
	}
	if len(meta) != 1 || !strings.HasPrefix(meta[0].sql, "INSERT INTO meta ") {
		return fmt.Errorf("the retention wrote %d statements: %v", len(meta), describe(meta))
	}
	want := []any{int64(2), "retain", seqToDB(2), defaultBase.Add(2 * time.Microsecond).UnixMicro(), sec(8).UnixNano(), int64(0)}
	if !reflect.DeepEqual(meta[0].args, want) {
		return fmt.Errorf("the retention's meta document is %v, want %v", meta[0].args, want)
	}
	if sys, ok := e.SystemTimeOf(2); !ok || int64(sys) != defaultBase.Add(2*time.Microsecond).UnixNano() {
		return fmt.Errorf("the token of the last batch names %d (%v), not the retention's system time", sys, ok)
	}
	// Nothing moved: nothing is written.
	n := len(node.statements["w"])
	if err := e.Retain(sec(8)); err != nil {
		return err
	}
	if err := e.Retain(sec(3)); err != nil {
		return err
	}
	if len(node.statements["w"]) != n {
		return errors.New("a Retain that moved nothing wrote to the node")
	}
	// A batch follows at the next ordinal.
	if err := e.Write([]engine.Record{observeA(3, "a", 12, 0)}); err != nil {
		return err
	}
	if got := beginTimes(node.statements["w"]); len(got) != 3 || got[2] != sysLit(defaultBase, 3) {
		return fmt.Errorf("the batch after the retention was at %v", got)
	}
	// A retention is the last thing the engine did; the horizon is on the node.
	if err := e.Retain(sec(9)); err != nil {
		return err
	}
	_ = e.Close()
	e2 := openFake(nopTB{}, node, e.mut)
	defer func() { _ = e2.Close() }()
	if e2.LastSeq() != 3 {
		return fmt.Errorf("a reopened engine is at last seq %d", e2.LastSeq())
	}
	if err := e2.Write([]engine.Record{observeA(4, "a", 8, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("after a reopen, a record before a horizon that no batch followed: %w", orNil(err))
	}
	if sys, ok := e2.SystemTimeOf(3); !ok || int64(sys) != defaultBase.Add(4*time.Microsecond).UnixNano() {
		return fmt.Errorf("after a reopen the token names %d (%v), want the retention's, ordinal 4", sys, ok)
	}
	if err := e2.Write([]engine.Record{observeA(4, "a", 9, 0)}); err != nil {
		return fmt.Errorf("after a reopen, a record at the horizon: %w", err)
	}
	if got := beginTimes(node.statements["w"]); got[len(got)-1] != sysLit(defaultBase, 5) {
		return fmt.Errorf("the batch after the reopen was at %v, want ordinal 5", got)
	}
	// A retention before any batch takes the first ordinal, and the first batch the second.
	node2 := newFakeNode()
	e3 := openFake(nopTB{}, node2, e.mut)
	defer func() { _ = e3.Close() }()
	if err := e3.Retain(sec(5)); err != nil {
		return err
	}
	if err := e3.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		return err
	}
	if got, want := beginTimes(node2.statements["w"]), []string{sysLit(defaultBase, 1), sysLit(defaultBase, 2)}; !slices.Equal(got, want) {
		return fmt.Errorf("a retention before any batch: transactions at %v, want %v", got, want)
	}
	if _, ok := e3.SystemTimeOf(0); ok {
		return errors.New("the token 0 names a system time")
	}
	e4 := openFake(nopTB{}, node2, e.mut)
	defer func() { _ = e4.Close() }()
	if err := e4.Write([]engine.Record{observeA(2, "a", 4, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("a horizon set before any batch is not found: %w", orNil(err))
	}
	return nil
}

func TestARetainIsATransactionOfItsOwn(t *testing.T) {
	node := newFakeNode()
	if err := retainCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
}

// TestAFailedRetainChangesNothing: a retention the node refuses leaves the horizon and the
// ordinal as they were.
func TestAFailedRetainChangesNothing(t *testing.T) {
	node := newFakeNode()
	e := openFake(t, node, 0)
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	failWhen(node, func(conn, sql string) bool { return conn == "w" && strings.HasPrefix(sql, "INSERT INTO meta ") }, &pgconn.PgError{Message: "refused"})
	if err := e.Retain(sec(8)); err == nil {
		t.Fatal("a refused retention returned no error")
	}
	failWhen(node, func(string, string) bool { return false }, nil)
	if err := e.Write([]engine.Record{observeA(2, "a", 5, 0)}); err != nil {
		t.Errorf("a record before a horizon the node refused: %v", err)
	}
	if got := beginTimes(node.statements["w"]); got[len(got)-1] != sysLit(defaultBase, 2) {
		t.Errorf("transactions at %v: the refused retention took an ordinal", got)
	}
}

// readBackCheck: a transaction whose commit says nothing of an indexer that has lost it is an
// error, leaves the engine as it was and ends writes, for a batch and for a retention.
func readBackCheck(node *fakeNode, e *Engine) error {
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		return err
	}
	node.loseCommit = true
	err := e.Write([]engine.Record{observeA(2, "a", 11, 0)})
	if err == nil || !strings.Contains(err.Error(), "meta document is not there") {
		return fmt.Errorf("a batch the node lost was not reported as such: %w", orNil(err))
	}
	if e.LastSeq() != 1 || e.Counts().Batches != 1 {
		return fmt.Errorf("a lost batch left last seq %d and %d batches", e.LastSeq(), e.Counts().Batches)
	}
	node.loseCommit = false
	if err := e.Write([]engine.Record{observeA(2, "a", 11, 0)}); err == nil {
		return errors.New("the engine was written to after a transaction that may have been lost")
	}
	if err := e.Retain(sec(5)); err == nil {
		return errors.New("the engine was given a retention after a transaction that may have been lost")
	}
	// A retention that is lost is reported too, and the horizon does not move.
	node2 := newFakeNode()
	e2 := openFake(nopTB{}, node2, e.mut)
	defer func() { _ = e2.Close() }()
	if err := e2.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		return err
	}
	node2.loseCommit = true
	if err := e2.Retain(sec(8)); err == nil {
		return errors.New("a retention the node lost was not reported")
	}
	if err := e2.Write([]engine.Record{observeA(2, "a", 5, 0)}); err == nil || errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("after a lost retention: %w", orNil(err))
	}
	return nil
}

func TestATransactionTheNodeLostIsAnError(t *testing.T) {
	node := newFakeNode()
	if err := readBackCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
}

// deadlineCheck: a commit that does not answer is an error when the write's time is up, and
// ends writes, for a batch and for a retention.
func deadlineCheck(node *fakeNode, e *Engine) error {
	e.writeTimeout = 100 * time.Millisecond
	node.hang = func(conn, sql string) bool { return conn == "w" && sql == "COMMIT" }
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("a commit that hung: %w", orNil(err))
	}
	if e.LastSeq() != 0 {
		return fmt.Errorf("a batch that timed out left last seq %d", e.LastSeq())
	}
	node.hang = nil
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err == nil {
		return errors.New("the engine was written to after a commit that hung")
	}
	return nil
}

func TestACommitThatHangsIsAnError(t *testing.T) {
	node := newFakeNode()
	if err := deadlineCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
	// So is a retention's.
	node = newFakeNode()
	e := openFake(t, node, 0)
	e.writeTimeout = 100 * time.Millisecond
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	node.hang = func(conn, sql string) bool { return conn == "w" && sql == "COMMIT" }
	if err := e.Retain(sec(8)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a retention whose commit hung: %v", orNil(err))
	}
}

// payloadCheck: a payload is sent as hex text, cast to a binary value, and never as bytes,
// which the driver would write as a quoted string that the node cannot cast and that stops its
// ingestion; and it is read back as it was, whatever its bytes.
func payloadCheck(node *fakeNode, e *Engine) error {
	payload := make([]byte, 0, 262)
	for i := range 256 {
		payload = append(payload, byte(i))
	}
	payload = append(payload, '\\', 'x', '4')
	r := observeA(1, "a", 10, 0)
	r.Payload = payload
	if err := e.Write([]engine.Record{r}); err != nil {
		return err
	}
	seen := 0
	for _, s := range node.statements["w"] {
		if !strings.HasPrefix(s.sql, "INSERT INTO log_") {
			continue
		}
		if !strings.Contains(s.sql, "CAST($") || !strings.Contains(s.sql, " AS VARBINARY)") {
			return fmt.Errorf("a log insert does not cast its payload: %s", s.sql)
		}
		for _, a := range s.args {
			if _, raw := a.([]byte); raw {
				return errors.New("bytes were sent as an argument")
			}
			if str, ok := a.(string); ok && str == hex.EncodeToString(payload) {
				seen++
			}
		}
	}
	if seen != 2 {
		return fmt.Errorf("the payload was sent as hex text %d times, want 2 (forward and reverse)", seen)
	}
	got, err := e.Window(pod1, engine.Forward, sec(0), sec(100), engine.Current(catalog.L2))
	if err != nil || len(got) != 1 || !slices.Equal(got[0].Payload, payload) {
		return fmt.Errorf("the payload read back as %v: %w", got, orNil(err))
	}
	return nil
}

func TestPayloadsAreSentAsHexText(t *testing.T) {
	node := newFakeNode()
	if err := payloadCheck(node, openFake(t, node, 0)); err != nil {
		t.Fatal(err)
	}
}
