package doltengine

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// These tests need no server: they run the engine against the fake one of fake_test.go and
// look at what it sends and at what it does with a failure. Each refusal path and each way
// the engine can get its order wrong has a defect (faults.go) that the check written for it
// must catch.

// committing answers every statement with success and every commit with a new hash.
func committing() func(string, []driver.Value) fakeResult {
	n := 0
	return func(q string, _ []driver.Value) fakeResult {
		if strings.HasPrefix(q, "CALL DOLT_COMMIT") {
			n++
			return fakeResult{cols: []string{"hash"}, rows: [][]driver.Value{{hashN(n)}}}
		}
		return fakeResult{affected: 1}
	}
}

// caught runs a scenario on an unbroken engine, which must pass, and on one with the defect
// switched on, which must not: the check can fail.
func caught(t *testing.T, arm doltmodel.Arm, fault string, handle func(string, []driver.Value) fakeResult, scenario func(*Engine, *fakeServer) error) {
	t.Helper()
	e, srv := fakeEngine(t, arm, handle)
	if err := scenario(e, srv); err != nil {
		t.Fatalf("with no defect: %v", err)
	}
	b, bsrv := fakeEngine(t, arm, handle)
	b.Break(fault)
	if err := scenario(b, bsrv); err == nil {
		t.Fatalf("an engine that %s passed", fault)
	}
}

func TestABatchIsOneTransactionThatEndsInItsCommit(t *testing.T) {
	e, srv := fakeEngine(t, doltmodel.ArmCommit, committing())
	srv.onCommit = func() {
		if e.lastSeq != 0 {
			t.Errorf("the sequence number was raised to %d before the transaction ended", e.lastSeq)
		}
	}
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0), del(edge2, 2, "b", 101), existence(pod1, 3, "a", 100, 0)}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"BEGIN", "INSERT log", "INSERT ent_log", "REPLACE state", "REPLACE ent_state", "UPDATE meta", "CALL DOLT_COMMIT", "COMMIT",
	}
	if got := srv.kinds(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	commit := srv.statements()[6]
	for _, part := range []string{"'-A'", "'-m', 'seq 3'", "'--author', 'toposhift-bench <bench@localhost.invalid>'", "'--date', '2000-01-01T00:00:01Z'"} {
		if !strings.Contains(commit, part) {
			t.Errorf("the commit %q lacks %s", commit, part)
		}
	}
	if e.LastSeq() != 3 {
		t.Errorf("LastSeq = %d", e.LastSeq())
	}
	head, ok := e.Head()
	if !ok || head.Hash != hashN(1) || head.Ordinal != 1 || head.Token != 3 || head.Message != "seq 3" || !head.Date.Equal(doltmodel.CommitDate(1)) {
		t.Errorf("head %+v", head)
	}
	if c := e.Counts(); c.Records != 3 || c.LogRows != 5 || c.StateWrites != 5 || c.Commits != 1 {
		t.Errorf("counts %+v", c)
	}

	// The meta row carries the token as the tables store it.
	meta := srv.argLog[5]
	if len(meta) != 1 || meta[0] != doltmodel.EncodeSeq(3) {
		t.Errorf("the meta row got %v", meta)
	}
}

// TestTheSequenceNumberRisesOnlyAfterTheCommitReturns: a Dolt commit that fails leaves the
// token, the counters and the writer as they were, and the same batch can be written again.
func TestTheSequenceNumberRisesOnlyAfterTheCommitReturns(t *testing.T) {
	scenario := func(e *Engine, srv *fakeServer) error {
		fail := errors.New("the server went away")
		inner := srv.handle
		srv.handle = func(q string, a []driver.Value) fakeResult {
			if strings.HasPrefix(q, "CALL DOLT_COMMIT") {
				return fakeResult{err: fail}
			}
			return inner(q, a)
		}
		batch := []engine.Record{observe(edge1, 1, "a", 100, 0)}
		err := e.Write(batch)
		if !errors.Is(err, fail) {
			return fmt.Errorf("a failed commit returned %w", errors.Join(err, errors.New("(accepted)")))
		}
		if got := e.LastSeq(); got != 0 {
			return fmt.Errorf("after a failed commit LastSeq = %d", got)
		}
		if c := e.Counts(); c != (doltmodel.Counts{}) {
			return fmt.Errorf("after a failed commit the counts are %+v", c)
		}
		if got := srv.kinds(); got[len(got)-1] != "ROLLBACK" {
			return fmt.Errorf("a failed commit did not roll the transaction back: %v", got)
		}
		srv.handle = inner
		if err := e.Write(batch); err != nil {
			return fmt.Errorf("the batch was refused after a failed commit: %w", err)
		}
		if got := e.LastSeq(); got != 1 {
			return fmt.Errorf("LastSeq = %d after the retry", got)
		}
		if c := e.Counts(); c.StateWrites != 2 {
			return fmt.Errorf("the retry counted %d state writes: the writer remembered the failed batch", c.StateWrites)
		}
		return nil
	}
	caught(t, doltmodel.ArmCommit, FaultPublishBeforeCommit, committing(), scenario)
}

// TestAnErrorEndingTheTransactionAfterTheCommitStopsTheEngine: the engine cannot say what the
// database holds, so it refuses everything until it is opened again.
func TestAnErrorEndingTheTransactionAfterTheCommitStopsTheEngine(t *testing.T) {
	e, srv := fakeEngine(t, doltmodel.ArmCommit, committing())
	srv.commitErr = errors.New("the connection dropped")
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0)}); err == nil {
		t.Fatal("a failed end of transaction was not an error")
	}
	srv.reset()
	if err := e.Write([]engine.Record{observe(edge1, 2, "a", 100, 0)}); err == nil || !strings.Contains(err.Error(), "open the engine again") {
		t.Fatalf("a write after it: %v", err)
	}
	if err := e.Retain(at(10)); err == nil {
		t.Fatal("a retention after it was accepted")
	}
	if got := srv.statements(); len(got) != 0 {
		t.Fatalf("it sent %v", got)
	}
	if e.LastSeq() != 0 {
		t.Fatalf("LastSeq = %d", e.LastSeq())
	}
}

// TestTheArmWithNoCommitsMakesNone.
func TestTheArmWithNoCommitsMakesNone(t *testing.T) {
	e, srv := fakeEngine(t, doltmodel.ArmRows, committing())
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(at(50)); err != nil {
		t.Fatal(err)
	}
	for _, k := range srv.kinds() {
		if strings.HasPrefix(k, "CALL") {
			t.Fatalf("the arm with no commits sent %v", srv.kinds())
		}
	}
	if len(e.Commits()) != 0 || e.Counts().Commits != 0 {
		t.Fatalf("commits %v, counted %d", e.Commits(), e.Counts().Commits)
	}
	if _, _, err := e.StateAt(t.Context(), 1); err == nil {
		t.Fatal("the arm with no commits has a state as of one")
	}
}

// TestRefusedBatchesSendNothing: every refusal is made before the first statement, so the
// database never holds a refused batch's valid records, and the writer is unchanged.
func TestRefusedBatchesSendNothing(t *testing.T) {
	big := observe(edge1, 3, "a", 100, 0)
	big.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity+1)
	exact := observe(edge1, 3, "a", 100, 0)
	exact.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity)
	longProducer := observe(edge1, 3, "a", 100, 0)
	longProducer.Producer = lifecycle.Producer(strings.Repeat("p", doltmodel.MaxProducerLen+1))
	otherLayer := observe(edge1, 3, "a", 100, 0)
	otherLayer.Layer = catalog.L3
	deleteWithPayload := del(edge1, 4, "a", 100)
	deleteWithPayload.Payload = []byte{1}
	noLayer := observe(edge1, 4, "a", 100, 0)
	noLayer.Layer = 0

	for _, c := range []struct {
		name  string
		setup func(e *Engine)
		batch []engine.Record
		fault string
		want  error
		count uint64 // payload refusals
	}{
		{"a record out of range", nil, []engine.Record{observe(edge2, 3, "a", 100, 0), noLayer}, FaultSkipValidate, engine.ErrInvalid, 0},
		{"a delete with a payload", nil, []engine.Record{observe(edge2, 3, "a", 100, 0), deleteWithPayload}, FaultSkipValidate, engine.ErrInvalid, 0},
		{"a producer over its column", nil, []engine.Record{longProducer}, FaultIgnoreProducerLen, engine.ErrInvalid, 0},
		{"a payload over its column", nil, []engine.Record{observe(edge2, 3, "a", 100, 0), big}, FaultIgnoreCapacity, engine.ErrInvalid, 1},
		{"a record before the horizon", func(e *Engine) { e.w.horizon = 200 * sec }, []engine.Record{observe(edge1, 3, "a", 100, 0)}, FaultAcceptBeforeHorizon, engine.ErrBeforeHorizon, 0},
		{"a record when the horizon is past the end", func(e *Engine) { e.w.pastEnd = true }, []engine.Record{observe(edge1, 3, "a", 100, 0)}, FaultAcceptBeforeHorizon, engine.ErrBeforeHorizon, 0},
		{"a sequence number that is not above the last", func(e *Engine) { e.w.lastSeq = 5 }, []engine.Record{observe(edge1, 5, "a", 100, 0)}, FaultAcceptStaleSeq, engine.ErrInvalid, 0},
		{"a sequence number that does not rise in the batch", nil, []engine.Record{observe(edge1, 4, "a", 100, 0), observe(edge2, 4, "a", 100, 0)}, FaultAcceptStaleSeq, engine.ErrInvalid, 0},
		{"a subject in a second layer", func(e *Engine) { e.w.layers[edge1] = catalog.L2 }, []engine.Record{otherLayer}, FaultAcceptLayerConflict, engine.ErrInvalid, 0},
		{"a subject in two layers in one batch", nil, []engine.Record{observe(edge1, 3, "a", 100, 0), func() engine.Record { r := otherLayer; r.Seq = 4; return r }()}, FaultAcceptLayerConflict, engine.ErrInvalid, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			scenario := func(e *Engine, srv *fakeServer) error {
				if c.setup != nil {
					c.setup(e)
				}
				err := e.Write(c.batch)
				if !errors.Is(err, c.want) {
					return fmt.Errorf("got %w, want %w", errors.Join(err, errors.New("(accepted)")), c.want)
				}
				if got := srv.statements(); len(got) != 0 {
					return fmt.Errorf("a refused batch sent %v", got)
				}
				if got := e.Counts(); got.PayloadRefusals != c.count || got.Records != 0 || got.LogRows != 0 {
					return fmt.Errorf("counts %+v after a refusal, want %d payload refusals", got, c.count)
				}
				if e.LastSeq() != 0 || len(e.w.newest) != 0 {
					return fmt.Errorf("a refused batch moved the engine: seq %d, %d state keys", e.LastSeq(), len(e.w.newest))
				}
				return nil
			}
			caught(t, doltmodel.ArmCommit, c.fault, committing(), scenario)
		})
	}

	// A payload of exactly the column's size is accepted, and one byte more is not.
	e, _ := fakeEngine(t, doltmodel.ArmCommit, committing())
	if err := e.Write([]engine.Record{exact}); err != nil {
		t.Fatalf("a payload of exactly the capacity: %v", err)
	}
}

// TestAWriteDoesNotChangeTheCallersRecords: the engine reads the batch and keeps nothing of it.
func TestAWriteDoesNotChangeTheCallersRecords(t *testing.T) {
	e, _ := fakeEngine(t, doltmodel.ArmCommit, committing())
	r := observe(edge1, 1, "a", 100, 30)
	r.Through = at(120)
	before := fmt.Sprintf("%+v", r)
	if err := e.Write([]engine.Record{r}); err != nil {
		t.Fatal(err)
	}
	if after := fmt.Sprintf("%+v", r); after != before {
		t.Fatalf("the record changed: %s, was %s", after, before)
	}
}

// TestALargeBatchIsSentInStatementsUnderThePlaceholderLimit: every row is sent once, and no
// statement carries more placeholders than the protocol allows.
func TestALargeBatchIsSentInStatementsUnderThePlaceholderLimit(t *testing.T) {
	e, srv := fakeEngine(t, doltmodel.ArmCommit, committing())
	const n = 3000
	var batch []engine.Record
	for i := range n {
		// A distinct edge for each record, so that each has its own state rows too.
		to := fp("k8s.node", byte(i%250))
		from := fp("k8s.pod", byte(i/250))
		batch = append(batch, observe(engine.EdgeSubject(from, to, catalog.ScheduledOn), uint64(i+1), "a", int64(100+i), 0))
	}
	if err := e.Write(batch); err != nil {
		t.Fatal(err)
	}
	logRows, stateRows := 0, 0
	for i, q := range srv.statements() {
		if got := len(srv.argLog[i]); got > 65535 {
			t.Fatalf("a statement of %d placeholders", got)
		}
		switch kindOf(q) {
		case "INSERT log":
			logRows += len(srv.argLog[i]) / len(logCols)
		case "REPLACE state":
			stateRows += len(srv.argLog[i]) / len(stateCols)
		}
	}
	if logRows != 2*n || stateRows != 2*n {
		t.Fatalf("sent %d log rows and %d state rows for %d edges", logRows, stateRows, n)
	}
	if c := srv.kinds(); len(c) < 8 {
		t.Fatalf("one statement of %d rows: %v", 2*n, c)
	}
}

// TestRetainSendsItsDeletesAndOneCommit: in one transaction, the state rows that have lapsed,
// the log rows older than each key's baseline, the new horizon and one commit, counted from
// the rows the server says it deleted; one that moves nothing sends nothing.
func TestRetainSendsItsDeletesAndOneCommit(t *testing.T) {
	handle := func(q string, a []driver.Value) fakeResult {
		r := committing()(q, a)
		switch kindOf(q) {
		case "DELETE state":
			r.affected = 3
		case "DELETE ent_state":
			r.affected = 1
		case "DELETE log":
			r.affected = 10
		case "DELETE ent_log":
			r.affected = 5
		}
		return r
	}
	e, srv := fakeEngine(t, doltmodel.ArmCommit, handle)
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0)}); err != nil {
		t.Fatal(err)
	}
	srv.reset()
	if err := e.Retain(at(50)); err != nil {
		t.Fatal(err)
	}
	want := []string{"BEGIN", "DELETE state", "DELETE ent_state", "DELETE log", "DELETE ent_log", "UPDATE meta", "CALL DOLT_COMMIT", "COMMIT"}
	if got := srv.kinds(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	if got := srv.statements()[6]; !strings.Contains(got, "'-m', 'retain 50000000000 seq 1'") || !strings.Contains(got, "'--date', '2000-01-01T00:00:02Z'") {
		t.Fatalf("the retention's commit: %s", got)
	}
	if c := e.Counts(); c.StateDeletes != 4 || c.LogDeletes != 15 || c.Commits != 2 {
		t.Fatalf("counts %+v", c)
	}
	cs := e.Commits()
	if len(cs) != 2 || cs[1].Kind != doltmodel.RetentionCommit || cs[1].Token != 1 || cs[1].Ordinal != 2 {
		t.Fatalf("commits %+v", cs)
	}
	// The token maps to the newest commit made at it: the retention's.
	if i := e.byToken[1]; i != 1 {
		t.Fatalf("token 1 maps to commit index %d", i)
	}

	srv.reset()
	for _, h := range []time.Time{at(50), at(10), at(0), time.Unix(-5, 0)} {
		if err := e.Retain(h); err != nil {
			t.Fatal(err)
		}
	}
	if got := srv.statements(); len(got) != 0 {
		t.Fatalf("a retention that does not move the horizon sent %v", got)
	}
	// A horizon past the last instant sends its own log delete, with no bound.
	if err := e.Retain(engine.MaxEventTime.Add(time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	sawUnbounded := false
	for i, q := range srv.statements() {
		if strings.HasPrefix(q, "DELETE l FROM log l") {
			sawUnbounded = !strings.Contains(q, "event_ns <") && len(srv.argLog[i]) == 0
		}
	}
	if !sawUnbounded {
		t.Fatalf("a horizon past the end: %v", srv.statements())
	}
}

// TestRetentionDefects: the baseline and the open state rows each have a defect the statements
// of a retention would show.
func TestRetentionDefects(t *testing.T) {
	scenario := func(want string) func(*Engine, *fakeServer) error {
		return func(e *Engine, srv *fakeServer) error {
			if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0)}); err != nil {
				return err
			}
			srv.reset()
			if err := e.Retain(at(50)); err != nil {
				return err
			}
			joined := strings.Join(srv.statements(), "\n")
			if !strings.Contains(joined, want) {
				return fmt.Errorf("the retention sent:\n%s\nwanted %s", joined, want)
			}
			return nil
		}
	}
	caught(t, doltmodel.ArmCommit, FaultRetainDropsHolding, committing(), scenario("ROW_NUMBER() OVER (PARTITION BY layer"))
	caught(t, doltmodel.ArmCommit, FaultRetainDropsOpenState, committing(), scenario("DELETE FROM state WHERE deadline_ns IS NOT NULL AND"))
}
