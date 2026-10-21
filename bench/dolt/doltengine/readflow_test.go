package doltengine

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Reads against the fake server: which state each token is read from, what the statement
// carries, and what the engine counts. What Dolt answers is the server tests'.

// afterBatches is an engine, over the fake, that has committed batches ending at the given
// tokens, with the commits' hashes hashN(1), hashN(2)...
func afterBatches(t *testing.T, arm doltmodel.Arm, tokens ...uint64) (*Engine, *fakeServer) {
	t.Helper()
	e, srv := fakeEngine(t, arm, committing())
	prev := uint64(0)
	for _, tok := range tokens {
		var batch []engine.Record
		for s := prev + 1; s <= tok; s++ {
			batch = append(batch, observe(edge1, s, "a", int64(s), 0))
		}
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
		prev = tok
	}
	srv.reset()
	return e, srv
}

// readStatement is the one statement a read sent, and whether the read was a transaction of
// its own that was read-only.
func readStatement(t *testing.T, srv *fakeServer) string {
	t.Helper()
	got := srv.statements()
	if len(got) != 3 || got[0] != "BEGIN" || got[2] != "ROLLBACK" {
		t.Fatalf("a read sent %v: it must be one statement in one transaction", got)
	}
	return got[1]
}

func TestAReadIsAnsweredFromTheStateItsTokenNames(t *testing.T) {
	e, srv := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	for _, c := range []struct {
		name    string
		token   uint64
		wantAs  string // the commit hash the state is read as of, or "" for none
		wantLog bool   // read from the log alone
	}{
		{"the newest token reads the current state", 9, "", false},
		{"a token above the newest, as Latest is", engine.Latest, "", false},
		{"an old batch boundary reads that commit", 3, hashN(1), false},
		{"another", 5, hashN(2), false},
		{"a token inside a batch names no commit", 4, "", true},
		{"a token before the first commit's", 1, "", true},
		{"token 0", 0, "", true},
	} {
		srv.reset()
		if _, err := e.Neighbors(node1, engine.Reverse, at(100), engine.Scope{Layer: catalog.L2, AsOf: c.token}); err != nil {
			t.Fatal(err)
		}
		q := readStatement(t, srv)
		switch {
		case c.wantLog:
			if strings.Contains(q, "state") || strings.Contains(q, "AS OF") {
				t.Errorf("%s: a read with no commit to read touched the state:\n%s", c.name, q)
			}
		case c.wantAs != "":
			if strings.Count(q, "AS OF '"+c.wantAs+"'") != 2 {
				t.Errorf("%s: not as of %s:\n%s", c.name, c.wantAs, q)
			}
		default:
			if strings.Contains(q, "AS OF") || !strings.Contains(q, "FROM state s") {
				t.Errorf("%s: a read of the current state:\n%s", c.name, q)
			}
		}
	}
}

func TestTheDefectsOfReadingAnOldTokenAreCaught(t *testing.T) {
	scenario := func(e *Engine, srv *fakeServer) error {
		srv.reset()
		if _, err := e.Neighbors(node1, engine.Reverse, at(100), engine.Scope{Layer: catalog.L2, AsOf: 3}); err != nil {
			return err
		}
		if q := srv.statements()[1]; strings.Count(q, "AS OF '"+hashN(1)+"'") != 2 {
			return fmt.Errorf("not as of the commit at token 3:\n%s", q)
		}
		return nil
	}
	for _, fault := range []string{FaultCommitAfter, FaultHeadForOldToken} {
		t.Run(fault, func(t *testing.T) {
			mk := func(brk string) (*Engine, *fakeServer) {
				e, srv := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
				if brk != "" {
					e.Break(brk)
				}
				return e, srv
			}
			if e, srv := mk(""); scenario(e, srv) != nil {
				t.Fatal(scenario(e, srv))
			}
			if e, srv := mk(fault); scenario(e, srv) == nil {
				t.Fatalf("an engine that %s passed", fault)
			}
		})
	}
}

func TestTheArmWithNoCommitsReadsOldTokensFromTheLog(t *testing.T) {
	e, srv := afterBatches(t, doltmodel.ArmRows, 3, 5, 9)
	if _, err := e.Neighbors(node1, engine.Reverse, at(100), engine.Scope{Layer: catalog.L2, AsOf: 3}); err != nil {
		t.Fatal(err)
	}
	if q := readStatement(t, srv); strings.Contains(q, "state") || strings.Contains(q, "AS OF") {
		t.Errorf("the arm with no commits read the state for an old token:\n%s", q)
	}
	srv.reset()
	if _, err := e.Neighbors(node1, engine.Reverse, at(100), engine.Current(catalog.L2)); err != nil {
		t.Fatal(err)
	}
	if q := readStatement(t, srv); !strings.Contains(q, "FROM state s") {
		t.Errorf("the arm with no commits did not read the current state for the newest token:\n%s", q)
	}
}

// TestACountedReadFollowsTheRowsTheServerReturns: the keys settled and the keys sent to the
// log are summed over every row, whether or not the key holds; the answer is the rows that
// hold, in the contract's order, whatever order the server returned.
func TestACountedReadFollowsTheRowsTheServerReturns(t *testing.T) {
	peerA, peerB := doltmodel.EncodeFingerprint(pod2), doltmodel.EncodeFingerprint(pod1) // pod1 sorts before pod2
	handle := func(q string, a []driver.Value) fakeResult {
		if strings.HasPrefix(q, "SELECT u.ord") {
			return fakeResult{
				cols: []string{"ord", "peer", "rel", "keys_state", "keys_log", "holds"},
				rows: [][]driver.Value{
					{int64(0), peerA, []byte("scheduled_on"), []byte("2"), []byte("1"), int64(1)},
					{int64(0), peerB, []byte("scheduled_on"), []byte("1"), []byte("0"), int64(1)},
					{int64(0), peerB, []byte("part_of"), []byte("0"), []byte("3"), int64(0)},
				},
			}
		}
		return committing()(q, a)
	}
	e, _ := fakeEngine(t, doltmodel.ArmCommit, handle)
	for seq := uint64(1); seq <= 2; seq++ {
		if err := e.Write([]engine.Record{observe(edge1, seq, "a", int64(seq), 0)}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := e.Neighbors(node1, engine.Reverse, at(100), engine.Scope{Layer: catalog.L2, AsOf: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Peer != pod1 || got[1].Peer != pod2 {
		t.Fatalf("neighbors %+v: the two that hold, pod1 before pod2", got)
	}
	p := e.Paths()
	if p["neighbors/keys-state"] != 3 || p["neighbors/keys-log"] != 4 || p["neighbors/needed-log"] != 1 || p["neighbors/as-of-alone"] != 0 {
		t.Errorf("paths %v", p)
	}
}

// TestReadsBeforeTheRangeAskNothing: an instant before 1970 is answered without a statement
// and counted as nothing; a batch of no entities likewise sends nothing but is counted.
func TestReadsBeforeTheRangeAskNothing(t *testing.T) {
	e, srv := afterBatches(t, doltmodel.ArmCommit, 2)
	before := at(-5)
	if ns, err := e.Neighbors(node1, engine.Reverse, before, engine.Current(catalog.L2)); err != nil || len(ns) != 0 {
		t.Fatal(ns, err)
	}
	if ok, err := e.Alive(pod1, before, engine.Current(catalog.L2)); err != nil || ok {
		t.Fatal(ok, err)
	}
	lists, err := e.NeighborsBatch([]identity.Fingerprint{pod1, pod2}, engine.Reverse, before, engine.Current(catalog.L2))
	if err != nil || len(lists) != 2 || lists[0] != nil || lists[1] != nil {
		t.Fatal(lists, err)
	}
	if ws, err := e.Window(pod1, engine.Forward, at(10), at(5), engine.Current(catalog.L2)); err != nil || ws != nil {
		t.Fatal(ws, err)
	}
	if len(srv.statements()) != 0 || len(e.Paths()) != 0 {
		t.Fatalf("sent %v, counted %v", srv.statements(), e.Paths())
	}
	lists, err = e.NeighborsBatch(nil, engine.Reverse, at(10), engine.Current(catalog.L2))
	if err != nil || len(lists) != 0 || len(srv.statements()) != 0 || e.Paths()["batch/as-of-alone"] != 1 {
		t.Fatalf("an empty batch: %v, %v, sent %v, counted %v", lists, err, srv.statements(), e.Paths())
	}
	if _, err := e.Neighbors(node1, engine.Reverse, at(10), engine.Scope{}); err == nil {
		t.Fatal("a scope with no layer was read")
	}
}
