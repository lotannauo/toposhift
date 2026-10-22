package doltengine

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestTheDiagnosticNeedsTheFrontierOfEveryCommit(t *testing.T) {
	e, _ := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	if _, err := NewCommitTime(e, []int64{1, 2, 3}); err != nil {
		t.Fatalf("three frontiers for three commits: %v", err)
	}
	for name, fs := range map[string][]int64{
		"too few":  {1, 2},
		"too many": {1, 2, 3, 4},
		"none":     nil,
		"falling":  {1, 3, 2},
	} {
		if _, err := NewCommitTime(e, fs); !errors.Is(err, ErrFrontiers) {
			t.Errorf("%s: %v", name, err)
		}
	}
	rows, _ := afterBatches(t, doltmodel.ArmRows, 3, 5, 9)
	if _, err := NewCommitTime(rows, nil); !errors.Is(err, ErrFrontiers) {
		t.Errorf("the arm with no commits: %v", err)
	}
	// What it was given is its own: a caller that changes the slice changes nothing.
	fs := []int64{1, 2, 3}
	c, _ := NewCommitTime(e, fs)
	fs[2] = 99
	if c.frontiers[2] != 3 {
		t.Error("the diagnostic holds the caller's slice")
	}
}

// TestTheCommitAReadIsAnsweredFromIsTheNewestWhoseFrontierIsNotAfterTheInstant: at most the
// token's, at least none.
func TestTheCommitAReadIsAnsweredFromIsTheNewestWhoseFrontierIsNotAfterTheInstant(t *testing.T) {
	// Tokens 3, 5, 9, 12 and frontiers 10, 10, 20, 15 -> not falling: 10, 10, 20, 20.
	e, _ := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9, 12)
	c, err := NewCommitTime(e, []int64{10, 10, 20, 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token uint64
		at    int64
		want  int
	}{
		{engine.Latest, 9, 0},  // before every frontier: no commit
		{engine.Latest, 10, 2}, // the newest of those at 10
		{engine.Latest, 19, 2},
		{engine.Latest, 20, 4}, // every frontier is at or before 20
		{engine.Latest, 1 << 62, 4},
		{5, 1 << 62, 2}, // the token limits it
		{8, 1 << 62, 2}, // a token inside a batch: the commits at or below it
		{2, 1 << 62, 0}, // below the first commit
		{12, 15, 2},     // the frontier limits it
		{9, 20, 3},      // both
		{0, 1 << 62, 0}, // token 0
	} {
		if got := c.ordinal(tc.token, tc.at); got != tc.want {
			t.Errorf("token %d at %d: commit %d, want %d", tc.token, tc.at, got, tc.want)
		}
	}
}

func diagHandle(rows [][]driver.Value) func(string, []driver.Value) fakeResult {
	commit := committing()
	return func(q string, a []driver.Value) fakeResult {
		switch {
		case strings.HasPrefix(q, "SELECT u.ord, u.peer, u.rel, MAX(u.holds)"):
			out := rows
			if strings.Contains(q, "JSON_TABLE") { // a batch's lists are numbered from 1
				out = nil
				for _, r := range rows {
					out = append(out, append([]driver.Value{int64(1)}, r[1:]...))
				}
			}
			return fakeResult{cols: []string{"ord", "peer", "rel", "holds"}, rows: out}
		case strings.HasPrefix(q, "SELECT COALESCE(MAX(IF("):
			return fakeResult{cols: []string{"holds"}, rows: [][]driver.Value{{int64(1)}}}
		}
		return commit(q, a)
	}
}

func TestTheDiagnosticReadsTheStateAsOfTheCommitAndNothingElse(t *testing.T) {
	peer := doltmodel.EncodeFingerprint(pod1)
	e, srv := fakeEngine(t, doltmodel.ArmCommit, diagHandle([][]driver.Value{
		{int64(0), peer, []byte("scheduled_on"), int64(1)},
		{int64(0), peer, []byte("part_of"), int64(0)},
	}))
	for seq := uint64(1); seq <= 3; seq++ {
		if err := e.Write([]engine.Record{observe(edge1, seq, "a", int64(seq), 0)}); err != nil {
			t.Fatal(err)
		}
	}
	c, err := NewCommitTime(e, []int64{1e9, 2e9, 3e9})
	if err != nil {
		t.Fatal(err)
	}
	sc := engine.Current(catalog.L2)

	srv.reset()
	got, err := c.Neighbors(node1, engine.Reverse, at(2), sc)
	if err != nil || len(got) != 1 || got[0].Relation != "scheduled_on" {
		t.Fatalf("%v, %v: the one that holds", got, err)
	}
	q := readStatement(t, srv)
	if strings.Count(q, "AS OF '"+hashN(2)+"'") != 1 || strings.Contains(q, "JOIN log") || strings.Contains(q, "ROW_NUMBER") {
		t.Fatalf("at 2 s the state as of the second commit was to be read, and nothing else:\n%s", q)
	}

	// Before the first frontier nothing is known: no statement, no neighbor, nobody alive.
	srv.reset()
	if got, err := c.Neighbors(node1, engine.Reverse, at(0), sc); err != nil || len(got) != 0 {
		t.Fatalf("%v, %v", got, err)
	}
	if ok, err := c.Alive(pod1, at(0), sc); err != nil || ok {
		t.Fatalf("%v, %v", ok, err)
	}
	if lists, err := c.NeighborsBatch([]identity.Fingerprint{node1, pod1}, engine.Reverse, at(0), sc); err != nil || len(lists) != 2 {
		t.Fatalf("%v, %v", lists, err)
	}
	for _, s := range srv.statements() {
		if s != "BEGIN" && s != "ROLLBACK" {
			t.Fatalf("a read before every commit's frontier sent %q", s)
		}
	}

	// A batch is one statement over JSON_TABLE, as of the commit.
	srv.reset()
	if _, err := c.NeighborsBatch([]identity.Fingerprint{node1, pod1}, engine.Reverse, at(3), sc); err != nil {
		t.Fatal(err)
	}
	q = readStatement(t, srv)
	if !strings.Contains(q, "JSON_TABLE") || strings.Count(q, "AS OF '"+hashN(3)+"'") != 1 {
		t.Fatalf("a batch:\n%s", q)
	}

	// Existence reads the entity state as of the commit.
	srv.reset()
	if ok, err := c.Alive(pod1, at(2), sc); err != nil || !ok {
		t.Fatalf("%v, %v", ok, err)
	}
	if q := readStatement(t, srv); !strings.Contains(q, "FROM ent_state AS OF '"+hashN(2)+"'") || strings.Contains(q, "ent_log") {
		t.Fatalf("alive:\n%s", q)
	}

	// Only these reads are the diagnostic's, and they are counted by the model's names, those
	// before every frontier too (they were asked); a window is the design's and is counted as
	// its own.
	if _, err := c.Window(pod1, engine.Forward, at(0), at(10), sc); err != nil {
		t.Fatal(err)
	}
	p := c.Paths()
	if p["neighbors/commit-time"] != 2 || p["alive/commit-time"] != 2 || p["batch/commit-time"] != 2 || p["window/log"] != 1 || len(p) != 4 {
		t.Fatalf("paths %v", p)
	}
}

func TestTheDiagnosticIsReadOnly(t *testing.T) {
	e, _ := afterBatches(t, doltmodel.ArmCommit, 3)
	c, err := NewCommitTime(e, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(c.Write([]engine.Record{observe(edge1, 4, "a", 4, 0)}), ErrReadOnly) || !errors.Is(c.Retain(at(1)), ErrReadOnly) {
		t.Fatal("the diagnostic accepted a write")
	}
	if c.LastSeq() != 3 || c.Close() != nil {
		t.Fatal("the diagnostic does not stand for its engine")
	}
}

func TestTheDefectThatMakesTheDiagnosticReadTheDesignWayIsSwitchedOnByName(t *testing.T) {
	e, srv := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	c, err := NewCommitTime(e, []int64{1e9, 2e9, 3e9})
	if err != nil {
		t.Fatal(err)
	}
	c.Break(FaultDiagUsesLog)
	srv.reset()
	if _, err := c.Neighbors(node1, engine.Reverse, at(2), engine.Current(catalog.L2)); err != nil {
		t.Fatal(err)
	}
	if q := readStatement(t, srv); !strings.Contains(q, "FROM state s") || !strings.Contains(q, "LEFT JOIN log l") {
		t.Fatalf("the defective diagnostic did not read as the design:\n%s", q)
	}
	defer func() {
		if recover() == nil {
			t.Error("a fault that does not exist was switched on")
		}
	}()
	c.Break("no such fault")
}
