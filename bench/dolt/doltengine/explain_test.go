package doltengine

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestLiteralsAreWrittenAsTheServerReadsThem(t *testing.T) {
	for _, c := range []struct {
		arg  any
		want string
	}{
		{int(7), "7"},
		{int64(-9223372036854775808), "-9223372036854775808"},
		{uint64(18446744073709551615), "18446744073709551615"},
		{[]byte{0x00, 0xAB, 0x7f}, "0x00ab7f"},
		{[]byte{}, "X''"},
		{"a'b", `'a''b'`},
		{`a\b`, `'a\\b'`},
		{`["61","62"]`, `'["61","62"]'`},
	} {
		got, err := Literal(c.arg)
		if err != nil || got != c.want {
			t.Errorf("Literal(%#v) = %q, %v; want %q", c.arg, got, err, c.want)
		}
	}
	for _, bad := range []any{1.5, nil, true, time.Now(), []string{"x"}} {
		if got, err := Literal(bad); err == nil {
			t.Errorf("Literal(%#v) = %q: an argument no read sends was written", bad, got)
		}
	}
}

func TestAStatementIsInterpolatedPlaceholderForArgument(t *testing.T) {
	s := Statement{Text: "SELECT a FROM t WHERE x = ? AND y = ? AND z = ?", Args: []any{int64(1), []byte{0xff}, "q"}}
	got, err := s.Interpolate()
	if want := "SELECT a FROM t WHERE x = 1 AND y = 0xff AND z = 'q'"; err != nil || got != want {
		t.Fatalf("%q, %v; want %q", got, err, want)
	}
	// As many arguments as placeholders, or nothing is written.
	for _, bad := range []Statement{
		{Text: "SELECT ?", Args: nil},
		{Text: "SELECT 1", Args: []any{int64(1)}},
		{Text: "SELECT ?, ?", Args: []any{int64(1)}},
	} {
		if got, err := bad.Interpolate(); err == nil {
			t.Errorf("%+v was written as %q", bad, got)
		}
	}
	// An argument that a literal cannot be made of refuses the statement.
	if _, err := (Statement{Text: "SELECT ?", Args: []any{3.5}}).Interpolate(); err == nil {
		t.Error("a float argument was written")
	}
}

// TestNoStatementOfARead holds a question mark that is not a placeholder: Interpolate counts
// them, so one in a string literal would shift every argument after it.
func TestNoStatementOfAReadHoldsAQuestionMarkThatIsNotAPlaceholder(t *testing.T) {
	e, _ := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	for _, q := range everyKindOfRead() {
		sts, err := e.ReadStatements(q)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sts {
			if n := strings.Count(s.Text, "?"); n != len(s.Args) {
				t.Errorf("%s: %d question marks, %d arguments:\n%s", q.Name(), n, len(s.Args), s.Text)
			}
			if _, err := s.Interpolate(); err != nil {
				t.Errorf("%s: %v", q.Name(), err)
			}
		}
	}
}

func everyKindOfRead() []runner.Query {
	var out []runner.Query
	for _, token := range []uint64{9, 5, 4} { // the newest, a boundary, inside a batch
		out = append(out,
			runner.Query{Group: "n", Op: runner.OpNeighbors, Layer: catalog.L2, Dir: 2, Fps: []identity.Fingerprint{node1}, At: at(100), AsOf: token},
			runner.Query{Group: "b", Op: runner.OpBatch, Layer: catalog.L2, Dir: 2, Fps: []identity.Fingerprint{node1, pod1}, At: at(100), AsOf: token},
			runner.Query{Group: "a", Op: runner.OpAlive, Layer: catalog.L2, Fps: []identity.Fingerprint{pod1}, At: at(100), AsOf: token},
			runner.Query{Group: "w", Op: runner.OpWindow, Layer: catalog.L2, Dir: 1, Fps: []identity.Fingerprint{pod1}, From: at(10), To: at(50), AsOf: token},
			runner.Query{Group: "w", Op: runner.OpWindow, Layer: catalog.L2, Dir: 1, Fps: []identity.Fingerprint{pod1}, From: at(10), To: time.Unix(0, 1<<63-1).UTC(), AsOf: token},
		)
	}
	return out
}

// TestTheStatementsOfAQuestionAreThoseTheReadSends: what ReadStatements returns is, text and
// arguments, what the engine sends to answer the question, for every kind of read and the
// token's every view. (The reads and the statements are built by one function each.)
func TestTheStatementsOfAQuestionAreThoseTheReadSends(t *testing.T) {
	for _, arm := range []doltmodel.Arm{doltmodel.ArmCommit, doltmodel.ArmRows} {
		e, srv := afterBatches(t, arm, 3, 5, 9)
		srv.handle = func(q string, _ []driver.Value) fakeResult {
			switch {
			case strings.HasPrefix(q, "SELECT COUNT(*) AS keys_n, COALESCE(SUM"):
				return fakeResult{cols: []string{"a", "b", "c"}, rows: [][]driver.Value{{int64(0), int64(0), int64(0)}}}
			case strings.HasPrefix(q, "SELECT COUNT(*) AS keys_n"):
				return fakeResult{cols: []string{"a", "b"}, rows: [][]driver.Value{{int64(0), int64(0)}}}
			}
			return fakeResult{}
		}
		for _, q := range everyKindOfRead() {
			t.Run(fmt.Sprintf("%s/%s/token %d", arm, q.Op, q.AsOf), func(t *testing.T) {
				sts, err := e.ReadStatements(q)
				if err != nil || len(sts) != 1 {
					t.Fatalf("%d statements, %v", len(sts), err)
				}
				srv.reset()
				if _, err := runner.Ask(e, q); err != nil {
					t.Fatal(err)
				}
				srv.mu.Lock()
				defer srv.mu.Unlock()
				if len(srv.log) != 3 {
					t.Fatalf("a read sent %v", srv.log)
				}
				if got := srv.log[1]; got != sts[0].Text {
					t.Fatalf("the read sent\n%s\nReadStatements says\n%s", got, sts[0].Text)
				}
				if got, want := fmt.Sprint(srv.argLog[1]), fmt.Sprint(driverValues(sts[0].Args)); got != want {
					t.Fatalf("the read sent arguments %s, ReadStatements says %s", got, want)
				}
			})
		}
	}
}

func driverValues(args []any) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case int:
			out[i] = int64(v)
		case uint64:
			out[i] = int64(v)
		default:
			out[i] = v
		}
	}
	return out
}

func TestTheKindOfAStatementSaysWhichViewTheTokenGot(t *testing.T) {
	e, _ := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	want := map[uint64][2]string{9: {"rule", "alive-rule"}, 5: {"rule", "alive-rule"}, 4: {"log", "alive-log"}}
	for token, kinds := range want {
		for i, op := range []runner.Op{runner.OpNeighbors, runner.OpAlive} {
			q := runner.Query{Op: op, Layer: catalog.L2, Dir: 2, Fps: []identity.Fingerprint{node1}, At: at(100), AsOf: token}
			sts, err := e.ReadStatements(q)
			if err != nil || len(sts) != 1 || sts[0].Kind != kinds[i] {
				t.Errorf("token %d, %s: %+v, %v; want a %s statement", token, op, sts, err, kinds[i])
			}
		}
	}
	// A read before the range, a window with no extent and a question with no entity ask
	// nothing; so does a question the engine would refuse.
	for _, q := range []runner.Query{
		{Op: runner.OpNeighbors, Layer: catalog.L2, Dir: 2, Fps: []identity.Fingerprint{node1}, At: at(-5), AsOf: 9},
		{Op: runner.OpWindow, Layer: catalog.L2, Dir: 1, Fps: []identity.Fingerprint{pod1}, From: at(50), To: at(10), AsOf: 9},
		{Op: runner.OpAlive, Layer: catalog.L2, At: at(10), AsOf: 9},
	} {
		if sts, err := e.ReadStatements(q); err != nil || len(sts) != 0 {
			t.Errorf("%+v: %+v, %v", q, sts, err)
		}
	}
	if _, err := e.ReadStatements(runner.Query{Op: runner.OpNeighbors, Fps: []identity.Fingerprint{node1}, At: at(10)}); err == nil {
		t.Error("a question in no layer has statements")
	}
	if _, err := e.ReadStatements(runner.Query{Op: "walk", Layer: catalog.L2, Fps: []identity.Fingerprint{node1}, At: at(10)}); err == nil {
		t.Error("a read the engine does not have has statements")
	}
}

// TestExplainSendsTheStatementWithItsArgumentsWritten: EXPLAIN takes none as arguments, and
// it is sent in a read-only transaction of the reader's.
func TestExplainSendsTheStatementWithItsArgumentsWritten(t *testing.T) {
	e, srv := afterBatches(t, doltmodel.ArmCommit, 3, 5, 9)
	srv.handle = func(q string, _ []driver.Value) fakeResult {
		if strings.HasPrefix(q, "EXPLAIN FORMAT=TREE ") {
			return fakeResult{cols: []string{"plan"}, rows: [][]driver.Value{{"Project"}, {" └─ IndexedTableAccess(state)"}}}
		}
		return fakeResult{}
	}
	q := runner.Query{Op: runner.OpAlive, Layer: catalog.L2, Fps: []identity.Fingerprint{pod1}, At: at(100), AsOf: 5}
	sts, err := e.ReadStatements(q)
	if err != nil {
		t.Fatal(err)
	}
	srv.reset()
	lines, err := e.Explain(t.Context(), sts[0])
	if err != nil || len(lines) != 2 || lines[0] != "Project" {
		t.Fatalf("%v, %v", lines, err)
	}
	text, _ := sts[0].Interpolate()
	got := srv.statements()
	if len(got) != 3 || got[0] != "BEGIN" || got[1] != "EXPLAIN FORMAT=TREE "+text || got[2] != "ROLLBACK" {
		t.Fatalf("explain sent %v", got)
	}
	if strings.Contains(got[1], "?") {
		t.Fatalf("a placeholder was left in %s", got[1])
	}
	if args := srv.argLog[1]; len(args) != 0 {
		t.Fatalf("explain was sent arguments %v", args)
	}
	// A statement that cannot be written is refused before anything is sent.
	srv.reset()
	if _, err := e.Explain(t.Context(), Statement{Text: "SELECT ?"}); err == nil || len(srv.statements()) != 0 {
		t.Fatalf("a statement with a missing argument: %v, sent %v", err, srv.statements())
	}
}
