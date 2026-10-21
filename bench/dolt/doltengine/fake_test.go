package doltengine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

// A fake server for the tests that need no Dolt: a database/sql driver that records what it
// is sent, checks that each statement has as many arguments as placeholders, and answers
// from a function the test gives it. It stands in for the server so that the engine's own
// logic (what it sends, in what order, and what it does with a failure) is tested without one;
// what Dolt does with the statements is the server tests' business.

// fakeResult is what the fake server answers to one statement.
type fakeResult struct {
	cols     []string
	rows     [][]driver.Value
	affected int64
	err      error
}

// fakeServer records statements and answers them.
type fakeServer struct {
	mu sync.Mutex
	// handle answers a statement; nil answers every statement with no rows.
	handle func(query string, args []driver.Value) fakeResult
	// log is every statement and transaction event, in order: "BEGIN", "COMMIT",
	// "ROLLBACK" and the text of each statement.
	log []string
	// argLog is the arguments of each statement in log (nil for the events).
	argLog [][]driver.Value
	// commitErr, if set, is the error the next COMMIT returns.
	commitErr error
	// onCommit runs when COMMIT is received.
	onCommit func()
}

func (s *fakeServer) record(entry string, args []driver.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entry)
	s.argLog = append(s.argLog, args)
}

func (s *fakeServer) statements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func (s *fakeServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log, s.argLog = nil, nil
}

// kinds returns the log as short labels: the transaction events and the first words of each
// statement, so that a test can assert the order.
func (s *fakeServer) kinds() []string {
	var out []string
	for _, q := range s.statements() {
		out = append(out, kindOf(q))
	}
	return out
}

func kindOf(q string) string {
	switch q {
	case "BEGIN", "COMMIT", "ROLLBACK":
		return q
	}
	f := strings.Fields(q)
	switch {
	case len(f) >= 3 && (f[0] == "INSERT" || f[0] == "REPLACE"):
		return f[0] + " " + f[2]
	case len(f) >= 3 && f[0] == "DELETE":
		for i, w := range f {
			if w == "FROM" && i+1 < len(f) {
				return "DELETE " + f[i+1]
			}
		}
	case len(f) >= 2 && f[0] == "UPDATE":
		return "UPDATE " + f[1]
	case len(f) >= 2 && f[0] == "CALL":
		return "CALL " + strings.SplitN(f[1], "(", 2)[0]
	}
	return strings.ToUpper(f[0])
}

type fakeConnector struct{ s *fakeServer }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{s: c.s}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("use the connector") }

type fakeConn struct{ s *fakeServer }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("the fake server takes only statements with their arguments")
}
func (c *fakeConn) Close() error { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.s.record("BEGIN", nil)
	return fakeTx{c.s}, nil
}

type fakeTx struct{ s *fakeServer }

func (t fakeTx) Commit() error {
	t.s.record("COMMIT", nil)
	if t.s.onCommit != nil {
		t.s.onCommit()
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	err := t.s.commitErr
	t.s.commitErr = nil
	return err
}

func (t fakeTx) Rollback() error {
	t.s.record("ROLLBACK", nil)
	return nil
}

func (c *fakeConn) answer(query string, named []driver.NamedValue) (fakeResult, error) {
	args := make([]driver.Value, len(named))
	for i, n := range named {
		args[i] = n.Value
	}
	if got, want := len(args), strings.Count(query, "?"); got != want {
		return fakeResult{}, fmt.Errorf("fake server: %d arguments for %d placeholders in: %s", got, want, query)
	}
	c.s.record(query, args)
	if c.s.handle == nil {
		return fakeResult{}, nil
	}
	r := c.s.handle(query, args)
	return r, r.err
}

func (c *fakeConn) ExecContext(_ context.Context, query string, named []driver.NamedValue) (driver.Result, error) {
	r, err := c.answer(query, named)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(r.affected), nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, named []driver.NamedValue) (driver.Rows, error) {
	r, err := c.answer(query, named)
	if err != nil {
		return nil, err
	}
	return &fakeRows{cols: r.cols, rows: r.rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// fakeEngine is an engine over a fake server that has the tables, with the meta row of a
// new database and no commits, as setup leaves one, answering every statement it is not
// scripted for with no rows.
func fakeEngine(t *testing.T, arm doltmodel.Arm, handle func(query string, args []driver.Value) fakeResult) (*Engine, *fakeServer) {
	t.Helper()
	srv := &fakeServer{handle: handle}
	db := sql.OpenDB(fakeConnector{srv})
	db.SetMaxOpenConns(1)
	e := newEngine(arm, "fake", db, db)
	t.Cleanup(func() { _ = e.Close() })
	return e, srv
}

// aHash is a commit hash as Dolt prints one.
const aHash = "0123456789abcdefghijklmnopqrstuv"

// hashN is the n-th distinct commit hash a fake server hands out.
func hashN(n int) string {
	const alphabet = "0123456789abcdefghijklmnopqrstuv"
	b := []byte(aHash)
	b[0], b[1] = alphabet[n%32], alphabet[(n/32)%32]
	return string(b)
}
