package xtengine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The tests in this file ask a live XTDB node what the model and the engine assume of
// it: each is one of the behaviours the package documentation lists as established,
// and each fails if the node behaves otherwise. The node is the one
// scripts/xt-server.sh starts, and TOPOSHIFT_XTDB_DSN names it (a pgx connection string);
// without it they skip.
//
// The node orders every transaction by system time, whatever table it touches, and
// refuses one at a system time before the last it committed. The tests that write at
// the system times of the baseline (2000-01-01 and the microseconds after it) must
// therefore run before any test that writes at a later one, on a node that no earlier
// test has written to; go runs the tests of a package in the order of their files, and
// this file's name sorts before the others that write.

// pgxMain is the query mode of the probes that do not ask about modes: the one the engine
// uses.
const pgxMain = pgx.QueryExecModeSimpleProtocol

const dsnEnv = "TOPOSHIFT_XTDB_DSN"

// serverDSN returns the node to test against, or skips.
func serverDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set: no XTDB node to test against (scripts/xt-server.sh starts one)", dsnEnv)
	}
	return dsn
}

// probeTable is a table name for one probe of one run: the node keeps every table it
// is ever given, so a probe takes a name no other has.
func probeTable(t testing.TB, name string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(t.TempDir()))
	return "pr" + hex.EncodeToString(sum[:4]) + "_" + name
}

// connect opens a connection to the node in a query mode.
func connect(t testing.TB, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cc, err := pgx.ParseConfig(serverDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	cc.DefaultQueryExecMode = mode
	c, err := pgx.ConnectConfig(context.Background(), cc)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// txAt runs statements in one read-write transaction at a system time, and returns the
// error of the first that fails, or of the commit. A transaction that fails is rolled
// back.
func txAt(c *pgx.Conn, sys time.Time, stmts ...stmt) error {
	ctx := context.Background()
	simple := pgx.QueryExecModeSimpleProtocol
	if _, err := c.Exec(ctx, "BEGIN READ WRITE WITH (SYSTEM_TIME = "+literal(sys)+")", simple); err != nil {
		return fmt.Errorf("begin: %s", pgDetail(err))
	}
	for _, s := range stmts {
		if _, err := c.Exec(ctx, s.sql, s.args...); err != nil {
			_, _ = c.Exec(ctx, "ROLLBACK", simple)
			return fmt.Errorf("%.80s: %s", s.sql, pgDetail(err))
		}
	}
	if _, err := c.Exec(ctx, "COMMIT", simple); err != nil {
		return fmt.Errorf("commit: %s", pgDetail(err))
	}
	return nil
}

// queryAll runs a query and returns each row's values, rendered as text.
func queryAll(c *pgx.Conn, sql string, args ...any) ([][]string, error) {
	rows, err := c.Query(context.Background(), sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = fmt.Sprint(v)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// pgDetail is an error with what the node put in its fields: XTDB sends its message in the
// detail, as JSON.
func pgDetail(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return fmt.Sprintf("%s %s %s", pe.Code, pe.Message, pe.Detail)
	}
	return fmt.Sprint(err)
}

func at(micros int64) time.Time { return time.UnixMicro(micros).UTC() }

func show(rows [][]string) string {
	var parts []string
	for _, r := range rows {
		parts = append(parts, "("+strings.Join(r, ",")+")")
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func ins(table string, id string, c int64, from, to any) stmt {
	return stmt{
		sql:  "INSERT INTO " + table + " (_id, c, _valid_from, _valid_to) VALUES ($1, CAST($2 AS BIGINT), $3, $4)",
		args: []any{id, c, from, to},
	}
}

func del(table, id string, from, to any) stmt {
	return stmt{sql: "DELETE FROM " + table + " FOR PORTION OF VALID_TIME FROM $1 TO $2 WHERE _id = $3", args: []any{from, to, id}}
}

// vt is the read of one document's column c at a valid time, as an answer: the value, or
// "absent".
func vtRead(c *pgx.Conn, table, id string, valid time.Time, sys *time.Time) (string, error) {
	sql := "SELECT c FROM " + table + " FOR VALID_TIME AS OF $1"
	args := []any{valid}
	if sys != nil {
		sql += " FOR SYSTEM_TIME AS OF $2"
		args = append(args, *sys)
	}
	sql += " WHERE _id = $" + fmt.Sprint(len(args)+1)
	args = append(args, id)
	rows, err := queryAll(c, sql, args...)
	if err != nil {
		return "", err
	}
	switch len(rows) {
	case 0:
		return "absent", nil
	case 1:
		return rows[0][0], nil
	}
	return "", fmt.Errorf("%d rows: %s", len(rows), show(rows))
}

// probeSys hands out system times for the probes, one microsecond apart from
// 2000-01-01, so that every transaction of the probes is later than the one before.
var probeSys = struct{ next int64 }{next: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() + 1}

func nextSys() time.Time {
	t := at(probeSys.next)
	probeSys.next++
	return t
}

// observe logs what a probe found, in a form the package documentation quotes.
func observe(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("OBSERVED "+format, args...)
}

// TestProbeBeginForms: the forms of the statement that opens a transaction at a system time,
// each opened and rolled back, so that none commits.
func TestProbeBeginForms(t *testing.T) {
	c := connect(t, pgxMain)
	ctx := context.Background()
	simple := pgx.QueryExecModeSimpleProtocol
	sys := time.Date(2000, 1, 1, 0, 0, 0, 1000, time.UTC)
	for _, f := range []struct {
		name string
		sql  string
		args []any
		mode pgx.QueryExecMode
	}{
		{"a TIMESTAMP literal, simple", "BEGIN READ WRITE WITH (SYSTEM_TIME = " + literal(sys) + ")", nil, simple},
		{"a TIMESTAMP literal, exec", "BEGIN READ WRITE WITH (SYSTEM_TIME = " + literal(sys) + ")", nil, pgx.QueryExecModeExec},
		{"a string literal, simple", "BEGIN READ WRITE WITH (SYSTEM_TIME = '2000-01-01T00:00:00.000001Z')", nil, simple},
		{"a TIMESTAMP literal with an offset, simple", "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2000-01-01 00:00:00.000001+00:00')", nil, simple},
		{"an argument, exec", "BEGIN READ WRITE WITH (SYSTEM_TIME = $1)", []any{sys}, pgx.QueryExecModeExec},
		{"a read-only begin, simple", "BEGIN READ ONLY", nil, simple},
	} {
		args := append([]any{f.mode}, f.args...)
		_, err := c.Exec(ctx, f.sql, args...)
		_, rerr := c.Exec(ctx, "ROLLBACK", simple)
		observe(t, "%s: begin %v, rollback %v", f.name, err, rerr)
	}
}

// TestProbeSystemTimeBeforeTheClock: a transaction may be given a system time years before
// the node's clock, and a read FOR SYSTEM_TIME AS OF a system time includes the transaction
// committed at it and not the one a microsecond later. It runs first: the node refuses a
// system time before the last it committed.
func TestProbeSystemTimeBeforeTheClock(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "sys")
	rows, err := queryAll(c, "SELECT * FROM xt.txs ORDER BY _id DESC LIMIT 1")
	if err != nil {
		observe(t, "reading xt.txs: %v", err)
	} else if len(rows) > 0 {
		observe(t, "the newest transaction before the probes: %s", show(rows))
		t.Skipf("the node already holds transactions; the probes of system time need a node that has none (scripts/xt-server.sh with an empty data directory)")
	}
	s1, s2 := nextSys(), nextSys()
	if err := txAt(c, s1, ins(table, "d", 1, at(10), nil)); err != nil {
		t.Fatalf("a transaction at %s was refused: %v", s1.Format(time.RFC3339Nano), err)
	}
	if err := txAt(c, s2, ins(table, "d", 2, at(10), nil)); err != nil {
		t.Fatalf("a second transaction at %s was refused: %v", s2.Format(time.RFC3339Nano), err)
	}
	before := s1.Add(-time.Microsecond)
	for _, tc := range []struct {
		name string
		sys  *time.Time
		want string
	}{
		{"as of the first", &s1, "1"},
		{"as of the second", &s2, "2"},
		{"as of a microsecond before the first", &before, "absent"},
		{"newest", nil, "2"},
	} {
		got, err := vtRead(c, table, "d", at(20), tc.sys)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: read %s, want %s", tc.name, got, tc.want)
		}
	}
	clock, err := queryAll(c, "SELECT current_timestamp")
	observe(t, "an explicit system time of %s was accepted; the node's clock reads %s (%v)", s1.Format(time.RFC3339Nano), show(clock), err)
	// The system time of a version is readable, to the microsecond.
	cols, err := queryAll(c, "SELECT _system_from, _system_to FROM "+table+" FOR VALID_TIME ALL FOR SYSTEM_TIME ALL WHERE _id = $1 ORDER BY _system_from", "d")
	observe(t, "the versions of the document by system time: %s (%v)", show(cols), err)
}

// TestProbeQueryModes: the statements the engine sends, in each query mode of the driver:
// which the node accepts. A write names the types of its arguments to the node (a parse
// message with their OIDs), and the driver's default and its describe mode ask the node to
// describe them, which it does with no parameters; its exec mode names no type. Only the
// simple protocol, which writes the arguments into the text of the statement, is accepted.
func TestProbeQueryModes(t *testing.T) {
	for _, m := range []struct {
		name string
		mode pgx.QueryExecMode
		ok   bool
	}{
		{"cache (the driver's default)", pgx.QueryExecModeCacheStatement, false},
		{"describe", pgx.QueryExecModeCacheDescribe, false},
		{"exec", pgx.QueryExecModeExec, false},
		{"simple", pgx.QueryExecModeSimpleProtocol, true},
	} {
		t.Run(m.name, func(t *testing.T) {
			c := connect(t, m.mode)
			table := probeTable(t, "mode")
			stmts := []stmt{
				{
					sql:  "INSERT INTO " + table + " (_id, i, s, t, y, n, _valid_from, _valid_to) VALUES ($1, CAST($2 AS BIGINT), $3, $4, CAST($5 AS VARBINARY), $6, $7, $8)",
					args: []any{"k1", int64(-9223372036854775808), "text", at(1_000_000), "000102ff", nil, at(5), nil},
				},
			}
			err := txAt(c, nextSys(), stmts...)
			observe(t, "mode %q, a write of an int, a string, a timestamp, bytes, a null and a null end of valid time: %v", m.name, err)
			if (err == nil) != m.ok {
				t.Errorf("mode %q: accepted %v, want %v", m.name, err == nil, m.ok)
			}
			if err != nil {
				return
			}
			var i int64
			var s string
			var y []byte
			err = c.QueryRow(context.Background(), "SELECT i, s, y FROM "+table+" FOR VALID_TIME AS OF $1 WHERE _id = $2", at(6), "k1").Scan(&i, &s, &y)
			observe(t, "mode %q, the read back: %d %s %x (%v)", m.name, i, s, y, err)
			if err != nil || i != -9223372036854775808 || s != "text" || !bytes.Equal(y, []byte{0, 1, 2, 255}) {
				t.Errorf("read back %d %q %x: %v", i, s, y, err)
			}
		})
	}
}

// TestProbeStatementOrder: statements of one transaction apply in the order they are given,
// so a later statement over an overlapping stretch of valid time wins; and the states in
// the middle of the transaction leave no history in system time.
func TestProbeStatementOrder(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "order")
	sys := nextSys()
	// Present over [10, 100), absent over [20, 30), present again over [25, 40): the
	// document is absent over [20, 25) and nowhere else after [10, 100).
	err := txAt(c, sys,
		ins(table, "d", 1, at(10), at(100)),
		del(table, "d", at(20), at(30)),
		ins(table, "d", 1, at(25), at(40)),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		valid int64
		want  string
	}{{9, "absent"}, {10, "1"}, {19, "1"}, {20, "absent"}, {24, "absent"}, {25, "1"}, {29, "1"}, {30, "1"}, {39, "1"}, {40, "1"}, {99, "1"}, {100, "absent"}} {
		got, err := vtRead(c, table, "d", at(tc.valid), nil)
		if err != nil {
			t.Fatalf("valid time %d: %v", tc.valid, err)
		}
		if got != tc.want {
			t.Errorf("valid time %d: %s, want %s (statements applied out of order, or merged)", tc.valid, got, tc.want)
		}
	}
	rows, err := queryAll(c, "SELECT _valid_from, _valid_to, _system_from, _system_to FROM "+table+" FOR VALID_TIME ALL FOR SYSTEM_TIME ALL WHERE _id = $1 ORDER BY _valid_from, _system_from", "d")
	observe(t, "the versions after the transaction: %s (%v)", show(rows), err)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r[3] != "<nil>" {
			t.Errorf("a version closed in system time at %s by the transaction that made it: the states in the middle of a transaction left history", r[3])
		}
	}
}

// TestProbeInsertOverwritesOnlyItsRange: an insert on an existing _id replaces the document
// over the valid-time stretch it names and leaves the rest; before it, the system time
// shows the document as it was.
func TestProbeInsertOverwritesOnlyItsRange(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "overwrite")
	s1, s2 := nextSys(), nextSys()
	if err := txAt(c, s1, ins(table, "d", 1, at(10), at(100))); err != nil {
		t.Fatal(err)
	}
	if err := txAt(c, s2, ins(table, "d", 2, at(50), at(60))); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		valid int64
		sys   *time.Time
		want  string
	}{
		{40, nil, "1"},
		{55, nil, "2"},
		{70, nil, "1"},
		{60, nil, "1"},
		{100, nil, "absent"},
		{55, &s1, "1"},
		{55, &s2, "2"},
		{70, &s1, "1"},
	} {
		got, err := vtRead(c, table, "d", at(tc.valid), tc.sys)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			which := "newest"
			if tc.sys != nil {
				which = tc.sys.Format("15:04:05.000000")
			}
			t.Errorf("valid time %d, system time %s: %s, want %s", tc.valid, which, got, tc.want)
		}
	}
}

// TestProbePortionDelete: a portion delete of a document that is not there, in a table that
// is, and in a table that is not, commits and changes nothing; a portion delete with a null
// end reaches the end of time.
func TestProbePortionDelete(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "portion")
	if err := txAt(c, nextSys(), ins(table, "d", 1, at(10), nil)); err != nil {
		t.Fatal(err)
	}
	if err := txAt(c, nextSys(), del(table, "absent", at(20), at(30))); err != nil {
		t.Errorf("a portion delete of a document that is not there: %v", err)
	}
	if err := txAt(c, nextSys(), del(probeTable(t, "no_such_table"), "d", at(20), at(30))); err != nil {
		t.Errorf("a portion delete in a table that does not exist: %v", err)
	}
	if err := txAt(c, nextSys(), del(table, "d", at(20), nil)); err != nil {
		t.Fatalf("a portion delete with a null end: %v", err)
	}
	for _, tc := range []struct {
		valid int64
		want  string
	}{{19, "1"}, {20, "absent"}, {1 << 50, "absent"}} {
		got, err := vtRead(c, table, "d", at(tc.valid), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("valid time %d: %s, want %s", tc.valid, got, tc.want)
		}
	}
	// A read of a table that does not exist is an empty answer, not an error.
	rows, err := queryAll(c, "SELECT c FROM "+probeTable(t, "never_written")+" FOR VALID_TIME ALL")
	observe(t, "a read of a table that was never written: %s (%v)", show(rows), err)
	if err != nil || len(rows) != 0 {
		t.Errorf("a read of a table that does not exist: %v, %v", rows, err)
	}
}

// TestProbeCommitIsSynchronous: when a commit has returned, a read on another connection that
// asks for no clause sees the transaction, every time; COMMIT SYNC is not a statement.
func TestProbeCommitIsSynchronous(t *testing.T) {
	w := connect(t, pgxMain)
	r := connect(t, pgxMain)
	table := probeTable(t, "sync")
	simple := pgx.QueryExecModeSimpleProtocol
	_, err := w.Exec(context.Background(), "BEGIN READ WRITE WITH (SYSTEM_TIME = "+literal(nextSys())+")", simple)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Exec(context.Background(), ins(table, "d", 1, at(10), nil).sql, "d", int64(1), at(10), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Exec(context.Background(), "COMMIT SYNC", simple)
	observe(t, "COMMIT SYNC: %v", err)
	if err == nil {
		t.Errorf("COMMIT SYNC was accepted: the guide's statement exists")
	}
	_, _ = w.Exec(context.Background(), "ROLLBACK", simple)

	missed := 0
	const rounds = 200
	for i := range rounds {
		id := fmt.Sprintf("d%d", i)
		if err := txAt(w, nextSys(), ins(table, id, int64(i), at(10), nil)); err != nil {
			t.Fatal(err)
		}
		got, err := vtRead(r, table, id, at(20), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != fmt.Sprint(i) {
			missed++
		}
		// The same in a read-only transaction, which fixes its snapshot when it begins.
		if _, err := r.Exec(context.Background(), "BEGIN READ ONLY", simple); err != nil {
			t.Fatal(err)
		}
		got, err = vtRead(r, table, id, at(20), nil)
		_, _ = r.Exec(context.Background(), "COMMIT", simple)
		if err != nil {
			t.Fatal(err)
		}
		if got != fmt.Sprint(i) {
			missed++
		}
	}
	observe(t, "%d of %d reads, on another connection, straight after a commit, missed the transaction", missed, 2*rounds)
	if missed > 0 {
		t.Errorf("%d reads after a commit did not see it", missed)
	}
}

// TestProbeMicroseconds: the resolution of the temporal columns. A boundary written with
// nanoseconds is stored to the microsecond or to the nanosecond, and a read at an instant
// inside the microsecond answers as the floor of it does.
func TestProbeMicroseconds(t *testing.T) {
	c := connect(t, pgx.QueryExecModeSimpleProtocol)
	table := probeTable(t, "micros")
	// The boundaries as SQL literals with nine fractional digits: 10.000000500 and 11.000000900.
	lit := func(s string) string { return "TIMESTAMP '" + s + "'" }
	err := txAt(c, nextSys(), stmt{sql: "INSERT INTO " + table + " (_id, c, _valid_from, _valid_to) VALUES ('d', 1, " +
		lit("1970-01-01T00:00:10.000000500Z") + ", " + lit("1970-01-01T00:00:11.000000900Z") + ")"})
	if err != nil {
		t.Fatalf("a boundary with nanoseconds: %v", err)
	}
	rows, err := queryAll(c, "SELECT _valid_from, _valid_to FROM "+table+" FOR VALID_TIME ALL WHERE _id = 'd'")
	observe(t, "the boundaries read back as %s (%v)", show(rows), err)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read: %v %v", rows, err)
	}
	typ, err := queryAll(c, "SELECT _valid_from FROM "+table+" FOR VALID_TIME ALL WHERE _id = 'd'")
	observe(t, "the type of the column as the driver decodes it: %s (%v)", show(typ), err)
	for _, tc := range []struct {
		at   string
		want string
	}{
		{"1970-01-01T00:00:10.000000000Z", "absent"},
		{"1970-01-01T00:00:10.000000499Z", "absent"},
		{"1970-01-01T00:00:10.000000500Z", "1"},
		{"1970-01-01T00:00:10.000000999Z", "1"},
		{"1970-01-01T00:00:11.000000000Z", "1"},
		{"1970-01-01T00:00:11.000000899Z", "1"},
		{"1970-01-01T00:00:11.000000900Z", "absent"},
		{"1970-01-01T00:00:11.000001000Z", "absent"},
	} {
		got, err := queryAll(c, "SELECT c FROM "+table+" FOR VALID_TIME AS OF "+lit(tc.at)+" WHERE _id = 'd'")
		res := "absent"
		if err == nil && len(got) == 1 {
			res = got[0][0]
		}
		observe(t, "a read as of %s: %s (%v)", tc.at, res, err)
	}
}

// TestProbeTemporalArguments: the temporal clauses take their instants as arguments, which
// the simple protocol writes as quoted timestamps; a null end is the end of time.
func TestProbeTemporalArguments(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "args")
	if err := txAt(c, nextSys(), ins(table, "d", 1, at(10), nil)); err != nil {
		t.Fatal(err)
	}
	if got, err := vtRead(c, table, "d", at(20), nil); err != nil || got != "1" {
		t.Errorf("FOR VALID_TIME AS OF an argument: %s, %v", got, err)
	}
	s := at(probeSysFloor())
	if got, err := vtRead(c, table, "d", at(20), &s); err != nil || got != "1" {
		t.Errorf("FOR SYSTEM_TIME AS OF an argument: %s, %v", got, err)
	}
	if got, err := vtRead(c, table, "d", at(1<<45), nil); err != nil || got != "1" {
		t.Errorf("a null end of valid time at a distant instant: %s, %v", got, err)
	}
}

// probeSysFloor is a system time after every probe so far.
func probeSysFloor() int64 { return probeSys.next + 1000 }

// TestProbeBatchedReadForms: the ways of asking for the segments of several entities at once.
func TestProbeBatchedReadForms(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "batch")
	for i := range 3 {
		if err := txAt(c, nextSys(), stmt{
			sql:  "INSERT INTO " + table + " (_id, fp, c, _valid_from, _valid_to) VALUES ($1, $2, CAST($3 AS BIGINT), $4, $5)",
			args: []any{fmt.Sprintf("d%d", i), fmt.Sprintf("e%d", i), int64(i), at(10), nil},
		}); err != nil {
			t.Fatal(err)
		}
	}
	base := "SELECT fp FROM " + table + " FOR VALID_TIME AS OF $1 WHERE "
	for _, f := range []struct {
		name string
		sql  string
		args []any
	}{
		{"IN a list of arguments", base + "fp IN ($2, $3)", []any{at(20), "e0", "e2"}},
		{"= ANY an array argument", base + "fp = ANY($2)", []any{at(20), []string{"e0", "e2"}}},
		{"IN a subquery of an array argument", base + "fp IN (SELECT x FROM UNNEST($2) AS u (x))", []any{at(20), []string{"e0", "e2"}}},
		{"a join of an unnested array argument", "SELECT t.fp FROM UNNEST($2) AS u (x) JOIN " + table + " FOR VALID_TIME AS OF $1 AS t ON t.fp = u.x", []any{at(20), []string{"e0", "e2"}}},
		{"a left join lateral over an ordinality", "SELECT u.ord, n.fp FROM UNNEST($2) WITH ORDINALITY AS u (x, ord) LEFT JOIN LATERAL (SELECT fp FROM " + table + " FOR VALID_TIME AS OF $1 WHERE fp = u.x) AS n ON true ORDER BY u.ord", []any{at(20), []string{"e0", "e9", "e2"}}},
	} {
		rows, err := queryAll(c, f.sql, f.args...)
		observe(t, "%s: %s (%v)", f.name, show(rows), err)
		// The array forms need an array argument, which the simple protocol writes as text.
		if f.name == "IN a list of arguments" && (err != nil || len(rows) != 2) {
			t.Errorf("%s: %s (%v)", f.name, show(rows), err)
		}
	}
}

// TestProbeSQLPieces: the rest of the SQL the engine uses: min over no rows, ORDER BY on two
// columns, LIMIT, a bigint at both ends, a bytea of every byte, a null, and a table that has
// a column another document lacks.
func TestProbeSQLPieces(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "pieces")
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	ins := func(id string, seq int64, through any, payload []byte) stmt {
		return stmt{
			sql:  "INSERT INTO " + table + " (_id, seq, event_ns, through_ns, payload) VALUES ($1, CAST($2 AS BIGINT), CAST($3 AS BIGINT), CAST($4 AS BIGINT), CAST($5 AS VARBINARY))",
			args: []any{id, seq, int64(5), through, hex.EncodeToString(payload)},
		}
	}
	if err := txAt(c, nextSys(),
		ins("a", -9223372036854775808, nil, all),
		ins("b", 9223372036854775807, int64(7), []byte{}),
		ins("c", 0, nil, nil),
	); err != nil {
		t.Fatal(err)
	}
	for _, q := range []struct {
		name string
		sql  string
		args []any
	}{
		{"min over rows", "SELECT min(event_ns) FROM " + table + " FOR VALID_TIME ALL WHERE event_ns > CAST($1 AS BIGINT)", []any{int64(1)}},
		{"min over no rows", "SELECT min(event_ns) FROM " + table + " FOR VALID_TIME ALL WHERE event_ns > CAST($1 AS BIGINT)", []any{int64(100)}},
		{"order and limit", "SELECT seq FROM " + table + " FOR VALID_TIME ALL ORDER BY event_ns, seq LIMIT 2", nil},
		{"an exact comparison of bigints", "SELECT _id FROM " + table + " FOR VALID_TIME ALL WHERE seq <= CAST($1 AS BIGINT) ORDER BY seq", []any{int64(0)}},
		{"a null column", "SELECT _id, through_ns FROM " + table + " FOR VALID_TIME ALL ORDER BY _id", nil},
		{"the payload", "SELECT _id, payload FROM " + table + " FOR VALID_TIME ALL ORDER BY _id", nil},
	} {
		rows, err := queryAll(c, q.sql, q.args...)
		s := show(rows)
		if len(s) > 300 {
			s = s[:300] + "..."
		}
		observe(t, "%s: %s (%v)", q.name, s, err)
		if err != nil {
			t.Errorf("%s: %v", q.name, err)
		}
	}
	// The payload comes back as it went in.
	var got []byte
	if err := c.QueryRow(context.Background(), "SELECT payload FROM "+table+" FOR VALID_TIME ALL WHERE _id = $1", "a").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(all) {
		t.Errorf("a payload of every byte came back as %x", got)
	}
	// What EXPLAIN ANALYZE says, if it counts anything.
	rows, err := queryAll(c, "EXPLAIN ANALYZE SELECT _id FROM "+table+" FOR VALID_TIME ALL WHERE seq <= CAST($1 AS BIGINT)", int64(0))
	s := show(rows)
	if len(s) > 1500 {
		s = s[:1500] + "..."
	}
	observe(t, "EXPLAIN ANALYZE: %s (%v)", s, err)
	rows, err = queryAll(c, "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY column_name", table)
	observe(t, "the columns of the table: %s (%v)", show(rows), err)
}

// TestProbeIDLayout: what XTDB does with a UUID `_id`: it is accepted and compared as a
// UUID, an `_id` range finds the documents inside it, and (the reason XT-iid is not
// used) the range is no cheaper than a filter on a column, which EXPLAIN ANALYZE shows
// if it counts anything.
func TestProbeIDLayout(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "uuid")
	const groups, per = 8, 40
	uuid := func(group, i int) string {
		return fmt.Sprintf("%02x000000-0000-0000-0000-0000000000%02x", group, i)
	}
	var stmts []stmt
	for g := range groups {
		for i := range per {
			stmts = append(stmts, stmt{
				sql:  "INSERT INTO " + table + " (_id, grp, _valid_from, _valid_to) VALUES (CAST($1 AS UUID), CAST($2 AS BIGINT), $3, $4)",
				args: []any{uuid(g, i), int64(g), at(10), nil},
			})
		}
	}
	if err := txAt(c, nextSys(), stmts...); err != nil {
		t.Fatalf("documents with a UUID _id: %v", err)
	}
	lo, hi := uuid(3, 0), uuid(3, 255)
	for _, q := range []struct {
		name string
		sql  string
		args []any
		want int
	}{
		{"an _id range", "SELECT _id FROM " + table + " FOR VALID_TIME AS OF $1 WHERE _id >= CAST($2 AS UUID) AND _id <= CAST($3 AS UUID)", []any{at(20), lo, hi}, per},
		{"an _id between", "SELECT _id FROM " + table + " FOR VALID_TIME AS OF $1 WHERE _id BETWEEN CAST($2 AS UUID) AND CAST($3 AS UUID)", []any{at(20), lo, hi}, per},
		{"an _id equal", "SELECT _id FROM " + table + " FOR VALID_TIME AS OF $1 WHERE _id = CAST($2 AS UUID)", []any{at(20), uuid(3, 7)}, 1},
		{"a column equal", "SELECT _id FROM " + table + " FOR VALID_TIME AS OF $1 WHERE grp = CAST($2 AS BIGINT)", []any{at(20), int64(3)}, per},
	} {
		rows, err := queryAll(c, q.sql, q.args...)
		if err != nil {
			t.Errorf("%s: %s", q.name, pgDetail(err))
			continue
		}
		if len(rows) != q.want {
			t.Errorf("%s: %d rows, want %d", q.name, len(rows), q.want)
		}
		plan, err := queryAll(c, "EXPLAIN ANALYZE "+q.sql, q.args...)
		s := show(plan)
		if len(s) > 2500 {
			s = s[:2500] + "..."
		}
		observe(t, "EXPLAIN ANALYZE of %s: %s (%v)", q.name, s, err)
	}
}

// TestProbeRollback: a transaction that is rolled back leaves nothing, whatever its statements
// were, and a statement the node cannot accept fails at the statement or at the commit, after
// which the connection is usable again.
func TestProbeRollback(t *testing.T) {
	c := connect(t, pgxMain)
	table := probeTable(t, "rollback")
	ctx := context.Background()
	simple := pgx.QueryExecModeSimpleProtocol
	if _, err := c.Exec(ctx, "BEGIN READ WRITE WITH (SYSTEM_TIME = "+literal(nextSys())+")", simple); err != nil {
		t.Fatal(err)
	}
	s := ins(table, "d", 1, at(10), nil)
	if _, err := c.Exec(ctx, s.sql, s.args...); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "ROLLBACK", simple); err != nil {
		t.Fatal(err)
	}
	if got, err := vtRead(c, table, "d", at(20), nil); err != nil || got != "absent" {
		t.Errorf("after a rollback: %s, %v", got, err)
	}

	// A statement with no _id: where does it fail?
	if _, err := c.Exec(ctx, "BEGIN READ WRITE WITH (SYSTEM_TIME = "+literal(nextSys())+")", simple); err != nil {
		t.Fatal(err)
	}
	_, stmtErr := c.Exec(ctx, "INSERT INTO "+table+" (c) VALUES ($1)", int64(1))
	_, commitErr := c.Exec(ctx, "COMMIT", simple)
	_, rollbackErr := c.Exec(ctx, "ROLLBACK", simple)
	observe(t, "a statement with no _id: fails at the statement with %v; the commit then says %v; a rollback after it says %v", stmtErr, commitErr, rollbackErr)
	if got, err := vtRead(c, table, "d", at(20), nil); err != nil || got != "absent" {
		t.Errorf("a read after a failed transaction: %s, %v", got, err)
	}
	// The next transaction, at the next system time, commits.
	if err := txAt(c, nextSys(), ins(table, "e", 2, at(10), nil)); err != nil {
		t.Errorf("a transaction after a failed one: %v", err)
	}
	if got, err := vtRead(c, table, "e", at(20), nil); err != nil || got != "2" {
		t.Errorf("a read after it: %s, %v", got, err)
	}
}
