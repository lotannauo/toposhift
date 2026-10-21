package doltengine_test

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// The tests of this file are the facts about Dolt that the design relies on, each checked
// against the registered server. The package documentation lists them with their results;
// a fact that stops holding makes its test fail, and the design changes (a new version of
// the registration) before anything is measured.

func mustExec(t testing.TB, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// hashes returns the values of one column of a query, as text.
func column(t testing.TB, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any,
) []string {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s sql.NullString
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// commit makes a Dolt commit of everything in a scratch database and returns its hash.
func commit(t testing.TB, db *sql.DB, msg string) string {
	t.Helper()
	var hash string
	if err := db.QueryRowContext(t.Context(), "CALL DOLT_COMMIT('-A', '-m', '"+msg+"')").Scan(&hash); err != nil {
		t.Fatal(err)
	}
	return hash
}

// TestTheServerIsTheRegisteredOne: the version and every observable setting are the file's.
func TestTheServerIsTheRegisteredOne(t *testing.T) {
	db := rawDB(t, "")
	if err := doltengine.CheckServer(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if got := column(t, db, "SELECT dolt_version()"); len(got) != 1 || got[0] != doltengine.RegisteredVersion {
		t.Fatalf("version %v", got)
	}
}

// TestFactWindowFunctionsAreAvailableAndCorrect: [U] that ROW_NUMBER over a partition, with
// the ordering the log path needs, numbers the newest row of each key 1.
func TestFactWindowFunctionsAreAvailableAndCorrect(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE r (k VARBINARY(8) NOT NULL, ev BIGINT NOT NULL, seq BIGINT NOT NULL, v CHAR(1) NOT NULL, PRIMARY KEY (k, ev, seq))")
	mustExec(t, db, "INSERT INTO r VALUES (?, 1, 1, 'a'), (?, 5, 2, 'b'), (?, 5, 9, 'c'), (?, 7, 3, 'd'), (?, 2, 4, 'e'), (?, 2, 5, 'f')",
		[]byte("x"), []byte("x"), []byte("x"), []byte("x"), []byte("y"), []byte("y"))
	got := column(t, db, `SELECT CONCAT(k, ':', v) FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ev DESC, seq DESC) AS rn FROM r WHERE ev <= ?) x WHERE rn = 1 ORDER BY k`, 6)
	if want := []string{"x:c", "y:f"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v: ties at one instant go to the higher sequence number", got, want)
	}
}

// TestFactAsOfWorksBesideAnotherTableReference: [U] that AS OF is accepted on one table
// reference beside another in one statement, in a join and in a derived table, with an alias, and
// that it reads that commit's rows while the other table is read as it is now.
func TestFactAsOfWorksBesideAnotherTableReference(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE s (k INT NOT NULL PRIMARY KEY, ev BIGINT NOT NULL)")
	mustExec(t, db, "CREATE TABLE l (k INT NOT NULL, ev BIGINT NOT NULL, PRIMARY KEY (k, ev))")
	mustExec(t, db, "INSERT INTO s VALUES (1, 10), (2, 20)")
	mustExec(t, db, "INSERT INTO l VALUES (1, 1), (1, 10), (2, 2)")
	h1 := commit(t, db, "one")
	mustExec(t, db, "UPDATE s SET ev = 99 WHERE k = 1")
	mustExec(t, db, "INSERT INTO l VALUES (1, 50)")
	commit(t, db, "two")

	q := fmt.Sprintf(`SELECT CONCAT(k.k, ':', COUNT(l.ev)) FROM (SELECT s.k FROM s AS OF '%s' s WHERE s.ev > ?) k LEFT JOIN l ON l.k = k.k AND l.ev <= ? GROUP BY k.k ORDER BY k.k`, h1)
	// As of the first commit key 2 has event 20 > 15; the log is read as it is now.
	if got := column(t, db, q, 15, 100); !slices.Equal(got, []string{"2:1"}) {
		t.Fatalf("a derived table as of a commit, left-joined to another: %v", got)
	}
	q = fmt.Sprintf(`SELECT CONCAT(a.k, ':', a.ev, ':', COUNT(l.ev)) FROM s AS OF '%s' a JOIN l ON l.k = a.k GROUP BY a.k, a.ev ORDER BY a.k`, h1)
	if got := column(t, db, q); !slices.Equal(got, []string{"1:10:3", "2:20:1"}) {
		t.Fatalf("a join with a table as of a commit: %v", got)
	}
	q = fmt.Sprintf(`SELECT CONCAT(u.k, ':', u.ev) FROM (SELECT k, ev FROM s AS OF '%s' UNION ALL SELECT k, ev FROM s) u ORDER BY u.k, u.ev`, h1)
	if got := column(t, db, q); !slices.Equal(got, []string{"1:10", "1:99", "2:20", "2:20"}) {
		t.Fatalf("a union of a table as of a commit and as it is: %v", got)
	}
}

// TestFactAsOfACommitIsTheTableAsThatCommitLeftIt: [U] that AS OF '<hash>' returns the rows
// deleted after it, not the rows added after it, and not the working set's changes.
func TestFactAsOfACommitIsTheTableAsThatCommitLeftIt(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE t (k INT NOT NULL PRIMARY KEY, v INT NOT NULL)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)")
	h1 := commit(t, db, "one")
	mustExec(t, db, "DELETE FROM t WHERE k = 2")
	mustExec(t, db, "REPLACE INTO t VALUES (3, 30), (4, 4)")
	h2 := commit(t, db, "two")
	mustExec(t, db, "INSERT INTO t VALUES (5, 5)") // not committed
	read := func(hash string) []string {
		return column(t, db, fmt.Sprintf("SELECT CONCAT(k, ':', v) FROM t AS OF '%s' ORDER BY k", hash))
	}
	if got := read(h1); !slices.Equal(got, []string{"1:1", "2:2", "3:3"}) {
		t.Errorf("as of the first commit: %v", got)
	}
	if got := read(h2); !slices.Equal(got, []string{"1:1", "3:30", "4:4"}) {
		t.Errorf("as of the second commit: %v", got)
	}
	if got := column(t, db, "SELECT CONCAT(k, ':', v) FROM t ORDER BY k"); !slices.Equal(got, []string{"1:1", "3:30", "4:4", "5:5"}) {
		t.Errorf("the working set: %v", got)
	}
}

// TestFactReplaceIntoReplacesTheRowOfTheKey: [U] that REPLACE INTO on the state key replaces
// the whole row, a NULL deadline included, and leaves other keys alone.
func TestFactReplaceIntoReplacesTheRowOfTheKey(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE s (a VARBINARY(8) NOT NULL, p VARBINARY(8) NOT NULL, ev BIGINT NOT NULL, dl BIGINT NULL, PRIMARY KEY (a, p))")
	mustExec(t, db, "INSERT INTO s VALUES (?, ?, 1, 10), (?, ?, 1, NULL), (?, ?, 1, 10)", []byte("x"), []byte("p"), []byte("x"), []byte("q"), []byte("y"), []byte("p"))
	mustExec(t, db, "REPLACE INTO s (a, p, ev, dl) VALUES (?, ?, 2, NULL), (?, ?, 2, 20), (?, ?, 2, 20)", []byte("x"), []byte("p"), []byte("x"), []byte("q"), []byte("z"), []byte("p"))
	got := column(t, db, "SELECT CONCAT(a, p, ':', ev, ':', COALESCE(dl, 'null')) FROM s ORDER BY a, p")
	if want := []string{"xp:2:null", "xq:2:20", "yp:1:10", "zp:2:20"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if n := column(t, db, "SELECT COUNT(*) FROM s"); n[0] != "4" {
		t.Fatalf("%v rows", n)
	}
}

// TestFactJSONTableWithOrdinalityAndAPreparedArgument: [U] that JSON_TABLE with FOR
// ORDINALITY takes its document as a prepared statement's argument, and joins to a binary
// column through UNHEX.
func TestFactJSONTableWithOrdinalityAndAPreparedArgument(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE s (a VARBINARY(8) NOT NULL PRIMARY KEY, v INT NOT NULL)")
	mustExec(t, db, "INSERT INTO s VALUES (?, 1), (?, 2), (?, 3)", []byte{0x00, 0x01}, []byte{0x01, 0x00}, []byte{0x61, 0x00})
	got := column(t, db, `SELECT CONCAT(k.ord, ':', COALESCE(s.v, 'none')) FROM JSON_TABLE(?, '$[*]' COLUMNS (ord FOR ORDINALITY, a_hex VARCHAR(16) PATH '$')) k LEFT JOIN s ON s.a = UNHEX(k.a_hex) ORDER BY k.ord`,
		`["0100","0001","0100","ff","6100"]`)
	if want := []string{"1:2", "2:1", "3:2", "4:none", "5:3"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v: ordinals in order, a repeat answered twice, a missing key kept, a trailing zero byte not trimmed", got, want)
	}
}

// TestFactLongPrimaryKeysAreAcceptedAndOrderedByBytes: [U] that the registered tables, whose
// largest key is about a kilobyte, are created, and a row of the longest values is stored and
// read back, with binary keys compared as bytes (a trailing zero byte is not padding).
func TestFactLongPrimaryKeysAreAcceptedAndOrderedByBytes(t *testing.T) {
	db, _ := scratch(t)
	for _, ddl := range doltmodel.DDL() {
		mustExec(t, db, ddl)
	}
	var longest int
	for _, tbl := range doltmodel.Schema() {
		n := 0
		for _, k := range tbl.Key {
			for _, c := range tbl.Columns {
				if c.Name == k {
					var size int
					if _, err := fmt.Sscanf(c.Type, "VARBINARY(%d)", &size); err == nil {
						n += size
					} else {
						n += 8
					}
				}
			}
		}
		longest = max(longest, n)
	}
	if longest < 1000 {
		t.Fatalf("the longest key is %d bytes: this test is about a kilobyte", longest)
	}
	long := strings.Repeat("a", 255)
	mustExec(t, db, "INSERT INTO log (layer, dir, a, peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload) VALUES (2, 1, ?, ?, ?, ?, 5, 6, 'o', 0, NULL, ?)",
		[]byte(strings.Repeat("a", 272)), []byte(strings.Repeat("b", 272)), []byte(long), []byte(long), []byte{})
	mustExec(t, db, "INSERT INTO log (layer, dir, a, peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload) VALUES (2, 1, ?, ?, ?, ?, 5, 6, 'o', 0, NULL, ?)",
		[]byte("a\x00"), []byte("b"), []byte("r"), []byte("p"), []byte{})
	mustExec(t, db, "INSERT INTO log (layer, dir, a, peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload) VALUES (2, 1, ?, ?, ?, ?, 5, 6, 'o', 0, NULL, ?)",
		[]byte("a"), []byte("b"), []byte("r"), []byte("p"), []byte{})
	got := column(t, db, "SELECT HEX(a) FROM log ORDER BY layer, dir, a, peer, rel, producer, event_ns, seq")
	if want := []string{"61", "6100", strings.ToUpper(fmt.Sprintf("%x", strings.Repeat("a", 272)))}; !slices.Equal(got, want) {
		t.Fatalf("got %v: 'a', then 'a' and a zero byte (a prefix sorts first, and a zero byte is not padding), then the long key", got)
	}
}

// TestFactACommitIsMadeAtTheGivenAuthorAndDate: [U] that DOLT_COMMIT takes the author and the
// date as given, so that the history is a function of the stream.
func TestFactACommitIsMadeAtTheGivenAuthorAndDate(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
	text := "CALL DOLT_COMMIT('-A', '-m', 'seq 1', '--author', '" + doltengine.Author() + "', '--date', '2000-01-01T00:00:01Z')"
	var hash string
	if err := db.QueryRowContext(t.Context(), text).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(t.Context(), "SELECT commit_hash, committer, email, date, message FROM dolt_log WHERE commit_hash = ?", hash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("the commit is not in the log")
	}
	var h, who, email, date, msg string
	if err := rows.Scan(&h, &who, &email, &date, &msg); err != nil {
		t.Fatal(err)
	}
	if who != doltengine.AuthorName || email != doltengine.AuthorEmail || msg != "seq 1" || !strings.HasPrefix(date, "2000-01-01 00:00:01") {
		t.Fatalf("the commit: %q %q %q %q", who, email, date, msg)
	}
}

// TestFactTheDateIsNotTheServersZone: the same date given with a zone is the same commit
// date whatever the session's zone is.
func TestFactTheDateIsNotTheServersZone(t *testing.T) {
	db, _ := scratch(t)
	mustExec(t, db, "CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
	mustExec(t, db, "SET time_zone = '+05:00'")
	var hash string
	if err := db.QueryRowContext(t.Context(), "CALL DOLT_COMMIT('-A', '-m', 'seq 1', '--author', '"+doltengine.Author()+"', '--date', '2000-01-01T00:00:01Z')").Scan(&hash); err != nil {
		t.Fatal(err)
	}
	var date string
	if err := db.QueryRowContext(t.Context(), "SELECT date FROM dolt_log WHERE commit_hash = ?", hash).Scan(&date); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(date, "2000-01-01 00:00:01") {
		t.Fatalf("with the session in +05:00 the commit is dated %s", date)
	}
}

// TestFactTheWindowReadUsesTheEventTimeIndex: [U] whether the planner picks the index on event
// time for a window without a hint.
func TestFactTheWindowReadUsesTheEventTimeIndex(t *testing.T) {
	db, _ := scratch(t)
	for _, ddl := range doltmodel.DDL() {
		mustExec(t, db, ddl)
	}
	rows, err := db.QueryContext(t.Context(), "EXPLAIN FORMAT=TREE SELECT peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload FROM log WHERE layer = 2 AND dir = 1 AND a = 0x61 AND event_ns >= 5 AND event_ns < 10 AND seq <= 7 ORDER BY event_ns, seq")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, s)
	}
	text := strings.Join(plan, "\n")
	// The plan names the index by its columns.
	if !strings.Contains(text, "IndexedTableAccess(log)") || !strings.Contains(text, "index: [log.layer,log.dir,log.a,log.event_ns,log.seq]") {
		t.Fatalf("the window read does not use the index on event time:\n%s", text)
	}
}

// TestFactReadersDoNotSeeAnOpenTransactionAndKeepTheirSnapshot: [U] that a reader sees none of
// a write transaction's rows until it commits, and that a read-only transaction keeps the
// snapshot it began with while another connection commits.
func TestFactReadersDoNotSeeAnOpenTransactionAndKeepTheirSnapshot(t *testing.T) {
	db, name := scratch(t)
	mustExec(t, db, "CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")
	commit(t, db, "one")
	reader := rawDB(t, name)

	count := func(q interface {
		QueryRow(string, ...any) *sql.Row
	},
	) string {
		var n string
		if err := q.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	w, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.ExecContext(t.Context(), "INSERT INTO t VALUES (2), (3)"); err != nil {
		t.Fatal(err)
	}
	if n := count(reader); n != "1" {
		t.Fatalf("a reader saw %s rows of a write transaction that has not committed", n)
	}
	rtx, err := reader.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rtx.Rollback() }()
	if n := count(rtx); n != "1" {
		t.Fatalf("%s rows", n)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(rtx); n != "1" {
		t.Fatalf("a read-only transaction saw %s rows after another connection committed: it must keep its snapshot", n)
	}
	if err := rtx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := count(reader); n != "3" {
		t.Fatalf("a new read saw %s rows after the commit", n)
	}
}

// TestFactACommitInsideTheTransactionEndsWithIt: [U] whether a Dolt commit made inside the
// open transaction makes the SQL commit and the Dolt commit one step. The engine does not rely
// on it (a reader holds the engine's view still while the step ends), but a step in which
// the commit exists and the rows do not, or the rows exist and the commit does not, would be
// seen by a reader of another process; this records which it is.
func TestFactACommitInsideTheTransactionEndsWithIt(t *testing.T) {
	db, name := scratch(t)
	mustExec(t, db, "CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
	commit(t, db, "schema")
	other := rawDB(t, name)
	w, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.ExecContext(t.Context(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var hash string
	if err := w.QueryRowContext(t.Context(), "CALL DOLT_COMMIT('-A', '-m', 'seq 1')").Scan(&hash); err != nil {
		t.Fatal(err)
	}
	logBefore := column(t, other, "SELECT message FROM dolt_log WHERE message = 'seq 1'")
	rowsBefore := column(t, other, "SELECT COUNT(*) FROM t")
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	logAfter := column(t, other, "SELECT message FROM dolt_log WHERE message = 'seq 1'")
	rowsAfter := column(t, other, "SELECT COUNT(*) FROM t")
	t.Logf("before the SQL commit another connection sees %d commit(s) and %s row(s); after it, %d and %s", len(logBefore), rowsBefore[0], len(logAfter), rowsAfter[0])
	if len(logAfter) != 1 || rowsAfter[0] != "1" {
		t.Fatalf("after the SQL commit: %d commits, %s rows", len(logAfter), rowsAfter[0])
	}
	if status := column(t, other, "SELECT COUNT(*) FROM dolt_status"); status[0] != "0" {
		t.Fatalf("the working set is not clean after the commit and the SQL commit: %v", status)
	}
}

// TestEveryStatementOfTheEngineRunsAsAPreparedStatement: [U] that the server accepts, as
// prepared statements, every statement the engine sends with arguments (the whole engine
// is exercised in this mode: InterpolateParams is false).
func TestEveryStatementOfTheEngineRunsAsAPreparedStatement(t *testing.T) {
	if doltengine.InterpolateParams {
		t.Fatal("the registered mode sends prepared statements")
	}
	e := openEngine(t, doltmodel.ArmCommit)
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 30), existence(pod1, 2, "a", 100, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(at(50)); err != nil {
		t.Fatal(err)
	}
	for _, token := range []uint64{1, 2, engine.Latest} {
		sc := engine.Scope{Layer: catalog.L2, AsOf: token}
		if _, err := e.Neighbors(node1, engine.Reverse, at(110), sc); err != nil {
			t.Fatal(err)
		}
		if _, err := e.NeighborsBatch([]identity.Fingerprint{pod1, node1}, engine.Reverse, at(110), sc); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Alive(pod1, at(110), sc); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Window(node1, engine.Reverse, at(0), at(1000), sc); err != nil {
			t.Fatal(err)
		}
	}
}

// the hash an engine's commit has is a commit hash.
var hashPattern = regexp.MustCompile(`^[0-9a-v]{32}$`)

// TestTwoLoadsOfOneStreamEndAtTheSameHeadCommit: [U] that, with the synthetic author and dates,
// two loads of one stream end at the same head commit hash, and that each commit's author, date
// and message are the engine's. The determinism of the head is the strongest check of the
// history that this baseline has.
func TestTwoLoadsOfOneStreamEndAtTheSameHeadCommit(t *testing.T) {
	load := func() (doltengine.Commit, []string) {
		e := openEngine(t, doltmodel.ArmCommit)
		p := newPair(t, e, doltmodel.ArmCommit)
		if err := p.write(observe(edge1, 1, "a", 100, 30), existence(pod1, 2, "a", 100, 0)); err != nil {
			t.Fatal(err)
		}
		if err := p.write(del(edge1, 3, "a", 130), observe(edge2, 4, "b", 90, 0)); err != nil {
			t.Fatal(err)
		}
		if err := p.retain(at(95)); err != nil {
			t.Fatal(err)
		}
		if err := p.write(observe(edge3, 5, "a", 200, 0)); err != nil {
			t.Fatal(err)
		}
		head, _ := e.Head()
		var log []string
		for _, c := range e.Commits() {
			log = append(log, fmt.Sprintf("%d %s %s", c.Ordinal, c.Message, c.Date.Format(time.RFC3339)))
		}
		db := rawDB(t, e.Database())
		log = append(log, column(t, db, "SELECT CONCAT(commit_hash, ' ', committer, ' ', email, ' ', date, ' ', message) FROM dolt_log ORDER BY date")...)
		return head, log
	}
	h1, l1 := load()
	h2, l2 := load()
	if !hashPattern.MatchString(h1.Hash) || h1.Hash != h2.Hash {
		t.Fatalf("two loads of one stream ended at %s and %s", h1.Hash, h2.Hash)
	}
	if !slices.Equal(l1, l2) {
		t.Fatalf("the histories differ:\n%v\n%v", l1, l2)
	}
	if h1.Ordinal != 4 || h1.Message != "seq 5" {
		t.Fatalf("head %+v", h1)
	}
}
