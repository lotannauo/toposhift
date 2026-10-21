package xtengine

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// readRig is an engine with two batches, ending at tokens 2 and 3: the edge from pod1
// to node1 and pod1 itself at 10s, then the edge from pod2 to node1 at 20s.
type readRig struct {
	node *fakeNode
	e    *Engine
}

func newReadRig(t testing.TB, mut mutation) *readRig {
	t.Helper()
	node := newFakeNode()
	e := openFake(t, node, mut)
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0), podRec(2, 10)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{edgeRec(3, edgeB, "b", 20, 0)}); err != nil {
		t.Fatal(err)
	}
	node.statements = make(map[string][]loggedStmt)
	return &readRig{node: node, e: e}
}

// sent returns the statement the last read sent, which must have been framed by a
// read-only transaction, and forgets what was sent.
func (r *readRig) sent(t testing.TB) []loggedStmt {
	t.Helper()
	got := r.node.statements["r"]
	r.node.statements = make(map[string][]loggedStmt)
	if len(got) < 3 || got[0].sql != "BEGIN READ ONLY" || got[len(got)-1].sql != "COMMIT" {
		t.Fatalf("a read is not framed by a read-only transaction:\n%s", describe(got))
	}
	return got[1 : len(got)-1]
}

func (r *readRig) one(t testing.TB) loggedStmt {
	t.Helper()
	got := r.sent(t)
	if len(got) != 1 {
		t.Fatalf("a read sent %d statements:\n%s", len(got), describe(got))
	}
	return got[0]
}

func l2() int64 { return int64(catalog.L2) }

func sysOf(n int) time.Time { return defaultBase.Add(time.Duration(n) * time.Microsecond) }

// checkTokenReads: how each read is sent, for a token of each kind.
func checkTokenReads(t testing.TB, mut mutation) {
	const (
		seg   = "SELECT peer, rel FROM seg_f FOR VALID_TIME AS OF $1"
		segR  = "SELECT peer, rel FROM seg_r FOR VALID_TIME AS OF $1"
		sysCl = " FOR SYSTEM_TIME AS OF $2"
	)
	r := newReadRig(t, mut)
	e := r.e
	fp := pod1.String()
	neighbors := func(dir engine.Direction, at time.Time, asOf uint64) []engine.Neighbor {
		t.Helper()
		got, err := e.Neighbors(pod1, dir, at, engine.Scope{Layer: catalog.L2, AsOf: asOf})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// The newest.
	if got := neighbors(engine.Forward, sec(15), engine.Latest); len(got) != 1 || got[0].Peer != node1 {
		t.Errorf("neighbors at the newest: %v", got)
	}
	q := r.one(t)
	if want := seg + " WHERE fp = $2 AND layer = CAST($3 AS BIGINT)"; q.sql != want || !reflect.DeepEqual(q.args, []any{sec(15), fp, l2()}) {
		t.Errorf("the newest: %q %v\nwant %q %v", q.sql, q.args, want, []any{sec(15), fp, l2()})
	}

	// Reverse is the other table, read from the target.
	rev, err := e.Neighbors(node1, engine.Reverse, sec(25), engine.Current(catalog.L2))
	if err != nil || len(rev) != 2 {
		t.Errorf("reverse neighbors of the node: %v, %v", rev, err)
	}
	q = r.one(t)
	if want := segR + " WHERE fp = $2 AND layer = CAST($3 AS BIGINT)"; q.sql != want || q.args[1] != node1.String() {
		t.Errorf("reverse: %q %v", q.sql, q.args)
	}

	// A token that ended a batch reads as of the system time of the batch; so does a token
	// that is the last, or beyond it.
	for _, c := range []struct {
		asOf uint64
		n    int
		want int // neighbors of node1 in reverse at 25s
	}{{2, 1, 1}, {3, 2, 2}, {1000, 2, 2}} {
		got, err := e.Neighbors(node1, engine.Reverse, sec(25), engine.Scope{Layer: catalog.L2, AsOf: c.asOf})
		if err != nil || len(got) != c.want {
			t.Errorf("token %d: %d neighbors (want %d), %v", c.asOf, len(got), c.want, err)
		}
		q = r.one(t)
		if want := segR + sysCl + " WHERE fp = $3 AND layer = CAST($4 AS BIGINT)"; q.sql != want || q.args[1] != sysOf(c.n) {
			t.Errorf("token %d: %q %v\nwant %q as of %v", c.asOf, q.sql, q.args, want, sysOf(c.n))
		}
	}

	// A token inside a batch, or before the first, names no system time: the log is read, up
	// to the token, and folded.
	for _, asOf := range []uint64{0, 1} {
		neighbors(engine.Forward, sec(15), asOf)
		q = r.one(t)
		want := "SELECT peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns FROM log_f FOR VALID_TIME ALL WHERE fp = $1 AND layer = CAST($2 AS BIGINT) AND seq <= CAST($3 AS BIGINT)"
		if q.sql != want || !reflect.DeepEqual(q.args, []any{fp, l2(), seqToDB(asOf)}) {
			t.Errorf("token %d: %q %v\nwant %q", asOf, q.sql, q.args, want)
		}
	}
	if rc := e.ReadCounts(); rc.ClientFold != 2 || rc.Native != 5 {
		t.Errorf("read counts %+v", rc)
	}
	// The answer of a fold: token 1 sees the edge, token 0 nothing.
	if got := neighbors(engine.Forward, sec(15), 1); len(got) != 1 {
		t.Errorf("neighbors at token 1: %v", got)
	}
	if got := neighbors(engine.Forward, sec(15), 0); len(got) != 0 {
		t.Errorf("neighbors at token 0: %v", got)
	}
	r.sent(t)
}

// checkFloor: a read at an instant between two whole
// microseconds asks about the one before it, which with every boundary on a whole microsecond
// is the same question.
func checkFloor(t testing.TB, mut mutation) {
	r := newReadRig(t, mut)
	for _, c := range []struct {
		at   time.Time
		want time.Time
	}{
		{sec(15).Add(999 * time.Nanosecond), sec(15)},
		{sec(15).Add(500 * time.Nanosecond), sec(15)},
		{sec(15).Add(time.Nanosecond), sec(15)},
		{sec(15).Add(-time.Nanosecond), sec(15).Add(-time.Microsecond)},
		{sec(15), sec(15)},
		{engine.MaxEventTime, time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()},
		{engine.MaxEventTime.Add(time.Nanosecond), time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()},
		{engine.MaxEventTime.Add(time.Hour), time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()},
	} {
		if _, err := r.e.Neighbors(pod1, engine.Forward, c.at, engine.Current(catalog.L2)); err != nil {
			t.Fatal(err)
		}
		if got := r.one(t).args[0]; got != c.want {
			t.Errorf("a read at %s asks as of %v, want %v", c.at.Format(time.RFC3339Nano), got, c.want)
		}
	}
	// Before the first instant nothing exists, and nothing is asked.
	if got, err := r.e.Neighbors(pod1, engine.Forward, sec(-1), engine.Current(catalog.L2)); err != nil || got != nil {
		t.Errorf("neighbors before 1970: %v, %v", got, err)
	}
	if got, err := r.e.Alive(pod1, sec(-1), engine.Current(catalog.L2)); err != nil || got {
		t.Errorf("alive before 1970: %v, %v", got, err)
	}
	if got, err := r.e.NeighborsBatch([]identity.Fingerprint{pod1, pod2}, engine.Forward, sec(-1), engine.Current(catalog.L2)); err != nil || len(got) != 2 || got[0] != nil || got[1] != nil {
		t.Errorf("a batch before 1970: %v, %v", got, err)
	}
	if n := len(r.node.statements["r"]); n != 0 {
		t.Errorf("a read before 1970 sent %d statements", n)
	}
	// A read that names no layer is refused before anything is sent.
	if _, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Scope{AsOf: engine.Latest}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a read with no layer: %v", orNil(err))
	}
	if n := len(r.node.statements["r"]); n != 0 {
		t.Errorf("a refused read sent %d statements", n)
	}
}

// checkOtherReads: the other three reads, at the newest, at the
// system time of an old batch and inside a batch.
func checkOtherReads(t testing.TB, mut mutation) {
	r := newReadRig(t, mut)
	e := r.e
	fp := pod1.String()

	alive := func(asOf uint64) bool {
		t.Helper()
		ok, err := e.Alive(pod1, sec(15), engine.Scope{Layer: catalog.L2, AsOf: asOf})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !alive(engine.Latest) {
		t.Error("the pod is not alive")
	}
	q := r.one(t)
	if want := "SELECT fp FROM ent FOR VALID_TIME AS OF $1 WHERE fp = $2 AND layer = CAST($3 AS BIGINT) LIMIT 1"; q.sql != want || !reflect.DeepEqual(q.args, []any{sec(15), fp, l2()}) {
		t.Errorf("alive: %q %v", q.sql, q.args)
	}
	if !alive(2) {
		t.Error("not alive at token 2")
	}
	q = r.one(t)
	if want := "SELECT fp FROM ent FOR VALID_TIME AS OF $1 FOR SYSTEM_TIME AS OF $2 WHERE fp = $3 AND layer = CAST($4 AS BIGINT) LIMIT 1"; q.sql != want || q.args[1] != sysOf(1) {
		t.Errorf("alive at token 2: %q %v", q.sql, q.args)
	}
	// Inside the first batch, at token 1, the entity's record (seq 2) is not yet written.
	if alive(1) {
		t.Error("alive at token 1, before the entity's record")
	}
	q = r.one(t)
	if want := "SELECT producer, seq, event_ns, kind, ttl_ns, through_ns FROM log_e FOR VALID_TIME ALL WHERE fp = $1 AND layer = CAST($2 AS BIGINT) AND seq <= CAST($3 AS BIGINT)"; q.sql != want {
		t.Errorf("alive at token 1: %q", q.sql)
	}

	window := func(from, to time.Time, asOf uint64) []engine.Record {
		t.Helper()
		got, err := e.Window(node1, engine.Reverse, from, to, engine.Scope{Layer: catalog.L2, AsOf: asOf})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := window(sec(0), sec(100), engine.Latest); len(got) != 2 {
		t.Errorf("a window of the node's reverse edges: %d records", len(got))
	}
	q = r.one(t)
	want := "SELECT layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns, payload FROM log_r FOR VALID_TIME ALL WHERE fp = $1 AND layer = CAST($2 AS BIGINT) AND event_ns >= CAST($3 AS BIGINT) AND event_ns < CAST($4 AS BIGINT) ORDER BY event_ns, seq"
	if q.sql != want || !reflect.DeepEqual(q.args, []any{node1.String(), l2(), int64(0), 100 * int64(time.Second)}) {
		t.Errorf("window: %q %v", q.sql, q.args)
	}
	// From the epoch, unbounded above: no upper bound at all.
	window(time.Time{}, engine.MaxEventTime.Add(time.Hour), engine.Latest)
	q = r.one(t)
	if want := "SELECT layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns, payload FROM log_r FOR VALID_TIME ALL WHERE fp = $1 AND layer = CAST($2 AS BIGINT) AND event_ns >= CAST($3 AS BIGINT) ORDER BY event_ns, seq"; q.sql != want {
		t.Errorf("an unbounded window: %q", q.sql)
	}
	// An old batch's system time; a token inside a batch, the sequence number.
	if got := window(sec(0), sec(100), 2); len(got) != 1 {
		t.Errorf("a window at token 2: %d records", len(got))
	}
	q = r.one(t)
	if want := "FROM log_r FOR VALID_TIME ALL FOR SYSTEM_TIME AS OF $1 WHERE fp = $2"; !strings.Contains(q.sql, want) || q.args[0] != sysOf(1) {
		t.Errorf("a window at token 2: %q %v", q.sql, q.args)
	}
	if got := window(sec(0), sec(100), 1); len(got) != 1 || got[0].Seq != 1 {
		t.Errorf("a window at token 1: %d records", len(got))
	}
	q = r.one(t)
	if want := "AND seq <= CAST($5 AS BIGINT) ORDER BY event_ns, seq"; !strings.Contains(q.sql, want) || q.args[4] != seqToDB(1) {
		t.Errorf("a window at token 1: %q %v", q.sql, q.args)
	}
	// An empty window sends nothing.
	if got := window(sec(5), sec(5), engine.Latest); got != nil {
		t.Errorf("an empty window: %v", got)
	}
	if n := len(r.node.statements["r"]); n != 0 {
		t.Errorf("an empty window sent %d statements", n)
	}

	// A batched read: the distinct entities once, in one statement, and an answer for each
	// entity asked, empty ones included, in the order asked.
	fps := []identity.Fingerprint{node1, pod1, node1, pod2}
	got, err := e.NeighborsBatch(fps, engine.Forward, sec(25), engine.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || len(got[0]) != 0 || len(got[1]) != 1 || len(got[2]) != 0 || len(got[3]) != 1 {
		t.Errorf("a batch of %v: %v", fps, got)
	}
	q = r.one(t)
	wantSQL := "SELECT fp, peer, rel FROM seg_f FOR VALID_TIME AS OF $1 WHERE fp IN ($2, $3, $4) AND layer = CAST($5 AS BIGINT)"
	if q.sql != wantSQL || !reflect.DeepEqual(q.args, []any{sec(25), node1.String(), pod1.String(), pod2.String(), l2()}) {
		t.Errorf("a batch: %q %v", q.sql, q.args)
	}
	// More than a statement holds: several statements in one transaction.
	var many []identity.Fingerprint
	for i := range 1200 {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i), byte(i>>8)
		f, _ := identity.FingerprintFromHash("k8s.pod", h)
		many = append(many, f)
	}
	if got, err := e.NeighborsBatch(many, engine.Forward, sec(25), engine.Current(catalog.L2)); err != nil || len(got) != 1200 {
		t.Errorf("a batch of 1200: %d answers, %v", len(got), err)
	}
	if n := len(r.sent(t)); n != 3 {
		t.Errorf("a batch of 1200 was %d statements, want 3", n)
	}
	// A batch inside a batch of the stream is folded entity by entity.
	if got, err := e.NeighborsBatch([]identity.Fingerprint{pod1, pod2}, engine.Forward, sec(25), engine.Scope{Layer: catalog.L2, AsOf: 1}); err != nil || len(got) != 2 || len(got[0]) != 1 || len(got[1]) != 0 {
		t.Errorf("a batch at token 1: %v, %v", got, err)
	}
	if n := len(r.sent(t)); n != 2 {
		t.Errorf("a folded batch of two was %d statements, want 2", n)
	}
}

// TestAFailedReadIsRolledBack: a read the node refuses ends its transaction, and the
// engine reads again.
func TestAFailedReadIsRolledBack(t *testing.T) {
	r := newReadRig(t, 0)
	failWhen(r.node, func(conn, sql string) bool { return conn == "r" && len(sql) > 6 && sql[:6] == "SELECT" }, errors.New("the node refused the query"))
	if _, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Current(catalog.L2)); err == nil {
		t.Fatal("a refused read returned no error")
	}
	got := r.node.statements["r"]
	if len(got) != 3 || got[2].sql != "ROLLBACK" {
		t.Errorf("a refused read:\n%s", describe(got))
	}
	failWhen(r.node, func(string, string) bool { return false }, nil)
	if got, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Current(catalog.L2)); err != nil || len(got) != 1 {
		t.Errorf("a read after a refused one: %v, %v", got, err)
	}
	// A read that cannot begin is an error too, with nothing to roll back.
	failWhen(r.node, func(conn, sql string) bool { return conn == "r" && sql == "BEGIN READ ONLY" }, errors.New("no transaction"))
	if _, err := r.e.Alive(pod1, sec(15), engine.Current(catalog.L2)); err == nil {
		t.Error("a read that could not begin returned no error")
	}
}

func TestReadsAskForWhatTheTokenNames(t *testing.T)             { checkTokenReads(t, 0) }
func TestTheInstantOfAReadIsFlooredToAMicrosecond(t *testing.T) { checkFloor(t, 0) }
func TestAliveWindowAndBatchAskForWhatTheyNeed(t *testing.T)    { checkOtherReads(t, 0) }

// checkReadRetry: a read that the node refuses because its plan is out of date is made
// again, and answers; one that fails otherwise is not, and is reported; and a read that
// stays out of date for every attempt is reported too.
func checkReadRetry(t testing.TB, mut mutation) {
	t.Helper()
	r := newReadRig(t, mut)
	stale := &pgconn.PgError{Code: "0A000", Message: "cached plan must not change result type"}
	failures := 2
	failWhen(r.node, func(conn, sql string) bool {
		if conn == "r" && strings.HasPrefix(sql, "SELECT") && failures > 0 {
			failures--
			return true
		}
		return false
	}, stale)
	got, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Current(catalog.L2))
	if err != nil || len(got) != 1 {
		t.Errorf("a read that found its plan out of date: %v, %v", got, err)
	}
	if stmts := r.node.statements["r"]; countPrefix(stmts, "BEGIN READ ONLY") != 3 || countPrefix(stmts, "ROLLBACK") != 2 {
		t.Errorf("the read was made again in %d transactions with %d rollbacks:\n%s", countPrefix(stmts, "BEGIN READ ONLY"), countPrefix(stmts, "ROLLBACK"), describe(stmts))
	}
	// Another refusal is not retried.
	other := &pgconn.PgError{Code: "XX000", Message: "something else"}
	failWhen(r.node, func(conn, sql string) bool { return conn == "r" && strings.HasPrefix(sql, "SELECT") }, other)
	r.node.statements = make(map[string][]loggedStmt)
	if _, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Current(catalog.L2)); !errors.Is(err, other) {
		t.Errorf("another failure: %v", orNil(err))
	}
	if n := countPrefix(r.node.statements["r"], "BEGIN READ ONLY"); n != 1 {
		t.Errorf("another failure was made %d times", n)
	}
	// A plan that never settles is reported after the attempts there are.
	failWhen(r.node, func(conn, sql string) bool { return conn == "r" && strings.HasPrefix(sql, "SELECT") }, stale)
	r.node.statements = make(map[string][]loggedStmt)
	if _, err := r.e.Neighbors(pod1, engine.Forward, sec(15), engine.Current(catalog.L2)); !errors.Is(err, stale) {
		t.Errorf("a plan that is always out of date: %v", orNil(err))
	}
	if n := countPrefix(r.node.statements["r"], "BEGIN READ ONLY"); n != maxReadAttempts && mut == 0 {
		t.Errorf("a plan that is always out of date was read %d times, want %d", n, maxReadAttempts)
	}
}

func TestAReadWhosePlanIsOutOfDateIsMadeAgain(t *testing.T) { checkReadRetry(t, 0) }
