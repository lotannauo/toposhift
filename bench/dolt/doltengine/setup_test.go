package doltengine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// What an engine does when it opens a database, against the fake server: it creates the
// tables of a new one, refuses one that is not its own, and rebuilds from the tables and the
// commit log what it remembers.

// database is a fake database's answers to what setup asks.
type database struct {
	tables  []string
	meta    []driver.Value // nil: no meta row
	layers  [][]driver.Value
	ents    [][]driver.Value
	state   [][]driver.Value
	entSt   [][]driver.Value
	log     [][]driver.Value
	created []string
}

func (d *database) handle(q string, a []driver.Value) fakeResult {
	switch {
	case strings.HasPrefix(q, "SELECT table_name"):
		var rows [][]driver.Value
		for _, n := range d.tables {
			rows = append(rows, []driver.Value{n})
		}
		return fakeResult{cols: []string{"table_name"}, rows: rows}
	case strings.HasPrefix(q, "CREATE TABLE"):
		d.created = append(d.created, q)
		return fakeResult{}
	case strings.HasPrefix(q, "INSERT INTO meta"):
		d.meta = []driver.Value{doltmodel.EncodeSeq(0), int64(0), int64(0), int64(doltmodel.SchemaVersion)}
		return fakeResult{affected: 1}
	case strings.HasPrefix(q, "SELECT last_seq"):
		if d.meta == nil {
			return fakeResult{cols: []string{"last_seq", "horizon_ns", "past_end", "schema_version"}}
		}
		return fakeResult{cols: []string{"last_seq", "horizon_ns", "past_end", "schema_version"}, rows: [][]driver.Value{d.meta}}
	case strings.HasPrefix(q, "SELECT DISTINCT layer, a"):
		return fakeResult{cols: []string{"layer", "a", "peer", "rel"}, rows: d.layers}
	case strings.HasPrefix(q, "SELECT DISTINCT fp"):
		return fakeResult{cols: []string{"fp", "layer"}, rows: d.ents}
	case strings.HasPrefix(q, "SELECT a, peer, rel, producer"):
		return fakeResult{cols: []string{"a", "peer", "rel", "producer", "event_ns", "seq", "deadline_ns"}, rows: d.state}
	case strings.HasPrefix(q, "SELECT fp, producer"):
		return fakeResult{cols: []string{"fp", "producer", "event_ns", "seq", "deadline_ns"}, rows: d.entSt}
	case strings.HasPrefix(q, "SELECT commit_hash"):
		return fakeResult{cols: []string{"commit_hash", "message", "date"}, rows: d.log}
	}
	return committing()(q, a)
}

func (d *database) open(t *testing.T, arm doltmodel.Arm) (*Engine, *fakeServer, error) {
	t.Helper()
	srv := &fakeServer{handle: d.handle}
	db := sql.OpenDB(fakeConnector{srv})
	db.SetMaxOpenConns(1)
	e := newEngine(arm, "fake", db, db)
	t.Cleanup(func() { _ = e.Close() })
	return e, srv, e.setup(context.Background())
}

var allTables = []string{"ent_log", "ent_state", "log", "meta", "state"}

func commitRow(hash, msg, date string) []driver.Value { return []driver.Value{hash, msg, []byte(date)} }

func TestANewDatabaseGetsTheTablesAndAMetaRow(t *testing.T) {
	d := &database{}
	e, srv, err := d.open(t, doltmodel.ArmCommit)
	if err != nil {
		t.Fatal(err)
	}
	if want := doltmodel.DDL(); fmt.Sprint(d.created) != fmt.Sprint(want) {
		t.Fatalf("created %v, want the registered DDL %v", d.created, want)
	}
	if e.LastSeq() != 0 || len(e.Commits()) != 0 {
		t.Fatalf("a new database: seq %d, commits %v", e.LastSeq(), e.Commits())
	}
	got := srv.kinds()
	if fmt.Sprint(got[:3]) != "[BEGIN SELECT ROLLBACK]" || got[3] != "BEGIN" || got[len(got)-1] != "ROLLBACK" {
		t.Fatalf("sent %v", got)
	}
	// The tables are not committed: the first batch's commit stages them. The one call is the
	// amendment of the commit the server made when it created the database.
	calls := 0
	for _, q := range srv.statements() {
		if strings.HasPrefix(q, "CALL") {
			calls++
			want := "CALL DOLT_COMMIT('--amend', '-m', 'toposhift init', '--author', 'toposhift-bench <bench@localhost.invalid>', '--date', '2000-01-01T00:00:00Z')"
			if q != want {
				t.Fatalf("the call is %q, want %q", q, want)
			}
		}
	}
	if calls != 1 {
		t.Fatalf("creating the tables made %d commits or amendments: %v", calls, got)
	}
}

func TestADatabaseThatIsNotTheEnginesIsRefused(t *testing.T) {
	for name, d := range map[string]*database{
		"another table":   {tables: append([]string{"orders"}, allTables...)},
		"a table less":    {tables: []string{"ent_log", "log", "meta", "state"}},
		"another schema":  {tables: []string{"orders"}},
		"no meta row":     {tables: allTables},
		"another version": {tables: allTables, meta: []driver.Value{doltmodel.EncodeSeq(0), int64(0), int64(0), int64(doltmodel.SchemaVersion + 1)}},
	} {
		t.Run(name, func(t *testing.T) {
			_, srv, err := d.open(t, doltmodel.ArmCommit)
			if !errors.Is(err, ErrSchema) {
				t.Fatalf("got %v", err)
			}
			for _, k := range srv.kinds() {
				if strings.HasPrefix(k, "CREATE") || strings.HasPrefix(k, "INSERT") || strings.HasPrefix(k, "CALL") {
					t.Fatalf("a database that is not the engine's was written to: %v", srv.kinds())
				}
			}
		})
	}
	// The system tables Dolt adds to every database are not tables of the database's own.
	d := &database{tables: append([]string{"dolt_log", "dolt_status"}, allTables...), meta: []driver.Value{doltmodel.EncodeSeq(0), int64(0), int64(0), int64(doltmodel.SchemaVersion)}}
	if _, _, err := d.open(t, doltmodel.ArmCommit); err != nil {
		t.Fatalf("a database with Dolt's system tables: %v", err)
	}
}

// reopened is a database an engine left: batches to seq 3 and 5, a retention at seq 5, a
// horizon at 20 seconds, one subject of each kind, and state rows.
func reopened() *database {
	a, b, rel := doltmodel.EncodeFingerprint(pod1), doltmodel.EncodeFingerprint(node1), []byte(catalog.ScheduledOn)
	ent := doltmodel.EncodeFingerprint(pod2)
	return &database{
		tables: allTables,
		meta:   []driver.Value{doltmodel.EncodeSeq(5), 20 * sec, int64(0), int64(doltmodel.SchemaVersion)},
		layers: [][]driver.Value{{int64(catalog.L2), a, b, rel}},
		ents:   [][]driver.Value{{ent, int64(catalog.L2)}},
		state: [][]driver.Value{
			{a, b, rel, []byte("p"), 10 * sec, doltmodel.EncodeSeq(4), nil},
			{a, b, rel, []byte("q"), 12 * sec, doltmodel.EncodeSeq(5), 40 * sec},
		},
		entSt: [][]driver.Value{{ent, []byte("p"), 11 * sec, doltmodel.EncodeSeq(3), nil}},
		log: [][]driver.Value{
			commitRow(hashN(1), "seq 3", "2000-01-01 00:00:01"),
			commitRow("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "Initialize data repository", "2026-10-21 09:00:00"),
			commitRow(hashN(2), "seq 5", "2000-01-01 00:00:02"),
			commitRow(hashN(3), "retain 20000000000 seq 5", "2000-01-01 00:00:03"),
		},
	}
}

func TestOpeningADatabaseRebuildsWhatTheEngineRemembers(t *testing.T) {
	e, _, err := reopened().open(t, doltmodel.ArmCommit)
	if err != nil {
		t.Fatal(err)
	}
	if e.LastSeq() != 5 || e.w.lastSeq != 5 || e.w.horizon != 20*sec || e.w.pastEnd {
		t.Errorf("seq %d, horizon %d, past end %v", e.LastSeq(), e.w.horizon, e.w.pastEnd)
	}
	if l, ok := e.w.layers[edge1]; !ok || l != catalog.L2 {
		t.Errorf("layers %v", e.w.layers)
	}
	if l, ok := e.w.layers[engine.EntitySubject(pod2)]; !ok || l != catalog.L2 {
		t.Errorf("layers %v", e.w.layers)
	}
	if got := e.w.newest[subjectProducer{edge1, "p"}]; got != (stateEntry{eventNs: 10 * sec, seq: doltmodel.EncodeSeq(4), open: true}) {
		t.Errorf("state of p: %+v", got)
	}
	if got := e.w.newest[subjectProducer{edge1, "q"}]; got != (stateEntry{eventNs: 12 * sec, seq: doltmodel.EncodeSeq(5), deadlineNs: 40 * sec}) {
		t.Errorf("state of q: %+v", got)
	}
	if got := e.w.newest[subjectProducer{engine.EntitySubject(pod2), "p"}]; got != (stateEntry{eventNs: 11 * sec, seq: doltmodel.EncodeSeq(3), open: true}) {
		t.Errorf("state of the entity: %+v", got)
	}
	cs := e.Commits()
	if len(cs) != 3 || cs[0].Hash != hashN(1) || cs[1].Hash != hashN(2) || cs[2].Hash != hashN(3) ||
		cs[0].Token != 3 || cs[2].Kind != doltmodel.RetentionCommit || cs[2].Ordinal != 3 || !cs[2].Date.Equal(doltmodel.CommitDate(3)) {
		t.Errorf("commits %+v: the commit Dolt made when it created the database is not one of them", cs)
	}
	// A token maps to the newest commit made at it: the retention's.
	if e.byToken[5] != 2 || e.byToken[3] != 0 {
		t.Errorf("by token %v", e.byToken)
	}
}

func TestAReopenedEngineContinuesTheCommitSequence(t *testing.T) {
	e, srv, err := reopened().open(t, doltmodel.ArmCommit)
	if err != nil {
		t.Fatal(err)
	}
	srv.reset()
	if err := e.Write([]engine.Record{observe(edge1, 6, "p", 30, 0)}); err != nil {
		t.Fatal(err)
	}
	var commit string
	for _, q := range srv.statements() {
		if strings.HasPrefix(q, "CALL") {
			commit = q
		}
	}
	if !strings.Contains(commit, "'-m', 'seq 6'") || !strings.Contains(commit, "'--date', '2000-01-01T00:00:04Z'") {
		t.Fatalf("the next commit is the fourth: %s", commit)
	}
	// The writer knows the state it found: an older record of p changes no state.
	srv.reset()
	if err := e.Write([]engine.Record{observe(edge1, 7, "p", 25, 0)}); err != nil {
		t.Fatal(err)
	}
	for _, q := range srv.statements() {
		if strings.HasPrefix(q, "REPLACE INTO state") {
			t.Fatalf("a late record replaced a state row the writer found: %s", q)
		}
	}
}

func TestADatabaseWhoseHistoryIsNotAFunctionOfTheStreamIsRefused(t *testing.T) {
	for name, mutate := range map[string]func(d *database){
		"a commit at another date":     func(d *database) { d.log[0] = commitRow(hashN(1), "seq 3", "2026-10-21 09:00:00") },
		"a date out of order":          func(d *database) { d.log[2], d.log[3] = d.log[3], d.log[2] },
		"a message that is not a hash": func(d *database) { d.log[0] = commitRow("not a hash", "seq 3", "2000-01-01 00:00:01") },
		"a newest commit behind the token": func(d *database) {
			d.meta = []driver.Value{doltmodel.EncodeSeq(9), 20 * sec, int64(0), int64(doltmodel.SchemaVersion)}
		},
		"batches and no commits": func(d *database) { d.log = nil },
	} {
		t.Run(name, func(t *testing.T) {
			d := reopened()
			mutate(d)
			if _, _, err := d.open(t, doltmodel.ArmCommit); !errors.Is(err, ErrSchema) {
				t.Fatalf("got %v", err)
			}
		})
	}
	t.Run("commits opened as the arm with none", func(t *testing.T) {
		if _, _, err := reopened().open(t, doltmodel.ArmRows); !errors.Is(err, ErrSchema) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("batches of the arm with none opened as it", func(t *testing.T) {
		d := reopened()
		d.log = [][]driver.Value{commitRow("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "Initialize data repository", "2026-10-21 09:00:00")}
		e, _, err := d.open(t, doltmodel.ArmRows)
		if err != nil {
			t.Fatal(err)
		}
		if e.LastSeq() != 5 {
			t.Fatalf("seq %d", e.LastSeq())
		}
	})
}

func TestADatabaseWithAHorizonPastTheEnd(t *testing.T) {
	d := reopened()
	d.meta = []driver.Value{doltmodel.EncodeSeq(5), int64(1<<63 - 1), int64(1), int64(doltmodel.SchemaVersion)}
	d.log = d.log[:2]
	d.log[1] = commitRow(hashN(2), "seq 5", "2000-01-01 00:00:02")
	e, _, err := d.open(t, doltmodel.ArmCommit)
	if err != nil {
		t.Fatal(err)
	}
	if !e.w.pastEnd || e.w.horizon != 1<<63-1 {
		t.Fatalf("horizon %d, past end %v", e.w.horizon, e.w.pastEnd)
	}
	if err := e.Write([]engine.Record{observe(edge1, 6, "p", 30, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Fatalf("a write after the horizon past the end: %v", err)
	}
}
