package doltengine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// loadedDatabase makes a database of the arm with a few batches and a retention, closes the
// engine, and returns the database's name and what the engine said of it.
func loadedDatabase(t *testing.T, arm doltmodel.Arm) (name string, head doltengine.Commit, hasHead bool) {
	t.Helper()
	dsn := serverDSN(t)
	name = doltengine.DatabaseName(t.TempDir())
	e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = doltengine.DropDatabase(context.Background(), dsn, name) })
	p := newPair(t, e, arm)
	for _, b := range [][]engine.Record{
		{observe(edge1, 1, "a", 100, 60), existence(pod1, 2, "a", 101, 0)},
		{observe(edge2, 3, "a", 110, 0), del(edge1, 4, "a", 120)},
		{observe(edge1, 5, "a", 130, 20), existence(node1, 6, "a", 135, 0)},
	} {
		if err := p.write(b...); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.retain(at(125)); err != nil {
		t.Fatal(err)
	}
	head, hasHead = e.Head()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	return name, head, hasHead
}

func TestGarbageCollectionRefusesADatabaseThatHasAConnectionOpenAndRunsWithNone(t *testing.T) {
	for _, arm := range arms {
		t.Run(arm.String(), func(t *testing.T) {
			dsn := serverDSN(t)
			name, head, hasHead := loadedDatabase(t, arm)

			// An engine open on the database: its connections would be broken, so the call
			// is refused, and the engine goes on working.
			e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
			if err != nil {
				t.Fatal(err)
			}
			if err := doltengine.GC(context.Background(), dsn, name); !errors.Is(err, doltengine.ErrBusy) {
				t.Fatalf("garbage collection with an engine open: %v", err)
			}
			if ok, err := e.Alive(pod1, at(110), engine.Current(catalog.L2)); err != nil || !ok {
				t.Fatalf("the engine after a refused collection: %v, %v", ok, err)
			}
			// A connection of any other kind in the database counts too.
			raw := rawDB(t, name)
			if _, err := raw.ExecContext(context.Background(), "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if err := doltengine.GC(context.Background(), dsn, name); !errors.Is(err, doltengine.ErrBusy) {
				t.Fatalf("garbage collection with a raw connection open: %v", err)
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}

			// With none open it runs, and the database opens again as it was.
			if err := doltengine.GC(context.Background(), dsn, name); err != nil {
				t.Fatal(err)
			}
			e2, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
			if err != nil {
				t.Fatalf("opening after garbage collection: %v", err)
			}
			defer func() { _ = e2.Close() }()
			got, ok := e2.Head()
			if ok != hasHead || got != head {
				t.Fatalf("head after garbage collection %+v (%v), before %+v (%v)", got, ok, head, hasHead)
			}
			if e2.LastSeq() != 6 {
				t.Fatalf("seq %d after garbage collection", e2.LastSeq())
			}
			// The reads after it, as of an old token too, are the answers before it.
			if arm == doltmodel.ArmCommit {
				if ok, err := e2.Alive(pod1, at(110), engine.Scope{Layer: catalog.L2, AsOf: 2}); err != nil || !ok {
					t.Fatalf("an old token after garbage collection: %v, %v", ok, err)
				}
			}
			edge, ent, err := e2.Held(context.Background())
			if err != nil || edge == 0 || ent == 0 {
				t.Fatalf("held %d and %d rows: %v", edge, ent, err)
			}
		})
	}
}

func TestGarbageCollectionNamesADatabaseTheServerHas(t *testing.T) {
	dsn := serverDSN(t)
	if err := doltengine.GC(context.Background(), dsn, "no_such_database_here"); err == nil {
		t.Fatal("garbage collection of a database that does not exist")
	}
	if err := doltengine.GC(context.Background(), dsn, "Not A Name"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a name no engine accepts: %v", err)
	}
	if ok, err := doltengine.DatabaseExists(context.Background(), dsn, "no_such_database_here"); err != nil || ok {
		t.Fatalf("exists: %v, %v", ok, err)
	}
	name, _, _ := loadedDatabase(t, doltmodel.ArmRows)
	if ok, err := doltengine.DatabaseExists(context.Background(), dsn, name); err != nil || !ok {
		t.Fatalf("exists: %v, %v", ok, err)
	}
	if _, err := doltengine.DatabaseExists(context.Background(), dsn, "Not A Name"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a name no engine accepts: %v", err)
	}
}

// TestTheRowsHeldAreTheLogsRowsAfterARetention: an edge record is a row in each direction,
// an entity record one.
func TestTheRowsHeldAreTheLogsRowsAfterARetention(t *testing.T) {
	e := openEngine(t, doltmodel.ArmRows)
	if edge, ent, err := e.Held(context.Background()); err != nil || edge != 0 || ent != 0 {
		t.Fatalf("an empty database holds %d and %d: %v", edge, ent, err)
	}
	if err := e.Write([]engine.Record{observe(edge1, 1, "a", 100, 0), observe(edge1, 2, "a", 110, 0), existence(pod1, 3, "a", 100, 0)}); err != nil {
		t.Fatal(err)
	}
	if edge, ent, err := e.Held(context.Background()); err != nil || edge != 4 || ent != 1 {
		t.Fatalf("held %d and %d: %v", edge, ent, err)
	}
	// A retention keeps the newest row of each key before the horizon, and deletes the rest.
	if err := e.Retain(at(200)); err != nil {
		t.Fatal(err)
	}
	if edge, ent, err := e.Held(context.Background()); err != nil || edge != 2 || ent != 1 {
		t.Fatalf("held %d and %d after a retention: %v", edge, ent, err)
	}
}
