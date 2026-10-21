package pgengine_test

import (
	"context"
	"flag"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The tests in this file need a Postgres server: TOPOSHIFT_PG_DSN names it (a pgx
// connection string), and scripts/pg-server.sh starts the one the settings of the
// baseline are written for. Without it they skip. Each test makes its engines'
// schemas and drops them when it ends.
const dsnEnv = "TOPOSHIFT_PG_DSN"

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// serverDSN returns the server to test against, or skips.
func serverDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set: no Postgres server to test against (scripts/pg-server.sh starts one)", dsnEnv)
	}
	return dsn
}

// openEngine opens an engine in a schema of its own and drops the schema when the
// test ends.
func openEngine(t testing.TB, arm pgschema.Arm) *pgengine.Engine {
	t.Helper()
	dsn := serverDSN(t)
	schema := pgschema.SchemaName(t.TempDir())
	e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: arm})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		if err := pgengine.DropSchema(context.Background(), dsn, schema); err != nil {
			t.Error(err)
		}
	})
	return e
}

// factory is the conformance factory for an arm: the directory the harness gives
// names the schema, so that an engine closed and opened again over the same
// directory finds what it left. The schemas are dropped when the test ends.
func factory(t *testing.T, arm pgschema.Arm) conformance.Factory {
	dsn := serverDSN(t)
	var mu sync.Mutex
	var made []string
	t.Cleanup(func() {
		for _, schema := range made {
			if err := pgengine.DropSchema(context.Background(), dsn, schema); err != nil {
				t.Error(err)
			}
		}
	})
	return func(dir string) (engine.Engine, error) {
		schema := pgschema.SchemaName(dir)
		mu.Lock()
		made = append(made, schema)
		mu.Unlock()
		return pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: arm})
	}
}

// TestConformance is the engine contract on a live server: the streaming arm is
// checked against the reference engine on random and fixed workloads with
// mid-stream retentions, on the scripted checks, on a reopen and on concurrent reads.
func TestConformance(t *testing.T) {
	conformance.Run(t, factory(t, pgschema.Stream))
}

// sqlConn is a connection to the server whose search path is an engine's schema, for
// the tests that read and change its tables directly.
func sqlConn(t testing.TB, e *pgengine.Engine) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), serverDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(context.Background(), "SET search_path TO "+pgx.Identifier{e.Schema()}.Sanitize()+", public"); err != nil {
		t.Fatal(err)
	}
	return conn
}
