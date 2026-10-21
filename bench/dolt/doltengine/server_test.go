package doltengine_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The tests in files named *_server_test.go need a Dolt server: TOPOSHIFT_DOLT_DSN names it
// (a go-sql-driver DSN, such as root@tcp(127.0.0.1:3306)/bench), and scripts/dolt-server.sh
// starts the one the settings of the baseline are written for. Without it they skip. Each
// test makes its engines' databases and drops them when it ends.
const dsnEnv = "TOPOSHIFT_DOLT_DSN"

// serverDSN returns the server to test against, or skips.
func serverDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set: no Dolt server to test against (scripts/dolt-server.sh starts one)", dsnEnv)
	}
	return dsn
}

// openEngine opens an engine in a database of its own and drops the database when the test
// ends.
func openEngine(t testing.TB, arm doltmodel.Arm) *doltengine.Engine {
	t.Helper()
	dsn := serverDSN(t)
	name := doltengine.DatabaseName(t.TempDir())
	e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		if err := doltengine.DropDatabase(context.Background(), dsn, name); err != nil {
			t.Error(err)
		}
	})
	return e
}

// factory is the conformance factory for an arm: the directory the harness gives names the
// database, so that an engine closed and opened again over the same directory finds what it
// left. The databases are dropped when the test ends.
func factory(t *testing.T, arm doltmodel.Arm) conformance.Factory {
	dsn := serverDSN(t)
	var mu sync.Mutex
	made := map[string]bool{}
	t.Cleanup(func() {
		for name := range made {
			if err := doltengine.DropDatabase(context.Background(), dsn, name); err != nil {
				t.Error(err)
			}
		}
	})
	return func(dir string) (engine.Engine, error) {
		name := doltengine.DatabaseName(dir)
		mu.Lock()
		made[name] = true
		mu.Unlock()
		return doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: name, Arm: arm})
	}
}

// connect opens a connection of the test's own to a database of the server, which the caller
// closes.
func connect(dsn, database string) (*sql.DB, error) {
	dc, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	dc.DBName = database
	dc.InterpolateParams = doltengine.InterpolateParams
	conn, err := mysql.NewConnector(dc)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(1)
	return db, nil
}

// rawDB is a connection of the test's own to a database of the server, for the tests that
// read and change tables directly or probe what Dolt does. It is not an engine's.
func rawDB(t testing.TB, database string) *sql.DB {
	t.Helper()
	db, err := connect(serverDSN(t), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// scratch makes a database of the test's own (not an engine's) and drops it when the test
// ends.
func scratch(t testing.TB) (db *sql.DB, name string) {
	t.Helper()
	name = doltengine.DatabaseName(t.TempDir())
	admin := rawDB(t, "")
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := doltengine.DropDatabase(context.Background(), serverDSN(t), name); err != nil {
			t.Error(err)
		}
	})
	return rawDB(t, name), name
}
