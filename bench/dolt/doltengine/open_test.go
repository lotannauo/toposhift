package doltengine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TestOpenRefusesWhatItCannotUseBeforeItConnects: none of these needs a server.
func TestOpenRefusesWhatItCannotUseBeforeItConnects(t *testing.T) {
	const dsn = "root@tcp(127.0.0.1:1)/x?timeout=2s"
	for name, cfg := range map[string]doltengine.Config{
		"an unknown arm":                      {DSN: dsn, Database: "d_abc", Arm: 9},
		"no database":                         {DSN: dsn},
		"a database with a quote":             {DSN: dsn, Database: "d`; drop database mysql; --"},
		"a database with capitals":            {DSN: dsn, Database: "D_abc"},
		"a database that starts with a digit": {DSN: dsn, Database: "1abc"},
		"a database with a dash":              {DSN: dsn, Database: "d-abc"},
		"a database name too long":            {DSN: dsn, Database: "d_" + strings.Repeat("a", 63)},
	} {
		if e, err := doltengine.Open(context.Background(), cfg); !errors.Is(err, engine.ErrInvalid) {
			if e != nil {
				_ = e.Close()
			}
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := doltengine.Open(context.Background(), doltengine.Config{Database: "d_abc"}); err == nil {
		t.Error("no DSN was accepted")
	}
	if _, err := doltengine.Open(context.Background(), doltengine.Config{DSN: "root@tcp(", Database: "d_abc"}); err == nil {
		t.Error("a DSN that does not parse was accepted")
	}
	// A server that is not there is an error, and no engine.
	if e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: "d_abc", Arm: doltmodel.ArmCommit}); err == nil {
		_ = e.Close()
		t.Error("an unreachable server was accepted")
	}
	if err := doltengine.DropDatabase(context.Background(), dsn, "x`; drop"); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("dropping a database with a bad name: %v", err)
	}
	if err := doltengine.DropDatabase(context.Background(), "root@tcp(", "d_abc"); err == nil {
		t.Error("a DSN that does not parse was accepted by DropDatabase")
	}
}

// TestADatabaseNameIsAFunctionOfTheDirectory: the same directory always names the same
// database, two directories never do, and every name is one Open accepts.
func TestADatabaseNameIsAFunctionOfTheDirectory(t *testing.T) {
	a, b := doltengine.DatabaseName("/tmp/one"), doltengine.DatabaseName("/tmp/two")
	if a == b || a != doltengine.DatabaseName("/tmp/one") {
		t.Fatalf("%s %s", a, b)
	}
	if _, _, err := (doltengine.Config{DSN: "root@tcp(127.0.0.1:1)/", Database: a}).Check(); err != nil {
		t.Fatalf("%s: %v", a, err)
	}
}
