package xtengine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TestOpenRefusesWhatItCannotUseBeforeItConnects: none of these needs a node.
func TestOpenRefusesWhatItCannotUseBeforeItConnects(t *testing.T) {
	const dsn = "host=127.0.0.1 port=1 user=xtdb dbname=xtdb sslmode=disable connect_timeout=2"
	for name, cfg := range map[string]Config{
		"a prefix with a quote":             {DSN: dsn, Prefix: `t"; drop`},
		"a prefix with capitals":            {DSN: dsn, Prefix: "T_"},
		"a prefix that starts with a digit": {DSN: dsn, Prefix: "1t"},
		"a prefix too long":                 {DSN: dsn, Prefix: strings.Repeat("a", 32)},
		"a query mode that is none":         {DSN: dsn, Mode: "pipeline"},
	} {
		if e, err := Open(context.Background(), cfg); !errors.Is(err, engine.ErrInvalid) {
			if e != nil {
				_ = e.Close()
			}
			t.Errorf("%s: %v", name, orNil(err))
		}
	}
	if _, err := Open(context.Background(), Config{}); err == nil {
		t.Error("no DSN was accepted")
	}
	if _, err := Open(context.Background(), Config{DSN: "postgres://%zz"}); err == nil {
		t.Error("a DSN that does not parse was accepted")
	}
	// A node that is not there is an error, and no engine.
	if e, err := Open(context.Background(), Config{DSN: dsn}); err == nil {
		_ = e.Close()
		t.Error("an unreachable node was accepted")
	}
}

func TestQueryModes(t *testing.T) {
	for mode, want := range map[QueryMode]pgx.QueryExecMode{
		"":         pgx.QueryExecModeSimpleProtocol,
		ModeCache:  pgx.QueryExecModeCacheStatement,
		ModeExec:   pgx.QueryExecModeExec,
		ModeSimple: pgx.QueryExecModeSimpleProtocol,
	} {
		got, err := mode.exec()
		if err != nil || got != want {
			t.Errorf("mode %q is %v, %v; want %v", mode, got, err, want)
		}
	}
	if DefaultMode == "" {
		t.Error("no default mode")
	}
}

// TestOpenReportsWhatItCannotRead: a node that refuses the engine's first questions is an
// error, and the connections are released.
func TestOpenReportsWhatItCannotRead(t *testing.T) {
	names, _ := namesFor("")
	node := newFakeNode()
	failWhen(node, func(conn, sql string) bool { return strings.Contains(sql, "FROM log_e") }, errors.New("the node refused the query"))
	w, r := node.conn("w"), node.conn("r")
	if e, err := openWith(context.Background(), Config{}, names, w, r); err == nil {
		_ = e.Close()
		t.Fatal("an engine opened over a node that refused its questions")
	}
	if !w.closed || !r.closed {
		t.Errorf("connections left open: writer closed %v, reader closed %v", w.closed, r.closed)
	}
}
