package pgengine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TestOpenRefusesWhatItCannotUseBeforeItConnects: none of these needs a server.
func TestOpenRefusesWhatItCannotUseBeforeItConnects(t *testing.T) {
	const dsn = "host=127.0.0.1 port=1 user=postgres dbname=postgres sslmode=disable connect_timeout=2"
	for name, cfg := range map[string]pgengine.Config{
		"the id arm":                        {DSN: dsn, Schema: "s_abc", Arm: pgschema.BulkID},
		"an unknown arm":                    {DSN: dsn, Schema: "s_abc", Arm: "pg-nothing"},
		"no arm":                            {DSN: dsn, Schema: "s_abc"},
		"no schema":                         {DSN: dsn, Arm: pgschema.Stream},
		"a schema with a quote":             {DSN: dsn, Schema: `s"; drop schema public; --`, Arm: pgschema.Stream},
		"a schema with capitals":            {DSN: dsn, Schema: "S_abc", Arm: pgschema.Stream},
		"a schema that starts with a digit": {DSN: dsn, Schema: "1abc", Arm: pgschema.Stream},
		"a schema name too long":            {DSN: dsn, Schema: "s_" + strings.Repeat("a", 63), Arm: pgschema.Stream},
	} {
		if e, err := pgengine.Open(context.Background(), cfg); !errors.Is(err, engine.ErrInvalid) {
			if e != nil {
				_ = e.Close()
			}
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := pgengine.Open(context.Background(), pgengine.Config{Schema: "s_abc", Arm: pgschema.Stream}); err == nil {
		t.Error("no DSN was accepted")
	}
	if _, err := pgengine.Open(context.Background(), pgengine.Config{DSN: "postgres://%zz", Schema: "s_abc", Arm: pgschema.Stream}); err == nil {
		t.Error("a DSN that does not parse was accepted")
	}
	// A server that is not there is an error, and no engine.
	if e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: "s_abc", Arm: pgschema.Stream}); err == nil {
		_ = e.Close()
		t.Error("an unreachable server was accepted")
	}
	if err := pgengine.DropSchema(context.Background(), dsn, "public; drop"); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("dropping a schema with a bad name: %v", err)
	}
}
