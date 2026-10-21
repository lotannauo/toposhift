package doltengine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestParseServerConfig(t *testing.T) {
	got, err := ParseServerConfig(`# a comment
log_level: warning

behavior:
  autocommit: false   # trailing
  auto_gc_behavior:
    enable: false
listener:
  max_connections: 8
  host: "0.0.0.0"
`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"log_level": "warning", "behavior.autocommit": "false", "behavior.auto_gc_behavior.enable": "false",
		"listener.max_connections": "8", "listener.host": "0.0.0.0",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	for name, text := range map[string]string{
		"a tab":                    "behavior:\n\tautocommit: false\n",
		"an odd indent":            "behavior:\n   autocommit: false\n",
		"an indent with no parent": "  autocommit: false\n",
		"a jump":                   "a:\n    b: 1\n",
		"a list":                   "a:\n  - b\n",
		"a flow list":              "a: [1, 2]\n",
		"a flow mapping":           "a: {b: 1}\n",
		"no colon":                 "autocommit false\n",
		"an empty key":             ": 1\n",
		"a repeated key":           "a: 1\na: 2\n",
		"a repeated nested":        "a:\n  b: 1\n  b: 2\n",
		"a key with a space":       "a b: 1\n",
		"a mapping with no keys":   "a:\n",
	} {
		if _, err := ParseServerConfig(text); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestTheObservableSettingsAreTheFilesOwn: every setting the engine checks the server
// against is a key of scripts/server.yaml and has the value the file gives it, so the file is
// the one source; and the settings the design depends on have the values it needs.
func TestTheObservableSettingsAreTheFilesOwn(t *testing.T) {
	text, err := os.ReadFile("../scripts/server.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseServerConfig(string(text))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range ObservableSettings {
		got, err := s.WantFromConfig(cfg)
		if err != nil {
			t.Error(err)
			continue
		}
		if got != s.Want {
			t.Errorf("%s: the file gives %q, the engine expects the server to print %q", s.Key, got, s.Want)
		}
	}
	for key, want := range map[string]string{
		"behavior.autocommit": "false", "behavior.dolt_transaction_commit": "false", "behavior.auto_gc_behavior.enable": "false",
		"behavior.read_only": "false", "log_level": "warning", "listener.max_connections": "8", "listener.host": "0.0.0.0",
	} {
		if cfg[key] != want {
			t.Errorf("%s is %q, the design needs %q", key, cfg[key], want)
		}
	}
	if (Setting{Key: "nothing.here"}).Var != "" {
		t.Fatal("zero setting")
	}
	if _, err := (Setting{Key: "nothing.here"}).WantFromConfig(cfg); err == nil {
		t.Error("a key the file does not have was found")
	}
}

func TestCheckServerRefusesAServerThatIsNotTheRegisteredOne(t *testing.T) {
	ok := func(q string, _ []driver.Value) fakeResult {
		switch {
		case strings.HasPrefix(q, "SELECT dolt_version()"):
			return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{RegisteredVersion}}}
		case strings.HasPrefix(q, "SELECT @@"):
			for _, s := range ObservableSettings {
				if q == "SELECT @@"+s.Var {
					return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{s.Want}}}
				}
			}
		}
		return fakeResult{err: errors.New("unexpected " + q)}
	}
	check := func(handle func(string, []driver.Value) fakeResult) error {
		srv := &fakeServer{handle: handle}
		db := sql.OpenDB(fakeConnector{srv})
		defer func() { _ = db.Close() }()
		return CheckServer(context.Background(), db)
	}
	if err := check(ok); err != nil {
		t.Fatal(err)
	}
	if err := check(func(q string, a []driver.Value) fakeResult {
		if strings.HasPrefix(q, "SELECT dolt_version()") {
			return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{"2.5.1"}}}
		}
		return ok(q, a)
	}); !errors.Is(err, ErrServer) {
		t.Errorf("another version: %v", err)
	}
	for _, s := range ObservableSettings {
		if err := check(func(q string, a []driver.Value) fakeResult {
			if q == "SELECT @@"+s.Var {
				return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{s.Want + "1"}}}
			}
			return ok(q, a)
		}); !errors.Is(err, ErrServer) {
			t.Errorf("a server with another %s: %v", s.Var, err)
		}
	}
	if err := check(func(string, []driver.Value) fakeResult { return fakeResult{err: errors.New("down")} }); err == nil {
		t.Error("a server that does not answer was accepted")
	}
}
