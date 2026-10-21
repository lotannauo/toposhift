package doltengine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// RegisteredVersion is the version of Dolt the baseline is registered against, as
// dolt_version() prints it.
const RegisteredVersion = "2.4.2"

// Setting ties a key of scripts/server.yaml to what a client can see of it: the system
// variable whose value the server prints, and the value the file's key gives it. A key a
// client cannot see (the log level, the garbage-collection switch) is not here, and is only
// in the file, which is the one source of every setting.
type Setting struct {
	// Key is the key in server.yaml, with its parents, dotted.
	Key string
	// Var is the system variable that shows it.
	Var string
	// Want is the value the variable prints when the key has the value the file gives.
	Want string
}

// ObservableSettings are the settings of server.yaml that the engine checks the server
// against, and a test holds to the file ([ParseServerConfig]).
var ObservableSettings = []Setting{
	{"behavior.autocommit", "autocommit", "0"},
	{"behavior.dolt_transaction_commit", "dolt_transaction_commit", "0"},
	{"listener.max_connections", "max_connections", "8"},
}

// CheckServer refuses a server that is not the registered one: another version of Dolt, or
// any observable setting that differs from scripts/server.yaml. Everything the engine
// measures was registered against that server, and a number from another is not that
// number.
func CheckServer(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT dolt_version()").Scan(&version); err != nil {
		return fmt.Errorf("doltengine: asking the server its version: %w", err)
	}
	if version != RegisteredVersion {
		return fmt.Errorf("doltengine: the server is Dolt %s, the registered version is %s: %w", version, RegisteredVersion, ErrServer)
	}
	for _, s := range ObservableSettings {
		var got string
		// The variable's name is one of the constants above, never input.
		if err := db.QueryRowContext(ctx, "SELECT @@"+s.Var).Scan(&got); err != nil {
			return fmt.Errorf("doltengine: reading @@%s: %w", s.Var, err)
		}
		if got != s.Want {
			return fmt.Errorf("doltengine: @@%s is %q on the server, server.yaml's %s gives %q: %w", s.Var, got, s.Key, s.Want, ErrServer)
		}
	}
	return nil
}

// ParseServerConfig reads the subset of YAML that scripts/server.yaml is written in, a
// tree of keys that are either a scalar or a mapping, two spaces of indent for each
// level, comments after a #, and returns each scalar by its dotted key. It refuses anything
// else (a tab, a list, an odd indent, a repeated key, a line with no colon), so that the
// file cannot hold a setting this reading does not see.
func ParseServerConfig(text string) (map[string]string, error) {
	out := make(map[string]string)
	parents := make(map[string]bool) // keys that hold a mapping
	var path []string
	for n, line := range strings.Split(text, "\n") {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("server.yaml line %d: %s", n+1, fmt.Sprintf(format, args...))
		}
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.ContainsRune(line, '\t') {
			return nil, fail("a tab")
		}
		body := strings.TrimLeft(line, " ")
		indent := len(line) - len(body)
		if indent%2 != 0 {
			return nil, fail("an indent of %d spaces", indent)
		}
		depth := indent / 2
		if depth > len(path) {
			return nil, fail("an indent that is not under a key")
		}
		path = path[:depth]
		key, value, found := strings.Cut(strings.TrimRight(body, " "), ":")
		if !found || key == "" || strings.ContainsAny(key, " \"'-") {
			return nil, fail("%q is not key: value", body)
		}
		value = strings.TrimSpace(value)
		full := strings.Join(append(path, key), ".")
		if _, dup := out[full]; dup || parents[full] {
			return nil, fail("%s is set twice", full)
		}
		if value == "" {
			path = append(path, key)
			parents[full] = true
			continue
		}
		if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "[") || strings.HasPrefix(value, "{") {
			return nil, fail("%s: a list or a flow mapping", full)
		}
		out[full] = strings.Trim(value, `"'`)
	}
	// A key that holds a mapping must hold something.
	for p := range parents {
		held := false
		for k := range out {
			held = held || strings.HasPrefix(k, p+".")
		}
		for k := range parents {
			held = held || strings.HasPrefix(k, p+".")
		}
		if !held {
			return nil, fmt.Errorf("server.yaml: %s is a mapping with no keys", p)
		}
	}
	return out, nil
}

// boolVar is how a MySQL server prints a boolean setting.
func boolVar(v string) (string, error) {
	switch v {
	case "true":
		return "1", nil
	case "false":
		return "0", nil
	}
	return "", fmt.Errorf("%q is not a boolean", v)
}

// WantFromConfig is the value a client sees for a setting, given the file's parsed keys: an
// integer as it is, a boolean as 0 or 1.
func (s Setting) WantFromConfig(cfg map[string]string) (string, error) {
	v, ok := cfg[s.Key]
	if !ok {
		return "", fmt.Errorf("server.yaml has no %s", s.Key)
	}
	if b, err := boolVar(v); err == nil {
		return b, nil
	}
	return v, nil
}
