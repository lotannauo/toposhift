package duckengine

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// ErrSettingsDiffer is wrapped by the error [CheckSettings] returns for a connection
// whose settings are not the ones the baseline is written for.
var ErrSettingsDiffer = errors.New("duckengine: the connection's settings differ from settings.sql")

// Setting is a statement of settings.sql: a name, the statement that sets it, and the
// value DuckDB prints for it afterwards.
type Setting struct {
	Name      string
	Statement string
	Want      string
}

var (
	settingName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	// A line is SET name = value; with an optional comment after the semicolon. The
	// value is true, false, a number or a quoted string without a quote in it.
	settingLine = regexp.MustCompile(`^SET\s+([A-Za-z0-9_]+)\s*=\s*(true|false|-?[0-9]+|'[^']*')\s*;\s*(?:--\s*(.*?))?\s*$`)
)

// ParseSettings reads settings.sql: one SET statement per line, comments starting with
// two dashes, blank lines. A line that is not a setting, a name given twice and a file
// with no setting are refused, because a setting that is silently ignored would let a
// measurement run on a connection it was not written for.
func ParseSettings(r io.Reader) ([]Setting, error) {
	var out []Setting
	seen := make(map[string]int)
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		m := settingLine.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("settings line %d: %q is not SET name = value;", n, line)
		}
		name, value, expect := m[1], m[2], m[3]
		if !settingName.MatchString(name) {
			return nil, fmt.Errorf("settings line %d: %q is not a setting name (lower case letters, digits and underscores)", n, name)
		}
		if first, dup := seen[name]; dup {
			return nil, fmt.Errorf("settings line %d: %s is set twice (first on line %d)", n, name, first)
		}
		seen[name] = n
		want := strings.Trim(value, "'")
		if expect != "" {
			want = expect
		}
		if want == "" {
			return nil, fmt.Errorf("settings line %d: %s would have to read back as nothing", n, name)
		}
		out = append(out, Setting{Name: name, Statement: fmt.Sprintf("SET %s = %s;", name, value), Want: want})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("settings: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("settings: no setting in the file")
	}
	return out, nil
}

// Execer runs a statement that returns no rows.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Querier runs a query for one row.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Apply runs the statement of each setting.
func Apply(ctx context.Context, ex Execer, settings []Setting) error {
	for _, s := range settings {
		if _, err := ex.ExecContext(ctx, s.Statement); err != nil {
			return fmt.Errorf("duckengine: %s: %w", s.Statement, err)
		}
	}
	return nil
}

// SettingsError lists the settings a connection has otherwise than written.
type SettingsError struct {
	// Differences are "name: DuckDB has X, settings.sql has Y".
	Differences []string
}

func (e *SettingsError) Error() string {
	return ErrSettingsDiffer.Error() + ": " + strings.Join(e.Differences, "; ")
}

func (e *SettingsError) Unwrap() error { return ErrSettingsDiffer }

// CheckSettings asks DuckDB for each setting and refuses the connection unless every
// value is the one written. A setting DuckDB does not know is a difference too: it is
// one the file asks for that the connection cannot have. An error from DuckDB, as
// opposed to a different value, is returned as it is.
func CheckSettings(ctx context.Context, q Querier, want []Setting) error {
	var diff []string
	for _, s := range want {
		var got sql.NullString
		err := q.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = ?", s.Name).Scan(&got)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			diff = append(diff, fmt.Sprintf("%s: DuckDB has no such setting, settings.sql has %s", s.Name, s.Want))
		case err != nil:
			return fmt.Errorf("duckengine: asking for %s: %w", s.Name, err)
		case !got.Valid || got.String != s.Want:
			diff = append(diff, fmt.Sprintf("%s: DuckDB has %s, settings.sql has %s", s.Name, got.String, s.Want))
		}
	}
	if len(diff) > 0 {
		return &SettingsError{Differences: diff}
	}
	return nil
}

// LoadSettings parses settings.sql from its bytes.
func LoadSettings(b []byte) ([]Setting, error) { return ParseSettings(bytes.NewReader(b)) }
