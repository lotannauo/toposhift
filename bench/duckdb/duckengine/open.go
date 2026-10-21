// Package duckengine reads the activity archive through DuckDB: it builds the Parquet
// files of each arm of the baseline from a stream, answers the spike's questions from
// them with SQL, and counts the work a read does.
//
// The package links DuckDB's native library through the database/sql driver, so it
// builds only with cgo; the model it is held to, [duckmodel], does not.
//
// # What is counted
//
// Counters are rows, row groups and bytes, never times. The work counter of a read is the
// row groups it admits: those whose bounds the read's constant filters do not exclude and
// which an equality's bloom filter does not exclude either, counted from the files'
// footers (see [FileMeta.Admit]); rows scanned are the rows of those row groups, and the
// bytes are those of the columns the read touches in them. The profiler's cardinality for
// the scan is recorded beside them. Counting by the bounds alone is a diagnostic and is in
// no slope class, because DuckDB's bloom filters skip row groups that bounds cannot; a
// batch read is counted by an upper bound, labelled as one, which is held to G0 and is in
// no slope class either. The profiler's own count of the rows a scan was handed is the
// whole file's whatever row groups were skipped, and its bytes read depend on what the
// connection has cached, so neither is a counter.
package duckengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
)

// Version is the DuckDB version the baseline is written for, the one that wrote the
// published archive. A connection to another is refused.
const Version = "v1.5.6"

// wantVersion is what [Version] is compared with; a test changes it to see a connection
// to another version refused.
var wantVersion = Version

// Mode says what a connection is for.
type Mode uint8

const (
	// ModeRead is a connection that answers questions: the settings of settings.sql.
	ModeRead Mode = iota + 1
	// ModeBuild is a connection that writes files. It keeps insertion order, as the
	// statement that made the archive did, so the files are a function of the stream.
	ModeBuild
	// ModeObserve is a connection that audits a read rather than counts it: it keeps
	// no cache, so that every read of a file reaches the file system, and logs the
	// file system's reads.
	ModeObserve
)

func (m Mode) String() string {
	switch m {
	case ModeRead:
		return "read"
	case ModeBuild:
		return "build"
	case ModeObserve:
		return "observe"
	}
	return fmt.Sprintf("Mode(%d)", uint8(m))
}

// Config says how to open a connection.
type Config struct {
	Mode Mode
	// TempDir is DuckDB's temp_directory, under the run's directory. It must be set.
	TempDir string
	// Profile turns profiling on, which a read needs to be counted. A connection that
	// is timed does not profile.
	Profile bool
}

// overrides are the settings a mode changes from settings.sql, with the value DuckDB
// prints for each.
func (c Config) overrides() []Setting {
	var out []Setting
	set := func(name, value, want string) {
		out = append(out, Setting{Name: name, Statement: fmt.Sprintf("SET %s = %s;", name, value), Want: want})
	}
	switch c.Mode {
	case ModeBuild:
		set("preserve_insertion_order", "true", "true")
	case ModeObserve:
		set("enable_external_file_cache", "false", "false")
		set("parquet_metadata_cache", "false", "false")
	}
	if c.Profile {
		set("enable_profiling", "'no_output'", "no_output")
		set("profiling_mode", "'standard'", "standard")
	}
	return out
}

// DB is a connection to an in-process DuckDB with the baseline's settings checked.
// There is one connection: a read's statements run one after another on it.
type DB struct {
	db       *sql.DB
	conn     *sql.Conn
	cfg      Config
	settings []Setting
	// connector makes the second connection a build appends on. Closing the database
	// closes it.
	connector *duckdb.Connector
}

// Open starts DuckDB in memory, applies settings.sql and the mode's overrides and the
// temp directory, and refuses the connection if the version or any setting differs
// from what is written.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.Mode < ModeRead || cfg.Mode > ModeObserve {
		return nil, fmt.Errorf("duckengine: mode %d", cfg.Mode)
	}
	if cfg.TempDir == "" {
		return nil, errors.New("duckengine: a temp directory is required")
	}
	temp, err := filepath.Abs(cfg.TempDir)
	if err != nil {
		return nil, err
	}
	base, err := LoadSettings(scripts.SettingsSQL)
	if err != nil {
		return nil, fmt.Errorf("duckengine: settings.sql: %w", err)
	}
	settings := withOverrides(base, cfg.overrides())
	settings = append(settings, Setting{Name: "temp_directory", Statement: "SET temp_directory = " + quote(temp) + ";", Want: temp})

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("duckengine: %w", err)
	}
	d := &DB{db: sql.OpenDB(connector), cfg: cfg, settings: settings, connector: connector}
	d.db.SetMaxOpenConns(1)
	fail := func(err error) (*DB, error) { _ = d.Close(); return nil, err }
	if d.conn, err = d.db.Conn(ctx); err != nil {
		return fail(fmt.Errorf("duckengine: %w", err))
	}
	var version string
	if err := d.conn.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return fail(fmt.Errorf("duckengine: asking for the version: %w", err))
	}
	if version != wantVersion {
		return fail(fmt.Errorf("duckengine: DuckDB is %s, the baseline is written for %s", version, wantVersion))
	}
	if err := Apply(ctx, d.conn, settings); err != nil {
		return fail(err)
	}
	if err := CheckSettings(ctx, d.conn, settings); err != nil {
		return fail(err)
	}
	if cfg.Mode == ModeObserve {
		if err := d.startLogging(ctx); err != nil {
			return fail(err)
		}
	}
	return d, nil
}

// withOverrides puts each override in the place of the setting of its name, or at the
// end if the file has none, so that a name is set and checked once.
func withOverrides(base, over []Setting) []Setting {
	out := slices.Clone(base)
	for _, o := range over {
		if i := slices.IndexFunc(out, func(s Setting) bool { return s.Name == o.Name }); i >= 0 {
			out[i] = o
		} else {
			out = append(out, o)
		}
	}
	return out
}

// Settings are the settings the connection was opened with and checked against.
func (d *DB) Settings() []Setting { return append([]Setting(nil), d.settings...) }

// Check asks again for every setting and refuses the connection if one differs. A
// caller that has run statements of its own checks before it counts.
func (d *DB) Check(ctx context.Context) error { return CheckSettings(ctx, d.conn, d.settings) }

// Config returns what the connection was opened with.
func (d *DB) Config() Config { return d.cfg }

// ExecContext runs a statement that returns no rows.
func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.conn.ExecContext(ctx, query, args...)
}

// QueryContext runs a query.
func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.conn.QueryContext(ctx, query, args...)
}

// QueryRowContext runs a query for one row.
func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.conn.QueryRowContext(ctx, query, args...)
}

// PrepareContext prepares a statement on the connection.
func (d *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.conn.PrepareContext(ctx, query)
}

// Version returns duckdb_version() as the connection reports it.
func (d *DB) Version(ctx context.Context) (string, error) {
	var v string
	err := d.conn.QueryRowContext(ctx, "SELECT version()").Scan(&v)
	return v, err
}

// Close closes the connection and the database.
func (d *DB) Close() error {
	var err error
	if d.conn != nil {
		err = d.conn.Close()
	}
	if d.db != nil {
		err = errors.Join(err, d.db.Close())
	}
	return err
}

// quote is a SQL string literal.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
