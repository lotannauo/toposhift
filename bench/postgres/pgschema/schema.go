// Package pgschema is the DDL of the Postgres baseline's tables, as text, and the
// column lists the statements that fill them use. It has no driver: the statements
// are run by the engine that owns a connection, and checked here only for their
// shape. (A statement that Postgres rejects is found by running it against a server;
// nothing in this package does.)
//
// Every arm lives in a schema of its own ([SchemaName]); the statements name no
// schema, so the connection's search_path decides. The extensions are installed in
// public, once per database, so that their operator classes are found from every
// arm's schema.
//
// The tables are those of the design: a log of every stored record (partitioned by
// the day of the event time for the streaming arm), a table of retention baselines,
// and the derived interval tables, edges and entities, whose time column is an
// int8range of Unix nanoseconds. See package pgmodel for what a row means and how it
// is kept.
package pgschema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"
)

// Arm is one set of tables.
type Arm string

const (
	// Stream is the arm a streaming writer keeps current: a day-partitioned log,
	// a primary key on the interval tables, heap-only updates of a provisional
	// deadline, and a btree on the end of the interval for retention.
	Stream Arm = "pg-stream"
	// StreamNoPartition is Stream with an unpartitioned log: the "delete and
	// vacuum" retention, kept as a diagnostic.
	StreamNoPartition Arm = "pg-stream-nopart"
	// Bulk is the arm a bulk load builds: nothing is updated, so no primary key,
	// no fill factor and no index on the end; GiST indexes on the interval, built
	// after the load.
	Bulk Arm = "pg-bulk"
	// BulkB is Bulk with btree indexes on the end of the interval, not the start
	// (index set B), where Bulk has GiST indexes (index set G).
	BulkB Arm = "pg-bulk-b"
	// BulkID is Bulk with a dictionary of entities and a bigint id in place of
	// each fingerprint: the same load, for the bytes only.
	BulkID Arm = "pg-bulk-id"
)

// Arms lists every arm.
func Arms() []Arm { return []Arm{Stream, StreamNoPartition, Bulk, BulkB, BulkID} }

func (a Arm) known() bool {
	for _, b := range Arms() {
		if a == b {
			return true
		}
	}
	return false
}

// Streaming reports whether the arm is kept current by a writer, as opposed to
// loaded in bulk.
func (a Arm) Streaming() bool { return a == Stream || a == StreamNoPartition }

// partitioned reports whether the arm's log is partitioned by day.
func (a Arm) partitioned() bool { return a == Stream }

// keyType is the type of the column that holds an entity.
func (a Arm) keyType() string {
	if a == BulkID {
		return "bigint"
	}
	return "bytea"
}

// maxInt64 is the upper bound an open interval is given by index set B.
const maxInt64 = "9223372036854775807"

// SchemaName is the schema an arm's tables live in for a directory: the first 12 hex
// digits of the SHA-256 of the directory, after "s_". A conformance factory that is
// given a directory and later given it again finds its tables.
func SchemaName(dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return "s_" + hex.EncodeToString(sum[:])[:12]
}

// Extensions are the statements that make the extensions the tables need available,
// in public. btree_gist gives GiST its operator classes for bytea, smallint and
// bigint. pg_buffercache lets a superuser evict buffers one at a time
// (pg_buffercache_evict, PostgreSQL 17); it is not a trusted extension, so the role
// that creates it must be a superuser.
func Extensions() []string {
	return []string{
		"CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public",
		"CREATE EXTENSION IF NOT EXISTS pg_buffercache SCHEMA public",
	}
}

// Column is a column of a table, with its type and constraints.
type Column struct{ Name, Def string }

// Columns of the tables. The statements that fill a table name its columns, and a
// test holds the DDL to these lists.
var (
	// LogColumns are the columns of log and log_base. src and dst hold fingerprints
	// (or ids, in the BulkID arm); for an entity record src is the entity and dst
	// and rel are null. seq, here and in meta, is the ingest sequence number, a uint64,
	// stored with its top bit flipped so that bigint order is its order.
	LogColumns = []Column{
		{"seq", "bigint NOT NULL"},
		{"event_ns", "bigint NOT NULL"},
		{"layer", "smallint NOT NULL"},
		{"subject_kind", `"char" NOT NULL`}, // 'e' entity, 'g' edge
		{"src", "{key} NOT NULL"},
		{"dst", "{key}"},
		{"rel", `text COLLATE "C"`},
		{"producer", `text COLLATE "C" NOT NULL`},
		{"kind", `"char" NOT NULL`},
		{"ttl_ns", "bigint NOT NULL"},
		{"through_ns", "bigint"},
		{"payload", "bytea NOT NULL"},
	}
	// EdgeColumns are the columns of edges.
	EdgeColumns = []Column{
		{"src", "{key} NOT NULL"},
		{"dst", "{key} NOT NULL"},
		{"rel", `text COLLATE "C" NOT NULL`},
		{"layer", "smallint NOT NULL"},
		{"start_ns", "bigint NOT NULL"},
		{"during", "int8range NOT NULL"},
		{"deadline_ns", "bigint"},
		{"end_source", "smallint NOT NULL"},
	}
	// EntityColumns are the columns of entities.
	EntityColumns = []Column{
		{"fp", "{key} NOT NULL"},
		{"layer", "smallint NOT NULL"},
		{"start_ns", "bigint NOT NULL"},
		{"during", "int8range NOT NULL"},
		{"deadline_ns", "bigint"},
		{"end_source", "smallint NOT NULL"},
	}
)

// Names lists the names of the columns.
func Names(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func columns(cols []Column, key string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.Name + " " + strings.ReplaceAll(c.Def, "{key}", key)
	}
	return strings.Join(parts, ",\n  ")
}

// Tables are the statements that create an arm's tables, empty and without indexes
// except the primary keys of the streaming arms.
func Tables(a Arm) ([]string, error) {
	if !a.known() {
		return nil, fmt.Errorf("pgschema: unknown arm %q", a)
	}
	key := a.keyType()
	var out []string
	if a == BulkID {
		out = append(out, "CREATE TABLE entity (\n  id bigint PRIMARY KEY,\n  fp bytea NOT NULL UNIQUE\n)")
	}
	// What an engine must find again when it opens a schema it left: the token, the
	// retention horizon, and whether the horizon was moved past the last instant (a
	// bigint cannot say it), under the fold logic and the arm that built the rows. Every
	// arm has it, the bulk ones too: the highest seq in the log is not the token once a
	// retention has dropped a late record.
	out = append(out, `CREATE TABLE meta (
  one bool PRIMARY KEY DEFAULT true CHECK (one),
  last_seq bigint NOT NULL,
  horizon_ns bigint,
  past_end bool NOT NULL DEFAULT false,
  fold_version int NOT NULL,
  arm text NOT NULL
)`)
	log := "CREATE TABLE log (\n  " + columns(LogColumns, key) + "\n)"
	if a.partitioned() {
		log += "\nPARTITION BY RANGE (event_ns)"
	}
	out = append(out, log)
	// Retention baselines: the last record of each subject and producer before the
	// horizon, as the log's columns, standing for everything a retention dropped.
	out = append(out, "CREATE TABLE log_base (\n  "+columns(LogColumns, key)+"\n)")

	fill := ""
	pkEdges, pkEntities := "", ""
	if a.Streaming() {
		// The interval tables a writer updates: a primary key, and room on each
		// page for heap-only updates.
		fill = "\nWITH (fillfactor = 85)"
		pkEdges = ",\n  PRIMARY KEY (src, dst, rel, start_ns)"
		pkEntities = ",\n  PRIMARY KEY (fp, start_ns)"
	}
	// The two columns that repeat the range say the same thing as it: the start is
	// the range's lower bound, and a deadline belongs only to a row whose range is
	// still unbounded (a provisional one).
	checks := ",\n  CHECK (start_ns = lower(during)),\n  CHECK (deadline_ns IS NULL OR upper_inf(during))"
	out = append(out,
		"CREATE TABLE edges (\n  "+columns(EdgeColumns, key)+pkEdges+checks+"\n)"+fill,
		"CREATE TABLE entities (\n  "+columns(EntityColumns, key)+pkEntities+checks+"\n)"+fill,
	)
	return out, nil
}

// Indexes are the statements that create an arm's secondary indexes. A streaming arm
// has them from the start; a bulk arm builds them after the load.
func Indexes(a Arm) ([]string, error) {
	if !a.known() {
		return nil, fmt.Errorf("pgschema: unknown arm %q", a)
	}
	// The log, read by Window (an edge's records in an event-time window), by the
	// reads as of an old token and by the writer (the records of a subject). On the
	// partitioned log an index is a set of per-partition indexes.
	out := []string{
		"CREATE INDEX log_g_src ON log (src, layer, event_ns, seq) WHERE subject_kind = 'g'",
		"CREATE INDEX log_g_dst ON log (dst, layer, event_ns, seq) WHERE subject_kind = 'g'",
		"CREATE INDEX log_e ON log (src, layer, event_ns, seq) WHERE subject_kind = 'e'",
		// One edge subject's records, for the writer's re-fold and the reads as of an
		// old token: the subject is (src, dst, rel), which log_g_src alone would
		// filter after reading every record of the source.
		"CREATE INDEX log_g_subject ON log (src, dst, rel, event_ns, seq) WHERE subject_kind = 'g'",
	}
	out = append(out, "CREATE INDEX log_base_subject ON log_base (src, dst, rel, producer)")
	switch a {
	case Stream, StreamNoPartition:
		// The *_end indexes find the rows retention deletes: those whose range has
		// ended. A provisional row has an unbounded range, so it is not in them; the
		// predicate of the retention delete is RetentionPredicate, whose second term is
		// answered by a scan unless the finalizer has closed the rows first.
		out = append(out,
			"CREATE INDEX edges_src_g ON edges USING gist (src, layer, during) INCLUDE (dst, rel)",
			"CREATE INDEX edges_dst_g ON edges USING gist (dst, layer, during) INCLUDE (src, rel)",
			"CREATE INDEX edges_end ON edges (upper(during)) WHERE NOT upper_inf(during)",
			"CREATE INDEX entities_g ON entities USING gist (fp, layer, during)",
			"CREATE INDEX entities_end ON entities (upper(during)) WHERE NOT upper_inf(during)",
		)
	case Bulk, BulkID:
		out = append(out,
			"CREATE INDEX edges_src_g ON edges USING gist (src, layer, during) INCLUDE (dst, rel)",
			"CREATE INDEX edges_dst_g ON edges USING gist (dst, layer, during) INCLUDE (src, rel)",
			"CREATE INDEX entities_g ON entities USING gist (fp, layer, during)",
		)
	case BulkB:
		// A btree on the end of the interval, with the interval carried in the index:
		// what is alive near now is what ends after now, so the rows scanned grow
		// with the intervals that end after the instant, not with all history. An
		// open interval ends at the largest bigint.
		end := "coalesce(upper(during), " + maxInt64 + ")"
		out = append(out,
			"CREATE INDEX edges_src_b ON edges (src, layer, "+end+") INCLUDE (dst, rel, during)",
			"CREATE INDEX edges_dst_b ON edges (dst, layer, "+end+") INCLUDE (src, rel, during)",
			"CREATE INDEX entities_b ON entities (fp, layer, "+end+") INCLUDE (during)",
		)
	}
	return out, nil
}

// RetentionPredicate is the condition under which retention deletes an interval row
// for the horizon $1: its range has ended at or before it, or it is provisional and its
// deadline has passed it. The first term is served by the *_end indexes.
const RetentionPredicate = "upper(during) <= $1 OR deadline_ns <= $1"

// Setup is the statements that make a streaming arm ready to write: the extensions,
// the tables and the indexes. For a bulk arm it is the extensions and the tables; the
// indexes follow the load.
func Setup(a Arm) ([]string, error) {
	tables, err := Tables(a)
	if err != nil {
		return nil, err
	}
	out := Extensions()
	out = append(out, tables...)
	if a.Streaming() {
		idx, err := Indexes(a)
		if err != nil {
			return nil, err
		}
		out = append(out, idx...)
	}
	return out, nil
}

// DayPartition returns the name of the partition of log that holds the UTC day of
// the instant, and the statement that creates it if it is not there. The partition's range is the day in
// Unix nanoseconds, from midnight to the next midnight; the last day an engine stores
// has no next midnight in a bigint, so it runs to MAXVALUE. An instant before the epoch
// is an error.
func DayPartition(ns int64) (name, stmt string, err error) {
	if ns < 0 {
		return "", "", fmt.Errorf("pgschema: %d is before the epoch", ns)
	}
	const day = int64(24 * time.Hour)
	lo := ns - ns%day
	name = "log_p" + time.Unix(0, lo).UTC().Format("20060102")
	hi := "MAXVALUE"
	if lo <= math.MaxInt64-day {
		hi = fmt.Sprint(lo + day)
	}
	return name, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF log FOR VALUES FROM (%d) TO (%s)", name, lo, hi), nil
}
