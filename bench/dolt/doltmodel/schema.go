package doltmodel

import (
	"fmt"
	"strings"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// MaxProducerLen is the size of the producer columns: a producer longer than this is refused
// with [engine.ErrInvalid], a finite capacity the engine contract allows.
const MaxProducerLen = 255

// Column is a column of a table: its name and SQL type, and whether it may be NULL.
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// Index is a secondary index.
type Index struct {
	Name    string
	Columns []string
}

// Table is a table of the design: its columns, its primary key and its secondary indexes.
type Table struct {
	Name    string
	Columns []Column
	Key     []string
	Indexes []Index
}

// Column types. Every time is a BIGINT of Unix nanoseconds, never a DATETIME, which keeps
// microseconds; a sequence number is a BIGINT with its sign bit flipped; a fingerprint is the
// bytes of [EncodeFingerprint]; and the payload is held in the row, in a column of the
// registered capacity.
var (
	fpType      = fmt.Sprintf("VARBINARY(%d)", catalog.MaxNameLen+1+identity.FingerprintBytes)
	relType     = fmt.Sprintf("VARBINARY(%d)", catalog.MaxNameLen)
	producerTyp = fmt.Sprintf("VARBINARY(%d)", MaxProducerLen)
	payloadType = fmt.Sprintf("VARBINARY(%d)", RegisteredPayloadCapacity)
)

// Schema is the tables, in the order they are created. It is the DDL of the design as data:
// the columns a row type has, the keys the reads and the writer rely on, and the one index on
// event time that windows read through. Whether Dolt accepts these statements, and whether it
// accepts keys of this length, is [U]: nothing here runs them.
//
// The entity tables carry the layer as a value column outside the key, because a
// fingerprint has one layer; a read filters on it.
func Schema() []Table {
	edgeKey := []string{"layer", "dir", "a", "peer", "rel", "producer"}
	edgeCols := []Column{
		{"layer", "TINYINT", false},
		{"dir", "TINYINT", false},
		{"a", fpType, false},
		{"peer", fpType, false},
		{"rel", relType, false},
		{"producer", producerTyp, false},
	}
	entCols := []Column{{"fp", fpType, false}, {"producer", producerTyp, false}}
	record := func(cols []Column) []Column {
		return append(cols[:len(cols):len(cols)],
			Column{"event_ns", "BIGINT", false}, Column{"seq", "BIGINT", false}, Column{"kind", "CHAR(1)", false},
			Column{"ttl_ns", "BIGINT", false}, Column{"through_ns", "BIGINT", true}, Column{"payload", payloadType, false})
	}
	state := func(cols []Column) []Column {
		return append(cols[:len(cols):len(cols)],
			Column{"event_ns", "BIGINT", false}, Column{"seq", "BIGINT", false}, Column{"kind", "CHAR(1)", false},
			Column{"deadline_ns", "BIGINT", true})
	}
	withLayer := func(cols []Column) []Column {
		return append(cols[:len(cols):len(cols)], Column{"layer", "TINYINT", false})
	}
	return []Table{
		{
			Name: "log", Columns: record(edgeCols),
			Key:     append(append([]string{}, edgeKey...), "event_ns", "seq"),
			Indexes: []Index{{"log_time", []string{"layer", "dir", "a", "event_ns", "seq"}}},
		},
		{Name: "ent_log", Columns: record(withLayer(entCols)), Key: []string{"fp", "producer", "event_ns", "seq"}},
		{Name: "state", Columns: state(edgeCols), Key: edgeKey},
		{Name: "ent_state", Columns: state(withLayer(entCols)), Key: []string{"fp", "producer"}},
		{
			Name: "meta",
			Columns: []Column{
				{"id", "TINYINT", false},
				{"last_seq", "BIGINT", false},
				{"horizon_ns", "BIGINT", false},
				{"past_end", "TINYINT", false},
				{"schema_version", "INT", false},
			},
			Key: []string{"id"},
		},
	}
}

// DDL is the statement that creates the table.
func (t Table) DDL() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE %s (\n", t.Name)
	for _, c := range t.Columns {
		null := "NOT NULL"
		if c.Nullable {
			null = "NULL"
		}
		fmt.Fprintf(&sb, "  %s %s %s,\n", c.Name, c.Type, null)
	}
	fmt.Fprintf(&sb, "  PRIMARY KEY (%s)", strings.Join(t.Key, ", "))
	for _, ix := range t.Indexes {
		fmt.Fprintf(&sb, ",\n  KEY %s (%s)", ix.Name, strings.Join(ix.Columns, ", "))
	}
	if t.Name == "meta" {
		sb.WriteString(",\n  CHECK (id = 1)")
	}
	sb.WriteString("\n)")
	return sb.String()
}

// DDL is the statements that create every table, one after another.
func DDL() []string {
	var out []string
	for _, t := range Schema() {
		out = append(out, t.DDL())
	}
	return out
}
