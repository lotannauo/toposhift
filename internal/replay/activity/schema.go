package activity

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// FormatVersion is the version of the file format this package writes and
// reads.
const FormatVersion = 1

// The columns, in file order. The numbers are positions in the schema and in
// every row group.
const (
	colSeq = iota
	colEventTime
	colLayer
	colSubjectKind
	colSource
	colTarget
	colRelation
	colProducer
	colKind
	colTTL
	colThrough
	colPayload
	colBoot
	colEventTimeBasis
	numColumns
)

// column describes one column of the schema.
type column struct {
	name     string
	physical parquet.Type
	logical  schema.LogicalType // nil for a column without an annotation
	rep      parquet.Repetition
}

var (
	typeInt32     = parquet.Types.Int32
	typeInt64     = parquet.Types.Int64
	typeBytes     = parquet.Types.ByteArray
	repRequired   = parquet.Repetitions.Required
	repOptional   = parquet.Repetitions.Optional
	logicalUint64 = schema.NewIntLogicalType(64, false)
	logicalInt32  = schema.NewIntLogicalType(32, true)
	logicalInt64  = schema.NewIntLogicalType(64, true)
	logicalString = schema.StringLogicalType{}
)

// columns is the schema of format version 1. It is exact: the writer builds
// the file's schema from it and the reader refuses any file that differs.
var columns = [numColumns]column{
	colSeq:            {"seq", typeInt64, logicalUint64, repRequired},
	colEventTime:      {"event_time_ns", typeInt64, logicalInt64, repRequired},
	colLayer:          {"layer", typeBytes, logicalString, repRequired},
	colSubjectKind:    {"subject_kind", typeBytes, logicalString, repRequired},
	colSource:         {"source", typeBytes, logicalString, repRequired},
	colTarget:         {"target", typeBytes, logicalString, repOptional},
	colRelation:       {"relation", typeBytes, logicalString, repOptional},
	colProducer:       {"producer", typeBytes, logicalString, repRequired},
	colKind:           {"kind", typeBytes, logicalString, repRequired},
	colTTL:            {"ttl_ns", typeInt64, logicalInt64, repRequired},
	colThrough:        {"through_ns", typeInt64, logicalInt64, repOptional},
	colPayload:        {"payload", typeBytes, nil, repRequired},
	colBoot:           {"boot", typeBytes, logicalString, repOptional},
	colEventTimeBasis: {"event_time_basis", typeInt32, logicalInt32, repRequired},
}

// The names in the footer's key-value metadata.
const (
	keyPrefix  = "toposhift.activity."
	keyVersion = "toposhift.activity.version"
	keyRecords = "toposhift.activity.records"
	keyMinSeq  = "toposhift.activity.min_seq"
	keyMaxSeq  = "toposhift.activity.max_seq"
	keyDigest  = "toposhift.activity.digest"
	keyGroups  = "toposhift.activity.group_digests"
)

// buildSchema returns the Parquet schema of format version 1.
func buildSchema() (*schema.GroupNode, error) {
	fields := make(schema.FieldList, numColumns)
	for i, c := range columns {
		var (
			n   *schema.PrimitiveNode
			err error
		)
		if c.logical != nil {
			n, err = schema.NewPrimitiveNodeLogical(c.name, c.rep, c.logical, c.physical, -1, -1)
		} else {
			n, err = schema.NewPrimitiveNode(c.name, c.rep, c.physical, -1, -1)
		}
		if err != nil {
			return nil, fmt.Errorf("activity schema, column %q: %w", c.name, err)
		}
		fields[i] = n
	}
	root, err := schema.NewGroupNode("activity", repRequired, fields, -1)
	if err != nil {
		return nil, fmt.Errorf("activity schema: %w", err)
	}
	return root, nil
}

// checkSchema reports, as a plain error, how sc differs from the schema of
// format version 1. It returns nil when they are the same.
func checkSchema(sc *schema.Schema) error {
	root := sc.Root()
	if root.NumFields() != numColumns || sc.NumColumns() != numColumns {
		return fmt.Errorf("the schema has %d fields and %d columns, want %d flat columns", root.NumFields(), sc.NumColumns(), numColumns)
	}
	for i, want := range columns {
		got := sc.Column(i)
		if got.Name() != want.name || len(got.ColumnPath()) != 1 {
			return fmt.Errorf("column %d is %q, want %q", i, got.Path(), want.name)
		}
		if got.PhysicalType() != want.physical {
			return fmt.Errorf("column %q has physical type %s, want %s", want.name, got.PhysicalType(), want.physical)
		}
		wantLogical := want.logical
		if wantLogical == nil {
			wantLogical = schema.NoLogicalType{}
		}
		if gotLogical := got.LogicalType(); gotLogical == nil || !gotLogical.Equals(wantLogical) {
			return fmt.Errorf("column %q has logical type %v, want %v", want.name, gotLogical, wantLogical)
		}
		if rep := got.SchemaNode().RepetitionType(); rep != want.rep {
			return fmt.Errorf("column %q is %s, want %s", want.name, rep, want.rep)
		}
	}
	return nil
}
