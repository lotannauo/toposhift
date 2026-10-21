// Package pgreport is the record pgspike writes of a load, of the reads of a plan and of
// the sizes of the tables, and the comparison of two of them.
//
// A record holds counts and bytes, never a time: what a read cost in blocks, entries and
// rows, what a load cost in log written, and how large the tables are. Every one of
// them is a function of the stream, the settings and the server's build, which every
// record names, so two runs of one thing can be held to each other: [DiffLoads],
// [DiffReads] and [DiffSizes] say what differs, and refuse to compare two runs that
// were not of the same stream, on the same server build, under the same settings.
//
// The package has no database driver and reads no file but the ones it is told to.
package pgreport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Version is the version of the format of every record.
const Version = 1

// Env is what a record names of the machine and the build that made it: enough to say
// whether two records can be compared, and what made each.
type Env struct {
	// Commit and Modified are the version control stamp of the pgspike binary ("unknown"
	// for a build that has none).
	Commit   string
	Modified string
	// GoVersion, GOOS and GOARCH are of the binary; BinarySHA256 is of its file.
	GoVersion, GOOS, GOARCH string
	BinarySHA256            string
	// SettingsSHA256 is the digest of settings.conf, which the server was checked
	// against.
	SettingsSHA256 string
	// Image is the server image the script pins, by digest.
	Image string
	// ServerVersion is version(); Configure the build options of the server.
	ServerVersion, Configure string
	BlockSize, WALBlockSize  int
	DataChecksums            string
	// ServerCPU is the processor the server runs on.
	ServerCPU string
}

// Input names the stream a run was made from, and the questions asked of it.
type Input struct {
	// Kind is "plan" for a stream a plan regenerates, "activity" for an activity file.
	Kind string
	// PlanSHA256 is the digest of the plan file; empty for an activity file.
	PlanSHA256 string `json:",omitempty"`
	// ContentDigest is the content digest of the activity file; empty for a plan.
	ContentDigest string `json:",omitempty"`
	// QuestionsSHA256 is the digest of the question set of an activity file.
	QuestionsSHA256 string `json:",omitempty"`
	// StreamDigest is the digest of the records and the retentions written.
	StreamDigest string
	// QueriesDigest is the digest of the questions without their answers.
	QueriesDigest string
	Records       uint64
	LastSeq       uint64
	// Label says what the stream is, for whoever reads a figure from it.
	Label string `json:",omitempty"`
}

// Phase is the log a phase of a load wrote; see pgengine.Phase.
type Phase struct {
	Name              string
	Contiguous        bool
	LogBytes          int64
	Records           int64
	FPIs              int64
	RecordBytes       int64
	CheckpointsInside int64
}

// Rows are the rows of each table a load left.
type Rows struct {
	Log, LogBase, Edges, Entities, Dictionary int64
}

// Relation is the size of one table or index; see pgengine.RelationSize.
type Relation struct {
	Name, Kind, Table string
	Main, FSM, VM     int64
}

// Total is the bytes of all forks.
func (r Relation) Total() int64 { return r.Main + r.FSM + r.VM }

// The groups of relations whose sizes the report adds up.
const (
	GroupIntervals  = "intervals"
	GroupLog        = "log"
	GroupDictionary = "dictionary"
	GroupOther      = "other"
)

// Group says which part of the schema a relation is: the interval tables with their
// indexes (what a read at an instant needs), the log with the baselines (what a window and
// a read of an old snapshot need), the dictionary of the id arm, or the rest. An index and
// a toast table belong to the table they serve.
func (r Relation) Group() string {
	owner := r.Name
	if r.Kind != "table" && r.Table != "" {
		owner = r.Table
	}
	switch {
	case owner == "edges" || owner == "entities":
		return GroupIntervals
	case owner == "log" || strings.HasPrefix(owner, "log_"):
		return GroupLog
	case owner == "entity":
		return GroupDictionary
	}
	return GroupOther
}

// Sizes is the bytes of a schema's relations.
type Sizes struct {
	Relations []Relation
	// Total is the sum over every relation, and ByGroup over each group's.
	Total   int64
	ByGroup map[string]int64
}

// NewSizes adds up a list of relations.
func NewSizes(rels []Relation) Sizes {
	s := Sizes{Relations: rels, ByGroup: map[string]int64{}}
	for _, r := range rels {
		s.Total += r.Total()
		s.ByGroup[r.Group()] += r.Total()
	}
	return s
}

// Load is the record of one load.
type Load struct {
	Version int
	Env     Env
	Arm     string
	// Schema is where the load was made. It is not compared: two loads of one stream are
	// in two schemas.
	Schema string
	Input  Input
	// Retentions is how many times the horizon moved, and HorizonNS (Unix nanoseconds,
	// 0 if it never moved) where it ended.
	Retentions int
	HorizonNS  int64
	PastEnd    bool
	// StreamVacuums is how many vacuums the stream's retentions called for.
	StreamVacuums int
	// ExtraVacuums is how many vacuums beyond the registered one the load needed to get
	// every page all-visible; it depends on what the server's background processes held
	// at the time, and is not compared.
	ExtraVacuums int
	Rows         Rows
	// TablesDigest is the digest of the interval tables clipped at the horizon, and
	// TablesRows the rows it covers.
	TablesDigest string
	TablesRows   int64
	Phases       []Phase
	// Sizes are the bytes the load left, as left: before any compaction.
	Sizes Sizes
}

// Counters are the work an explained statement did; see pgengine.Counters.
type Counters struct {
	SharedHit, SharedRead                     int64
	RowsReturned                              int64
	IndexEntries, HeapFetches                 int64
	RowsRemovedByFilter, RowsRemovedByRecheck int64
}

// Add is the sum of two sets of counters, for a read that ran more than one statement.
func (c Counters) Add(o Counters) Counters {
	return Counters{
		SharedHit: c.SharedHit + o.SharedHit, SharedRead: c.SharedRead + o.SharedRead,
		RowsReturned: c.RowsReturned + o.RowsReturned,
		IndexEntries: c.IndexEntries + o.IndexEntries, HeapFetches: c.HeapFetches + o.HeapFetches,
		RowsRemovedByFilter:  c.RowsRemovedByFilter + o.RowsRemovedByFilter,
		RowsRemovedByRecheck: c.RowsRemovedByRecheck + o.RowsRemovedByRecheck,
	}
}

// StatementRun is a statement of a read, cold, with the counters of its plan and its
// shape (the nodes, with the table and index of each scan).
type StatementRun struct {
	// Statement indexes Read.Statements.
	Statement int
	Counters  Counters
	Shape     string
}

// Result is one question: its answer and what the read cost.
type Result struct {
	Name, Group, Age, Op string
	// Digest and Size are the answer, which equals the reference's: a record with a
	// result that does not has no counters at all.
	Digest string
	Size   int
	// Naive says the read was a fold of the log made by the reader, not bounded by
	// time (a read as of an old snapshot, which the interval tables have no axis for),
	// and RowsFetched is the rows of the log it fetched. Otherwise both are zero.
	Naive       bool
	RowsFetched int64
	Runs        []StatementRun
	// Total is the sum over the statements. BytesRead is the blocks read from storage
	// and BytesTouched the blocks read or found in the buffers, times the block size.
	Total        Counters
	BytesRead    int64
	BytesTouched int64
}

// Mismatch is an answer that differs from the reference's.
type Mismatch struct {
	Query, Op             string
	GotDigest, WantDigest string
	GotSize, WantSize     int
	Error                 string `json:",omitempty"`
}

// Read is the record of the reads of a plan's questions on one load.
type Read struct {
	Version int
	Env     Env
	Arm     string
	// Schema is not compared.
	Schema string
	Input  Input
	// Queries is how many questions the plan has and Matched how many were answered as
	// the reference answered them. A read with Matched below Queries has no Results.
	Queries, Matched int
	Mismatches       []Mismatch `json:",omitempty"`
	// Repeats is how many cold runs each statement was explained in; they were equal.
	Repeats int
	// Statements is the text of every distinct statement the reads ran.
	Statements []string
	Results    []Result
}

// SizesReport is the record of the sizes of a schema, as left and, if it was asked for,
// after compaction.
type SizesReport struct {
	Version int
	Env     Env
	Arm     string
	// Schema is not compared.
	Schema string
	// Records is the records of the stream in the tables, so that a size can be given
	// per record.
	Records   uint64
	AsLeft    Sizes
	Compacted *Sizes `json:",omitempty"`
}

// Write writes a record as JSON, indented, to dir/name. It creates dir. The output
// holds no time, so the same record is the same bytes.
func Write(dir, name string, v any) error {
	b, err := marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), b, 0o600)
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", " ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ErrFormat is returned by [ReadFile] for a record of another version or one that
// has fields this version does not know.
var ErrFormat = errors.New("pgreport: not a record of this version")

// ReadFile reads a record of the type v points to, refusing an unknown field: a record
// that has one was made by a build that records something this one would not compare.
func ReadFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w: %w", path, ErrFormat, err)
	}
	var version int
	switch r := v.(type) {
	case *Load:
		version = r.Version
	case *Read:
		version = r.Version
	case *SizesReport:
		version = r.Version
	default:
		return fmt.Errorf("pgreport: cannot read a %T", v)
	}
	if version != Version {
		return fmt.Errorf("%s: version %d, this build reads %d: %w", path, version, Version, ErrFormat)
	}
	return nil
}
