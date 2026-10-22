package doltspike

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
)

// RecordFormat is the version of the records doltspike writes. Nothing in a record is a
// time.
const RecordFormat = 1

// The kinds of record.
const (
	KindLoad    = "load"
	KindRead    = "read"
	KindSizes   = "sizes"
	KindExplain = "explain"
)

// Env is what a record is of: the binary that wrote it, the server it reached and the files
// that make the server what it is. Two records are comparable only if these agree.
type Env struct {
	Commit, Modified string
	GoVersion        string
	GOOS, GOARCH     string
	BinarySHA256     string
	// Image is the image the server was said to run, and checked to be the registered one.
	Image string
	// The files that make the server: the settings, the global configuration and the script,
	// as embedded in the binary.
	ServerYAMLSHA256, GlobalJSONSHA256, ServerScriptSHA256 string
	// DoltVersion is what the server says it is, and Settings the observable settings it
	// was checked against.
	DoltVersion       string
	DriverVersion     string
	InterpolateParams bool
	SchemaVersion     int
	PayloadCapacity   int
}

// Input says what the stream and the questions are.
type Input struct {
	// Kind is "plan" or "activity".
	Kind string
	// PlanSHA256 or QuestionsSHA256 name the file the questions are in.
	PlanSHA256, QuestionsSHA256 string
	// StreamDigest is the plan's stream digest, or an activity file's content digest, which is
	// ContentDigest too.
	StreamDigest, ContentDigest string
	QueriesDigest               string
	Records, LastSeq            uint64
	// MaxPayload, DroppedBasis and DroppedBoot are the longest payload of an activity file
	// and the records that had a field the conversion dropped.
	MaxPayload                int `json:",omitempty"`
	DroppedBasis, DroppedBoot uint64
	Label                     string `json:",omitempty"`
}

// Capability is a row of what the design cannot do, or was not asked to: reported as a row,
// never as a failure or a win.
type Capability struct {
	Name   string
	Status string
	// Count is how many records or questions the row names, where it names any.
	Count *uint64 `json:",omitempty"`
}

// LoadRecord is what a load of a stream into one arm did: the writer's counts, the commits
// and the head, and the frontier of each commit, which the diagnostic arm reads.
type LoadRecord struct {
	Kind     string
	Format   int
	Env      Env
	Input    Input
	Arm      string
	Database string
	// Counts are the writer's counts, which are a function of the stream; ModelChecked says
	// the model of the design was written the same stream and its counts and commits were
	// equal.
	Counts       doltmodel.Counts
	ModelChecked bool
	Commits      int
	// Retentions is the number of retentions the stream applied.
	Retentions int
	// HeadHash is the newest commit's hash: two loads of one stream must end at one.
	HeadHash    string `json:",omitempty"`
	HeadOrdinal int    `json:",omitempty"`
	// Frontiers are, for each commit in order, the largest event time (nanoseconds) among the
	// records at or below its token. The database does not hold them; the diagnostic arm
	// reads commit history as event time with them.
	Frontiers       []int64 `json:",omitempty"`
	FrontiersSHA256 string  `json:",omitempty"`
	Capabilities    []Capability
}

// QueryResult is the outcome of one question of a read.
type QueryResult struct {
	Group string
	Rank  int
	Age   string
	Op    string
	Match bool
	// Outcome is how the read was answered: "as-of-alone" (the state as of a commit settled
	// every key), "needed-log" (some keys went to the log), "no-commit" (the token names no
	// commit), "log" (a window) or "commit-time" (the diagnostic).
	Outcome string
	// KeysState and KeysLog are the keys the rule settled by a state row and answered from
	// the log; Work the rows of the key ranges the read holds, derived by the model.
	KeysState, KeysLog uint64
	Work               map[string]uint64 `json:",omitempty"`
}

// GroupResult sums questions of one group at one age, or of one age.
type GroupResult struct {
	Group, Age                       string `json:",omitempty"`
	Queries, Matched                 int
	AsOfAlone, NeededLog, NoCommit   int
	KeysState, KeysLog               uint64
	WorkRowsState, WorkRowsLog       uint64
	WorkWindowRows, WorkWindowPrefix uint64
	Wrong                            int `json:",omitempty"`
}

// ReadRecord is what the questions of a load were answered as, by one arm.
type ReadRecord struct {
	Kind     string
	Format   int
	Env      Env
	Input    Input
	Arm      string
	Database string
	// LoadSHA256 names the load record the database was loaded by, and HeadHash the head
	// commit the reads found, equal to the load's.
	LoadSHA256 string
	HeadHash   string `json:",omitempty"`
	// Diagnostic says the arm is the commit-time reading, whose wrong answers are its result.
	Diagnostic bool
	Queries    int
	Matched    int
	// ModelChecked says the model answered the same questions with the same counts.
	ModelChecked bool
	Groups       []GroupResult
	Ages         []GroupResult
	Results      []QueryResult
	Mismatches   []g0.Mismatch `json:",omitempty"`
}

// Bytes are the bytes of a database's directory, by kind of file.
type Bytes struct {
	Total int64
	// Journal is the chunk journal, Tables the table files (archives included), Statistics
	// the nested repository the server keeps its statistics in, Other the rest.
	Journal, Tables, Statistics, Other int64
	Files                              int
}

// SizesRecord is the bytes a loaded database takes, with the history its commits keep, as
// left and after garbage collection.
type SizesRecord struct {
	Kind     string
	Format   int
	Env      Env
	Input    Input
	Arm      string
	Database string
	// EdgeLogRows and EntityLogRows are the rows the logs hold; RecordsHeld is the records
	// they stand for (an edge record is a row in each direction).
	EdgeLogRows, EntityLogRows, RecordsHeld int64
	Commits                                 int
	AsLeft                                  Bytes
	// AfterGC is set if garbage collection was run.
	AfterGC *Bytes `json:",omitempty"`
	// BytesPerRecord are the totals over the records held, as left and after collection;
	// BytesPerRecordNoStats leaves out the statistics repository.
	BytesPerRecord, BytesPerRecordNoStats     float64
	GCBytesPerRecord, GCBytesPerRecordNoStats float64 `json:",omitempty"`
	// HeadHash is the head commit before and after collection, which must be equal.
	HeadHash string `json:",omitempty"`
}

// ExplainedStatement is the plan the server gave one statement of a read.
type ExplainedStatement struct {
	Kind string
	// Variant says, for a statement the planner may read by one of two indexes of the log
	// that the assumption allows, which it chose: "index on event time" or "primary key".
	Variant string `json:",omitempty"`
	// Plan is the lines of EXPLAIN FORMAT=TREE.
	Plan []string
	// Accesses are the tables the plan reads and how.
	Accesses []Access
	// Range says every table is read through an index (no table scan).
	Range bool
}

// ExplainedQuery is a question and the statements it sends.
type ExplainedQuery struct {
	Group      string
	Rank       int
	Age        string
	Op         string
	Statements []ExplainedStatement
}

// ExplainRecord is the plans of the statements of the reads of a load.
type ExplainRecord struct {
	Kind       string
	Format     int
	Env        Env
	Input      Input
	Arm        string
	Database   string
	LoadSHA256 string
	Queries    []ExplainedQuery
}

// writeNew writes a record under dir, refusing to replace one that is there.
func writeNew(dir, name string, v any) error {
	if err := checkFree(dir, name); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("doltspike: %w", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
