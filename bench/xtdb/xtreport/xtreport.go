// Package xtreport is the record xtspike writes of an environment, of a load, of the reads
// of a plan and of sizes.
//
// A record holds counts, bytes and digests, never a time: it has no field of a time or
// duration type (a test holds the types to that), and the only seconds in it are the
// constants of the settle protocol the run was configured with. Every figure in it is a
// function of the stream, the build of the baseline and the node it ran against, all of
// which the record names, so that two records of one thing can be held to each other and a
// record of a node that was not the registered one says so.
//
// The package has no database driver and reads no file but the ones it is told to.
package xtreport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Version is the version of the format of every record.
const Version = 1

// Container is what the node's container said about itself.
type Container struct {
	Name         string
	ImageRef     string
	ImageID      string
	JavaOptions  string
	XTDBVersion  string
	GitSHA       string
	ConfigSHA256 string
	Running      bool
}

// Settle is the settle protocol a run was configured with.
type Settle struct {
	QuietSeconds int
	MaxSeconds   int
	Registered   bool
}

// Env is what a record names of the machine, the build and the node that made it: enough to
// say whether two records can be compared, and whether the node was the registered one.
type Env struct {
	// Commit and Modified are the version control stamp of the xtspike binary ("unknown"
	// for a build that has none).
	Commit   string
	Modified string
	// GoVersion, GOOS and GOARCH are of the binary; BinarySHA256 is of its file.
	GoVersion, GOOS, GOARCH string
	BinarySHA256            string
	// CPUModel and NumCPU are of the machine xtspike ran on (the node's container shares
	// it); CPUModel is "unknown" where the machine does not say.
	CPUModel string
	NumCPU   int
	// QueryMode is how the driver sends statements.
	QueryMode string
	// Image, JavaOptions and ConfigSHA256 are what the server script and xtdb.yaml register.
	Image        string
	JavaOptions  string
	ConfigSHA256 string
	// Container is what the node's container said about itself, if it was inspected.
	Container *Container `json:",omitempty"`
	// Overrides name the environment variables that change the node's configuration and
	// are set. Registered is true only for a node whose container was inspected and is the
	// pinned image with the pinned options and configuration, and no override set;
	// NotRegistered says how it is not.
	Overrides     []string `json:",omitempty"`
	Registered    bool
	NotRegistered []string `json:",omitempty"`
	Settle        Settle
}

// EnvRecord is the record of an environment alone.
type EnvRecord struct {
	Version int
	Kind    string // "env"
	Env     Env
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
	// Retentions are the horizons the stream moved (a plan's), and Dropped the records the
	// stream's driver did not offer because they were before the horizon.
	Retentions int
	Dropped    uint64
	// Label says what the stream is, for whoever reads a figure from it.
	Label string `json:",omitempty"`
}

// Table is the statements the planner wrote to one table.
type Table struct {
	Name           string
	Inserts        uint64
	PortionDeletes uint64
}

// Writer is what the engine's planner wrote for the stream, the units an XTDB writer is
// charged in: a function of the stream, the batch boundaries and the planner, the same on
// every run.
type Writer struct {
	// Batches and Records are what was committed. Transactions are the batches and the
	// retentions together, the ordinals the node's system times count.
	Batches      uint64
	Records      uint64
	Transactions uint64
	Retentions   uint64
	// InOrder and Late are the records planned from the planner's memory and from the log.
	InOrder, Late uint64
	MergedAway    uint64
	Tables        []Table
	// Statements are all the inserts and portion deletes of the planner, and Documents the
	// inserts only (one document each).
	Statements uint64
	Documents  uint64
	// StatementsPerRecord and DocumentsPerRecord are the two over the records.
	StatementsPerRecord float64
	DocumentsPerRecord  float64
}

// Dropped is the fields of a record an engine record does not hold, counted over a stream.
type Dropped struct {
	Basis uint64
	Boot  uint64
}

// Load is the record of a load.
type Load struct {
	Version int
	Kind    string // "load"
	Env     Env
	Input   Input
	Prefix  string
	// SystemBase is the system time of the zeroth transaction: the n-th is at this plus n
	// microseconds. The registered base is 2000-01-01T00:00:00Z.
	SystemBase string
	Writer     Writer
	// Dropped are the fields dropped by the input (an activity file's).
	Dropped Dropped `json:",omitempty"`
	// BlockFinished says whether the node was asked to finish the block it was filling after
	// the last batch and processed the request, before the settle protocol began: only then are
	// the node's directories sized.
	BlockFinished bool
	// Settled says whether the node went quiet within the cap; SettleReason says what it was
	// still doing if it did not.
	Settled      bool
	SettleReason string `json:",omitempty"`
}

// Group is the questions of one group at one age, and how many had the reference's answer.
type Group struct {
	Group   string
	Age     string
	Queries int
	Matched int
}

// Mismatch is a question answered differently from the reference engine.
type Mismatch struct {
	Query string
	Op    string
	Got   string
	Want  string
	Error string `json:",omitempty"`
}

// G0 is the first gate: every question of the plan had the reference engine's answer.
type G0 struct {
	Queries       int
	Matched       int
	OK            bool
	Groups        []Group
	Mismatches    []Mismatch `json:",omitempty"`
	AnswersDigest string
}

// Reads says how the node's answers were made.
type Reads struct {
	// Native are the entity reads answered at a system time or the newest; ClientFold the
	// reads answered by folding the log in Go (a token inside a batch).
	Native     uint64
	ClientFold uint64
}

// Capability is a family of what XTDB, in this mapping, did not do or could not hold, reported as
// a row and never as a failure. Rows counts what the family covers; Queries names the
// questions of the plan it leaves out of a timing cell, which the plan's questions do not
// reach at all for the first two.
type Capability struct {
	Family  string
	Rows    uint64
	Unit    string
	Queries []string `json:",omitempty"`
	Note    string
}

// Read is the record of the reads of a plan against a loaded node.
type Read struct {
	Version int
	Kind    string // "read"
	Env     Env
	Input   Input
	Prefix  string
	// LoadSHA256 is the digest of the load record this read was held to.
	LoadSHA256 string
	Settled    bool
	// SettleReason says what the node was still doing, if it did not settle.
	SettleReason string `json:",omitempty"`
	G0           G0
	Reads        Reads
	Capability   []Capability
}

// Sizes is the record of what the node holds on disk, taken after the settle protocol.
type Sizes struct {
	Version int
	Kind    string // "sizes"
	Env     Env
	Input   Input
	Prefix  string
	// LoadSHA256 is the digest of the load record the sizes are of.
	LoadSHA256 string
	// BlockFinished is the load's: sizes are recorded only for a node whose last block was
	// finished.
	BlockFinished bool
	Settled       bool
	// SettleReason says what the node was still doing, if it did not settle: its sizes are
	// not those of a settled node, and are not to be compared.
	SettleReason string `json:",omitempty"`
	// RecordsHeld are the records the node holds: the whole stream, since retention is not
	// mapped, so every record offered is held.
	RecordsHeld uint64
	Dirs        []Dir
	// Bytes is the total of the directories, and BytesPerRecord is Bytes over RecordsHeld.
	Bytes          int64
	BytesPerRecord float64
	// ListingDigest covers the directories' listings, so that two loads can be said to agree
	// byte for byte by comparing one string.
	ListingDigest string
	Note          string
}

// Dir is the size of one directory.
type Dir struct {
	Path          string
	Bytes         int64
	Files         int
	ListingDigest string
}

// Write writes a record under dir, never replacing a file: a record that was overwritten by a
// later run is a number that cannot be traced.
func Write(dir, name string, v any) error {
	b, err := marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
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

// ErrFormat is returned by [ReadFile] for a record of another version or one that has fields
// this version does not know.
var ErrFormat = errors.New("xtreport: not a record of this version")

// ReadFile reads a record of the type v points to, refusing an unknown field: a record
// that this version cannot read in full is not read in part, and its version is checked.
func ReadFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrFormat, path, err)
	}
	var head struct{ Version int }
	if err := json.Unmarshal(b, &head); err != nil || head.Version != Version {
		return fmt.Errorf("%w: %s: version %d, this build reads %d", ErrFormat, path, head.Version, Version)
	}
	return nil
}
