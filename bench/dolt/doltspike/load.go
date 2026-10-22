package doltspike

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// ErrCapacity is wrapped by the error a load returns for an input whose longest payload is
// over the payload column's registered capacity: a record the design cannot hold is a
// capability row, and the count of them must be zero on a registered input.
var ErrCapacity = errors.New("doltspike: the input's payloads are over the registered capacity")

// loadOpts say what a load is of.
type loadOpts struct {
	DSN, Database string
	Arm           doltmodel.Arm
	Stream        *stream
	// Model also writes the stream to the model of the design and requires its counts, its
	// commits and the frontiers of its commits to equal the database's.
	Model bool
	// Replace drops a database of the name that exists; without it one is refused.
	Replace bool
	Say     runner.Progress
}

// loader is the sink a stream is written through: the engine, the model if there is one, and
// the frontier of each commit, the largest event time among the records written so far.
type loader struct {
	e *doltengine.Engine
	m *doltmodel.Model

	frontier   int64
	frontiers  []int64
	retentions int
}

func (l *loader) note() {
	head, ok := l.e.Head()
	if !ok {
		return
	}
	for len(l.frontiers) < head.Ordinal {
		l.frontiers = append(l.frontiers, l.frontier)
	}
}

func (l *loader) Write(b []engine.Record) error {
	if err := l.e.Write(b); err != nil {
		return err
	}
	for _, r := range b {
		l.frontier = max(l.frontier, doltmodel.Nanos(r.EventTime))
	}
	l.note()
	if l.m != nil {
		if err := l.m.Write(slices.Clone(b)); err != nil {
			return fmt.Errorf("doltspike: the model refused a batch the server took: %w", err)
		}
	}
	return nil
}

func (l *loader) Retain(h time.Time) error {
	if err := l.e.Retain(h); err != nil {
		return err
	}
	l.retentions++
	l.note()
	if l.m != nil {
		if err := l.m.Retain(h); err != nil {
			return fmt.Errorf("doltspike: the model refused a retention the server made: %w", err)
		}
	}
	return nil
}

// frontiersDigest is the digest of the frontiers, as big-endian integers in order.
func frontiersDigest(fs []int64) string {
	h := sha256.New()
	var b [8]byte
	for _, f := range fs {
		binary.BigEndian.PutUint64(b[:], uint64(f))
		h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// load writes the stream into a new database of the arm and describes what it did. A load
// that fails drops the database it made, so no partial one is left.
func load(ctx context.Context, o loadOpts, env Env) (rec *LoadRecord, err error) {
	if need := o.Stream.maxPayload(); need > doltmodel.RegisteredPayloadCapacity {
		return nil, fmt.Errorf("%w: the stream's payloads reach %d bytes, the column holds %d", ErrCapacity, need, doltmodel.RegisteredPayloadCapacity)
	}
	exists, err := doltengine.DatabaseExists(ctx, o.DSN, o.Database)
	if err != nil {
		return nil, err
	}
	if exists {
		if !o.Replace {
			return nil, fmt.Errorf("doltspike: database %s exists (-replace drops it; a stream is never loaded into a database twice)", o.Database)
		}
		if err := doltengine.DropDatabase(ctx, o.DSN, o.Database); err != nil {
			return nil, err
		}
	}
	e, err := doltengine.Open(ctx, doltengine.Config{DSN: o.DSN, Database: o.Database, Arm: o.Arm})
	if err != nil {
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = e.Close()
		}
		if err != nil {
			// A failed load leaves nothing behind. Dropping is best effort: the load's own
			// error is the one to report.
			_ = doltengine.DropDatabase(context.WithoutCancel(ctx), o.DSN, o.Database)
		}
	}()
	l := &loader{e: e, frontier: doltmodel.NoFrontier}
	if o.Model {
		l.m = doltmodel.New(doltmodel.WithArm(o.Arm), doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
	}
	tell(o.Say, "load: writing the stream into %s (arm %s)", o.Database, o.Arm)
	if err := o.Stream.drive(ctx, l, e.LastSeq, o.Say); err != nil {
		return nil, err
	}
	c := e.Counts()
	rec = &LoadRecord{
		Kind: KindLoad, Format: RecordFormat, Env: env, Input: o.Stream.input(), Arm: o.Arm.String(), Database: o.Database,
		Counts: c, Retentions: l.retentions,
	}
	if head, ok := e.Head(); ok {
		rec.HeadHash, rec.HeadOrdinal = head.Hash, head.Ordinal
		rec.Commits = head.Ordinal
	}
	if o.Arm == doltmodel.ArmCommit {
		if len(l.frontiers) != rec.Commits {
			return nil, fmt.Errorf("doltspike: %d frontiers for %d commits", len(l.frontiers), rec.Commits)
		}
		rec.Frontiers, rec.FrontiersSHA256 = l.frontiers, frontiersDigest(l.frontiers)
	}
	if l.m != nil {
		if err := checkModel(l.m, e, rec); err != nil {
			return nil, err
		}
		rec.ModelChecked = true
	}
	refused := c.PayloadRefusals
	rec.Capabilities = []Capability{
		{Name: "a payload over the column's capacity", Status: "records refused for it; must be zero on a registered input", Count: &refused},
		{Name: "retention that frees history", Status: "none: a commit references what a retention deletes, so no bytes are freed while commits are kept (see sizes)"},
		{Name: "a diff between two tokens", Status: "Dolt has one (DOLT_DIFF); it has no counterpart in the other engines, so it is listed and not compared"},
		{Name: "multi-hop reads", Status: "not built"},
	}
	if err := e.Close(); err != nil {
		return nil, err
	}
	closed = true
	return rec, nil
}

// checkModel holds the database's counts to the model's: the writer's counts, the number of
// commits and, commit for commit, the model's frontier to the one the loader computed.
func checkModel(m *doltmodel.Model, e *doltengine.Engine, rec *LoadRecord) error {
	if mc, ec := m.Counts(), e.Counts(); mc != ec {
		return fmt.Errorf("doltspike: the writer's counts are %+v, the model's %+v", ec, mc)
	}
	mcs := m.Commits()
	if len(mcs) != rec.Commits {
		return fmt.Errorf("doltspike: %d commits on the server, the model made %d", rec.Commits, len(mcs))
	}
	for i, c := range mcs {
		if rec.Arm == doltmodel.ArmCommit.String() && c.Frontier != rec.Frontiers[i] {
			return fmt.Errorf("doltspike: the frontier of commit %d is %d, the model's %d", i+1, rec.Frontiers[i], c.Frontier)
		}
	}
	return nil
}

func tell(say runner.Progress, format string, args ...any) {
	if say != nil {
		say(format, args...)
	}
}

func runLoad(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("load", stderr)
	var (
		c   common
		in  inputFlags
		arm string
		mod bool
		rep bool
	)
	c.register(fs, true)
	in.register(fs, true)
	fs.StringVar(&arm, "arm", "", "the arm: commit (a Dolt commit per batch) or rows (none)")
	fs.BoolVar(&mod, "model", false, "also write the stream to the model of the design and require equal counts, commits and frontiers (the model holds the whole stream in memory)")
	fs.BoolVar(&rep, "replace", false, "drop the database first if it exists")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: doltspike load -dsn dsn -image image -db name -arm commit|rows -out dir (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha -activity file -expect-activity digest)")
	}
	a, err := parseArm(arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(true, true); err != nil {
		return err
	}
	name := "load-" + a.String() + ".json"
	if err := checkFree(c.out, name); err != nil {
		return err
	}
	s, err := in.resolve(true)
	if err != nil {
		return err
	}
	env, err := serverEnv(ctx, c.dsn, c.image)
	if err != nil {
		return err
	}
	rec, err := load(ctx, loadOpts{DSN: c.dsn, Database: c.db, Arm: a, Stream: s, Model: mod, Replace: rep, Say: progress(stderr)}, env)
	if err != nil {
		return err
	}
	if err := writeNew(c.out, name, rec); err != nil {
		return err
	}
	printLoad(stdout, rec)
	return nil
}

// parseArm reads an arm's name. The diagnostic is an arm of a read, not of a load.
func parseArm(s string, diagnostic bool) (doltmodel.Arm, error) {
	switch s {
	case "commit":
		return doltmodel.ArmCommit, nil
	case "rows":
		return doltmodel.ArmRows, nil
	}
	if diagnostic {
		return 0, fmt.Errorf("doltspike: no arm %q: the arms are commit, rows and commit-time", s)
	}
	return 0, fmt.Errorf("doltspike: no arm %q: a load is of commit or rows", s)
}

func printLoad(w io.Writer, r *LoadRecord) {
	fmt.Fprintf(w, "input: %s, stream digest %s, %d records to seq %d\n", r.Input.Kind, r.Input.StreamDigest, r.Input.Records, r.Input.LastSeq)
	fmt.Fprintf(w, "arm %s, database %s\n", r.Arm, r.Database)
	c := r.Counts
	fmt.Fprintf(w, "writer counts: records %d, log rows %d, state replacements %d, log deletes %d, state deletes %d, commits %d, retentions %d\n",
		c.Records, c.LogRows, c.StateWrites, c.LogDeletes, c.StateDeletes, c.Commits, r.Retentions)
	if r.HeadHash != "" {
		fmt.Fprintf(w, "head commit %s (ordinal %d); frontiers sha256 %s\n", r.HeadHash, r.HeadOrdinal, r.FrontiersSHA256)
	}
	if r.ModelChecked {
		fmt.Fprintln(w, "model: the same stream gave equal counts, commits and frontiers")
	}
	for _, cap := range r.Capabilities {
		if cap.Count != nil {
			fmt.Fprintf(w, "capability: %s: %d (%s)\n", cap.Name, *cap.Count, cap.Status)
			continue
		}
		fmt.Fprintf(w, "capability: %s: %s\n", cap.Name, cap.Status)
	}
}
