package doltspike

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// armCommitTime is the name of the diagnostic arm of a read.
const armCommitTime = "commit-time"

// readLoad reads a load record and returns it with the digest of its file.
func readLoad(path string) (*LoadRecord, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var r LoadRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, "", fmt.Errorf("doltspike: %s: %w", path, err)
	}
	if r.Kind != KindLoad || r.Format != RecordFormat {
		return nil, "", fmt.Errorf("doltspike: %s is not a load record of format %d", path, RecordFormat)
	}
	sum := sha256.Sum256(b)
	return &r, hex.EncodeToString(sum[:]), nil
}

// sameInput says whether two descriptions of a stream are of one: everything but the label.
func sameInput(a, b Input) bool {
	a.Label, b.Label = "", ""
	return a == b
}

// loaded is a database a load made, opened again.
type loaded struct {
	e    *doltengine.Engine
	load *LoadRecord
	sum  string
}

// openLoaded opens the database of an arm that a load made and checks that it is that load's:
// the stream is the same (if s names it), and the database is at the load's last sequence number, with the
// load's head commit and as many commits.
func openLoaded(ctx context.Context, c *common, loadPath string, arm doltmodel.Arm, s *stream) (*loaded, error) {
	rec, sum, err := readLoad(loadPath)
	if err != nil {
		return nil, err
	}
	if rec.Arm != arm.String() {
		return nil, fmt.Errorf("doltspike: the load record is of the %s arm, not %s", rec.Arm, arm)
	}
	if rec.Database != c.db {
		return nil, fmt.Errorf("doltspike: the load record is of database %s, not %s", rec.Database, c.db)
	}
	if s != nil && !sameInput(rec.Input, s.input()) {
		return nil, fmt.Errorf("doltspike: the load record is of another stream or question set: %+v, not %+v", rec.Input, s.input())
	}
	e, err := doltengine.Open(ctx, doltengine.Config{DSN: c.dsn, Database: c.db, Arm: arm})
	if err != nil {
		return nil, err
	}
	l := &loaded{e: e, load: rec, sum: sum}
	if err := l.check(); err != nil {
		_ = e.Close()
		return nil, err
	}
	return l, nil
}

// check holds the database to the load record.
func (l *loaded) check() error {
	if got := l.e.LastSeq(); got != l.load.Input.LastSeq {
		return fmt.Errorf("doltspike: database %s is at seq %d, the load ended at %d", l.load.Database, got, l.load.Input.LastSeq)
	}
	head, ok := l.e.Head()
	switch {
	case ok != (l.load.HeadHash != ""):
		return fmt.Errorf("doltspike: database %s and its load record disagree about having commits", l.load.Database)
	case ok && (head.Hash != l.load.HeadHash || head.Ordinal != l.load.Commits):
		return fmt.Errorf("doltspike: database %s ends at commit %d (%s), its load record at %d (%s)", l.load.Database, head.Ordinal, head.Hash, l.load.Commits, l.load.HeadHash)
	}
	return nil
}

// modelSink writes a stream to a model.
type modelSink struct{ m engine.Engine }

func (s modelSink) Write(b []engine.Record) error { return s.m.Write(slices.Clone(b)) }
func (s modelSink) Retain(h time.Time) error      { return s.m.Retain(h) }

func runRead(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("read", stderr)
	var (
		c        common
		in       inputFlags
		arm      string
		loadPath string
		mod      bool
	)
	c.register(fs, true)
	in.register(fs, true)
	fs.StringVar(&arm, "arm", "", "the arm: commit, rows, or commit-time (the diagnostic, read from the commit arm's database)")
	fs.StringVar(&loadPath, "load", "", "the load record (load-<arm>.json) of the database")
	fs.BoolVar(&mod, "model", false, "also ask the model of the design, fed the same stream, and require the same answers, the same counts and the same paths for every question (it holds the whole stream in memory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if loadPath == "" || fs.NArg() != 0 {
		return errors.New("usage: doltspike read -dsn dsn -image image -db name -arm commit|rows|commit-time -load load.json [-out dir] (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha [-activity file -expect-activity digest, with -model]) [-model]")
	}
	diag := arm == armCommitTime
	loadArm := arm
	if diag {
		loadArm = "commit"
	}
	a, err := parseArm(loadArm, true)
	if err != nil {
		return err
	}
	if err := c.validate(false, true); err != nil {
		return err
	}
	name := "read-" + arm + ".json"
	if c.out != "" {
		if err := checkFree(c.out, name); err != nil {
			return err
		}
	}
	// A question set needs the file it was made for only to feed the model.
	s, err := in.resolve(mod && in.plan == "")
	if err != nil {
		return err
	}
	env, err := serverEnv(ctx, c.dsn, c.image)
	if err != nil {
		return err
	}
	l, err := openLoaded(ctx, &c, loadPath, a, s)
	if err != nil {
		return err
	}
	defer func() { _ = l.e.Close() }()

	say := progress(stderr)
	var eng engine.Engine = l.e
	if diag {
		if l.load.FrontiersSHA256 != frontiersDigest(l.load.Frontiers) {
			return errors.New("doltspike: the load record's frontiers do not hash to its digest")
		}
		if eng, err = doltengine.NewCommitTime(l.e, l.load.Frontiers); err != nil {
			return err
		}
	}
	plan := s.questions()
	rep, err := g0.Check(ctx, plan, eng, say, s.info())
	if err != nil {
		return err
	}
	rec := &ReadRecord{
		Kind: KindRead, Format: RecordFormat, Env: env, Input: s.input(), Arm: arm, Database: c.db, LoadSHA256: l.sum,
		Diagnostic: diag, Queries: rep.Queries, Matched: rep.Matched, Mismatches: rep.Mismatches,
	}
	if head, ok := l.e.Head(); ok {
		rec.HeadHash = head.Hash
	}
	rec.Results = resultsOf(rep)
	rec.Groups, rec.Ages = sums(rec.Results)

	if mod {
		if err := checkAgainstModel(ctx, s, a, diag, rep, rec, say); err != nil {
			return err
		}
		rec.ModelChecked = true
	}
	printRead(stdout, rec)
	if !diag && !rep.OK() {
		for _, mm := range rep.Mismatches {
			fmt.Fprintf(stdout, "MISMATCH %s: got %+v, plan %+v %s\n", mm.Query, mm.Got, mm.Want, mm.AskError)
		}
		return fmt.Errorf("%d answers differ from the plan's: no count is kept", rep.Queries-rep.Matched)
	}
	if c.out != "" {
		if err := writeNew(c.out, name, rec); err != nil {
			return err
		}
	}
	return nil
}

// checkAgainstModel writes the stream to the model of the arm and asks it the same questions.
// The server's answers, its counts of keys and its paths must equal the model's for every
// question (the model's derived work is added to the record).
func checkAgainstModel(ctx context.Context, s *stream, arm doltmodel.Arm, diag bool, srv *g0.Report, rec *ReadRecord, say runner.Progress) error {
	capacity := doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)
	var m interface {
		engine.Engine
		g0.PathReporter
	}
	if diag {
		m = doltmodel.NewCommitTime(capacity)
	} else {
		m = doltmodel.New(doltmodel.WithArm(arm), capacity)
	}
	tell(say, "read: writing the stream to the model")
	if err := s.drive(ctx, modelSink{m}, m.LastSeq, say); err != nil {
		return err
	}
	mrep, err := g0.Check(ctx, s.questions(), m, say, s.info())
	if err != nil {
		return err
	}
	for i, r := range srv.Results {
		mr := mrep.Results[i]
		if r.Match != mr.Match || !maps.Equal(r.Paths, mr.Paths) {
			return fmt.Errorf("doltspike: question %d (%s #%d at %s): the server matched %v with paths %v, the model matched %v with paths %v",
				i+1, r.Group, r.Rank, r.Age, r.Match, r.Paths, mr.Match, mr.Paths)
		}
		rec.Results[i].Work = mr.Work
	}
	rec.Groups, rec.Ages = sums(rec.Results)
	return nil
}

// outcomes are the paths a read can be answered by, as the engines name them.
var outcomes = []string{"as-of-alone", "needed-log", "no-commit", "commit-time", "log"}

// resultsOf turns the report of a gate into the record's results: how each question was
// answered, from the counts of the paths the engine reported for it.
func resultsOf(rep *g0.Report) []QueryResult {
	out := make([]QueryResult, 0, len(rep.Results))
	for _, r := range rep.Results {
		qr := QueryResult{Group: r.Group, Rank: r.Rank, Age: r.Age, Op: string(r.Op), Match: r.Match, Work: r.Work}
		kind := string(r.Op)
		qr.KeysState, qr.KeysLog = r.Paths[kind+"/keys-state"], r.Paths[kind+"/keys-log"]
		for _, o := range outcomes {
			if r.Paths[kind+"/"+o] > 0 {
				qr.Outcome = o
			}
		}
		out = append(out, qr)
	}
	return out
}

// sums are the results summed by group and age, and by age alone (which leaves out windows:
// they read the log in every arm).
func sums(results []QueryResult) (groups, ages []GroupResult) {
	byGroup := map[[2]string]int{}
	byAge := map[string]int{}
	for _, qr := range results {
		gi, ok := byGroup[[2]string{qr.Group, qr.Age}]
		if !ok {
			gi = len(groups)
			byGroup[[2]string{qr.Group, qr.Age}] = gi
			groups = append(groups, GroupResult{Group: qr.Group, Age: qr.Age})
		}
		add(&groups[gi], qr)
		if qr.Op == string(runner.OpWindow) {
			continue
		}
		ai, ok := byAge[qr.Age]
		if !ok {
			ai = len(ages)
			byAge[qr.Age] = ai
			ages = append(ages, GroupResult{Age: qr.Age})
		}
		add(&ages[ai], qr)
	}
	return groups, ages
}

func add(g *GroupResult, q QueryResult) {
	g.Queries++
	if q.Match {
		g.Matched++
	} else {
		g.Wrong++
	}
	switch q.Outcome {
	case "as-of-alone":
		g.AsOfAlone++
	case "needed-log":
		g.NeededLog++
	case "no-commit":
		g.NoCommit++
	}
	g.KeysState += q.KeysState
	g.KeysLog += q.KeysLog
	if q.Op == string(runner.OpWindow) {
		g.WorkWindowRows += q.Work["window/rows-log"]
		g.WorkWindowPrefix += q.Work["window/rows-prefix"]
		return
	}
	g.WorkRowsState += q.Work[q.Op+"/rows-state"]
	g.WorkRowsLog += q.Work[q.Op+"/rows-log"]
}

func printRead(w io.Writer, r *ReadRecord) {
	label := "G0"
	if r.Diagnostic {
		label = "DIAGNOSTIC"
	}
	fmt.Fprintf(w, "input: %s, stream digest %s, queries digest %s\n", r.Input.Kind, r.Input.StreamDigest, r.Input.QueriesDigest)
	fmt.Fprintf(w, "arm %s, database %s, load record sha256 %s\n", r.Arm, r.Database, r.LoadSHA256)
	for _, g := range r.Groups {
		fmt.Fprintf(w, "%s %-40s %-10s %d of %d%s\n", label, g.Group, g.Age, g.Matched, g.Queries, groupDetail(g))
	}
	if r.Diagnostic {
		fmt.Fprintf(w, "DIAGNOSTIC total: %d of %d answers wrong (the commit-time reading; not a comparator)\n", r.Queries-r.Matched, r.Queries)
		fmt.Fprintln(w, "wrong answers by age (neighbors, batch and alive reads):")
		for _, a := range r.Ages {
			fmt.Fprintf(w, "  %-10s %d of %d wrong\n", a.Age, a.Wrong, a.Queries)
		}
		return
	}
	fmt.Fprintf(w, "G0 total: %d of %d answers equal the plan's\n", r.Matched, r.Queries)
	fmt.Fprintln(w, "settlement by age (neighbors, batch and alive reads: queries answered by AS OF alone, queries that needed the log, queries whose token names no commit, keys from a state row, keys from the log):")
	for _, a := range r.Ages {
		if a.AsOfAlone+a.NeededLog+a.NoCommit == 0 {
			continue
		}
		fmt.Fprintf(w, "  %-10s as-of-alone %d, needed-log %d, no-commit %d, keys-state %d, keys-log %d\n", a.Age, a.AsOfAlone, a.NeededLog, a.NoCommit, a.KeysState, a.KeysLog)
	}
	if r.ModelChecked {
		var rs, rl, wr, wp uint64
		for _, g := range r.Groups {
			rs, rl, wr, wp = rs+g.WorkRowsState, rl+g.WorkRowsLog, wr+g.WorkWindowRows, wp+g.WorkWindowPrefix
		}
		fmt.Fprintf(w, "rows of the key ranges the reads hold (derived by the model from the stored rows, not measured in Dolt): state %d, log %d, windows %d by the index on event time or %d by the primary key's prefix (the plan says which)\n", rs, rl, wr, wp)
		fmt.Fprintln(w, "model: the same stream gave the same answers, key counts and paths for every question")
	}
}

func groupDetail(g GroupResult) string {
	var parts []string
	if g.AsOfAlone+g.NeededLog+g.NoCommit > 0 {
		parts = append(parts, fmt.Sprintf("as-of-alone %d needed-log %d", g.AsOfAlone, g.NeededLog))
		if g.NoCommit > 0 {
			parts = append(parts, fmt.Sprintf("no-commit %d", g.NoCommit))
		}
		parts = append(parts, fmt.Sprintf("keys-state %d keys-log %d", g.KeysState, g.KeysLog))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}
