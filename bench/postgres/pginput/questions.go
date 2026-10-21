package pginput

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

// QuestionsVersion is the version of the format of a question set, and of the way the
// questions are chosen.
const QuestionsVersion = 1

// QuestionBatch is the number of records a batch of an activity file's stream has when
// the old snapshot is taken: the unit a plan's stream is written in.
const QuestionBatch = 1000

// QuestionSet is what a plan is for a stream that is a file: the questions asked of
// it, with the answer the reference engine gave to each, and the stream they were made
// for. It is made once, before any database holds the stream, and its digest is what a
// load and its reads are held to.
type QuestionSet struct {
	Version int
	// Label says what the stream is, for whoever reads a figure from it.
	Label   string
	Options Options
	// Limit is how many records were read, or 0 for the whole file. A set made from a
	// prefix is for trying the pipeline, and is labelled so.
	Limit int64
	// ContentDigest is the content digest the file's footer gives, which is checked
	// against the rows only when the whole file was read.
	ContentDigest string
	Records       uint64
	LastSeq       uint64
	// Start and End are the first and the last event time of the stream: "now" is End.
	Start, End time.Time
	// OldToken and OldAt are the snapshot the old-token reads are made as of, and at.
	OldToken uint64
	OldAt    time.Time
	Hot      int
	Median   int
	Queries  []runner.Query
	// QueriesDigest is the digest of the questions without the answers.
	QueriesDigest string
}

// Info is the stream as a [runner.StreamInfo], for a report of the questions' answers.
func (q *QuestionSet) Info() runner.StreamInfo {
	return runner.StreamInfo{
		Records: q.Records, LastSeq: q.LastSeq, OldToken: q.OldToken, OldAt: q.OldAt, Start: q.Start, End: q.End,
		Digest: q.ContentDigest,
	}
}

// Save writes the set as JSON.
func (q *QuestionSet) Save(path string) error {
	b, err := json.MarshalIndent(q, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

// ErrQuestions is wrapped by the error [LoadQuestions] returns for a set that does not
// say what it was made from.
var ErrQuestions = errors.New("pginput: the question set is not what it says")

// LoadQuestions reads a set and checks that the questions hash to the digest recorded
// in it.
func LoadQuestions(path string) (*QuestionSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var q QuestionSet
	if err := json.Unmarshal(b, &q); err != nil {
		return nil, fmt.Errorf("pginput: %s: %w", path, err)
	}
	if q.Version != QuestionsVersion {
		return nil, fmt.Errorf("pginput: %s: version %d, this build reads %d: %w", path, q.Version, QuestionsVersion, ErrQuestions)
	}
	if d, err := runner.QueriesDigest(q.Queries); err != nil || d != q.QueriesDigest {
		return nil, fmt.Errorf("pginput: %s: the queries do not hash to %s: %w", path, q.QueriesDigest, ErrQuestions)
	}
	return &q, nil
}

// snapshot finds the old snapshot of a stream by the rule runner.Drive uses: the token
// is the one before the first batch whose last record reaches three quarters through the
// period, and the instant it is read at is the newest event time the earlier batches
// reached, brought before the instant any record after the token takes effect (its event
// time, or for an extension its Through), so that what took effect by then is in the
// snapshot.
type snapshot struct {
	oldAt    time.Time // three quarters through the period
	found    bool
	token    uint64
	at       time.Time
	frontier time.Time
	unseen   time.Time
	last     uint64
}

func (s *snapshot) batch(b []engine.Record) {
	if len(b) == 0 {
		return
	}
	if !s.found && !b[len(b)-1].EventTime.Before(s.oldAt) {
		s.found, s.token, s.at = true, s.last, s.oldAt
		if !s.frontier.IsZero() {
			s.at = s.frontier
		}
	}
	if s.found {
		for _, r := range b {
			at := r.EventTime
			if r.Through.After(at) {
				at = r.Through
			}
			if s.unseen.IsZero() || at.Before(s.unseen) {
				s.unseen = at
			}
		}
	}
	for _, r := range b {
		if r.EventTime.After(s.frontier) {
			s.frontier = r.EventTime
		}
	}
	s.last = b[len(b)-1].Seq
}

func (s *snapshot) finish() (token uint64, at time.Time) {
	if !s.found {
		return s.last, s.oldAt
	}
	at = s.at
	if !s.unseen.IsZero() && s.unseen.Add(-time.Nanosecond).Before(at) {
		at = s.unseen.Add(-time.Nanosecond)
	}
	return s.token, at
}

// BuildQuestions chooses the questions of an activity file and records the reference
// engine's answer to each. The file is read three times: for its extent; for the
// analyzer that chooses the busiest prefixes, and the old snapshot; and for the
// reference engine, which is given only the records that touch an entity the questions
// read. Questions that need more history than the stream has are not asked: of the ages
// of a plan only now, the old token and the one-hour window are left for a stream of an
// hour, and the read one hour back, which would be made at the first instant, is cut.
//
// limit, if positive, reads only about that many records, in whole batches.
func BuildQuestions(path string, o Options, limit int64, label string, say func(format string, args ...any)) (*QuestionSet, error) {
	tell := func(f string, a ...any) {
		if say != nil {
			say(f, a...)
		}
	}
	q := &QuestionSet{Version: QuestionsVersion, Label: label, Options: o, Limit: limit}

	tell("questions: pass 1, the extent of %s", path)
	var lo, hi time.Time
	info, err := ForEach(path, QuestionBatch, o, StopAfter(limit, func(rs []engine.Record) error {
		for _, r := range rs {
			if lo.IsZero() || r.EventTime.Before(lo) {
				lo = r.EventTime
			}
			if r.EventTime.After(hi) {
				hi = r.EventTime
			}
			q.Records++
			q.LastSeq = r.Seq
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	if q.Records == 0 {
		return nil, errors.New("pginput: the file has no record to ask about")
	}
	q.ContentDigest, q.Start, q.End = info.Digest, lo, hi

	tell("questions: pass 2, analyzing %d records", q.Records)
	period := hi.Sub(lo)
	an := workload.NewAnalyzer(lo, period)
	snap := &snapshot{oldAt: lo.Add(period / 4 * 3)}
	if _, err := ForEach(path, QuestionBatch, o, StopAfter(limit, func(rs []engine.Record) error {
		for _, r := range rs {
			an.Add(r)
		}
		snap.batch(rs)
		return nil
	})); err != nil {
		return nil, err
	}
	q.OldToken, q.OldAt = snap.finish()

	spec := runner.DefaultSpec(workload.Config{})
	q.Hot, q.Median = spec.Hot, spec.Median
	all := runner.BuildQueries(spec, an, runner.StreamInfo{Start: lo, End: hi, OldToken: q.OldToken, OldAt: q.OldAt})
	for _, qu := range all {
		if qu.Age != runner.Age1h {
			q.Queries = append(q.Queries, qu)
		}
	}
	if len(q.Queries) == 0 {
		return nil, errors.New("pginput: the stream has nothing to ask about")
	}

	set := map[identity.Fingerprint]struct{}{}
	for _, qu := range q.Queries {
		for _, fp := range qu.Fps {
			set[fp] = struct{}{}
		}
	}
	tell("questions: pass 3, %d questions over %d entities, feeding the reference engine", len(q.Queries), len(set))
	ora := oracle.New()
	if _, err := ForEach(path, QuestionBatch, o, StopAfter(limit, func(rs []engine.Record) error {
		var mine []engine.Record
		for _, r := range rs {
			if _, ok := set[r.Subject.A]; ok {
				mine = append(mine, r)
			} else if r.Subject.Kind == engine.SubjectEdge {
				if _, ok := set[r.Subject.B]; ok {
					mine = append(mine, r)
				}
			}
		}
		if len(mine) == 0 {
			return nil
		}
		return ora.Write(mine)
	})); err != nil {
		return nil, err
	}
	for i := range q.Queries {
		a, err := runner.Ask(ora, q.Queries[i])
		if err != nil {
			return nil, fmt.Errorf("pginput: the reference engine on %s: %w", q.Queries[i].Name(), err)
		}
		q.Queries[i].Expect, q.Queries[i].Size = a.Digest, a.Size
	}
	if q.QueriesDigest, err = runner.QueriesDigest(q.Queries); err != nil {
		return nil, err
	}
	return q, nil
}

// StopAfter wraps the function [ForEach] calls so that the read ends after about limit
// records, in whole batches; a limit of zero reads everything.
func StopAfter(limit int64, fn func([]engine.Record) error) func([]engine.Record) error {
	var n int64
	return func(rs []engine.Record) error {
		if err := fn(rs); err != nil {
			return err
		}
		if n += int64(len(rs)); limit > 0 && n >= limit {
			return ErrStop
		}
		return nil
	}
}
