package filestream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

// QuestionsFormat is the version of [QuestionSet].
const QuestionsFormat = 1

// QuestionSet is what a plan is for a stream with no generator: the stream, the
// questions chosen from it, and the reference engine's answer to each. It holds no time
// of the run and no path, so the same file and options make the same question set byte
// for byte, and the SHA-256 of the file that holds it names the questions.
type QuestionSet struct {
	Format int
	// Input names the file the stream is replayed from.
	Input Input
	// Period, BatchSize, Hot, Median and MinNonEmpty are the options the set was made
	// with.
	Period      Period
	BatchSize   int
	Hot, Median int
	MinNonEmpty float64
	// Stream is the stream as replayed: [Replay]'s description, whose digest is the
	// runner's for these records.
	Stream runner.StreamInfo
	// Queries are the questions with the reference engine's answers, and QueriesDigest
	// the digest of the questions without them, as a plan's.
	Queries       []runner.Query
	QueriesDigest string
	Groups        []runner.GroupInfo
	// Shadow is how many entities the reference engine was fed the records of.
	Shadow int
}

// Input is the file a question set was made from.
type Input struct {
	// Digest is the file's content digest, the footer's; Records its records.
	Digest  string
	Records uint64
}

// ReplayOptions are the options that replay the stream of the set from its file.
func (q *QuestionSet) ReplayOptions() Options {
	return Options{Period: q.Period, BatchSize: q.BatchSize, Digest: q.Input.Digest}
}

// DefaultMinNonEmpty is the share of non-empty answers the runner's own plans require of
// each group.
const DefaultMinNonEmpty = 0.25

// MakeOptions say how questions are chosen.
type MakeOptions struct {
	Options
	// Hot is how many of the busiest prefixes of each class are asked about, by records
	// and by run extensions, and Median how many around the middle; zero is the
	// runner's default (10 and 5).
	Hot, Median int
	// MinNonEmpty is the least share of each group of questions, at one age, that must
	// have an answer that is not empty, as the runner requires of a plan's (its default
	// is [DefaultMinNonEmpty]); zero asks for nothing.
	MinNonEmpty float64
}

type analyzerSink struct{ a *workload.Analyzer }

func (s analyzerSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		s.a.Add(r)
	}
	return nil
}
func (analyzerSink) Retain(time.Time) error { return nil }

// shadowSink feeds the reference engine the records that touch a set of entities, which
// is all it needs to answer reads about them exactly: a record of an entity, or of an
// edge with the entity at either end. The sequence numbers are the stream's own.
type shadowSink struct {
	o   *oracle.Oracle
	set map[identity.Fingerprint]struct{}
}

func (s shadowSink) Write(batch []engine.Record) error {
	var mine []engine.Record
	for _, r := range batch {
		if touches(s.set, r) {
			mine = append(mine, r)
		}
	}
	if len(mine) == 0 {
		return nil
	}
	return s.o.Write(mine)
}
func (s shadowSink) Retain(time.Time) error { return nil }

func touches(set map[identity.Fingerprint]struct{}, r engine.Record) bool {
	if _, ok := set[r.Subject.A]; ok {
		return true
	}
	if r.Subject.Kind == engine.SubjectEdge && fault != mutShadowSourceOnly {
		_, ok := set[r.Subject.B]
		return ok
	}
	return false
}

// Make replays the file twice: the first time an analyzer finds the prefixes the
// questions ask about, and the questions are chosen from them with the runner's rules
// ([runner.BuildQueries], whose ages that do not fit the period are left out); the second
// the reference engine is fed the records that touch them and answers each. The stream
// must be the same both times. It refuses a set with a group of questions that is mostly
// empty, which engines would agree on without being tested, and a read past the end of
// the stream that finds an entity alive.
func Make(ctx context.Context, path string, opts MakeOptions, say runner.Progress) (*QuestionSet, error) {
	spec := runner.DefaultSpec(workload.Config{Start: opts.Period.Start, Duration: opts.Period.Duration})
	// BuildQueries reads from the spec only how many prefixes to ask about and whether a
	// heartbeat makes entities lapse, which the reads past the end of the stream need.
	// Entities of a file lapse by the TTLs of its records, and the heartbeat's value is
	// not used.
	spec.Workload.HeartbeatInterval = time.Minute
	spec.BatchSize = opts.batch()
	if opts.Hot > 0 {
		spec.Hot = opts.Hot
	}
	if opts.Median > 0 {
		spec.Median = opts.Median
	}
	spec.MinNonEmpty = opts.MinNonEmpty
	if spec.MinNonEmpty < 0 || spec.MinNonEmpty > 1 {
		return nil, fmt.Errorf("filestream: the least share of non-empty answers, %v, is not between 0 and 1", spec.MinNonEmpty)
	}

	tell(say, "questions: pass 1, analyzing the stream")
	an := workload.NewAnalyzer(opts.Period.Start, opts.Period.Duration)
	info, err := Replay(ctx, path, opts.Options, analyzerSink{an})
	if err != nil {
		return nil, err
	}
	queries := runner.BuildQueries(spec, an, info)
	if len(queries) == 0 {
		return nil, errors.New("filestream: the stream has nothing to ask about")
	}
	set := map[identity.Fingerprint]struct{}{}
	for _, q := range queries {
		for _, fp := range q.Fps {
			set[fp] = struct{}{}
		}
	}
	tell(say, "questions: pass 2, %d questions over %d entities, feeding the reference engine", len(queries), len(set))
	ora := oracle.New()
	again, err := Replay(ctx, path, opts.Options, shadowSink{o: ora, set: set})
	if err != nil {
		return nil, err
	}
	if !sameStream(info, again) {
		return nil, fmt.Errorf("filestream: the stream is not the same twice (digest %s, then %s)", info.Digest, again.Digest)
	}

	qs := &QuestionSet{
		Format: QuestionsFormat, Input: Input{Digest: opts.Digest, Records: info.Records},
		Period: opts.Period, BatchSize: spec.BatchSize, Hot: spec.Hot, Median: spec.Median, MinNonEmpty: spec.MinNonEmpty,
		Stream: info, Queries: queries, Shadow: len(set),
	}
	groups := map[string]*runner.GroupInfo{}
	for i := range qs.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := &qs.Queries[i]
		a, err := runner.Ask(ora, *q)
		if err != nil {
			return nil, fmt.Errorf("filestream: the reference engine on %s: %w", q.Name(), err)
		}
		q.Expect, q.Size = a.Digest, a.Size
		key := q.Group + " " + q.Age
		g := groups[key]
		if g == nil {
			g = &runner.GroupInfo{Group: q.Group, Age: q.Age}
			groups[key] = g
		}
		g.Queries++
		if a.Size > 0 {
			g.NonEmpty++
		}
	}
	for _, g := range groups {
		qs.Groups = append(qs.Groups, *g)
	}
	sort.Slice(qs.Groups, func(i, j int) bool {
		a, b := qs.Groups[i], qs.Groups[j]
		return a.Group < b.Group || a.Group == b.Group && a.Age < b.Age
	})
	var thin []string
	for _, g := range qs.Groups {
		if g.Age == runner.AgeDead {
			if g.NonEmpty > 0 {
				return nil, fmt.Errorf("filestream: %d of %d reads of %s %s past the end find an entity alive: the instant is not past every refresh's TTL", g.NonEmpty, g.Queries, g.Group, runner.DeadAfter)
			}
			continue
		}
		if float64(g.NonEmpty) < spec.MinNonEmpty*float64(g.Queries) {
			thin = append(thin, fmt.Sprintf("%s at %s (%d of %d)", g.Group, g.Age, g.NonEmpty, g.Queries))
		}
	}
	if len(thin) > 0 {
		return nil, fmt.Errorf("filestream: groups of questions, at one age, with mostly empty answers, which engines would agree on trivially: %s", strings.Join(thin, "; "))
	}
	if qs.QueriesDigest, err = runner.QueriesDigest(qs.Queries); err != nil {
		return nil, err
	}
	return qs, nil
}

func tell(say runner.Progress, format string, args ...any) {
	if say != nil {
		say(format, args...)
	}
}

func sameStream(a, b runner.StreamInfo) bool {
	return a.Digest == b.Digest && a.Records == b.Records && a.LastSeq == b.LastSeq && a.OldToken == b.OldToken && a.OldAt.Equal(b.OldAt)
}

// Marshal is the bytes of the question set as Save writes them.
func (q *QuestionSet) Marshal() ([]byte, error) {
	b, err := json.MarshalIndent(q, "", " ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Save writes the question set to a file, atomically, and returns the SHA-256 of what it
// wrote, as hex: the name of the set.
func (q *QuestionSet) Save(path string) (string, error) {
	b, err := q.Marshal()
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".questions-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Load reads a question set and checks that it says what it was made from: the format,
// the questions' digest and, for the stream and the input, that they agree with each
// other.
func Load(path string) (*QuestionSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var q QuestionSet
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&q); err != nil {
		return nil, fmt.Errorf("filestream: %s: %w", path, err)
	}
	if q.Format != QuestionsFormat {
		return nil, fmt.Errorf("filestream: %s is format %d, not %d", path, q.Format, QuestionsFormat)
	}
	if d, err := runner.QueriesDigest(q.Queries); err != nil || d != q.QueriesDigest {
		return nil, fmt.Errorf("filestream: %s: the questions do not hash to %s", path, q.QueriesDigest)
	}
	if q.Stream.Records != q.Input.Records || !validDigest(q.Input.Digest) {
		return nil, fmt.Errorf("filestream: %s: the stream has %d records, the input %d", path, q.Stream.Records, q.Input.Records)
	}
	if err := q.Period.Validate(); err != nil {
		return nil, err
	}
	return &q, nil
}
