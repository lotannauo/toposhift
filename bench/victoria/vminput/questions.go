package vminput

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/identity"
)

// QuestionsFormat is the version of the questions file.
const QuestionsFormat = 1

// Hot and Median are how many of the busiest prefixes of each class are queried, and how
// many around the middle: the spike's own numbers (runner.DefaultSpec).
const (
	Hot    = 10
	Median = 5
)

// Questions is the set of questions asked of a stream that has no plan: chosen by the
// spike's selector (workload.Analyzer and runner.BuildQueries) from the stream, and
// answered by the reference engine fed the same stream. It carries the description of the
// file it was made from, so that a read refuses a file that is another.
type Questions struct {
	Format int
	// Digest, Records and LastSeq describe the activity file: its content digest from the
	// footer, the records it holds and the last sequence number.
	Digest  string
	Records uint64
	LastSeq uint64
	// Start and End are the earliest and the latest event time of the stream: the period
	// "now" and the ages are counted from. OldToken is the sequence number a read of the old
	// snapshot is pinned to and OldAt the instant it is read at; the metrics store cannot
	// read an old snapshot, so these questions are refused as a row of the capability table.
	Start, End, OldAt time.Time
	OldToken          uint64
	// Queries are the questions with the reference's answers, and QueriesDigest the
	// digest of the questions alone.
	Queries       []runner.Query
	QueriesDigest string
	Groups        []runner.GroupInfo
}

// Entities returns the entities the questions read, in the order they first appear.
func (q *Questions) Entities() []identity.Fingerprint {
	seen := map[identity.Fingerprint]bool{}
	var out []identity.Fingerprint
	for _, qu := range q.Queries {
		for _, fp := range qu.Fps {
			if !seen[fp] {
				seen[fp] = true
				out = append(out, fp)
			}
		}
	}
	return out
}

// Save writes the questions as JSON.
func (q *Questions) Save(path string) error {
	b, err := json.MarshalIndent(q, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// LoadQuestions reads a questions file and checks that the questions still hash to the
// digest recorded in it.
func LoadQuestions(path string) (*Questions, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var q Questions
	if err := json.Unmarshal(b, &q); err != nil {
		return nil, fmt.Errorf("vminput: %s: %w", path, err)
	}
	if q.Format != QuestionsFormat {
		return nil, fmt.Errorf("vminput: %s is format %d, not %d", path, q.Format, QuestionsFormat)
	}
	if d, err := runner.QueriesDigest(q.Queries); err != nil || d != q.QueriesDigest {
		return nil, fmt.Errorf("vminput: %s: the questions do not hash to %s", path, q.QueriesDigest)
	}
	return &q, nil
}

// shape is what the first pass learns of the stream.
type shape struct {
	records, lastSeq uint64
	start, end       time.Time
	oldAt            time.Time
	oldToken         uint64
}

// shapeSink finds the period of a stream and its old snapshot the way runner.Drive does:
// the snapshot is taken three quarters through the period, at the newest event time the
// records before it had reached, and before the instant any record after it takes effect.
type shapeSink struct {
	s shape
	// events are every record's, kept compactly so the token can be found once the period
	// is known.
	events []event
}

// event is a record's sequence number, event time and the instant it takes effect, in
// nanoseconds: 24 bytes a record.
type event struct {
	seq     uint64
	at, eff int64
}

func (s *shapeSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		eff := r.EventTime
		if r.Through.After(eff) {
			eff = r.Through
		}
		s.events = append(s.events, event{r.Seq, r.EventTime.UnixNano(), eff.UnixNano()})
		if s.s.records == 0 || r.EventTime.Before(s.s.start) {
			s.s.start = r.EventTime
		}
		if s.s.records == 0 || r.EventTime.After(s.s.end) {
			s.s.end = r.EventTime
		}
		s.s.records++
		s.s.lastSeq = r.Seq
	}
	return nil
}
func (*shapeSink) Retain(time.Time) error { return nil }

func (s *shapeSink) finish() shape {
	out := s.s
	target := out.start.Add(out.end.Sub(out.start) / 4 * 3).UnixNano()
	var frontier, oldAt, unseen int64
	var token uint64
	var found, haveFrontier, haveUnseen bool
	for _, e := range s.events {
		if !found && e.at >= target {
			found, out.oldToken, oldAt = true, token, target
			if haveFrontier {
				oldAt = frontier
			}
		}
		if found && (!haveUnseen || e.eff < unseen) {
			unseen, haveUnseen = e.eff, true
		}
		if !haveFrontier || e.at > frontier {
			frontier, haveFrontier = e.at, true
		}
		token = e.seq
	}
	if !found {
		out.oldToken, oldAt = out.lastSeq, target
	}
	if haveUnseen && unseen-1 < oldAt {
		oldAt = unseen - 1
	}
	out.oldAt = time.Unix(0, oldAt).UTC()
	return out
}

type analyzerSink struct{ a *workload.Analyzer }

func (s analyzerSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		s.a.Add(r)
	}
	return nil
}
func (analyzerSink) Retain(time.Time) error { return nil }

type shadowSink struct {
	o   *oracle.Oracle
	set map[identity.Fingerprint]struct{}
}

func (s shadowSink) Write(batch []engine.Record) error {
	var mine []engine.Record
	for _, r := range batch {
		if vmmodel.Touches(s.set, r) {
			mine = append(mine, r)
		}
	}
	if len(mine) == 0 {
		return nil
	}
	return s.o.Write(mine)
}
func (shadowSink) Retain(time.Time) error { return nil }

// BuildQuestions chooses the questions of an activity file with the spike's selector and
// records the reference engine's answer to each. It reads the file three times: to learn
// its period and old snapshot, to analyse it, and to feed the reference engine the records
// of the entities asked about. The file must be the same every time (its content digest is
// checked).
func BuildQuestions(ctx context.Context, path string, say runner.Progress) (*Questions, error) {
	sh := &shapeSink{}
	st1, err := Drive(path, sh)
	if err != nil {
		return nil, err
	}
	if st1.Records == 0 {
		return nil, fmt.Errorf("vminput: %s has no records", path)
	}
	period := sh.finish()
	sh.events = nil
	if say != nil {
		say("questions: %d records, last seq %d", period.records, period.lastSeq)
	}

	// Nothing but the end of the period, the selector's numbers and whether a heartbeat
	// exists (it does: the stream is sampled, so the read past its end is made) decide the
	// questions.
	spec := runner.Spec{Hot: Hot, Median: Median}
	spec.Workload.HeartbeatInterval = time.Minute
	an := workload.NewAnalyzer(period.start, period.end.Sub(period.start))
	st2, err := Drive(path, analyzerSink{an})
	if err != nil {
		return nil, err
	}
	if st2.Digest != st1.Digest {
		return nil, fmt.Errorf("vminput: %s changed between two reads (digest %s, then %s)", path, st1.Digest, st2.Digest)
	}
	info := runner.StreamInfo{Start: period.start, End: period.end, OldAt: period.oldAt, OldToken: period.oldToken, LastSeq: period.lastSeq, Records: period.records}
	queries := runner.BuildQueries(spec, an, info)
	if len(queries) == 0 {
		return nil, fmt.Errorf("vminput: the stream has nothing to ask about")
	}

	set := map[identity.Fingerprint]struct{}{}
	for _, q := range queries {
		for _, fp := range q.Fps {
			set[fp] = struct{}{}
		}
	}
	if say != nil {
		say("questions: %d questions over %d entities, feeding the reference engine", len(queries), len(set))
	}
	ora := oracle.New()
	st3, err := Drive(path, shadowSink{o: ora, set: set})
	if err != nil {
		return nil, err
	}
	if st3.Digest != st1.Digest {
		return nil, fmt.Errorf("vminput: %s changed between two reads (digest %s, then %s)", path, st1.Digest, st3.Digest)
	}

	groups := map[string]*runner.GroupInfo{}
	for i := range queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := &queries[i]
		a, err := runner.Ask(ora, *q)
		if err != nil {
			return nil, fmt.Errorf("vminput: the reference engine on %s: %w", q.Name(), err)
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
	out := &Questions{
		Format: QuestionsFormat, Digest: st1.Digest, Records: period.records, LastSeq: period.lastSeq,
		Start: period.start, End: period.end, OldAt: period.oldAt, OldToken: period.oldToken, Queries: queries,
	}
	for _, g := range groups {
		out.Groups = append(out.Groups, *g)
	}
	slices.SortFunc(out.Groups, func(a, b runner.GroupInfo) int {
		return strings.Compare(a.Group+"\x00"+a.Age, b.Group+"\x00"+b.Age)
	})
	for _, g := range out.Groups {
		if g.Age == runner.AgeDead && g.NonEmpty > 0 {
			return nil, fmt.Errorf("vminput: %d of %d reads of %s %s past the end find an entity alive: the instant is not past every refresh's TTL", g.NonEmpty, g.Queries, g.Group, runner.DeadAfter)
		}
	}
	if out.QueriesDigest, err = runner.QueriesDigest(out.Queries); err != nil {
		return nil, err
	}
	return out, nil
}
