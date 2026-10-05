package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Plan is the one file every step after it takes: what the stream is, what is
// asked of each candidate, and what the reference engine answered. It holds no
// time or machine, so the same spec makes the same plan byte for byte.
type Plan struct {
	Spec        Spec
	SpecDigest  string
	RulesDigest string
	Stream      StreamInfo
	Queries     []Query
	// QueriesDigest covers the questions without the answers; [Plan.Digest] covers
	// the answers too.
	QueriesDigest string
	Groups        []GroupInfo
	// CacheBytes is the block cache every candidate gets.
	CacheBytes int64
	// Shadow is how many entities the reference engine was fed the records of.
	Shadow int
}

// GroupInfo is how many queries a group has at one age and how many have an
// answer that is not empty.
type GroupInfo struct {
	Group    string
	Age      string
	Queries  int
	NonEmpty int
}

// Progress receives a line about what is happening, or nil to say nothing.
type Progress func(format string, args ...any)

func (p Progress) say(format string, args ...any) {
	if p != nil {
		p(format, args...)
	}
}

// analyzerSink feeds the stream to an analyzer.
type analyzerSink struct{ a *workload.Analyzer }

func (s analyzerSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		s.a.Add(r)
	}
	return nil
}
func (analyzerSink) Retain(time.Time) error { return nil }

// shadowSink feeds the reference engine the records that touch a set of
// entities, which is all it needs to answer reads about them exactly. The
// sequence numbers are the stream's own, so a read pinned to a token sees what a
// full engine would.
type shadowSink struct {
	o   *oracle.Oracle
	set map[identity.Fingerprint]struct{}
}

func (s shadowSink) touches(r engine.Record) bool { return touches(s.set, r) }

func (s shadowSink) Write(batch []engine.Record) error {
	var mine []engine.Record
	for _, r := range batch {
		if s.touches(r) {
			mine = append(mine, r)
		}
	}
	if len(mine) == 0 {
		return nil
	}
	return s.o.Write(mine)
}

func (s shadowSink) Retain(h time.Time) error { return s.o.Retain(h) }

// analyze runs the stream through an analyzer and chooses the queries from what
// it found, or, for a spec with pins, learns the shape of the projected stream and
// asks about the pins. The analyzer holds every prefix of the stream, so it is kept to this
// function and freed before the second pass.
func analyze(ctx context.Context, spec Spec) (StreamInfo, []Query, error) {
	if spec.Pins != nil { // the prefixes are chosen: there is nothing to analyze, only the stream's shape to learn
		info, err := Drive(ctx, spec)
		if err != nil {
			return info, nil, err
		}
		return info, BuildQueries(spec, spec.Pins, info), nil
	}
	an := workload.NewAnalyzer(spec.Workload.Start, spec.Workload.Duration)
	if n := len(spec.Retentions); n > 0 {
		an.SetRetainedFrom(spec.Retentions[n-1].Horizon(spec.Workload.Start))
	}
	info, err := Drive(ctx, spec, analyzerSink{an})
	if err != nil {
		return info, nil, err
	}
	return info, BuildQueries(spec, an, info), nil
}

// MakePlan analyzes the stream, chooses the queries and records the reference
// engine's answer to each. The stream is generated twice and must come out the
// same both times.
func MakePlan(ctx context.Context, spec Spec, say Progress) (*Plan, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	specDigest, err := spec.Digest()
	if err != nil {
		return nil, err
	}
	rulesDigest, err := DefaultRules().Digest()
	if err != nil {
		return nil, err
	}

	say.say("plan: pass 1, analyzing the stream")
	info, queries, err := analyze(ctx, spec)
	if err != nil {
		return nil, err
	}
	say.say("plan: %d records written, %d dropped before the horizon", info.Records, info.Dropped)
	if len(queries) == 0 {
		return nil, fmt.Errorf("runner: the stream has nothing to ask about")
	}

	set := map[identity.Fingerprint]struct{}{}
	for _, fp := range fingerprintsOf(queries) {
		set[fp] = struct{}{}
	}
	say.say("plan: pass 2, %d queries over %d entities, feeding the reference engine", len(queries), len(set))
	ora := oracle.New()
	again, err := Drive(ctx, spec, shadowSink{o: ora, set: set})
	if err != nil {
		return nil, err
	}
	if !sameStream(info, again) {
		return nil, fmt.Errorf("runner: the stream is not the same twice (digest %s, then %s): %w", info.Digest, again.Digest, errMismatch)
	}

	plan := &Plan{
		Spec: spec, SpecDigest: specDigest, RulesDigest: rulesDigest, Stream: info,
		Queries: queries, Shadow: len(set),
		CacheBytes: spec.cache(info.PayloadBytes),
	}
	groups := map[string]*GroupInfo{}
	for i := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := &plan.Queries[i]
		a, err := Ask(ora, *q)
		if err != nil {
			return nil, fmt.Errorf("runner: the reference engine on %s: %w", q.Name(), err)
		}
		q.Expect, q.Size = a.Digest, a.Size
		key := q.Group + " " + q.Age
		g := groups[key]
		if g == nil {
			g = &GroupInfo{Group: q.Group, Age: q.Age}
			groups[key] = g
		}
		g.Queries++
		if a.Size > 0 {
			g.NonEmpty++
		}
	}
	for _, g := range groups {
		plan.Groups = append(plan.Groups, *g)
	}
	sort.Slice(plan.Groups, func(i, j int) bool {
		a, b := plan.Groups[i], plan.Groups[j]
		return a.Group < b.Group || a.Group == b.Group && a.Age < b.Age
	})
	var thin []string
	for _, g := range plan.Groups {
		if float64(g.NonEmpty) < spec.MinNonEmpty*float64(g.Queries) {
			thin = append(thin, fmt.Sprintf("%s at %s (%d of %d)", g.Group, g.Age, g.NonEmpty, g.Queries))
		}
	}
	if len(thin) > 0 {
		return nil, fmt.Errorf("runner: groups of queries, at one age, with mostly empty answers, which candidates would agree on trivially: %s", strings.Join(thin, "; "))
	}
	if plan.QueriesDigest, err = QueriesDigest(plan.Queries); err != nil {
		return nil, err
	}
	return plan, nil
}

// cacheBytes is the block cache every candidate gets: the share of the payload
// bytes of the stream, in whole megabytes and at least one.
func cacheBytes(payload uint64, fraction float64) int64 {
	return max(int64(float64(payload)*fraction)>>20<<20, 1<<20)
}

func sameStream(a, b StreamInfo) bool {
	return a.Digest == b.Digest && a.Records == b.Records && a.LastSeq == b.LastSeq && a.OldToken == b.OldToken
}

// Digest is the SHA-256, as hex, of what a build and a read must agree on with
// the plan: the spec, the stream, the questions with their answers and the cache
// size. The rules are not in it: they are applied to counters that do not depend
// on them, so setting a threshold does not make a plan, or what was built from
// it, stale. The report shows the digest of the rules a plan was made under.
func (p *Plan) Digest() (string, error) {
	b, err := json.Marshal(struct {
		Spec, Stream string
		Queries      []Query
		CacheBytes   int64
	}{p.SpecDigest, p.Stream.Digest, p.Queries, p.CacheBytes})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Save writes the plan as JSON.
func (p *Plan) Save(path string) error {
	b, err := json.MarshalIndent(p, "", " ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(b, '\n'))
}

// LoadPlan reads a plan and checks that it still says what it was made from:
// the spec and the questions must hash to the digests recorded in it.
func LoadPlan(path string) (*Plan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Plan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", path, err)
	}
	if d, err := p.Spec.Digest(); err != nil || d != p.SpecDigest {
		return nil, fmt.Errorf("runner: %s: the spec does not hash to %s: %w", path, p.SpecDigest, errMismatch)
	}
	if d, err := QueriesDigest(p.Queries); err != nil || d != p.QueriesDigest {
		return nil, fmt.Errorf("runner: %s: the queries do not hash to %s: %w", path, p.QueriesDigest, errMismatch)
	}
	return &p, nil
}
