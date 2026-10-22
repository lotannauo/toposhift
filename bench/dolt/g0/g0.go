// Package g0 is the first gate of the Dolt baseline: an engine must give the reference
// engine's answer to every question of a plan before any counter of it is kept. A faster
// but wrong engine does not count, and neither does one whose answers were not compared.
//
// The plan is the file the spike's runner makes (runner.MakePlan): the stream, as a spec
// that regenerates it and a digest that proves it was regenerated, and every question with
// the digest and size of the reference engine's answer. This package regenerates the
// stream into an engine with runner.Drive, checks that the stream is the planned one, and
// asks every question with runner.Ask. It needs no reference code of its own and builds
// nothing: the plan's answers are the reference.
//
// An engine that can say which tables it has read (a [PathReporter]) has the split counted
// per group and age: how many questions each path answered, a function of the questions and
// the stream.
package g0

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// ErrStreamDiffers is wrapped by the error Run returns when the stream it regenerated
// is not the one the plan was made from, so no answer can be compared.
var ErrStreamDiffers = errors.New("g0: the stream is not the plan's")

// MaxMismatches is how many mismatches a report lists. The count of them is exact.
const MaxMismatches = 20

// PathReporter is implemented by an engine that counts the reads it has answered by kind
// and path, as "kind/path" keys; see doltmodel's Paths.
type PathReporter interface {
	Paths() map[string]uint64
}

// WorkReporter is implemented by a model that counts the rows the key ranges of its reads
// hold (see doltmodel's Work), derived from the rows it stores; a server does not.
type WorkReporter interface {
	Work() map[string]uint64
}

// Mismatch is a question the engine answered differently from the reference engine.
type Mismatch struct {
	Query    string
	Op       runner.Op
	Got      runner.Answer
	Want     runner.Answer
	AskError string `json:",omitempty"`
}

// Result is one question's outcome: whether its answer equalled the reference's and, for
// an engine that reports its paths, how many reads each path answered for it (a
// question is one read, a batched one counts once, so the counts of a question are
// the keys it settled each way and the one outcome it had).
type Result struct {
	Group string
	Rank  int
	Age   string
	Op    runner.Op
	Match bool
	Paths map[string]uint64 `json:",omitempty"`
	// Work is the rows of the key ranges the question's read holds, from an engine that
	// derives them.
	Work map[string]uint64 `json:",omitempty"`
}

// Group is the questions of one group at one age.
type Group struct {
	Group   string
	Age     string
	Queries int
	Matched int
	// Paths are the reads each path answered for these questions, if the engine reports
	// them.
	Paths map[string]uint64 `json:",omitempty"`
}

// Report is the result of a run.
type Report struct {
	// Stream is the stream as regenerated, which equals the plan's in the fields
	// that decide it.
	Stream runner.StreamInfo
	// Queries and Matched count the questions and the answers equal to the
	// reference engine's, in digest and size.
	Queries, Matched int
	Groups           []Group
	// Results is one entry per question, in the plan's order.
	Results []Result
	// Mismatches are the first MaxMismatches wrong answers.
	Mismatches []Mismatch
}

// OK reports whether every answer matched.
func (r *Report) OK() bool { return r.Queries > 0 && r.Matched == r.Queries }

func tell(say runner.Progress, format string, args ...any) {
	if say != nil {
		say(format, args...)
	}
}

// sink writes a stream to an engine.
type sink struct{ e engine.Engine }

func (s sink) Write(b []engine.Record) error { return s.e.Write(b) }
func (s sink) Retain(h time.Time) error      { return s.e.Retain(h) }

// Run regenerates the stream of the plan into e, which must be empty, and asks e every
// question of the plan. A question e cannot answer is a mismatch with its error. Run
// returns an error if the stream could not be written or is not the plan's.
func Run(ctx context.Context, plan *runner.Plan, e engine.Engine, say runner.Progress) (*Report, error) {
	tell(say, `g0: writing the stream of the plan (%d records, last seq %d)`, plan.Stream.Records, plan.Stream.LastSeq)
	info, err := runner.Drive(ctx, plan.Spec, sink{e})
	if err != nil {
		return nil, err
	}
	if err := CheckStream(info, plan.Stream, e); err != nil {
		return nil, err
	}
	return Check(ctx, plan, e, say, info)
}

// CheckStream says whether the stream a run regenerated is the plan's, and whether e has
// been written all of it: the fields that decide the stream are equal, and e is at the last
// sequence number.
func CheckStream(info, want runner.StreamInfo, e interface{ LastSeq() uint64 }) error {
	if info.Digest != want.Digest || info.Records != want.Records || info.LastSeq != want.LastSeq || info.OldToken != want.OldToken || info.Dropped != want.Dropped || len(info.Retentions) != len(want.Retentions) {
		return fmt.Errorf("%w: digest %s, %d records, last seq %d, old token %d; the plan's: %s, %d, %d, %d",
			ErrStreamDiffers, info.Digest, info.Records, info.LastSeq, info.OldToken, want.Digest, want.Records, want.LastSeq, want.OldToken)
	}
	if got := e.LastSeq(); got != want.LastSeq {
		return fmt.Errorf("%w: the engine is at seq %d, the stream ended at %d", ErrStreamDiffers, got, want.LastSeq)
	}
	return nil
}

// Check asks e every question of the plan and compares each answer with the reference
// engine's, writing nothing: e holds the plan's stream already (written by [Run], or
// loaded earlier and opened again). stream is what the stream is said to be, and is
// returned in the report as it is; Check does not look at it. A question e cannot
// answer is a mismatch with its error.
func Check(ctx context.Context, plan *runner.Plan, e engine.Engine, say runner.Progress, stream runner.StreamInfo) (*Report, error) {
	tell(say, "g0: asking %d questions", len(plan.Queries))
	reporter, _ := e.(PathReporter)
	worker, _ := e.(WorkReporter)
	rep := &Report{Stream: stream, Queries: len(plan.Queries)}
	groups := map[[2]string]*Group{}
	var order [][2]string
	for _, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		k := [2]string{q.Group, q.Age}
		g := groups[k]
		if g == nil {
			g = &Group{Group: q.Group, Age: q.Age}
			groups[k] = g
			order = append(order, k)
		}
		g.Queries++
		var before, workBefore map[string]uint64
		if reporter != nil {
			before = reporter.Paths()
		}
		if worker != nil {
			workBefore = worker.Work()
		}
		got, err := runner.Ask(e, q)
		var delta map[string]uint64
		if reporter != nil {
			delta = addPaths(nil, before, reporter.Paths())
			for k, v := range delta {
				if g.Paths == nil {
					g.Paths = make(map[string]uint64)
				}
				g.Paths[k] += v
			}
		}
		var work map[string]uint64
		if worker != nil {
			work = addPaths(nil, workBefore, worker.Work())
		}
		wantAns := runner.Answer{Digest: q.Expect, Size: q.Size}
		match := err == nil && got == wantAns
		rep.Results = append(rep.Results, Result{Group: q.Group, Rank: q.Rank, Age: q.Age, Op: q.Op, Match: match, Paths: delta, Work: work})
		if match {
			g.Matched++
			rep.Matched++
			continue
		}
		if len(rep.Mismatches) < MaxMismatches {
			m := Mismatch{Query: q.Name(), Op: q.Op, Got: got, Want: wantAns}
			if err != nil {
				m.AskError = err.Error()
			}
			rep.Mismatches = append(rep.Mismatches, m)
		}
	}
	for _, k := range order {
		rep.Groups = append(rep.Groups, *groups[k])
	}
	return rep, nil
}

// addPaths adds to total what the counters rose by between before and after.
func addPaths(total, before, after map[string]uint64) map[string]uint64 {
	for k, v := range after {
		if d := v - before[k]; d > 0 {
			if total == nil {
				total = make(map[string]uint64)
			}
			total[k] += d
		}
	}
	return total
}

// TotalPaths is the sum of the groups' path counts.
func (r *Report) TotalPaths() map[string]uint64 {
	total := make(map[string]uint64)
	for _, g := range r.Groups {
		for k, v := range g.Paths {
			total[k] += v
		}
	}
	return total
}
