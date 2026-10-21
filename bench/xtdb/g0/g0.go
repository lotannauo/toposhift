// Package g0 is the first gate of the XTDB baseline: an engine must give the
// reference engine's answer to every question of a plan before any counter of it is
// kept. A faster but wrong engine does not count.
//
// The plan is the file the spike's runner makes (runner.MakePlan): the stream, as a
// spec that regenerates it and a digest that proves it was regenerated, and every
// question with the digest and size of the reference engine's answer. This package
// regenerates the stream into an engine with runner.Drive, checks that the stream is
// the planned one, and asks every question with runner.Ask. It needs no reference code
// of its own and builds nothing: the plan's answers are the reference.
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

// Mismatch is a question the engine answered differently from the reference engine.
type Mismatch struct {
	Query    string
	Op       runner.Op
	Got      runner.Answer
	Want     runner.Answer
	AskError string `json:",omitempty"`
}

// Group is the questions of one group at one age.
type Group struct {
	Group   string
	Age     string
	Queries int
	Matched int
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

// Run regenerates the stream of the plan into e, which must be empty, and asks e
// every question of the plan. A question e cannot answer is a mismatch with its error.
// Run returns an error if the stream could not be written or is not the plan's.
func Run(ctx context.Context, plan *runner.Plan, e engine.Engine, say runner.Progress) (*Report, error) {
	tell(say, `g0: writing the stream of the plan (%d records, last seq %d)`, plan.Stream.Records, plan.Stream.LastSeq)
	info, err := runner.Drive(ctx, plan.Spec, sink{e})
	if err != nil {
		return nil, err
	}
	want := plan.Stream
	if info.Digest != want.Digest || info.Records != want.Records || info.LastSeq != want.LastSeq || info.OldToken != want.OldToken || info.Dropped != want.Dropped || len(info.Retentions) != len(want.Retentions) {
		return nil, fmt.Errorf("%w: digest %s, %d records, last seq %d, old token %d; the plan's: %s, %d, %d, %d",
			ErrStreamDiffers, info.Digest, info.Records, info.LastSeq, info.OldToken, want.Digest, want.Records, want.LastSeq, want.OldToken)
	}
	if got := e.LastSeq(); got != want.LastSeq {
		return nil, fmt.Errorf("%w: the engine is at seq %d, the stream ended at %d", ErrStreamDiffers, got, want.LastSeq)
	}

	tell(say, "g0: asking %d questions", len(plan.Queries))
	rep := &Report{Stream: info, Queries: len(plan.Queries)}
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
		got, err := runner.Ask(e, q)
		wantAns := runner.Answer{Digest: q.Expect, Size: q.Size}
		if err == nil && got == wantAns {
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
