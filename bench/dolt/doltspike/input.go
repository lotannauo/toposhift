package doltspike

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// inputFlags are the flags that name the stream and the questions: a plan, which
// regenerates its stream, or an activity file with the question set made for it.
type inputFlags struct {
	plan, expectPlan           string
	activity, expectActivity   string
	questions, expectQuestions string
	dropBasis, dropBoot        bool
	label                      string
}

func (f *inputFlags) register(fs *flag.FlagSet, withActivity bool) {
	fs.StringVar(&f.plan, "plan", "", "the plan.json: the stream and its questions")
	fs.StringVar(&f.expectPlan, "expect-plan", "", "the SHA-256 of the plan file (required with -plan)")
	fs.StringVar(&f.questions, "questions", "", "the question set made for an activity file (questions.json)")
	fs.StringVar(&f.expectQuestions, "expect-questions", "", "the SHA-256 of the question set (required with -questions)")
	fs.StringVar(&f.label, "label", "", "what the stream is, recorded with every count from it")
	if withActivity {
		fs.StringVar(&f.activity, "activity", "", "the activity file (Parquet) the questions were made for")
		fs.StringVar(&f.expectActivity, "expect-activity", "", "the content digest of the activity file, or a prefix of at least 12 hex digits (required with -activity)")
		fs.BoolVar(&f.dropBasis, "drop-basis", false, "accept records that have an event time basis, and drop it")
		fs.BoolVar(&f.dropBoot, "drop-boot", false, "accept records that have a boot id, and drop it")
	}
}

// stream is a stream and its questions, from a plan or from an activity file.
type stream struct {
	plan    *runner.Plan
	planSHA string
	set     *doltinput.QuestionSet
	setSHA  string

	activity, expect string
	opts             doltinput.Options
	label            string
}

// resolve checks the files named by the flags against their digests, and reads them. With
// needStream, an activity file must be named as well as the questions made for it.
func (f *inputFlags) resolve(needStream bool) (*stream, error) {
	s := &stream{label: f.label, opts: doltinput.Options{DropBasis: f.dropBasis, DropBoot: f.dropBoot}, activity: f.activity, expect: f.expectActivity}
	switch {
	case f.plan != "" && (f.questions != "" || f.activity != ""):
		return nil, errors.New("doltspike: a plan and an activity file are two inputs: name one")
	case f.plan != "":
		sum, err := CheckDigest(f.plan, f.expectPlan, "plan")
		if err != nil {
			return nil, err
		}
		p, err := runner.LoadPlan(f.plan)
		if err != nil {
			return nil, err
		}
		s.plan, s.planSHA = p, sum
	case f.questions != "":
		sum, err := CheckDigest(f.questions, f.expectQuestions, "questions")
		if err != nil {
			return nil, err
		}
		q, err := doltinput.LoadQuestions(f.questions)
		if err != nil {
			return nil, err
		}
		s.set, s.setSHA = q, sum
		if needStream {
			if q.Options != s.opts {
				return nil, fmt.Errorf("doltspike: the question set was made with %+v, not %+v", q.Options, s.opts)
			}
			if f.activity == "" {
				return nil, errors.New("doltspike: -questions needs the -activity file it was made for")
			}
			if f.expectActivity == "" {
				return nil, errors.New("doltspike: -activity must be named by its content digest (-expect-activity)")
			}
		}
	default:
		return nil, errors.New("doltspike: name the input: -plan with -expect-plan, or -questions with -expect-questions")
	}
	return s, nil
}

// questions is the plan a gate takes: the plan itself, or the question set as one.
func (s *stream) questions() *runner.Plan {
	if s.plan != nil {
		return s.plan
	}
	return s.set.Plan()
}

// info is the stream as the plan or the set describes it.
func (s *stream) info() runner.StreamInfo { return s.questions().Stream }

// input is the record of what the stream is.
func (s *stream) input() Input {
	p := s.questions()
	in := Input{QueriesDigest: p.QueriesDigest, Records: p.Stream.Records, LastSeq: p.Stream.LastSeq, Label: s.label}
	if s.plan != nil {
		in.Kind, in.PlanSHA256, in.StreamDigest = "plan", s.planSHA, s.plan.Stream.Digest
		return in
	}
	in.Kind, in.QuestionsSHA256, in.ContentDigest, in.StreamDigest = "activity", s.setSHA, s.set.ContentDigest, s.set.ContentDigest
	in.MaxPayload, in.DroppedBasis, in.DroppedBoot = s.set.MaxPayload, s.set.Dropped.Basis, s.set.Dropped.Boot
	if s.set.Limit > 0 && in.Label == "" {
		in.Label = fmt.Sprintf("a prefix of the file, about %d records", s.set.Limit)
	}
	return in
}

// maxPayload is the longest payload the stream is said to carry: a plan's workload, or the
// question set's count over the file.
func (s *stream) maxPayload() int {
	if s.plan != nil {
		return doltmodel.WorkloadPayload(s.plan.Spec.Workload)
	}
	return s.set.MaxPayload
}

// sink is what a stream is written to: an engine, or a tee of an engine and its model.
type sink interface {
	Write([]engine.Record) error
	Retain(time.Time) error
}

// drive writes the stream to w and checks that it is the one that was named: a plan's stream
// as regenerated equals the plan's (digest, records, last sequence number, retentions), and
// an activity file read to its end has the content digest that was named and the records
// and last sequence number the question set says. last is the last sequence number the
// engine reports once the stream is written.
func (s *stream) drive(ctx context.Context, w sink, last func() uint64, say runner.Progress) error {
	if s.plan != nil {
		info, err := runner.Drive(ctx, s.plan.Spec, w)
		if err != nil {
			return err
		}
		return g0.CheckStream(info, s.plan.Stream, lastSeq(last))
	}
	var n, lastWritten uint64
	info, dropped, err := doltinput.ForEachCount(s.activity, doltinput.QuestionBatch, s.opts, doltinput.StopAfter(s.set.Limit, func(rs []engine.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.Write(rs); err != nil {
			return err
		}
		n += uint64(len(rs))
		lastWritten = rs[len(rs)-1].Seq
		if say != nil && n%(100*doltinput.QuestionBatch) == 0 {
			say("load: %d records", n)
		}
		return nil
	}))
	if err != nil {
		return err
	}
	if s.set.Limit == 0 {
		if err := CheckContentDigest(info.Digest, s.expect); err != nil {
			return err
		}
	}
	switch {
	case info.Digest != s.set.ContentDigest:
		return fmt.Errorf("%w: the file's content digest is %s, the question set's is %s", ErrDigestDiffers, info.Digest, s.set.ContentDigest)
	case n != s.set.Records || lastWritten != s.set.LastSeq:
		return fmt.Errorf("doltspike: the file gave %d records to seq %d, the question set was made from %d to %d", n, lastWritten, s.set.Records, s.set.LastSeq)
	case dropped != s.set.Dropped:
		return fmt.Errorf("doltspike: the file dropped %+v, the question set %+v", dropped, s.set.Dropped)
	case last() != lastWritten:
		return fmt.Errorf("doltspike: the engine is at seq %d, the stream ended at %d", last(), lastWritten)
	}
	return nil
}

// lastSeq is the part of an engine g0.CheckStream looks at.
type lastSeq func() uint64

func (l lastSeq) LastSeq() uint64 { return l() }
