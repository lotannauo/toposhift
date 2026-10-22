package xtspike

import (
	"errors"
	"flag"
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

// inputFlags is the flags that name the stream and the questions: a plan, which regenerates
// its stream, or an activity file with the question set made for it.
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
	fs.StringVar(&f.label, "label", "", "what the stream is, recorded with every figure from it")
	if withActivity {
		fs.StringVar(&f.activity, "activity", "", "the activity file (Parquet) to load, with -questions")
		fs.StringVar(&f.expectActivity, "expect-activity", "", "the content digest of the activity file, or a prefix of at least 12 hex digits (required with -activity)")
		fs.BoolVar(&f.dropBasis, "drop-basis", false, "accept records that have an event time basis, and drop it (counted)")
		fs.BoolVar(&f.dropBoot, "drop-boot", false, "accept records that have a boot id, and drop it (counted)")
	}
}

// stream is a stream and its questions, from a plan or from an activity file.
type stream struct {
	plan     *runner.Plan
	planSHA  string
	set      *xtinput.QuestionSet
	setSHA   string
	activity string
	expect   string
	opts     xtinput.Options
	label    string
}

// resolve checks the files named by the flags against their digests, and reads them. With
// needActivity, a question set must come with the activity file it was made for.
func (f *inputFlags) resolve(needActivity bool) (*stream, error) {
	s := &stream{label: f.label, opts: xtinput.Options{DropBasis: f.dropBasis, DropBoot: f.dropBoot}, activity: f.activity, expect: f.expectActivity}
	switch {
	case f.plan != "" && (f.questions != "" || f.activity != ""):
		return nil, errors.New("xtspike: a plan and an activity file are two inputs: name one")
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
		q, err := xtinput.LoadQuestions(f.questions)
		if err != nil {
			return nil, err
		}
		s.set, s.setSHA = q, sum
		if needActivity {
			if f.activity == "" {
				return nil, errors.New("xtspike: -questions needs the -activity file it was made for")
			}
			if f.expectActivity == "" {
				return nil, errors.New("xtspike: -activity must be named by its content digest (-expect-activity)")
			}
			if q.Options != s.opts {
				return nil, fmt.Errorf("xtspike: the question set was made with %+v, not %+v", q.Options, s.opts)
			}
		}
	default:
		return nil, errors.New("xtspike: name the input: -plan with -expect-plan, or -questions with -expect-questions")
	}
	return s, nil
}

func (s *stream) queries() []runner.Query {
	if s.plan != nil {
		return s.plan.Queries
	}
	return s.set.Queries
}

func (s *stream) queriesDigest() string {
	if s.plan != nil {
		return s.plan.QueriesDigest
	}
	return s.set.QueriesDigest
}

func (s *stream) lastSeq() uint64 {
	if s.plan != nil {
		return s.plan.Stream.LastSeq
	}
	return s.set.LastSeq
}

func (s *stream) records() uint64 {
	if s.plan != nil {
		return s.plan.Stream.Records
	}
	return s.set.Records
}

// input is the record of what the stream is.
func (s *stream) input() xtreport.Input {
	in := xtreport.Input{QueriesDigest: s.queriesDigest(), Records: s.records(), LastSeq: s.lastSeq(), Label: s.label}
	if s.plan != nil {
		in.Kind, in.PlanSHA256, in.StreamDigest = "plan", s.planSHA, s.plan.Stream.Digest
		in.Retentions, in.Dropped = len(s.plan.Stream.Retentions), s.plan.Stream.Dropped
		return in
	}
	in.Kind, in.QuestionsSHA256, in.ContentDigest, in.StreamDigest = "activity", s.setSHA, s.set.ContentDigest, s.set.ContentDigest
	if s.set.Limit > 0 && in.Label == "" {
		in.Label = fmt.Sprintf("a prefix of the file, about %d records", s.set.Limit)
	}
	return in
}
