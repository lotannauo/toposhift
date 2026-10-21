package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/duckdb/filestream"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// input is where the questions of a command come from: a plan the runner made from a
// generated stream, or a question set made from an activity file. Either is named by the
// SHA-256 of its file, which a command is told in advance, and gives the questions with
// the reference engine's answers and the stream they are about.
type input struct {
	kind          string // "plan" or "questions"
	sum           string
	queries       []runner.Query
	queriesDigest string
	stream        runner.StreamInfo
	plan          *runner.Plan
	set           *filestream.QuestionSet
}

// source is the stream of the input as a build can replay it: a plan's is generated, and
// a question set's is read from the activity file it was made from, which must be named.
func (in *input) source(activity string) (duckengine.Source, error) {
	switch {
	case in.plan != nil && activity != "":
		return duckengine.Source{}, errors.New("-activity names the file a question set was made from; a plan generates its stream")
	case in.plan != nil:
		return duckengine.PlanSource(in.plan, in.sum), nil
	case activity == "":
		return duckengine.Source{}, errors.New("-activity is required with -questions: the activity file the questions were made from")
	}
	return duckengine.FileSource(activity, in.set, in.sum), nil
}

// inputFlags are the flags that name the input of a command.
type inputFlags struct {
	plan, expectPlan, questions, expectQuestions *string
}

func addInputFlags(fs *flag.FlagSet) *inputFlags {
	return &inputFlags{
		plan:            fs.String("plan", "", "the plan.json of the questions"),
		expectPlan:      fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; required with -plan"),
		questions:       fs.String("questions", "", "the question set made from an activity file (duckspike questions), in place of a plan"),
		expectQuestions: fs.String("expect-questions", "", "the SHA-256 of the question set's file, as hex; required with -questions"),
	}
}

// load reads the input: exactly one of a plan and a question set, with the SHA-256 it
// must have.
func (f *inputFlags) load() (*input, error) {
	switch {
	case *f.plan != "" && *f.questions != "":
		return nil, errors.New("-plan and -questions are two ways to name the questions: give one")
	case *f.questions == "":
		if *f.expectQuestions != "" {
			return nil, errors.New("-expect-questions names the SHA-256 of a question set, and no -questions was given")
		}
		p, sum, err := loadPlan(*f.plan, *f.expectPlan, true)
		if err != nil {
			return nil, err
		}
		return &input{kind: "plan", sum: sum, queries: p.Queries, queriesDigest: p.QueriesDigest, stream: p.Stream, plan: p}, nil
	}
	if *f.expectPlan != "" {
		return nil, errors.New("-expect-plan names the SHA-256 of a plan, and -questions was given: use -expect-questions")
	}
	qs, sum, err := loadQuestions(*f.questions, *f.expectQuestions)
	if err != nil {
		return nil, err
	}
	return &input{kind: "questions", sum: sum, queries: qs.Queries, queriesDigest: qs.QueriesDigest, stream: qs.Stream, set: qs}, nil
}

// loadQuestions reads a question set, refusing it if its SHA-256 is not the expected one.
func loadQuestions(path, expect string) (*filestream.QuestionSet, string, error) {
	if expect == "" {
		return nil, "", errors.New("-expect-questions is required: a counter is only of questions that were named in advance")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(b)
	got := hex.EncodeToString(sum[:])
	if !strings.EqualFold(expect, got) {
		return nil, "", fmt.Errorf("%s has SHA-256 %s, not the expected %s", path, got, expect)
	}
	qs, err := filestream.Load(path)
	if err != nil {
		return nil, "", err
	}
	return qs, got, nil
}

// planRef names the input a counters file is of.
func (in *input) planRef() duckengine.PlanRef {
	return duckengine.PlanRef{Kind: in.kind, SHA256: in.sum, StreamDigest: in.stream.Digest, QueriesDigest: in.queriesDigest, Queries: len(in.queries)}
}
