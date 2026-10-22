package vmrun

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vminput"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/identity"
)

// The kinds of input.
const (
	KindPlan     = "plan"
	KindActivity = "activity"
)

// ErrPlanStream marks a plan whose regenerated stream is not the one it records.
var ErrPlanStream = errors.New("vmrun: the plan's stream does not regenerate to the digest it records")

// Input is the stream a run is made from, with the registry that reads it and the
// questions asked of it.
//
// A plan is a synthetic workload: the reference's answers come from its coalesced stream
// (the plan's own), and the store is written the every-sample stream of the same spec
// ([vmmodel.EverySampleSpec]). An activity file is a recorded stream (the Alibaba
// placement trace's every-sample file): the questions come with it ([vminput.Questions]),
// and the reference's answers come from the same file, so its two views are one.
type Input struct {
	Kind string
	// Plan and PlanSHA are the plan and the SHA-256 of its file (a plan input).
	Plan    *runner.Plan
	PlanSHA string
	// Activity is the path of the file the store is written from; Questions and
	// QuestionsSHA the question set and the SHA-256 of its file (an activity input; the
	// questions are needed only to read).
	Activity     string
	Questions    *vminput.Questions
	QuestionsSHA string

	// Registry reads the input's producers; Until ends the period a producer in watch
	// mode is scraped to (zero when there is none).
	Registry *vmmodel.Registry
	Until    time.Time

	// every replaces Every in tests.
	every func(context.Context, ...runner.Sink) (Stream, error)
}

// LoadPlanInput reads a plan file, refusing it unless its SHA-256 is the one named in
// advance, and returns the input over it.
func LoadPlanInput(path, expectSHA string) (*Input, error) {
	sum, err := FileSHA256(path)
	if err != nil {
		return nil, err
	}
	if err := checkSHA("plan", path, sum, expectSHA); err != nil {
		return nil, err
	}
	p, err := runner.LoadPlan(path)
	if err != nil {
		return nil, err
	}
	return PlanInput(p, sum)
}

// PlanInput returns the input over a plan whose file has the SHA-256 sum.
func PlanInput(p *runner.Plan, sum string) (*Input, error) {
	reg, err := vmmodel.RegistryFor(p.Spec.Workload)
	if err != nil {
		return nil, err
	}
	return &Input{
		Kind: KindPlan, Plan: p, PlanSHA: sum, Registry: reg,
		Until: p.Spec.Workload.Start.Add(p.Spec.Workload.Duration),
	}, nil
}

// ActivityOptions say how an activity file is read.
type ActivityOptions struct {
	// TTL is the TTL the file's records carry, which the registry reads its one producer
	// through; zero is [vminput.AlibabaC300TTL].
	TTL time.Duration
	// Questions and ExpectQuestions are the question set's file and the SHA-256 it must
	// have; both empty means no questions (enough to convert, to check facts and to load).
	Questions, ExpectQuestions string
}

// ActivityInput returns the input over an activity file.
func ActivityInput(path string, o ActivityOptions) (*Input, error) {
	ttl := o.TTL
	if ttl == 0 {
		ttl = vminput.AlibabaC300TTL
	}
	reg, err := vminput.AlibabaRegistry(ttl)
	if err != nil {
		return nil, err
	}
	in := &Input{Kind: KindActivity, Activity: path, Registry: reg}
	if o.Questions == "" {
		return in, nil
	}
	sum, err := FileSHA256(o.Questions)
	if err != nil {
		return nil, err
	}
	if err := checkSHA("questions", o.Questions, sum, o.ExpectQuestions); err != nil {
		return nil, err
	}
	q, err := vminput.LoadQuestions(o.Questions)
	if err != nil {
		return nil, err
	}
	info, err := vminput.Info(path)
	if err != nil {
		return nil, err
	}
	if q.Digest != info.Digest || q.Records != uint64(info.Records) || q.LastSeq != info.MaxSeq {
		return nil, fmt.Errorf("vmrun: the questions %s were chosen from the file with digest %s (%d records, last seq %d), and %s is %s (%d records, last seq %d)",
			o.Questions, q.Digest, q.Records, q.LastSeq, path, info.Digest, info.Records, info.MaxSeq)
	}
	in.Questions, in.QuestionsSHA = q, sum
	return in, nil
}

// Stream is what a pass over the every-sample stream amounted to: its digest, the
// records the generator or the file produced and the last sequence number.
type Stream struct {
	Digest  string
	Records uint64
	LastSeq uint64
}

// Every writes the every-sample stream of the input into the sinks: for a plan, the
// generator with the coalescer off, projected onto the pinned entities if the plan has
// pins; for an activity file, the file.
func (in *Input) Every(ctx context.Context, sinks ...runner.Sink) (Stream, error) {
	if in.every != nil {
		return in.every(ctx, sinks...)
	}
	switch in.Kind {
	case KindPlan:
		every, pinned := vmmodel.EverySampleSpec(in.Plan.Spec)
		info, err := runner.Drive(ctx, every, vmmodel.Projection(pinned, sinks...))
		return Stream{Digest: info.Digest, Records: info.Records, LastSeq: info.LastSeq}, err
	case KindActivity:
		st, err := vminput.Drive(in.Activity, sinks...)
		return Stream{Digest: st.Digest, Records: st.Records, LastSeq: st.LastSeq}, err
	}
	return Stream{}, fmt.Errorf("vmrun: no input kind %q", in.Kind)
}

// Queries returns the questions asked of the input, with the reference's answers.
func (in *Input) Queries() ([]runner.Query, error) {
	switch {
	case in.Kind == KindPlan:
		return in.Plan.Queries, nil
	case in.Questions != nil:
		return in.Questions.Queries, nil
	}
	return nil, errors.New("vmrun: the input has no questions (give -questions for an activity file)")
}

// LastSeq is the sequence number of the last record the questions' reference was made
// from; the engine reads an earlier token as an old snapshot, which it refuses.
func (in *Input) LastSeq() uint64 {
	switch {
	case in.Kind == KindPlan:
		return in.Plan.Stream.LastSeq
	case in.Questions != nil:
		return in.Questions.LastSeq
	}
	return 0
}

// CoalescedDigest is the digest of the coalesced stream the plan's answers come from,
// recorded beside the every-sample stream's. It is empty for an activity file.
func (in *Input) CoalescedDigest() string {
	if in.Kind == KindPlan {
		return in.Plan.Stream.Digest
	}
	return ""
}

// Ref names the input in an output.
type Ref struct {
	Kind string
	// PlanSHA256 is the SHA-256 of the plan file; Activity the base name of the activity
	// file; QuestionsSHA256 the SHA-256 of the questions file.
	PlanSHA256      string `json:",omitempty"`
	Activity        string `json:",omitempty"`
	QuestionsSHA256 string `json:",omitempty"`
	// CoalescedStreamDigest is the digest of the stream the plan's answers come from.
	CoalescedStreamDigest string `json:",omitempty"`
	// RegistrySHA256 is the digest of the registry's canonical text.
	RegistrySHA256 string
}

// Ref returns how the input is named in an output.
func (in *Input) Ref() Ref {
	r := Ref{Kind: in.Kind, PlanSHA256: in.PlanSHA, QuestionsSHA256: in.QuestionsSHA, CoalescedStreamDigest: in.CoalescedDigest(), RegistrySHA256: in.Registry.Digest()}
	if in.Activity != "" {
		r.Activity = filepath.Base(in.Activity)
	}
	return r
}

// Worlds returns the reference's view of the subjects the questions read, from the stream
// the answers come from, and the view of the stream the store was written from (nil when
// they are the same stream, as for an activity file). For a plan the reference's stream is
// regenerated, and refused unless it is the digest the plan records.
func (in *Input) Worlds(ctx context.Context) (ref, held *vmmodel.World, err error) {
	qs, err := in.Queries()
	if err != nil {
		return nil, nil, err
	}
	var fps []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	for _, q := range qs {
		for _, fp := range q.Fps {
			if !seen[fp] {
				seen[fp] = true
				fps = append(fps, fp)
			}
		}
	}
	ref = vmmodel.NewWorld(fps...)
	if in.Kind == KindActivity {
		st, err := in.Every(ctx, ref)
		if err != nil {
			return nil, nil, err
		}
		if st.Digest != in.Questions.Digest {
			return nil, nil, fmt.Errorf("vmrun: the file's digest is %s, the questions' is %s", st.Digest, in.Questions.Digest)
		}
		return ref, nil, nil
	}
	info, err := runner.Drive(ctx, in.Plan.Spec, ref)
	if err != nil {
		return nil, nil, err
	}
	if info.Digest != in.Plan.Stream.Digest || info.LastSeq != in.Plan.Stream.LastSeq || info.Records != in.Plan.Stream.Records {
		return nil, nil, fmt.Errorf("%w: it regenerates to %s (%d records, last seq %d), the plan records %s (%d, %d)",
			ErrPlanStream, info.Digest, info.Records, info.LastSeq, in.Plan.Stream.Digest, in.Plan.Stream.Records, in.Plan.Stream.LastSeq)
	}
	held = vmmodel.NewWorld(fps...)
	if _, err := in.Every(ctx, held); err != nil {
		return nil, nil, err
	}
	return ref, held, nil
}
