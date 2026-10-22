package vmrun

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/lotannauo/toposhift/bench/victoria/vminput"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// FactsRun is the check that the coalesced stream the reference's answers come from and
// the every-sample stream written to the store describe the same existence, made on a
// full input.
type FactsRun struct {
	Format int
	Env    Env
	Input  Ref
	// Coalesced is the base name of the coalesced file (an activity input).
	Coalesced string `json:",omitempty"`
	// Facts is the comparison: the subjects, how many ended identically and how many a run
	// earlier, the largest amount an end is earlier and the smallest band of a subject
	// compared. Every number is a function of the input and the registry.
	Facts vmmodel.FactsReport
	// WithinSmallestBand says the largest amount an end is earlier is below the smallest
	// band of any subject compared. The check does not need it (it holds each subject to its
	// own band, and a subject with a wide band may end earlier than a narrow one's), so it is
	// reported beside the check; with strict set, it is the check.
	WithinSmallestBand bool
	// ExtendFraction, ExtendEvery are the coalescer's extension rule that was checked.
	ExtendFraction float64
	ExtendEvery    string `json:",omitempty"`
	// Projected says the plan is read at pinned prefixes: PinnedEntities is how many, and
	// the store side is written through that projection. Retentions is how many the plan
	// has; the every-sample spec carries the same ones.
	Projected      bool
	PinnedEntities int
	Retentions     int
	Label          string
}

// ErrBand marks, under the strict reading, an end that is earlier by as much as the smallest
// band of any subject compared.
var ErrBand = errors.New("vmrun: the largest early end is not below the smallest band")

// Facts checks the premise on a full input. For a plan it generates the stream twice (the
// coalescer on, and the every-sample spec) and compares them with
// [vmmodel.CheckSameFacts]; for an activity file it compares the file with its coalesced
// counterpart. It fails if a subject's intervals differ beyond the coalescer's bound, if
// the coalescer's extension interval exceeds the window of a producer whose runs it
// extends, or, for a plan with pins or retentions, if the every-sample spec does not carry
// the pinned projection and the retentions. With strict it also fails if the largest amount
// any end is earlier is not below the smallest band of any subject compared, which holds a
// narrow-banded subject's limit against a wide-banded subject's earlier end.
func Facts(ctx context.Context, in *Input, coalesced string, fraction float64, strict bool, eo EnvOptions) (*FactsRun, error) {
	env, err := CollectEnv(ctx, nil, eo)
	if err != nil {
		return nil, err
	}
	rep := &FactsRun{Format: ReportFormat, Env: env, Input: in.Ref(), Label: DeterministicLabel}
	switch in.Kind {
	case KindPlan:
		spec := in.Plan.Spec
		every, pinned := vmmodel.EverySampleSpec(spec)
		if (spec.Pins != nil) != (pinned != nil) || (spec.Pins != nil && len(pinned) == 0) {
			return nil, errors.New("vmrun: the every-sample spec does not carry the plan's pinned projection")
		}
		if !slices.Equal(every.Retentions, spec.Retentions) {
			return nil, errors.New("vmrun: the every-sample spec does not carry the plan's retentions")
		}
		rep.Projected, rep.PinnedEntities, rep.Retentions = spec.Pins != nil, len(pinned), len(spec.Retentions)
		rep.ExtendFraction = spec.Workload.ExtendTTLFraction
		if spec.Workload.ExtendEvery > 0 {
			rep.ExtendEvery = spec.Workload.ExtendEvery.String()
		}
		if rep.Facts, err = vmmodel.CheckSameFacts(ctx, spec, in.Registry); err != nil {
			return rep, err
		}
	case KindActivity:
		if coalesced == "" {
			return nil, errors.New("vmrun: an activity input needs its coalesced file to be compared with (-coalesced)")
		}
		rep.Coalesced, rep.ExtendFraction = filepath.Base(coalesced), fraction
		if rep.Facts, err = vminput.CheckSameFacts(coalesced, in.Activity, in.Registry, fraction); err != nil {
			return rep, err
		}
	default:
		return nil, fmt.Errorf("vmrun: no input kind %q", in.Kind)
	}
	rep.WithinSmallestBand = rep.Facts.SmallestBand == 0 || rep.Facts.MaxEarly < rep.Facts.SmallestBand
	if strict && !rep.WithinSmallestBand {
		return rep, fmt.Errorf("%w: an end is %s earlier, the smallest band of a subject compared is %s", ErrBand, rep.Facts.MaxEarly, rep.Facts.SmallestBand)
	}
	return rep, nil
}
