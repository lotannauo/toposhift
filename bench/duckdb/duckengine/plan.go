package duckengine

import (
	"context"

	"github.com/lotannauo/toposhift/bench/duckdb/g0"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// PlanSource is the stream of a plan as a [Source]: the runner regenerates it, and each
// pass is checked to be the stream the plan was made from (the digest, the counts and
// the old snapshot token), so that a build of files from a stream that is not the
// plan's is refused before any file is read.
func PlanSource(plan *runner.Plan, label string) Source {
	return Source{
		Label:   label,
		Horizon: plan.Stream.Horizon,
		Replay: func(ctx context.Context, sink runner.Sink) (string, error) {
			info, err := runner.Drive(ctx, plan.Spec, sink)
			if err != nil {
				return "", err
			}
			if err := g0.CheckStream(info, plan.Stream); err != nil {
				return "", err
			}
			return info.Digest, nil
		},
	}
}
