package duckengine

import (
	"context"

	"github.com/lotannauo/toposhift/bench/duckdb/filestream"
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

// FileSource is the stream of a question set as a [Source]: the activity file it was made
// from, replayed, and each pass checked to be the stream the set was made from (the
// digest, the counts and the old snapshot token). The file is copied as the build's
// activity file as it is, so that the arms' files are made from the input's own bytes.
func FileSource(path string, qs *filestream.QuestionSet, label string) Source {
	return Source{
		Label: label,
		File:  path,
		Replay: func(ctx context.Context, sink runner.Sink) (string, error) {
			info, err := filestream.Replay(ctx, path, qs.ReplayOptions(), sink)
			if err != nil {
				return "", err
			}
			if err := g0.CheckStream(info, qs.Stream); err != nil {
				return "", err
			}
			return info.Digest, nil
		},
	}
}
