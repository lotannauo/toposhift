package vmrun

import (
	"context"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// SetEvery replaces the stream the input writes into its sinks, for a test.
func (in *Input) SetEvery(f func(context.Context, ...runner.Sink) (Stream, error)) { in.every = f }
