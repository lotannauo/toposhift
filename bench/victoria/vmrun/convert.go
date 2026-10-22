package vmrun

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// ReportFormat is the version of the outputs' layout.
const ReportFormat = 1

// ErrNotDeterministic marks a conversion whose two runs over the same input differ.
var ErrNotDeterministic = errors.New("vmrun: two conversions of the same input differ")

// ErrMonths marks samples that fall in more than one UTC month.
var ErrMonths = errors.New("vmrun: the samples fall in more than one UTC month")

// DeterministicLabel is how the counts of a conversion are labelled once two runs agree.
const DeterministicLabel = "local, deterministic"

// ConvertReport is what a conversion comes to: every number in it is a function of the
// input, the registry and the converter, and the same on every run.
type ConvertReport struct {
	Format int
	Env    Env
	Input  Ref
	// Every is the every-sample stream that was converted, and Counts and SamplesDigest
	// what it came to: the samples, markers, series, churn per hour and the digest of the
	// samples and series.
	Every         Stream
	Counts        vmmodel.Counts
	SamplesDigest string
	// Months is the number of UTC months from the first sample to the last.
	Months int
	// Runs is how many conversions agreed, and Label what the counts are called.
	Runs  int
	Label string
}

// convertOnce converts the input's every-sample stream and calls emit with each batch of
// samples (nil drops them: the counts are kept).
func convertOnce(ctx context.Context, in *Input, mk func(*vmmodel.Converter) func([]vmmodel.Sample) error) (*vmmodel.Converter, Stream, error) {
	var conv *vmmodel.Converter
	opts := vmmodel.Options{Registry: in.Registry, Until: in.Until}
	if mk != nil {
		opts.Emit = func(ss []vmmodel.Sample) error { return mk(conv)(ss) }
	}
	conv, err := vmmodel.NewConverter(opts)
	if err != nil {
		return nil, Stream{}, err
	}
	st, err := in.Every(ctx, conv)
	if err != nil {
		return nil, st, err
	}
	if err := conv.Close(); err != nil {
		return nil, st, err
	}
	return conv, st, nil
}

// Months is the number of UTC months from the first sample to the last, counting the
// months between: zero if nothing was written.
func Months(c vmmodel.Counts) int {
	if c.Samples+c.Markers == 0 {
		return 0
	}
	idx := func(ms int64) int {
		t := time.UnixMilli(ms).UTC()
		return t.Year()*12 + int(t.Month()) - 1
	}
	return idx(c.LastMs) - idx(c.FirstMs) + 1
}

// CheckOneMonth refuses samples that fall in more than one UTC month. The server keeps
// its data in monthly partitions, and the series count it reports at /api/v1/series/count
// counts a series once for each partition it has samples in, so for a stream that crosses
// a month boundary that count is not the converter's. (The counter a load is checked
// against, the series the server created, counts each once.) The rule keeps every series
// count the baseline reports one number.
func CheckOneMonth(c vmmodel.Counts) error {
	if n := Months(c); n > 1 {
		return fmt.Errorf("%w: from %s to %s, %d months: a series is counted once per monthly partition, so the server's series count would not be the converter's",
			ErrMonths, time.UnixMilli(c.FirstMs).UTC().Format(time.RFC3339), time.UnixMilli(c.LastMs).UTC().Format(time.RFC3339), n)
	}
	return nil
}

// Convert converts the input's every-sample stream runs times (at least two) and returns
// its counts, which are labelled deterministic only if every run came to the same counts
// and the same digest. It needs no server.
func Convert(ctx context.Context, in *Input, runs int, eo EnvOptions) (*ConvertReport, error) {
	if runs < 2 {
		return nil, fmt.Errorf("vmrun: %d runs: counts are called deterministic only after two equal runs", runs)
	}
	env, err := CollectEnv(ctx, nil, eo)
	if err != nil {
		return nil, err
	}
	rep := &ConvertReport{Format: ReportFormat, Env: env, Input: in.Ref(), Label: DeterministicLabel}
	for i := 0; i < runs; i++ {
		conv, st, err := convertOnce(ctx, in, nil)
		if err != nil {
			return nil, err
		}
		counts, digest := conv.Counts(), conv.Digest()
		if i == 0 {
			rep.Every, rep.Counts, rep.SamplesDigest = st, counts, digest
			continue
		}
		if st != rep.Every || digest != rep.SamplesDigest || !reflect.DeepEqual(counts, rep.Counts) {
			return nil, fmt.Errorf("%w: run 1 gave digest %s, run %d gave %s", ErrNotDeterministic, rep.SamplesDigest, i+1, digest)
		}
	}
	rep.Runs = runs
	rep.Months = Months(rep.Counts)
	return rep, nil
}
