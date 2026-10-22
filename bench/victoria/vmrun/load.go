package vmrun

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// ErrServerNotEmpty marks a server that already holds series of the baseline.
var ErrServerNotEmpty = errors.New("vmrun: the server already holds the baseline's series")

// ErrNotLoaded marks a load that is not valid: the server's counters are not the
// converter's.
var ErrNotLoaded = errors.New("vmrun: the load is INVALID")

// LoadOptions say how a stream is written.
type LoadOptions struct {
	// RequestSamples is the samples of a remote-write request; zero is
	// [vmengine.DefaultRequestSamples].
	RequestSamples int
	// Wait is how long to wait for the server's counters to arrive; zero is a minute.
	Wait time.Duration
	// Settle and Stable are how often the counters are read while the server settles
	// before the load, and how many equal reads in a row say it has: [DefaultSettle] and
	// [DefaultStable] if zero.
	Settle time.Duration
	Stable int
	Env    EnvOptions
}

// LoadReport is what a load did and what the server's own counters said of it.
type LoadReport struct {
	Format int
	Env    Env
	Input  Ref
	// Every, Counts, SamplesDigest and Months are the conversion's: what was to be written.
	Every         Stream
	Counts        vmmodel.Counts
	SamplesDigest string
	Months        int
	// Requests and Samples are what was sent: the number of remote-write requests and the
	// rows in them (samples and markers).
	Requests, Samples int
	// Before are the server's counters before the load, and Server the change in the
	// received, added, ignored and series-created counters the load made.
	Before, Server vmengine.Counters
	// Invalid is why the load is not valid, empty if it is. The run is INVALID unless
	// the server received and added exactly the rows written, ignored none and created
	// exactly the converter's series.
	Invalid string `json:",omitempty"`
}

// Load writes the input's every-sample stream, as samples, into a server that holds none
// of the baseline's series, and checks the server's counters against the converter's: the
// load is INVALID (and the report says why, with the error wrapping [ErrNotLoaded] or the
// guard that refused it) unless every row arrived, none was ignored and the series are the
// converter's. The server is first checked ([CollectEnv]) and the stream converted without
// writing, so that a stream that crosses a month is refused before the first write. The
// stream is converted twice, and the second conversion, the one that is written, must be
// the first's digest.
func Load(ctx context.Context, in *Input, c *vmengine.Client, o LoadOptions) (*LoadReport, error) {
	rep := &LoadReport{Format: ReportFormat, Input: in.Ref()}
	env, err := CollectEnv(ctx, c, o.Env)
	rep.Env = env
	if err != nil {
		rep.Invalid = err.Error()
		return rep, err
	}
	if !env.FlagsChecked {
		err := errors.New("vmrun: a load needs a server whose flags were checked")
		rep.Invalid = err.Error()
		return rep, err
	}
	fail := func(err error) (*LoadReport, error) {
		rep.Invalid = err.Error()
		return rep, err
	}

	// Nothing is written before the counts say the stream can be checked.
	first, st, err := convertOnce(ctx, in, nil)
	if err != nil {
		return fail(err)
	}
	rep.Every, rep.Counts, rep.SamplesDigest, rep.Months = st, first.Counts(), first.Digest(), Months(first.Counts())
	if err := CheckOneMonth(rep.Counts); err != nil {
		return fail(err)
	}
	if err := c.Flush(ctx); err != nil {
		return fail(err)
	}
	if has, err := c.HasSeries(ctx, `{__name__=~"topo_.*"}`); err != nil {
		return fail(err)
	} else if has {
		return fail(fmt.Errorf("%w: start one on an empty data directory", ErrServerNotEmpty))
	}

	var ld *vmengine.Loader
	var conv *vmmodel.Converter
	conv, err = vmmodel.NewConverter(vmmodel.Options{
		Registry: in.Registry, Until: in.Until,
		Emit: func(ss []vmmodel.Sample) error { return ld.Emit(ss) },
	})
	if err != nil {
		return fail(err)
	}
	settle, stable := o.Settle, o.Stable
	if settle == 0 {
		settle = DefaultSettle
	}
	if stable == 0 {
		stable = DefaultStable
	}
	if ld, err = vmengine.NewLoaderSettling(ctx, c, conv, o.RequestSamples, settle, stable); err != nil {
		return fail(err)
	}
	ld.Wait = o.Wait
	rep.Before = ld.Before()
	st2, err := in.Every(ctx, conv)
	if err != nil {
		return fail(err)
	}
	if err := conv.Close(); err != nil {
		return fail(err)
	}
	if err := ld.Flush(); err != nil {
		return fail(err)
	}
	rep.Requests, rep.Samples = ld.Requests, ld.Samples
	if st2 != st || conv.Digest() != rep.SamplesDigest {
		return fail(fmt.Errorf("%w: the stream that was written (digest %s) is not the one that was counted (digest %s)", ErrNotDeterministic, conv.Digest(), rep.SamplesDigest))
	}
	got, err := ld.Verify(ctx)
	rep.Server = got
	if err != nil {
		return fail(fmt.Errorf("%w: %w", ErrNotLoaded, err))
	}
	return rep, nil
}
