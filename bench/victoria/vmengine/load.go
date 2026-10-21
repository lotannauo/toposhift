package vmengine

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmwrite"
)

// settleInterval is how often a loader reads the counters while it waits for the server to
// be idle.
var settleInterval = 500 * time.Millisecond

// DefaultRequestSamples is the number of samples of a remote-write request.
const DefaultRequestSamples = 10_000

// Loader writes the converter's samples to a server, one request at a time, in the order
// the converter emits them (one writer). It does not wait for the server to make them
// readable: see [Loader.Verify].
type Loader struct {
	ctx     context.Context
	client  *Client
	batcher *vmwrite.Batcher
	conv    *vmmodel.Converter
	before  Counters
	// Requests and Samples are what was sent.
	Requests, Samples int
	// Wait is how long Verify waits for the counters to arrive; zero is a minute.
	Wait time.Duration
}

// NewLoader returns a loader of requests of size samples (zero is
// [DefaultRequestSamples]) for the series of conv, remembering the server's counters as
// they are now: what is checked afterwards is what changed.
func NewLoader(ctx context.Context, c *Client, conv *vmmodel.Converter, size int) (*Loader, error) {
	if size == 0 {
		size = DefaultRequestSamples
	}
	if err := c.Flush(ctx); err != nil {
		return nil, err
	}
	// Whatever was written before is counted by the time the server is idle.
	before, err := c.Settle(ctx, settleInterval, 3)
	if err != nil {
		return nil, err
	}
	l := &Loader{ctx: ctx, client: c, conv: conv, before: before}
	l.batcher, err = vmwrite.NewBatcher(size, func(i uint32) []vmmodel.Label { return conv.Series(i).Labels() }, l.send)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loader) send(r vmwrite.Request) error {
	if err := l.client.Write(l.ctx, r); err != nil {
		return err
	}
	l.Requests++
	l.Samples += r.Samples
	return nil
}

// Emit is the function to give the converter as Options.Emit.
func (l *Loader) Emit(ss []vmmodel.Sample) error { return l.batcher.Add(ss) }

// Flush sends the samples held back as a last request.
func (l *Loader) Flush() error { return l.batcher.Flush() }

// ErrNotIngested is wrapped by the error [Loader.Verify] returns when the server's
// counters do not say that every sample and series written arrived.
var ErrNotIngested = fmt.Errorf("vmengine: the server did not take in what was written")

// Verify makes what was written readable and checks the server's own counters against the
// converter's: the rows it received and added are the samples and markers written, none
// was ignored, and the series it created are the converter's. It waits (up to Wait) for
// the counters to arrive, because the server makes them current some time after a
// request is answered. The converter must be closed.
func (l *Loader) Verify(ctx context.Context) (Counters, error) {
	want := l.conv.Counts()
	rows := want.Samples + want.Markers
	var got Counters
	wait := l.Wait
	if wait == 0 {
		wait = time.Minute
	}
	deadline := time.Now().Add(wait)
	for {
		if err := l.client.Flush(ctx); err != nil {
			return Counters{}, err
		}
		c, err := l.client.Counters(ctx)
		if err != nil {
			return Counters{}, err
		}
		got = Counters{
			Received: c.Received - l.before.Received, Added: c.Added - l.before.Added,
			Ignored: c.Ignored - l.before.Ignored, NewSeries: c.NewSeries - l.before.NewSeries,
		}
		if got.Received >= rows && got.Added >= rows {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return Counters{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if got.Received != rows || got.Added != rows || got.Ignored != 0 || got.NewSeries != uint64(want.Series) || uint64(l.Samples) != rows {
		return got, fmt.Errorf("%w: written %d rows (%d samples, %d markers) in %d series; the server received %d, added %d, ignored %d and made %d series",
			ErrNotIngested, l.Samples, want.Samples, want.Markers, want.Series, got.Received, got.Added, got.Ignored, got.NewSeries)
	}
	return got, nil
}
