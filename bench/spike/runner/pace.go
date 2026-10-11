package runner

import (
	"math"
	"time"
)

// PaceFromDay is the stream day a background build of a window of days days of
// retained history is paced from: 2W + 1.5 - ceil(W/2) (11.5 for 7 days, 46.5 for 30).
// The build writes the days before it unpaced, and holds the writer to the rate from
// then on; the retentions that start at or after it are the ones the gate judges.
func PaceFromDay(days int) float64 {
	return float64(2*days) + 1.5 - math.Ceil(float64(days)/2)
}

// A pacer holds a writer to a rate from the moment it starts: record n is due at
// start + n/rate. It never lets a write begin ahead of its schedule, and it never
// sleeps when the writer is behind it. The schedule is open-loop: the due time of a
// batch comes from the start and the number of records before it, never from how long
// the batches before it took, so a writer that falls behind writes without a pause
// until it is back on schedule and no further.
type pacer struct {
	rate  float64 // records per second, above 0
	now   func() time.Time
	sleep func(time.Duration)
	// start is the instant of the first wait; zero before it.
	start   time.Time
	written int64
}

func newPacer(rate int) *pacer {
	return &pacer{rate: float64(rate), now: time.Now, sleep: time.Sleep}
}

// wait is called before a batch of n records: on the first call it starts the schedule
// and returns at once, and on the others it sleeps until the instant the batch is due
// if that is later than now.
func (p *pacer) wait(n int) {
	if p.start.IsZero() {
		p.start = p.now()
		return
	}
	due := p.start.Add(time.Duration(float64(p.written) / p.rate * float64(time.Second)))
	if d := due.Sub(p.now()); d > 0 {
		p.sleep(d)
	}
}

// done is called after the batch is written.
func (p *pacer) done(n int) { p.written += int64(n) }
