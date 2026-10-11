package runner

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// fakeClock is a clock that moves only when a test moves it or a pacer sleeps.
type fakeClock struct {
	t      time.Time
	sleeps []time.Duration
}

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) sleep(d time.Duration) {
	c.sleeps = append(c.sleeps, d)
	c.t = c.t.Add(d)
}

func testPacer(rate int, c *fakeClock) *pacer {
	p := newPacer(rate)
	p.now, p.sleep = c.now, c.sleep
	return p
}

// The write before record n never begins before start + n/R, whatever the writes
// before it took.
func TestThePacerNeverRunsAheadOfItsSchedule(t *testing.T) {
	t.Parallel()

	const rate, batch = 4500, 450 // a batch is due every 100 ms
	rng := rand.New(rand.NewPCG(1, 2))
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p := testPacer(rate, c)
	var start time.Time
	written := 0
	for range 500 {
		p.wait(batch)
		if start.IsZero() {
			start = c.t
		}
		due := start.Add(time.Duration(float64(written) / rate * float64(time.Second)))
		if c.t.Before(due) {
			t.Fatalf("the batch of record %d began at %s, %s before it was due", written, c.t.Sub(start), due.Sub(c.t))
		}
		// Writes of any length up to one and a half intervals: the writer is ahead of its
		// schedule more often than not, and sleeps, and sometimes falls behind.
		c.t = c.t.Add(time.Duration(rng.Int64N(int64(150 * time.Millisecond))))
		p.done(batch)
		written += batch
	}
}

// A writer behind its schedule writes without sleeping until it is back on it, and not
// for longer: the sleeping that follows is only what the schedule asks.
func TestThePacerDoesNotSleepWhenBehind(t *testing.T) {
	t.Parallel()

	const rate, batch = 1000, 100 // a batch is due every 100 ms
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p := testPacer(rate, c)
	p.wait(batch) // the first call starts the schedule
	if len(c.sleeps) != 0 {
		t.Fatalf("the first batch slept %v", c.sleeps)
	}
	c.t = c.t.Add(time.Second) // a write that took ten intervals
	p.done(batch)
	for i := 1; i <= 9; i++ { // the schedule is nine batches behind
		p.wait(batch)
		if len(c.sleeps) != 0 {
			t.Fatalf("batch %d slept %v while behind", i, c.sleeps)
		}
		c.t = c.t.Add(time.Millisecond) // writes that take almost nothing
		p.done(batch)
	}
	p.wait(batch) // due at 1.0 s, it is 1.009 s: still no sleep
	p.done(batch)
	p.wait(batch) // due at 1.1 s, now 1.009 s: sleeps the difference
	want := 100*time.Millisecond*11 - (time.Second + 9*time.Millisecond)
	if len(c.sleeps) != 1 || c.sleeps[0] != want {
		t.Errorf("slept %v, want one sleep of %s", c.sleeps, want)
	}
}

// 10^5 records at 4,500 a second with writes of jittered length (many longer than the
// interval of a batch) come out at the rate, within 2%.
func TestThePacerAchievesItsRate(t *testing.T) {
	t.Parallel()

	const rate, batch, records = 4500, 1000, 100_000
	rng := rand.New(rand.NewPCG(7, 8))
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p := testPacer(rate, c)
	var start time.Time
	for written := 0; written < records; written += batch {
		p.wait(batch)
		if start.IsZero() {
			start = c.t
		}
		// A write of 0 to 400 ms against a batch interval of 222 ms: it is behind some of the time.
		c.t = c.t.Add(time.Duration(rng.Int64N(int64(400 * time.Millisecond))))
		p.done(batch)
	}
	got := float64(records) / c.t.Sub(start).Seconds()
	if math.Abs(got-rate)/rate > 0.02 {
		t.Errorf("achieved %.0f records/s, want %d within 2%%", got, rate)
	}
}

// The day a build is paced from is 2W + 1.5 - ceil(W/2).
func TestThePaceStartIsTheRulesDay(t *testing.T) {
	t.Parallel()

	for days, want := range map[int]float64{2: 4.5, 7: 11.5, 14: 22.5, 30: 46.5, 3: 5.5} {
		if got := PaceFromDay(days); got != want {
			t.Errorf("PaceFromDay(%d) = %v, want %v", days, got, want)
		}
	}
}
