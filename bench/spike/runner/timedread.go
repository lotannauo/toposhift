package runner

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TimingsFile is the name of the file a timed read writes beside results.json.
const TimingsFile = "timings.json"

// ReadTimingsVersion is the version of the layout of [ReadTimings].
const ReadTimingsVersion = 1

// The warm-up and timed asks per query when a timed read is not told otherwise: three
// that are not timed, so that what the query needs is in the engine's memory, and
// thirty-one that are, an odd number, so that the median of them is one of them.
const (
	DefaultTimedWarmup = 3
	DefaultTimedReps   = 31
)

// WarmProbe reads a counter that grows when a read is served from outside the memory
// the engine was given (for Pebble: block-cache misses). A query whose timed asks move
// it was not served from memory and is not warm. The name goes into the timings file.
// An engine that has no such counter has no probe, and is not timed by this harness.
type WarmProbe interface {
	WarmCounter() (name string, n int64, err error)
}

// TimedReadOptions say how [TimeQueries] times.
type TimedReadOptions struct {
	// Warmup and Reps are the untimed and the timed asks per query; zero means
	// [DefaultTimedWarmup] and [DefaultTimedReps].
	Warmup, Reps int
	// Clock reads a monotonic clock in nanoseconds; nil is the real one. A test gives
	// a clock it moves itself.
	Clock func() int64
	// Mute stops the engine's counter reporting from being recorded for as long as
	// the function it returns is not called; nil is nothing to mute. It is called once.
	Mute func() func()
}

// QueryTiming is what the timed asks of one query came to.
type QueryTiming struct {
	Name, Group, Age string
	Op               Op
	// Digest is the answer of the first timed ask, and Mismatch says that an ask, timed
	// or not, answered otherwise than the reference engine.
	Digest   string
	Mismatch bool
	// Ns are the durations of the timed asks, in the order they were made.
	Ns []int64
	// WarmDelta is how much the probe's counter grew over the timed asks, and Warm that
	// it did not.
	WarmDelta int64
	Warm      bool
	// GCs is how many collections ran from the start of the first timed ask to the end
	// of the last (the hashing of the answers between the asks is among them).
	GCs uint32
}

// ReadTimings is the file a timed read writes: what the read ran on and how, and the
// durations of every query's timed asks. The CPU model is not here: the job file the
// bench workflow writes beside it has it.
type ReadTimings struct {
	Version                               int
	Candidate, PlanDigest, ManifestDigest string
	Build                                 BuildInfo
	Untimed                               bool
	GOMAXPROCS, NumCPU                    int
	// GOGC is the environment's value, empty for unset, and GoMemLimit the runtime's
	// memory limit (math.MaxInt64 for none).
	GOGC       string
	GoMemLimit int64
	// CacheBytes is the opened store's block cache.
	CacheBytes            int64
	Warmup, Reps, Readers int
	// WarmCounter is the name of the probe's counter.
	WarmCounter string
	// Queries is parallel to the plan's queries.
	Queries []QueryTiming
}

// askSteps are the two parts of an ask, so that a test can see what is between the
// clock's two readings.
type askSteps struct {
	fetch  func(engine.Engine, Query) (fetched, error)
	answer func(fetched, Query) (Answer, error)
}

var realSteps = askSteps{fetch: fetch, answer: fetched.answer}

// TimeQueries puts every query of the plan to e: Warmup untimed asks, then Reps timed
// ones, one reader, in the plan's order. It reads the probe before and after the timed
// asks of each query, checks every answer against the query's expected one outside
// the timed interval, and counts the collections that ran during them. The interval
// of a timed ask holds the engine's call and nothing else: the answer is hashed after
// it. It never prints a duration.
//
// The context is checked between queries, never inside an interval.
func TimeQueries(ctx context.Context, e engine.Engine, probe WarmProbe, plan *Plan, o TimedReadOptions) ([]QueryTiming, error) {
	return timeQueries(ctx, e, probe, plan, o, realSteps)
}

func timeQueries(ctx context.Context, e engine.Engine, probe WarmProbe, plan *Plan, o TimedReadOptions, steps askSteps) ([]QueryTiming, error) {
	if o.Warmup < 0 || o.Reps < 0 {
		return nil, fmt.Errorf("runner: a timed read cannot make %d warm-up and %d timed asks", o.Warmup, o.Reps)
	}
	if o.Warmup == 0 {
		o.Warmup = DefaultTimedWarmup
	}
	if o.Reps == 0 {
		o.Reps = DefaultTimedReps
	}
	clock := o.Clock
	if clock == nil {
		start := time.Now()
		clock = func() int64 { return int64(time.Since(start)) }
	}
	if o.Mute != nil {
		defer o.Mute()()
	}
	out := make([]QueryTiming, len(plan.Queries))
	for i, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		qt := QueryTiming{Name: q.Name(), Group: q.Group, Age: q.Age, Op: q.Op, Ns: make([]int64, 0, o.Reps)}
		fail := func(what string, err error) error {
			return fmt.Errorf("runner: %s (%s): %w", q.Name(), what, err)
		}
		for range o.Warmup {
			f, err := steps.fetch(e, q)
			if err != nil {
				return nil, fail("warm-up", err)
			}
			a, err := steps.answer(f, q)
			if err != nil {
				return nil, fail("warm-up", err)
			}
			if a.Digest != q.Expect {
				qt.Mismatch = true
			}
		}
		_, p0, err := probe.WarmCounter()
		if err != nil {
			return nil, fail("warm counter", err)
		}
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		for rep := range o.Reps {
			t0 := clock()
			f, err := steps.fetch(e, q)
			d := clock() - t0
			if err != nil {
				return nil, fail("timed", err)
			}
			a, err := steps.answer(f, q)
			if err != nil {
				return nil, fail("timed", err)
			}
			if rep == 0 {
				qt.Digest = a.Digest
			}
			if a.Digest != q.Expect {
				qt.Mismatch = true
			}
			qt.Ns = append(qt.Ns, d)
		}
		runtime.ReadMemStats(&m1)
		_, p1, err := probe.WarmCounter()
		if err != nil {
			return nil, fail("warm counter", err)
		}
		qt.WarmDelta, qt.Warm = p1-p0, p1 == p0
		qt.GCs = m1.NumGC - m0.NumGC
		out[i] = qt
	}
	return out, nil
}
