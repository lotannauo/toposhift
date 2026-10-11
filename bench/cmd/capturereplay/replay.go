package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// replayOptions says where the stores are made and what to say while working.
type replayOptions struct {
	// Tmp is the directory the stores are made in; empty is the system's.
	Tmp string
	// Keep leaves the stores' directories in place.
	Keep bool
	// Progress, if not nil, gets a line for each run.
	Progress io.Writer
	// FlushTimes are the times, in order, at which each pass flushes its coalescer, on
	// the capture's clock: before a batch whose time is at or after one that has not
	// been used, and after the last run for the rest. The sequencer flushes on its
	// ingest clock at least every 30 s; the replay uses the times the truth was listed.
	FlushTimes []time.Time
}

// replayed is the result of replaying the runs of a capture: the counts that do not
// depend on the translators, and two passes over two stores each.
type replayed struct {
	Files []*runFile
	Stats []*runStats
	// A is the pass in which toposhift kept running while the collector restarted; B is
	// the pass in which it restarted with the collector.
	A, B           *pass
	storesA        *stores
	storesB        *stores
	keep           bool
	Elapsed        time.Duration
	largestRun     int
	largestLine    int
	largestPullLen int64
}

// close closes the stores.
func (r *replayed) close() error {
	var err error
	if r.storesA != nil {
		err = errors.Join(err, r.storesA.close(r.keep))
	}
	if r.storesB != nil {
		err = errors.Join(err, r.storesB.close(r.keep))
	}
	return err
}

// replayRuns reads the run files in order and feeds both passes, a line at a time, in the
// order of the file. On success the caller closes the result.
func replayRuns(ctx context.Context, paths []string, o replayOptions) (_ *replayed, err error) {
	start := time.Now()
	rep := &replayed{keep: o.Keep, largestRun: -1}
	defer func() {
		if err != nil {
			err = errors.Join(err, rep.close())
		}
	}()
	if rep.storesA, err = openStores(o.Tmp, "a"); err != nil {
		return nil, err
	}
	if rep.storesB, err = openStores(o.Tmp, "b"); err != nil {
		return nil, err
	}
	ma, pa := rep.storesA.asStores()
	mb, pb := rep.storesB.asStores()
	if rep.A, err = newPass("A", false, ma, pa); err != nil {
		return nil, err
	}
	if rep.B, err = newPass("B", true, mb, pb); err != nil {
		return nil, err
	}
	rep.A.flushTimes, rep.B.flushTimes = o.FlushTimes, o.FlushTimes
	for i, path := range paths {
		rf, err := scanRun(path, i+1)
		if err != nil {
			return nil, err
		}
		order, err := replayOrder(rf.Refs)
		if err != nil {
			return nil, err
		}
		rs := newRunStats(rf)
		rep.Files = append(rep.Files, rf)
		rep.Stats = append(rep.Stats, rs)
		for _, p := range []*pass{rep.A, rep.B} {
			if err := p.startRun(ctx, rf.N); err != nil {
				return nil, err
			}
		}
		err = rf.walk(order, func(b batch) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			rs.observe(b)
			if err := rep.A.feed(ctx, b); err != nil {
				return err
			}
			return rep.B.feed(ctx, b)
		})
		if err != nil {
			return nil, err
		}
		if rs.LargestPull.Bytes > rep.largestPullLen {
			rep.largestPullLen, rep.largestRun, rep.largestLine = rs.LargestPull.Bytes, i, rs.LargestPull.Line-1
		}
		if o.Progress != nil {
			fmt.Fprintf(o.Progress, "run %d: %d lines, %d records, %s elapsed\n", rf.N, len(rf.Lines), rs.Records, time.Since(start).Round(time.Second))
		}
	}
	for _, p := range []*pass{rep.A, rep.B} {
		if err := p.flushRest(ctx); err != nil {
			return nil, err
		}
		p.snapshot()
	}
	rep.Elapsed = time.Since(start)
	return rep, nil
}

// largestPull measures the largest pull line of the capture.
func (r *replayed) largestPull() (pullSize, int, int, error) {
	if r.largestRun < 0 {
		return pullSize{}, 0, 0, errors.New("the capture has no pull line")
	}
	rf := r.Files[r.largestRun]
	raw, err := rf.readLine(r.largestLine)
	if err != nil {
		return pullSize{}, 0, 0, err
	}
	ps, err := measurePull(raw)
	return ps, rf.N, r.largestLine + 1, err
}
