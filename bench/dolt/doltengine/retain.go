package doltengine

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// Retain implements [engine.Engine]. In one transaction it deletes the log rows before
// the horizon but, for each key, the newest (the baseline that stands for what the deleted
// rows left), and the state rows whose deadline is at or before it (not holding at any
// instant from the horizon on); then, in the arm with commits, it commits. The earlier
// commits still hold what it deleted, so the server frees no history by it. A horizon
// before the range, or one that does not move the horizon, changes nothing. One after the
// range discards every record but each key's newest, and refuses every later one, as the
// reference engine does.
func (e *Engine) Retain(horizon time.Time) error {
	h := int64(0)
	past := horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = doltmodel.Nanos(horizon)
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.fatal != nil {
		return e.fatal
	}
	newPast, newHorizon, changed := e.w.pastEnd, e.w.horizon, false
	if past && !e.w.pastEnd {
		newPast, changed = true, true
	}
	if h > e.w.horizon {
		newHorizon, changed = h, true
	}
	if !changed {
		return nil
	}
	var counts doltmodel.Counts
	last := e.w.lastSeq
	return e.run(step{
		kind: doltmodel.RetentionCommit, token: last,
		message: func(int) string { return retainMessage(newHorizon, last) },
		apply: func(ctx context.Context, tx *sql.Tx) error {
			var err error
			counts, err = e.applyRetention(ctx, tx, h, newPast, newHorizon)
			return err
		},
		publish: func(c *Commit) {
			e.w.horizon, e.w.pastEnd = newHorizon, newPast
			e.w.expire(h)
			if c != nil {
				counts.Commits++
			}
			e.addCounts(counts)
		},
	})
}

// applyRetention sends the deletes of a retention at h and the new horizon, and returns the
// rows each deleted.
func (e *Engine) applyRetention(ctx context.Context, tx *sql.Tx, h int64, pastEnd bool, horizon int64) (doltmodel.Counts, error) {
	var c doltmodel.Counts
	del := func(text string, args ...any) (uint64, error) {
		res, err := tx.ExecContext(ctx, text, args...)
		if err != nil {
			return 0, fmt.Errorf("doltengine: retention: %.60s...: %w", text, err)
		}
		n, err := res.RowsAffected()
		return uint64(n), err
	}
	stateText, entStateText := retainState, retainEntState
	if e.f.retainDropsOpenState {
		stateText = "DELETE FROM state WHERE deadline_ns IS NULL OR deadline_ns <= ?"
		entStateText = "DELETE FROM ent_state WHERE deadline_ns IS NULL OR deadline_ns <= ?"
	}
	var err error
	if c.StateDeletes, err = del(stateText, h); err != nil {
		return c, err
	}
	n, err := del(entStateText, h)
	if err != nil {
		return c, err
	}
	c.StateDeletes += n

	logText := retainLog("log", []string{"layer", "dir", "a", "peer", "rel", "producer"}, pastEnd)
	entText := retainLog("ent_log", []string{"fp", "producer"}, pastEnd)
	args := retainLogArgs(h, pastEnd)
	if e.f.retainDropsHolding {
		// Every row before the horizon goes, the baseline too.
		logText, entText = "DELETE FROM log WHERE event_ns < ?", "DELETE FROM ent_log WHERE event_ns < ?"
		args = []any{h}
		if pastEnd {
			logText, entText, args = "DELETE FROM log", "DELETE FROM ent_log", nil
		}
	}
	if c.LogDeletes, err = del(logText, args...); err != nil {
		return c, err
	}
	if n, err = del(entText, args...); err != nil {
		return c, err
	}
	c.LogDeletes += n
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET horizon_ns = ?, past_end = ? WHERE id = 1", horizon, pastEnd); err != nil {
		return c, fmt.Errorf("doltengine: updating the meta row: %w", err)
	}
	return c, nil
}
