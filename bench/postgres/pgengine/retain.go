package pgengine

import (
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The log columns a baseline copies, which are all of them.
const logColumns = "seq, event_ns, layer, subject_kind, src, dst, rel, producer, kind, ttl_ns, through_ns, payload"

// The statements of a retention for the horizon $1 (and $2, whether it is past the
// last instant, when every record is before it).
const (
	// The baseline of a subject and a producer is the last of its records before the
	// horizon by (event time, seq), among the baseline an earlier retention left and the
	// log. Every record still in the log is at or after the earlier horizon, so it is
	// later than any baseline: new baselines are inserted, and the ones they supersede
	// are the older of each pair.
	baselineInsert = `INSERT INTO log_base (` + logColumns + `)
SELECT DISTINCT ON (subject_kind, src, dst, rel, producer) ` + logColumns + `
FROM log WHERE event_ns < $1::bigint OR $2::bool
ORDER BY subject_kind, src, dst, rel, producer, event_ns DESC, seq DESC`
	baselineTrim = `DELETE FROM log_base WHERE ctid IN (
  SELECT ctid FROM (
    SELECT ctid, row_number() OVER (PARTITION BY subject_kind, src, dst, rel, producer ORDER BY event_ns DESC, seq DESC) AS n
    FROM log_base) ranked
  WHERE n > 1)`
	logTrim = `DELETE FROM log WHERE event_ns < $1::bigint OR $2::bool`
	// A row is gone once it ends at or before the horizon: its range has ended, or it
	// is provisional and its deadline has passed. A row that spans the horizon stays as
	// it is, with its old start.
	edgesTrim    = `DELETE FROM edges WHERE ` + pgschema.RetentionPredicate
	entitiesTrim = `DELETE FROM entities WHERE ` + pgschema.RetentionPredicate
)

// Retain implements [engine.Engine]. The horizon only moves forward: a call that does
// not move it changes nothing. A horizon after the last representable instant
// discards every record and refuses every later one.
//
// The log before the horizon is replaced by baselines, one per subject and producer,
// and the interval rows that have ended are deleted, in one transaction, so a reader
// sees the tables as they were or as they are. The log rows are deleted, in every arm:
// a day partition of the streaming arm that has emptied stays. Dropping whole
// partitions is a different transaction, and one that takes a lock readers would wait
// on, so it belongs to a writer that is not read from while it retains.
func (e *Engine) Retain(horizon time.Time) error {
	h, past := int64(0), horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = pgmodel.ToNanos(horizon)
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	ctx := e.ctx
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	meta, err := lockMeta(ctx, tx)
	if err != nil {
		return err
	}
	changed := false
	if past && !meta.pastEnd {
		meta.pastEnd, changed = true, true
	}
	if h > meta.horizon {
		meta.horizon, changed = h, true
	}
	if !changed {
		return nil
	}
	if _, err := tx.Exec(ctx, "UPDATE meta SET horizon_ns = $1, past_end = $2", meta.horizon, meta.pastEnd); err != nil {
		return fmt.Errorf("pgengine: retaining: %w", err)
	}
	// With the horizon past the end every record is before it, whatever its number says.
	both := []any{meta.horizon, meta.pastEnd}
	var deleted uint64
	for _, st := range []struct {
		sql     string
		args    []any
		deleted bool // counts rows of the interval tables
	}{
		{baselineInsert, both, false},
		{baselineTrim, nil, false},
		{logTrim, both, false},
		{edgesTrim, both[:1], true},
		{entitiesTrim, both[:1], true},
	} {
		tag, err := tx.Exec(ctx, st.sql, st.args...)
		if err != nil {
			return fmt.Errorf("pgengine: retaining: %.40s...: %w", st.sql, err)
		}
		if st.deleted {
			deleted += uint64(tag.RowsAffected())
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgengine: committing a retention: %w", err)
	}
	e.counts.RowsDeleted += deleted
	return nil
}
