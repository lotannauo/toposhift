package pgengine

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

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
// sees the tables as they were or as they are. The log rows are deleted in the same
// transaction: this is the correctness writer's retention, for the bulk arms, and the
// retention a bulk load applies (retainTx). The streaming arms retain with
// retainStream, which leaves the log's old day partitions to be detached and dropped
// after the transaction, and is the retention that is measured.
func (e *Engine) Retain(horizon time.Time) error {
	if e.lay.stream {
		return e.retainStream(horizon)
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	ctx := e.ctx
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rs, err := retainTx(ctx, tx, horizon)
	if err != nil || !rs.changed {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgengine: committing a retention: %w", err)
	}
	e.counts.RowsDeleted += rs.intervalRows
	return nil
}

// retainStats is what a retention did: whether it changed anything, the meta row as it
// left it, and the rows each statement inserted or deleted.
type retainStats struct {
	changed bool
	meta    metaState
	// baselinesInserted and baselinesTrimmed are the rows of log_base the retention
	// added and the superseded ones it removed; logDeleted the rows of the log; and
	// intervalRows the rows of the interval tables.
	baselinesInserted, baselinesTrimmed, logDeleted, intervalRows uint64
}

// retainTx is the retention in the transaction of the connection whose search path is
// the arm's schema: the engine's, and a bulk load's, which is the same statements on
// the same tables. The horizon only moves forward, so a retention that does not move
// it changes nothing. The caller commits.
func retainTx(ctx context.Context, tx pgx.Tx, horizon time.Time) (retainStats, error) {
	h, past := int64(0), horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = pgmodel.ToNanos(horizon)
	}
	meta, err := lockMeta(ctx, tx)
	if err != nil {
		return retainStats{}, err
	}
	rs := retainStats{meta: meta}
	if past && !meta.pastEnd {
		rs.meta.pastEnd, rs.changed = true, true
	}
	if h > meta.horizon {
		rs.meta.horizon, rs.changed = h, true
	}
	if !rs.changed {
		return rs, nil
	}
	if _, err := tx.Exec(ctx, "UPDATE meta SET horizon_ns = $1, past_end = $2", rs.meta.horizon, rs.meta.pastEnd); err != nil {
		return retainStats{}, fmt.Errorf("pgengine: retaining: %w", err)
	}
	// With the horizon past the end every record is before it, whatever its number says.
	both := []any{rs.meta.horizon, rs.meta.pastEnd}
	for _, st := range []struct {
		sql  string
		args []any
		add  *uint64
	}{
		{baselineInsert, both, &rs.baselinesInserted},
		{baselineTrim, nil, &rs.baselinesTrimmed},
		{logTrim, both, &rs.logDeleted},
		{edgesTrim, both[:1], &rs.intervalRows},
		{entitiesTrim, both[:1], &rs.intervalRows},
	} {
		tag, err := tx.Exec(ctx, st.sql, st.args...)
		if err != nil {
			return retainStats{}, fmt.Errorf("pgengine: retaining: %.40s...: %w", st.sql, err)
		}
		*st.add += uint64(tag.RowsAffected())
	}
	return rs, nil
}
