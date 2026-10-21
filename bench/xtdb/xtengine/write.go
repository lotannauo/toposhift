package xtengine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// writeState is what Write reads of the engine's state, under its lock.
type writeState struct {
	lastSeq uint64
	ordinal uint64
	horizon horizonState
}

func (e *Engine) writeState() writeState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return writeState{lastSeq: e.lastSeq, ordinal: e.ordinal, horizon: horizonState{e.horizon, e.pastEnd}}
}

// check validates a batch whole, so that a refused batch changes nothing: every
// record valid, none before the retention horizon, sequence numbers above the last
// written and ascending, and every subject in the layer it was stored in.
func (e *Engine) check(batch []engine.Record, st writeState) error {
	seq := st.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if e.mut != mutAcceptBeforeHorizon && (st.horizon.pastEnd || r.EventTime.UnixNano() < st.horizon.ns) {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := inBatch[r.Subject]
		if !ok {
			known, ok = e.layers[r.Subject]
		}
		if ok && known != r.Layer {
			return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}
	return nil
}

// Write implements [engine.Engine]. The batch is validated whole, planned, and then
// stored by one transaction at the system time of the next ordinal. The planner, the
// counters and LastSeq change only after the transaction has committed, so a batch
// that is refused uses neither an ordinal nor a sequence number.
//
// A batch with a boundary that is not a whole microsecond is refused whole with
// [engine.ErrInvalid] and counted ([Engine.Refused]): XTDB's temporal columns hold
// microseconds.
func (e *Engine) Write(batch []engine.Record) error {
	if len(batch) == 0 {
		return nil
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.broken != nil {
		return fmt.Errorf("xtengine: an earlier transaction ended in an unknown state, so the engine is not written to again: %w", e.broken)
	}
	ctx, cancel := e.writeContext()
	defer cancel()
	e.wctx = ctx
	defer func() { e.wctx = e.ctx }()
	st := e.writeState()
	if err := e.check(batch, st); err != nil {
		return err
	}
	n := st.ordinal + 1
	pl, err := e.pl.Plan(n, batch)
	if err != nil {
		return err
	}
	if pl.Counts.OffMicrosecond > 0 && e.mut != mutWriteOffMicrosecond {
		e.refused.OffMicrosecond += pl.Counts.OffMicrosecond
		return fmt.Errorf("xtengine: %d records of the batch have a boundary that is not a whole microsecond: %w", pl.Counts.OffMicrosecond, engine.ErrInvalid)
	}
	sys := e.sysTime(n)
	if e.mut == mutRepeatSystemTime && st.ordinal > 0 {
		sys = e.sysTime(st.ordinal)
	}
	stmts := make([]stmt, 0, len(pl.Txn.Ops))
	for _, o := range pl.Txn.Ops {
		s, err := e.encode(o, sys, st.horizon)
		if err != nil {
			return err
		}
		stmts = append(stmts, s)
	}
	last := batch[len(batch)-1].Seq
	if e.mut == mutLastSeqEarly {
		e.mu.Lock()
		e.lastSeq = last
		e.mu.Unlock()
	}
	if err := e.transact(ctx, n, sys, stmts, last); err != nil {
		if e.mut == mutFailedBatchTakesOrdinal {
			e.mu.Lock()
			e.ordinal = n
			e.mu.Unlock()
		}
		return err
	}
	e.pl.Accept(pl)
	e.counts.Add(pl.Counts)
	e.mu.Lock()
	e.ordinal = n
	e.lastSeq = last
	e.tokens[last] = n // the newest transaction at this sequence number: a Retain may follow
	for _, r := range batch {
		e.layers[r.Subject] = r.Layer
	}
	e.mu.Unlock()
	return nil
}

// writeContext is the context of one write: the engine's, with the deadline every write has,
// so that a commit that hangs is an error and not a wait for ever.
func (e *Engine) writeContext() (context.Context, context.CancelFunc) {
	if e.mut == mutNoDeadline {
		return context.WithCancel(e.ctx)
	}
	return context.WithTimeout(e.ctx, e.writeTimeout)
}

// transact stores a transaction and reads back its meta document: the node's commit having
// returned is not enough, because an indexer that has failed can leave a commit that says
// nothing of it, and the engine's state would then run ahead of the node's. The document of
// ordinal n, as of the transaction's own system time, must be there with the sequence number
// the transaction ended at; if it is not, or if the write ran out of time, the outcome is
// unknown and the engine is not written to again.
func (e *Engine) transact(ctx context.Context, n uint64, sys xtmodel.Instant, stmts []stmt, last uint64) error {
	err := e.commit(ctx, sys, stmts)
	if err == nil && e.mut != mutNoReadBack {
		err = e.verifyMeta(ctx, n, sys, last)
	}
	if err != nil && ctx.Err() != nil && e.broken == nil {
		e.broken = err
	}
	return err
}

// verifyMeta reads the meta document of ordinal n at the system time sys.
func (e *Engine) verifyMeta(ctx context.Context, n uint64, sys xtmodel.Instant, last uint64) error {
	var q sqlText
	q.s("SELECT last_seq FROM " + e.names.of(xtmodel.Meta) + " FOR VALID_TIME ALL FOR SYSTEM_TIME AS OF ").p(systemTimestamp(sys)).s(" WHERE _id = ").p(metaID(n))
	var got []int64
	err := scanRows(ctx, e.w, q.stmt(), func(rows pgx.Rows) error {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		got = append(got, v)
		return nil
	})
	switch {
	case err != nil:
		err = fmt.Errorf("xtengine: reading back the meta document of transaction %d: %w", n, err)
	case len(got) != 1 || seqFromDB(got[0]) != last:
		err = fmt.Errorf("xtengine: transaction %d at %s committed, and its meta document is not there with last seq %d (found %d): the node's indexer may have failed", n, sys, last, len(got))
	default:
		return nil
	}
	e.broken = err
	return err
}

// commit stores a transaction: the statements, in order, between the begin at the
// system time and the commit. XTDB's commit waits for the transaction to be indexed,
// so the writer's later reads, and every other connection's, see it. A failure before
// the commit is rolled back and changes nothing on the node (the node holds the
// statements back until the commit). A failure of the commit itself that the node
// reports is an abort: the transaction is in its log, with no effect. Any other
// failure of the commit leaves its outcome unknown, and the engine is not written to
// again.
func (e *Engine) commit(ctx context.Context, sys xtmodel.Instant, stmts []stmt) error {
	if err := control(ctx, e.w, beginWrite(sys)); err != nil {
		return fmt.Errorf("xtengine: opening a transaction at %s: %w", sys, err)
	}
	for i, s := range stmts {
		if _, err := e.w.Exec(ctx, s.sql, s.args...); err != nil {
			err = fmt.Errorf("xtengine: statement %d of %d (%.60s): %w", i+1, len(stmts), s.sql, err)
			return e.abandon(ctx, err)
		}
	}
	if err := control(ctx, e.w, commitTx); err != nil {
		err = fmt.Errorf("xtengine: committing a transaction at %s: %w", sys, err)
		var server *pgconn.PgError
		if errors.As(err, &server) {
			return e.abandon(ctx, err)
		}
		e.broken = err
		return err
	}
	return nil
}

// abandon rolls the open transaction back after err. If the node does not take the
// rollback either, the connection is in an unknown state and the engine is not written
// to again.
func (e *Engine) abandon(ctx context.Context, err error) error {
	if rerr := control(ctx, e.w, rollbackTx); rerr != nil {
		e.broken = errors.Join(err, rerr)
		return e.broken
	}
	return err
}

// control runs a statement that takes no argument, as text: the driver's simple
// protocol, whatever the mode of the engine.
func control(ctx context.Context, c conn, sql string) error {
	_, err := c.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

// Retain implements [engine.Engine]. XTDB's only removal is ERASE, which removes a
// document for all valid and system time, so one document's old valid-time portions
// cannot be dropped while its later ones stay: retention is not mapped. Retain records
// the horizon, from which Write refuses records before it, and frees nothing, which
// the contract allows. A horizon before the range changes nothing; one after the range
// refuses every later record, as the reference engine does.
//
// A Retain that moves the horizon is a transaction of its own, at the system time of the
// next ordinal, that writes one meta document: the horizon and the sequence number the last
// batch ended at, which it does not change. Ordinals count batches and retentions alike. The
// token of that sequence number then names the newest of the two transactions, so a read at it
// sees the state after the retention, which is the same data, and the horizon is on the node
// when Retain returns, so an engine opened afterwards has it. A Retain that does not move the
// horizon writes nothing.
func (e *Engine) Retain(horizon time.Time) error {
	h := int64(0)
	past := horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = horizon.UnixNano()
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.broken != nil {
		return fmt.Errorf("xtengine: an earlier transaction ended in an unknown state, so the engine is not written to again: %w", e.broken)
	}
	st := e.writeState()
	next := horizonState{ns: max(st.horizon.ns, h), pastEnd: st.horizon.pastEnd || past}
	if next == st.horizon {
		return nil
	}
	if e.mut == mutRetainNoTransaction {
		e.mu.Lock()
		e.horizon, e.pastEnd = next.ns, next.pastEnd
		e.mu.Unlock()
		return nil
	}
	ctx, cancel := e.writeContext()
	defer cancel()
	n := st.ordinal + 1
	sys := e.sysTime(n)
	s := e.metaStmt(n, metaRetain, st.lastSeq, sys, next)
	if err := e.transact(ctx, n, sys, []stmt{s}, st.lastSeq); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ordinal = n
	e.horizon, e.pastEnd = next.ns, next.pastEnd
	if st.lastSeq > 0 {
		e.tokens[st.lastSeq] = n
	}
	return nil
}

// history answers the planner's questions about the committed batches from the log
// tables, on the writer's connection.
type history struct{ e *Engine }

// Next implements [xtmodel.History].
func (h history) Next(s engine.Subject, p lifecycle.Producer, after int64) (int64, bool, error) {
	var q sqlText
	if s.Kind == engine.SubjectEdge {
		q.s("SELECT min(event_ns) FROM " + h.e.names.of(xtmodel.LogF) + " FOR VALID_TIME ALL WHERE fp = ").p(s.A.String()).
			s(" AND peer = ").p(s.B.String()).s(" AND rel = ").p(string(s.Relation))
	} else {
		q.s("SELECT min(event_ns) FROM " + h.e.names.of(xtmodel.LogE) + " FOR VALID_TIME ALL WHERE fp = ").p(s.A.String())
	}
	q.s(" AND producer = ").p(string(p)).s(" AND event_ns > ").p(after)
	rows, err := h.e.w.Query(h.e.wctx, q.b.String(), q.args...)
	if err != nil {
		return 0, false, fmt.Errorf("xtengine: the next assertion: %w", err)
	}
	defer rows.Close()
	var next *int64
	for rows.Next() {
		if err := rows.Scan(&next); err != nil {
			return 0, false, fmt.Errorf("xtengine: the next assertion: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("xtengine: the next assertion: %w", err)
	}
	if next == nil {
		return 0, false, nil
	}
	return *next, true, nil
}
