package doltengine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

// queryer is what a read of a whole table needs of a transaction.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Tables returns every table as it is now, as the rows of the model's types in the order of
// the primary keys, so that the engine's tables can be held to a [doltmodel.Model]'s
// ([doltmodel.Tables.FirstDifference]). It reads a snapshot, and holds the engine's view still.
func (e *Engine) Tables(ctx context.Context) (doltmodel.Tables, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	tx, err := e.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return doltmodel.Tables{}, fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var t doltmodel.Tables
	var pastEnd bool
	if err := tx.QueryRowContext(ctx, "SELECT last_seq, horizon_ns, past_end, schema_version FROM meta WHERE id = 1").
		Scan(&t.Meta.LastSeq, &t.Meta.HorizonNs, &pastEnd, &t.Meta.SchemaVersion); err != nil {
		return t, fmt.Errorf("doltengine: reading the meta row: %w", err)
	}
	t.Meta.PastEnd = pastEnd
	if t.Log, err = readLog(ctx, tx); err != nil {
		return t, err
	}
	if t.EntLog, err = readEntLog(ctx, tx); err != nil {
		return t, err
	}
	if t.State, err = readState(ctx, tx, ""); err != nil {
		return t, err
	}
	t.EntState, err = readEntState(ctx, tx, "")
	return t, err
}

// StateAt returns the state tables as the commit with the ordinal left them, which is what a
// read as of that commit sees ([doltmodel.Model.StateAt] is the model's). It is an error in
// the arm with no commits, or for an ordinal that is no commit's.
func (e *Engine) StateAt(ctx context.Context, ordinal int) ([]doltmodel.StateRow, []doltmodel.EntStateRow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.arm != doltmodel.ArmCommit {
		return nil, nil, fmt.Errorf("doltengine: the %s arm has no commits", e.arm)
	}
	if ordinal < 1 || ordinal > len(e.commits) {
		return nil, nil, fmt.Errorf("doltengine: no commit %d among %d", ordinal, len(e.commits))
	}
	asOf, err := asOfClause(e.commits[ordinal-1].Hash)
	if err != nil {
		return nil, nil, err
	}
	tx, err := e.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	state, err := readState(ctx, tx, asOf)
	if err != nil {
		return nil, nil, err
	}
	ents, err := readEntState(ctx, tx, asOf)
	return state, ents, err
}

func readLog(ctx context.Context, q queryer) ([]doltmodel.LogRow, error) {
	rows, err := q.QueryContext(ctx, "SELECT layer, dir, a, peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload FROM log ORDER BY layer, dir, a, peer, rel, producer, event_ns, seq")
	if err != nil {
		return nil, fmt.Errorf("doltengine: reading the log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []doltmodel.LogRow
	for rows.Next() {
		var r doltmodel.LogRow
		var a, peer, rel, producer, kind, payload []byte
		var through sql.NullInt64
		if err := rows.Scan(&r.Layer, &r.Dir, &a, &peer, &rel, &producer, &r.EventNs, &r.Seq, &kind, &r.TTLNs, &through, &payload); err != nil {
			return nil, fmt.Errorf("doltengine: reading the log: %w", err)
		}
		r.A, r.Peer, r.Rel, r.Producer, r.Kind, r.Payload = string(a), string(peer), string(rel), string(producer), string(kind), string(payload)
		r.HasThrough, r.ThroughNs = through.Valid, through.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}

func readEntLog(ctx context.Context, q queryer) ([]doltmodel.EntLogRow, error) {
	rows, err := q.QueryContext(ctx, "SELECT fp, producer, layer, event_ns, seq, kind, ttl_ns, through_ns, payload FROM ent_log ORDER BY fp, producer, event_ns, seq")
	if err != nil {
		return nil, fmt.Errorf("doltengine: reading the entity log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []doltmodel.EntLogRow
	for rows.Next() {
		var r doltmodel.EntLogRow
		var fp, producer, kind, payload []byte
		var through sql.NullInt64
		if err := rows.Scan(&fp, &producer, &r.Layer, &r.EventNs, &r.Seq, &kind, &r.TTLNs, &through, &payload); err != nil {
			return nil, fmt.Errorf("doltengine: reading the entity log: %w", err)
		}
		r.Fp, r.Producer, r.Kind, r.Payload = string(fp), string(producer), string(kind), string(payload)
		r.HasThrough, r.ThroughNs = through.Valid, through.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}

func readState(ctx context.Context, q queryer, asOf string) ([]doltmodel.StateRow, error) {
	rows, err := q.QueryContext(ctx, "SELECT layer, dir, a, peer, rel, producer, event_ns, seq, kind, deadline_ns FROM state"+asOf+" ORDER BY layer, dir, a, peer, rel, producer")
	if err != nil {
		return nil, fmt.Errorf("doltengine: reading the state: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []doltmodel.StateRow
	for rows.Next() {
		var r doltmodel.StateRow
		var a, peer, rel, producer, kind []byte
		var deadline sql.NullInt64
		if err := rows.Scan(&r.Layer, &r.Dir, &a, &peer, &rel, &producer, &r.EventNs, &r.Seq, &kind, &deadline); err != nil {
			return nil, fmt.Errorf("doltengine: reading the state: %w", err)
		}
		r.A, r.Peer, r.Rel, r.Producer, r.Kind = string(a), string(peer), string(rel), string(producer), string(kind)
		r.Open, r.DeadlineNs = !deadline.Valid, deadline.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}

func readEntState(ctx context.Context, q queryer, asOf string) ([]doltmodel.EntStateRow, error) {
	rows, err := q.QueryContext(ctx, "SELECT fp, producer, layer, event_ns, seq, kind, deadline_ns FROM ent_state"+asOf+" ORDER BY fp, producer")
	if err != nil {
		return nil, fmt.Errorf("doltengine: reading the entity state: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []doltmodel.EntStateRow
	for rows.Next() {
		var r doltmodel.EntStateRow
		var fp, producer, kind []byte
		var deadline sql.NullInt64
		if err := rows.Scan(&fp, &producer, &r.Layer, &r.EventNs, &r.Seq, &kind, &deadline); err != nil {
			return nil, fmt.Errorf("doltengine: reading the entity state: %w", err)
		}
		r.Fp, r.Producer, r.Kind = string(fp), string(producer), string(kind)
		r.Open, r.DeadlineNs = !deadline.Valid, deadline.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}
