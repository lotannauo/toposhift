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
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// querier is what a read runs its statements on: the pool, for a statement that is
// its own snapshot, or the transaction a read that needs a token runs in.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// readOnlyRepeatable is the transaction of a read that needs one snapshot.
var readOnlyRepeatable = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

// maxInt64 is the key an open interval has in index set B.
const maxInt64 = "9223372036854775807"

// live is the condition that a row contains the instant $3, which is every read's
// third parameter. Each arm states it as its index set can answer it:
//
//   - the streaming arms have GiST on the interval and rows that may be provisional:
//     the range contains the instant, and a provisional row (a deadline, and a range
//     that is unbounded) ends at its deadline;
//   - the bulk arm with index set G has the same GiST index and no provisional rows;
//   - the bulk arm with index set B has a btree on the end, an open interval ending at
//     the largest bigint. A row is alive after t if it ends after t and began at or
//     before it. At the last instant nothing ends after it and only an open row
//     contains it, so that case reads the open rows.
func (l layout) live(t int64) string {
	switch l.arm {
	case pgschema.BulkB:
		if t == math.MaxInt64 {
			return "upper_inf(during) AND lower(during) <= $3::bigint"
		}
		return "coalesce(upper(during), " + maxInt64 + ") > $3::bigint AND lower(during) <= $3::bigint"
	case pgschema.Bulk:
		return "during @> $3::bigint"
	}
	return "during @> $3::bigint AND (deadline_ns IS NULL OR $3::bigint < deadline_ns)"
}

// The columns a direction reads: the entity asked about, and its peer.
func columns(dir engine.Direction) (key, peer string) {
	if dir == engine.Reverse {
		return "dst", "src"
	}
	return "src", "dst"
}

// NeighborsSQL is the statement a neighbors read at the instant t, in Unix
// nanoseconds, runs on the latest token: its parameters are the entity (an encoded
// fingerprint), the layer (a smallint) and the instant. It is exported for EXPLAIN.
func (e *Engine) NeighborsSQL(dir engine.Direction, t int64) string {
	return e.lay.neighborsSQL(dir, t)
}

// BatchSQL is the statement a batched neighbors read runs: its parameters are an array
// of encoded fingerprints, the layer and the instant.
func (e *Engine) BatchSQL(dir engine.Direction, t int64) string { return e.lay.batchSQL(dir, t) }

// AliveSQL is the statement an existence read runs: the entity, the layer and the
// instant.
func (e *Engine) AliveSQL(t int64) string { return e.lay.aliveSQL(t) }

// WindowSQL is the statement a window read runs: the entity, the layer, the first
// instant, the last (left out if unbounded) and the token, as a bigint of the stored
// form of a sequence number.
func WindowSQL(dir engine.Direction, unbounded bool) string { return windowSQL(dir, unbounded) }

func (l layout) neighborsSQL(dir engine.Direction, t int64) string {
	key, peer := columns(dir)
	return "SELECT " + peer + ", rel FROM edges WHERE " + key + " = $1 AND layer = $2::smallint AND " + l.live(t) +
		" ORDER BY " + peer + ", rel"
}

// batchSQL reads the neighbors of many entities in one statement. LEFT JOIN LATERAL
// keeps an entity with no edges as a row with a null peer, which is an empty list.
func (l layout) batchSQL(dir engine.Direction, t int64) string {
	key, peer := columns(dir)
	return `SELECT f.ord, n.peer, n.rel FROM unnest($1::bytea[]) WITH ORDINALITY AS f(fp, ord)
LEFT JOIN LATERAL (SELECT ` + peer + ` AS peer, rel FROM edges WHERE ` + key + ` = f.fp AND layer = $2::smallint AND ` + l.live(t) + `) n ON true
ORDER BY f.ord, n.peer, n.rel`
}

func (l layout) aliveSQL(t int64) string {
	return "SELECT EXISTS (SELECT 1 FROM entities WHERE fp = $1 AND layer = $2::smallint AND " + l.live(t) + ")"
}

// windowSQL reads every record of the edges touching an entity in an event-time
// window and a token, overwritten ones included. The upper bound is left out for a
// window that reaches past the last instant.
func windowSQL(dir engine.Direction, unbounded bool) string {
	key, _ := columns(dir)
	hi, seq := " AND event_ns < $4::bigint", "$5::bigint"
	if unbounded {
		hi, seq = "", "$4::bigint"
	}
	return "SELECT seq, event_ns, layer, src, dst, rel, producer, kind::int4, ttl_ns, through_ns, payload FROM log WHERE " +
		key + " = $1 AND subject_kind = 'g' AND layer = $2::smallint AND event_ns >= $3::bigint" + hi +
		" AND seq <= " + seq + " ORDER BY event_ns, seq"
}

// The log of the edges touching an entity, and of an entity itself, as of a token,
// for the reads whose token is not the latest. Baselines stand for the records a
// retention dropped, so they are read with the log.
func foldNeighborsSQL(dir engine.Direction) string {
	key, peer := columns(dir)
	cols := peer + ", rel, seq, event_ns, producer, kind::int4, ttl_ns, through_ns"
	where := " WHERE subject_kind = 'g' AND " + key + " = $1 AND layer = $2::smallint AND seq <= $3::bigint"
	return "SELECT " + cols + " FROM log" + where + " UNION ALL SELECT " + cols + " FROM log_base" + where + " ORDER BY 1, 2"
}

const foldEntitySQL = `SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns FROM log
  WHERE subject_kind = 'e' AND src = $1 AND layer = $2::smallint AND seq <= $3::bigint
UNION ALL
SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns FROM log_base
  WHERE subject_kind = 'e' AND src = $1 AND layer = $2::smallint AND seq <= $3::bigint`

// read runs f with the statements of one snapshot. current says the interval tables
// are as current as the token asks: the token is at or above the last committed
// sequence number. A read at the latest token is one statement and needs no more. Any
// other token has to be compared with the last sequence number of the snapshot the
// statements read, so it runs in one repeatable-read transaction that reads that first.
func (e *Engine) read(asOf uint64, f func(q querier, current bool) error) error {
	if asOf == engine.Latest {
		return f(e.pool, true)
	}
	tx, err := e.pool.BeginTx(e.ctx, readOnlyRepeatable)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(e.ctx) }()
	var last int64
	if err := tx.QueryRow(e.ctx, "SELECT last_seq FROM meta").Scan(&last); err != nil {
		return fmt.Errorf("pgengine: reading the token: %w", err)
	}
	return f(tx, asOf >= seqFromDB(last))
}

func encode(fp identity.Fingerprint) []byte { return pgmodel.EncodeFingerprint(fp) }

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := pgmodel.ClampInstant(t)
	if !ok {
		return nil, nil
	}
	var out []engine.Neighbor
	err := e.read(sc.AsOf, func(q querier, current bool) (err error) {
		out, err = e.neighbors(q, current, fp, dir, ns, sc)
		return err
	})
	return out, err
}

func (e *Engine) neighbors(q querier, current bool, fp identity.Fingerprint, dir engine.Direction, t int64, sc engine.Scope) ([]engine.Neighbor, error) {
	if !current {
		return e.foldNeighbors(q, fp, dir, t, sc)
	}
	rows, err := q.Query(e.ctx, e.lay.neighborsSQL(dir, t), encode(fp), int16(sc.Layer), t)
	if err != nil {
		return nil, fmt.Errorf("pgengine: neighbors: %w", err)
	}
	defer rows.Close()
	var out []engine.Neighbor
	for rows.Next() {
		var peer []byte
		var rel string
		if err := rows.Scan(&peer, &rel); err != nil {
			return nil, fmt.Errorf("pgengine: neighbors: %w", err)
		}
		n, err := neighbor(peer, rel)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: neighbors: %w", err)
	}
	return out, nil
}

func neighbor(peer []byte, rel string) (engine.Neighbor, error) {
	p, err := pgmodel.DecodeFingerprint(peer)
	if err != nil {
		return engine.Neighbor{}, err
	}
	return engine.Neighbor{Peer: p, Relation: catalog.RelationType(rel)}, nil
}

// foldNeighbors answers a read as of an earlier token: the intervals carry no ingest
// time, so the log of every edge that touches the entity is read as the token saw it,
// and each edge is folded by the reader.
func (e *Engine) foldNeighbors(q querier, fp identity.Fingerprint, dir engine.Direction, t int64, sc engine.Scope) ([]engine.Neighbor, error) {
	rows, err := q.Query(e.ctx, foldNeighborsSQL(dir), encode(fp), int16(sc.Layer), seqToDB(sc.AsOf))
	if err != nil {
		return nil, fmt.Errorf("pgengine: neighbors as of seq %d: %w", sc.AsOf, err)
	}
	defer rows.Close()
	var out []engine.Neighbor
	var cur *engine.Neighbor
	var group []pgmodel.LogRow
	flush := func() error {
		if cur == nil {
			return nil
		}
		alive, err := pgmodel.AliveAt(group, t)
		if err != nil {
			return err
		}
		if alive {
			out = append(out, *cur)
		}
		cur, group = nil, group[:0]
		return nil
	}
	for rows.Next() {
		var peer []byte
		var rel string
		var row pgmodel.LogRow
		var seq, ttl int64
		var through *int64
		var producer string
		var kind int32
		if err := rows.Scan(&peer, &rel, &seq, &row.Event, &producer, &kind, &ttl, &through); err != nil {
			return nil, fmt.Errorf("pgengine: neighbors as of seq %d: %w", sc.AsOf, err)
		}
		row.Seq, row.Producer, row.Kind, row.TTL = seqFromDB(seq), lifecycle.Producer(producer), lifecycle.Kind(kind), time.Duration(ttl)
		if through != nil {
			row.Through, row.HasThrough = *through, true
		}
		n, err := neighbor(peer, rel)
		if err != nil {
			return nil, err
		}
		if cur != nil && *cur != n {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		cur = &n
		group = append(group, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: neighbors as of seq %d: %w", sc.AsOf, err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// NeighborsBatch implements [engine.Engine]. One statement, or one transaction for
// an earlier token, covers the whole call, so all the lists are of one snapshot.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	out := make([][]engine.Neighbor, len(fps))
	ns, ok := pgmodel.ClampInstant(t)
	if !ok || len(fps) == 0 {
		return out, nil
	}
	err := e.read(sc.AsOf, func(q querier, current bool) error {
		if !current {
			for i, fp := range fps {
				var err error
				if out[i], err = e.foldNeighbors(q, fp, dir, ns, sc); err != nil {
					return err
				}
			}
			return nil
		}
		keys := make([][]byte, len(fps))
		for i, fp := range fps {
			keys[i] = encode(fp)
		}
		rows, err := q.Query(e.ctx, e.lay.batchSQL(dir, ns), keys, int16(sc.Layer), ns)
		if err != nil {
			return fmt.Errorf("pgengine: neighbors batch: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var ord int64
			var peer []byte
			var rel *string
			if err := rows.Scan(&ord, &peer, &rel); err != nil {
				return fmt.Errorf("pgengine: neighbors batch: %w", err)
			}
			if peer == nil {
				continue // an entity with no edge alive: an empty list
			}
			if rel == nil || ord < 1 || ord > int64(len(fps)) {
				return fmt.Errorf("pgengine: neighbors batch: a row with ordinal %d and relation %v", ord, rel)
			}
			n, err := neighbor(peer, *rel)
			if err != nil {
				return err
			}
			out[ord-1] = append(out[ord-1], n)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("pgengine: neighbors batch: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Alive implements [engine.Engine]. The entity is in the layer of the scope: the
// same fingerprint in another layer is not alive here.
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := pgmodel.ClampInstant(t)
	if !ok {
		return false, nil
	}
	var alive bool
	err := e.read(sc.AsOf, func(q querier, current bool) error {
		if current {
			if err := q.QueryRow(e.ctx, e.lay.aliveSQL(ns), encode(fp), int16(sc.Layer), ns).Scan(&alive); err != nil {
				return fmt.Errorf("pgengine: alive: %w", err)
			}
			return nil
		}
		// An entity's own records, as the token saw them, and not the records of the
		// edges that touch it.
		rows, err := q.Query(e.ctx, foldEntitySQL, encode(fp), int16(sc.Layer), seqToDB(sc.AsOf))
		if err != nil {
			return fmt.Errorf("pgengine: alive as of seq %d: %w", sc.AsOf, err)
		}
		defer rows.Close()
		var input []pgmodel.LogRow
		for rows.Next() {
			var row pgmodel.LogRow
			var seq, ttl int64
			var through *int64
			var producer string
			var kind int32
			if err := rows.Scan(&seq, &row.Event, &producer, &kind, &ttl, &through); err != nil {
				return fmt.Errorf("pgengine: alive as of seq %d: %w", sc.AsOf, err)
			}
			row.Seq, row.Producer, row.Kind, row.TTL = seqFromDB(seq), lifecycle.Producer(producer), lifecycle.Kind(kind), time.Duration(ttl)
			if through != nil {
				row.Through, row.HasThrough = *through, true
			}
			input = append(input, row)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("pgengine: alive as of seq %d: %w", sc.AsOf, err)
		}
		alive, err = pgmodel.AliveAt(input, ns)
		return err
	})
	return alive, err
}

// Window implements [engine.Engine]. It reads the log, the table every record is
// kept in, and no interval; a statement is its own snapshot, and the token is a
// column of the log, so it needs no transaction.
func (e *Engine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := pgmodel.ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	args := []any{encode(fp), int16(sc.Layer), b.Lo}
	if !b.Unbounded {
		args = append(args, b.Hi)
	}
	args = append(args, seqToDB(sc.AsOf))
	rows, err := e.pool.Query(e.ctx, windowSQL(dir, b.Unbounded), args...)
	if err != nil {
		return nil, fmt.Errorf("pgengine: window: %w", err)
	}
	defer rows.Close()
	var out []engine.Record
	for rows.Next() {
		var seq, event, ttl int64
		var layer int16
		var src, dst, payload []byte
		var rel, producer string
		var kind int32
		var through *int64
		if err := rows.Scan(&seq, &event, &layer, &src, &dst, &rel, &producer, &kind, &ttl, &through, &payload); err != nil {
			return nil, fmt.Errorf("pgengine: window: %w", err)
		}
		a, err := pgmodel.DecodeFingerprint(src)
		if err != nil {
			return nil, err
		}
		z, err := pgmodel.DecodeFingerprint(dst)
		if err != nil {
			return nil, err
		}
		r := engine.Record{
			Layer: catalog.Layer(layer), Subject: engine.EdgeSubject(a, z, catalog.RelationType(rel)),
			Producer: lifecycle.Producer(producer), EventTime: pgmodel.FromNanos(event), Seq: seqFromDB(seq),
			Kind: lifecycle.Kind(kind), TTL: time.Duration(ttl), Payload: payload,
		}
		if through != nil {
			r.Through = pgmodel.FromNanos(*through)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: window: %w", err)
	}
	return out, nil
}
