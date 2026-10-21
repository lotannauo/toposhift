package xtengine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ReadCounts is how the reads an engine has answered were answered.
type ReadCounts struct {
	// Native are the entity reads (one per entity of a batched read) answered by the
	// segment or log tables of the node at a system time or at the newest.
	Native uint64
	// ClientFold are the reads at a token inside a batch, which no system time names:
	// they are answered from the log tables by folding them in Go.
	ClientFold uint64
}

// readCounters are the atomics behind ReadCounts.
type readCounters struct{ native, folded atomic.Uint64 }

// ReadCounts returns how the reads so far were answered.
func (e *Engine) ReadCounts() ReadCounts {
	return ReadCounts{Native: e.reads.native.Load(), ClientFold: e.reads.folded.Load()}
}

// viewKind says how a read sees ingest time.
type viewKind uint8

const (
	// viewLatest is a read of the newest system time, with no clause for it.
	viewLatest viewKind = iota
	// viewSystem is a read FOR SYSTEM_TIME AS OF the system time of a batch.
	viewSystem
	// viewFold is a read at a token that is inside a batch, or before the first: no
	// system time is that token's, so the log tables are read and folded in Go.
	viewFold
)

// view is the ingest-time scope of a read.
type view struct {
	kind viewKind
	sys  xtmodel.Instant
}

// resolve says how a token is read. engine.Latest is the newest system time. A token
// that ended a batch is that batch's system time, and so is any token at or above the
// last one (it sees every batch committed, and no later one may appear in the
// answer). Any other token is inside a batch or before the first.
func (e *Engine) resolve(asOf uint64) view {
	e.mu.RLock()
	defer e.mu.RUnlock()
	switch {
	case e.mut == mutNoSystemTime || asOf == engine.Latest || e.ordinal == 0:
		return view{kind: viewLatest}
	case e.mut == mutMidBatchBySystemTime:
		var n uint64
		var best uint64
		for tok, ord := range e.tokens {
			if tok <= asOf && tok >= best {
				best, n = tok, ord
			}
		}
		return view{kind: viewSystem, sys: e.sysTime(n)}
	}
	if n, ok := e.tokens[asOf]; ok {
		return view{kind: viewSystem, sys: e.sysTime(n)}
	}
	if asOf >= e.lastSeq {
		return view{kind: viewSystem, sys: e.sysTime(e.ordinal)}
	}
	return view{kind: viewFold}
}

// outOfDate is the error the node gives a query that it planned against the columns a
// table had, and ran against more: the result types it planned for are not those it found
// (xtdb/pgwire.clj, bind-stmt). It happens to a read that races a write that gives a table
// a column or a type it did not have, and the read succeeds when it is made again, against
// the table as it now is.
func outOfDate(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && strings.Contains(pe.Message+pe.Detail, "cached plan must not change result type")
}

// maxReadAttempts is how many times a read that finds its plan out of date is made.
const maxReadAttempts = 5

// read runs f on the reader's connection, in one read-only transaction, so that all of
// its statements see one snapshot of whole committed batches. A read whose plan the node
// found out of date is made again, from its transaction's begin.
func (e *Engine) read(f func(c conn) error) error {
	e.rmu.Lock()
	defer e.rmu.Unlock()
	var err error
	for attempt := 0; attempt < maxReadAttempts; attempt++ {
		if err = e.readOnce(f); err == nil || e.mut == mutNoReadRetry || !outOfDate(err) {
			return err
		}
	}
	return err
}

func (e *Engine) readOnce(f func(c conn) error) error {
	if err := control(e.ctx, e.r, beginRead); err != nil {
		return fmt.Errorf("xtengine: opening a read: %w", err)
	}
	if err := f(e.r); err != nil {
		if rerr := control(e.ctx, e.r, rollbackTx); rerr != nil {
			return errors.Join(err, rerr)
		}
		return err
	}
	if err := control(e.ctx, e.r, commitTx); err != nil {
		return fmt.Errorf("xtengine: closing a read: %w", err)
	}
	return nil
}

// asOfClauses writes the temporal clauses of a read of the segment tables: valid time
// at an instant, and system time at a batch's unless the read is of the newest.
func (e *Engine) asOfClauses(q *sqlText, at time.Time, v view) {
	q.s(" FOR VALID_TIME AS OF ").p(at)
	if v.kind == viewSystem {
		q.s(" FOR SYSTEM_TIME AS OF ").p(systemTimestamp(v.sys))
	}
}

// layerCond writes " AND layer = $n", the layer filter.
func (e *Engine) layerCond(q *sqlText, l catalog.Layer) {
	if e.mut == mutNoLayerFilter {
		return
	}
	q.s(" AND layer = ").p(int64(l))
}

func segTable(dir engine.Direction) xtmodel.Table {
	if dir == engine.Reverse {
		return xtmodel.SegR
	}
	return xtmodel.SegF
}

func logTable(dir engine.Direction) xtmodel.Table {
	if dir == engine.Reverse {
		return xtmodel.LogR
	}
	return xtmodel.LogF
}

// scanRows runs a query and calls scan once per row.
func scanRows(ctx context.Context, c conn, q stmt, scan func(pgx.Rows) error) error {
	rows, err := c.Query(ctx, q.sql, q.args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// neighborsSQL is the native read of the edges alive at an instant that touch one entity.
func (e *Engine) neighborsSQL(dir engine.Direction, at time.Time, v view, fp identity.Fingerprint, layer catalog.Layer) stmt {
	var q sqlText
	q.s("SELECT peer, rel FROM " + e.names.of(segTable(dir)))
	e.asOfClauses(&q, at, v)
	q.s(" WHERE fp = ").p(fp.String())
	e.layerCond(&q, layer)
	return q.stmt()
}

// batchSQL is the native read of the edges alive at an instant for several entities.
func (e *Engine) batchSQL(dir engine.Direction, at time.Time, v view, fps []identity.Fingerprint, layer catalog.Layer) stmt {
	var q sqlText
	q.s("SELECT fp, peer, rel FROM " + e.names.of(segTable(dir)))
	e.asOfClauses(&q, at, v)
	q.s(" WHERE fp IN (")
	for i, fp := range fps {
		if i > 0 {
			q.s(", ")
		}
		q.p(fp.String())
	}
	q.s(")")
	e.layerCond(&q, layer)
	return q.stmt()
}

// aliveSQL is the native read of whether an entity exists at an instant.
func (e *Engine) aliveSQL(at time.Time, v view, fp identity.Fingerprint, layer catalog.Layer) stmt {
	var q sqlText
	q.s("SELECT fp FROM " + e.names.of(xtmodel.Ent))
	e.asOfClauses(&q, at, v)
	q.s(" WHERE fp = ").p(fp.String())
	e.layerCond(&q, layer)
	q.s(" LIMIT 1")
	return q.stmt()
}

// windowSQL is the read of the records of the edges touching an entity in a window of
// event time, from the log, as of a system time or, for a token no system time names,
// of a sequence number.
func (e *Engine) windowSQL(dir engine.Direction, v view, asOf uint64, fp identity.Fingerprint, layer catalog.Layer, b xtmodel.Bounds) stmt {
	var q sqlText
	q.s("SELECT layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns, payload FROM " + e.names.of(logTable(dir)) + " FOR VALID_TIME ALL")
	if v.kind == viewSystem {
		q.s(" FOR SYSTEM_TIME AS OF ").p(systemTimestamp(v.sys))
	}
	q.s(" WHERE fp = ").p(fp.String())
	e.layerCond(&q, layer)
	q.s(" AND event_ns >= ").p(b.Lo)
	if !b.Unbounded {
		q.s(" AND event_ns < ").p(b.Hi)
	}
	if v.kind == viewFold {
		q.s(" AND seq <= ").p(seqToDB(asOf))
	}
	q.s(" ORDER BY event_ns, seq")
	return q.stmt()
}

// foldNeighborsSQL reads the log of the edges touching an entity up to a token.
func (e *Engine) foldNeighborsSQL(dir engine.Direction, fp identity.Fingerprint, layer catalog.Layer, asOf uint64) stmt {
	var q sqlText
	q.s("SELECT peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns FROM " + e.names.of(logTable(dir)) + " FOR VALID_TIME ALL WHERE fp = ").p(fp.String())
	e.layerCond(&q, layer)
	q.s(" AND seq <= ").p(seqToDB(asOf))
	return q.stmt()
}

// foldAliveSQL reads the log of an entity's own records up to a token.
func (e *Engine) foldAliveSQL(fp identity.Fingerprint, layer catalog.Layer, asOf uint64) stmt {
	var q sqlText
	q.s("SELECT producer, seq, event_ns, kind, ttl_ns, through_ns FROM " + e.names.of(xtmodel.LogE) + " FOR VALID_TIME ALL WHERE fp = ").p(fp.String())
	e.layerCond(&q, layer)
	q.s(" AND seq <= ").p(seqToDB(asOf))
	return q.stmt()
}

func neighborOf(peer, rel string) (engine.Neighbor, error) {
	p, err := identity.ParseFingerprint(peer)
	if err != nil {
		return engine.Neighbor{}, fmt.Errorf("xtengine: a peer %q: %w", peer, err)
	}
	return engine.Neighbor{Peer: p, Relation: catalog.RelationType(rel)}, nil
}

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	at, ok := xtmodel.ClampInstant(t)
	if !ok {
		return nil, nil
	}
	v := e.resolve(sc.AsOf)
	var out []engine.Neighbor
	err := e.read(func(c conn) (err error) {
		out, err = e.neighbors(c, fp, dir, t, at, sc, v)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (e *Engine) neighbors(c conn, fp identity.Fingerprint, dir engine.Direction, t time.Time, at xtmodel.Instant, sc engine.Scope, v view) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	if v.kind == viewFold {
		e.reads.folded.Add(1)
		subjects, err := e.foldSubjects(c, fp, dir, t, sc)
		if err != nil {
			return nil, err
		}
		out = subjects
	} else {
		e.reads.native.Add(1)
		q := e.neighborsSQL(dir, e.floorMicros(at), v, fp, sc.Layer)
		err := scanRows(e.ctx, c, q, func(rows pgx.Rows) error {
			var peer, rel string
			if err := rows.Scan(&peer, &rel); err != nil {
				return err
			}
			n, err := neighborOf(peer, rel)
			if err != nil {
				return err
			}
			out = append(out, n)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("xtengine: neighbors: %w", err)
		}
	}
	// Several producers can make one edge alive.
	engine.SortNeighbors(out)
	return slices.Compact(out), nil
}

// foldSubjects is the client fold: the neighbors of fp alive at the instant when only
// the records with Seq at or below the token are visible, found by folding each edge's
// records of the log with the lifecycle specification.
func (e *Engine) foldSubjects(c conn, fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	type edge struct{ peer, rel string }
	var order []edge
	by := make(map[edge][]lifecycle.Assertion)
	err := scanRows(e.ctx, c, e.foldNeighborsSQL(dir, fp, sc.Layer, sc.AsOf), func(rows pgx.Rows) error {
		var k edge
		a, err := scanAssertion(rows, &k.peer, &k.rel)
		if err != nil {
			return err
		}
		if _, seen := by[k]; !seen {
			order = append(order, k)
		}
		by[k] = append(by[k], a)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("xtengine: reading the log: %w", err)
	}
	var out []engine.Neighbor
	for _, k := range order {
		tl, err := lifecycle.Fold(by[k], lifecycle.Policy{})
		if err != nil {
			return nil, fmt.Errorf("xtengine: %w", err)
		}
		if tl.AliveAt(t) {
			n, err := neighborOf(k.peer, k.rel)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
	}
	return out, nil
}

// scanAssertion reads the columns of a log row that a fold needs, after the leading
// columns the caller names: producer, seq, event_ns, kind, ttl_ns, through_ns.
func scanAssertion(rows pgx.Rows, lead ...any) (lifecycle.Assertion, error) {
	var (
		producer         string
		seq, event, kind int64
		ttl              int64
		through          *int64
	)
	dest := append(lead, &producer, &seq, &event, &kind, &ttl, &through)
	if err := rows.Scan(dest...); err != nil {
		return lifecycle.Assertion{}, err
	}
	a := lifecycle.Assertion{
		Producer: lifecycle.Producer(producer), EventTime: time.Unix(0, event).UTC(), Seq: seqFromDB(seq),
		Kind: lifecycle.Kind(kind), TTL: time.Duration(ttl),
	}
	if through != nil {
		a.Through = time.Unix(0, *through).UTC()
	}
	return a, nil
}

// batchChunk is how many entities one statement of a batched read names.
const batchChunk = 500

// NeighborsBatch implements [engine.Engine]. The whole call is one read-only
// transaction, so all the lists are of one snapshot, and an entity with no edges has an
// empty list.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	at, ok := xtmodel.ClampInstant(t)
	out := make([][]engine.Neighbor, len(fps))
	if !ok || len(fps) == 0 {
		return out, nil
	}
	v := e.resolve(sc.AsOf)
	err := e.read(func(c conn) error {
		if v.kind == viewFold {
			for i, fp := range fps {
				ns, err := e.neighbors(c, fp, dir, t, at, sc, v)
				if err != nil {
					return err
				}
				out[i] = ns
			}
			return nil
		}
		// Each distinct entity once, however often the call repeats it.
		var distinct []identity.Fingerprint
		seen := make(map[identity.Fingerprint]bool, len(fps))
		for _, fp := range fps {
			if !seen[fp] {
				seen[fp] = true
				distinct = append(distinct, fp)
			}
		}
		found := make(map[string][]engine.Neighbor, len(distinct))
		for chunk := range slices.Chunk(distinct, batchChunk) {
			e.reads.native.Add(uint64(len(chunk)))
			err := scanRows(e.ctx, c, e.batchSQL(dir, e.floorMicros(at), v, chunk, sc.Layer), func(rows pgx.Rows) error {
				var fp, peer, rel string
				if err := rows.Scan(&fp, &peer, &rel); err != nil {
					return err
				}
				n, err := neighborOf(peer, rel)
				if err != nil {
					return err
				}
				found[fp] = append(found[fp], n)
				return nil
			})
			if err != nil {
				return fmt.Errorf("xtengine: batched neighbors: %w", err)
			}
		}
		for i, fp := range fps {
			ns := slices.Clone(found[fp.String()])
			engine.SortNeighbors(ns)
			out[i] = slices.Compact(ns)
		}
		if e.mut == mutBatchDropsEmpty {
			out = slices.DeleteFunc(out, func(ns []engine.Neighbor) bool { return len(ns) == 0 })
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Alive implements [engine.Engine].
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	at, ok := xtmodel.ClampInstant(t)
	if !ok {
		return false, nil
	}
	v := e.resolve(sc.AsOf)
	var alive bool
	err := e.read(func(c conn) error {
		if v.kind == viewFold {
			e.reads.folded.Add(1)
			var as []lifecycle.Assertion
			err := scanRows(e.ctx, c, e.foldAliveSQL(fp, sc.Layer, sc.AsOf), func(rows pgx.Rows) error {
				a, err := scanAssertion(rows)
				as = append(as, a)
				return err
			})
			if err != nil {
				return fmt.Errorf("xtengine: reading the log: %w", err)
			}
			tl, err := lifecycle.Fold(as, lifecycle.Policy{})
			if err != nil {
				return fmt.Errorf("xtengine: %w", err)
			}
			alive = tl.AliveAt(t)
			return nil
		}
		e.reads.native.Add(1)
		return scanRows(e.ctx, c, e.aliveSQL(e.floorMicros(at), v, fp, sc.Layer), func(pgx.Rows) error {
			alive = true
			return nil
		})
	})
	if err != nil {
		return false, fmt.Errorf("xtengine: alive: %w", err)
	}
	return alive, nil
}

// Window implements [engine.Engine]. It reads the log tables over all of valid time,
// which hold every record, those a later record at the same instant overwrites in
// effect included.
func (e *Engine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := xtmodel.ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	v := e.resolve(sc.AsOf)
	if v.kind == viewFold {
		e.reads.folded.Add(1)
	} else {
		e.reads.native.Add(1)
	}
	var out []engine.Record
	err := e.read(func(c conn) error {
		return scanRows(e.ctx, c, e.windowSQL(dir, v, sc.AsOf, fp, sc.Layer, b), func(rows pgx.Rows) error {
			r, err := scanRecord(rows, dir)
			if err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("xtengine: window: %w", err)
	}
	engine.SortRecords(out)
	return out, nil
}

// scanRecord reads a row of windowSQL.
func scanRecord(rows pgx.Rows, dir engine.Direction) (engine.Record, error) {
	var (
		layer, seq, event, kind, ttl int64
		fp, peer, rel, producer      string
		through                      *int64
		payload                      []byte
	)
	if err := rows.Scan(&layer, &fp, &peer, &rel, &producer, &seq, &event, &kind, &ttl, &through, &payload); err != nil {
		return engine.Record{}, err
	}
	from, to := fp, peer
	if dir == engine.Reverse {
		from, to = peer, fp
	}
	a, err := identity.ParseFingerprint(from)
	if err != nil {
		return engine.Record{}, err
	}
	b, err := identity.ParseFingerprint(to)
	if err != nil {
		return engine.Record{}, err
	}
	r := engine.Record{
		Layer: catalog.Layer(layer), Subject: engine.EdgeSubject(a, b, catalog.RelationType(rel)),
		Producer: lifecycle.Producer(producer), EventTime: time.Unix(0, event).UTC(), Seq: seqFromDB(seq),
		Kind: lifecycle.Kind(kind), TTL: time.Duration(ttl),
	}
	if through != nil {
		r.Through = time.Unix(0, *through).UTC()
	}
	if len(payload) > 0 {
		r.Payload = payload
	}
	return r, nil
}
