package doltengine

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// view says which state a read at the token is answered from: the current state if the
// token is at or above the newest sequence number; the state as the newest commit at the
// token left it, in the arm with commits; none otherwise. The caller holds the engine's
// view locked, so the answer cannot move under it.
func (e *Engine) view(token uint64) (hash string, haveState bool) {
	if token >= e.lastSeq || e.f.headForOldToken {
		return "", true
	}
	if e.arm != doltmodel.ArmCommit {
		return "", false
	}
	i, ok := e.byToken[token]
	if !ok {
		return "", false
	}
	if e.f.commitAfter && i+1 < len(e.commits) {
		i++ // the commit after the token
	}
	return e.commits[i].Hash, true
}

// read runs fn in one read-only transaction, so that everything it reads is of one
// snapshot, with the view of the token fixed for the whole of it. The state table is read
// as of the commit the view names, and the engine's lock keeps a batch from moving the
// database between the view and the read.
func (e *Engine) read(sc engine.Scope, fn func(ctx context.Context, tx *sql.Tx, hash string, haveState bool) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	hash, have := e.view(sc.AsOf)
	tx, err := e.rdb.BeginTx(e.ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return fn(e.ctx, tx, hash, have)
}

// neighborLists answers the lists for fps, one statement for the whole batch if batch is
// set, else for the one fingerprint, and says how the rule settled the keys.
func (e *Engine) neighborLists(fps []identity.Fingerprint, batch bool, dir engine.Direction, at int64, sc engine.Scope) ([][]engine.Neighbor, tally, error) {
	out := make([][]engine.Neighbor, len(fps))
	var ty tally
	if len(fps) == 0 {
		return out, ty, nil
	}
	err := e.read(sc, func(ctx context.Context, tx *sql.Tx, hash string, have bool) error {
		c := readCtx{hash: hash, layer: int(sc.Layer), dir: int(dir), at: at, token: doltmodel.EncodeSeq(sc.AsOf), batch: batch, f: &e.f}
		if batch {
			hexes := make([]string, len(fps))
			for i, fp := range fps {
				hexes[i] = hex.EncodeToString(doltmodel.EncodeFingerprint(fp))
			}
			j, err := json.Marshal(hexes)
			if err != nil {
				return fmt.Errorf("doltengine: %w", err)
			}
			c.subjects = string(j)
		} else {
			c.subject = doltmodel.EncodeFingerprint(fps[0])
		}
		var s stmt
		if have {
			var err error
			if s, err = neighborsRule(c); err != nil {
				return err
			}
		} else {
			s = neighborsLog(c)
		}
		rows, err := tx.QueryContext(ctx, s.text, s.args...)
		if err != nil {
			return fmt.Errorf("doltengine: neighbors: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var ord, ks, kl, holds int64
			var peer, rel []byte
			if have {
				err = rows.Scan(&ord, &peer, &rel, &ks, &kl, &holds)
			} else {
				err = rows.Scan(&ord, &peer, &rel, &holds)
				ty.noCommit = true
			}
			if err != nil {
				return fmt.Errorf("doltengine: neighbors: %w", err)
			}
			ty.state += uint64(ks)
			ty.log += uint64(kl)
			if holds == 0 {
				continue
			}
			i := 0
			if batch {
				i = int(ord) - 1
			}
			if i < 0 || i >= len(out) {
				return fmt.Errorf("doltengine: neighbors: list %d of %d", ord, len(out))
			}
			p, err := doltmodel.DecodeFingerprint(peer)
			if err != nil {
				return fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
			}
			out[i] = append(out[i], engine.Neighbor{Peer: p, Relation: catalog.RelationType(rel)})
		}
		return rows.Err()
	})
	if err != nil {
		return nil, tally{}, err
	}
	for _, l := range out {
		// The order is the contract's, made here: by the encoding's bytes, never by the text
		// form of a fingerprint.
		engine.SortNeighbors(l)
	}
	return out, ty, nil
}

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := doltmodel.ClampInstant(t)
	if !ok {
		return nil, nil
	}
	lists, ty, err := e.neighborLists([]identity.Fingerprint{fp}, false, dir, ns, sc)
	if err != nil {
		return nil, err
	}
	e.countKeyed(opNeighbors, ty)
	return lists[0], nil
}

// NeighborsBatch implements [engine.Engine]. One statement, in one transaction, answers
// every list, so they are of one snapshot, and an entity with no edges has an empty list.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := doltmodel.ClampInstant(t)
	if !ok {
		return make([][]engine.Neighbor, len(fps)), nil
	}
	lists, ty, err := e.neighborLists(fps, true, dir, ns, sc)
	if err != nil {
		return nil, err
	}
	e.countKeyed(opBatch, ty)
	if e.f.batchDropsEmpty {
		kept := lists[:0]
		for _, l := range lists {
			if len(l) > 0 {
				kept = append(kept, l)
			}
		}
		lists = kept
	}
	return lists, nil
}

// Alive implements [engine.Engine].
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	at, ok := doltmodel.ClampInstant(t)
	if !ok {
		return false, nil
	}
	var alive bool
	var ty tally
	err := e.read(sc, func(ctx context.Context, tx *sql.Tx, hash string, have bool) error {
		c := readCtx{hash: hash, layer: int(sc.Layer), at: at, token: doltmodel.EncodeSeq(sc.AsOf), subject: doltmodel.EncodeFingerprint(fp), f: &e.f}
		var keys, state, holds int64
		if have {
			s, err := aliveRule(c)
			if err != nil {
				return err
			}
			if err := tx.QueryRowContext(ctx, s.text, s.args...).Scan(&keys, &state, &holds); err != nil {
				return fmt.Errorf("doltengine: alive: %w", err)
			}
			ty.state, ty.log = uint64(state), uint64(keys-state)
		} else {
			s := aliveLog(c)
			if err := tx.QueryRowContext(ctx, s.text, s.args...).Scan(&keys, &holds); err != nil {
				return fmt.Errorf("doltengine: alive: %w", err)
			}
			ty.noCommit = keys > 0
		}
		alive = holds != 0
		return nil
	})
	if err != nil {
		return false, err
	}
	e.countKeyed(opAlive, ty)
	return alive, nil
}

// Window implements [engine.Engine]. It reads the log, the table every record is kept in,
// through the index on event time, and the state tables not at all.
func (e *Engine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := doltmodel.ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	e.reads.queries[opWindow][outWindowLog].Add(1)
	s := windowSQL(int(sc.Layer), int(dir), doltmodel.EncodeFingerprint(fp), b.Lo, b.Hi, b.Unbounded, doltmodel.EncodeSeq(sc.AsOf), &e.f)
	var out []engine.Record
	err := e.read(sc, func(ctx context.Context, tx *sql.Tx, _ string, _ bool) error {
		rows, err := tx.QueryContext(ctx, s.text, s.args...)
		if err != nil {
			return fmt.Errorf("doltengine: window: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var peer, rel, producer, kind, payload []byte
			var ev, seq, ttl int64
			var through sql.NullInt64
			if err := rows.Scan(&peer, &rel, &producer, &ev, &seq, &kind, &ttl, &through, &payload); err != nil {
				return fmt.Errorf("doltengine: window: %w", err)
			}
			p, err := doltmodel.DecodeFingerprint(peer)
			if err != nil {
				return fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
			}
			r := engine.Record{
				Layer: sc.Layer, Producer: lifecycle.Producer(producer), EventTime: doltmodel.FromNanos(ev), Seq: doltmodel.DecodeSeq(seq),
				Kind: lifecycle.Observe, TTL: time.Duration(ttl), Payload: slices.Clone(payload),
			}
			if string(kind) == doltmodel.KindDelete {
				r.Kind = lifecycle.Delete
			}
			if through.Valid {
				r.Through = doltmodel.FromNanos(through.Int64)
			}
			if dir == engine.Forward {
				r.Subject = engine.EdgeSubject(fp, p, catalog.RelationType(rel))
			} else {
				r.Subject = engine.EdgeSubject(p, fp, catalog.RelationType(rel))
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if e.f.windowDedup {
		out = dedupWindow(out)
	}
	return out, nil
}

// dedupWindow is the defect of a window that leaves out a record that a later one of the
// same subject and producer overwrites at its instant.
func dedupWindow(in []engine.Record) []engine.Record {
	type key struct {
		s  engine.Subject
		p  lifecycle.Producer
		ev time.Time
	}
	seen := make(map[key]bool)
	var rev []engine.Record
	for i := len(in) - 1; i >= 0; i-- {
		k := key{in[i].Subject, in[i].Producer, in[i].EventTime}
		if !seen[k] {
			seen[k] = true
			rev = append(rev, in[i])
		}
	}
	slices.Reverse(rev)
	return rev
}
