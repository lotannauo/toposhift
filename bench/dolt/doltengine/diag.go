package doltengine

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// CommitTime is the diagnostic arm over a database of the arm with commits: the design
// read as if the position of a commit in history were an event time. A neighbors, batched or
// existence read at the instant t is answered from the state table as the newest commit left
// it whose frontier is at or before t (and which is at or below the token), and from nothing
// else: no log, no per-key settlement. That is the shortcut a versioned table invites ("the
// history of the commits is the history of the topology"), and it is wrong wherever a record
// arrives after the instant it is about; the arm exists to count how often. Windows read the
// log, as in the design. It is a diagnostic: never timed, never a comparator, and its wrong
// answers are its result.
//
// A commit's frontier is the largest event time among the records at or below its token. The
// database does not hold it (the design's rule never reads it), so the loader computes it from
// the stream it writes, and the caller gives it here, one for each commit in order. It is the
// model's reading ([doltmodel.CommitTime]) on a server, and a test holds the two to each
// other.
type CommitTime struct {
	e         *Engine
	frontiers []int64
	useLog    bool // the defect of answering as the design does

	reads [numOps]atomic.Uint64
}

var _ engine.Engine = (*CommitTime)(nil)

// ErrFrontiers is wrapped by the error [NewCommitTime] returns for frontiers that are not
// those of the engine's commits.
var ErrFrontiers = errors.New("doltengine: the frontiers are not the commits'")

// NewCommitTime returns the diagnostic reading of an engine of the arm with commits, given
// the frontier of each of its commits, oldest first. The frontiers must be as many as the
// commits and must not fall: each is the largest of a prefix of the stream.
func NewCommitTime(e *Engine, frontiers []int64) (*CommitTime, error) {
	if e.arm != doltmodel.ArmCommit {
		return nil, fmt.Errorf("doltengine: the diagnostic reads the arm with commits, not the %s arm: %w", e.arm, ErrFrontiers)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(frontiers) != len(e.commits) {
		return nil, fmt.Errorf("doltengine: %d frontiers for %d commits: %w", len(frontiers), len(e.commits), ErrFrontiers)
	}
	for i := 1; i < len(frontiers); i++ {
		if frontiers[i] < frontiers[i-1] {
			return nil, fmt.Errorf("doltengine: the frontier of commit %d falls below the one before it: %w", i+1, ErrFrontiers)
		}
	}
	return &CommitTime{e: e, frontiers: slices.Clone(frontiers)}, nil
}

// FaultDiagUsesLog is the defect of a diagnostic that answers from the design's rule and not
// from commit time.
const FaultDiagUsesLog = "diagnostic reads as the design does"

// Break switches a deliberate defect on, before the diagnostic is used.
func (c *CommitTime) Break(name string) {
	if name != FaultDiagUsesLog {
		panic(fmt.Sprintf("doltengine: no fault %q", name))
	}
	c.useLog = true
}

// Ordinal is the ordinal of the commit a read at the instant (nanoseconds) and the token is
// answered from: the newest, among those at or below the token, whose frontier is at or
// before the instant; 0 if there is none. The caller holds the engine's lock.
func (c *CommitTime) ordinal(token uint64, at int64) int {
	byFrontier := sort.Search(len(c.frontiers), func(i int) bool { return c.frontiers[i] > at })
	byToken := sort.Search(len(c.e.commits), func(i int) bool { return c.e.commits[i].Token > token })
	return min(byFrontier, byToken)
}

// Paths are the reads answered so far, as "kind/commit-time" counts for the reads of the
// state, and the engine's own count for windows. The model's names.
func (c *CommitTime) Paths() map[string]uint64 {
	out := make(map[string]uint64)
	for op := range numOps {
		if n := c.reads[op].Load(); n > 0 {
			out[opNames[op]+"/commit-time"] = n
		}
	}
	for k, v := range c.e.Paths() {
		if c.useLog || strings.HasPrefix(k, "window/") {
			out[k] = v
		}
	}
	return out
}

// view reads the state as of the commit that the read's instant and token name, in one
// read-only transaction, with the engine's view held still.
func (c *CommitTime) view(sc engine.Scope, at int64, fn func(ctx context.Context, tx *sql.Tx, hash string, have bool) error) error {
	c.e.mu.RLock()
	defer c.e.mu.RUnlock()
	ord := c.ordinal(sc.AsOf, at)
	hash := ""
	if ord > 0 {
		hash = c.e.commits[ord-1].Hash
	}
	tx, err := c.e.rdb.BeginTx(c.e.ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return fn(c.e.ctx, tx, hash, ord > 0)
}

// holdsClause is the state row's hold test, as the design's reads write it.
func holdsClause(x string) string {
	return fmt.Sprintf("%[1]s.kind = 'o' AND (%[1]s.deadline_ns IS NULL OR %[1]s.deadline_ns > ?)", x)
}

// neighborState is the diagnostic's statement for a neighbors read: the state table as of the
// commit, each (peer, relation) held if any producer's row holds, and nothing else.
func (c *CommitTime) neighborState(fps []identity.Fingerprint, batch bool, dir engine.Direction, at int64, sc engine.Scope, hash string) (stmt, error) {
	asOf, err := asOfClause(hash)
	if err != nil {
		return stmt{}, err
	}
	rc := readCtx{hash: hash, layer: int(sc.Layer), dir: int(dir), at: at, batch: batch}
	if batch {
		hexes := make([]string, len(fps))
		for i, fp := range fps {
			hexes[i] = hex.EncodeToString(doltmodel.EncodeFingerprint(fp))
		}
		j, err := json.Marshal(hexes)
		if err != nil {
			return stmt{}, fmt.Errorf("doltengine: %w", err)
		}
		rc.subjects = string(j)
	} else {
		rc.subject = doltmodel.EncodeFingerprint(fps[0])
	}
	var s stmt
	s.add("SELECT u.ord, u.peer, u.rel, MAX(u.holds) AS holds\nFROM (\n")
	inner := stmt{}
	rc.scanState(&inner, "s.peer AS peer, s.rel AS rel, IF("+holdsClause("s")+", 1, 0) AS holds", "state", asOf, "")
	s.add(inner.text, append([]any{at}, inner.args...)...)
	s.add("\n) u\nGROUP BY u.ord, u.peer, u.rel")
	return s, nil
}

func (c *CommitTime) neighborLists(fps []identity.Fingerprint, batch bool, dir engine.Direction, at int64, sc engine.Scope) ([][]engine.Neighbor, error) {
	out := make([][]engine.Neighbor, len(fps))
	if len(fps) == 0 {
		return out, nil
	}
	err := c.view(sc, at, func(ctx context.Context, tx *sql.Tx, hash string, have bool) error {
		if !have {
			return nil // before every commit's frontier: nothing is known
		}
		s, err := c.neighborState(fps, batch, dir, at, sc, hash)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, s.text, s.args...)
		if err != nil {
			return fmt.Errorf("doltengine: neighbors (commit time): %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var ord, holds int64
			var peer, rel []byte
			if err := rows.Scan(&ord, &peer, &rel, &holds); err != nil {
				return fmt.Errorf("doltengine: neighbors (commit time): %w", err)
			}
			if holds == 0 {
				continue
			}
			i := 0
			if batch {
				i = int(ord) - 1
			}
			if i < 0 || i >= len(out) {
				return fmt.Errorf("doltengine: neighbors (commit time): list %d of %d", ord, len(out))
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
		return nil, err
	}
	for _, l := range out {
		engine.SortNeighbors(l)
	}
	return out, nil
}

// Neighbors implements [engine.Engine].
func (c *CommitTime) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if c.useLog {
		return c.e.Neighbors(fp, dir, t, sc)
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	at, ok := doltmodel.ClampInstant(t)
	if !ok {
		return nil, nil
	}
	lists, err := c.neighborLists([]identity.Fingerprint{fp}, false, dir, at, sc)
	if err != nil {
		return nil, err
	}
	c.reads[opNeighbors].Add(1)
	return lists[0], nil
}

// NeighborsBatch implements [engine.Engine].
func (c *CommitTime) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if c.useLog {
		return c.e.NeighborsBatch(fps, dir, t, sc)
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	at, ok := doltmodel.ClampInstant(t)
	if !ok {
		return make([][]engine.Neighbor, len(fps)), nil
	}
	lists, err := c.neighborLists(fps, true, dir, at, sc)
	if err != nil {
		return nil, err
	}
	c.reads[opBatch].Add(1)
	return lists, nil
}

// Alive implements [engine.Engine].
func (c *CommitTime) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if c.useLog {
		return c.e.Alive(fp, t, sc)
	}
	if err := sc.Validate(); err != nil {
		return false, err
	}
	at, ok := doltmodel.ClampInstant(t)
	if !ok {
		return false, nil
	}
	var alive bool
	err := c.view(sc, at, func(ctx context.Context, tx *sql.Tx, hash string, have bool) error {
		if !have {
			return nil
		}
		asOf, err := asOfClause(hash)
		if err != nil {
			return err
		}
		var holds int64
		q := "SELECT COALESCE(MAX(IF(" + holdsClause("s") + ", 1, 0)), 0) FROM ent_state" + asOf + " s WHERE s.fp = ? AND s.layer = ?"
		if err := tx.QueryRowContext(ctx, q, at, doltmodel.EncodeFingerprint(fp), int(sc.Layer)).Scan(&holds); err != nil {
			return fmt.Errorf("doltengine: alive (commit time): %w", err)
		}
		alive = holds != 0
		return nil
	})
	if err != nil {
		return false, err
	}
	c.reads[opAlive].Add(1)
	return alive, nil
}

// Window implements [engine.Engine]: the log, as the design reads it.
func (c *CommitTime) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	return c.e.Window(fp, dir, from, to, sc)
}

// ErrReadOnly is the error of a write to a diagnostic reading.
var ErrReadOnly = errors.New("doltengine: the diagnostic reading is read only")

// Write implements [engine.Engine]; the diagnostic reads a database that is loaded.
func (c *CommitTime) Write([]engine.Record) error { return ErrReadOnly }

// Retain implements [engine.Engine]; see Write.
func (c *CommitTime) Retain(time.Time) error { return ErrReadOnly }

// LastSeq implements [engine.Engine].
func (c *CommitTime) LastSeq() uint64 { return c.e.LastSeq() }

// Size implements [engine.Engine].
func (c *CommitTime) Size() (int64, error) { return c.e.Size() }

// Close implements [engine.Engine]; the engine it reads is the caller's to close.
func (c *CommitTime) Close() error { return nil }
