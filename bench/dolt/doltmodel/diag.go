package doltmodel

import (
	"sort"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/identity"
)

// CommitTime is the diagnostic arm: the commit-per-batch design read as if the position of
// a commit in history were an event time. A read of the neighbors or the existence of an
// entity at t is answered from the state as the newest commit whose frontier is at or
// before t left it, which is the shortcut a versioned table invites ("commit history is
// the history of the topology"). A commit records what was known when it was made, not
// when it happened, so the shortcut is wrong wherever a record arrives after the instant
// it is about: the arm exists to count how often. Windows read the log, as in the real
// design. It is a diagnostic: it is never timed and never a comparator.
type CommitTime struct{ *Model }

// NewCommitTime returns an empty diagnostic model over a new model with commits.
func NewCommitTime(opts ...Option) *CommitTime {
	return &CommitTime{New(append([]Option{WithArm(ArmCommit)}, opts...)...)}
}

// commitFor is the ordinal of the newest commit, among those at or below the token, whose
// frontier is at or before t; 0 if there is none. Frontiers and tokens never fall, so
// each condition holds for a prefix of the commits. The caller holds the lock.
func (c CommitTime) commitFor(sc engine.Scope, t int64) int {
	byFrontier := sort.Search(len(c.commits), func(i int) bool { return c.commits[i].Frontier > t })
	byToken := sort.Search(len(c.commits), func(i int) bool { return c.commits[i].Token > sc.AsOf })
	return min(byFrontier, byToken)
}

func (c CommitTime) view(sc engine.Scope, t int64) view {
	if c.f.diagUsesLog {
		return c.choose(sc, t)
	}
	return view{path: PathCommitTime, ord: c.commitFor(sc, t)}
}

// countDiag counts a read: as the diagnostic's own, or, with the defect that makes it read
// the design's way, as the design's.
func (c CommitTime) countDiag(op int, v view, ty *tally) {
	if v.path == PathCommitTime {
		c.reads.queries[op][outCommitTime].Add(1)
		return
	}
	c.countKeyed(op, *ty)
}

// Neighbors implements [engine.Engine].
func (c CommitTime) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return nil, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := c.view(sc, ns)
	var ty tally
	defer c.countDiag(opNeighbors, v, &ty)
	return c.neighbors(fp, dir, ns, sc, v, &ty), nil
}

// NeighborsBatch implements [engine.Engine].
func (c CommitTime) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := c.view(sc, ns)
	var ty tally
	if ok {
		defer c.countDiag(opBatch, v, &ty)
	}
	return engine.NeighborsEach(fps, func(fp identity.Fingerprint) ([]engine.Neighbor, error) {
		if !ok {
			return nil, nil
		}
		return c.neighbors(fp, dir, ns, sc, v, &ty), nil
	})
}

// Alive implements [engine.Engine].
func (c CommitTime) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := c.view(sc, ns)
	var ty tally
	defer c.countDiag(opAlive, v, &ty)
	return c.alive(fp, ns, sc, v, &ty), nil
}
