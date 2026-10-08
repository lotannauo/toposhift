package storetest_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The positive controls: stores that conform, and that differ from the reference
// in how they are built, so that a mutant caught by the suite is not caught for
// being a different kind of store.

// A store with no damage passes the whole suite. It is run with the checks one at
// a time, which is how a test double that is not safe for concurrent use runs.
func TestAStoreWithNoDamageConforms(t *testing.T) {
	t.Parallel()
	storetest.Run(t, storetest.Factory{
		Open:          func(_ string, p lifecycle.Policy) (store.Store, error) { return newBroken(p), nil },
		NotConcurrent: true,
	})
}

// compacting is a store that really discards history on Retain, by the rule a
// layout is meant to follow: it keeps every record at or after the horizon, and for
// each producer's reference to a subject the newest record before it, only if that
// reference is still live at the horizon. It is the honest control for retention,
// which the reference, that discards nothing, cannot be.
func compacting(p lifecycle.Policy) *broken {
	b := newBroken(p)
	b.retain = compact
	return b
}

func TestAStoreThatReallyDiscardsHistoryConforms(t *testing.T) {
	t.Parallel()
	storetest.Run(t, storetest.Factory{
		Open: func(_ string, p lifecycle.Policy) (store.Store, error) { return compacting(p), nil },
	})
}

// The control means something only if the retentions took history away.
func TestTheCompactingStoreReallyDropsRecords(t *testing.T) {
	t.Parallel()
	for _, w := range storetest.Workloads() {
		b := compacting(w.Policy)
		if err := storetest.Check(b, w, storetest.Options{RetainAt: []float64{0.3, 0.7}}); err != nil {
			t.Fatalf("workload %q: %v", w.Name, err)
		}
		if b.dropped == 0 || len(b.written) == 0 {
			t.Errorf("workload %q: the store dropped %d records and kept %d", w.Name, b.dropped, len(b.written))
		}
	}
}

// journals keeps, per directory, the batches a store accepted and the
// retentions that moved its horizon, in order. Opening a directory it knows
// replays them into a new store: the double of a store that is durable.
type journals struct {
	mu   sync.Mutex
	dirs map[string]*journal
	// damage, if set, is how the store comes back wrongly.
	damage *reopenDamage
}

type journal struct {
	mu      sync.Mutex
	entries []journalEntry
}

type journalEntry struct {
	batch  []store.Record
	retain time.Time // set for a retention, and then batch is empty
}

// reopenDamage changes what is replayed, or the store after it was replayed.
type reopenDamage struct {
	entries func([]journalEntry) []journalEntry
	after   func(b *broken, lastSeq uint64)
}

// durable logs what its inner store accepts.
type durable struct {
	*broken
	jr *journal
}

func (j *journals) open(dir string, p lifecycle.Policy) (store.Store, error) {
	j.mu.Lock()
	jr, known := j.dirs[dir]
	if !known {
		if j.dirs == nil {
			j.dirs = map[string]*journal{}
		}
		jr = &journal{}
		j.dirs[dir] = jr
	}
	j.mu.Unlock()

	b := newBroken(p)
	if known {
		jr.mu.Lock()
		entries := slices.Clone(jr.entries)
		jr.mu.Unlock()
		var lastSeq uint64
		for _, e := range entries {
			if n := len(e.batch); n > 0 {
				lastSeq = e.batch[n-1].Seq
			}
		}
		if j.damage != nil && j.damage.entries != nil {
			entries = j.damage.entries(entries)
		}
		for _, e := range entries {
			var err error
			if e.retain.IsZero() {
				err = b.Write(bg, cloneRecs(e.batch))
			} else {
				err = b.Retain(bg, e.retain)
			}
			if err != nil {
				return nil, err
			}
		}
		if j.damage != nil && j.damage.after != nil {
			j.damage.after(b, lastSeq)
		}
	}
	return &durable{broken: b, jr: jr}, nil
}

func (d *durable) Write(ctx context.Context, batch []store.Record) error {
	if err := d.broken.Write(ctx, batch); err != nil || len(batch) == 0 {
		return err
	}
	d.jr.mu.Lock()
	defer d.jr.mu.Unlock()
	d.jr.entries = append(d.jr.entries, journalEntry{batch: cloneRecs(batch)})
	return nil
}

func (d *durable) Retain(ctx context.Context, h time.Time) error {
	before := d.Horizon()
	if err := d.broken.Retain(ctx, h); err != nil {
		return err
	}
	if after := d.Horizon(); after.Time.Equal(before.Time) {
		return nil // it moved nothing, so there is nothing to replay
	}
	d.jr.mu.Lock()
	defer d.jr.mu.Unlock()
	d.jr.entries = append(d.jr.entries, journalEntry{retain: h})
	return nil
}

func TestADurableStoreConforms(t *testing.T) {
	t.Parallel()
	j := &journals{}
	storetest.Run(t, storetest.Factory{Open: j.open, Durable: true})
}

// slow is a store that is consistent but slow: it takes its time inside each read,
// so writes land while a read is in progress. A concurrent-read check that failed
// it would be wrong. Each of its flags damages it in one way.
type slow struct {
	*memstore.Store
	// tear makes Neighbors inconsistent by answering one half from before a pause
	// and the other half from after it.
	tear bool
	// tearBatchEnd answers a batched read with one snapshot per entry.
	tearBatchEnd bool
	// misindex puts the answers after the first 64 of a large batched read in the
	// wrong place.
	misindex bool
	// ahead raises LastSeq before a batch is visible.
	ahead   bool
	pending atomic.Uint64
	// retainMode damages reads that overlap a Retain.
	retainMode retainMode
	retaining  atomic.Bool
	retainTo   atomic.Int64
	// backward makes LastSeq lose ground on every other call.
	backward bool
	calls    atomic.Uint64
}

type retainMode int

const (
	retainHonestly retainMode = iota
	// retainEmpty answers every read that overlaps a Retain with nothing, as a
	// store that deletes history before it has written the baseline that stands
	// for it does.
	retainEmpty
	// retainDiscardFirst discards history before it publishes the horizon: reads at
	// or after the new horizon see nothing while it retains.
	retainDiscardFirst
	// retainRefuses refuses every read that overlaps a Retain for the horizon,
	// including those at or after the new one.
	retainRefuses
)

func pause() { runtime.Gosched(); time.Sleep(300 * time.Microsecond) }

func (s *slow) Retain(ctx context.Context, h time.Time) error {
	if s.retainMode == retainHonestly {
		return s.Store.Retain(ctx, h)
	}
	s.retainTo.Store(h.UnixNano())
	s.retaining.Store(true)
	pause()
	defer s.retaining.Store(false)
	return s.Store.Retain(ctx, h)
}

// damaged says whether a read of instant t overlaps a Retain that damages it, and
// then what it gets.
func (s *slow) damaged(t time.Time) (bool, error) {
	if !s.retaining.Load() {
		return false, nil
	}
	switch s.retainMode {
	case retainEmpty:
		return true, nil
	case retainDiscardFirst:
		return !t.Before(time.Unix(0, s.retainTo.Load())), nil
	case retainRefuses:
		return true, store.ErrBeforeHorizon
	}
	return false, nil
}

// pivot splits peers into two halves by the first byte of their hash.
func pivot(n store.Neighbor) bool { h := n.Peer.Hash(); return h[0] < 128 }

func (s *slow) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if hit, err := s.damaged(at); hit {
		return nil, err
	}
	first, err := s.Store.Neighbors(ctx, fp, d, at, sc)
	pause()
	if err != nil || !s.tear {
		return first, err
	}
	// The first half of the answer comes from before the pause and the second half
	// from after it, as a store would answer if it read one key range from one
	// state and the next from another.
	second, err := s.Store.Neighbors(ctx, fp, d, at, sc)
	if err != nil {
		return nil, err
	}
	var out []store.Neighbor
	for _, n := range first {
		if pivot(n) {
			out = append(out, n)
		}
	}
	for _, n := range second {
		if !pivot(n) {
			out = append(out, n)
		}
	}
	store.SortNeighbors(out)
	return out, nil
}

func (s *slow) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	if hit, err := s.damaged(at); hit {
		if err != nil {
			return nil, err
		}
		return make([][]store.Neighbor, len(fps)), nil
	}
	if !s.tearBatchEnd {
		pause()
		out, err := s.Store.NeighborsBatch(ctx, fps, d, at, sc)
		if s.misindex {
			for i := 64; i < len(out); i++ {
				out[i] = out[i%64]
			}
		}
		return out, err
	}
	// One snapshot per entry instead of one for the whole batch.
	out := make([][]store.Neighbor, len(fps))
	for i, fp := range fps {
		ns, err := s.Store.Neighbors(ctx, fp, d, at, sc)
		if err != nil {
			return nil, err
		}
		out[i] = ns
		pause()
	}
	return out, nil
}

func (s *slow) Alive(ctx context.Context, fp identity.Fingerprint, at time.Time, sc store.Scope) (bool, error) {
	if hit, err := s.damaged(at); hit {
		return false, err
	}
	pause()
	return s.Store.Alive(ctx, fp, at, sc)
}

func (s *slow) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if hit, err := s.damaged(from); hit {
		return nil, err
	}
	pause()
	return s.Store.Window(ctx, fp, d, from, to, sc)
}

func (s *slow) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if hit, err := s.damaged(from); hit {
		return nil, err
	}
	pause()
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

func (s *slow) Write(ctx context.Context, batch []store.Record) error {
	if s.ahead && len(batch) > 0 {
		s.pending.Store(batch[len(batch)-1].Seq)
		pause()
	}
	return s.Store.Write(ctx, batch)
}

func (s *slow) LastSeq() uint64 {
	real := s.Store.LastSeq()
	if s.backward && s.calls.Add(1)%2 == 0 && real > 0 {
		return real - 1
	}
	if s.ahead {
		return max(real, s.pending.Load())
	}
	return real
}

func newSlow(t *testing.T, mod func(*slow)) *slow {
	t.Helper()
	m, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := &slow{Store: m}
	if mod != nil {
		mod(s)
	}
	return s
}

// refusing counts the writes a store refuses for being before the horizon.
type refusing struct {
	*memstore.Store
	refused atomic.Int64
}

func (r *refusing) Write(ctx context.Context, batch []store.Record) error {
	err := r.Store.Write(ctx, batch)
	if errors.Is(err, store.ErrBeforeHorizon) {
		r.refused.Add(1)
	}
	return err
}
