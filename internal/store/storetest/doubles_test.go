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

	"github.com/lotannauo/toposhift/internal/catalog"
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
	// tear makes Neighbors inconsistent. A read whose answer a batch moves (the
	// batch deletes one of its edges and observes another) takes one half of its
	// answer before the batch commits and the other after, as a store would answer
	// if it read the key range that holds the deleted edge before the batch and the
	// range that holds the observed one after: the answer has both. A handshake puts
	// the batch between the halves however the goroutines are scheduled: Write holds
	// the batch until such a read has taken its first half, and the read waits for
	// the commit to take its second.
	tear bool
	hs   handshake
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

// handshake holds a batch of a tearing store until a read the batch can change
// has taken the first half of its answer, and holds that read until the batch has
// committed. The zero value is ready to use.
type handshake struct {
	mu   sync.Mutex
	cond sync.Cond
	// holding is set while batch is held; seq is the token before it.
	holding bool
	batch   []store.Record
	seq     uint64
	// holds counts the batches held, and released is the last one let go: by a read
	// that took its first half, or by its time running out.
	holds, released uint64
	// commits counts the batches committed.
	commits uint64
	// tore is set once a read answered with both halves; no batch is held after.
	tore bool
}

// holdLimit is how long a batch that moves a read waits for one before it commits
// anyway. It bounds the wait when no reader reads what the batch moves, or the
// readers have stopped. Every batch of the check's second phase moves the pods the
// readers read most, and the first that a read of one joins tears it.
const holdLimit = time.Second

func (h *handshake) init() {
	if h.cond.L == nil {
		h.cond.L = &h.mu
	}
}

// commit holds the batch until a read it moves has taken its first half, or
// holdLimit has passed, then commits it with write. A batch that moves no read,
// and every batch once a read has been torn, is not held.
func (h *handshake) commit(batch []store.Record, last uint64, write func() error) error {
	h.mu.Lock()
	h.init()
	if h.tore || !moves(batch) {
		h.mu.Unlock()
		return h.committed(write())
	}
	h.holds++
	hold := h.holds
	h.holding, h.batch, h.seq = true, batch, last
	t := time.AfterFunc(holdLimit, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.release(hold)
	})
	for h.released < hold {
		h.cond.Wait()
	}
	h.holding, h.batch = false, nil
	h.mu.Unlock()
	t.Stop()

	return h.committed(write())
}

// committed counts a commit, and wakes the read waiting for it.
func (h *handshake) committed(err error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commits++
	h.cond.Broadcast()
	return err
}

// moves says whether batch deletes an edge and observes another at one end, in
// one layer: a read of that end's neighbors has a peer taken away and one added.
func moves(batch []store.Record) bool {
	type end struct {
		fp    identity.Fingerprint
		dir   store.Direction
		layer catalog.Layer
	}
	deletes, observes := map[end]bool{}, map[end]bool{}
	for _, r := range batch {
		if r.Subject.Kind != store.SubjectEdge {
			continue
		}
		for _, e := range []end{{r.Subject.A, store.Forward, r.Layer}, {r.Subject.B, store.Reverse, r.Layer}} {
			switch r.Kind {
			case lifecycle.Delete:
				deletes[e] = true
			case lifecycle.Observe:
				observes[e] = true
			}
			if deletes[e] && observes[e] {
				return true
			}
		}
	}
	return false
}

// torn records that a read answered with both halves.
func (h *handshake) torn() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tore = true
}

// release lets hold go, unless it has gone already. mu is held.
func (h *handshake) release(hold uint64) {
	if h.released < hold {
		h.released = hold
		h.cond.Broadcast()
	}
}

// joined is a read that took the first half of its answer while a batch that
// moves it was held.
type joined struct{ hold, commits uint64 }

// join says whether the batch held moves the neighbors of fp in direction d, at
// instant at in sc: the read sees the batch (its token is above seq), and the
// batch deletes an edge of fp's and observes another, at or before at.
func (h *handshake) join(fp identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) (joined, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.init()
	j := joined{h.holds, h.commits}
	if !h.holding || sc.AsOf <= h.seq {
		return j, false
	}
	var deletes, observes bool
	for _, r := range h.batch {
		end := r.Subject.A
		if d == store.Reverse {
			end = r.Subject.B
		}
		if r.Subject.Kind != store.SubjectEdge || end != fp || r.Layer != sc.Layer || r.EventTime.After(at) {
			continue
		}
		deletes = deletes || r.Kind == lifecycle.Delete
		observes = observes || r.Kind == lifecycle.Observe
	}
	return j, deletes && observes
}

// between lets the batch j joined go, and waits for it to commit.
func (h *handshake) between(j joined) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.release(j.hold)
	for h.commits == j.commits {
		h.cond.Wait()
	}
}

func (s *slow) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if hit, err := s.damaged(at); hit {
		return nil, err
	}
	if !s.tear {
		first, err := s.Store.Neighbors(ctx, fp, d, at, sc)
		pause()
		return first, err
	}
	j, joins := s.hs.join(fp, d, at, sc)
	first, err := s.Store.Neighbors(ctx, fp, d, at, sc)
	if err != nil || !joins {
		return first, err
	}
	s.hs.between(j)
	second, err := s.Store.Neighbors(ctx, fp, d, at, sc)
	if err != nil {
		return nil, err
	}
	out := slices.Clone(first)
	for _, n := range second {
		if !slices.Contains(first, n) {
			out = append(out, n)
		}
	}
	store.SortNeighbors(out)
	if len(out) > len(first) && len(out) > len(second) {
		s.hs.torn()
	}
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
	if s.tear && len(batch) > 0 {
		return s.hs.commit(batch, s.Store.LastSeq(), func() error { return s.Store.Write(ctx, batch) })
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
