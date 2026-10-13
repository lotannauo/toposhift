package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Instrument is what tests and the benchmarks look into a store with. It is not
// part of the store's contract, and a caller that is neither has no use for it.
type Instrument struct{ s *Store }

// Instrument returns the store's instrument.
func (s *Store) Instrument() *Instrument { return &Instrument{s} }

// KV is the database under the store. Writing to it breaks the store.
func (i *Instrument) KV() *pebblekv.KV { return i.s.kv }

// LastRetain is how the last call of Retain spent its time: the rewrite up to its
// last commit, the flush and the wait of SettleRetention, and whether the wait
// reached its deadline. All zero before the first Retain; flush and settle are
// zero when SettleRetention is off or the retention returned early, before
// rewriting anything.
//
// In a Background store the time of the last Retain is the time it took to publish
// the horizons, which is all the work it does itself, and the time of the chunks of
// the pass is in the samples "retain.chunk_hold_ns". Flush and settle are those of
// the pass's settle (see [pebblekv.KV.CompactRanges]), which are set when the pass
// ends, so they are zero until [Instrument.WaitRetained] returns, and stay zero for a
// pass a later Retain replaced while it settled.
func (i *Instrument) LastRetain() (work, flush, settle time.Duration, deadlineHit bool) {
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	return i.s.last.work, i.s.last.flush, i.s.last.settle, i.s.last.deadlineHit
}

// SizeByLayer is Pebble's estimate of the bytes of tables that hold each layer's
// keys (the key range of a layer is one contiguous span, since a layer leads every
// key). It counts tables, not the log or what is still in the memtable, so it is
// meant for a database at rest. A layer with none is left out.
func (i *Instrument) SizeByLayer() (map[catalog.Layer]int64, error) {
	out := map[catalog.Layer]int64{}
	for l := catalog.L0; l <= catalog.L3; l++ {
		lo, hi := prefixBounds([]byte{pebblekv.LayerByte(l)})
		n, err := i.s.kv.EstimateDiskUsage(lo, hi)
		if err != nil {
			return nil, fmt.Errorf("pebblestore: size of %s: %w", l, err)
		}
		if n > 0 {
			out[l] = int64(n)
		}
	}
	return out, nil
}

// Part names a share of the logical bytes of a layer's keys and values.
type Part struct {
	Layer catalog.Layer
	// Kind is "observe" (a record that asserts, not a run extension), "extension"
	// (a record that re-asserts a run with a later Through), "delete",
	// "payload forward", "payload reverse" or "payload entity" (the payload of the
	// records stored under that direction), "checkpoint" or "baseline".
	Kind string
}

// The kinds of Part.
const (
	partObserve        = "observe"
	partExtension      = "extension"
	partDelete         = "delete"
	partPayloadForward = "payload forward"
	partPayloadReverse = "payload reverse"
	partPayloadEntity  = "payload entity"
	partCheckpoint     = "checkpoint"
	partBaseline       = "baseline"
)

// recordParts splits the total logical bytes of one stored record, its key and
// its value, into the bytes of its kind and the bytes of its payload, and names
// the part each belongs to. dir is the direction byte of the prefix it is stored
// under.
func recordParts(v pebblekv.Value, dir byte, total int) (kind string, kindBytes int, payload string, payloadBytes int) {
	switch {
	case v.Kind == lifecycle.Delete:
		kind = partDelete
	case v.HasThrough:
		kind = partExtension
	default:
		kind = partObserve
	}
	switch dir {
	case 1:
		payload = partPayloadForward
	case 2:
		payload = partPayloadReverse
	default:
		payload = partPayloadEntity
	}
	return kind, total - len(v.Payload), payload, len(v.Payload)
}

// Breakdown scans every data key and divides the bytes of key and value into the
// parts: a record's bytes by its kind, its payload by the direction it is stored
// under, and a checkpoint or the baseline as a whole. The parts add up to the
// bytes of every data key and value.
func (i *Instrument) Breakdown() (map[Part]int64, error) {
	lo, hi := dataBounds()
	it, err := i.s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := map[Part]int64{}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		layer, ok := pebblekv.LayerFromByte(prefix[0])
		if !ok {
			return nil, fmt.Errorf("pebblestore: key %x has no layer", it.Key())
		}
		total := len(it.Key()) + len(it.Value())
		switch kind {
		case kindCheckpoint:
			out[Part{layer, partCheckpoint}] += int64(total)
		case kindBaseline:
			out[Part{layer, partBaseline}] += int64(total)
		default:
			dir := prefix[prefixLen-1]
			_, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return nil, err
			}
			k, kb, p, pb := recordParts(v, dir, total)
			out[Part{layer, k}] += int64(kb)
			out[Part{layer, p}] += int64(pb)
		}
	}
	return out, it.Error()
}

// WaitScanned returns when no scan of the stored prefixes is running: the filter of
// the last one is installed, or the scan was stopped. It returns an error wrapping
// [store.ErrClosed] if the store is closed, and the context's error if ctx ends
// first. A Background pass starts its scan when its rewriting is over, so after a
// Retain the caller waits for [Instrument.WaitRetained] first. For tests and
// measurements, whose counts would otherwise depend on how fast a scan is.
func (i *Instrument) WaitScanned(ctx context.Context) error {
	s := i.s
	for {
		s.mu.Lock()
		if s.closed.Load() {
			s.mu.Unlock()
			return closedError("WaitScanned")
		}
		done := s.scanWait
		s.flushCounts()
		s.mu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return contextError("WaitScanned", ctx.Err())
		}
	}
}

// AssumeEveryPrefixMayExist makes the filter of the stored prefixes answer that
// every prefix may hold keys, so that the writer reads every prefix whose state it
// does not keep, for tests and measurements that compare the two. It changes no
// stored byte.
func (i *Instrument) AssumeEveryPrefixMayExist() {
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	i.s.filterMaybe = true
}

// CapCachedStates makes the writer keep the states of at most n prefixes, besides
// the budget of [Options.StateCacheBytes], so that a test or a measurement can make
// it evict and read again. Zero removes the cap.
func (i *Instrument) CapCachedStates(n int) {
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	i.s.states.setCap(max(n, 0))
}

// VerifyCheckpoints walks every checkpoint in the database of the current fold
// version, builds it again from what is below it, and returns an error for the
// first that has other entries than the build gives, or a W below the highest Seq
// among the records before its instant: a stale checkpoint. The B and the W of the
// two are not compared, B being the last sequence number at the build and W
// conservative. For tests and measurements.
func (i *Instrument) VerifyCheckpoints() error {
	s := i.s
	// No write or chunk changes the database while the checkpoints are listed and
	// built again.
	s.mu.Lock()
	defer s.mu.Unlock()
	type ckpt struct {
		prefix []byte
		ns     int64
		val    []byte
	}
	var found []ckpt
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			_ = it.Close()
			return err
		}
		if kind == kindCheckpoint {
			found = append(found, ckpt{slices.Clone(prefix), ns, slices.Clone(it.Value())})
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	// The builds are the store's own, through a store that counts nothing: a
	// verification is not part of what a measurement counts.
	v := &Store{kv: s.kv, rec: nopRecorder{}}
	for _, c := range found {
		stored, err := decodeStamp(c.val)
		if err != nil {
			return fmt.Errorf("pebblestore: the checkpoint at %d of prefix %x does not decode: %w", c.ns, c.prefix, err)
		}
		if stored.foldVersion != foldVersion {
			continue
		}
		raw, err := v.build(c.prefix, c.ns)
		if err != nil {
			return err
		}
		built, err := decodeStamp(raw)
		if err != nil {
			return err
		}
		floor, err := s.highestSeqBefore(c.prefix, c.ns)
		if err != nil {
			return err
		}
		if stored.w < floor {
			return fmt.Errorf("pebblestore: the checkpoint at %d of prefix %x has W %d, below %d, the highest Seq among the records before it", c.ns, c.prefix, stored.w, floor)
		}
		stored.through, built.through, stored.w, built.w = 0, 0, 0, 0
		x, xerr := appendStamp(nil, stored)
		y, yerr := appendStamp(nil, built)
		if err := errors.Join(xerr, yerr); err != nil {
			return err
		}
		if !bytes.Equal(x, y) {
			return fmt.Errorf("pebblestore: the checkpoint at %d (%d entries) of prefix %x is not what building it from the records gives (%d entries): it is stale", c.ns, len(stored.entries), c.prefix, len(built.entries))
		}
	}
	return nil
}

// highestSeqBefore is the highest Seq among the records of the prefix with an event
// time before ns.
func (s *Store) highestSeqBefore(prefix []byte, ns int64) (uint64, error) {
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	var top uint64
	for ok := it.First(); ok; ok = it.Next() {
		_, at, seq, kind, err := parseKey(it.Key())
		if err != nil {
			_ = it.Close()
			return 0, err
		}
		if kind == kindRecord && at < ns {
			top = max(top, seq)
		}
	}
	return top, errors.Join(it.Error(), it.Close())
}

// WaitRetained returns when no background pass is pending: the last chunk is
// committed, its writer state installed, the pass settled (if the store settles) and
// the marker deleted; the scan of the stored prefixes that follows the pass has
// started, and [Instrument.WaitScanned] waits for it. A Retain
// while a pass settles replaces it, and the caller then waits for the newer pass. It returns the error that stopped the pass
// if the store has latched one, an error wrapping [store.ErrClosed] if the store
// is closed with the pass still pending (the next Open resumes it), and the
// context's error if ctx ends first. In a Synchronous store, where Retain returns
// when it has finished, it returns at once.
func (i *Instrument) WaitRetained(ctx context.Context) error {
	s := i.s
	if s.retention != Background {
		return nil
	}
	for {
		s.mu.Lock()
		switch {
		case s.failed != nil:
			err := s.failedError("WaitRetained")
			s.mu.Unlock()
			return err
		case s.pass == nil:
			s.mu.Unlock()
			return nil
		case s.closed.Load() || s.passDone == nil:
			s.mu.Unlock()
			return closedError("WaitRetained")
		}
		done := s.passDone
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return contextError("WaitRetained", ctx.Err())
		}
	}
}

// invalidation is the checkpoints of one prefix that the Write in progress has
// deleted, for [Instrument.OnInvalidate].
type invalidation struct {
	prefix []byte
	n      int
}

// OnInvalidate sets fn to be called after each Write's records are committed, once
// for every prefix in which the Write's records deleted checkpoints (a record
// timed before a checkpoint makes it untrue), with the prefix and how many were
// deleted from it by the whole Write. A Write that deletes none calls nothing, and
// the counts of one Write add up to its change in "checkpoint.invalidated". Nil
// removes the hook. For tests and measurements.
//
// fn is called with the store's lock held, so it must return quickly and must not
// call the store. The prefix is the store's own: valid during the call, copy it to
// keep it. While no hook is set a Write builds no list and allocates nothing for
// this.
func (i *Instrument) OnInvalidate(fn func(prefix []byte, n int)) {
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	i.s.onInvalidate = fn
	i.s.invalidated = nil
}

// noteInvalidated records that n checkpoints of prefix were deleted by the Write
// in progress, if a hook is set to be told. The caller holds s.mu.
func (s *Store) noteInvalidated(prefix []byte, n int) {
	if s.onInvalidate == nil {
		return
	}
	for i := range s.invalidated {
		if bytes.Equal(s.invalidated[i].prefix, prefix) {
			s.invalidated[i].n += n
			return
		}
	}
	s.invalidated = append(s.invalidated, invalidation{prefix, n})
}

// reportInvalidated tells the hook what the Write that has just committed noted,
// and forgets it. The caller holds s.mu.
func (s *Store) reportInvalidated() {
	if s.onInvalidate == nil {
		return
	}
	for _, x := range s.invalidated {
		s.onInvalidate(x.prefix, x.n)
	}
	clear(s.invalidated)
	s.invalidated = s.invalidated[:0]
}
