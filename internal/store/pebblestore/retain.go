package pebblestore

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Retain implements [store.Store]. It lets the store discard history before
// horizon, in four steps.
//
// First the horizon of every layer it moves is committed on its own, with the
// last sequence number L at that moment, and published to readers and writers:
// from then on a write older than it, and a read of an instant before it or of a token below L,
// is refused, so nothing can arrive for a prefix whose past is being collapsed,
// and no read can ask for what is being discarded. If the process stops after it,
// the prefixes not yet rewritten keep their old records, which no answer at or
// after the horizon depends on being absent, and only a Retain with a later
// horizon reclaims the space (one with this horizon is a no-op).
//
// Then each prefix that has history before the horizon is rewritten in one commit:
// the prefix is replayed, a baseline at the horizon holds every reference still
// alive at it, and everything older is range-deleted, the previous baseline
// included. The commits fall between prefixes and carry about 4 MiB each, and are
// not synced.
//
// Last, with Config.SettleRetention, the database is flushed and waited on until
// it is at rest, or until the deadline passes (see [pebblekv.KV.SettleAfterRetention]).
//
// A prefix with a boot in a record it would discard is kept whole when the store's
// policy names a boot key (see the package comment). A Retain that does not move
// any layer's horizon changes nothing and does not raise LastSeq. A Retain moves
// the horizon of each layer it is after, and only of those: the others keep
// theirs, and their data is left alone. It holds the store's lock for as long as
// it runs, settling included, so Close must not be called meanwhile.
func (s *Store) Retain(ctx context.Context, horizon time.Time) error {
	if s.closed.Load() {
		return closedError("Retain")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Retain", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failedError("Retain")
	}
	s.last = retainPhases{}
	// The clock starts after the lock, so a writer's wait is not counted as work.
	// The work ends at the last commit (workEnd), or at the return on any path that
	// does not reach it.
	start := time.Now()
	var workEnd time.Time
	defer func() {
		if workEnd.IsZero() {
			workEnd = time.Now()
		}
		s.last.work = workEnd.Sub(start)
	}()
	horizon = horizon.UTC()
	// The layers whose horizon this moves: those it is after. A layer whose horizon
	// is at or after it keeps its own, and its data is not touched.
	var moved [layers]bool
	anyMoved := false
	for i, h := range s.horizons.Load() {
		if horizon.After(h.Time) {
			moved[i], anyMoved = true, true
		}
	}
	if !anyMoved {
		return nil
	}
	last := s.lastSeq.Load()
	if err := s.publishHorizon(horizon, last, moved); err != nil {
		return err
	}

	hNs, where := pebblekv.Locate(horizon)
	if where == pebblekv.Before || (where == pebblekv.Inside && hNs == 0) {
		return nil // no instant a record can have is before it: nothing changes
	}
	// The newest instant strictly before the horizon, and where the baseline is
	// keyed. After the end of the range the baseline is keyed at the last instant,
	// inside what the range delete covers, so it is written after the delete.
	oldMax, baseNs := hNs-1, hNs
	if where == pebblekv.After {
		oldMax, baseNs = math.MaxInt64, math.MaxInt64
	}

	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	closeIter := sync.OnceFunc(func() { _ = it.Close() })
	defer closeIter()
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()

	boots := s.policy.BootKey != ""
	var visited, replayed, records, baselines, deletes, seeks, kept, commits int64
	commit := func() error {
		if b.Empty() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return contextError("Retain", err)
		}
		// The horizon is already published and durable: the rewriting below it is
		// not synced, because a later synced commit keeps it ordered behind.
		if err := s.kv.Apply(b, pebble.NoSync); err != nil {
			return err
		}
		if s.afterRetainCommit != nil {
			s.afterRetainCommit()
		}
		b.Reset()
		if commits++; s.stopAfter > 0 && commits >= int64(s.stopAfter) {
			return errInjected
		}
		return nil
	}
	for ok := it.First(); ok; {
		key := it.Key()
		if len(key) < prefixLen {
			return fmt.Errorf("pebblestore: key %x is shorter than a prefix", key)
		}
		prefix := slices.Clone(key[:prefixLen])
		if l, known := pebblekv.LayerFromByte(prefix[0]); !known || !moved[int(l)-int(catalog.L0)] {
			seeks++
			ok = it.SeekGE([]byte{prefix[0] + 1}) // the whole layer is left as it is
			continue
		}
		dir := prefix[prefixLen-1]
		visited++
		// Go to the newest record of this prefix strictly before the horizon. The
		// seek may land in a later prefix, which the loop then takes up.
		seeks++
		ok = it.SeekGE(seekKey(prefix, oldMax))
		if !ok || !hasPrefix(it.Key(), prefix) {
			continue // nothing is rewritten here
		}
		replayed++
		decided := map[string]struct{}{}
		var entries []entry
		bootSeen := false
		consider := func(ref []byte, ns int64, v pebblekv.Value) {
			if _, done := decided[string(ref)]; done {
				return
			}
			decided[string(ref)] = struct{}{}
			if v.Holds(ns, hNs) {
				entries = append(entries, entryOf(ref, ns, v))
			}
		}
		for ; ok && hasPrefix(it.Key(), prefix); ok = it.Next() {
			_, ns, seq, kind, err := parseKey(it.Key())
			if err != nil {
				return err
			}
			switch kind {
			case kindRecord:
				records++
				ref, v, err := decodeRecordValue(dir, it.Value())
				if err != nil {
					return err
				}
				if v.Seq != seq {
					return fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
				}
				bootSeen = bootSeen || v.Boot != ""
				consider(ref, ns, v)
			case kindBaseline:
				st, err := decodeStamp(it.Value())
				if err != nil {
					return err
				}
				if st.kind != kindBaseline {
					return fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
				}
				for _, en := range st.entries {
					consider(en.ref, en.eventNs, en.value)
				}
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		if boots && dir == dirEntity && bootSeen {
			// A later record of this entity may collide with a boot in what would
			// be discarded, and the baseline keeps no boot history: the prefix
			// stays whole, which the contract allows.
			kept++
		} else {
			// Everything older than the horizon goes, the previous baseline
			// included; what is still alive at the horizon is one new baseline, in
			// the same commit and after the delete.
			if err := b.DeleteRange(seekKey(prefix, oldMax), prefixSucc(prefix), nil); err != nil {
				return err
			}
			deletes++
			if s.anyCkpt && where == pebblekv.Inside {
				// A checkpoint exactly at the horizon would sort before the
				// baseline. The writer never writes one at or below the horizon,
				// and the ones in the prefixes this retention rewrites go.
				if err := b.Delete(stampKey(prefix, hNs, kindCheckpoint), nil); err != nil {
					return err
				}
			}
			if len(entries) > 0 {
				val, err := appendStamp(nil, stamp{kind: kindBaseline, foldVersion: foldVersion, through: last, w: last, horizon: horizon, entries: entries})
				if err != nil {
					return err
				}
				if err := b.Set(stampKey(prefix, baseNs, kindBaseline), val, nil); err != nil {
					return err
				}
				baselines++
			}
			if b.Len() >= s.retainBytes {
				if err := commit(); err != nil {
					return err
				}
			}
		}
		seeks++
		ok = it.SeekGE(prefixSucc(prefix))
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := commit(); err != nil {
		return err
	}
	s.rec.Count("retain.prefixes_visited", visited)
	s.rec.Count("retain.prefixes_replayed", replayed)
	s.rec.Count("retain.prefixes_kept_for_boots", kept)
	s.rec.Count("retain.records_replayed", records)
	s.rec.Count("retain.baselines_written", baselines)
	s.rec.Count("retain.range_deletes", deletes)
	s.rec.Count("retain.seeks", seeks)
	workEnd = time.Now()
	// Settling changes no data. The iterator is closed first: it would pin the
	// tables the compactions replace.
	closeIter()
	return s.settleRetention()
}

// publishHorizon commits the new horizon of every layer, with the sequence number
// L, in a commit of its own and synced if the database is, and then publishes it
// to readers and writers. The commit comes first: a horizon that is published but
// not stored would be forgotten by a restart that then accepts a write below it.
func (s *Store) publishHorizon(horizon time.Time, last uint64, moved [layers]bool) error {
	raw, err := pebblekv.EncodeLayerHorizon(horizon, last)
	if err != nil {
		return err
	}
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	hs := *s.horizons.Load() // a copy, with the moved layers replaced
	first := -1
	for i := range hs {
		if !moved[i] {
			continue
		}
		if first < 0 {
			first = i
		}
		hs[i] = store.Horizon{Time: horizon, Seq: last}
		if err := b.Set(metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(i))), raw, nil); err != nil {
			return err
		}
	}
	if err := s.commitHorizon(b); err != nil {
		// Whether the commit is stored is decided only when the store is reopened
		// (see [Store.uncertainCommit]); until then the store stops writing and
		// retaining. The horizon is published if the database shows it, because
		// refusing reads and writes before it is the safe side of not knowing.
		raw, rerr := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L0 + catalog.Layer(first))))
		if rerr != nil {
			s.failed = fmt.Errorf("a horizon commit failed (%w), and the horizon cannot be read back (%w): reopen the store", err, rerr)
			return s.failedError("Retain")
		}
		if t, seq, derr := pebblekv.DecodeLayerHorizon(raw); derr == nil && t.Equal(horizon) && seq == last {
			s.horizons.Store(&hs)
		}
		s.failed = fmt.Errorf("a horizon commit failed (%w), so whether it is stored is decided only when the store is reopened: reopen the store", err)
		return s.failedError("Retain")
	}
	s.horizons.Store(&hs)
	return nil
}

// commitHorizon commits the batch of horizons, synced if the database is.
func (s *Store) commitHorizon(b *pebble.Batch) error {
	if s.beforeHorizonApply != nil {
		if err := s.beforeHorizonApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	if err := s.kv.Apply(b, s.kv.WriteOptions()); err != nil {
		return err
	}
	if s.afterHorizonApply != nil {
		return s.afterHorizonApply() // tests: a commit that landed, reported as failed
	}
	return nil
}
