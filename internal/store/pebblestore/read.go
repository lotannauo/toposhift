package pebblestore

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// checkArgs checks the arguments of a read, before anything is read: the scope,
// the direction (when the read has one), and the fingerprints, none of which may
// be zero. Every error wraps [store.ErrInvalid]. None of it depends on what is
// stored, so it comes before the horizon.
func checkArgs(op string, sc store.Scope, dir store.Direction, hasDir bool, fps ...identity.Fingerprint) error {
	if err := sc.Validate(); err != nil {
		return fmt.Errorf("pebblestore: %s: %w", op, err)
	}
	if hasDir && dir != store.Forward && dir != store.Reverse {
		return fmt.Errorf("pebblestore: %s: direction %s: %w", op, dir, store.ErrInvalid)
	}
	for i, fp := range fps {
		if fp.IsZero() {
			return fmt.Errorf("pebblestore: %s: fingerprint %d is the zero fingerprint: %w", op, i, store.ErrInvalid)
		}
	}
	return nil
}

// checkHorizon refuses a read of instant t (the start of the window, for a
// window) in the scope if the history it asks about may be gone: t before the
// horizon of the scope's layer, or a token below the horizon's Seq. While the
// horizon is zero nothing is refused, and a token of Latest is never below it.
func (s *Store) checkHorizon(op string, t time.Time, sc store.Scope) error {
	h := s.horizonOf(sc.Layer)
	if h.IsZero() {
		return nil
	}
	if t.Before(h.Time) {
		return fmt.Errorf("pebblestore: %s: instant %s is before the horizon %s: %w", op, formatTime(t), formatTime(h.Time), store.ErrBeforeHorizon)
	}
	if sc.AsOf < h.Seq {
		return fmt.Errorf("pebblestore: %s: token %d is below the horizon's seq %d: %w", op, sc.AsOf, h.Seq, store.ErrBeforeHorizon)
	}
	return nil
}

// reader is one read in progress: the iterator it runs on, which gives it one
// consistent view of whole committed batches, and a count of the records it
// stepped over.
type reader struct {
	s       *Store
	op      string // what the read is for, which names its iterator statistics
	it      *pebble.Iterator
	stepped int64
	// checkpoints met: used, skipped for a token below its W, skipped for another
	// fold version.
	hits, skippedW, skippedVersion int64
	// entries decoded from the checkpoints and the baseline met, whether or not a
	// checkpoint was then used: decoding is the cost of meeting one.
	ckptEntries, baselineEntries int64
}

// begin starts a read: it checks the context, opens the iterator, and only then
// checks the horizon. The order matters. A retention publishes its horizon before
// it discards anything, so a read whose horizon check passes after its iterator
// was opened can only have seen a database from before the discarding; checked
// first, it could pass and then open an iterator over a database already rewritten.
func (s *Store) begin(ctx context.Context, op string, t time.Time, sc store.Scope, opts *pebble.IterOptions) (*reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, contextError(op, err)
	}
	it, err := s.kv.NewIter(opts)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: %s: %w", op, err)
	}
	r := &reader{s: s, op: opReadName(op), it: it}
	if err := s.checkHorizon(op, t, sc); err != nil {
		_ = it.Close()
		return nil, err
	}
	return r, nil
}

// opReadName is the name the iterator statistics of a read are recorded under.
func opReadName(op string) string {
	switch op {
	case "Neighbors":
		return "neighbors"
	case "NeighborsBatch":
		return "batch"
	case "Alive":
		return "alive"
	case "Window":
		return "window"
	case "EntityWindow":
		return "entitywindow"
	}
	return op
}

func (r *reader) close() {
	r.s.rec.Count("read.records_stepped", r.stepped)
	if r.hits+r.skippedW+r.skippedVersion > 0 {
		r.s.rec.Count("read.checkpoint_hits", r.hits)
		r.s.rec.Count("read.checkpoint_skipped_w", r.skippedW)
		r.s.rec.Count("read.checkpoint_skipped_version", r.skippedVersion)
	}
	if r.ckptEntries > 0 {
		r.s.rec.Count("read.checkpoint_entries_decoded", r.ckptEntries)
	}
	if r.baselineEntries > 0 {
		r.s.rec.Count("read.baseline_entries_decoded", r.baselineEntries)
	}
	pebblekv.RecordIter(r.s.rec, r.op, r.it)
	_ = r.it.Close()
}

// decide walks the prefix from the newest record at or before the instant toward
// older ones and calls fn once for each reference, with the record (or baseline
// entry) that decides it: the newest one the token sees. It stops when fn returns
// false, and at the retention baseline, which stands for everything older.
func (r *reader) decide(prefix []byte, tNs int64, asOf uint64, fn func(ref []byte, ns int64, v pebblekv.Value) (bool, error)) error {
	dir := prefix[prefixLen-1]
	lo, hi := prefixBounds(prefix)
	r.it.SetBounds(lo, hi)
	decided := map[string]struct{}{}
	for ok := r.it.SeekGE(seekKey(prefix, tNs)); ok; ok = r.it.Next() {
		_, ns, seq, kind, err := parseKey(r.it.Key())
		if err != nil {
			return err
		}
		switch kind {
		case kindRecord:
			r.stepped++
			if seq > asOf {
				continue
			}
			ref, v, err := decodeRecordValue(dir, r.it.Value())
			if err != nil {
				return err
			}
			if v.Seq != seq {
				return fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			if _, done := decided[string(ref)]; done {
				continue
			}
			decided[string(ref)] = struct{}{}
			more, err := fn(ref, ns, v)
			if err != nil || !more {
				return err
			}
		case kindCheckpoint:
			st, err := decodeStamp(r.it.Value())
			if err != nil {
				return err
			}
			if st.kind != kindCheckpoint {
				return fmt.Errorf("pebblestore: a checkpoint key holds a stamp of kind %d", st.kind)
			}
			r.ckptEntries += int64(len(st.entries))
			// Use it only if it was built by this fold logic and every record it
			// depends on is visible to the token; otherwise walk on to an older
			// one, or to the baseline.
			if st.foldVersion != foldVersion {
				r.skippedVersion++
				continue
			}
			if st.w > asOf {
				r.skippedW++
				continue
			}
			r.hits++
			for _, en := range st.entries {
				if _, done := decided[string(en.ref)]; done {
					continue
				}
				more, err := fn(en.ref, en.eventNs, en.value)
				if err != nil || !more {
					return err
				}
			}
			return r.it.Error()
		case kindBaseline:
			st, err := decodeStamp(r.it.Value())
			if err != nil {
				return err
			}
			if st.kind != kindBaseline {
				return fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
			}
			r.baselineEntries += int64(len(st.entries))
			for _, en := range st.entries {
				if _, done := decided[string(en.ref)]; done {
					continue
				}
				more, err := fn(en.ref, en.eventNs, en.value)
				if err != nil || !more {
					return err
				}
			}
			return r.it.Error()
		}
	}
	return r.it.Error()
}

func (r *reader) neighbors(fp identity.Fingerprint, dir store.Direction, tNs int64, sc store.Scope) ([]store.Neighbor, error) {
	prefix, ok := r.s.prefixOf(sc.Layer, fp, byte(dir))
	if !ok {
		return nil, nil
	}
	alive := map[string]struct{}{} // subjects with a live reference
	err := r.decide(prefix, tNs, sc.AsOf, func(ref []byte, ns int64, v pebblekv.Value) (bool, error) {
		if v.Holds(ns, tNs) {
			alive[string(ref[:subjectLen])] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	var out []store.Neighbor
	for subject := range alive {
		peer, rel, err := r.s.subjectOf([]byte(subject))
		if err != nil {
			return nil, err
		}
		out = append(out, store.Neighbor{Peer: peer, Relation: rel})
	}
	store.SortNeighbors(out)
	return out, nil
}

// subjectOf reads the peer and the relation at the start of a reference.
func (s *Store) subjectOf(ref []byte) (identity.Fingerprint, catalog.RelationType, error) {
	peer, err := s.ids.Fingerprint(ref)
	if err != nil {
		return identity.Fingerprint{}, "", err
	}
	rel, ok := s.ids.Relation(catalog.RelationID(uint16(ref[peerLen])<<8 | uint16(ref[peerLen+1])))
	if !ok {
		return identity.Fingerprint{}, "", fmt.Errorf("pebblestore: a stored relation id is unknown")
	}
	return peer, rel, nil
}

// instant converts a read's instant to nanoseconds, or says nothing can exist at
// it (before 1970). After the last representable instant it is the largest
// int64, which no deadline exceeds.
func instant(t time.Time) (ns int64, empty bool) {
	ns, where := pebblekv.Locate(t)
	return ns, where == pebblekv.Before
}

// Neighbors implements [store.Store].
func (s *Store) Neighbors(ctx context.Context, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if s.closed.Load() {
		return nil, closedError("Neighbors")
	}
	if err := checkArgs("Neighbors", sc, dir, true, fp); err != nil {
		return nil, err
	}
	r, err := s.begin(ctx, "Neighbors", t, sc, nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	tNs, empty := instant(t)
	if empty {
		return nil, nil
	}
	return r.neighbors(fp, dir, tNs, sc)
}

// NeighborsBatch implements [store.Store]: the naive loop on one iterator, so the
// whole batch is one consistent view.
func (s *Store) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	if s.closed.Load() {
		return nil, closedError("NeighborsBatch")
	}
	if err := checkArgs("NeighborsBatch", sc, dir, true, fps...); err != nil {
		return nil, err
	}
	r, err := s.begin(ctx, "NeighborsBatch", t, sc, nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	out := make([][]store.Neighbor, len(fps))
	tNs, empty := instant(t)
	if empty || len(fps) == 0 {
		return out, nil
	}
	for i, fp := range fps {
		if out[i], err = r.neighbors(fp, dir, tNs, sc); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Alive implements [store.Store]. With a policy that names a boot key it folds
// every record of the entity the scope sees (see quarantine.go); otherwise it
// walks the entity's records back to the first live reference.
func (s *Store) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if s.closed.Load() {
		return false, closedError("Alive")
	}
	if err := checkArgs("Alive", sc, 0, false, fp); err != nil {
		return false, err
	}
	r, err := s.begin(ctx, "Alive", t, sc, nil)
	if err != nil {
		return false, err
	}
	defer r.close()
	prefix, ok := s.prefixOf(sc.Layer, fp, dirEntity)
	if !ok {
		return false, nil
	}
	if s.policy.BootKey != "" {
		return r.aliveFolded(prefix, fp, t, sc)
	}
	tNs, empty := instant(t)
	if empty {
		return false, nil
	}
	alive := false
	err = r.decide(prefix, tNs, sc.AsOf, func(_ []byte, ns int64, v pebblekv.Value) (bool, error) {
		if v.Holds(ns, tNs) {
			alive = true
			return false, nil // one producer's live reference is enough
		}
		return true, nil
	})
	return alive, err
}

// window returns the records under a prefix with from <= EventTime < to that the
// scope sees, newest first; the payloads are copies. dir is 0 for the entity's own
// records.
func (r *reader) window(fp identity.Fingerprint, dir byte, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	// The window is the instants lo to hi inclusive, in nanoseconds.
	lo, loWhere := pebblekv.Locate(from)
	if loWhere == pebblekv.After {
		return nil, nil
	}
	hi, hiWhere := pebblekv.Locate(to)
	switch hiWhere {
	case pebblekv.Before:
		return nil, nil
	case pebblekv.Inside:
		hi-- // to is exclusive
	}
	if hi < lo {
		return nil, nil
	}
	prefix, ok := r.s.prefixOf(sc.Layer, fp, dir)
	if !ok {
		return nil, nil
	}
	lowerBound, upperBound := prefixBounds(prefix)
	r.it.SetBounds(lowerBound, upperBound)

	var out []store.Record
	for ok := r.it.SeekGE(seekKey(prefix, hi)); ok; ok = r.it.Next() {
		_, ns, seq, kind, err := parseKey(r.it.Key())
		if err != nil {
			return nil, err
		}
		if ns < lo {
			break
		}
		if kind != kindRecord { // a checkpoint or the baseline is not a record
			continue
		}
		r.stepped++
		if seq > sc.AsOf {
			continue
		}
		ref, v, err := decodeRecordValue(dir, r.it.Value())
		if err != nil {
			return nil, err
		}
		if v.Seq != seq {
			return nil, fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
		}
		rec := store.Record{
			Layer: sc.Layer, EventTime: pebblekv.Time(ns), Seq: seq, Kind: v.Kind, TTL: v.TTL,
			Boot: v.Boot, EventTimeBasis: store.EventTimeBasis(v.Basis),
		}
		if v.HasThrough {
			rec.Through = pebblekv.Time(v.Through)
		}
		if len(v.Payload) > 0 {
			rec.Payload = bytes.Clone(v.Payload)
		}
		if dir == dirEntity {
			rec.Producer = producerOf(dir, ref)
			rec.Subject = store.EntitySubject(fp)
		} else {
			peer, rel, err := r.s.subjectOf(ref)
			if err != nil {
				return nil, err
			}
			rec.Producer = producerOf(dir, ref)
			if store.Direction(dir) == store.Forward {
				rec.Subject = store.EdgeSubject(fp, peer, rel)
			} else {
				rec.Subject = store.EdgeSubject(peer, fp, rel)
			}
		}
		out = append(out, rec)
	}
	if err := r.it.Error(); err != nil {
		return nil, err
	}
	store.SortRecords(out)
	return out, nil
}

// Window implements [store.Store].
func (s *Store) Window(ctx context.Context, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if s.closed.Load() {
		return nil, closedError("Window")
	}
	if err := checkArgs("Window", sc, dir, true, fp); err != nil {
		return nil, err
	}
	r, err := s.begin(ctx, "Window", from, sc, nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	if !from.Before(to) {
		return nil, nil
	}
	return r.window(fp, byte(dir), from, to, sc)
}

// EntityWindow implements [store.Store]. It returns the records of the entity's
// own existence, never those of an edge.
func (s *Store) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if s.closed.Load() {
		return nil, closedError("EntityWindow")
	}
	if err := checkArgs("EntityWindow", sc, 0, false, fp); err != nil {
		return nil, err
	}
	r, err := s.begin(ctx, "EntityWindow", from, sc, nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	if !from.Before(to) {
		return nil, nil
	}
	return r.window(fp, dirEntity, from, to, sc)
}

// producerOf is the producer of a reference: all of an entity's, and what follows
// the peer and relation of an edge's.
func producerOf(dir byte, ref []byte) lifecycle.Producer {
	if dir == dirEntity {
		return lifecycle.Producer(ref)
	}
	return lifecycle.Producer(ref[subjectLen:])
}
