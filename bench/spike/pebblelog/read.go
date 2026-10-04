package pebblelog

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func checkDir(dir engine.Direction) error {
	if dir != engine.Forward && dir != engine.Reverse {
		return fmt.Errorf("direction %s: %w", dir, engine.ErrInvalid)
	}
	return nil
}

// reader is one read in progress: the iterator it runs on, which gives it one
// consistent view of whole committed batches, and a count of the records it
// stepped over.
type reader struct {
	e       *Engine
	op      string // what the read is for, which names its iterator statistics
	it      *pebble.Iterator
	stepped int64
	// checkpoints met: used, skipped for a token below its W, skipped for another
	// fold version.
	hits, skippedW, skippedVersion int64
}

func (e *Engine) newReader(op string, opts *pebble.IterOptions) (*reader, error) {
	it, err := e.kv.NewIter(opts)
	if err != nil {
		return nil, err
	}
	return &reader{e: e, op: op, it: it}, nil
}

func (r *reader) close() {
	r.e.rec.Count("read.records_stepped", r.stepped)
	if r.hits+r.skippedW+r.skippedVersion > 0 {
		r.e.rec.Count("read.checkpoint_hits", r.hits)
		r.e.rec.Count("read.checkpoint_skipped_w", r.skippedW)
		r.e.rec.Count("read.checkpoint_skipped_version", r.skippedVersion)
	}
	pebblekv.RecordIter(r.e.rec, r.op, r.it)
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
				return fmt.Errorf("pebblelog: a record's key has seq %d and its value %d", seq, v.Seq)
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
			if st.Kind != kindCheckpoint {
				return fmt.Errorf("pebblelog: a checkpoint key holds a stamp of kind %d", st.Kind)
			}
			// Use it only if it was built by this fold logic and every record it
			// depends on is visible to the token; otherwise walk on to an older
			// one, or to the baseline.
			if st.FoldVersion != FoldVersion {
				r.skippedVersion++
				continue
			}
			if st.W > asOf {
				r.skippedW++
				continue
			}
			r.hits++
			for _, en := range st.Entries {
				if _, done := decided[string(en.Ref)]; done {
					continue
				}
				more, err := fn(en.Ref, en.EventNs, en.Value)
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
			if st.Kind != kindBaseline {
				return fmt.Errorf("pebblelog: a baseline key holds a stamp of kind %d", st.Kind)
			}
			for _, en := range st.Entries {
				if _, done := decided[string(en.Ref)]; done {
					continue
				}
				more, err := fn(en.Ref, en.EventNs, en.Value)
				if err != nil || !more {
					return err
				}
			}
			return r.it.Error()
		}
	}
	return r.it.Error()
}

func (r *reader) neighbors(fp identity.Fingerprint, dir engine.Direction, tNs int64, s engine.Scope) ([]engine.Neighbor, error) {
	prefix, ok := r.e.prefixOf(s.Layer, fp, byte(dir))
	if !ok {
		return nil, nil
	}
	alive := map[string]struct{}{} // subjects with a live reference
	err := r.decide(prefix, tNs, s.AsOf, func(ref []byte, ns int64, v pebblekv.Value) (bool, error) {
		if v.Holds(ns, tNs) {
			alive[string(ref[:subjectLen])] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	var out []engine.Neighbor
	for subject := range alive {
		peer, err := r.e.ids.Fingerprint([]byte(subject))
		if err != nil {
			return nil, err
		}
		rel, ok := r.e.ids.Relation(uint16(subject[peerLen])<<8 | uint16(subject[peerLen+1]))
		if !ok {
			return nil, fmt.Errorf("pebblelog: a stored relation id is unknown")
		}
		out = append(out, engine.Neighbor{Peer: peer, Relation: rel})
	}
	engine.SortNeighbors(out)
	return out, nil
}

// instant converts a read's instant to nanoseconds, or says nothing can exist at
// it (before 1970). After the last representable instant it is the largest
// int64, which no deadline exceeds.
func instant(t time.Time) (ns int64, empty bool) {
	ns, where := pebblekv.Locate(t)
	return ns, where == pebblekv.Before
}

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	tNs, empty := instant(t)
	if empty {
		return nil, nil
	}
	r, err := e.newReader("neighbors", nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	return r.neighbors(fp, dir, tNs, s)
}

// NeighborsBatch implements [engine.Engine]: the naive loop on one iterator, so
// the whole batch is one consistent view.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	out := make([][]engine.Neighbor, len(fps))
	tNs, empty := instant(t)
	if empty || len(fps) == 0 {
		return out, nil
	}
	r, err := e.newReader("batch", nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	for i, fp := range fps {
		if out[i], err = r.neighbors(fp, dir, tNs, s); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Alive implements [engine.Engine].
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	if err := s.Validate(); err != nil {
		return false, err
	}
	tNs, empty := instant(t)
	if empty {
		return false, nil
	}
	prefix, ok := e.prefixOf(s.Layer, fp, dirEntity)
	if !ok {
		return false, nil
	}
	r, err := e.newReader("alive", nil)
	if err != nil {
		return false, err
	}
	defer r.close()
	alive := false
	err = r.decide(prefix, tNs, s.AsOf, func(_ []byte, ns int64, v pebblekv.Value) (bool, error) {
		if v.Holds(ns, tNs) {
			alive = true
			return false, nil // one producer's live reference is enough
		}
		return true, nil
	})
	return alive, err
}

// Window implements [engine.Engine].
func (e *Engine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	if !to.After(from) {
		return nil, nil
	}
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
	prefix, ok := e.prefixOf(s.Layer, fp, byte(dir))
	if !ok {
		return nil, nil
	}
	lowerBound, upperBound := prefixBounds(prefix)
	r, err := e.newReader("window", &pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, err
	}
	defer r.close()

	var out []engine.Record
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
		if seq > s.AsOf {
			continue
		}
		ref, v, err := decodeRecordValue(prefix[prefixLen-1], r.it.Value())
		if err != nil {
			return nil, err
		}
		if v.Seq != seq {
			return nil, fmt.Errorf("pebblelog: a record's key has seq %d and its value %d", seq, v.Seq)
		}
		rec := engine.Record{
			Layer: s.Layer, Producer: lifecycle.Producer(ref[subjectLen:]), EventTime: pebblekv.Time(ns),
			Seq: seq, Kind: v.Kind, TTL: v.TTL,
		}
		if v.HasThrough {
			rec.Through = pebblekv.Time(v.Through)
		}
		if len(v.Payload) > 0 {
			rec.Payload = bytes.Clone(v.Payload)
		}
		peer, err := e.ids.Fingerprint(ref)
		if err != nil {
			return nil, err
		}
		relID := uint16(ref[peerLen])<<8 | uint16(ref[peerLen+1])
		rel, ok := e.ids.Relation(relID)
		if !ok {
			return nil, fmt.Errorf("pebblelog: a stored relation id is unknown")
		}
		if dir == engine.Forward {
			rec.Subject = engine.EdgeSubject(fp, peer, rel)
		} else {
			rec.Subject = engine.EdgeSubject(peer, fp, rel)
		}
		out = append(out, rec)
	}
	if err := r.it.Error(); err != nil {
		return nil, err
	}
	engine.SortRecords(out)
	return out, nil
}
