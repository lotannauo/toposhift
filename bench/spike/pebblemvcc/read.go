package pebblemvcc

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/sstable"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func checkDir(dir engine.Direction) error {
	if dir != engine.Forward && dir != engine.Reverse {
		return fmt.Errorf("direction %s: %w", dir, engine.ErrInvalid)
	}
	return nil
}

// prefixOf is the prefix of a read, or ok false if the entity's type is not one
// the catalog knows: nothing can be stored for it, so a read of it is empty.
func (e *Engine) prefixOf(layer catalog.Layer, fp identity.Fingerprint, dir byte) (prefix []byte, ok bool) {
	prefix, err := e.appendPrefix(make([]byte, 0, prefixLen), layer, fp, dir)
	return prefix, err == nil
}

// instant converts a read's instant to its wall time and its nanoseconds, or
// says nothing can exist at it (before 1970). After the last representable
// instant the nanoseconds are the largest int64, which no deadline exceeds.
func instant(t time.Time) (wall uint64, ns int64, empty bool) {
	ns, where := pebblekv.Locate(t)
	if where == pebblekv.Before {
		return 0, 0, true
	}
	return wallOf(ns), ns, false
}

// reader is one read in progress: the iterator it runs on, which gives it one
// consistent view of whole committed batches, and a count of the versions it
// stepped over.
type reader struct {
	e       *Engine
	op      string // what the read is for, which names its iterator statistics
	it      *pebble.Iterator
	stepped int64
}

func (e *Engine) newReader(op string, opts *pebble.IterOptions) (*reader, error) {
	it, err := e.kv.NewIter(opts)
	if err != nil {
		return nil, err
	}
	return &reader{e: e, op: op, it: it}, nil
}

func (r *reader) close() {
	r.e.rec.Count("read.versions_stepped", r.stepped)
	pebblekv.RecordIter(r.e.rec, r.op, r.it)
	_ = r.it.Close()
}

// walkLive calls fn for each roach key under prefix whose newest version at or
// before the instant, among those the token sees, is a live reference, in key
// order, until fn returns false or an error. wallT is the wall of the instant and
// tNs the instant itself, which can differ at the far end of the range: see
// [instant].
func (r *reader) walkLive(prefix []byte, wallT uint64, tNs int64, asOf uint64, fn func(roach []byte) (bool, error)) error {
	lo, hi := prefixBounds(prefix)
	r.it.SetBounds(lo, hi)
	ok := r.it.First()
	for ok {
		roach, wall, _, err := split(r.it.Key())
		if err != nil {
			return err
		}
		if wall > wallT {
			// Newer than the instant: go to the newest version at or before it.
			// That may be a different roach key, which the loop then reads.
			ok = r.it.SeekGE(atOrBefore(roach, wallT))
			continue
		}
		roach = append([]byte(nil), roach...)
		found, live := false, false
		for ok {
			cur, w, _, err := split(r.it.Key())
			if err != nil {
				return err
			}
			if !bytes.Equal(cur, roach) {
				break // no version of roach is visible; this is the next key
			}
			v, err := pebblekv.DecodeValue(r.it.Value())
			if err != nil {
				return err
			}
			r.stepped++
			if v.Seq <= asOf {
				found, live = true, v.Holds(nanosOf(w), tNs)
				break
			}
			ok = r.it.Next()
		}
		if !found {
			continue
		}
		if live {
			more, err := fn(roach)
			if err != nil {
				return err
			}
			if !more {
				return r.it.Error()
			}
		}
		ok = r.it.NextPrefix()
	}
	return r.it.Error()
}

func (r *reader) neighbors(fp identity.Fingerprint, dir engine.Direction, wallT uint64, tNs int64, s engine.Scope) ([]engine.Neighbor, error) {
	prefix, ok := r.e.prefixOf(s.Layer, fp, byte(dir))
	if !ok {
		return nil, nil
	}
	var out []engine.Neighbor
	var last []byte // the peer and relation of the neighbor just added
	err := r.walkLive(prefix, wallT, tNs, s.AsOf, func(roach []byte) (bool, error) {
		subject := roach[prefixLen:edgeKeyLen]
		if last != nil && bytes.Equal(subject, last) {
			return true, nil // another producer's reference to an edge already alive
		}
		peer, rel, _, err := r.e.edgeOf(roach)
		if err != nil {
			return false, err
		}
		last = append(last[:0], subject...)
		out = append(out, engine.Neighbor{Peer: peer, Relation: rel})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	engine.SortNeighbors(out)
	return out, nil
}

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	wallT, tNs, empty := instant(t)
	if empty {
		return nil, nil
	}
	r, err := e.newReader("neighbors", nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	return r.neighbors(fp, dir, wallT, tNs, s)
}

// NeighborsBatch implements [engine.Engine]. It is the naive loop on one
// iterator, so the whole batch is one consistent view; the seeks are not yet
// ordered by key, which the measurement stage may find worth doing.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	out := make([][]engine.Neighbor, len(fps))
	wallT, tNs, empty := instant(t)
	if empty || len(fps) == 0 {
		return out, nil
	}
	r, err := e.newReader("batch", nil)
	if err != nil {
		return nil, err
	}
	defer r.close()
	for i, fp := range fps {
		if out[i], err = r.neighbors(fp, dir, wallT, tNs, s); err != nil {
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
	wallT, tNs, empty := instant(t)
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
	err = r.walkLive(prefix, wallT, tNs, s.AsOf, func([]byte) (bool, error) {
		alive = true
		return false, nil // one producer's live reference is enough
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
	wallLo, wallHi := wallOf(lo), wallOf(hi)

	lowerBound, upperBound := prefixBounds(prefix)
	opts := &pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound}
	if e.kv.Config().TimeFilter {
		opts.PointKeyFilters = []sstable.BlockPropertyFilter{cockroachkvs.NewMVCCTimeIntervalFilter(wallLo, wallHi)}
	}
	r, err := e.newReader("window", opts)
	if err != nil {
		return nil, err
	}
	defer r.close()

	var out []engine.Record
	ok = r.it.First()
	for ok {
		roach, wall, _, err := split(r.it.Key())
		if err != nil {
			return nil, err
		}
		switch {
		case wall > wallHi:
			ok = r.it.SeekGE(atOrBefore(roach, wallHi))
			continue
		case wall < wallLo:
			ok = r.it.NextPrefix()
			continue
		}
		roach = append([]byte(nil), roach...)
		peer, rel, producer, err := e.edgeOf(roach)
		if err != nil {
			return nil, err
		}
		older := false // stopped on an older version of this same key
		for ok {
			cur, w, _, err := split(r.it.Key())
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(cur, roach) {
				break
			}
			if w < wallLo {
				older = true
				break
			}
			v, err := pebblekv.DecodeValue(r.it.Value())
			if err != nil {
				return nil, err
			}
			r.stepped++
			if v.Seq <= s.AsOf {
				rec := engine.Record{
					Layer: s.Layer, Producer: producer, EventTime: pebblekv.Time(nanosOf(w)),
					Seq: v.Seq, Kind: v.Kind, TTL: v.TTL,
				}
				if v.HasThrough {
					rec.Through = pebblekv.Time(v.Through)
				}
				if len(v.Payload) > 0 {
					rec.Payload = append([]byte(nil), v.Payload...)
				}
				if dir == engine.Forward {
					rec.Subject = engine.EdgeSubject(fp, peer, rel)
				} else {
					rec.Subject = engine.EdgeSubject(peer, fp, rel)
				}
				out = append(out, rec)
			}
			ok = r.it.Next()
		}
		if older {
			ok = r.it.NextPrefix() // the rest of this key is older than the window too
		}
	}
	if err := r.it.Error(); err != nil {
		return nil, err
	}
	engine.SortRecords(out)
	return out, nil
}
