package pebblestore

import (
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// assertionOf is the lifecycle assertion of a stored record, or of a baseline's
// entry (which has no payload, and whose boot is kept).
func assertionOf(dir byte, ref []byte, ns int64, v pebblekv.Value) lifecycle.Assertion {
	r := store.Record{
		Producer: producerOf(dir, ref), EventTime: pebblekv.Time(ns), Seq: v.Seq, Kind: v.Kind, TTL: v.TTL,
		Payload: v.Payload, Boot: v.Boot,
	}
	if v.HasThrough {
		r.Through = pebblekv.Time(v.Through)
	}
	return r.Assertion()
}

// aliveFolded answers Alive for a store whose policy names a boot key. Whether an
// entity is quarantined is decided by folding everything the scope sees of it,
// whatever the instant is, so the whole prefix is read: every record the token
// sees, and the entries of the retention baseline, which stand for the records
// before the horizon. The answer is then the timeline's.
func (r *reader) aliveFolded(prefix []byte, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	lo, hi := prefixBounds(prefix)
	r.it.SetBounds(lo, hi)
	var as []lifecycle.Assertion
	for ok := r.it.First(); ok; ok = r.it.Next() {
		_, ns, seq, kind, err := parseKey(r.it.Key())
		if err != nil {
			return false, err
		}
		switch kind {
		case kindRecord:
			r.stepped++
			if seq > sc.AsOf {
				continue
			}
			ref, v, err := decodeRecordValue(dirEntity, r.it.Value())
			if err != nil {
				return false, err
			}
			if v.Seq != seq {
				return false, fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			as = append(as, assertionOf(dirEntity, ref, ns, v))
		case kindBaseline:
			st, err := decodeStamp(r.it.Value())
			if err != nil {
				return false, err
			}
			if st.kind != kindBaseline {
				return false, fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
			}
			r.baselineEntries += int64(len(st.entries))
			// Every entry's Seq is at most the retention's, and a read below that
			// is refused, so the token sees all of them.
			for _, en := range st.entries {
				as = append(as, assertionOf(dirEntity, en.ref, en.eventNs, en.value))
			}
		}
	}
	if err := r.it.Error(); err != nil {
		return false, err
	}
	tl, err := lifecycle.Fold(as, r.s.policy)
	if err != nil {
		var cc *lifecycle.CloneCollisionError
		if errors.As(err, &cc) {
			collision := *cc
			return false, &store.QuarantineError{Entity: fp, Layer: sc.Layer, Collision: &collision}
		}
		return false, fmt.Errorf("pebblestore: Alive: folding %s: %w", fp, err)
	}
	return tl.AliveAt(t), nil
}
