package pebblestore

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
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

// bootAssertionOf is assertionOf with only the attribute named key, the boot key,
// kept: all a fold of the boots reads of it.
func bootAssertionOf(dir byte, ref []byte, ns int64, v pebblekv.Value, key catalog.AttributeKey) lifecycle.Assertion {
	a := assertionOf(dir, ref, ns, v)
	a.Attrs = slices.DeleteFunc(a.Attrs, func(x identity.Attr) bool { return x.Key != key })
	return a
}

// foldsWhole says whether Alive folds the whole of a data prefix. That is true of the
// existence prefix (direction 0) of a host in a store whose policy names a boot key,
// and of no other. Whether an entity is quarantined depends on every boot its
// records report, so the prefix of a host is read whole.
//
// Only a host record carries a boot id: [store.Record.Validate] refuses it on every
// other, and Write validates every record. So the prefix of any other entity has no
// boot and no collision, and is read as it is in a store with no boot key, by the
// walk back from the instant, which stops at a checkpoint. Its checkpoints are
// placed, kept and used as there.
func (s *Store) foldsWhole(prefix []byte) bool { return foldsWhole(s, prefix) }

// foldsWholeKey is foldsWhole for a prefix held as a string, as the writer's map
// keys it.
func (s *Store) foldsWholeKey(prefix string) bool { return foldsWhole(s, prefix) }

func foldsWhole[P ~string | ~[]byte](s *Store, prefix P) bool {
	return s.policy.BootKey != "" && prefix[prefixLen-1] == dirEntity &&
		s.hasHostType && prefix[1] == s.hostType[0] && prefix[2] == s.hostType[1]
}

// aliveFolded answers Alive for a store whose policy names a boot key, for a prefix
// that foldsWhole. Whether an entity is quarantined is decided by folding everything the scope sees of it,
// whatever the instant is, so the whole prefix is read: every record the token
// sees, and the retention baseline, which stands for the records before the
// horizon. Every record the baseline stands for has a Seq at most the retention's,
// and a read below that is refused, so the token sees all of them.
//
// The boots are folded first, as the fold of the whole history folds them: a
// tracker is seeded with the baseline's boot state, the boot fold at the horizon,
// and takes the sightings the baseline carries together with those of the
// records, in order (see foldBoots). A collision quarantines the entity. Otherwise
// the answer is the existence folded from the records and the baseline's entries,
// which hold every reference alive at the horizon; an entry has no payload, so
// that fold tracks no boots.
func (r *reader) aliveFolded(prefix []byte, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	lo, hi := prefixBounds(prefix)
	r.it.SetBounds(lo, hi)
	var as, records []lifecycle.Assertion
	var base *bootState
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
			a := assertionOf(dirEntity, ref, ns, v)
			as = append(as, a)
			records = append(records, a)
		case kindBaseline:
			st, err := decodeStamp(r.it.Value())
			if err != nil {
				return false, err
			}
			if st.kind != kindBaseline {
				return false, fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
			}
			r.baselineEntries += int64(len(st.entries))
			for _, en := range st.entries {
				as = append(as, assertionOf(dirEntity, en.ref, en.eventNs, en.value))
			}
			base = st.boots
		}
	}
	if err := r.it.Error(); err != nil {
		return false, err
	}
	sightings, err := lifecycle.Sightings(records, r.s.policy)
	if err != nil {
		return false, fmt.Errorf("pebblestore: Alive: folding %s: %w", fp, err)
	}
	tr, _, err := foldBoots(r.s.policy, base, sightings, nil)
	if err != nil {
		return false, fmt.Errorf("pebblestore: Alive: folding the boots of %s: %w", fp, err)
	}
	if cc := tr.Collision(); cc != nil {
		return false, &store.QuarantineError{Entity: fp, Layer: sc.Layer, Collision: cc}
	}
	existence := r.s.policy
	existence.BootKey = ""
	tl, err := lifecycle.Fold(as, existence)
	if err != nil {
		return false, fmt.Errorf("pebblestore: Alive: folding %s: %w", fp, err)
	}
	return tl.AliveAt(t), nil
}

// foldBoots seeds a tracker with base, the boot state of a baseline (nil for none,
// or for a baseline of format 1), and has it take, in order, the sightings base
// carries together with sightings, which are sorted and none of which is before
// base's horizon. With until set, only the sightings before it are taken, and the
// others are returned, in order: what a baseline at until carries. A collision is
// kept in the tracker; any other error is returned.
func foldBoots(p lifecycle.Policy, base *bootState, sightings []lifecycle.Sighting, until *time.Time) (_ *lifecycle.BootTracker, carried []lifecycle.Sighting, err error) {
	tr := lifecycle.NewBootTracker(p)
	all := sightings
	if base != nil {
		if err := tr.Seed(base.boots, base.collision); err != nil {
			return nil, nil, fmt.Errorf("the baseline's boot state: %w: %w", err, pebblekv.ErrValue)
		}
		all = mergeSightings(base.carried, sightings)
	}
	for i, sg := range all {
		if until != nil && !sg.At.Before(*until) {
			return tr, all[i:], nil
		}
		if err := tr.Observe(sg.ID, sg.At); err != nil && !errors.Is(err, lifecycle.ErrCloneCollision) {
			return nil, nil, err
		}
	}
	return tr, nil, nil
}

// mergeSightings merges two sorted lists of sightings into one, sorted and
// without repeats.
func mergeSightings(a, b []lifecycle.Sighting) []lifecycle.Sighting {
	out := make([]lifecycle.Sighting, 0, len(a)+len(b))
	out = append(append(out, a...), b...)
	slices.SortFunc(out, lifecycle.CompareSightings)
	return slices.CompactFunc(out, func(x, y lifecycle.Sighting) bool { return lifecycle.CompareSightings(x, y) == 0 })
}
