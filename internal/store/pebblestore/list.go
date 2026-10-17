package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// listCheckEvery is how many entity groups a listing walks between looks at its
// context.
const listCheckEvery = 64

// ListEntities returns at most n ids of type typ, ascending by
// [store.CompareFingerprints], each greater than after (the zero Fingerprint: from
// the first), of the entities with anything stored under their own existence in
// the type's catalog layer. It takes no token and no horizon, so it may list an
// entity that does not exist at a given token, or no longer exists; a read at the
// token decides. It never lists an id with nothing under its own existence, such
// as the target of an edge only. It wraps [store.ErrInvalid] for a type the
// catalog does not know, n < 1, or an after of another type, and [store.ErrClosed]
// after Close. It is not part of [store.Store].
//
// The keys of one type in one layer are contiguous, and those of one entity are
// contiguous within them, the entity's own prefix first (see key.go), so the walk
// reads one key of each entity and seeks past the rest of its group: it costs the
// entities it passes, not their records. The iterator is a snapshot, so a write or
// a Background pass that runs during the call is seen whole or not at all. The call
// counts "list.iterators" and samples "list.keys_seen", the groups visited.
//
// The result is a superset of the entities that exist at any token: a prefix keeps
// a baseline after a retention, and a prefix whose every key a retention deleted is
// not listed.
func (s *Store) ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, n int) ([]identity.Fingerprint, error) {
	if s.closed.Load() {
		return nil, closedError("ListEntities")
	}
	if err := ctx.Err(); err != nil {
		return nil, contextError("ListEntities", err)
	}
	ent, known := catalog.Default().Entity(typ)
	if !known {
		return nil, fmt.Errorf("pebblestore: ListEntities: entity type %q is not in the catalog: %w", typ, store.ErrInvalid)
	}
	id, known := s.ids.EntityID(typ)
	if !known {
		return nil, fmt.Errorf("pebblestore: ListEntities: entity type %q has no id: %w", typ, store.ErrInvalid)
	}
	if n < 1 {
		return nil, fmt.Errorf("pebblestore: ListEntities: n is %d, want at least 1: %w", n, store.ErrInvalid)
	}
	if !after.IsZero() && after.Type() != typ {
		return nil, fmt.Errorf("pebblestore: ListEntities: after is of type %q, want %q: %w", after.Type(), typ, store.ErrInvalid)
	}

	// lo is the first key of the type in its layer: [layer][type id]. The group key
	// of an entity, [layer][type id][digest], is never all 0xFF, because its first
	// byte is a layer, 1 to 4, so prefixSucc, which carries, always has a successor
	// here though its comment speaks of data prefixes.
	lo := []byte{pebblekv.LayerByte(ent.Layer()), byte(id >> 8), byte(id)}
	hi := prefixSucc(lo)
	start := lo
	if !after.IsZero() {
		group, err := s.ids.AppendFingerprint([]byte{pebblekv.LayerByte(ent.Layer())}, after)
		if err != nil {
			return nil, fmt.Errorf("pebblestore: ListEntities: %w", err)
		}
		start = prefixSucc(group)
	}

	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: ListEntities: %w", err)
	}
	s.rec.Count("list.iterators", 1)

	var (
		out    []identity.Fingerprint
		groups int64
		walk   error
	)
	for valid := it.SeekGE(start); valid && len(out) < n; {
		key := it.Key()
		if _, _, _, _, err := parseKey(key); err != nil {
			walk = err
			break
		}
		groups++
		if groups%listCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				walk = contextError("ListEntities", err)
				break
			}
		}
		group := key[:1+pebblekv.FingerprintKeyLen]
		if key[prefixLen-1] == dirEntity {
			fp, err := s.ids.Fingerprint(group[1:])
			if err != nil {
				walk = fmt.Errorf("pebblestore: ListEntities: %w", err)
				break
			}
			out = append(out, fp)
		}
		// One seek past the rest of the group, whatever it holds.
		valid = it.SeekGE(prefixSucc(group))
	}
	s.rec.Sample("list.keys_seen", groups)
	iterErr := errors.Join(it.Error(), it.Close())
	if walk != nil {
		return nil, walk
	}
	if iterErr != nil {
		return nil, fmt.Errorf("pebblestore: ListEntities: %w", iterErr)
	}
	return slices.Clip(out), nil
}
