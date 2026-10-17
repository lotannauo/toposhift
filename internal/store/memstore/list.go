package memstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

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
// This is the reference: it looks at every subject the store holds, so a call
// costs the store's size, not the page's.
func (s *Store) ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, n int) ([]identity.Fingerprint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("ListEntities")
	}
	if err := ctx.Err(); err != nil {
		return nil, contextError("ListEntities", err)
	}
	if _, ok := catalog.Default().Entity(typ); !ok {
		return nil, fmt.Errorf("memstore: ListEntities: entity type %q is not in the catalog: %w", typ, store.ErrInvalid)
	}
	if n < 1 {
		return nil, fmt.Errorf("memstore: ListEntities: n is %d, want at least 1: %w", n, store.ErrInvalid)
	}
	if !after.IsZero() && after.Type() != typ {
		return nil, fmt.Errorf("memstore: ListEntities: after is of type %q, want %q: %w", after.Type(), typ, store.ErrInvalid)
	}
	var out []identity.Fingerprint
	for sub := range s.bySubject {
		if sub.Kind != store.SubjectEntity || sub.A.Type() != typ {
			continue
		}
		if !after.IsZero() && store.CompareFingerprints(sub.A, after) <= 0 {
			continue
		}
		out = append(out, sub.A)
	}
	slices.SortFunc(out, store.CompareFingerprints)
	if len(out) > n {
		out = out[:n:n]
	}
	return out, nil
}
