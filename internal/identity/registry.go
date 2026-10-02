package identity

import (
	"fmt"
	"sync"
)

// Registry remembers the canonical identity behind every fingerprint it has
// seen, and fails loudly when one fingerprint shows up with two different
// identities. It is the in-memory form of the check the store makes on every
// fingerprint hit, and is safe for concurrent use.
//
// It holds one string per distinct identity and never evicts, so it suits
// tests, tools and bounded workloads; the store owns the persistent check.
type Registry struct {
	mu   sync.Mutex
	seen map[Fingerprint]string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{seen: make(map[Fingerprint]string)}
}

// Register records id. It returns nil if the fingerprint is new or was seen
// before with the same canonical identity, and an error wrapping
// [ErrCollision] if it was seen with a different one. A collision never merges
// the two entities and leaves the registry unchanged.
func (r *Registry) Register(id Identity) error {
	if id.IsZero() {
		return nonCanonical("zero identity")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, ok := r.seen[id.fp]
	switch {
	case !ok:
		r.seen[id.fp] = id.canon
	case prev != id.canon:
		return fmt.Errorf("%w: %s is claimed by two different identities", ErrCollision, id.fp)
	}
	return nil
}

// Len returns the number of distinct identities registered.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}
