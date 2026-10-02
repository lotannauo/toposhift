package identity_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestRegistryDetectsCollisions(t *testing.T) {
	t.Parallel()

	// A hash that maps everything to one value forces the collision that
	// 128 bits make astronomically unlikely, so the check can be tested.
	constant := identity.WithHash(func([]byte) [identity.FingerprintBytes]byte {
		return [identity.FingerprintBytes]byte{1}
	})
	r := identity.NewResolver(catalog.Default(), constant)
	a, err1 := r.Resolve(catalog.Host, attrs(catalog.HostID, "a"))
	b, err2 := r.Resolve(catalog.Host, attrs(catalog.HostID, "b"))
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("the forced hash did not collide")
	}

	reg := identity.NewRegistry()
	if err := reg.Register(a); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.Register(a); err != nil {
		t.Errorf("registering the same identity again: %v", err)
	}
	err := reg.Register(b)
	if !errors.Is(err, identity.ErrCollision) {
		t.Fatalf("a colliding identity: err = %v, want ErrCollision", err)
	}
	if reg.Len() != 1 {
		t.Errorf("Len() = %d after a collision, want 1: a collision must not change the registry", reg.Len())
	}
	if err := reg.Register(a); err != nil {
		t.Errorf("the original identity stopped registering after a collision: %v", err)
	}
}

func TestRegistryAcceptsDistinctIdentities(t *testing.T) {
	t.Parallel()

	r := newResolver()
	reg := identity.NewRegistry()
	for i := range 100 {
		id, err := r.Resolve(catalog.Host, attrs(catalog.HostID, fmt.Sprintf("h%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(id); err != nil {
			t.Fatalf("host %d: %v", i, err)
		}
	}
	if reg.Len() != 100 {
		t.Errorf("Len() = %d, want 100", reg.Len())
	}
}

func TestRegistryRejectsZeroIdentity(t *testing.T) {
	t.Parallel()

	if err := identity.NewRegistry().Register(identity.Identity{}); !errors.Is(err, identity.ErrNonCanonical) {
		t.Errorf("err = %v, want ErrNonCanonical", err)
	}
}

// TestRegistryConcurrentUse is for the race detector: many goroutines
// registering overlapping identities must neither race nor report a
// collision.
func TestRegistryConcurrentUse(t *testing.T) {
	t.Parallel()

	r := newResolver()
	reg := identity.NewRegistry()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				id, err := r.Resolve(catalog.Host, attrs(catalog.HostID, fmt.Sprintf("h%d", (i+g)%50)))
				if err != nil {
					t.Error(err)
					return
				}
				if err := reg.Register(id); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if reg.Len() != 50 {
		t.Errorf("Len() = %d, want 50", reg.Len())
	}
}
