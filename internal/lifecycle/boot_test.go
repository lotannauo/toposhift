package lifecycle_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func boot(id string) identity.Attr { return identity.Attr{Key: lifecycle.BootID, Value: id} }

var hostPolicy = lifecycle.Policy{BootKey: lifecycle.BootID}

func wantBoots(t *testing.T, tl lifecycle.Timeline, want ...lifecycle.Boot) {
	t.Helper()
	if got := tl.Boots(); !sameBoots(got, want) {
		t.Errorf("boots = %+v\nwant %+v", got, want)
	}
}

func span(id string, first, last time.Duration) lifecycle.Boot {
	return lifecycle.Boot{ID: id, FirstSeen: at(first), LastSeen: at(last)}
}

func TestBoots(t *testing.T) {
	t.Parallel()

	t.Run("a reboot is a new boot of the same subject", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, hostPolicy,
			obs("node", 1, 0, 5*minute, boot("A")),
			obs("node", 2, 3*minute, 5*minute, boot("A")),
			obs("node", 3, 8*minute, 5*minute, boot("B")), // a quick reboot: the deadline is met exactly, so no gap
		)
		wantBoots(t, tl, span("A", 0, 3*minute), span("B", 8*minute, 8*minute))
		wantExistence(t, tl, iv(0, 13*minute, lifecycle.EndLivenessExpiry))
	})

	t.Run("the same boot after a gap is one boot, not a reboot", func(t *testing.T) {
		t.Parallel()
		// A monitoring outage: the host was unobserved, not restarted.
		tl := fold(t, hostPolicy,
			obs("node", 1, 0, 5*minute, boot("A")),
			obs("node", 2, 20*minute, 5*minute, boot("A")),
		)
		wantBoots(t, tl, span("A", 0, 20*minute))
		wantExistence(t, tl,
			iv(0, 5*minute, lifecycle.EndLivenessExpiry), iv(20*minute, 25*minute, lifecycle.EndLivenessExpiry))
	})

	t.Run("boots are listed in order of first appearance", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, hostPolicy,
			obs("node", 3, 20*minute, 0, boot("C")),
			obs("node", 1, 0, 0, boot("A")),
			obs("node", 2, 10*minute, 0, boot("B")),
		)
		wantBoots(t, tl, span("A", 0, 0), span("B", 10*minute, 10*minute), span("C", 20*minute, 20*minute))
	})

	t.Run("observations without a boot ID do not count", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, hostPolicy,
			obs("cloud", 1, 0, 5*minute, attr("host.name", "web-1")),
			obs("node", 2, 1*minute, 5*minute, boot("A")),
			del("cloud", 3, 2*minute),
		)
		wantBoots(t, tl, span("A", 1*minute, 1*minute))
	})

	t.Run("a policy without an boot key tracks none", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, lifecycle.Policy{}, obs("node", 1, 0, 0, boot("A")), obs("node", 2, 5*minute, 0, boot("B")))
		if got := tl.Boots(); got != nil {
			t.Errorf("boots = %+v, want none", got)
		}
	})

	t.Run("two producers reporting one boot are one boot", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, hostPolicy,
			obs("node", 1, 0, 0, boot("A")),
			obs("agent", 2, 2*minute, 0, boot("A")),
		)
		wantBoots(t, tl, span("A", 0, 2*minute))
	})
}

func collision(t *testing.T, pol lifecycle.Policy, as ...lifecycle.Assertion) *lifecycle.CloneCollisionError {
	t.Helper()
	tl, err := lifecycle.Fold(as, pol)
	if !errors.Is(err, lifecycle.ErrCloneCollision) {
		t.Fatalf("err = %v, want ErrCloneCollision", err)
	}
	var ce *lifecycle.CloneCollisionError
	if !errors.As(err, &ce) {
		t.Fatalf("error is not a *CloneCollisionError: %v", err)
	}
	if tl.AliveAt(at(0)) || tl.Existence() != nil || tl.Boots() != nil {
		t.Error("a collision returned a timeline: it must not be merged")
	}
	return ce
}

func TestCloneCollision(t *testing.T) {
	t.Parallel()

	t.Run("an older boot reappearing after a newer one", func(t *testing.T) {
		t.Parallel()
		ce := collision(t, hostPolicy,
			obs("node", 1, 0, 0, boot("A")),
			obs("node", 2, 10*minute, 0, boot("B")),
			obs("node", 3, 15*minute, 0, boot("A")),
		)
		if ce.StaleBoot != "A" || ce.NewerBoot != "B" ||
			!ce.ObservedAt.Equal(at(15*minute)) || !ce.NewerFirstSeen.Equal(at(10*minute)) {
			t.Errorf("collision detail = %+v", ce)
		}
	})

	t.Run("two clones interleaving their boots", func(t *testing.T) {
		t.Parallel()
		_ = collision(t, hostPolicy,
			obs("clone-1", 1, 0, 0, boot("A")),
			obs("clone-2", 2, 1*minute, 0, boot("B")),
			obs("clone-1", 3, 2*minute, 0, boot("A")),
			obs("clone-2", 4, 3*minute, 0, boot("B")),
		)
	})

	t.Run("it does not matter which producer saw which", func(t *testing.T) {
		t.Parallel()
		_ = collision(t, hostPolicy,
			obs("p1", 1, 0, 0, boot("A")),
			obs("p2", 2, 10*minute, 0, boot("B")),
			obs("p3", 3, 15*minute, 0, boot("A")),
		)
	})

	t.Run("the error says what happened", func(t *testing.T) {
		t.Parallel()
		_, err := lifecycle.Fold([]lifecycle.Assertion{
			obs("n", 1, 0, 0, boot("A")), obs("n", 2, minute, 0, boot("B")), obs("n", 3, 2*minute, 0, boot("A")),
		}, hostPolicy)
		for _, want := range []string{`"A"`, `"B"`, "clone collision"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("error does not mention %s: %v", want, err)
			}
		}
	})
}

// TestCloneCollisionSkewBoundary names the exact boundary: an older boot may
// be observed again up to and including Skew after the newer one appeared.
func TestCloneCollisionSkewBoundary(t *testing.T) {
	t.Parallel()

	pol := lifecycle.Policy{BootKey: lifecycle.BootID, Skew: 2 * time.Second}
	build := func(late time.Duration) []lifecycle.Assertion {
		return []lifecycle.Assertion{
			obs("p1", 1, 0, 0, boot("A")),
			obs("p2", 2, 10*time.Second, 0, boot("B")),
			obs("p1", 3, 10*time.Second+late, 0, boot("A")),
		}
	}

	if _, err := lifecycle.Fold(build(2*time.Second), pol); err != nil {
		t.Errorf("exactly Skew after the newer boot appeared: %v, want tolerated", err)
	}
	_ = collision(t, pol, build(2*time.Second+time.Nanosecond)...)

	// Strict by default: only the identical instant is tolerated.
	if _, err := lifecycle.Fold(build(0), hostPolicy); err != nil {
		t.Errorf("at the identical instant with no skew: %v, want tolerated", err)
	}
	_ = collision(t, hostPolicy, build(time.Nanosecond)...)

	// A tolerated return still counts toward the older boot's span.
	tl := fold(t, pol, build(time.Second)...)
	wantBoots(t, tl,
		lifecycle.Boot{ID: "A", FirstSeen: at(0), LastSeen: at(11 * time.Second)},
		lifecycle.Boot{ID: "B", FirstSeen: at(10 * time.Second), LastSeen: at(10 * time.Second)})
}

// TestCloneCollisionKnownLimits pins the two ways boot-ID interleaving is
// wrong. They are documented limits, not bugs to fix quietly: a change here
// should be a deliberate decision.
func TestCloneCollisionKnownLimits(t *testing.T) {
	t.Parallel()

	t.Run("false positive: a machine reverted to an older snapshot", func(t *testing.T) {
		t.Parallel()
		// One VM boots A, is snapshotted, reboots into B, then is reverted to
		// the snapshot: boot A reappears. Indistinguishable from a clone.
		_ = collision(t, hostPolicy,
			obs("node", 1, 0, 0, boot("A")),
			obs("node", 2, 10*minute, 0, boot("B")),
			obs("node", 3, 20*minute, 0, boot("A")),
		)
	})

	t.Run("false negative: clones restored from one snapshot share a boot ID", func(t *testing.T) {
		t.Parallel()
		// A restore is not a boot, so both clones report boot A, and both
		// carry the same machine ID. They are merged as one host.
		tl := fold(t, hostPolicy,
			obs("clone-1", 1, 0, 0, boot("A")),
			obs("clone-2", 2, 1*minute, 0, boot("A")),
			obs("clone-1", 3, 2*minute, 0, boot("A")),
		)
		wantBoots(t, tl, span("A", 0, 2*minute))
	})
}

func TestAnOverwrittenAssertionHasNoEffectOnBoots(t *testing.T) {
	t.Parallel()

	// The producer said boot A, then at the same instant, with a later Seq,
	// said boot B. The first statement is replaced, so A was never seen.
	tl := fold(t, hostPolicy,
		obs("node", 1, 0, 0, boot("A")),
		obs("node", 2, 0, 0, boot("B")),
	)
	wantBoots(t, tl, span("B", 0, 0))
}
