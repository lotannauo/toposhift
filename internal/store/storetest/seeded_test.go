package storetest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// sinceMutants are stores whose WindowSince breaks one rule each. The double works
// the seeds out itself (see broken.sinceOf), so each damage is one changed line of
// the rule.
func sinceMutants() map[string]struct {
	damage sinceDamage
	// scriptedOnly says the generated workloads need not reach it: only the
	// scripted check is held to catching it.
	scriptedOnly bool
} {
	type m = struct {
		damage       sinceDamage
		scriptedOnly bool
	}
	return map[string]m{
		"decides per subject, not per reference":                     {damage: sinceDamage{perSubject: true}},
		"keeps a deciding record that does not hold":                 {damage: sinceDamage{keepDead: true}},
		"requires a seed to hold at from, not just before it":        {damage: sinceDamage{liveAtFrom: true}},
		"judges the seeds of a window from the horizon at from-1ns":  {damage: sinceDamage{noHorizonInstant: true}, scriptedOnly: true},
		"lets the records at from decide":                            {damage: sinceDamage{atFrom: true}},
		"decides the seeds from every record, whatever the token":    {damage: sinceDamage{ignoreToken: true}},
		"keeps the payload of a seed before the horizon":             {damage: sinceDamage{payloadBefore: true}},
		"gives seeds their payload when none was asked for":          {damage: sinceDamage{payloadAlways: true}},
		"returns the seeds in the order of their deciding records":   {damage: sinceDamage{unsorted: true}},
		"starts the window part one nanosecond late":                 {damage: sinceDamage{windowFrom: time.Nanosecond}},
		"swaps the answers of the first two fingerprints of a batch": {damage: sinceDamage{misindex: true}},
	}
}

// Each WindowSince mutant is caught for a wrong answer, by the scripted check and,
// unless nothing a workload does reaches it, by the generated workloads too.
func TestTheSinceChecksCatchBrokenStores(t *testing.T) {
	t.Parallel()
	isMismatch := func(err error) bool { return errors.Is(err, storetest.ErrMismatch) }
	for name, m := range sinceMutants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mk := func(p lifecycle.Policy) *broken {
				b := newBroken(p)
				b.sd = m.damage
				return b
			}
			if err := storetest.CheckSinceContract(mk(zeroPolicy)); !isMismatch(err) {
				t.Errorf("the window since check did not notice a store that %s: %v", name, err)
			}
			if m.scriptedOnly || storetest.RaceEnabled {
				return
			}
			var err error
			for _, w := range storetest.Workloads() {
				if err = storetest.CheckSince(compactingWith(mk(w.Policy)), w, storetest.Options{RetainAt: []float64{0.3, 0.6}, CheckEvery: 8}); isMismatch(err) {
					return
				}
			}
			t.Errorf("the generated workloads did not notice a store that %s: %v", name, err)
		})
	}
}

// compactingWith makes a mutant discard history at Retain, as the honest
// compacting store does, so a damage is judged on a store that has a baseline.
func compactingWith(b *broken) *broken {
	b.retain = compact
	return b
}

// tornSince answers WindowSince one fingerprint at a time, with a pause between, so
// a batch can land between two of them: an answer that is not one world.
type tornSince struct{ *slow }

func (s tornSince) WindowSince(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	out := make([]store.Since, 0, len(fps))
	for _, fp := range fps {
		one, err := s.slow.WindowSince(ctx, []identity.Fingerprint{fp}, dir, from, to, sc, o)
		if err != nil {
			return nil, err
		}
		out = append(out, one...)
		pause()
	}
	return out, nil
}

func TestTheConcurrencyCheckCatchesAWindowSinceThatIsNotOneSnapshot(t *testing.T) {
	t.Parallel()
	// As for the other tears: a few attempts make a miss on a loaded runner
	// vanishingly unlikely, and a check that cannot see the tear misses every time.
	var err error
	for range 5 {
		if err = storetest.CheckConcurrentReads(tornSince{newSlow(t, nil)}); errors.Is(err, storetest.ErrTorn) {
			return
		}
	}
	t.Errorf("a WindowSince answered from a snapshot per fingerprint: err = %v, want ErrTorn", err)
}

// The honest doubles agree with the reference and the contract on WindowSince,
// with no retention and with history really discarded.
func TestTheHonestDoublesPassTheSinceChecks(t *testing.T) {
	t.Parallel()
	for _, w := range storetest.Workloads() {
		for name, b := range map[string]*broken{"keeping": newBroken(w.Policy), "compacting": compacting(w.Policy)} {
			if err := storetest.CheckSince(b, w, storetest.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
				t.Errorf("workload %q, the %s double: %v", w.Name, name, err)
			}
		}
	}
	if err := storetest.CheckSinceContract(newBroken(zeroPolicy)); err != nil {
		t.Error(err)
	}
}
