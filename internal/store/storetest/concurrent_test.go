package storetest_test

import (
	"errors"
	"testing"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func TestCheckConcurrentReads(t *testing.T) {
	t.Parallel()

	if err := storetest.CheckConcurrentReads(newSlow(t, nil)); err != nil {
		t.Fatalf("a slow but consistent store was reported torn: %v", err)
	}
	if err := storetest.CheckConcurrentReads(newSlow(t, func(s *slow) { s.backward = true })); err == nil {
		t.Error("a store whose LastSeq goes backward was not noticed")
	}
	for name, mod := range map[string]func(*slow){
		"answers half a read from before a batch and half from after it": func(s *slow) { s.tear = true },
		"answers a batched read from a snapshot per entry":               func(s *slow) { s.tearBatchEnd = true },
		"loses what is before the horizon while it retains":              func(s *slow) { s.retainMode = retainEmpty },
		"publishes the horizon after discarding":                         func(s *slow) { s.retainMode = retainDiscardFirst },
		"raises LastSeq before the batch is visible":                     func(s *slow) { s.ahead = true },
		"puts the answers of a large batched read in the wrong place":    func(s *slow) { s.misindex = true },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Whether a tear shows depends on how the goroutines happen to
			// interleave, which a slow or busy machine changes. The check is built so
			// that it shows almost every time; a few attempts make a miss on a
			// loaded runner vanishingly unlikely without hiding a check that cannot
			// see the tear at all, which would miss on every attempt.
			var err error
			for range 5 {
				if err = storetest.CheckConcurrentReads(newSlow(t, mod)); errors.Is(err, storetest.ErrTorn) {
					return
				}
			}
			t.Errorf("a store that %s: err = %v, want ErrTorn", name, err)
		})
	}

	// A store that refuses reads while it retains, though they are at or after the
	// horizon, is not torn: the error names the refusal.
	t.Run("refuses a read at or after the announced horizon while it retains", func(t *testing.T) {
		t.Parallel()
		var err error
		for range 5 {
			err = storetest.CheckConcurrentReads(newSlow(t, func(s *slow) { s.retainMode = retainRefuses }))
			if errors.Is(err, store.ErrBeforeHorizon) {
				if errors.Is(err, storetest.ErrTorn) {
					t.Errorf("a refusal was reported as torn: %v", err)
				}
				return
			}
		}
		t.Errorf("err = %v, want an error wrapping ErrBeforeHorizon", err)
	})
}

// TestConcurrentReadsAgreeWithAStoreThatReallyDiscardsHistory is the honest
// control for the check: a store that compacts on every retention, and that is
// safe for concurrent use because every operation holds a lock, is not reported,
// and its retentions did take history away.
func TestConcurrentReadsAgreeWithAStoreThatReallyDiscardsHistory(t *testing.T) {
	t.Parallel()
	for i := range 2 {
		b := compacting(zeroPolicy)
		if err := storetest.CheckConcurrentReads(b); err != nil {
			t.Fatalf("run %d: an honest store that compacts on every retention was reported: %v", i, err)
		}
		if b.dropped == 0 || len(b.written) == 0 {
			t.Fatalf("run %d: the store dropped %d records and kept %d", i, b.dropped, len(b.written))
		}
	}
}
