package store_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestQuarantineError(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	collision := &lifecycle.CloneCollisionError{
		StaleBoot:      "boot-old",
		ObservedAt:     time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		NewerBoot:      "boot-new",
		NewerFirstSeen: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}

	t.Run("with a collision", func(t *testing.T) {
		t.Parallel()
		var err error = &store.QuarantineError{Entity: pod, Layer: catalog.L2, Collision: collision}
		if !errors.Is(err, store.ErrQuarantined) {
			t.Errorf("errors.Is(err, ErrQuarantined) = false for %v", err)
		}
		if !errors.Is(err, lifecycle.ErrCloneCollision) {
			t.Errorf("errors.Is(err, ErrCloneCollision) = false for %v", err)
		}
		var got *lifecycle.CloneCollisionError
		if !errors.As(err, &got) || got != collision {
			t.Errorf("errors.As found %v, want the collision %v", got, collision)
		}
		var q *store.QuarantineError
		if !errors.As(err, &q) || q.Entity != pod {
			t.Errorf("errors.As did not find the QuarantineError for %s", pod)
		}
		for _, want := range []string{"boot-old", "boot-new", pod.String(), catalog.L2.String()} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Error() = %q, want it to contain %q", err.Error(), want)
			}
		}
	})

	t.Run("without a collision", func(t *testing.T) {
		t.Parallel()
		var err error = &store.QuarantineError{Entity: pod, Layer: catalog.L2}
		if !errors.Is(err, store.ErrQuarantined) {
			t.Errorf("errors.Is(err, ErrQuarantined) = false for %v", err)
		}
		if errors.Is(err, lifecycle.ErrCloneCollision) {
			t.Errorf("errors.Is(err, ErrCloneCollision) = true for %v, want false: no collision is named", err)
		}
		var got *lifecycle.CloneCollisionError
		if errors.As(err, &got) {
			t.Errorf("errors.As found a collision %v in %v, want none", got, err)
		}
		for _, want := range []string{pod.String(), catalog.L2.String()} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Error() = %q, want it to contain %q", err.Error(), want)
			}
		}
	})
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err    error
		target error
		want   bool
	}{
		"before the horizon is invalid":         {store.ErrBeforeHorizon, store.ErrInvalid, true},
		"invalid is not before the horizon":     {store.ErrInvalid, store.ErrBeforeHorizon, false},
		"closed is not invalid":                 {store.ErrClosed, store.ErrInvalid, false},
		"quarantined is not invalid":            {store.ErrQuarantined, store.ErrInvalid, false},
		"closed is not quarantined":             {store.ErrClosed, store.ErrQuarantined, false},
		"a wrapped horizon error is still that": {errors.Join(errors.New("read"), store.ErrBeforeHorizon), store.ErrBeforeHorizon, true},
	}
	for name, tc := range tests {
		if got := errors.Is(tc.err, tc.target); got != tc.want {
			t.Errorf("%s: errors.Is(%v, %v) = %v, want %v", name, tc.err, tc.target, got, tc.want)
		}
	}
}
