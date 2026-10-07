package store_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// stubStore is a type check, not an implementation: it exists so the compiler
// proves the method set of store.Store is what the tests below assume.
type stubStore struct{}

var _ store.Store = (*stubStore)(nil)

func (*stubStore) Write(context.Context, []store.Record) error { return store.ErrClosed }
func (*stubStore) LastSeq() uint64                             { return 0 }
func (*stubStore) Horizon() store.Horizon                      { return store.Horizon{} }
func (*stubStore) Neighbors(context.Context, identity.Fingerprint, store.Direction, time.Time, store.Scope) ([]store.Neighbor, error) {
	return nil, store.ErrClosed
}

func (*stubStore) NeighborsBatch(context.Context, []identity.Fingerprint, store.Direction, time.Time, store.Scope) ([][]store.Neighbor, error) {
	return nil, store.ErrClosed
}

func (*stubStore) Alive(context.Context, identity.Fingerprint, time.Time, store.Scope) (bool, error) {
	return false, store.ErrClosed
}

func (*stubStore) Window(context.Context, identity.Fingerprint, store.Direction, time.Time, time.Time, store.Scope) ([]store.Record, error) {
	return nil, store.ErrClosed
}

func (*stubStore) EntityWindow(context.Context, identity.Fingerprint, time.Time, time.Time, store.Scope) ([]store.Record, error) {
	return nil, store.ErrClosed
}
func (*stubStore) Retain(context.Context, time.Time) error { return store.ErrClosed }
func (*stubStore) Close() error                            { return nil }

func TestStubStoreIsAStore(t *testing.T) {
	t.Parallel()

	var s store.Store = &stubStore{}
	if err := s.Write(context.Background(), nil); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Write on the stub: err = %v, want ErrClosed", err)
	}
	if !s.Horizon().IsZero() || s.LastSeq() != 0 {
		t.Errorf("the stub reports horizon %+v and token %d, want the zero values", s.Horizon(), s.LastSeq())
	}
}

func TestScopeValidate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		layer   catalog.Layer
		invalid bool
	}{
		"L0":       {layer: catalog.L0},
		"L1":       {layer: catalog.L1},
		"L2":       {layer: catalog.L2},
		"L3":       {layer: catalog.L3},
		"zero":     {layer: 0, invalid: true},
		"past L3":  {layer: catalog.L3 + 1, invalid: true},
		"far past": {layer: 200, invalid: true},
	}
	for name, tc := range tests {
		err := store.Scope{Layer: tc.layer, AsOf: store.Latest}.Validate()
		switch {
		case tc.invalid && !errors.Is(err, store.ErrInvalid):
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		case !tc.invalid && err != nil:
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
}

func TestCurrentReadsEverythingInTheLayer(t *testing.T) {
	t.Parallel()

	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		cur := store.Current(l)
		if cur.Layer != l || cur.AsOf != store.Latest {
			t.Errorf("Current(%s) = %+v, want that layer at Latest", l, cur)
		}
		if err := cur.Validate(); err != nil {
			t.Errorf("Current(%s) was rejected: %v", l, err)
		}
	}
	if store.Latest != math.MaxUint64 {
		t.Errorf("Latest = %d, want the largest uint64", store.Latest)
	}
}

func TestDirectionString(t *testing.T) {
	t.Parallel()

	tests := map[store.Direction]string{
		store.Forward:      "forward",
		store.Reverse:      "reverse",
		store.Direction(0): "Direction(0)",
		store.Direction(7): "Direction(7)",
	}
	for d, want := range tests {
		if got := d.String(); got != want {
			t.Errorf("Direction(%d).String() = %q, want %q", uint8(d), got, want)
		}
	}
}

func TestHorizonIsZero(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		h    store.Horizon
		want bool
	}{
		"zero value":   {h: store.Horizon{}, want: true},
		"time only":    {h: store.Horizon{Time: time.Unix(1, 0)}, want: false},
		"seq only":     {h: store.Horizon{Seq: 1}, want: false},
		"time and seq": {h: store.Horizon{Time: time.Unix(1, 0), Seq: 1}, want: false},
	}
	for name, tc := range tests {
		if got := tc.h.IsZero(); got != tc.want {
			t.Errorf("%s: IsZero() = %v, want %v", name, got, tc.want)
		}
	}
}
