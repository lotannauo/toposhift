package memstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func openClusterStore(t *testing.T) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// want checks what the store records.
func wantClusterName(t *testing.T, s *memstore.Store, name string, set, present bool) {
	t.Helper()
	gotName, gotSet, gotPresent, err := s.ClusterName()
	if err != nil || gotName != name || gotSet != set || gotPresent != present {
		t.Fatalf("ClusterName = %q, set %v, present %v, %v; want %q, set %v, present %v", gotName, gotSet, gotPresent, err, name, set, present)
	}
}

func TestMemstoreClusterNameRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openClusterStore(t)
	wantClusterName(t, s, "", false, false)

	if err := s.RecordClusterName(ctx, "prod", true); err != nil {
		t.Fatal(err)
	}
	wantClusterName(t, s, "prod", true, true)

	// None set is a recorded value, and the name given with it is not kept.
	if err := s.RecordClusterName(ctx, "ignored", false); err != nil {
		t.Fatal(err)
	}
	wantClusterName(t, s, "", false, true)

	// The longest name a store writes.
	longest := strings.Repeat("a", store.MaxClusterNameLen)
	if err := s.RecordClusterName(ctx, longest, true); err != nil {
		t.Fatal(err)
	}
	wantClusterName(t, s, longest, true, true)

	// Recording again replaces.
	if err := s.RecordClusterName(ctx, "staging", true); err != nil {
		t.Fatal(err)
	}
	wantClusterName(t, s, "staging", true, true)
}

func TestMemstoreClusterNameRefusals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, in := range map[string]string{
		"empty":                  "",
		"one over the limit":     strings.Repeat("a", store.MaxClusterNameLen+1),
		"the format's longest":   strings.Repeat("a", store.MaxClusterNameFormat),
		"over the format":        strings.Repeat("a", store.MaxClusterNameFormat+1),
		"a byte outside the set": "pr d",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := openClusterStore(t)
			if err := s.RecordClusterName(ctx, "prod", true); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordClusterName(ctx, in, true); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("RecordClusterName(%q) = %v, want ErrInvalid", in, err)
			}
			// Nothing was written: the earlier value stands.
			wantClusterName(t, s, "prod", true, true)

			fresh := openClusterStore(t)
			if err := fresh.RecordClusterName(ctx, in, true); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("RecordClusterName(%q) = %v, want ErrInvalid", in, err)
			}
			wantClusterName(t, fresh, "", false, false)
		})
	}
}

func TestMemstoreClusterNameAfterClose(t *testing.T) {
	t.Parallel()
	s := openClusterStore(t)
	if err := s.RecordClusterName(context.Background(), "prod", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordClusterName(context.Background(), "prod", true); !errors.Is(err, store.ErrClosed) {
		t.Errorf("RecordClusterName after Close = %v, want ErrClosed", err)
	}
	if _, _, _, err := s.ClusterName(); !errors.Is(err, store.ErrClosed) {
		t.Errorf("ClusterName after Close = %v, want ErrClosed", err)
	}
}

func TestMemstoreClusterNameHonoursTheContext(t *testing.T) {
	t.Parallel()
	s := openClusterStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.RecordClusterName(ctx, "prod", true); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordClusterName with a cancelled context = %v", err)
	}
	wantClusterName(t, s, "", false, false)
}
