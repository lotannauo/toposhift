package memstore

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests in this file look inside the store, for what a caller cannot see:
// the size of the fold cache, and what Open keeps of its options.

func internalFingerprint(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func TestFoldCacheIsBounded(t *testing.T) {
	t.Parallel()
	s, err := Open(Options{})
	if err != nil {
		t.Fatal(err)
	}
	pod := internalFingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	node := internalFingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	sub := store.EdgeSubject(pod, node, catalog.ScheduledOn)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	const records = 4 * maxCachedFolds
	for i := range records {
		kind := lifecycle.Observe
		if i%2 == 1 {
			kind = lifecycle.Delete
		}
		r := store.Record{
			Layer: catalog.L2, Subject: sub, Producer: "k8s", EventTime: base.Add(time.Duration(i) * time.Second),
			Seq: uint64(i + 1), Kind: kind,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte{1}
		}
		if err := s.Write(context.Background(), []store.Record{r}); err != nil {
			t.Fatal(err)
		}
		// Read at every token so far: every one is a distinct prefix, so a cache
		// that kept them all would hold i+1 folds.
		for tok := 0; tok <= i+1; tok++ {
			if _, err := s.Neighbors(context.Background(), pod, store.Forward, base.Add(time.Hour), store.Scope{Layer: catalog.L2, AsOf: uint64(tok)}); err != nil {
				t.Fatal(err)
			}
			if n := len(s.folds[sub]); n > maxCachedFolds {
				t.Fatalf("after %d records and token %d, %d folds are cached, want at most %d", i+1, tok, n, maxCachedFolds)
			}
		}
	}
	if len(s.folds[sub]) == 0 {
		t.Error("nothing is cached")
	}
}

func TestOpenKeepsItsOwnCopyOfTheRank(t *testing.T) {
	t.Parallel()
	rank := map[lifecycle.Producer]int{"a": 1}
	s, err := Open(Options{Policy: lifecycle.Policy{Rank: rank}})
	if err != nil {
		t.Fatal(err)
	}
	rank["b"] = 2
	if want := (map[lifecycle.Producer]int{"a": 1}); !maps.Equal(s.policy.Rank, want) {
		t.Errorf("the store's rank = %v after the caller changed its map, want %v", s.policy.Rank, want)
	}
}

func TestCloseDropsWhatTheStoreHolds(t *testing.T) {
	t.Parallel()
	s, err := Open(Options{})
	if err != nil {
		t.Fatal(err)
	}
	pod := internalFingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	node := internalFingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	err = s.Write(context.Background(), []store.Record{{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 1, Kind: lifecycle.Observe, Payload: []byte{1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.bySubject != nil || s.layers != nil || s.folds != nil || s.incident[store.Forward] != nil || s.incident[store.Reverse] != nil {
		t.Error("a closed store still holds its maps")
	}
}
