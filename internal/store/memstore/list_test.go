package memstore_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// listFP is a fingerprint of a type whose hash is first, first+1, and so on.
func listFP(t testing.TB, typ catalog.EntityType, first byte) identity.Fingerprint {
	t.Helper()
	var h [identity.FingerprintBytes]byte
	for i := range h {
		h[i] = first + byte(i)
	}
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// listEntityRecord is an observation of an entity's own existence, in its
// catalog layer.
func listEntityRecord(t testing.TB, fp identity.Fingerprint, seq uint64) store.Record {
	t.Helper()
	ent, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		t.Fatalf("type %s is not in the catalog", fp.Type())
	}
	return store.Record{
		Layer: ent.Layer(), Subject: store.EntitySubject(fp), Producer: "p",
		EventTime: at(0), Seq: seq, Kind: lifecycle.Observe,
	}
}

func list(t testing.TB, s interface {
	ListEntities(context.Context, catalog.EntityType, identity.Fingerprint, int) ([]identity.Fingerprint, error)
}, typ catalog.EntityType, after identity.Fingerprint, n int,
) []identity.Fingerprint {
	t.Helper()
	got, err := s.ListEntities(context.Background(), typ, after, n)
	if err != nil {
		t.Fatalf("ListEntities(%s, %v, %d): %v", typ, after, n, err)
	}
	return got
}

func TestListEntities(t *testing.T) {
	t.Parallel()
	s := open(t)

	if got := list(t, s, catalog.K8sPod, identity.Fingerprint{}, 10); len(got) != 0 {
		t.Fatalf("an empty store lists %v", got)
	}

	// Written out of id order, so that a listing in write order would fail.
	pods := []identity.Fingerprint{listFP(t, catalog.K8sPod, 0x50), listFP(t, catalog.K8sPod, 0x10), listFP(t, catalog.K8sPod, 0x90), listFP(t, catalog.K8sPod, 0x30)}
	nodes := []identity.Fingerprint{listFP(t, catalog.K8sNode, 0x70), listFP(t, catalog.K8sNode, 0x20)}
	container := listFP(t, catalog.Container, 0x40)
	host := listFP(t, catalog.Host, 0x11)
	endpoint := listFP(t, catalog.NetworkEndpoint, 0x12)
	edgeOnlyPod := listFP(t, catalog.K8sPod, 0x01)

	var recs []store.Record
	seq := uint64(0)
	add := func(fp identity.Fingerprint) {
		seq++
		recs = append(recs, listEntityRecord(t, fp, seq))
	}
	for _, fp := range pods {
		add(fp)
	}
	for _, fp := range nodes {
		add(fp)
	}
	add(container)
	add(host)
	add(endpoint)
	// A pod that is only the target of an edge has no record of its own.
	seq++
	recs = append(recs, store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(container, edgeOnlyPod, catalog.PartOf), Producer: "p",
		EventTime: at(0), Seq: seq, Kind: lifecycle.Observe,
	})
	write(t, s, recs...)

	sortedPods := slices.Clone(pods)
	slices.SortFunc(sortedPods, store.CompareFingerprints)
	sortedNodes := slices.Clone(nodes)
	slices.SortFunc(sortedNodes, store.CompareFingerprints)

	for _, tc := range []struct {
		typ  catalog.EntityType
		want []identity.Fingerprint
	}{
		{catalog.K8sPod, sortedPods},
		{catalog.K8sNode, sortedNodes},
		{catalog.Container, []identity.Fingerprint{container}},
		{catalog.Host, []identity.Fingerprint{host}},
		{catalog.NetworkEndpoint, []identity.Fingerprint{endpoint}},
		{catalog.Service, nil},
	} {
		got := list(t, s, tc.typ, identity.Fingerprint{}, 100)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: listed %v, want %v", tc.typ, got, tc.want)
		}
	}

	t.Run("after is exclusive and resumes", func(t *testing.T) {
		for i, fp := range sortedPods {
			got := list(t, s, catalog.K8sPod, fp, 100)
			if want := sortedPods[i+1:]; !slices.Equal(got, want) {
				t.Errorf("after %v: listed %v, want %v", fp, got, want)
			}
		}
		// An after that is no entity of the store still bounds the list: the id that
		// is the edge's target only sorts first.
		if got := list(t, s, catalog.K8sPod, edgeOnlyPod, 100); !slices.Equal(got, sortedPods) {
			t.Errorf("after the edge-only pod: listed %v, want all %v", got, sortedPods)
		}
	})

	t.Run("n cuts, and pages join", func(t *testing.T) {
		var all []identity.Fingerprint
		var after identity.Fingerprint
		for i := 0; ; i++ {
			if i > 20 {
				t.Fatalf("the pages do not end: %v", all)
			}
			page := list(t, s, catalog.K8sPod, after, 3)
			if len(page) == 0 {
				break
			}
			if len(page) > 3 {
				t.Fatalf("a page of %d, n was 3", len(page))
			}
			all = append(all, page...)
			after = page[len(page)-1]
		}
		if !slices.Equal(all, sortedPods) {
			t.Errorf("pages of 3 joined to %v, want %v", all, sortedPods)
		}
		if got := list(t, s, catalog.K8sPod, identity.Fingerprint{}, 1); !slices.Equal(got, sortedPods[:1]) {
			t.Errorf("n = 1 listed %v", got)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		for name, call := range map[string]func() error{
			"n is 0": func() error {
				_, err := s.ListEntities(context.Background(), catalog.K8sPod, identity.Fingerprint{}, 0)
				return err
			},
			"n is negative": func() error {
				_, err := s.ListEntities(context.Background(), catalog.K8sPod, identity.Fingerprint{}, -1)
				return err
			},
			"unknown type": func() error {
				_, err := s.ListEntities(context.Background(), "no.such.type", identity.Fingerprint{}, 1)
				return err
			},
			"after of another type": func() error {
				_, err := s.ListEntities(context.Background(), catalog.K8sPod, nodes[0], 1)
				return err
			},
		} {
			if err := call(); !errors.Is(err, store.ErrInvalid) {
				t.Errorf("%s: %v, want an error wrapping ErrInvalid", name, err)
			}
		}
	})

	t.Run("a cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.ListEntities(ctx, catalog.K8sPod, identity.Fingerprint{}, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want context.Canceled", err)
		}
	})

	t.Run("a closed store", func(t *testing.T) {
		c := open(t)
		write(t, c, listEntityRecord(t, pods[0], 1))
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ListEntities(context.Background(), catalog.K8sPod, identity.Fingerprint{}, 1); !errors.Is(err, store.ErrClosed) {
			t.Errorf("got %v, want an error wrapping ErrClosed", err)
		}
	})
}
