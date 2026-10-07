package store_test

import (
	"cmp"
	"errors"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// kinds are the entity types the generated fingerprints draw from, with the
// attribute key each one's identity is made of. Their type names differ, so
// the type part of the order is exercised.
var kinds = []struct {
	typ catalog.EntityType
	key catalog.AttributeKey
}{
	{catalog.Host, catalog.HostID},
	{catalog.K8sNode, catalog.K8sNodeUID},
	{catalog.K8sPod, catalog.K8sPodUID},
}

// genFingerprint draws a fingerprint from a small pool of identities, so that
// equal fingerprints turn up often.
func genFingerprint() *rapid.Generator[identity.Fingerprint] {
	resolver := identity.NewResolver(catalog.Default())
	return rapid.Custom(func(t *rapid.T) identity.Fingerprint {
		k := rapid.SampledFrom(kinds).Draw(t, "kind")
		uid := rapid.StringMatching(`[a-c]{1,2}`).Draw(t, "uid")
		id, err := resolver.Resolve(k.typ, []identity.Attr{{Key: k.key, Value: uid}})
		if err != nil {
			t.Fatalf("resolving %s %q: %v", k.typ, uid, err)
		}
		return id.Fingerprint()
	})
}

var relations = []catalog.RelationType{catalog.RunsOn, catalog.PartOf, catalog.ScheduledOn}

func genNeighbor() *rapid.Generator[store.Neighbor] {
	return rapid.Custom(func(t *rapid.T) store.Neighbor {
		return store.Neighbor{
			Peer:     genFingerprint().Draw(t, "peer"),
			Relation: rapid.SampledFrom(relations).Draw(t, "relation"),
		}
	})
}

func genRecord() *rapid.Generator[store.Record] {
	return rapid.Custom(func(t *rapid.T) store.Record {
		return store.Record{
			EventTime: time.Unix(rapid.Int64Range(0, 4).Draw(t, "seconds"), 0).UTC(),
			Seq:       rapid.Uint64Range(1, 6).Draw(t, "seq"),
		}
	})
}

func sign(n int) int { return cmp.Compare(n, 0) }

func TestCompareFingerprintsIsAStrictTotalOrder(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		a, b, c := genFingerprint().Draw(t, "a"), genFingerprint().Draw(t, "b"), genFingerprint().Draw(t, "c")

		fwd, rev := store.CompareFingerprints(a, b), store.CompareFingerprints(b, a)
		if sign(fwd) != -sign(rev) {
			t.Fatalf("not antisymmetric: compare(%s, %s) = %d but compare(%s, %s) = %d", a, b, fwd, b, a, rev)
		}
		if (fwd == 0) != (a == b) {
			t.Fatalf("compare(%s, %s) = %d, but the fingerprints equal is %v", a, b, fwd, a == b)
		}
		if store.CompareFingerprints(a, a) != 0 {
			t.Fatalf("%s does not compare equal to itself", a)
		}
		bc, ac := store.CompareFingerprints(b, c), store.CompareFingerprints(a, c)
		if fwd <= 0 && bc <= 0 && ac > 0 {
			t.Fatalf("not transitive: %s <= %s <= %s but %s > %s", a, b, c, a, c)
		}
	})
}

func TestCompareFingerprintsOrdersByTypeBeforeHash(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		a, b := genFingerprint().Draw(t, "a"), genFingerprint().Draw(t, "b")
		want := cmp.Compare(a.Type(), b.Type())
		if want == 0 {
			t.Skip("same type: the hash decides")
		}
		if got := store.CompareFingerprints(a, b); sign(got) != want {
			t.Fatalf("compare(%s, %s) = %d, want the sign of the type order, %d", a, b, got, want)
		}
	})

	// And within one type, by the hash bytes.
	x, y := fp(t, catalog.K8sPod, catalog.K8sPodUID, "x"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "y")
	hx, hy := x.Hash(), y.Hash()
	if got, want := store.CompareFingerprints(x, y), slices.Compare(hx[:], hy[:]); sign(got) != want {
		t.Errorf("same-type compare = %d, want the sign of the hash order, %d", got, want)
	}
}

func TestSortNeighborsSortsAndKeepsEveryNeighbor(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		in := rapid.SliceOf(genNeighbor()).Draw(t, "neighbors")
		got := slices.Clone(in)
		store.SortNeighbors(got)

		inOrder := func(a, b store.Neighbor) int {
			if c := store.CompareFingerprints(a.Peer, b.Peer); c != 0 {
				return c
			}
			return cmp.Compare(a.Relation, b.Relation)
		}
		if !slices.IsSortedFunc(got, inOrder) {
			t.Fatalf("not sorted by peer then relation: %v", got)
		}
		counts := map[store.Neighbor]int{}
		for _, n := range in {
			counts[n]++
		}
		for _, n := range got {
			counts[n]--
		}
		for n, c := range counts {
			if c != 0 {
				t.Fatalf("not a permutation of the input: %v is off by %d (input %v, got %v)", n, c, in, got)
			}
		}
	})
}

func TestSortRecordsSortsAndKeepsEveryRecord(t *testing.T) {
	t.Parallel()

	type key struct {
		ns  int64
		seq uint64
	}
	rapid.Check(t, func(t *rapid.T) {
		in := rapid.SliceOf(genRecord()).Draw(t, "records")
		got := slices.Clone(in)
		store.SortRecords(got)

		if !slices.IsSortedFunc(got, func(a, b store.Record) int {
			if c := cmp.Compare(a.EventTime.UnixNano(), b.EventTime.UnixNano()); c != 0 {
				return c
			}
			return cmp.Compare(a.Seq, b.Seq)
		}) {
			t.Fatalf("not sorted by event time then seq: %v", got)
		}
		counts := map[key]int{}
		for _, r := range in {
			counts[key{r.EventTime.UnixNano(), r.Seq}]++
		}
		for _, r := range got {
			counts[key{r.EventTime.UnixNano(), r.Seq}]--
		}
		for k, c := range counts {
			if c != 0 {
				t.Fatalf("not a permutation of the input: %v is off by %d", k, c)
			}
		}
	})
}

func TestNeighborsEachIsParallelToItsInput(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		fps := rapid.SliceOfN(genFingerprint(), 0, 12).Draw(t, "fps")
		var asked []identity.Fingerprint
		got, err := store.NeighborsEach(fps, func(f identity.Fingerprint) ([]store.Neighbor, error) {
			asked = append(asked, f)
			return []store.Neighbor{{Peer: f, Relation: catalog.PartOf}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(fps) {
			t.Fatalf("got %d answers for %d fingerprints", len(got), len(fps))
		}
		for i, f := range fps {
			if len(got[i]) != 1 || got[i][0].Peer != f {
				t.Fatalf("answer %d is %v, want the answer for %s", i, got[i], f)
			}
		}
		if !slices.Equal(asked, fps) {
			t.Fatalf("read was asked %v, want each fingerprint once, in order: %v", asked, fps)
		}
	})
}

func TestNeighborsEachReturnsTheFirstError(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		fps := rapid.SliceOfN(genFingerprint(), 1, 12).Draw(t, "fps")
		failAt := rapid.IntRange(0, len(fps)-1).Draw(t, "failAt")
		first, later := errors.New("first"), errors.New("later")
		calls := 0
		got, err := store.NeighborsEach(fps, func(identity.Fingerprint) ([]store.Neighbor, error) {
			calls++
			switch {
			case calls-1 == failAt:
				return nil, first
			case calls-1 > failAt:
				return nil, later
			}
			return nil, nil
		})
		if !errors.Is(err, first) {
			t.Fatalf("err = %v, want the first read error", err)
		}
		if got != nil {
			t.Fatalf("result = %v, want nil with an error", got)
		}
		if calls != failAt+1 {
			t.Fatalf("read was called %d times, want it to stop after the failure at call %d", calls, failAt+1)
		}
	})
}

func TestNeighborsEachAsksForRepeatsAndForNothing(t *testing.T) {
	t.Parallel()

	a, b := fp(t, catalog.K8sPod, catalog.K8sPodUID, "a"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "b")
	var asked int
	got, err := store.NeighborsEach([]identity.Fingerprint{b, a, b}, func(f identity.Fingerprint) ([]store.Neighbor, error) {
		asked++
		return []store.Neighbor{{Peer: f, Relation: catalog.PartOf}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0][0].Peer != b || got[1][0].Peer != a || got[2][0].Peer != b || asked != 3 {
		t.Errorf("answers %v after %d reads, want one per fingerprint including the repeat", got, asked)
	}

	if empty, err := store.NeighborsEach(nil, nil); err != nil || len(empty) != 0 {
		t.Errorf("an empty input answered %v, %v", empty, err)
	}
}
