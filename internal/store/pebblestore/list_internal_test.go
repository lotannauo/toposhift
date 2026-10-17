package pebblestore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// listRecord is a record of an entity's own existence, in its catalog layer.
func listRecord(fp identity.Fingerprint, seq uint64, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	ent, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		panic("type " + string(fp.Type()) + " is not in the catalog")
	}
	return store.Record{Layer: ent.Layer(), Subject: store.EntitySubject(fp), Producer: "p", EventTime: at, Seq: seq, Kind: kind, TTL: ttl}
}

// lister is what the two stores share.
type lister interface {
	ListEntities(context.Context, catalog.EntityType, identity.Fingerprint, int) ([]identity.Fingerprint, error)
}

func listOf(t *testing.T, s lister, typ catalog.EntityType, after identity.Fingerprint, n int) []identity.Fingerprint {
	t.Helper()
	got, err := s.ListEntities(bg, typ, after, n)
	if err != nil {
		t.Fatalf("ListEntities(%s, %v, %d): %v", typ, after, n, err)
	}
	return got
}

// bothList lists on both stores of the pair and fails if they differ.
func (p *pair) bothList(typ catalog.EntityType, after identity.Fingerprint, n int) []identity.Fingerprint {
	p.t.Helper()
	got := listOf(p.t, p.s, typ, after, n)
	want := listOf(p.t, p.ref, typ, after, n)
	if !slices.Equal(got, want) {
		p.t.Fatalf("ListEntities(%s, %v, %d): pebblestore %v, memstore %v", typ, after, n, got, want)
	}
	return got
}

func sortedFPs(fps ...identity.Fingerprint) []identity.Fingerprint {
	out := slices.Clone(fps)
	slices.SortFunc(out, store.CompareFingerprints)
	return out
}

func TestListEntities(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: off()})
	zero := identity.Fingerprint{}

	if got := p.bothList(catalog.K8sPod, zero, 10); len(got) != 0 {
		t.Fatalf("an empty store lists %v", got)
	}

	// Written out of id order, so that a listing in write order would fail. The
	// hashes are spread over the whole byte range, 0xFF included.
	pods := []identity.Fingerprint{
		fingerprintOf(catalog.K8sPod, 0x50), fingerprintOf(catalog.K8sPod, 0x10),
		fingerprintOf(catalog.K8sPod, 0xF0), fingerprintOf(catalog.K8sPod, 0x30),
	}
	nodes := []identity.Fingerprint{fingerprintOf(catalog.K8sNode, 0x70), fingerprintOf(catalog.K8sNode, 0x20)}
	container := fingerprintOf(catalog.Container, 0x40)
	host := fingerprintOf(catalog.Host, 0x11)
	endpoint := fingerprintOf(catalog.NetworkEndpoint, 0x12)
	service := fingerprintOf(catalog.Service, 0x13)
	edgeOnlyPod := fingerprintOf(catalog.K8sPod, 0x01)
	scheduledOnly := fingerprintOf(catalog.K8sPod, 0x02)

	var recs []store.Record
	seq := uint64(0)
	for _, fp := range slices.Concat(pods, nodes, []identity.Fingerprint{container, host, endpoint, service}) {
		seq++
		recs = append(recs, listRecord(fp, seq, sec(10), lifecycle.Observe, 0))
	}
	// A pod that is only the target of an edge has no record of its own, and a pod
	// that is only the source of one has nothing under its own existence either.
	seq++
	recs = append(recs, store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(container, edgeOnlyPod, catalog.PartOf), Producer: "p",
		EventTime: sec(10), Seq: seq, Kind: lifecycle.Observe,
	})
	seq++
	recs = append(recs, store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(scheduledOnly, nodes[0], catalog.ScheduledOn), Producer: "p",
		EventTime: sec(10), Seq: seq, Kind: lifecycle.Observe,
	})
	p.write(recs...)

	for _, tc := range []struct {
		typ  catalog.EntityType
		want []identity.Fingerprint
	}{
		{catalog.K8sPod, sortedFPs(pods...)},
		{catalog.K8sNode, sortedFPs(nodes...)},
		{catalog.Container, []identity.Fingerprint{container}},
		{catalog.Host, []identity.Fingerprint{host}},
		{catalog.NetworkEndpoint, []identity.Fingerprint{endpoint}},
		{catalog.Service, []identity.Fingerprint{service}},
		{catalog.Process, nil},
	} {
		if got := p.bothList(tc.typ, zero, 100); !slices.Equal(got, tc.want) {
			t.Errorf("%s: listed %v, want %v", tc.typ, got, tc.want)
		}
	}

	t.Run("after is exclusive and resumes", func(t *testing.T) {
		sorted := sortedFPs(pods...)
		for i, fp := range sorted {
			if got := p.bothList(catalog.K8sPod, fp, 100); !slices.Equal(got, sorted[i+1:]) {
				t.Errorf("after %v: listed %v, want %v", fp, got, sorted[i+1:])
			}
		}
		// An after that holds nothing still bounds the list.
		if got := p.bothList(catalog.K8sPod, edgeOnlyPod, 100); !slices.Equal(got, sorted) {
			t.Errorf("after the edge-only pod: listed %v, want %v", got, sorted)
		}
		past := fingerprintOf(catalog.K8sPod, 0xFF)
		if got := p.bothList(catalog.K8sPod, past, 100); len(got) != 0 {
			t.Errorf("after the largest id: listed %v", got)
		}
	})

	t.Run("n cuts, and pages join", func(t *testing.T) {
		var all []identity.Fingerprint
		after := zero
		for i := 0; ; i++ {
			if i > 20 {
				t.Fatalf("the pages do not end: %v", all)
			}
			page := p.bothList(catalog.K8sPod, after, 3)
			if len(page) == 0 {
				break
			}
			if len(page) > 3 {
				t.Fatalf("a page of %d, n was 3", len(page))
			}
			all = append(all, page...)
			after = page[len(page)-1]
		}
		if !slices.Equal(all, sortedFPs(pods...)) {
			t.Errorf("pages of 3 joined to %v, want %v", all, sortedFPs(pods...))
		}
		if got := p.bothList(catalog.K8sPod, zero, 1); len(got) != 1 {
			t.Errorf("n = 1 listed %v", got)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		for name, call := range map[string]func(lister) error{
			"n is 0": func(s lister) error { _, err := s.ListEntities(bg, catalog.K8sPod, zero, 0); return err },
			"n is negative": func(s lister) error {
				_, err := s.ListEntities(bg, catalog.K8sPod, zero, -1)
				return err
			},
			"unknown type": func(s lister) error { _, err := s.ListEntities(bg, "no.such.type", zero, 1); return err },
			"after of another type": func(s lister) error {
				_, err := s.ListEntities(bg, catalog.K8sPod, nodes[0], 1)
				return err
			},
		} {
			for sname, s := range map[string]lister{"pebblestore": p.s, "memstore": p.ref} {
				if err := call(s); !errors.Is(err, store.ErrInvalid) {
					t.Errorf("%s, %s: %v, want an error wrapping ErrInvalid", sname, name, err)
				}
			}
		}
	})

	t.Run("a cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bg)
		cancel()
		for sname, s := range map[string]lister{"pebblestore": p.s, "memstore": p.ref} {
			if _, err := s.ListEntities(ctx, catalog.K8sPod, zero, 1); !errors.Is(err, context.Canceled) {
				t.Errorf("%s: %v, want context.Canceled", sname, err)
			}
		}
	})

	t.Run("a closed store", func(t *testing.T) {
		s := openMem(t, Options{Checkpoints: off()})
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ListEntities(bg, catalog.K8sPod, zero, 1); !errors.Is(err, store.ErrClosed) {
			t.Errorf("got %v, want an error wrapping ErrClosed", err)
		}
	})
}

// The walk reads one key of each entity and seeks past the rest of its group, in
// one iterator: a pod with a thousand records costs a key, not a thousand.
func TestListEntitiesSkipsTheRestOfAGroup(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})

	heavy := fingerprintOf(catalog.K8sPod, 0x80)
	var recs []store.Record
	seq := uint64(0)
	for i := range 1000 {
		seq++
		recs = append(recs, listRecord(heavy, seq, sec(i), lifecycle.Observe, 0))
	}
	// The pod's edges are in the group too, after its own existence.
	seq++
	recs = append(recs, edgeRecord(seq, "p", sec(10), lifecycle.Observe, 0))
	var others []identity.Fingerprint
	for i := range 20 {
		fp := fingerprintOf(catalog.K8sPod, byte(0x90+i*3))
		others = append(others, fp)
		seq++
		recs = append(recs, listRecord(fp, seq, sec(10), lifecycle.Observe, 0))
	}
	if err := s.Write(bg, recs); err != nil {
		t.Fatal(err)
	}
	rec.counters, rec.samples = map[string]int64{}, map[string][]int64{}

	got := listOf(t, s, catalog.K8sPod, identity.Fingerprint{}, 100)
	if want := sortedFPs(append(others, heavy)...); !slices.Equal(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	if c := rec.Counter("list.iterators"); c != 1 {
		t.Errorf("list.iterators = %d, want 1", c)
	}
	seen := rec.samples["list.keys_seen"]
	if len(seen) != 1 {
		t.Fatalf("%d samples of list.keys_seen, want 1", len(seen))
	}
	// 22 groups: the heavy pod, the 20 others and podFP, whose group holds an edge
	// only and is looked at, not listed.
	if seen[0] > 22+1 {
		t.Errorf("list.keys_seen = %d, want at most 23: one key per entity group, not per record", seen[0])
	}
}

// A retention keeps a baseline of what is alive at the horizon and deletes the
// rest, so a prefix with nothing alive at it is not listed by the pebblestore, and
// is by the store that discards nothing. The existence read at a token decides, so
// both are right. A Background pass that has not reached a prefix leaves its
// records, and the prefix is listed.
func TestListEntitiesAcrossARetention(t *testing.T) {
	t.Parallel()
	zero := identity.Fingerprint{}
	lapsed := fingerprintOf(catalog.K8sPod, 0x10)  // alive 10s to 15s, gone at the horizon
	kept := fingerprintOf(catalog.K8sPod, 0x20)    // alive for good: a baseline
	deleted := fingerprintOf(catalog.K8sPod, 0x30) // deleted at 20s
	above := fingerprintOf(catalog.K8sPod, 0x40)   // first seen above the horizon
	records := []store.Record{
		listRecord(lapsed, 1, sec(10), lifecycle.Observe, 5*time.Second),
		listRecord(kept, 2, sec(10), lifecycle.Observe, 0),
		listRecord(deleted, 3, sec(10), lifecycle.Observe, 0),
		listRecord(deleted, 4, sec(20), lifecycle.Delete, 0),
		listRecord(above, 5, sec(70), lifecycle.Observe, 0),
	}
	everything := sortedFPs(lapsed, kept, deleted, above)
	retained := sortedFPs(kept, above)

	t.Run("synchronous", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Checkpoints: off()})
		p.write(records...)
		if got := p.bothList(catalog.K8sPod, zero, 100); !slices.Equal(got, everything) {
			t.Fatalf("before the retention: %v, want %v", got, everything)
		}
		p.retain(sec(50))
		if got := listOf(t, p.s, catalog.K8sPod, zero, 100); !slices.Equal(got, retained) {
			t.Errorf("pebblestore after the retention: %v, want the baseline and the newer: %v", got, retained)
		}
		if got := listOf(t, p.ref, catalog.K8sPod, zero, 100); !slices.Equal(got, everything) {
			t.Errorf("memstore after the retention: %v, want everything it holds: %v", got, everything)
		}
	})

	t.Run("background, before and after the pass reaches the prefixes", func(t *testing.T) {
		t.Parallel()
		pk := newParker(t, 1)
		o := bgOptions(vfs.NewMem(), off())
		o.beforeRetainChunk = pk.hook
		s := mustOpen(t, o)
		t.Cleanup(pk.release)
		if err := s.Write(bg, records); err != nil {
			t.Fatal(err)
		}
		if err := s.Retain(bg, sec(50)); err != nil {
			t.Fatal(err)
		}
		pk.wait(t)
		if got := listOf(t, s, catalog.K8sPod, zero, 100); !slices.Equal(got, everything) {
			t.Errorf("before the pass: %v, want every prefix, rewritten or not: %v", got, everything)
		}
		pk.release()
		waitRetained(t, s)
		if got := listOf(t, s, catalog.K8sPod, zero, 100); !slices.Equal(got, retained) {
			t.Errorf("after the pass: %v, want %v", got, retained)
		}
	})
}
