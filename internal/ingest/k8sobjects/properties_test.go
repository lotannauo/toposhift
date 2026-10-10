package k8sobjects_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var captures = []string{"run1.jsonl", "run2.jsonl"}

// Every record a translator returns passes the store's own validation, in both
// modes, for the fixtures and for the synthetic scenarios' shapes.
func TestEveryRecordValidates(t *testing.T) {
	for _, ttl := range ttls {
		for _, name := range captures {
			in := loadCapture(t, name)
			seq := uint64(0)
			for i, r := range translateAll(t, newTranslator(t, ttl), in) {
				for _, rec := range r.Records {
					seq++
					rec.Seq = seq
					if err := rec.Validate(); err != nil {
						t.Errorf("ttl %s, %s: %v", ttl, in[i].Name, err)
					}
				}
			}
			if seq == 0 {
				t.Fatalf("%s gave no records", name)
			}
		}
	}
}

// Translating a whole capture twice with fresh translators gives identical
// results, pending edges and skips included.
func TestTranslatingTwiceGivesTheSameRecords(t *testing.T) {
	for _, ttl := range ttls {
		for _, name := range captures {
			in := loadCapture(t, name)
			a := translateAll(t, newTranslator(t, ttl), in)
			b := translateAll(t, newTranslator(t, ttl), in)
			if !reflect.DeepEqual(a, b) {
				t.Errorf("ttl %s, %s: two translators disagree", ttl, name)
			}
		}
	}
}

// storeFrom sequences the records of the results in order, passes each through
// the coalescer, and writes the batch of each input to a memory store.
func storeFrom(t *testing.T, rs ...[]store.Record) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	co, err := coalesce.New(coalesce.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	seq := uint64(0)
	for _, batch := range rs {
		var out []store.Record
		for _, r := range batch {
			out = co.Add(r, out)
		}
		for i := range out {
			seq++
			out[i].Seq = seq
		}
		if err := s.Write(context.Background(), out); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// Fed through the coalescer into a store, each pod's existence starts at its
// creation and ends at the observed time of its DELETED, in both modes.
func TestPodIntervalsInTheStore(t *testing.T) {
	for _, ttl := range ttls {
		for _, name := range captures {
			t.Run(name+"/ttl="+ttl.String(), func(t *testing.T) {
				in := loadCapture(t, name)
				res := translateAll(t, newTranslator(t, ttl), in)
				batches := make([][]store.Record, len(res))
				for i, r := range res {
					batches[i] = r.Records
				}
				s := storeFrom(t, batches...)

				created := map[string]time.Time{}
				deleted := map[string]time.Time{}
				for _, x := range in {
					typ, o := objectOf(x.Rec)
					if subStr(o, "kind") != "Pod" {
						continue
					}
					uid := subStr(o, "metadata", "uid")
					if _, ok := created[uid]; !ok {
						c, err := time.Parse(time.RFC3339, subStr(o, "metadata", "creationTimestamp"))
						if err != nil {
							t.Fatal(err)
						}
						created[uid] = c
					}
					if typ == "DELETED" {
						deleted[uid] = observedOf(x)
					}
				}
				if len(deleted) == 0 {
					t.Fatal("the capture deletes no pod")
				}
				ctx := context.Background()
				scope := store.Current(catalog.L2)
				alive := func(f identity.Fingerprint, at time.Time) bool {
					ok, err := s.Alive(ctx, f, at, scope)
					if err != nil {
						t.Fatal(err)
					}
					return ok
				}
				for uid, end := range deleted {
					f, born := podFP(t, uid), created[uid]
					for _, c := range []struct {
						what string
						at   time.Time
						want bool
					}{
						{"just before creation", born.Add(-time.Nanosecond), false},
						{"at creation", born, true},
						{"just before its DELETED", end.Add(-time.Nanosecond), true},
						{"at its DELETED", end, false},
						{"long after", end.Add(24 * time.Hour), false},
					} {
						if got := alive(f, c.at); got != c.want {
							t.Errorf("pod %s %s (%s): alive = %v, want %v", uid[:8], c.what, c.at.Format(time.RFC3339Nano), got, c.want)
						}
					}
				}
			})
		}
	}
}

// A pod stays alive between its node's DELETED and its own, and the scheduled_on
// edge ends with the pod, not with the node.
func TestPodOutlivesItsDeletedNode(t *testing.T) {
	in := loadCapture(t, "run1.jsonl")
	res := translateAll(t, newTranslator(t, 0), in)
	batches := make([][]store.Record, len(res))
	for i, r := range res {
		batches[i] = r.Records
	}
	s := storeFrom(t, batches...)

	var nodeGone, podGone time.Time
	var podUID, nodeUID string
	for _, x := range in {
		typ, o := objectOf(x.Rec)
		uid := subStr(o, "metadata", "uid")
		switch {
		case typ == "DELETED" && strings.HasPrefix(uid, uidNode2Old):
			nodeGone, nodeUID = observedOf(x), uid
		case typ == "DELETED" && strings.HasPrefix(uid, uidPodGC):
			podGone, podUID = observedOf(x), uid
		}
	}
	mid := nodeGone.Add(time.Second)
	if !mid.Before(podGone) {
		t.Fatalf("node deleted %s, pod %s: not PodGC", nodeGone, podGone)
	}
	ctx := context.Background()
	pod, node := podFP(t, podUID), nodeFP(t, nodeUID)
	ok, err := s.Alive(ctx, pod, mid, store.Current(catalog.L2))
	if err != nil || !ok {
		t.Fatalf("pod alive after its node's DELETED = %v, %v, want true", ok, err)
	}
	ok, err = s.Alive(ctx, node, mid, store.Current(catalog.L2))
	if err != nil || ok {
		t.Fatalf("node alive after its DELETED = %v, %v, want false", ok, err)
	}
	for _, c := range []struct {
		at   time.Time
		want int
	}{{mid, 1}, {podGone.Add(-time.Nanosecond), 1}, {podGone, 0}} {
		ns, err := s.Neighbors(ctx, pod, store.Forward, c.at, store.Current(catalog.L2))
		if err != nil {
			t.Fatal(err)
		}
		if len(ns) != c.want {
			t.Errorf("at %s the pod has %d neighbors, want %d", c.at.Format(time.RFC3339Nano), len(ns), c.want)
		}
	}
}

// With a TTL, each pull is a refresh, and the coalescer turns the refreshes of one
// pod into a run that stays alive for as long as the pulls keep coming, and
// lapses a TTL after the last.
func TestPullsKeepAPodAliveThroughTheCoalescer(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running"}
	var batches [][]store.Record
	for i := 0; i < 6; i++ {
		p.Heartbeat = string(rune('a' + i))
		at := t0.Add(time.Duration(i) * 10 * time.Minute)
		batches = append(batches, translate(t, tr, pullRec(at, p.object())).Records)
	}
	s := storeFrom(t, batches...)
	ctx, f := context.Background(), podFP(t, "p1")
	last := t0.Add(50 * time.Minute)
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{t0.Add(-time.Nanosecond), false},
		{t0, true},
		{t0.Add(25 * time.Minute), true},
		{last, true},
		{last.Add(ttl - time.Second), true},
		{last.Add(ttl + time.Minute), false},
	} {
		got, err := s.Alive(ctx, f, c.at, store.Current(catalog.L2))
		if err != nil || got != c.want {
			t.Errorf("alive at +%s = %v, %v, want %v", c.at.Sub(t0), got, err, c.want)
		}
	}
}
