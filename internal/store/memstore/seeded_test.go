package memstore_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The seeds of a node's reverse edges, worked out by hand: each line of want is a
// seed as pod/producer/seq, and a trailing "+p" says it carries its payload.
func TestWindowSinceSeedsAreTheReferencesLiveJustBeforeFrom(t *testing.T) {
	t.Parallel()
	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "seed-node")
	pods := []identity.Fingerprint{
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "seed-a"),
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "seed-b"),
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "seed-c"),
	}
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	var seq uint64
	edge := func(pod int, p lifecycle.Producer, d time.Duration, kind lifecycle.Kind, ttl time.Duration) store.Record {
		seq++
		r := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pods[pod], node, catalog.ScheduledOn), Producer: p,
			EventTime: at(d), Seq: seq, Kind: kind, TTL: ttl,
		}
		if kind == lifecycle.Observe {
			r.Payload = fmt.Appendf(nil, "p%d", seq)
		}
		return r
	}
	m := time.Minute
	write := func(recs ...store.Record) {
		t.Helper()
		if err := s.Write(context.Background(), recs); err != nil {
			t.Fatal(err)
		}
	}
	write(
		edge(0, "a", 0, lifecycle.Observe, 0),     // 1: a holds pod 0 for good
		edge(0, "b", 0, lifecycle.Observe, 3*m),   // 2: b holds it until 3m
		edge(1, "a", m, lifecycle.Observe, 0),     // 3
		edge(1, "a", m, lifecycle.Delete, 0),      // 4: overwrites 3 at its instant
		edge(2, "a", 2*m, lifecycle.Observe, 2*m), // 5: until 4m
	)
	write(edge(0, "b", 2*m, lifecycle.Delete, 0))  // 6: b lets go of pod 0
	write(edge(1, "a", m, lifecycle.Observe, 0))   // 7: re-asserts pod 1 at m
	write(edge(2, "a", 4*m, lifecycle.Observe, 0)) // 8: at 4m, the window's from below

	seeds := func(from time.Time, sc store.Scope, o store.SinceOptions) []string {
		t.Helper()
		got, err := s.WindowSince(context.Background(), []identity.Fingerprint{node}, store.Reverse, from, from.Add(time.Hour), sc, o)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, sd := range got[0].Seeds {
			x := fmt.Sprintf("%d/%s/%d", slices.Index(pods, sd.Record.Subject.A), sd.Record.Producer, sd.Record.Seq)
			if sd.PayloadKnown {
				x += "+p"
				if string(sd.Record.Payload) != fmt.Sprintf("p%d", sd.Record.Seq) {
					t.Errorf("seed %s has the payload %q", x, sd.Record.Payload)
				}
			} else if sd.Record.Payload != nil {
				t.Errorf("seed %s has a payload it does not know", x)
			}
			out = append(out, x)
		}
		slices.Sort(out)
		return out
	}
	tok := func(asOf uint64) store.Scope { return store.Scope{Layer: catalog.L2, AsOf: asOf} }
	for _, c := range []struct {
		name string
		from time.Duration
		sc   store.Scope
		o    store.SinceOptions
		want []string
	}{
		{"before anything", 0, l2, store.SinceOptions{}, nil},
		{"two producers of one subject, at 90s", 90 * time.Second, l2, store.SinceOptions{}, []string{"0/a/1", "0/b/2", "1/a/7"}},
		{"per reference: a holds after b let go", 150 * time.Second, l2, store.SinceOptions{}, []string{"0/a/1", "1/a/7", "2/a/5"}},
		{"the token that does not see the re-assertion", 90 * time.Second, tok(6), store.SinceOptions{}, []string{"0/a/1", "0/b/2"}},
		{"the token that sees only the first records", 90 * time.Second, tok(3), store.SinceOptions{}, []string{"0/a/1", "0/b/2", "1/a/3"}},
		{"a deadline at from holds just before it", 4 * m, l2, store.SinceOptions{Payloads: true}, []string{"0/a/1+p", "1/a/7+p", "2/a/5+p"}},
		{"and is gone a nanosecond later", 4*m + 1, l2, store.SinceOptions{}, []string{"0/a/1", "1/a/7", "2/a/8"}},
	} {
		if got := seeds(at(c.from), c.sc, c.o); !slices.Equal(got, c.want) {
			t.Errorf("%s: seeds %v, want %v", c.name, got, c.want)
		}
	}

	// At the horizon 4m: pod 2's reference by a ends exactly there, and is no seed;
	// what was asserted before 4m has no payload.
	if err := s.Retain(context.Background(), at(4*m)); err != nil {
		t.Fatal(err)
	}
	if got, want := seeds(at(4*m), l2, store.SinceOptions{Payloads: true}), []string{"0/a/1", "1/a/7"}; !slices.Equal(got, want) {
		t.Errorf("from the horizon: seeds %v, want %v", got, want)
	}
	if got, want := seeds(at(5*m), l2, store.SinceOptions{Payloads: true}), []string{"0/a/1", "1/a/7", "2/a/8+p"}; !slices.Equal(got, want) {
		t.Errorf("after the horizon: seeds %v, want %v", got, want)
	}
	if _, err := s.WindowSince(context.Background(), nil, 0, at(4*m-1), at(5*m), l2, store.SinceOptions{}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a window from before the horizon with no fingerprints: %v, want ErrBeforeHorizon", err)
	}
}
