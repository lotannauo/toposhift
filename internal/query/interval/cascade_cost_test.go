package interval_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// endpointReads counts the existence reads in a spy's log: one Alive per
// distinct endpoint read.
func endpointReads(s *spy) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.log {
		if strings.HasPrefix(l, "Alive ") {
			n++
		}
	}
	return n
}

// A node with a long history of short-lived pods: the cascade reads the
// endpoints of the edges in the window only, not of every pod the node ever
// had.
func TestCascadeReadsOnlyTheEndpointsOfEdgesInTheWindow(t *testing.T) {
	const history = 50
	inner := openStore(t, memstore.Options{})
	f := newFeed(t, inner)
	n := mkfp(t, catalog.K8sNode, 1)
	f.write(observe(ent(n), k8s, hm(7, 0), 0))
	for i := range history {
		p := mkfp(t, catalog.K8sPod, byte(10+i))
		e := edge(p, n, catalog.ScheduledOn)
		f.write(
			observe(ent(p), k8s, hm(8, i), 0),
			observe(e, k8s, hm(8, i), 0), deleted(e, k8s, hm(8, i).Add(30*time.Second)),
		)
	}
	x, y := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sPod, 2)
	f.write(
		observe(ent(x), k8s, hm(9, 0), 0), observe(ent(y), k8s, hm(9, 0), 0),
		observe(edge(x, n, catalog.ScheduledOn), k8s, hm(9, 50), 0),
		observe(edge(y, n, catalog.ScheduledOn), k8s, hm(10, 10), 0),
	)
	w := win(hm(10, 0), hm(10, 30))

	st := &spy{Store: inner}
	res := mustEdges(t, interval.NewReader(st, interval.Options{}), n, store.Reverse, w)
	if len(res.Elements) != 2 {
		t.Fatalf("Edges(N, Reverse) = %v", describeAll(res.Elements))
	}
	if got := endpointReads(st); got != 3 {
		t.Errorf("%d endpoint reads, want 3 (the node and the two pods in the window): %q", got, st.log)
	}

	every := &spy{Store: inner}
	all := mustEdges(t, interval.ReadingEveryEndpoint(interval.NewReader(every, interval.Options{})), n, store.Reverse, w)
	check(t, "against reading every endpoint", res.Elements, all.Elements)
	if got := endpointReads(every); got != history+3 {
		t.Errorf("reading every endpoint made %d endpoint reads, want %d", got, history+3)
	}
}

// An edge with nothing in the window touches no endpoint layer, so a retained
// endpoint layer does not refuse it; an edge in the window still is refused
// (scenario H).
func TestCascadeOfAnEdgeOutsideTheWindowReadsNoEndpointLayer(t *testing.T) {
	st := openStore(t, memstore.Options{Keep: [4]bool{true, true, false, true}})
	nodeA, h := mkfp(t, catalog.K8sNode, 1), mkfp(t, catalog.Host, 1)
	e := edge(nodeA, h, catalog.RunsOn)
	newFeed(t, st).write(
		observe(ent(nodeA), k8s, hm(9, 0), 0), observe(ent(h), k8s, hm(9, 0), 0),
		observe(e, k8s, hm(9, 0), 0), deleted(e, k8s, hm(9, 30)),
	)
	if err := st.Retain(ctx, hm(10, 0)); err != nil {
		t.Fatal(err)
	}
	w := win(hm(10, 0), hm(10, 30))
	res, err := interval.NewReader(st, interval.Options{}).Edges(ctx, nodeA, store.Forward, w)
	if err != nil || len(res.Elements) != 0 {
		t.Errorf("Edges(N, Forward) = %v, %v; want an empty answer", describeAll(res.Elements), err)
	}
	_, err = interval.ReadingEveryEndpoint(interval.NewReader(st, interval.Options{})).Edges(ctx, nodeA, store.Forward, w)
	if !errors.Is(err, interval.ErrRetained) {
		t.Errorf("reading every endpoint: %v, want ErrRetained", err)
	}
}

// TestP13CascadeSkipChangesNoAnswer: on the grid, the cascade answers equal
// those of reading every endpoint, for Edges and EdgesBatch, and the endpoints
// read are exactly the distinct endpoints of the edges with an interval of
// their own in the window.
func TestP13CascadeSkipChangesNoAnswer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		raw := interval.NewReader(c.st, interval.Options{Raw: true})
		for _, d := range dirs {
			for _, e := range c.g.entities {
				sp := &spy{Store: c.st}
				got, err := interval.NewReader(sp, interval.Options{}).Edges(ctx, e, d, c.w)
				if err != nil {
					t.Fatal(err)
				}
				want, err := interval.ReadingEveryEndpoint(interval.NewReader(c.st, interval.Options{})).Edges(ctx, e, d, c.w)
				if err != nil {
					t.Fatal(err)
				}
				if df := diff(got.Elements, want.Elements); df != "" || got.Token != want.Token {
					t.Fatalf("Edges(%s, %s) with the skip and without:\n%s", e, d, df)
				}
				own, err := raw.Edges(ctx, e, d, c.w)
				if err != nil {
					t.Fatal(err)
				}
				ends := map[identity.Fingerprint]bool{}
				for _, el := range own.Elements {
					ends[el.Subject.A], ends[el.Subject.B] = true, true
				}
				if n := endpointReads(sp); n != len(ends) {
					t.Fatalf("Edges(%s, %s): %d endpoint reads, want %d: %q", e, d, n, len(ends), sp.log)
				}
			}
			_, got, err := interval.NewReader(c.st, interval.Options{}).EdgesBatch(ctx, c.g.entities, d, c.w)
			if err != nil {
				t.Fatal(err)
			}
			_, want, err := interval.ReadingEveryEndpoint(interval.NewReader(c.st, interval.Options{})).EdgesBatch(ctx, c.g.entities, d, c.w)
			if err != nil {
				t.Fatal(err)
			}
			for i := range got {
				if df := diff(got[i], want[i]); df != "" {
					t.Fatalf("EdgesBatch[%d] %s with the skip and without:\n%s", i, d, df)
				}
			}
		}
	})
}
