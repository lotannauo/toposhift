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

// A node with a long history of short-lived pods: the read returns the edges
// alive just before the window and those with records in it, never the history
// before them, and the cascade reads their endpoints only. Reading every
// endpoint of what is read reads the same three, because the pods placed and
// gone before the window are not even in what is read.
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
	if got := endpointReads(every); got != 3 {
		t.Errorf("reading every endpoint made %d endpoint reads, want 3", got)
	}
	recs, err := inner.WindowSince(ctx, []identity.Fingerprint{n}, store.Reverse, w.From, w.To, store.Current(catalog.L2), store.SinceOptions{})
	if err != nil || len(recs[0].Seeds)+len(recs[0].Window) != 2 {
		t.Errorf("the node's reverse read gives %+v, %v; want x's seed and y's record, not the %d pods before them", recs, err, history)
	}
}

// An edge that ended before the window is not read at all, so even reading every
// endpoint reads none of its endpoints; and a window from the horizon of a
// retained endpoint layer is answered, from the seeds, as a store that kept
// everything answers it. One that starts before that horizon is refused by the
// store at the endpoint's existence.
func TestCascadeOfAnEdgeOutsideTheWindowReadsNoEndpointLayer(t *testing.T) {
	proc, h := mkfp(t, catalog.Process, 1), mkfp(t, catalog.Host, 1)
	e := edge(proc, h, catalog.RunsOn)
	recs := []store.Record{
		observe(ent(proc), k8s, hm(9, 0), 0), observe(ent(h), k8s, hm(9, 0), 0),
		observe(e, k8s, hm(9, 0), 0), deleted(e, k8s, hm(9, 30)),
	}
	inner := openStore(t, memstore.Options{Keep: [4]bool{true, false, false, true}})
	newFeed(t, inner).write(recs...)
	if err := inner.Retain(ctx, hm(10, 0)); err != nil {
		t.Fatal(err)
	}
	w := win(hm(10, 0), hm(10, 30))
	for _, every := range []bool{false, true} {
		st := &spy{Store: inner}
		r := interval.NewReader(st, interval.Options{})
		if every {
			r = interval.ReadingEveryEndpoint(r)
		}
		res, err := r.Edges(ctx, proc, store.Forward, w)
		if err != nil || len(res.Elements) != 0 || endpointReads(st) != 0 {
			t.Errorf("every %t: Edges(process, Forward) = %v, %v after reads %q; want an empty answer and no endpoint read", every, describeAll(res.Elements), err, st.log)
		}
	}

	// The edge alive across the horizon: its endpoints are read at From, which is
	// the horizon, and the answer is the twin's.
	inner2 := openStore(t, memstore.Options{Keep: [4]bool{true, false, false, true}})
	twin := openStore(t, memstore.Options{})
	for _, st := range []store.Store{inner2, twin} {
		newFeed(t, st).write(recs[:3]...)
	}
	if err := inner2.Retain(ctx, hm(10, 0)); err != nil {
		t.Fatal(err)
	}
	got, err := interval.NewReader(inner2, interval.Options{}).Edges(ctx, proc, store.Forward, w)
	want := mustEdges(t, interval.NewReader(twin, interval.Options{}), proc, store.Forward, w)
	if err != nil || diff(got.Elements, want.Elements) != "" || len(want.Elements) != 1 {
		t.Errorf("Edges(process, Forward) after the retention = %v, %v; want %v", describeAll(got.Elements), err, describeAll(want.Elements))
	}
	_, err = interval.NewReader(inner2, interval.Options{}).Edges(ctx, proc, store.Forward, win(hm(9, 59), hm(10, 30)))
	if !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a window before the horizon: %v, want ErrBeforeHorizon", err)
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
