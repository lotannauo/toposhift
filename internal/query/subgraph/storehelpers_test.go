package subgraph_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

const (
	k8s   lifecycle.Producer = "k8sobjects/default"
	agent lifecycle.Producer = "nodeagent/default"
)

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func sub(k subgraph.Key) store.Subject {
	if k.Kind == subgraph.KindEntity {
		return store.EntitySubject(k.A)
	}
	return store.EdgeSubject(k.A, k.B, k.Relation)
}

// observe is an Observe at sec seconds after base.
func observe(k subgraph.Key, p lifecycle.Producer, sec int, ttl time.Duration) store.Record {
	s := sub(k)
	return store.Record{
		Layer: layerOf(s), Subject: s, Producer: p, EventTime: at(sec),
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("{}"),
	}
}

func deleted(k subgraph.Key, p lifecycle.Producer, sec int) store.Record {
	s := sub(k)
	return store.Record{Layer: layerOf(s), Subject: s, Producer: p, EventTime: at(sec), Kind: lifecycle.Delete}
}

// feed writes batches to a store, numbering records after its LastSeq.
type feed struct {
	t   testing.TB
	st  store.Store
	seq uint64
}

func newFeed(t testing.TB, st store.Store) *feed { return &feed{t: t, st: st, seq: st.LastSeq()} }

func (f *feed) write(rs ...store.Record) {
	f.t.Helper()
	rs = slices.Clone(rs)
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(context.Background(), rs); err != nil {
		f.t.Fatal(err)
	}
}

func openStore(t testing.TB, opts memstore.Options) (*memstore.Store, *feed) {
	t.Helper()
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, newFeed(t, st)
}

// spy wraps a store and counts every call; it records each read's token, counts
// writes, and can run a hook at the n-th LastSeq call.
type spy struct {
	store.Store
	mu       sync.Mutex
	calls    int
	lastSeqs int
	writes   int
	tokens   []uint64
	onLast   func(n int)
	// lastFn, when set, changes the value the n-th LastSeq call returns.
	lastFn func(n int, last uint64) uint64
}

func (s *spy) note(tok uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.tokens = append(s.tokens, tok)
}

func (s *spy) LastSeq() uint64 {
	s.mu.Lock()
	s.calls++
	s.lastSeqs++
	n, hook, fn := s.lastSeqs, s.onLast, s.lastFn
	s.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	last := s.Store.LastSeq()
	if fn != nil {
		last = fn(n, last)
	}
	return last
}

func (s *spy) LayerHorizon(l catalog.Layer) store.Horizon {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Store.LayerHorizon(l)
}

func (s *spy) Write(ctx context.Context, b []store.Record) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Write(ctx, b)
}

func (s *spy) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	s.note(sc.AsOf)
	return s.Store.Neighbors(ctx, fp, d, t, sc)
}

func (s *spy) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	s.note(sc.AsOf)
	return s.Store.NeighborsBatch(ctx, fps, d, t, sc)
}

func (s *spy) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.note(sc.AsOf)
	return s.Store.Alive(ctx, fp, t, sc)
}

func (s *spy) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc.AsOf)
	return s.Store.Window(ctx, fp, d, from, to, sc)
}

func (s *spy) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc.AsOf)
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

// pointPresence is what the store's point reads say is present at x, with the
// cascade: each scope entity Alive, and each edge listed by Neighbors of a scope
// entity with both endpoints Alive. Quarantined entities and the edges touching
// them are left out.
func pointPresence(t tb, st store.Store, scope []identity.Fingerprint, x time.Time, token uint64) map[subgraph.Key]bool {
	t.Helper()
	alive := func(fp identity.Fingerprint) (ok, quarantined bool) {
		ok, err := st.Alive(ctx, fp, x, store.Scope{Layer: layerOf(sub(ent(fp))), AsOf: token})
		var qe *store.QuarantineError
		if errors.As(err, &qe) {
			return false, true
		}
		if err != nil {
			t.Fatal(err)
		}
		return ok, false
	}
	cat := catalog.Default()
	out := map[subgraph.Key]bool{}
	for _, fp := range scope {
		if ok, q := alive(fp); ok && !q {
			out[ent(fp)] = true
		}
		for _, dir := range []store.Direction{store.Forward, store.Reverse} {
			for _, l := range interval.EdgeLayers(cat, fp.Type(), dir) {
				ns, err := st.Neighbors(ctx, fp, dir, x, store.Scope{Layer: l, AsOf: token})
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range ns {
					k := subgraph.EdgeKey(fp, n.Peer, n.Relation)
					if dir == store.Reverse {
						k = subgraph.EdgeKey(n.Peer, fp, n.Relation)
					}
					a, qa := alive(k.A)
					b, qb := alive(k.B)
					if a && b && !qa && !qb {
						out[k] = true
					}
				}
			}
		}
	}
	return out
}

// checkAgainstPointReads checks the subgraph against the store's point reads at
// every instant of a delta and its neighbours: the elements the keyframes and
// deltas give, forward and backward, are exactly the keys the point reads show
// present, and each keyframe too.
func checkAgainstPointReads(t tb, st store.Store, scope []identity.Fingerprint, sg subgraph.Subgraph) {
	t.Helper()
	keys := func(set []uint32) map[subgraph.Key]bool {
		out := map[subgraph.Key]bool{}
		for _, id := range set {
			out[sg.Elements[id].Key] = true
		}
		return out
	}
	same := func(a, b map[subgraph.Key]bool) bool {
		if len(a) != len(b) {
			return false
		}
		for k := range a {
			if !b[k] {
				return false
			}
		}
		return true
	}
	for _, rel := range instants(sg) {
		want := pointPresence(t, st, scope, sg.From.Add(time.Duration(rel)), sg.Token)
		if got := keys(forward(t, sg, rel)); !same(got, want) {
			t.Fatalf("forward at +%dns: %v, the point reads say %v", rel, got, want)
		}
		if set, ok := backward(t, sg, rel); ok {
			if got := keys(set); !same(got, want) {
				t.Fatalf("backward at +%dns: %v, the point reads say %v", rel, got, want)
			}
		}
	}
	for i, kf := range sg.Keyframes {
		want := pointPresence(t, st, scope, sg.From.Add(time.Duration(kf.At)), sg.Token)
		if got := keys(kf.Elements); !same(got, want) {
			t.Fatalf("keyframe %d: %v, the point reads say %v", i, got, want)
		}
	}
}
