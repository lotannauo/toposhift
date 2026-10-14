package interval_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// mkfp is an entity of type typ whose hash starts with n, so tests fix the
// order of peers: store.CompareFingerprints orders by type name, then hash.
func mkfp(t testing.TB, typ catalog.EntityType, n byte) identity.Fingerprint {
	t.Helper()
	var h [identity.FingerprintBytes]byte
	h[0] = n
	copy(h[1:], typ)
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// layerOf is the catalog layer a subject is stored in: its entity type's, or
// its relation's (zero for a relation the catalog does not know).
func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func edge(a, b identity.Fingerprint, rel catalog.RelationType) store.Subject {
	return store.EdgeSubject(a, b, rel)
}

// observe is an Observe of sub by p at at, in the subject's catalog layer.
func observe(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("x"),
	}
}

// run is an Observe that stands for a run of refreshes through through.
func run(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration, through time.Time) store.Record {
	r := observe(sub, p, at, ttl)
	r.Through = through
	return r
}

func deleted(sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Delete}
}

// feed writes batches to a store, numbering records from 1 in write order.
type feed struct {
	t   testing.TB
	st  store.Store
	seq uint64
}

func newFeed(t testing.TB, st store.Store) *feed { return &feed{t: t, st: st, seq: st.LastSeq()} }

// write writes one batch and returns the records with their Seq.
func (f *feed) write(rs ...store.Record) []store.Record {
	f.t.Helper()
	rs = slices.Clone(rs)
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(context.Background(), rs); err != nil {
		f.t.Fatal(err)
	}
	return rs
}

// each writes every record as a batch of its own.
func (f *feed) each(rs ...store.Record) []store.Record {
	f.t.Helper()
	var out []store.Record
	for _, r := range rs {
		out = append(out, f.write(r)...)
	}
	return out
}

func openStore(t testing.TB, opts memstore.Options) *memstore.Store {
	t.Helper()
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// spy wraps a store, counts its reads and writes, logs each read with its
// layer, and can run a hook before each Window.
type spy struct {
	store.Store
	mu       sync.Mutex
	reads    int
	writes   int
	log      []string
	onWindow func(n int) // before the n-th Window, from 1
	afterWin func(n int) // after the n-th Window returns
	windows  int
}

func (s *spy) read(op string, l catalog.Layer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	s.log = append(s.log, fmt.Sprintf("%s %s", op, l))
}

func (s *spy) Write(ctx context.Context, b []store.Record) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Write(ctx, b)
}

func (s *spy) Retain(ctx context.Context, h time.Time) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Retain(ctx, h)
}

func (s *spy) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	s.read("Neighbors", sc.Layer)
	return s.Store.Neighbors(ctx, fp, d, t, sc)
}

func (s *spy) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	s.read("NeighborsBatch", sc.Layer)
	return s.Store.NeighborsBatch(ctx, fps, d, t, sc)
}

func (s *spy) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.read("Alive "+string(fp.Type()), sc.Layer)
	return s.Store.Alive(ctx, fp, t, sc)
}

func (s *spy) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.read("Window "+string(fp.Type()), sc.Layer)
	s.mu.Lock()
	s.windows++
	n, hook, after := s.windows, s.onWindow, s.afterWin
	s.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	recs, err := s.Store.Window(ctx, fp, d, from, to, sc)
	if after != nil {
		after(n)
	}
	return recs, err
}

func (s *spy) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.read("EntityWindow "+string(fp.Type()), sc.Layer)
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

func (s *spy) counts() (reads, writes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads, s.writes
}

func (s *spy) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads, s.writes, s.log, s.windows = 0, 0, nil, 0
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// describe is an element as one comparable line.
func describe(el interval.Element) string {
	var b strings.Builder
	s := el.Subject
	if s.Kind == store.SubjectEntity {
		fmt.Fprintf(&b, "entity %s", s.A)
	} else {
		fmt.Fprintf(&b, "edge %s -[%s]-> %s", s.A, s.Relation, s.B)
	}
	fmt.Fprintf(&b, " %s %s transitions=%d", el.Layer, el.Class, el.Transitions)
	if el.Quarantine != nil {
		fmt.Fprintf(&b, " quarantine=%q", el.Quarantine.Error())
	}
	if el.Uncascaded != nil {
		fmt.Fprintf(&b, " uncascaded=%q", el.Uncascaded.Error())
	}
	if el.Pieces == nil {
		b.WriteString(" pieces=nil")
	}
	for _, p := range el.Pieces {
		fmt.Fprintf(&b, " [%s, %s) before=%t %s %v", ts(p.Start), ts(p.End), p.Before, p.EndSource, p.Producers)
	}
	return b.String()
}

func describeAll(els []interval.Element) []string {
	out := make([]string, len(els))
	for i, el := range els {
		out[i] = describe(el)
	}
	return out
}

// diff returns "" when the two lists of elements are the same, and otherwise
// both, line by line.
func diff(got, want []interval.Element) string {
	g, w := describeAll(got), describeAll(want)
	if slices.Equal(g, w) {
		return ""
	}
	return fmt.Sprintf("got:\n  %s\nwant:\n  %s", strings.Join(g, "\n  "), strings.Join(w, "\n  "))
}

// keys is the set of the elements' subjects.
func keys(els []interval.Element) map[store.Subject]bool {
	out := map[store.Subject]bool{}
	for _, el := range els {
		out[el.Subject] = true
	}
	return out
}

var ctx = context.Background()
