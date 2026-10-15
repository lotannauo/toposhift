package extract

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// These tests are the guard of the memo's keys, its horizon and token rules
// and its cap, which nothing varies inside one extraction.

var bg = context.Background()

func fpOf(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

var (
	tPod   = fpOf(catalog.K8sPod, 1)
	tPod2  = fpOf(catalog.K8sPod, 2)
	tNode  = fpOf(catalog.K8sNode, 1)
	tNode2 = fpOf(catalog.K8sNode, 2)
)

func tm(m int) time.Time { return time.Date(2026, 1, 1, 10, m, 0, 0, time.UTC) }

func entityLayer(typ catalog.EntityType) catalog.Layer {
	e, ok := catalog.Default().Entity(typ)
	if !ok {
		panic(typ)
	}
	return e.Layer()
}

var podLayer = entityLayer(catalog.K8sPod)

func scheduled(p, n identity.Fingerprint) store.Subject {
	return store.EdgeSubject(p, n, catalog.ScheduledOn)
}

func edgeRec(p, n identity.Fingerprint, at time.Time) store.Record {
	rel, _ := catalog.Default().Relation(catalog.ScheduledOn)
	return store.Record{
		Layer: rel.Layer(), Subject: scheduled(p, n), Producer: "k8sobjects/default",
		EventTime: at, Kind: lifecycle.Observe, Payload: []byte("x"),
	}
}

func entityRec(fp identity.Fingerprint, at time.Time) store.Record {
	return store.Record{
		Layer: entityLayer(fp.Type()), Subject: store.EntitySubject(fp), Producer: "k8sobjects/default",
		EventTime: at, Kind: lifecycle.Observe, Payload: []byte("x"),
	}
}

// fixture is a memstore with a counting layer beneath a memo.
type fixture struct {
	t    *testing.T
	st   *memstore.Store
	cnt  *counting
	memo *memo
	seq  uint64
}

func newFixture(t *testing.T, capacity int) *fixture {
	t.Helper()
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cnt := &counting{Store: st}
	return &fixture{t: t, st: st, cnt: cnt, memo: newMemo(cnt, capacity)}
}

func (f *fixture) write(rs ...store.Record) {
	f.t.Helper()
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(bg, rs); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) scope() store.Scope { return store.Scope{Layer: edgeLayer(), AsOf: f.seq} }

func edgeLayer() catalog.Layer {
	rel, _ := catalog.Default().Relation(catalog.ScheduledOn)
	return rel.Layer()
}

func (f *fixture) window(fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) []store.Record {
	f.t.Helper()
	rs, err := f.memo.Window(bg, fp, d, from, to, s)
	if err != nil {
		f.t.Fatal(err)
	}
	return rs
}

func (f *fixture) storeReads() int {
	return f.cnt.reads.Windows + f.cnt.reads.EntityWindows + f.cnt.reads.Alive
}

func seqs(rs []store.Record) []uint64 {
	var out []uint64
	for _, r := range rs {
		out = append(out, r.Seq)
	}
	return out
}

func wantSeqs(t *testing.T, what string, rs []store.Record, want ...uint64) {
	t.Helper()
	if got := seqs(rs); !slices.Equal(got, want) {
		t.Errorf("%s: records %v, want %v", what, got, want)
	}
}

func wantReads(t *testing.T, f *fixture, want int) {
	t.Helper()
	if got := f.storeReads(); got != want {
		t.Errorf("%d reads reached the store, want %d", got, want)
	}
}

func TestMemoServesARepeatedReadFromMemory(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	s := f.scope()
	first := f.window(tNode, store.Reverse, tm(0), tm(30), s)
	second := f.window(tNode, store.Reverse, tm(0), tm(30), s)
	wantSeqs(t, "first", first, 1)
	wantSeqs(t, "second", second, 1)
	wantReads(t, f, 1)
	if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), s); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), s); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.Alive(bg, tPod, tm(0), s); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.Alive(bg, tPod, tm(0), s); err != nil {
		t.Fatal(err)
	}
	wantReads(t, f, 3)
}

// Each part of the key separates reads: a read that differs in one reaches the
// store, and a read that does not differ in any does not.
func TestMemoKeyParts(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)), entityRec(tPod, tm(0)))
	base := f.scope()
	other := store.Scope{Layer: catalog.L3, AsOf: base.AsOf}
	if other.Layer == base.Layer {
		t.Fatal("the test needs two layers")
	}
	f.window(tNode, store.Reverse, tm(0), tm(30), base)
	reads := 1
	for name, read := range map[string]func(){
		"fingerprint": func() { f.window(tNode2, store.Reverse, tm(0), tm(30), base) },
		"direction":   func() { f.window(tNode, store.Forward, tm(0), tm(30), base) },
		"from":        func() { f.window(tNode, store.Reverse, tm(1), tm(30), base) },
		"to":          func() { f.window(tNode, store.Reverse, tm(0), tm(29), base) },
		"nanosecond":  func() { f.window(tNode, store.Reverse, tm(0).Add(time.Nanosecond), tm(30), base) },
		"layer":       func() { f.window(tNode, store.Reverse, tm(0), tm(30), other) },
		"token": func() {
			f.window(tNode, store.Reverse, tm(0), tm(30), store.Scope{Layer: base.Layer, AsOf: base.AsOf - 1})
		},
		"method": func() {
			if _, err := f.memo.EntityWindow(bg, tNode, tm(0), tm(30), base); err != nil {
				t.Error(err)
			}
		},
	} {
		read()
		reads++
		if got := f.storeReads(); got != reads {
			t.Errorf("a read differing in the %s: %d store reads, want %d", name, got, reads)
		}
	}
	// A repeat of each is now a hit.
	f.window(tNode, store.Reverse, tm(0), tm(30), base)
	f.window(tNode2, store.Reverse, tm(0), tm(30), base)
	f.window(tNode, store.Forward, tm(0), tm(30), base)
	wantReads(t, f, reads)
}

// The key is the instant, not the time.Time: one instant in two locations, or
// with and without a monotonic reading, is one entry.
func TestMemoKeyIsTheInstant(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	s := f.scope()
	zone := time.FixedZone("x", 3*3600)
	f.window(tNode, store.Reverse, tm(0), tm(30), s)
	f.window(tNode, store.Reverse, tm(0).In(zone), tm(30).In(zone), s)
	wantReads(t, f, 1)
	now := time.Now()
	f.window(tNode2, store.Reverse, now, now.Add(time.Hour), s)
	f.window(tNode2, store.Reverse, now.Round(0), now.Round(0).Add(time.Hour), s)
	wantReads(t, f, 2)
	// Two instants a nanosecond apart, in the same second, are two entries.
	f.window(tNode2, store.Reverse, now.Add(time.Nanosecond), now.Add(time.Hour), s)
	wantReads(t, f, 3)
}

// The key has the token: a read at the next token sees the record written
// between them.
func TestMemoKeyHasTheToken(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	k := f.scope()
	wantSeqs(t, "at k", f.window(tNode, store.Reverse, tm(0), tm(30), k), 1)
	f.write(edgeRec(tPod2, tNode, tm(2)))
	wantSeqs(t, "at k+1", f.window(tNode, store.Reverse, tm(0), tm(30), f.scope()), 1, 2)
	wantSeqs(t, "at k again", f.window(tNode, store.Reverse, tm(0), tm(30), k), 1)
	wantReads(t, f, 2)
}

// A read at Latest is never kept: it sees the next write.
func TestMemoNeverKeepsLatest(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	s := store.Scope{Layer: edgeLayer(), AsOf: store.Latest}
	wantSeqs(t, "before", f.window(tNode, store.Reverse, tm(0), tm(30), s), 1)
	f.write(edgeRec(tPod2, tNode, tm(2)))
	wantSeqs(t, "after", f.window(tNode, store.Reverse, tm(0), tm(30), s), 1, 2)
	wantReads(t, f, 2)
	if f.memo.used != 0 {
		t.Errorf("%d units kept for Latest reads", f.memo.used)
	}
}

// A token above the store's last Seq (not Latest) is treated as the last Seq by
// the store, so its answer is not final and is never kept.
func TestMemoNeverKeepsATokenAboveTheLastSeq(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	s := store.Scope{Layer: edgeLayer(), AsOf: f.seq + 5}
	wantSeqs(t, "before", f.window(tNode, store.Reverse, tm(0), tm(30), s), 1)
	f.write(edgeRec(tPod2, tNode, tm(2)))
	wantSeqs(t, "after", f.window(tNode, store.Reverse, tm(0), tm(30), s), 1, 2)
	wantReads(t, f, 2)
	// Once the store has caught up with the token, the answer is final.
	for f.seq < s.AsOf {
		f.write(edgeRec(tPod, tNode2, tm(3)))
	}
	f.window(tNode, store.Reverse, tm(0), tm(30), s)
	f.window(tNode, store.Reverse, tm(0), tm(30), s)
	wantReads(t, f, 3)
}

// A hit is not served once a Retain has moved the layer's horizon: the store
// refuses the read.
func TestMemoNoHitAfterTheHorizonMoves(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)), entityRec(tPod, tm(0)))
	s := f.scope()
	f.window(tNode, store.Reverse, tm(0), tm(30), s)
	if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), store.Scope{Layer: podLayer, AsOf: s.AsOf}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.Alive(bg, tPod, tm(0), store.Scope{Layer: podLayer, AsOf: s.AsOf}); err != nil {
		t.Fatal(err)
	}
	if err := f.st.Retain(bg, tm(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.Window(bg, tNode, store.Reverse, tm(0), tm(30), s); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("Window after the horizon moved: %v, want ErrBeforeHorizon", err)
	}
	if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), store.Scope{Layer: podLayer, AsOf: s.AsOf}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("EntityWindow after the horizon moved: %v, want ErrBeforeHorizon", err)
	}
	if _, err := f.memo.Alive(bg, tPod, tm(0), store.Scope{Layer: podLayer, AsOf: s.AsOf}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("Alive after the horizon moved: %v, want ErrBeforeHorizon", err)
	}
}

// A read made after the horizon moved is not kept either.
func TestMemoKeepsNothingUnderAHorizon(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(11)))
	if err := f.st.Retain(bg, tm(10)); err != nil {
		t.Fatal(err)
	}
	s := f.scope()
	f.window(tNode, store.Reverse, tm(10), tm(30), s)
	f.window(tNode, store.Reverse, tm(10), tm(30), s)
	wantReads(t, f, 2)
	if f.memo.used != 0 {
		t.Errorf("%d units kept under a horizon", f.memo.used)
	}
}

// A hit fails under a cancelled context, as a store read would.
func TestMemoCancelledContextFailsAHit(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)), entityRec(tPod, tm(0)))
	s, ps := f.scope(), store.Scope{Layer: podLayer, AsOf: f.seq}
	f.window(tNode, store.Reverse, tm(0), tm(30), s)
	if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), ps); err != nil {
		t.Fatal(err)
	}
	if _, err := f.memo.Alive(bg, tPod, tm(0), ps); err != nil {
		t.Fatal(err)
	}
	n := f.storeReads()
	c, cancel := context.WithCancel(bg)
	cancel()
	if _, err := f.memo.Window(c, tNode, store.Reverse, tm(0), tm(30), s); !errors.Is(err, context.Canceled) {
		t.Errorf("Window hit: %v, want context.Canceled", err)
	}
	if _, err := f.memo.EntityWindow(c, tPod, tm(0), tm(30), ps); !errors.Is(err, context.Canceled) {
		t.Errorf("EntityWindow hit: %v, want context.Canceled", err)
	}
	if _, err := f.memo.Alive(c, tPod, tm(0), ps); !errors.Is(err, context.Canceled) {
		t.Errorf("Alive hit: %v, want context.Canceled", err)
	}
	// And a miss fails too, before it reaches the store.
	if _, err := f.memo.Window(c, tNode2, store.Reverse, tm(0), tm(30), s); !errors.Is(err, context.Canceled) {
		t.Errorf("Window miss: %v, want context.Canceled", err)
	}
	if f.storeReads() != n {
		t.Errorf("a cancelled call reached the store")
	}
}

// An answer is a copy: a caller that changes it does not change the next.
func TestMemoAnswersAreCopies(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)), edgeRec(tPod2, tNode, tm(2)))
	s := f.scope()
	for round := range 3 {
		got := f.window(tNode, store.Reverse, tm(0), tm(30), s)
		wantSeqs(t, fmt.Sprintf("round %d", round), got, 1, 2)
		got[0].Seq = 99
		got[1] = store.Record{}
	}
	wantReads(t, f, 1)
}

// failing makes the first read of each kind fail with a plain error.
type failing struct {
	store.Store
	mu    sync.Mutex
	fails map[string]bool
}

func (s *failing) fail(op string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fails[op] {
		s.fails[op] = false
		return true
	}
	return false
}

var errBoom = errors.New("boom")

func (s *failing) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if s.fail("Window") {
		return nil, errBoom
	}
	return s.Store.Window(ctx, fp, d, from, to, sc)
}

func (s *failing) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if s.fail("EntityWindow") {
		return nil, errBoom
	}
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

func (s *failing) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if s.fail("Alive") {
		return false, errBoom
	}
	return s.Store.Alive(ctx, fp, t, sc)
}

// An error other than a quarantine is returned and never kept: the next read
// reaches the store and succeeds.
func TestMemoDoesNotKeepAnError(t *testing.T) {
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Write(bg, []store.Record{
		func() store.Record { r := edgeRec(tPod, tNode, tm(1)); r.Seq = 1; return r }(),
		func() store.Record { r := entityRec(tPod, tm(0)); r.Seq = 2; return r }(),
	}); err != nil {
		t.Fatal(err)
	}
	fl := &failing{Store: st, fails: map[string]bool{"Window": true, "EntityWindow": true, "Alive": true}}
	cnt := &counting{Store: fl}
	m := newMemo(cnt, memoCap)
	s, ps := store.Scope{Layer: edgeLayer(), AsOf: 2}, store.Scope{Layer: podLayer, AsOf: 2}
	if _, err := m.Window(bg, tNode, store.Reverse, tm(0), tm(30), s); !errors.Is(err, errBoom) {
		t.Fatalf("Window: %v", err)
	}
	if rs, err := m.Window(bg, tNode, store.Reverse, tm(0), tm(30), s); err != nil || len(rs) != 1 {
		t.Errorf("Window after the failure: %v, %d records", err, len(rs))
	}
	if _, err := m.EntityWindow(bg, tPod, tm(0), tm(30), ps); !errors.Is(err, errBoom) {
		t.Fatalf("EntityWindow: %v", err)
	}
	if rs, err := m.EntityWindow(bg, tPod, tm(0), tm(30), ps); err != nil || len(rs) != 1 {
		t.Errorf("EntityWindow after the failure: %v, %d records", err, len(rs))
	}
	if _, err := m.Alive(bg, tPod, tm(0), ps); !errors.Is(err, errBoom) {
		t.Fatalf("Alive: %v", err)
	}
	if alive, err := m.Alive(bg, tPod, tm(0), ps); err != nil || !alive {
		t.Errorf("Alive after the failure: %v, %t", err, alive)
	}
	if got := cnt.reads.Windows + cnt.reads.EntityWindows + cnt.reads.Alive; got != 6 {
		t.Errorf("%d reads reached the store, want 6", got)
	}
	// Both are now kept.
	_, _ = m.Alive(bg, tPod, tm(0), ps)
	_, _ = m.Window(bg, tNode, store.Reverse, tm(0), tm(30), s)
	if got := cnt.reads.Windows + cnt.reads.EntityWindows + cnt.reads.Alive; got != 6 {
		t.Errorf("%d reads reached the store after kept answers, want 6", got)
	}
}

// quarantining answers an Alive with a quarantine, as a store does for an
// entity with a clone collision.
type quarantining struct {
	store.Store
	err error
}

func (s *quarantining) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	return false, s.err
}

// A quarantine is an answer: it is kept, and served as the same error.
func TestMemoKeepsAQuarantine(t *testing.T) {
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := &store.QuarantineError{Entity: tPod, Layer: podLayer}
	for name, e := range map[string]error{"bare": q, "wrapped": fmt.Errorf("alive: %w", q)} {
		t.Run(name, func(t *testing.T) {
			cnt := &counting{Store: &quarantining{Store: st, err: e}}
			m := newMemo(cnt, memoCap)
			s := store.Scope{Layer: podLayer, AsOf: 0}
			for range 3 {
				alive, err := m.Alive(bg, tPod, tm(0), s)
				var got *store.QuarantineError
				if alive || !errors.Is(err, e) || !errors.As(err, &got) || got != q {
					t.Fatalf("got %t, %v; want false and the store's error", alive, err)
				}
			}
			if cnt.reads.Alive != 1 {
				t.Errorf("%d Alive reads reached the store, want 1", cnt.reads.Alive)
			}
		})
	}
}

// The cap: each answer costs its records and one; a cap of 0 keeps nothing,
// not even an existence answer or an empty read; and an answer past the cap is
// given but not kept.
func TestMemoCap(t *testing.T) {
	two := []store.Record{edgeRec(tPod, tNode, tm(1)), edgeRec(tPod2, tNode, tm(2))}
	t.Run("zero keeps nothing", func(t *testing.T) {
		f := newFixture(t, 0)
		f.write(two...)
		s := f.scope()
		ps := store.Scope{Layer: podLayer, AsOf: f.seq}
		for range 2 {
			f.window(tNode, store.Reverse, tm(0), tm(30), s)
			f.window(tNode2, store.Reverse, tm(0), tm(30), s) // no records
			if _, err := f.memo.Alive(bg, tPod, tm(0), ps); err != nil {
				t.Fatal(err)
			}
			if _, err := f.memo.EntityWindow(bg, tPod, tm(0), tm(30), ps); err != nil {
				t.Fatal(err)
			}
		}
		wantReads(t, f, 8)
		if f.memo.used != 0 || len(f.memo.m) != 0 {
			t.Errorf("used %d, %d entries", f.memo.used, len(f.memo.m))
		}
	})
	t.Run("an existence answer or an empty read costs one", func(t *testing.T) {
		f := newFixture(t, 2)
		f.write(entityRec(tPod, tm(0)), entityRec(tPod2, tm(0)))
		ps := store.Scope{Layer: podLayer, AsOf: f.seq}
		for _, p := range []identity.Fingerprint{tPod, tPod2, tPod} {
			if _, err := f.memo.Alive(bg, p, tm(0), ps); err != nil {
				t.Fatal(err)
			}
		}
		f.window(tNode, store.Reverse, tm(0), tm(30), f.scope()) // empty, past the cap
		f.window(tNode, store.Reverse, tm(0), tm(30), f.scope())
		wantReads(t, f, 4)
		if f.memo.used != 2 {
			t.Errorf("used %d, want 2", f.memo.used)
		}
	})
	t.Run("a read fits exactly its records and one", func(t *testing.T) {
		f := newFixture(t, 3)
		f.write(two...)
		s := f.scope()
		f.window(tNode, store.Reverse, tm(0), tm(30), s)
		f.window(tNode, store.Reverse, tm(0), tm(30), s)
		wantReads(t, f, 1)
		g := newFixture(t, 2)
		g.write(two...)
		g.window(tNode, store.Reverse, tm(0), tm(30), g.scope())
		wantSeqs(t, "past the cap", g.window(tNode, store.Reverse, tm(0), tm(30), g.scope()), 1, 2)
		wantReads(t, g, 2)
		if g.memo.used != 0 {
			t.Errorf("used %d past the cap", g.memo.used)
		}
	})
	t.Run("a small answer still fits after a large one is refused", func(t *testing.T) {
		f := newFixture(t, 3)
		f.write(two...)
		s := f.scope()
		f.window(tNode, store.Reverse, tm(0), tm(30), s)  // kept: 3
		f.window(tNode2, store.Reverse, tm(0), tm(30), s) // empty: 1, past the cap
		f.window(tNode2, store.Reverse, tm(0), tm(30), s)
		wantReads(t, f, 3)
	})
}

// The memo writes nothing and does not close the store.
func TestMemoIsReadOnly(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	before := f.st.LastSeq()
	r := edgeRec(tPod2, tNode, tm(2))
	r.Seq = before + 1
	if err := f.memo.Write(bg, []store.Record{r}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Write: %v, want ErrInvalid", err)
	}
	if err := f.memo.Retain(bg, tm(10)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Retain: %v, want ErrInvalid", err)
	}
	if f.st.LastSeq() != before || !f.st.Horizon().IsZero() {
		t.Errorf("the store changed: seq %d, horizon %+v", f.st.LastSeq(), f.st.Horizon())
	}
	if err := f.memo.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// The store is still open: a read through it works.
	if _, err := f.st.Window(bg, tNode, store.Reverse, tm(0), tm(30), f.scope()); err != nil {
		t.Errorf("the store after the memo's Close: %v", err)
	}
}

// One key read by many goroutines reaches the store once.
func TestMemoReadsAKeyOnceUnderConcurrency(t *testing.T) {
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	s := f.scope()
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rs, err := f.memo.Window(bg, tNode, store.Reverse, tm(0), tm(30), s); err != nil || len(rs) != 1 {
				t.Errorf("%v, %d records", err, len(rs))
			}
		}()
	}
	wg.Wait()
	wantReads(t, f, 1)
}

func (s *failing) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if s.fail("WindowSince") {
		return nil, errBoom
	}
	return s.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func (f *fixture) since(fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) []store.Since {
	f.t.Helper()
	out, err := f.memo.WindowSince(bg, fps, d, from, to, s, o)
	if err != nil {
		f.t.Fatal(err)
	}
	if len(out) != len(fps) {
		f.t.Fatalf("%d answers for %d fingerprints", len(out), len(fps))
	}
	return out
}

func sinceSeqs(s store.Since) []uint64 {
	var out []uint64
	for _, sd := range s.Seeds {
		out = append(out, sd.Record.Seq)
	}
	return append(out, seqs(s.Window)...)
}

// WindowSince is kept per fingerprint: a repeat in one call, and a later call,
// reach the store only for what the memo does not hold, each fingerprint once.
// The option is part of the key, and the answers equal the store's.
func TestMemoKeepsWindowSincePerFingerprint(t *testing.T) {
	t.Parallel()
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)), edgeRec(tPod2, tNode, tm(2)), edgeRec(tPod, tNode2, tm(5)))
	sc, from, to := f.scope(), tm(3), tm(10)
	got := f.since([]identity.Fingerprint{tNode, tNode, tNode2}, store.Reverse, from, to, sc, store.SinceOptions{})
	want, err := f.st.WindowSince(bg, []identity.Fingerprint{tNode, tNode, tNode2}, store.Reverse, from, to, sc, store.SinceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if !slices.Equal(sinceSeqs(got[i]), sinceSeqs(want[i])) {
			t.Errorf("answer %d: %v, want %v", i, sinceSeqs(got[i]), sinceSeqs(want[i]))
		}
	}
	if f.cnt.reads.Windows != 2 {
		t.Errorf("%d fingerprints reached the store, want the two distinct ones", f.cnt.reads.Windows)
	}
	f.since([]identity.Fingerprint{tNode2, tNode, tNode2}, store.Reverse, from, to, sc, store.SinceOptions{})
	wantReads(t, f, 2)
	f.since([]identity.Fingerprint{tNode, tNode2}, store.Reverse, from, to, sc, store.SinceOptions{Payloads: true})
	wantReads(t, f, 4)
	// The entities' own existence (direction 0) is kept apart from their edges.
	f.since([]identity.Fingerprint{tNode}, 0, from, to, store.Scope{Layer: entityLayer(catalog.K8sNode), AsOf: f.seq}, store.SinceOptions{})
	if f.cnt.reads.EntityWindows != 1 {
		t.Errorf("%d entity reads, want 1", f.cnt.reads.EntityWindows)
	}
	// A changed answer changes nothing kept.
	a := f.since([]identity.Fingerprint{tNode}, store.Reverse, from, to, sc, store.SinceOptions{})
	a[0].Seeds[0].Record.Seq = 99
	b := f.since([]identity.Fingerprint{tNode}, store.Reverse, from, to, sc, store.SinceOptions{})
	if b[0].Seeds[0].Record.Seq == 99 {
		t.Error("a change to an answer reached the memo")
	}
}

// A WindowSince under Latest, or of a retained layer, is never kept; an error is
// returned and not kept; and no fingerprints still reach the store, which judges
// the horizon.
func TestMemoKeepsWindowSinceOnlyByTheRules(t *testing.T) {
	t.Parallel()
	f := newFixture(t, memoCap)
	f.write(edgeRec(tPod, tNode, tm(1)))
	latest := store.Current(edgeLayer())
	for range 2 {
		f.since([]identity.Fingerprint{tNode}, store.Reverse, tm(0), tm(5), latest, store.SinceOptions{})
	}
	wantReads(t, f, 2)

	fl := &failing{Store: f.st, fails: map[string]bool{"WindowSince": true}}
	m := newMemo(fl, memoCap)
	if _, err := m.WindowSince(bg, []identity.Fingerprint{tNode}, store.Reverse, tm(0), tm(5), f.scope(), store.SinceOptions{}); !errors.Is(err, errBoom) {
		t.Fatalf("the failing read: %v", err)
	}
	if got, err := m.WindowSince(bg, []identity.Fingerprint{tNode}, store.Reverse, tm(0), tm(5), f.scope(), store.SinceOptions{}); err != nil || len(got[0].Window) != 1 {
		t.Fatalf("after the failure: %v, %v", got, err)
	}

	// An answer kept before a retention is not served after it: the store judges
	// every read of a layer with a horizon.
	f.since([]identity.Fingerprint{tNode}, store.Reverse, tm(2), tm(5), f.scope(), store.SinceOptions{})
	f.since([]identity.Fingerprint{tNode}, store.Reverse, tm(2), tm(5), f.scope(), store.SinceOptions{})
	if err := f.st.Retain(bg, tm(2)); err != nil {
		t.Fatal(err)
	}
	before := f.storeReads()
	for range 2 {
		f.since([]identity.Fingerprint{tNode}, store.Reverse, tm(2), tm(5), f.scope(), store.SinceOptions{})
	}
	wantReads(t, f, before+2)
	if _, err := f.memo.WindowSince(bg, nil, store.Reverse, tm(1), tm(5), f.scope(), store.SinceOptions{}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("no fingerprints before the horizon: %v, want ErrBeforeHorizon", err)
	}
}
