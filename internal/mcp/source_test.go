package mcp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The store the query packages see has no Write or Retain, and its Close does
// nothing.
func TestReadOnlyStore(t *testing.T) {
	spy := &spyStore{Store: openMem(t, memstore.Options{})}
	ro := readOnly{spy}
	if err := ro.Write(ctx, []store.Record{{}}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Write: %v", err)
	}
	if err := ro.Retain(ctx, hm(9, 0)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Retain: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if spy.writes+spy.retains+spy.closes != 0 {
		t.Errorf("the wrapper let through %d writes, %d retains, %d closes", spy.writes, spy.retains, spy.closes)
	}
	// Reads go through.
	if ro.LastSeq() != 0 {
		t.Error("LastSeq")
	}
	// A source reads through it: what it holds refuses a write, and the store is
	// not touched.
	s, err := NewSource(Config{Store: spy})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.st.Write(ctx, []store.Record{{}}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("the source's store accepted a write: %v", err)
	}
	if err := s.st.Retain(ctx, hm(9, 0)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("the source's store accepted a retain: %v", err)
	}
	if err := s.st.Close(); err != nil || spy.closes != 0 || spy.writes != 0 || spy.retains != 0 {
		t.Errorf("the source's store closed or wrote: %v, %d %d %d", err, spy.closes, spy.writes, spy.retains)
	}
}

// Across every scenario of the fixture, errors included, the source never writes,
// retains or closes the store it reads.
func TestSourceNeverWrites(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	allAnswers(t, w, s)
	for _, c := range calls(w) {
		_, _ = c.ask(s, "")
	}
	_, _ = s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w1030, To: w10})
	_, _ = s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2000-01-01T00:00:00Z"})
	if spy.reads == 0 {
		t.Fatal("nothing was read")
	}
	if spy.writes != 0 || spy.retains != 0 || spy.closes != 0 {
		t.Errorf("%d writes, %d retains, %d closes", spy.writes, spy.retains, spy.closes)
	}
	if w.st.LastSeq() != 33 {
		t.Errorf("the store moved to %d", w.st.LastSeq())
	}
}

// Range reads are on only for memstore, and only when the caller sets them.
func TestRangeReadsOnlyForMemstore(t *testing.T) {
	mem := openMem(t, memstore.Options{})
	if !RangeReadsFor(mem) {
		t.Error("memstore has no range reads")
	}
	peb, err := pebblestore.Open(t.TempDir(), pebblestore.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peb.Close() })
	if RangeReadsFor(peb) {
		t.Error("pebblestore has range reads")
	}
	if RangeReadsFor(readOnly{mem}) || RangeReadsFor(&spyStore{Store: mem}) {
		t.Error("a wrapped store has range reads")
	}
	// The zero configuration has none.
	s, err := NewSource(Config{Store: mem})
	if err != nil {
		t.Fatal(err)
	}
	if s.RangeReads() {
		t.Error("a source has range reads without being told")
	}
	if _, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: numbered("host", 1).String(), At: w10}); !errors.Is(err, ErrNoRangeReads) {
		t.Errorf("%v", err)
	}
	// A source over pebblestore answers a point tool, and refuses a range tool.
	ps, err := NewSource(Config{Store: peb, RangeReads: RangeReadsFor(peb)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ps.NeighborsAt(ctx, NeighborsAtInput{Entity: numbered("host", 1).String(), At: w10}); err != nil {
		t.Errorf("neighbors_at on pebblestore: %v", err)
	}
	if _, err := ps.WhatChanged(ctx, WhatChangedInput{Scope: []string{numbered("host", 1).String()}, From: w10, To: w1030}); !errors.Is(err, ErrNoRangeReads) {
		t.Errorf("what_changed on pebblestore: %v", err)
	}
	if _, err := NewSource(Config{}); err == nil {
		t.Error("a source without a store")
	}
}

func TestDefaultBounds(t *testing.T) {
	s, err := NewSource(Config{Store: openMem(t, memstore.Options{})})
	if err != nil {
		t.Fatal(err)
	}
	if cap(s.gate) != 2 || s.busyAfter != 5*time.Second || s.callTimeout != 30*time.Second {
		t.Errorf("slots %d, busy after %s, deadline %s", cap(s.gate), s.busyAfter, s.callTimeout)
	}
}

// At most MaxConcurrent calls run at once; the others wait, and are refused as
// busy after BusyAfter.
func TestGate(t *testing.T) {
	w := newWorld(t, rankObjs)
	bs := newBlockStore(w.st)
	s, err := NewSource(Config{Store: bs, Rank: rankObjs, RangeReads: true, MaxConcurrent: 1, BusyAfter: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ask := NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"}
	first := make(chan error, 1)
	go func() { _, err := s.NeighborsAt(ctx, ask); first <- err }()
	<-bs.entered

	start := time.Now()
	_, err = s.NeighborsAt(ctx, ask)
	wantErr(t, "a second call", err, "The server is busy; try again shortly.", ErrBusy)
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Errorf("refused as busy after %s", d)
	}
	close(bs.release)
	if err := <-first; err != nil {
		t.Errorf("the first call: %v", err)
	}
	// The slot is free again.
	if _, err := s.NeighborsAt(ctx, ask); err != nil {
		t.Errorf("after the first call: %v", err)
	}
}

// A call queued at the gate ends with its context.
func TestQueuedCallCancels(t *testing.T) {
	w := newWorld(t, rankObjs)
	bs := newBlockStore(w.st)
	s, err := NewSource(Config{Store: bs, Rank: rankObjs, MaxConcurrent: 1, BusyAfter: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ask := NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"}
	go func() { _, _ = s.NeighborsAt(ctx, ask) }()
	<-bs.entered
	c, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := s.NeighborsAt(c, ask); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	wantErr(t, "queued", <-done, "The call was cancelled.", context.Canceled)
	close(bs.release)
}

// stubborn is a store whose reads ignore their context: they end when released.
type stubborn struct {
	*spyStore
	release chan struct{}
	entered chan struct{}
	once    sync.Once
	// returned is set by the store once a read has returned to its caller. Drain
	// can only return after that, so it is the deterministic evidence that the
	// read was over; a flag set by the test after the call returns is not.
	returned atomic.Bool
}

func (s *stubborn) wait() {
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func (s *stubborn) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.wait()
	defer s.returned.Store(true)
	return s.spyStore.Alive(ctx, fp, t, sc)
}

// Drain withdraws the store: a new call is refused, a queued call ends, and it
// returns only when the call that was reading has returned, so the store is
// closed under no read.
func TestDrain(t *testing.T) {
	w := newWorld(t, rankObjs)
	st := &stubborn{spyStore: &spyStore{Store: w.st}, release: make(chan struct{}), entered: make(chan struct{})}
	s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true, MaxConcurrent: 1, BusyAfter: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ask := ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"` + hostSecret + `"}`), At: w10}

	var running, queued atomic.Bool
	runErr, queueErr := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.ResolveEntity(ctx, ask)
		running.Store(true)
		runErr <- err
	}()
	<-st.entered
	go func() {
		_, err := s.ResolveEntity(ctx, ask)
		queued.Store(true)
		queueErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	drained := make(chan error, 1)
	go func() { drained <- s.Drain(ctx) }()

	// The queued call never took the store, and ends at once.
	wantErr(t, "the queued call", <-queueErr, "The server is shutting down.", ErrShuttingDown)
	// A new call is refused.
	_, err = s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: w10})
	wantErr(t, "a new call", err, "The server is shutting down.", ErrShuttingDown)
	// The running call is still reading: the drain does not return.
	select {
	case err := <-drained:
		t.Fatalf("Drain returned (%v) while a call was reading", err)
	case <-time.After(60 * time.Millisecond):
	}
	if running.Load() {
		t.Fatal("the running call returned before it was released")
	}
	close(st.release)
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// By the time it returns, every call has returned.
	if !st.returned.Load() {
		t.Error("Drain returned before the running call's read did")
	}
	wantErr(t, "the running call", <-runErr, "The server is shutting down.")
	// So closing the store is safe.
	if err := w.st.Close(); err != nil {
		t.Error(err)
	}
	if err := s.Drain(ctx); err != nil {
		t.Errorf("a second Drain: %v", err)
	}
}

// A drain that cannot finish says so, and the caller must not close the store.
func TestDrainTimesOut(t *testing.T) {
	w := newWorld(t, rankObjs)
	st := &stubborn{spyStore: &spyStore{Store: w.st}, release: make(chan struct{}), entered: make(chan struct{})}
	s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"x"}`), At: w10})
	}()
	<-st.entered
	c, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := s.Drain(c); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Drain: %v", err)
	}
	close(st.release)
	if err := s.Drain(ctx); err != nil {
		t.Errorf("Drain after the release: %v", err)
	}
}

// Calls from many goroutines, with the race detector.
func TestConcurrentCalls(t *testing.T) {
	w := newWorld(t, rankObjs)
	s, err := NewSource(Config{Store: w.st, Rank: rankObjs, RangeReads: true, BusyAfter: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	cs := calls(w)
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := cs[i%len(cs)]
			if _, err := c.ask(s, ""); err != nil {
				t.Errorf("%s: %v", c.tool, err)
			}
		}()
	}
	wg.Wait()
	if len(s.gate) != 0 {
		t.Errorf("%d slots still taken", len(s.gate))
	}
}
