package decay_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// edgeWorld is one edge between two services whose endpoints exist from before
// the epoch, and the records written to it in the order given.
type edgeWorld struct {
	t    *testing.T
	st   store.Store
	a, b identity.Fingerprint
	sub  store.Subject
	seq  uint64
}

func newEdgeWorld(t *testing.T) *edgeWorld {
	t.Helper()
	w := &edgeWorld{t: t, st: openMem(t), a: fingerprint(catalog.Service, 1), b: fingerprint(catalog.Service, 2)}
	t.Cleanup(func() { _ = w.st.Close() })
	w.sub = store.EdgeSubject(w.a, w.b, catalog.DependsOn)
	for _, fp := range []identity.Fingerprint{w.a, w.b} {
		w.write(store.Record{Layer: serviceLayer(), Subject: store.EntitySubject(fp), Producer: "ep", EventTime: at(-time.Hour), Kind: lifecycle.Observe})
	}
	return w
}

func (w *edgeWorld) write(r store.Record) {
	w.t.Helper()
	w.seq++
	r.Seq = w.seq
	if err := w.st.Write(ctx, []store.Record{r}); err != nil {
		w.t.Fatal(err)
	}
}

func (w *edgeWorld) observe(p lifecycle.Producer, when time.Duration, ttl time.Duration, through time.Duration) {
	w.t.Helper()
	r := store.Record{Layer: depLayer(), Subject: w.sub, Producer: p, EventTime: at(when), Kind: lifecycle.Observe, TTL: ttl}
	if through != 0 {
		r.Through = at(through)
	}
	w.write(r)
}

func (w *edgeWorld) delete(p lifecycle.Producer, when time.Duration) {
	w.t.Helper()
	w.write(store.Record{Layer: depLayer(), Subject: w.sub, Producer: p, EventTime: at(when), Kind: lifecycle.Delete})
}

// recency reads the edge over [from, instant+1ns) at the latest token and derives
// its recency at the instant.
func (w *edgeWorld) recency(from, instant time.Duration) (decay.Recency, bool, interval.Element) {
	w.t.Helper()
	res, err := interval.NewReader(w.st, interval.Options{Evidence: true}).Edges(ctx, w.a, store.Forward,
		interval.Window{From: at(from), To: at(instant).Add(time.Nanosecond), AsOf: store.Latest})
	if err != nil {
		w.t.Fatal(err)
	}
	if len(res.Elements) == 0 {
		return decay.Recency{}, false, interval.Element{}
	}
	el := res.Elements[0]
	rec, ok := decay.Of(el.Last, at(instant), decay.DefaultPolicy())
	return rec, ok, el
}

func TestWatchModeHasNoRecency(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, 0, 0)
	rec, ok, el := w.recency(0, 100*hour)
	if len(el.Pieces) != 1 || len(el.Last) != 1 || el.Last[0].TTL != 0 {
		t.Fatalf("the edge is live, held by one watch-mode reference: %+v", el)
	}
	if ok {
		t.Errorf("recency %+v for a reference with no TTL", rec)
	}
}

// A producer with no TTL among the live ones leaves the edge with none, not the
// bound of the producer that has a TTL: the one with none may have seen it at
// any time.
func TestAWatchModeProducerAmongTheLiveOnesLeavesNoRecency(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 0)
	w.observe("q", 0, 0, 0)
	rec, ok, el := w.recency(0, 2*hour)
	if len(el.Last) != 2 {
		t.Fatalf("Last: %+v", el.Last)
	}
	if ok {
		t.Errorf("recency %+v with a watch-mode producer live", rec)
	}
	// Once q ends its reference, the edge has the bound of p.
	w.delete("q", 3*hour)
	rec, ok, el = w.recency(4*hour, 5*hour)
	if len(el.Last) != 1 || !ok || rec.Producer != "p" || rec.SilentAtLeast != 2*hour {
		t.Errorf("q is deleted and p is live until 6h: %+v %t %+v", rec, ok, el.Last)
	}
}

func TestADeleteEndsTheEvidence(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 0)
	w.delete("p", hour)
	rec, ok, el := w.recency(0, 2*hour)
	if len(el.Pieces) != 1 || el.Last != nil || ok {
		t.Errorf("the edge ended at 1h: pieces %v, Last %+v, recency %+v %t", el.Pieces, el.Last, rec, ok)
	}
	if rec, ok, el := w.recency(0, 59*minute); !ok || rec.SilentAtLeast != 0 || len(el.Last) != 1 {
		t.Errorf("at 59m: %+v %t", rec, ok)
	}
	// The edge is not there at all in a window that starts after the delete.
	if rec, ok, el := w.recency(2*hour, 3*hour); ok || len(el.Pieces) != 0 || el.Last != nil {
		t.Errorf("after the delete: %+v %t %+v", rec, ok, el)
	}
}

// An edge cut by an endpoint at the last instant has no piece there, and so no
// evidence, though its own producer's reference is live.
func TestACascadeCutPieceHasNoEvidence(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 0)
	w.write(store.Record{Layer: serviceLayer(), Subject: store.EntitySubject(w.b), Producer: "ep", EventTime: at(hour), Kind: lifecycle.Delete})
	if rec, ok, el := w.recency(0, hour-time.Nanosecond); !ok || len(el.Last) != 1 || rec.SilentAtLeast != 0 {
		t.Errorf("b is alive until 1h exclusive: %+v %t %+v", rec, ok, el.Last)
	}
	rec, ok, el := w.recency(0, 2*hour)
	if len(el.Pieces) != 1 || el.Last != nil || ok {
		t.Errorf("b is gone at 1h: pieces %v, Last %+v, recency %+v %t", el.Pieces, el.Last, rec, ok)
	}
}

// Of two producers the fresher one's record gives the recency, whichever name
// sorts first.
func TestTwoProducersTheFresherOnesRecord(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("a", 0, ttl6h, 0)
	w.observe("b", hour, ttl6h, 0)
	rec, ok, _ := w.recency(0, 5*hour)
	if !ok || rec.Producer != "b" || rec.SilentAtLeast != hour || !rec.LastSeen.Equal(at(hour)) {
		t.Errorf("%+v %t", rec, ok)
	}
	w.observe("a", 4*hour, ttl6h, 0)
	rec, ok, _ = w.recency(0, 5*hour)
	if !ok || rec.Producer != "a" || rec.SilentAtLeast != 0 || !rec.LastSeen.Equal(at(4*hour)) {
		t.Errorf("after a's later sighting: %+v %t", rec, ok)
	}
	// At the earlier token, only the first two are there.
	res, err := interval.NewReader(w.st, interval.Options{Evidence: true}).Edges(ctx, w.a, store.Forward,
		interval.Window{From: at(0), To: at(5 * hour).Add(time.Nanosecond), AsOf: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := decay.Of(res.Elements[0].Last, at(5*hour), decay.DefaultPolicy()); !ok || got.Producer != "b" {
		t.Errorf("at token 4: %+v %t", got, ok)
	}
}

// At the instant a run starts its record is the evidence, seen then, with no
// silence; one nanosecond before, the earlier record's.
func TestAtARunStart(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 0)
	w.observe("p", 5*hour, ttl6h, 0)
	rec, ok, el := w.recency(0, 5*hour)
	if !ok || !rec.LastSeen.Equal(at(5*hour)) || rec.SilentAtLeast != 0 || el.Last[0].Seq != 4 {
		t.Errorf("at the new run's start: %+v %t %+v", rec, ok, el.Last)
	}
	rec, ok, el = w.recency(0, 5*hour-time.Nanosecond)
	if !ok || !rec.LastSeen.Equal(at(0)) || rec.SilentAtLeast != 2*hour-time.Nanosecond || el.Last[0].Seq != 3 {
		t.Errorf("a nanosecond before it: %+v %t %+v", rec, ok, el.Last)
	}
}

// A late refresh, one the coalescer passes on at an event time before a run it
// does not remember, is the deciding record from its instant until the next
// record, and its own slack covers the run it fell inside: its bound is a bound
// of that run's silence too.
func TestALateRefreshInsideAnOlderRun(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 2*hour) // a run, 0 through 2h
	w.observe("p", hour, ttl6h, 0)   // a late refresh inside it
	w.observe("p", 8*hour, ttl6h, 0) // the next run
	rec, ok, el := w.recency(0, 6*hour)
	// The record at 1h decides, and the run's Through, 2h, is the latest last
	// instant read: seen at 2h, silent at least 6h - 2h - 3h. Read from 3h on, the
	// run's record is not read and the bound is 6h - 1h - 3h = 2h; the producer was
	// last seen at 2h at the latest, so it falls short of the silence by 2h, inside
	// X = 3h.
	if !ok || el.Last[0].Seq != 4 || !rec.LastSeen.Equal(at(2*hour)) || rec.SilentAtLeast != hour {
		t.Errorf("%+v %t", rec, ok)
	}
	if rec, ok, _ := w.recency(hour, 6*hour); !ok || !rec.LastSeen.Equal(at(2*hour)) || rec.SilentAtLeast != hour {
		t.Errorf("from 1h: %+v %t", rec, ok)
	}
	if rec, ok, _ := w.recency(3*hour, 6*hour); !ok || !rec.LastSeen.Equal(at(hour)) || rec.SilentAtLeast != 2*hour {
		t.Errorf("from 3h, the seed alone: %+v %t", rec, ok)
	}
}

// Two attested spans of one producer that overlap, the later one ending first,
// with both records in the window: the later record replaces the earlier from its
// event time, as it does for existence, but the evidence names the latest last
// instant of every record read, so the span is not hidden and at 7h the edge was
// not silent at all.
func TestOverlappingAttestedSpansInsideTheWindow(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 10*hour)   // attests the edge from 0 through 10h
	w.observe("p", hour, ttl6h, 2*hour) // attests it from 1h through 2h, replacing the first from 1h
	rec, ok, el := w.recency(0, 7*hour)
	if !ok || el.Last[0].Seq != 4 || !el.Last[0].Latest.Equal(at(10*hour)) {
		t.Fatalf("%+v %t %+v", rec, ok, el.Last)
	}
	if !rec.LastSeen.Equal(at(7*hour)) || rec.SilentAtLeast != 0 || rec.Confidence != 1 {
		t.Errorf("%+v", rec)
	}
}

// What the bound does not claim. The same two spans, with the long one's record
// before the window's start: the call reads the producer's seed, which is the
// later record, and not the earlier one, so the bound follows the later record
// and exceeds the silence the long span implies. Only a record that carries its
// own Through can do this (the coalescer never writes one whose span reaches past
// a later record by more than X, and no writer of dependencies writes any other),
// and the limit is pinned so that a change to it is deliberate.
func TestAnAttestedSpanFromBeforeTheWindowIsOutsideTheBound(t *testing.T) {
	w := newEdgeWorld(t)
	w.observe("p", 0, ttl6h, 10*hour)   // attests the edge from 0 through 10h
	w.observe("p", hour, ttl6h, 2*hour) // attests it from 1h through 2h, replacing the first from 1h
	rec, ok, el := w.recency(3*hour, 7*hour)
	if !ok || el.Last[0].Seq != 4 || !el.Last[0].Latest.Equal(at(2*hour)) {
		t.Fatalf("%+v %t %+v", rec, ok, el.Last)
	}
	if !rec.LastSeen.Equal(at(2*hour)) || rec.SilentAtLeast != 2*hour {
		t.Fatalf("%+v", rec)
	}
	// The long span says the edge was seen through 10h, so at 7h it was not silent.
	if impliedSilence := time.Duration(0); rec.SilentAtLeast <= impliedSilence {
		t.Errorf("the bound %s is within the silence %s the long span implies: the limit no longer holds, so the docs and the property's generator may be relaxed", rec.SilentAtLeast, impliedSilence)
	}
}
