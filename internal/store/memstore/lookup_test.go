package memstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func lookupEdge(tp topology, seq uint64, producer lifecycle.Producer, d time.Duration, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(tp.pod, tp.node, catalog.ScheduledOn), Producer: producer,
		EventTime: at(d), Seq: seq, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte{'p', byte('0' + seq)}
	}
	return r
}

func lookupRef(tp topology, producer lifecycle.Producer) store.LookupRef {
	return store.LookupRef{Layer: catalog.L2, Subject: store.EdgeSubject(tp.pod, tp.node, catalog.ScheduledOn), Producer: producer}
}

func lookup(t *testing.T, s *memstore.Store, ref store.LookupRef) store.LastAssertion {
	t.Helper()
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	got, err := lk.Last(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome == store.LookupInvalid {
		t.Fatal("Last returned the zero outcome with no error")
	}
	return got
}

func TestLookupFindsTheNewestRecordOfAReference(t *testing.T) {
	t.Parallel()
	tp := newTopology(t)
	s := open(t)
	through := lookupEdge(tp, 3, "objects", 30*time.Second, lifecycle.Observe, time.Minute)
	through.Through, through.EventTimeBasis = at(50*time.Second), store.BasisObjectField
	write(t, s,
		lookupEdge(tp, 1, "objects", 10*time.Second, lifecycle.Observe, 0),
		lookupEdge(tp, 2, "cluster", 20*time.Second, lifecycle.Observe, 0),
		through,
		lookupEdge(tp, 4, "cluster", 35*time.Second, lifecycle.Delete, 0),
	)
	got := lookup(t, s, lookupRef(tp, "objects"))
	if got.Outcome != store.LookupFound || got.Seq != 3 || !got.EventTime.Equal(at(30*time.Second)) || !got.Through.Equal(at(50*time.Second)) ||
		!got.Last.Equal(at(50*time.Second)) || !got.LastFromThrough || got.TTL != time.Minute || got.Basis != store.BasisObjectField ||
		got.Kind != lifecycle.Observe || !bytes.Equal(got.Payload, []byte("p3")) {
		t.Errorf("objects: %+v", got)
	}
	got = lookup(t, s, lookupRef(tp, "cluster"))
	if got.Outcome != store.LookupFound || got.Seq != 4 || got.Kind != lifecycle.Delete || got.Payload != nil || !got.Last.Equal(at(35*time.Second)) || got.LastFromThrough {
		t.Errorf("cluster: %+v", got)
	}
	// The payload is a copy.
	got = lookup(t, s, lookupRef(tp, "objects"))
	got.Payload[0] = 'X'
	if again := lookup(t, s, lookupRef(tp, "objects")); string(again.Payload) != "p3" {
		t.Errorf("a changed answer changed the store: %q", again.Payload)
	}
}

func TestLookupPicksTheLatestLastAtOneInstant(t *testing.T) {
	t.Parallel()
	tp := newTopology(t)
	s := open(t)
	long := lookupEdge(tp, 2, "p", 10*time.Second, lifecycle.Observe, 0)
	long.Through = at(30 * time.Second)
	write(t, s,
		lookupEdge(tp, 1, "p", 10*time.Second, lifecycle.Observe, 0),
		long,
		lookupEdge(tp, 3, "p", 10*time.Second, lifecycle.Observe, 0),
		lookupEdge(tp, 4, "q", 10*time.Second, lifecycle.Observe, 0),
	)
	if got := lookup(t, s, lookupRef(tp, "p")); got.Seq != 2 {
		t.Errorf("seq %d, want 2: the latest Last at the instant", got.Seq)
	}
	// With equal Last the highest Seq wins.
	s2 := open(t)
	write(t, s2, lookupEdge(tp, 1, "p", 10*time.Second, lifecycle.Observe, 0), lookupEdge(tp, 2, "p", 10*time.Second, lifecycle.Observe, 0))
	if got := lookup(t, s2, lookupRef(tp, "p")); got.Seq != 2 {
		t.Errorf("seq %d, want 2: the highest Seq", got.Seq)
	}
	// An older instant never competes, whatever its Through.
	s3 := open(t)
	old := lookupEdge(tp, 1, "p", 5*time.Second, lifecycle.Observe, 0)
	old.Through = at(time.Hour)
	write(t, s3, old, lookupEdge(tp, 2, "p", 10*time.Second, lifecycle.Observe, 0))
	if got := lookup(t, s3, lookupRef(tp, "p")); got.Seq != 2 {
		t.Errorf("seq %d, want 2: the newest instant", got.Seq)
	}
}

func TestLookupOfWhatIsNotStoredIsNone(t *testing.T) {
	t.Parallel()
	tp := newTopology(t)
	s := open(t)
	write(t, s, lookupEdge(tp, 1, "p", 10*time.Second, lifecycle.Observe, 0))
	other := lookupRef(tp, "q")
	wrongLayer := lookupRef(tp, "p")
	wrongLayer.Layer = catalog.L1
	wrongPeer := lookupRef(tp, "p")
	wrongPeer.Subject.B = tp.node2
	wrongRelation := lookupRef(tp, "p")
	wrongRelation.Subject.Relation = catalog.RunsOn
	entity := store.LookupRef{Layer: catalog.L2, Subject: store.EntitySubject(tp.pod), Producer: "p"}
	for name, ref := range map[string]store.LookupRef{
		"another producer": other, "another layer": wrongLayer, "another target": wrongPeer, "another relation": wrongRelation,
		"the entity": entity,
	} {
		if got := lookup(t, s, ref); got.Outcome != store.LookupNone || got.Seq != 0 || got.Payload != nil {
			t.Errorf("%s: %+v, want none", name, got)
		}
	}
}

// Where a retention would have folded a reference into a baseline, the answer is
// the baseline's: baseline only, with no payload, for an Observe alive at the
// horizon, and none for a Delete or a lapsed one. A record at or after the horizon
// is found.
func TestLookupAnswersBelowTheHorizonAsABaselineWould(t *testing.T) {
	t.Parallel()
	tp := newTopology(t)
	s := open(t)
	run := lookupEdge(tp, 2, "run", 10*time.Second, lifecycle.Observe, time.Minute)
	run.Through, run.EventTimeBasis = at(30*time.Second), store.BasisObserved
	write(t, s,
		lookupEdge(tp, 1, "alive", 10*time.Second, lifecycle.Observe, 0),
		run,
		lookupEdge(tp, 3, "lapsed", 10*time.Second, lifecycle.Observe, 5*time.Second),
		lookupEdge(tp, 4, "gone", 10*time.Second, lifecycle.Observe, 0),
		lookupEdge(tp, 5, "gone", 20*time.Second, lifecycle.Delete, 0),
		lookupEdge(tp, 6, "edge", 50*time.Second, lifecycle.Observe, 0), // at the horizon: kept
		lookupEdge(tp, 7, "above", 60*time.Second, lifecycle.Observe, 0),
		lookupEdge(tp, 8, "twice", 10*time.Second, lifecycle.Observe, 0),
		lookupEdge(tp, 9, "twice", 10*time.Second, lifecycle.Delete, 0),
	)
	if err := s.Retain(context.Background(), at(50*time.Second)); err != nil {
		t.Fatal(err)
	}
	got := lookup(t, s, lookupRef(tp, "alive"))
	if got.Outcome != store.LookupBaselineOnly || got.Payload != nil || got.Seq != 1 || got.Kind != lifecycle.Observe || !got.Last.Equal(at(10*time.Second)) {
		t.Errorf("alive: %+v, want baseline only", got)
	}
	got = lookup(t, s, lookupRef(tp, "run"))
	if got.Outcome != store.LookupBaselineOnly || got.Payload != nil || !got.Last.Equal(at(30*time.Second)) || !got.LastFromThrough || got.Basis != store.BasisObserved || got.TTL != time.Minute {
		t.Errorf("run: %+v, want baseline only with its Through", got)
	}
	for _, p := range []lifecycle.Producer{"lapsed", "gone"} {
		if got := lookup(t, s, lookupRef(tp, p)); got.Outcome != store.LookupNone {
			t.Errorf("%s: %+v, want none", p, got)
		}
	}
	// Of two records at one instant a baseline keeps the one a replay meets first,
	// the higher Seq: here a Delete, so none.
	if got := lookup(t, s, lookupRef(tp, "twice")); got.Outcome != store.LookupNone {
		t.Errorf("twice: %+v, want none", got)
	}
	for _, p := range []lifecycle.Producer{"edge", "above"} {
		if got := lookup(t, s, lookupRef(tp, p)); got.Outcome != store.LookupFound || got.Payload == nil {
			t.Errorf("%s: %+v, want found with its payload", p, got)
		}
	}
}

func TestALookupIsRefusedAfterCloseAndForWhatItCannotAnswer(t *testing.T) {
	t.Parallel()
	tp := newTopology(t)
	s := open(t)
	write(t, s, lookupEdge(tp, 1, "p", 10*time.Second, lifecycle.Observe, 0))
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lk.Last(context.Background(), lookupRef(tp, "")); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a reference with no producer: %v, want ErrInvalid", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lk.Last(ctx, lookupRef(tp, "p")); !errors.Is(err, context.Canceled) {
		t.Errorf("a done context: %v, want its error", err)
	}
	if err := lk.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lk.Close(); err != nil {
		t.Errorf("a second Close = %v", err)
	}
	if got, err := lk.Last(context.Background(), lookupRef(tp, "p")); !errors.Is(err, store.ErrClosed) || got.Outcome != store.LookupInvalid {
		t.Errorf("Last after Close = %+v, %v; want ErrClosed", got, err)
	}
	live, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := live.Last(context.Background(), lookupRef(tp, "p")); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Last on a closed store = %v, want ErrClosed", err)
	}
	if _, err := s.NewLookup(); !errors.Is(err, store.ErrClosed) {
		t.Errorf("NewLookup on a closed store = %v, want ErrClosed", err)
	}
}
