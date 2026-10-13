package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// lookuper is what the two layouts' lookups share.
type lookuper interface {
	Last(context.Context, store.LookupRef) (store.LastAssertion, error)
	Close() error
}

var (
	_ lookuper = (*Lookup)(nil)
	_ lookuper = (*memstore.Lookup)(nil)
)

// edgeRef is the reference of a producer to the edge from the pod to the node.
func edgeRef(producer lifecycle.Producer) store.LookupRef {
	return store.LookupRef{Layer: catalog.L2, Subject: store.EdgeSubject(podFP, nodeFP, catalog.ScheduledOn), Producer: producer}
}

// entityRef is the reference of a producer to the pod's existence.
func entityRef(producer lifecycle.Producer) store.LookupRef {
	return store.LookupRef{Layer: catalog.L2, Subject: store.EntitySubject(podFP), Producer: producer}
}

func podEntityRecord(seq uint64, producer lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{Layer: catalog.L2, Subject: store.EntitySubject(podFP), Producer: producer, EventTime: at, Seq: seq, Kind: kind, TTL: ttl}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "entity-%d", seq)
	}
	return r
}

func withThrough(r store.Record, through time.Time) store.Record {
	r.Through = through
	return r
}

func withBasis(r store.Record, b store.EventTimeBasis) store.Record {
	r.EventTimeBasis = b
	return r
}

// wantFound is the answer for a lookup whose newest record is r.
func wantFound(r store.Record) store.LastAssertion {
	a := store.LastAssertion{
		Outcome: store.LookupFound, Kind: r.Kind, EventTime: r.EventTime, Through: r.Through, TTL: r.TTL,
		Basis: r.EventTimeBasis, Payload: r.Payload, Seq: r.Seq,
	}
	a.Last, a.LastFromThrough = store.LastOf(r.EventTime, r.Through)
	return a
}

// wantBaseline is the answer for a reference whose newest record r was folded
// into a baseline: its times and no payload.
func wantBaseline(r store.Record) store.LastAssertion {
	a := wantFound(r)
	a.Outcome, a.Payload = store.LookupBaselineOnly, nil
	return a
}

func wantNone() store.LastAssertion { return store.LastAssertion{Outcome: store.LookupNone} }

// differ says how two answers differ, or "" if they do not.
func differ(got, want store.LastAssertion) string {
	switch {
	case got.Outcome != want.Outcome:
		return fmt.Sprintf("outcome %s, want %s", got.Outcome, want.Outcome)
	case got.Kind != want.Kind:
		return fmt.Sprintf("kind %d, want %d", got.Kind, want.Kind)
	case !got.EventTime.Equal(want.EventTime):
		return fmt.Sprintf("event time %s, want %s", got.EventTime, want.EventTime)
	case !got.Through.Equal(want.Through):
		return fmt.Sprintf("through %s, want %s", got.Through, want.Through)
	case !got.Last.Equal(want.Last) || got.LastFromThrough != want.LastFromThrough:
		return fmt.Sprintf("last %s (from through %v), want %s (%v)", got.Last, got.LastFromThrough, want.Last, want.LastFromThrough)
	case got.TTL != want.TTL:
		return fmt.Sprintf("ttl %s, want %s", got.TTL, want.TTL)
	case got.Basis != want.Basis:
		return fmt.Sprintf("basis %s, want %s", got.Basis, want.Basis)
	case !bytes.Equal(got.Payload, want.Payload) || (got.Payload == nil) != (want.Payload == nil):
		return fmt.Sprintf("payload %q, want %q", got.Payload, want.Payload)
	case got.Seq != want.Seq:
		return fmt.Sprintf("seq %d, want %d", got.Seq, want.Seq)
	}
	return ""
}

// ask is one lookup that must succeed and must not come back as the zero outcome.
func ask(t *testing.T, lk lookuper, ref store.LookupRef) store.LastAssertion {
	t.Helper()
	got, err := lk.Last(bg, ref)
	if err != nil {
		t.Fatalf("Last(%v) failed: %v", ref, err)
	}
	if got.Outcome == store.LookupInvalid {
		t.Fatalf("Last(%v) returned the zero outcome with no error", ref)
	}
	return got
}

// duo is a lookup on each of a pair's stores, opened over what the pair holds
// now.
type duo struct {
	t  *testing.T
	ps *Lookup
	ms *memstore.Lookup
}

// lookups opens a lookup on each store of the pair, to be closed before the
// stores are.
func (p *pair) lookups() *duo {
	p.t.Helper()
	ps, err := p.s.NewLookup()
	if err != nil {
		p.t.Fatal(err)
	}
	ms, err := p.ref.NewLookup()
	if err != nil {
		p.t.Fatal(err)
	}
	p.t.Cleanup(func() { _ = ps.Close(); _ = ms.Close() })
	return &duo{t: p.t, ps: ps, ms: ms}
}

// is checks that both stores give the answer want for ref.
func (d *duo) is(ref store.LookupRef, want store.LastAssertion) {
	d.t.Helper()
	for name, lk := range map[string]lookuper{"pebblestore": d.ps, "memstore": d.ms} {
		if why := differ(ask(d.t, lk, ref), want); why != "" {
			d.t.Errorf("%s: Last(%s %v) = %s", name, ref.Producer, ref.Subject, why)
		}
	}
}

// walked is the keys the lookups of a recorder walked, one per call of Last.
func walked(rec *memRecorder) []int64 { return rec.samples["lookup.keys_walked"] }

// lastWalked is the keys the latest call walked.
func lastWalked(t *testing.T, rec *memRecorder) int64 {
	t.Helper()
	w := walked(rec)
	if len(w) == 0 {
		t.Fatal("no call of Last was sampled")
	}
	return w[len(w)-1]
}

// S1. Each producer's reference to an edge, on one forward prefix, comes back as
// its own newest record, with Last the later of the event time and the Through.
func TestLookupFindsEachProducersNewestRecord(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: off()})
	a1 := edgeRecord(1, "objects", sec(10), lifecycle.Observe, 0)
	b2 := withBasis(edgeRecord(2, "cluster", sec(20), lifecycle.Observe, 0), store.BasisObserved)
	a3 := withBasis(withThrough(edgeRecord(3, "objects", sec(30), lifecycle.Observe, time.Minute), sec(50)), store.BasisObjectField)
	b4 := withBasis(edgeRecord(4, "cluster", sec(35), lifecycle.Observe, 0), store.BasisObserved)
	e5 := podEntityRecord(5, "objects", sec(10), lifecycle.Observe, 0)
	e6 := withBasis(withThrough(podEntityRecord(6, "objects", sec(40), lifecycle.Observe, 0), sec(60)), store.BasisObjectField)
	p.write(a1, b2, a3, b4, e5, e6)
	d := p.lookups()
	d.is(edgeRef("objects"), wantFound(a3))
	d.is(edgeRef("cluster"), wantFound(b4))
	d.is(entityRef("objects"), wantFound(e6))

	// Not only the helper's arithmetic: the values, by hand.
	got := ask(t, d.ps, edgeRef("objects"))
	if !got.Last.Equal(sec(50)) || !got.LastFromThrough || !got.EventTime.Equal(sec(30)) || got.Seq != 3 || string(got.Payload) != "payload-3" {
		t.Errorf("objects: %+v, want the record at 30s with Last 50s from its Through", got)
	}
	got = ask(t, d.ps, edgeRef("cluster"))
	if !got.Last.Equal(sec(35)) || got.LastFromThrough || !got.Through.IsZero() || got.Basis != store.BasisObserved {
		t.Errorf("cluster: %+v, want the record at 35s with Last 35s", got)
	}
}

// S2. Of the records of a reference at its newest instant, the one with the
// latest Last wins, then the highest Seq; an older instant never competes, whatever
// its Through.
func TestLookupTakesTheLatestLastAmongRecordsAtOneInstant(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		recs []store.Record
		want int // index of the record that must win
	}{
		"the latest Last beats a higher Seq": {
			recs: []store.Record{
				edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
				withThrough(edgeRecord(2, "p", sec(10), lifecycle.Observe, 0), sec(30)),
				edgeRecord(3, "p", sec(10), lifecycle.Observe, 0),
			},
			want: 1,
		},
		"equal Last: the highest Seq": {
			recs: []store.Record{
				edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
				edgeRecord(2, "p", sec(10), lifecycle.Observe, 0),
				edgeRecord(3, "p", sec(10), lifecycle.Observe, 0),
			},
			want: 2,
		},
		"two with the same Last: the higher Seq": {
			recs: []store.Record{
				withThrough(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), sec(30)),
				edgeRecord(2, "p", sec(10), lifecycle.Observe, 0),
				withThrough(edgeRecord(3, "p", sec(10), lifecycle.Observe, 0), sec(30)),
			},
			want: 2,
		},
		"another producer's higher Seq does not compete": {
			recs: []store.Record{
				edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
				withThrough(edgeRecord(2, "q", sec(10), lifecycle.Observe, 0), sec(90)),
				edgeRecord(3, "q", sec(10), lifecycle.Observe, 0),
			},
			want: 0,
		},
		"an older instant never competes, whatever its Through": {
			recs: []store.Record{
				withThrough(edgeRecord(1, "p", sec(5), lifecycle.Observe, 0), sec(1000)),
				edgeRecord(2, "p", sec(10), lifecycle.Observe, 0),
			},
			want: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newPair(t, Options{Checkpoints: off()})
			p.write(tc.recs...)
			p.lookups().is(edgeRef("p"), wantFound(tc.recs[tc.want]))
		})
	}
}

// S3. A checkpoint above the reference's newest record is stepped over: the
// answer is the record, with its payload, not the checkpoint's entry for it.
func TestLookupStepsOverACheckpoint(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	p := newPair(t, Options{Checkpoints: off(), Recorder: rec})
	k1 := withBasis(edgeRecord(1, "k8sobjects", sec(10), lifecycle.Observe, 0), store.BasisObjectField)
	p.write(k1, edgeRecord(2, "k8scluster", sec(20), lifecycle.Observe, 0), edgeRecord(3, "k8scluster", sec(25), lifecycle.Observe, 0))
	if err := p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(30)); err != nil {
		t.Fatal(err)
	}
	if got := len(slices.DeleteFunc(forward(dump(t, p.s)), func(s stored) bool { return s.kind != kindCheckpoint })); got != 1 {
		t.Fatalf("%d checkpoints on the pod's forward prefix, want the 1 the test placed", got)
	}
	d := p.lookups()
	d.is(edgeRef("k8sobjects"), wantFound(k1))
	// The checkpoint, k8scluster's two records and k8sobjects's: four keys.
	if got := lastWalked(t, rec); got != 4 {
		t.Errorf("keys walked = %d, want 4", got)
	}
}

// S4. A reference whose newest record a retention folded into the baseline comes
// back as the baseline's entry: its times, kind and basis, and no payload. A
// reference the baseline does not hold, because it was deleted or lapsed before the
// horizon, is none.
func TestLookupOfABaselineEntryHasNoPayload(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	p := newPair(t, Options{Checkpoints: off(), Recorder: rec})
	alive := withBasis(edgeRecord(1, "alive", sec(10), lifecycle.Observe, 0), store.BasisObjectField)
	run := withBasis(withThrough(edgeRecord(2, "run", sec(10), lifecycle.Observe, time.Minute), sec(30)), store.BasisObserved)
	lapsed := edgeRecord(3, "lapsed", sec(10), lifecycle.Observe, 5*time.Second)
	gone1 := edgeRecord(4, "gone", sec(10), lifecycle.Observe, 0)
	gone2 := edgeRecord(5, "gone", sec(20), lifecycle.Delete, 0)
	above := edgeRecord(6, "above", sec(60), lifecycle.Observe, 0)
	ent := podEntityRecord(7, "alive", sec(12), lifecycle.Observe, 0)
	later := edgeRecord(8, "alive", sec(100), lifecycle.Observe, 0)
	p.write(alive, run, lapsed, gone1, gone2, above, ent)
	p.retain(sec(50))
	d := p.lookups()
	d.is(edgeRef("alive"), wantBaseline(alive))
	d.is(edgeRef("run"), wantBaseline(run))
	d.is(edgeRef("lapsed"), wantNone())
	d.is(edgeRef("gone"), wantNone())
	d.is(edgeRef("above"), wantFound(above))
	d.is(entityRef("alive"), wantBaseline(ent))

	got := ask(t, d.ps, edgeRef("run"))
	if got.Payload != nil || !got.Last.Equal(sec(30)) || !got.LastFromThrough || got.Basis != store.BasisObserved || got.Seq != 2 || got.Kind != lifecycle.Observe {
		t.Errorf("run: %+v, want the baseline's entry with Last 30s from its Through, no payload", got)
	}

	// A record newer than the horizon is found over the baseline.
	p.write(later)
	d = p.lookups()
	d.is(edgeRef("alive"), wantFound(later))
	if got := lastWalked(t, rec); got != 1 {
		t.Errorf("keys walked = %d, want 1: the newest record", got)
	}
}

// S5. A Delete as the newest record is an answer like any other.
func TestLookupFindsADelete(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: off()})
	obs := edgeRecord(1, "p", sec(10), lifecycle.Observe, 0)
	del := withBasis(edgeRecord(2, "p", sec(20), lifecycle.Delete, 0), store.BasisProducerEvent)
	other := edgeRecord(3, "q", sec(30), lifecycle.Observe, 0)
	eobs := podEntityRecord(4, "p", sec(10), lifecycle.Observe, 0)
	edel := podEntityRecord(5, "p", sec(20), lifecycle.Delete, 0)
	p.write(obs, del, other, eobs, edel)
	d := p.lookups()
	d.is(edgeRef("p"), wantFound(del))
	d.is(entityRef("p"), wantFound(edel))
	got := ask(t, d.ps, edgeRef("p"))
	if got.Kind != lifecycle.Delete || !got.EventTime.Equal(sec(20)) || got.Payload != nil {
		t.Errorf("%+v, want the Delete at 20s", got)
	}
}

// fillForward writes n records of producer q to the edge from the pod to the node,
// at one-second steps from sec(start), from seq first on, and returns the next Seq.
func fillForward(t *testing.T, s store.Store, n, start int, first uint64) uint64 {
	t.Helper()
	seq := first
	for done := 0; done < n; {
		var batch []store.Record
		for ; done < n && len(batch) < 200; done++ {
			batch = append(batch, edgeRecord(seq, "q", sec(start+done), lifecycle.Observe, 0))
			seq++
		}
		if err := s.Write(bg, batch); err != nil {
			t.Fatal(err)
		}
	}
	return seq
}

// S6. The walk is bounded at 1,024 keys: the reference's own record may be the
// 1,024th, and not the 1,025th.
func TestLookupStopsAtTheBound(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		newer     int
		want      store.LookupOutcome
		exhausted int64
	}{
		"the reference's record is the last key walked": {newer: LookupWalkBound - 1, want: store.LookupFound},
		"the reference's record is one key too far":     {newer: LookupWalkBound, want: store.LookupExhausted, exhausted: 1},
		"many more keys": {newer: LookupWalkBound + 76, want: store.LookupExhausted, exhausted: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newMemRecorder()
			s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
			mine := edgeRecord(1, "p", sec(10), lifecycle.Observe, 0)
			if err := s.Write(bg, []store.Record{mine}); err != nil {
				t.Fatal(err)
			}
			fillForward(t, s, tc.newer, 100, 2)
			lk, err := s.NewLookup()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lk.Close() }()
			got := ask(t, lk, edgeRef("p"))
			if got.Outcome != tc.want {
				t.Fatalf("outcome %s, want %s", got.Outcome, tc.want)
			}
			if tc.want == store.LookupFound {
				if why := differ(got, wantFound(mine)); why != "" {
					t.Error(why)
				}
			} else if why := differ(got, store.LastAssertion{Outcome: store.LookupExhausted}); why != "" {
				t.Errorf("an exhausted answer carries nothing else: %s", why)
			}
			if w := lastWalked(t, rec); w != LookupWalkBound {
				t.Errorf("keys walked = %d, want %d", w, LookupWalkBound)
			}
			if c := rec.Counter("lookup.exhausted"); c != tc.exhausted {
				t.Errorf("lookup.exhausted = %d, want %d", c, tc.exhausted)
			}
		})
	}
}

// A prefix of exactly the bound's length is walked whole, and is none, not
// exhausted.
func TestLookupOfAPrefixAsLongAsTheBoundIsNone(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	fillForward(t, s, LookupWalkBound, 100, 1)
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	if got := ask(t, lk, edgeRef("p")); got.Outcome != store.LookupNone {
		t.Errorf("outcome %s, want none", got.Outcome)
	}
	if w := lastWalked(t, rec); w != LookupWalkBound {
		t.Errorf("keys walked = %d, want %d", w, LookupWalkBound)
	}
	if c := rec.Counter("lookup.exhausted"); c != 0 {
		t.Errorf("lookup.exhausted = %d, want 0", c)
	}
}

// The records of the reference's newest instant are all read even if that takes
// the walk to the bound: an answer whose instant is not seen whole is exhausted,
// not a guess.
func TestLookupDoesNotGuessWhenTheBoundCutsTheNewestInstant(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	var batch []store.Record
	for i := range LookupWalkBound + 1 {
		batch = append(batch, edgeRecord(uint64(i+1), "p", sec(10), lifecycle.Observe, 0))
	}
	if err := s.Write(bg, batch); err != nil {
		t.Fatal(err)
	}
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	if got := ask(t, lk, edgeRef("p")); got.Outcome != store.LookupExhausted {
		t.Errorf("outcome %s, want exhausted", got.Outcome)
	}
	if w := lastWalked(t, rec); w != LookupWalkBound {
		t.Errorf("keys walked = %d, want %d", w, LookupWalkBound)
	}
}

// podN is the nth pod of a crowd, none of them the test's pod.
func podN(i int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = 0x80+byte(i>>8), byte(i)
	fp, err := identity.FingerprintFromHash(catalog.K8sPod, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// S7. The walk reads the pod's own side. The node's reverse prefix, which a crowd
// of pods has filled with more than the bound's worth of records newer than the
// pod's, is never read.
func TestLookupDoesNotReadTheReverseSide(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	mine := edgeRecord(1, "objects", sec(10), lifecycle.Observe, 0)
	if err := s.Write(bg, []store.Record{mine, edgeRecord(2, "x", sec(20), lifecycle.Observe, 0), edgeRecord(3, "y", sec(21), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	const crowd = LookupWalkBound + 76
	seq := uint64(4)
	for done := 0; done < crowd; {
		var batch []store.Record
		for ; done < crowd && len(batch) < 200; done++ {
			r := edgeRecord(seq, "objects", sec(100+done), lifecycle.Observe, 0)
			r.Subject = store.EdgeSubject(podN(done), nodeFP, catalog.ScheduledOn)
			batch = append(batch, r)
			seq++
		}
		if err := s.Write(bg, batch); err != nil {
			t.Fatal(err)
		}
	}
	reverse := 0
	for _, k := range dump(t, s) {
		if k.dir == byte(store.Reverse) {
			reverse++
		}
	}
	if reverse <= LookupWalkBound {
		t.Fatalf("the node's reverse prefix holds %d keys, want more than %d for the test to mean anything", reverse, LookupWalkBound)
	}
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	got := ask(t, lk, edgeRef("objects"))
	if why := differ(got, wantFound(mine)); why != "" {
		t.Error(why)
	}
	if w := lastWalked(t, rec); w != 3 {
		t.Errorf("keys walked = %d, want 3: the pod's forward prefix has three keys", w)
	}
}

// S8. A lookup is one iterator, however many references it is asked about.
func TestALookupIsOneIterator(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), podEntityRecord(2, "p", sec(10), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	for range 3 {
		lk, err := s.NewLookup()
		if err != nil {
			t.Fatal(err)
		}
		for i := range 25 {
			ref := edgeRef("p")
			if i%2 == 1 {
				ref = entityRef("p")
			}
			if got := ask(t, lk, ref); got.Outcome != store.LookupFound {
				t.Fatalf("outcome %s, want found", got.Outcome)
			}
			calls++
		}
		if err := lk.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if c := rec.Counter("lookup.iterators"); c != 3 {
		t.Errorf("lookup.iterators = %d, want 3: one per lookup, not %d: one per call", c, calls)
	}
	if got := len(walked(rec)); got != calls {
		t.Errorf("%d samples of lookup.keys_walked, want %d: one per call", got, calls)
	}

	// An independent check that a Lookup is one iterator and so one snapshot: a
	// record written after it was opened is not seen by any of its calls, and is by a
	// Lookup opened afterwards.
	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	before := ask(t, lk, edgeRef("p"))
	newer := edgeRecord(100, "p", sec(20), lifecycle.Observe, 0)
	if err := s.Write(bg, []store.Record{newer}); err != nil {
		t.Fatal(err)
	}
	if after := ask(t, lk, edgeRef("p")); differ(after, before) != "" || after.Seq != 1 {
		t.Errorf("a record written after the Lookup was opened was seen: %+v, was %+v", after, before)
	}
	fresh, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close() }()
	if why := differ(ask(t, fresh, edgeRef("p")), wantFound(newer)); why != "" {
		t.Errorf("a new Lookup does not see the record written: %s", why)
	}
}

// S9. A reference with no record is none, after the walk of its side.
func TestLookupOfAnUnknownReferenceIsNone(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	p := newPair(t, Options{Checkpoints: off(), Recorder: rec})
	p.write(
		edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
		edgeRecord(2, "q", sec(20), lifecycle.Observe, 0),
		edgeRecord(3, "p", sec(30), lifecycle.Observe, 0),
		podEntityRecord(4, "p", sec(10), lifecycle.Observe, 0),
	)
	d := p.lookups()
	elsewhere := func(f func(*store.LookupRef)) store.LookupRef {
		r := edgeRef("p")
		f(&r)
		return r
	}
	for name, tc := range map[string]struct {
		ref  store.LookupRef
		keys int64
	}{
		"another producer":              {edgeRef("nobody"), 3},
		"a producer that is a prefix":   {edgeRef("p2"), 3},
		"another target":                {elsewhere(func(r *store.LookupRef) { r.Subject.B = podN(1) }), 3},
		"another relation":              {elsewhere(func(r *store.LookupRef) { r.Subject.Relation = catalog.RunsOn }), 3},
		"another layer":                 {elsewhere(func(r *store.LookupRef) { r.Layer = catalog.L1 }), 0},
		"an entity with no records":     {store.LookupRef{Layer: catalog.L2, Subject: store.EntitySubject(podN(2)), Producer: "p"}, 0},
		"an entity of another producer": {entityRef("q"), 1},
	} {
		d.is(tc.ref, wantNone())
		if got := lastWalked(t, rec); got != tc.keys {
			t.Errorf("%s: keys walked = %d, want %d", name, got, tc.keys)
		}
	}
}

// S10. The two layouts answer alike over a history of many producers, deletes,
// runs and lapsed references, before and after a retention, with checkpoints
// written as often as the store allows. The draws are from a fixed seed.
func TestLookupParityOverARandomHistory(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x6c6f6f6b7570))
			p := newPair(t, Options{Checkpoints: policy(stress(0))})
			producers := []lifecycle.Producer{"a", "b", "c", "d"}
			seq := uint64(0)
			draw := func(from, to int) store.Record {
				seq++
				at := sec(from + rng.IntN(to-from))
				prod := producers[rng.IntN(len(producers))]
				kind := lifecycle.Observe
				if rng.IntN(100) < 15 {
					kind = lifecycle.Delete
				}
				var ttl time.Duration
				if kind == lifecycle.Observe && rng.IntN(3) == 0 {
					ttl = time.Duration(1+rng.IntN(30)) * time.Second
				}
				var r store.Record
				if rng.IntN(4) == 0 {
					r = podEntityRecord(seq, prod, at, kind, ttl)
				} else {
					r = edgeRecord(seq, prod, at, kind, ttl)
				}
				if kind == lifecycle.Observe && rng.IntN(100) < 30 {
					r.Through = at.Add(time.Duration(1+rng.IntN(40)) * time.Second)
				}
				r.EventTimeBasis = store.EventTimeBasis(rng.IntN(5))
				return r
			}
			writeSome := func(n, from, to int) {
				for range n / 10 {
					batch := make([]store.Record, 10)
					for i := range batch {
						batch[i] = draw(from, to)
					}
					p.write(batch...)
				}
			}
			compare := func(when string) {
				d := p.lookups()
				defer func() { _ = d.ps.Close(); _ = d.ms.Close() }()
				for _, prod := range append(slices.Clone(producers), "nobody") {
					for _, ref := range []store.LookupRef{edgeRef(prod), entityRef(prod)} {
						want := ask(t, d.ms, ref)
						if why := differ(ask(t, d.ps, ref), want); why != "" {
							t.Errorf("%s: %s %v: pebblestore %s than memstore", when, prod, ref.Subject.Kind, why)
						}
					}
				}
			}
			writeSome(150, 0, 100)
			compare("before a retention")
			p.retain(sec(40))
			compare("after it")
			writeSome(60, 40, 120)
			compare("after more records")
			p.retain(sec(90))
			compare("after a second retention")
		})
	}
}

// S11. A hook set on the instrument is told, after a Write's records are
// committed, the prefixes in which the Write deleted checkpoints and how many; the
// counts add up to the store's.
func TestOnInvalidateReportsEachPrefixOnce(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	podFwd, _ := s.prefixOf(catalog.L2, podFP, byte(store.Forward))
	nodeRev, _ := s.prefixOf(catalog.L2, nodeFP, byte(store.Reverse))

	type told struct {
		prefix string
		n      int
	}
	var calls []told
	s.Instrument().OnInvalidate(func(prefix []byte, n int) { calls = append(calls, told{string(prefix), n}) })
	put := func(rs ...store.Record) {
		t.Helper()
		if err := s.Write(bg, rs); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(secs ...int) {
		t.Helper()
		for _, c := range secs {
			for _, f := range []func() error{
				func() error { return s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(c)) },
				func() error { return s.Instrument().CheckpointEdges(catalog.L2, nodeFP, store.Reverse, sec(c)) },
			} {
				if err := f(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	put(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), podEntityRecord(2, "p", sec(10), lifecycle.Observe, 0))
	checkpoint(15, 25)
	if len(calls) != 0 {
		t.Fatalf("a Write that invalidated nothing told the hook: %v", calls)
	}

	// A late edge record, before both checkpoints: the pod's forward prefix and the
	// node's reverse prefix lose two each. The entity's prefix is not touched.
	before := rec.Counter("checkpoint.invalidated")
	put(edgeRecord(3, "q", sec(12), lifecycle.Observe, 0))
	want := []told{{string(podFwd), 2}, {string(nodeRev), 2}}
	if !slices.Equal(calls, want) {
		t.Errorf("told %v, want %v", calls, want)
	}
	if got := rec.Counter("checkpoint.invalidated") - before; got != 4 {
		t.Errorf("checkpoint.invalidated rose by %d, want 4: the sum of the counts", got)
	}

	// Two late records of one Write in one prefix are one call with their sum. The
	// checkpoints are put back first, and the first record deletes the later one
	// only, the second the earlier.
	calls = nil
	checkpoint(15, 25)
	before = rec.Counter("checkpoint.invalidated")
	put(edgeRecord(4, "q", sec(20), lifecycle.Observe, 0), edgeRecord(5, "q", sec(12), lifecycle.Observe, 0))
	want = []told{{string(podFwd), 2}, {string(nodeRev), 2}}
	if !slices.Equal(calls, want) {
		t.Errorf("told %v, want %v", calls, want)
	}
	if got := rec.Counter("checkpoint.invalidated") - before; got != 4 {
		t.Errorf("checkpoint.invalidated rose by %d, want 4", got)
	}

	// A Write of other prefixes, or after every checkpoint, tells no one.
	calls = nil
	checkpoint(25)
	put(edgeRecord(6, "q", sec(30), lifecycle.Observe, 0), podEntityRecord(7, "q", sec(30), lifecycle.Observe, 0))
	if len(calls) != 0 {
		t.Errorf("told %v of a Write that invalidated nothing", calls)
	}

	// Removing the hook stops the calls.
	checkpoint(35)
	s.Instrument().OnInvalidate(nil)
	put(edgeRecord(8, "q", sec(5), lifecycle.Observe, 0))
	if len(calls) != 0 {
		t.Errorf("told %v after the hook was removed", calls)
	}
}

// What an earlier, failed Write noted is not told to the hook by the next.
func TestOnInvalidateForgetsAWriteThatDidNotFinish(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{Checkpoints: off()})
	var calls int
	s.Instrument().OnInvalidate(func([]byte, int) { calls++ })
	s.mu.Lock()
	s.invalidated = append(s.invalidated, invalidation{[]byte("stale"), 7})
	s.mu.Unlock()
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", sec(10), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Errorf("the hook was told %d times of what a Write that did not finish had noted", calls)
	}
}

// S12. After Close a lookup is refused with the store's closed error, and closing
// twice is harmless; a closed store refuses a new one.
func TestALookupAfterCloseIsRefused(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: off()})
	p.write(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0))
	for name, mk := range map[string]func() (lookuper, error){
		"pebblestore": func() (lookuper, error) { return p.s.NewLookup() },
		"memstore":    func() (lookuper, error) { return p.ref.NewLookup() },
	} {
		lk, err := mk()
		if err != nil {
			t.Fatal(err)
		}
		if err := lk.Close(); err != nil {
			t.Errorf("%s: Close = %v", name, err)
		}
		if err := lk.Close(); err != nil {
			t.Errorf("%s: a second Close = %v", name, err)
		}
		got, err := lk.Last(bg, edgeRef("p"))
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("%s: Last after Close = %+v, %v; want an error wrapping ErrClosed", name, got, err)
		}
		if got.Outcome != store.LookupInvalid {
			t.Errorf("%s: an error came with outcome %s", name, got.Outcome)
		}
	}
	if err := p.s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.ref.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.NewLookup(); !errors.Is(err, store.ErrClosed) {
		t.Errorf("pebblestore: NewLookup on a closed store = %v, want ErrClosed", err)
	}
	if _, err := p.ref.NewLookup(); !errors.Is(err, store.ErrClosed) {
		t.Errorf("memstore: NewLookup on a closed store = %v, want ErrClosed", err)
	}
}

// A lookup refuses a reference that cannot be one, and a context that is done.
func TestLookupRefusesWhatItCannotAnswer(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: off()})
	p.write(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0))
	d := p.lookups()
	bad := map[string]store.LookupRef{
		"no producer":          edgeRef(""),
		"an odd producer":      edgeRef("a b"),
		"no relation":          {Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEdge, A: podFP, B: nodeFP}, Producer: "p"},
		"an entity with a B":   {Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEntity, A: podFP, B: nodeFP}, Producer: "p"},
		"no subject":           {Layer: catalog.L2, Producer: "p"},
		"no fingerprint":       {Layer: catalog.L2, Subject: store.EntitySubject(identity.Fingerprint{}), Producer: "p"},
		"a layer out of range": {Layer: catalog.Layer(9), Subject: store.EntitySubject(podFP), Producer: "p"},
	}
	for name, ref := range bad {
		for who, lk := range map[string]lookuper{"pebblestore": d.ps, "memstore": d.ms} {
			if got, err := lk.Last(bg, ref); !errors.Is(err, store.ErrInvalid) || got.Outcome != store.LookupInvalid {
				t.Errorf("%s: %s: Last = %+v, %v; want an error wrapping ErrInvalid and no outcome", who, name, got, err)
			}
		}
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	for who, lk := range map[string]lookuper{"pebblestore": d.ps, "memstore": d.ms} {
		if got, err := lk.Last(ctx, edgeRef("p")); !errors.Is(err, context.Canceled) || got.Outcome != store.LookupInvalid {
			t.Errorf("%s: Last with a done context = %+v, %v", who, got, err)
		}
	}
}

// S13. With no hook set a Write builds no list for it, even when its records
// delete checkpoints.
func TestTheInvalidationHookBuildsNothingWhenUnset(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Checkpoints: off(), Recorder: rec})
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", sec(10), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(2); seq < 6; seq++ {
		for _, f := range []func() error{
			func() error { return s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(1000)) },
			func() error { return s.Instrument().CheckpointEdges(catalog.L2, nodeFP, store.Reverse, sec(1000)) },
		} {
			if err := f(); err != nil {
				t.Fatal(err)
			}
		}
		before := rec.Counter("checkpoint.invalidated")
		if err := s.Write(bg, []store.Record{edgeRecord(seq, "p", sec(500), lifecycle.Observe, 0)}); err != nil {
			t.Fatal(err)
		}
		if got := rec.Counter("checkpoint.invalidated") - before; got != 2 {
			t.Fatalf("the Write deleted %d checkpoints, want the 2 the test placed", got)
		}
		if s.invalidated != nil || cap(s.invalidated) != 0 {
			t.Fatalf("a Write with no hook built a list of %d (capacity %d)", len(s.invalidated), cap(s.invalidated))
		}
	}
}

// S14. The zero value of an answer is not an answer.
func TestTheZeroLastAssertionIsNotFound(t *testing.T) {
	t.Parallel()
	var zero store.LastAssertion
	if zero.Outcome != store.LookupInvalid || zero.Outcome == store.LookupFound {
		t.Errorf("the zero answer has outcome %s", zero.Outcome)
	}
	for _, o := range []store.LookupOutcome{store.LookupFound, store.LookupNone, store.LookupExhausted, store.LookupBaselineOnly} {
		if o == store.LookupInvalid {
			t.Errorf("%s is the zero outcome", o)
		}
	}
}

// A Background retention that has published its horizon and not yet rewritten a
// prefix leaves the records below the horizon in place; the lookup answers for them
// as the baseline the pass will write, so it is the same before and after the pass,
// and the same as the store that keeps everything. The case with a Through at or
// after the horizon and an event time before it is the one where the two would
// differ: the record's Through, not its event time, keeps it alive.
func TestLookupDoesNotDependOnHowFarABackgroundPassIs(t *testing.T) {
	t.Parallel()
	pk := newParker(t, 1)
	o := bgOptions(vfs.NewMem(), off())
	o.beforeRetainChunk = pk.hook
	s := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	ref := newRef(t)
	span := withBasis(withThrough(edgeRecord(1, "span", sec(10), lifecycle.Observe, 5*time.Second), sec(60)), store.BasisObserved)
	alive := edgeRecord(2, "alive", sec(10), lifecycle.Observe, 0)
	lapsed := edgeRecord(3, "lapsed", sec(10), lifecycle.Observe, 5*time.Second)
	gone1 := edgeRecord(4, "gone", sec(10), lifecycle.Observe, 0)
	gone2 := edgeRecord(5, "gone", sec(20), lifecycle.Delete, 0)
	above := edgeRecord(6, "above", sec(70), lifecycle.Observe, 0)
	entity := podEntityRecord(7, "span", sec(10), lifecycle.Observe, 0)
	for _, x := range []store.Store{s, ref} {
		if err := x.Write(bg, cloneAll([]store.Record{span, alive, lapsed, gone1, gone2, above, entity})); err != nil {
			t.Fatal(err)
		}
		if err := x.Retain(bg, sec(50)); err != nil {
			t.Fatal(err)
		}
	}
	pk.wait(t)
	if m := marked(t, s); m.phase != phaseRewrite {
		t.Fatalf("the marker is %+v: the pass should be waiting to rewrite", m)
	}
	check := func(when string) {
		ps, err := s.NewLookup()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ps.Close() }()
		ms, err := ref.NewLookup()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ms.Close() }()
		for _, r := range []store.LookupRef{
			edgeRef("span"), edgeRef("alive"), edgeRef("lapsed"), edgeRef("gone"), edgeRef("above"), edgeRef("nobody"),
			entityRef("span"), entityRef("nobody"),
		} {
			if why := differ(ask(t, ps, r), ask(t, ms, r)); why != "" {
				t.Errorf("%s: %s %v: pebblestore %s than memstore", when, r.Producer, r.Subject.Kind, why)
			}
		}
		for r, want := range map[store.LookupRef]store.LastAssertion{
			edgeRef("span"): wantBaseline(span), edgeRef("alive"): wantBaseline(alive), edgeRef("lapsed"): wantNone(),
			edgeRef("gone"): wantNone(), edgeRef("above"): wantFound(above), entityRef("span"): wantBaseline(entity),
		} {
			if why := differ(ask(t, ps, r), want); why != "" {
				t.Errorf("%s: %s: %s", when, r.Producer, why)
			}
		}
	}
	check("before the pass has rewritten anything")
	pk.release()
	waitRetained(t, s)
	check("after the pass")
}
