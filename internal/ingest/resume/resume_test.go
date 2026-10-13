package resume_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// Both stores' lookups are Lookups as they are.
var (
	_ resume.Lookup = (*pebblestore.Lookup)(nil)
	_ resume.Lookup = (*memstore.Lookup)(nil)
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func at(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }

const producer = lifecycle.Producer("k8sobjects/default")

func fpOf(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// fakeLookup answers each reference as set, or None; err, when set, is the answer
// to every reference not set.
type fakeLookup struct {
	answers map[store.LookupRef]fakeAnswer
	err     error
	calls   int
}

type fakeAnswer struct {
	last store.LastAssertion
	err  error
}

func (f *fakeLookup) Last(_ context.Context, ref store.LookupRef) (store.LastAssertion, error) {
	f.calls++
	if a, ok := f.answers[ref]; ok {
		return a.last, a.err
	}
	if f.err != nil {
		return store.LastAssertion{}, f.err
	}
	return store.LastAssertion{Outcome: store.LookupNone}, nil
}

func (f *fakeLookup) Close() error { return nil }

func (f *fakeLookup) set(r store.Record, a store.LastAssertion, err error) {
	if f.answers == nil {
		f.answers = map[store.LookupRef]fakeAnswer{}
	}
	f.answers[refOf(r)] = fakeAnswer{a, err}
}

func refOf(r store.Record) store.LookupRef {
	return store.LookupRef{Layer: r.Layer, Subject: r.Subject, Producer: r.Producer}
}

// found is a stored Observe at event, through through (zero for none), with basis
// and payload.
func found(event, through time.Time, basis store.EventTimeBasis, payload string) store.LastAssertion {
	a := store.LastAssertion{
		Outcome: store.LookupFound, Kind: lifecycle.Observe, EventTime: event, Through: through,
		TTL: 15 * time.Minute, Basis: basis, Payload: []byte(payload),
	}
	a.Last, a.LastFromThrough = store.LastOf(event, through)
	return a
}

// recorder is a Resumer that keeps what it is told.
type recorder struct{ got []resumed }

type resumed struct {
	handle any
	t      time.Time
	basis  store.EventTimeBasis
}

func (r *recorder) Resumed(h any, t time.Time, b store.EventTimeBasis) {
	r.got = append(r.got, resumed{h, t, b})
}

// firstSighting is a pod's first record as k8sobjects gives it: at its creation,
// with basis object field, through the time it was observed.
func firstSighting(fp identity.Fingerprint, created, observed time.Time, payload string) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: producer, EventTime: created,
		Through: observed, Kind: lifecycle.Observe, TTL: 15 * time.Minute, Payload: []byte(payload),
		EventTimeBasis: store.BasisObjectField,
	}
}

func noHorizon(catalog.Layer) time.Time { return time.Time{} }

func apply(t *testing.T, lk resume.Lookup, h func(catalog.Layer) time.Time, recs []store.Record, tr resume.Resumer) (resume.Result, resume.Stats) {
	t.Helper()
	res := resume.Result{Records: recs, Marks: []resume.Mark{{Index: 0, Handle: "h0"}}}
	st, err := resume.Apply(context.Background(), lk, h, &res, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	return res, st
}

type want struct {
	event, through time.Time
	basis          store.EventTimeBasis
	dropped        bool
}

// The decision table, row by row, on one marked record: a pod created at 00:00 and
// observed at 00:40.
func TestTheDecisionTable(t *testing.T) {
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	rec := firstSighting(pod, at(0), at(40), `{"phase":"Running"}`)
	h20 := func(catalog.Layer) time.Time { return at(20) }
	del := func(event time.Time) store.LastAssertion {
		return store.LastAssertion{Outcome: store.LookupFound, Kind: lifecycle.Delete, EventTime: event, Last: event, Basis: store.BasisObserved}
	}
	fallback := want{event: at(40), basis: store.BasisObserved}
	unchanged := want{event: at(0), through: at(40), basis: store.BasisObjectField}
	for _, c := range []struct {
		name    string
		last    store.LastAssertion
		err     error
		horizon func(catalog.Layer) time.Time
		want    want
		count   func(resume.Stats) int64
	}{
		{"none: a genuine first sighting", store.LastAssertion{Outcome: store.LookupNone}, nil, noHorizon, unchanged, func(s resume.Stats) int64 { return s.New }},
		{"exhausted", store.LastAssertion{Outcome: store.LookupExhausted}, nil, noHorizon, fallback, func(s resume.Stats) int64 { return s.UnknownExhausted }},
		{"baseline only", store.LastAssertion{Outcome: store.LookupBaselineOnly, Kind: lifecycle.Observe, EventTime: at(10), Last: at(10)}, nil, noHorizon, fallback, func(s resume.Stats) int64 { return s.UnknownBaseline }},
		{"a delete after the start", del(at(5)), nil, noHorizon, want{dropped: true}, func(s resume.Stats) int64 { return s.Dropped }},
		{"a delete at the start", del(at(0)), nil, noHorizon, want{dropped: true}, func(s resume.Stats) int64 { return s.Dropped }},
		{"a delete before the start: a new life", del(at(-5)), nil, noHorizon, unchanged, func(s resume.Stats) int64 { return s.New }},
		{"an Observe whose last is at the start", found(at(-10), at(0), store.BasisObserved, `{"phase":"Running"}`), nil, noHorizon, unchanged, func(s resume.Stats) int64 { return s.New }},
		{
			"unchanged, last its event time", found(at(30), time.Time{}, store.BasisObserved, `{"phase":"Running"}`), nil, noHorizon,
			want{event: at(30), through: at(40), basis: store.BasisObserved},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{
			"unchanged, last an object field time", found(at(30), time.Time{}, store.BasisObjectField, `{"phase":"Running"}`), nil, noHorizon,
			want{event: at(30), through: at(40), basis: store.BasisObjectField},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{
			"unchanged, last its Through", found(at(0), at(35), store.BasisObjectField, `{"phase":"Running"}`), nil, noHorizon,
			want{event: at(35), through: at(40), basis: store.BasisObserved},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{
			"unchanged, last after the observation: held at it", found(at(30), at(50), store.BasisObserved, `{"phase":"Running"}`), nil, noHorizon,
			want{event: at(40), basis: store.BasisObserved},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{
			"unchanged, last at the observation", found(at(40), time.Time{}, store.BasisObjectField, `{"phase":"Running"}`), nil, noHorizon,
			want{event: at(40), basis: store.BasisObjectField},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{"changed within the TTL", found(at(30), time.Time{}, store.BasisObserved, `{"phase":"Pending"}`), nil, noHorizon, fallback, func(s resume.Stats) int64 { return s.Changed }},
		{"below the horizon", found(at(10), time.Time{}, store.BasisObserved, `{"phase":"Running"}`), nil, h20, fallback, func(s resume.Stats) int64 { return s.UnknownBelowHorizon }},
		{
			"at the horizon", found(at(20), time.Time{}, store.BasisObserved, `{"phase":"Running"}`), nil, h20,
			want{event: at(20), through: at(40), basis: store.BasisObserved},
			func(s resume.Stats) int64 { return s.Raised },
		},
		{"a read error", store.LastAssertion{}, errors.New("a key could not be decoded"), noHorizon, fallback, func(s resume.Stats) int64 { return s.UnknownError }},
		{"an invalid reference", store.LastAssertion{}, fmt.Errorf("lookup: %w", store.ErrInvalid), noHorizon, fallback, func(s resume.Stats) int64 { return s.UnknownError }},
	} {
		t.Run(c.name, func(t *testing.T) {
			lk := &fakeLookup{}
			lk.set(rec, c.last, c.err)
			tr := &recorder{}
			res, st := apply(t, lk, c.horizon, []store.Record{rec}, tr)
			if got := c.count(st); got != 1 {
				t.Errorf("counter %d, want 1: %+v", got, st)
			}
			sum := st.New + st.Raised + st.Changed + st.Dropped + st.Unknown
			if sum != 1 || st.Unknown != st.UnknownExhausted+st.UnknownBaseline+st.UnknownBelowHorizon+st.UnknownError {
				t.Errorf("the counts do not add up to one decision: %+v", st)
			}
			if c.want.dropped {
				if len(res.Records) != 0 || len(res.Marks) != 0 || len(res.Dropped) != 1 || res.Dropped[0] != 0 {
					t.Errorf("dropped: records %v, marks %v, dropped %v", res.Records, res.Marks, res.Dropped)
				}
				if len(tr.got) != 0 {
					t.Errorf("a dropped record is resumed: %v", tr.got)
				}
				return
			}
			r := res.Records[0]
			if !r.EventTime.Equal(c.want.event) || !r.Through.Equal(c.want.through) || r.EventTimeBasis != c.want.basis {
				t.Errorf("record at %s through %s basis %s; want %s through %s basis %s", r.EventTime.Sub(t0), r.Through.Sub(t0), r.EventTimeBasis,
					c.want.event.Sub(t0), c.want.through.Sub(t0), c.want.basis)
			}
			if string(r.Payload) != `{"phase":"Running"}` {
				t.Errorf("payload %s, want the record's own", r.Payload)
			}
			if len(tr.got) != 1 || tr.got[0].handle != "h0" || !tr.got[0].t.Equal(r.EventTime) || tr.got[0].basis != r.EventTimeBasis {
				t.Errorf("Resumed %v, want the record's final time and basis once", tr.got)
			}
			if c.err != nil && (st.FirstError == nil || !errors.Is(st.FirstError, c.err) || st.ErrorExample != refOf(rec)) {
				t.Errorf("FirstError %v example %v, want the read error and the reference", st.FirstError, st.ErrorExample)
			}
		})
	}
}

// A changed description after a silence longer than the TTL is also counted as a
// gap, with the reference as its example; one within the TTL is not.
func TestAChangeAfterTheTTLIsAGap(t *testing.T) {
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	rec := firstSighting(pod, at(0), at(60), `{"phase":"Running"}`)
	lk := &fakeLookup{}
	lk.set(rec, found(at(30), time.Time{}, store.BasisObserved, `{"phase":"Pending"}`), nil)
	_, st := apply(t, lk, noHorizon, []store.Record{rec}, nil)
	if st.Changed != 1 || st.ChangedGap != 1 || st.GapExample != refOf(rec) {
		t.Errorf("%+v, want changed and a gap (60 > 30 + 15 minutes)", st)
	}
	lk.set(rec, found(at(45), time.Time{}, store.BasisObserved, `{"phase":"Pending"}`), nil)
	if _, st := apply(t, lk, noHorizon, []store.Record{rec}, nil); st.Changed != 1 || st.ChangedGap != 0 {
		t.Errorf("%+v, want changed and no gap (60 = 45 + 15 minutes)", st)
	}
}

// An error of a closed store, a closed lookup or a done context aborts: Apply
// returns it before it changes a record, tells the Resumer anything, or counts
// anything, even when an earlier mark had its answer.
func TestAnAbortChangesNothing(t *testing.T) {
	pod1 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	pod2 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, abort := range []error{
		fmt.Errorf("pebblestore: Lookup.Last: %w", store.ErrClosed),
		fmt.Errorf("pebblestore: Lookup.Last: %w", context.Canceled),
		fmt.Errorf("memstore: Lookup.Last: %w", context.DeadlineExceeded),
	} {
		a := firstSighting(pod1, at(0), at(40), "{}")
		b := firstSighting(pod2, at(0), at(40), "{}")
		lk := &fakeLookup{}
		lk.set(a, found(at(30), time.Time{}, store.BasisObserved, "{}"), nil)
		lk.set(b, store.LastAssertion{}, abort)
		res := resume.Result{Records: []store.Record{a, b}, Marks: []resume.Mark{{Index: 0}, {Index: 1}}, Dropped: []int{7}}
		tr := &recorder{}
		st, err := resume.Apply(ctx, lk, noHorizon, &res, tr, nil)
		if !errors.Is(err, abort) || !resume.IsAbort(err) {
			t.Errorf("%v: Apply returned %v, want the abort", abort, err)
		}
		if st != (resume.Stats{}) || len(tr.got) != 0 {
			t.Errorf("%v: counted %+v, resumed %v; want nothing", abort, st, tr.got)
		}
		want := resume.Result{Records: []store.Record{a, b}, Marks: []resume.Mark{{Index: 0}, {Index: 1}}, Dropped: []int{7}}
		if !reflect.DeepEqual(res, want) {
			t.Errorf("%v: the result changed: %+v", abort, res)
		}
	}
	if resume.IsAbort(store.ErrInvalid) || resume.IsAbort(errors.New("decode")) {
		t.Error("a read error that is not an abort is taken for one")
	}
}

// Any other read error falls back for its mark alone: the other marks are looked
// up and decided, and the first error is kept.
func TestAReadErrorFallsBackForItsMarkAlone(t *testing.T) {
	pods := []identity.Fingerprint{
		fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1"), fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p2"),
		fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p3"),
	}
	var recs []store.Record
	var marks []resume.Mark
	for i, p := range pods {
		recs = append(recs, firstSighting(p, at(0), at(40), "{}"))
		marks = append(marks, resume.Mark{Index: i})
	}
	first := errors.New("the first")
	lk := &fakeLookup{}
	lk.set(recs[0], store.LastAssertion{}, first)
	lk.set(recs[1], found(at(30), time.Time{}, store.BasisObserved, "{}"), nil)
	lk.set(recs[2], store.LastAssertion{}, errors.New("the second"))
	res := resume.Result{Records: recs, Marks: marks}
	st, err := resume.Apply(context.Background(), lk, noHorizon, &res, nil, nil)
	if err != nil {
		t.Fatalf("Apply returned %v, want the group to go on", err)
	}
	if lk.calls != 3 || st.UnknownError != 2 || st.Raised != 1 || !errors.Is(st.FirstError, first) || st.ErrorExample != refOf(recs[0]) {
		t.Errorf("calls %d, %+v", lk.calls, st)
	}
	for i, w := range []time.Time{at(40), at(30), at(40)} {
		if r := res.Records[i]; !r.EventTime.Equal(w) {
			t.Errorf("record %d at %s, want %s", i, r.EventTime.Sub(t0), w.Sub(t0))
		}
	}
}

// Later records of the same producer and subject timed before a resumed record
// take its time and basis, and keep their Through only while it is after it; a
// record of another subject or producer is not touched.
func TestTheMirrorRaise(t *testing.T) {
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	other := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p2")
	first := firstSighting(pod, at(0), at(40), "{}")
	later := firstSighting(pod, at(20), at(41), "{}")      // a watch event timed by its change
	atObs := firstSighting(pod, at(25), time.Time{}, "{}") // at or after its observation: no Through
	atObs.EventTimeBasis = store.BasisObserved
	elsewhere := firstSighting(other, at(10), at(41), "{}")
	otherProducer := later
	otherProducer.Producer = "k8scluster/default"
	lk := &fakeLookup{}
	lk.set(first, found(at(0), at(30), store.BasisObjectField, "{}"), nil)
	res := resume.Result{Records: []store.Record{first, later, atObs, elsewhere, otherProducer}, Marks: []resume.Mark{{Index: 0}}}
	st, err := resume.Apply(context.Background(), lk, noHorizon, &res, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Raised != 1 || st.MirrorRaised != 2 {
		t.Errorf("%+v, want one raised and two mirrored", st)
	}
	for i, w := range []want{
		{event: at(30), through: at(40), basis: store.BasisObserved},
		{event: at(30), through: at(41), basis: store.BasisObserved},
		{event: at(30), basis: store.BasisObserved},
		{event: at(10), through: at(41), basis: store.BasisObjectField},
		{event: at(20), through: at(41), basis: store.BasisObjectField},
	} {
		r := res.Records[i]
		if !r.EventTime.Equal(w.event) || !r.Through.Equal(w.through) || r.EventTimeBasis != w.basis {
			t.Errorf("record %d at %s through %s basis %s, want %s through %s basis %s", i, r.EventTime.Sub(t0), r.Through.Sub(t0), r.EventTimeBasis,
				w.event.Sub(t0), w.through.Sub(t0), w.basis)
		}
	}
}

// A dropped record leaves Records, its mark goes, and the other marks follow their
// records.
func TestADropMovesTheMarks(t *testing.T) {
	p1 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	p2 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p2")
	a, b := firstSighting(p1, at(0), at(40), "{}"), firstSighting(p2, at(0), at(40), "{}")
	plain := firstSighting(p2, at(40), time.Time{}, "{}")
	lk := &fakeLookup{}
	lk.set(a, store.LastAssertion{Outcome: store.LookupFound, Kind: lifecycle.Delete, EventTime: at(20), Last: at(20)}, nil)
	lk.set(b, found(at(30), time.Time{}, store.BasisObserved, "{}"), nil)
	res := resume.Result{Records: []store.Record{a, plain, b}, Marks: []resume.Mark{{Index: 0, Handle: "a"}, {Index: 2, Handle: "b"}}}
	tr := &recorder{}
	st, err := resume.Apply(context.Background(), lk, noHorizon, &res, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Dropped != 1 || st.Raised != 1 || len(res.Records) != 2 || len(res.Dropped) != 1 || res.Dropped[0] != 0 {
		t.Fatalf("%+v; records %d, dropped %v", st, len(res.Records), res.Dropped)
	}
	if len(res.Marks) != 1 || res.Marks[0].Index != 1 || res.Marks[0].Handle != "b" || res.Records[1].Subject != b.Subject {
		t.Errorf("marks %+v", res.Marks)
	}
	if len(tr.got) != 1 || tr.got[0].handle != "b" {
		t.Errorf("resumed %v, want b alone", tr.got)
	}
}

// A mark on a record that is not a backdated Observe is left as it is, with no
// lookup; a mark out of range panics.
func TestMarksThatAreNotBackdated(t *testing.T) {
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	plain := firstSighting(pod, at(40), time.Time{}, "{}")
	del := plain
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	lk := &fakeLookup{}
	res := resume.Result{Records: []store.Record{plain, del}, Marks: []resume.Mark{{Index: 0}, {Index: 1}}}
	st, err := resume.Apply(context.Background(), lk, noHorizon, &res, nil, nil)
	if err != nil || st.New != 2 || lk.calls != 0 || !reflect.DeepEqual(res.Records, []store.Record{plain, del}) {
		t.Errorf("%+v %v, %d lookups", st, err, lk.calls)
	}
	defer func() {
		if recover() == nil {
			t.Error("a mark out of range does not panic")
		}
	}()
	res = resume.Result{Records: []store.Record{plain}, Marks: []resume.Mark{{Index: 1}}}
	_, _ = resume.Apply(context.Background(), lk, noHorizon, &res, nil, nil)
}

// The description is compared through the given function, on both payloads.
func TestTheComparisonIsTheGivenOne(t *testing.T) {
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	rec := firstSighting(pod, at(0), at(40), `{"a":1,"derived":2}`)
	lk := &fakeLookup{}
	lk.set(rec, found(at(30), time.Time{}, store.BasisObserved, `{"a":1,"derived":3}`), nil)
	strip := func(_ store.Subject, p []byte) []byte { return p[:6] }
	res := resume.Result{Records: []store.Record{rec}, Marks: []resume.Mark{{Index: 0}}}
	if st, _ := resume.Apply(context.Background(), lk, noHorizon, &res, nil, strip); st.Raised != 1 || string(res.Records[0].Payload) != `{"a":1,"derived":2}` {
		t.Errorf("%+v, payload %s: want raised with its own payload", st, res.Records[0].Payload)
	}
	res = resume.Result{Records: []store.Record{rec}, Marks: []resume.Mark{{Index: 0}}}
	if st, _ := resume.Apply(context.Background(), lk, noHorizon, &res, nil, nil); st.Changed != 1 {
		t.Errorf("%+v: raw payloads that differ are not a change", st)
	}
}

func TestStatsAdd(t *testing.T) {
	ref := store.LookupRef{Layer: catalog.L2, Producer: producer}
	e1, e2 := errors.New("1"), errors.New("2")
	a := resume.Stats{New: 1, Unknown: 1, UnknownError: 1, FirstError: e1, ErrorExample: ref}
	b := resume.Stats{New: 2, Raised: 3, MirrorRaised: 4, ChangedGap: 1, FirstError: e2, GapExample: ref}
	a.Add(b)
	if a.New != 3 || a.Raised != 3 || a.MirrorRaised != 4 || a.ChangedGap != 1 || !errors.Is(a.FirstError, e1) || a.GapExample != ref {
		t.Errorf("%+v", a)
	}
	c := a.Counters()
	if c["new"] != 3 || c["unknown.error"] != 1 || c["mirror_raised"] != 4 || len(c) != 11 {
		t.Errorf("counters %v", c)
	}
}
