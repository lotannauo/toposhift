package coalesce_test

import (
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// These tests are for Flush and FlushAll: the write of a run's absorbed refresh before
// its stored copy lapses. They use the setting ingest uses (an extension at most once
// per half a TTL, each run's age bounded by that interval) unless a test says
// otherwise, a TTL of 15 minutes and a pull every 5.

const pullTTL = 15 * time.Minute

func minutes(n int) time.Duration { return time.Duration(n) * time.Minute }

func pod(t testing.TB, name string) identity.Fingerprint {
	t.Helper()
	return fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, name)
}

// pull is the observation of fp by the node collector, d into the stream, with the TTL
// of a pull and the given payload buffer, which the caller may overwrite once the
// coalescer has taken it.
func pull(t testing.TB, fp identity.Fingerprint, d time.Duration, payload []byte) store.Record {
	t.Helper()
	r := heartbeat(t, fp, producer, d, pullTTL)
	r.Payload = payload
	return r
}

// richPull is a pull of a host that reports a boot and the basis of its event time, so
// that a record written for it has every field to compare.
func richPull(t testing.TB, fp identity.Fingerprint, d time.Duration, payload []byte) store.Record {
	t.Helper()
	r := pull(t, fp, d, payload)
	r.Boot = "boot-1"
	r.EventTimeBasis = store.BasisObserved
	return r
}

// emitted is a record a coalescer wrote, by Add or by a flush, and the clock a flush
// was called with.
type emitted struct {
	rec   store.Record
	tick  time.Duration
	flush bool
}

func onlyFlushed(es []emitted) []emitted {
	return slices.DeleteFunc(slices.Clone(es), func(e emitted) bool { return !e.flush })
}

func records(es []emitted) []store.Record {
	rs := make([]store.Record, len(es))
	for i, e := range es {
		rs[i] = e.rec
	}
	return rs
}

// drive gives the pulls, in event-time order, to the coalescer, and calls Flush at
// every period on a clock equal to the event times: a pull at a tick is added before
// the flush of that tick. It goes on to end. With mutate, it overwrites each pull's
// payload buffer once the pull is added (after detaching the records Add returned
// from it, which may keep the buffer).
func drive(co *coalesce.Coalescer, pulls []store.Record, period, end time.Duration, mutate bool) []emitted {
	var all []emitted
	tick := period
	flushTo := func(limit time.Duration, inclusive bool) {
		for tick < limit || (inclusive && tick == limit) {
			for _, r := range co.Flush(at(tick), nil) {
				all = append(all, emitted{r, tick, true})
			}
			tick += period
		}
	}
	for _, p := range pulls {
		flushTo(p.EventTime.Sub(base), false)
		for _, r := range co.Add(p, nil) {
			r.Payload = slices.Clone(r.Payload)
			all = append(all, emitted{rec: r})
		}
		if mutate {
			for i := range p.Payload {
				p.Payload[i] = 'Z'
			}
		}
	}
	flushTo(end, true)
	return all
}

// want is the record a flush writes for a run with the description of from: it starts
// start minutes into the stream and goes through through minutes (zero for none).
func want(from store.Record, start, through int) store.Record {
	r := store.Record{
		Layer: from.Layer, Subject: from.Subject, Producer: from.Producer,
		EventTime: at(minutes(start)), Kind: lifecycle.Observe, TTL: from.TTL,
		Payload: slices.Clone(from.Payload), Boot: from.Boot, EventTimeBasis: from.EventTimeBasis,
	}
	if through != 0 {
		r.Through = at(minutes(through))
	}
	return r
}

func sameRecord(t testing.TB, what string, got, want store.Record) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got  %+v\n want %+v", what, got, want)
	}
}

// lapse is when the store's copy of the records of one key lapses: the greatest of
// their Through or event time, plus the TTL.
func lapse(rs []store.Record) time.Time {
	var out time.Time
	for _, r := range rs {
		end := r.EventTime
		if r.Through.After(end) {
			end = r.Through
		}
		if l := end.Add(r.TTL); l.After(out) {
			out = l
		}
	}
	return out
}

func ofKey(rs []store.Record, s store.Subject, p lifecycle.Producer) []store.Record {
	return slices.DeleteFunc(slices.Clone(rs), func(r store.Record) bool { return r.Subject != s || r.Producer != p })
}

func pulls(t testing.TB, fp identity.Fingerprint, mins ...int) []store.Record {
	t.Helper()
	var out []store.Record
	for _, m := range mins {
		out = append(out, pull(t, fp, minutes(m), []byte("a")))
	}
	return out
}

// F1: a producer that pulls on time is never flushed: the output is what it is with no
// flush.
func TestFlushWritesNothingForAProducerThatPullsOnTime(t *testing.T) {
	t.Parallel()

	configs := map[string]coalesce.Config{
		"age from the TTL": ageFromTTL(),
		"max age 30m":      {ExtendTTLFraction: 0.5, RunMaxAge: 30 * time.Minute},
	}
	for name, cfg := range configs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fp := host(t, "h")
			var stream []store.Record
			for m := 0; m <= 60; m += 5 {
				stream = append(stream, pull(t, fp, minutes(m), []byte("a")))
			}
			plain := feedAll(mustNew(t, cfg), stream)
			flushed := drive(mustNew(t, cfg), stream, coalesce.FlushPeriod, minutes(60), false)
			if n := len(onlyFlushed(flushed)); n != 0 {
				t.Errorf("%d records written by a flush, want none: %s", n, spans(records(flushed)))
			}
			if got, want := records(flushed), plain; !reflect.DeepEqual(got, want) {
				t.Errorf("with flush %s, without %s", spans(got), spans(want))
			}
		})
	}
}

// F2: after the producer goes silent, the absorbed refresh is written at the first
// flush within the lead of the stored deadline.
func TestFlushWritesTheAbsorbedRefreshOfARunWhoseProducerWentSilent(t *testing.T) {
	t.Parallel()

	fp := host(t, "h")
	var stream []store.Record
	for _, m := range []int{0, 5, 10, 15} {
		stream = append(stream, richPull(t, fp, minutes(m), []byte("a")))
	}
	es := drive(mustNew(t, ageFromTTL()), stream, coalesce.FlushPeriod, minutes(40), true)

	if got, want := spans(records(es)), "0s 10m0s 10m0s..15m0s"; got != want {
		t.Fatalf("records %s, want %s", got, want)
	}
	extra := onlyFlushed(es)
	if len(extra) != 1 {
		t.Fatalf("%d records written by a flush, want 1", len(extra))
	}
	if want := minutes(25) - coalesce.FlushLead; extra[0].tick != want {
		t.Errorf("written at %s, want the first flush within the lead of the deadline, %s", extra[0].tick, want)
	}
	sameRecord(t, "the flush record", extra[0].rec, want(richPull(t, fp, 0, []byte("a")), 10, 15))
	if string(extra[0].rec.Payload) != "a" {
		t.Errorf("payload %q, want the run's own copy %q", extra[0].rec.Payload, "a")
	}
	if got, want := lapse(records(es)[1:]), at(minutes(30)); !got.Equal(want) {
		t.Errorf("stored lapse %s, want the last refresh plus the TTL, %s", got.Sub(base), want.Sub(base))
	}
	if es[len(es)-1].tick != extra[0].tick || !es[len(es)-1].flush {
		t.Errorf("a record was written after the flush record")
	}
}

// F2b: the flush happens at the lead, not a nanosecond before.
func TestFlushWritesAtExactlyTheLeadBeforeTheDeadline(t *testing.T) {
	t.Parallel()

	deadline := at(minutes(25))
	for _, tc := range []struct {
		name string
		now  time.Time
		want int
	}{
		{"at the lead", deadline.Add(-coalesce.FlushLead), 1},
		{"a nanosecond before", deadline.Add(-coalesce.FlushLead - 1), 0},
		{"after the deadline", deadline.Add(time.Hour), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			co := mustNew(t, ageFromTTL())
			feedAll(co, pulls(t, host(t, "h"), 0, 5, 10, 15))
			if got := co.Flush(tc.now, nil); len(got) != tc.want {
				t.Errorf("Flush wrote %d records, want %d", len(got), tc.want)
			}
		})
	}
}

// F3: a flush does not change when a later refresh starts a run.
func TestFlushDoesNotChangeWhenALaterRefreshStartsARun(t *testing.T) {
	t.Parallel()

	fp := host(t, "h")
	stream := pulls(t, fp, 0, 5, 10, 15)
	for _, later := range []int{20, 28} {
		with, without := mustNew(t, ageFromTTL()), mustNew(t, ageFromTTL())
		feedAll(without, stream)
		feedAll(with, stream)
		if n := len(with.Flush(at(minutes(24)), nil)); n != 1 {
			t.Fatalf("flush wrote %d records, want 1", n)
		}
		r := pull(t, fp, minutes(later), []byte("a"))
		gotWith, gotWithout := with.Add(r, nil), without.Add(r, nil)
		if len(gotWith) == 0 || !reflect.DeepEqual(gotWith[len(gotWith)-1], gotWithout[len(gotWithout)-1]) {
			t.Errorf("refresh at %dm: with flush %s, without %s", later, spans(gotWith), spans(gotWithout))
		}
		if last := gotWith[len(gotWith)-1]; !last.EventTime.Equal(at(minutes(later))) || !last.Through.IsZero() {
			t.Errorf("refresh at %dm did not start a run: %s", later, spans(gotWith))
		}
		// What the flush wrote, the refresh that starts the run no longer has to.
		if later == 28 && (len(gotWith) != 1 || len(gotWithout) != 2) {
			t.Errorf("refresh at 28m: with flush %s, without %s", spans(gotWith), spans(gotWithout))
		}
	}
}

// F4: two subjects of one producer, the second starting one pull later, each end at
// their own last refresh plus the TTL.
func TestFlushEndsEachSubjectAtItsOwnLastRefreshPlusTheTTL(t *testing.T) {
	t.Parallel()

	p, n := pod(t, "p"), node(t, "n")
	edge := store.EdgeSubject(p, n, catalog.ScheduledOn)
	stream := pulls(t, p, 0, 5, 10, 15)
	for _, m := range []int{5, 10, 15, 20} {
		e := pull(t, p, minutes(m), []byte("a"))
		e.Subject = edge
		stream = append(stream, e)
	}
	slices.SortStableFunc(stream, func(a, b store.Record) int { return a.EventTime.Compare(b.EventTime) })

	es := drive(mustNew(t, ageFromTTL()), stream, coalesce.FlushPeriod, minutes(50), false)
	rs := records(es)
	podLapse := lapse(ofKey(rs, store.EntitySubject(p), producer))
	edgeLapse := lapse(ofKey(rs, edge, producer))
	if want := at(minutes(15) + pullTTL); !podLapse.Equal(want) {
		t.Errorf("pod lapses at %s, want %s", podLapse.Sub(base), want.Sub(base))
	}
	if want := at(minutes(20) + pullTTL); !edgeLapse.Equal(want) {
		t.Errorf("edge lapses at %s, want %s", edgeLapse.Sub(base), want.Sub(base))
	}
	if d := edgeLapse.Sub(podLapse); d < 0 || d > minutes(5) {
		t.Errorf("the lapses differ by %s, want at most the pulls' spread, 5m", d)
	}
}

// shuffledRun is a run of one subject and producer: the pull that starts it and the one
// it absorbs.
type shuffledRun struct {
	start, second []store.Record // one pull each; the first starts the run
	startMin      int
}

// manyRuns is 52 runs, of entities and edges and two producers, that start at five
// different times and absorb one refresh each, at times that put the runs in another
// order by their deadlines after a flush than before.
func manyRuns(t testing.TB) (runs []shuffledRun) {
	t.Helper()
	var subjects []store.Subject
	var layers []catalog.Layer
	for i := range 10 {
		fp := host(t, "h"+strconv.Itoa(i))
		subjects, layers = append(subjects, store.EntitySubject(fp)), append(layers, layerOf(t, fp))
	}
	for i := range 10 {
		fp := node(t, "n"+strconv.Itoa(i))
		subjects, layers = append(subjects, store.EntitySubject(fp)), append(layers, layerOf(t, fp))
	}
	for i := range 5 {
		a, b := node(t, "n"+strconv.Itoa(i)), host(t, "h"+strconv.Itoa(i))
		subjects, layers = append(subjects, store.EdgeSubject(a, b, catalog.RunsOn)), append(layers, layerOf(t, a))
	}
	producers := []lifecycle.Producer{"node-collector", "pod-collector"}
	n := 0
	for i, s := range subjects {
		for j, p := range producers {
			startMin := n % 5
			n++
			mk := func(d time.Duration) store.Record {
				return store.Record{
					Layer: layers[i], Subject: s, Producer: p, EventTime: at(d), Kind: lifecycle.Observe,
					TTL: pullTTL, Payload: []byte("a"),
				}
			}
			delta := time.Duration(20*(1+(i*7+j*3)%20)) * time.Second
			runs = append(runs, shuffledRun{
				start:    []store.Record{mk(minutes(startMin))},
				second:   []store.Record{mk(minutes(startMin) + delta)},
				startMin: startMin,
			})
		}
	}
	// Two edges of one pair of entities and one producer, in one relation each, that
	// start together: only the relation puts them in order.
	a, b := node(t, "n9"), host(t, "h9")
	for k, rel := range []catalog.RelationType{catalog.ScheduledOn, catalog.PartOf} {
		mk := func(d time.Duration) store.Record {
			return store.Record{
				Layer: layerOf(t, a), Subject: store.EdgeSubject(a, b, rel), Producer: producers[0],
				EventTime: at(d), Kind: lifecycle.Observe, TTL: pullTTL, Payload: []byte("a"),
			}
		}
		delta := time.Duration(30+10*k) * time.Second
		runs = append(runs, shuffledRun{
			start: []store.Record{mk(0)}, second: []store.Record{mk(delta)}, startMin: 0,
		})
	}
	return runs
}

// expectedOrder is the order of a flush, written out for the test: by the deadline
// before the flush, then the subject (kind, then each entity by type and hash, then the
// relation), then the producer.
func expectedOrder(runs []shuffledRun) []shuffledRun {
	out := slices.Clone(runs)
	fpCmp := func(a, b identity.Fingerprint) int {
		ha, hb := a.Hash(), b.Hash()
		if c := cmp.Compare(string(a.Type()), string(b.Type())); c != 0 {
			return c
		}
		return bytes.Compare(ha[:], hb[:])
	}
	slices.SortFunc(out, func(x, y shuffledRun) int {
		a, b := x.start[0], y.start[0]
		for _, c := range []int{
			cmp.Compare(x.startMin, y.startMin),
			cmp.Compare(a.Subject.Kind, b.Subject.Kind),
			fpCmp(a.Subject.A, b.Subject.A),
			fpCmp(a.Subject.B, b.Subject.B),
			cmp.Compare(string(a.Subject.Relation), string(b.Subject.Relation)),
			cmp.Compare(string(a.Producer), string(b.Producer)),
		} {
			if c != 0 {
				return c
			}
		}
		return 0
	})
	return out
}

// F5: the order of a flush is fixed.
func TestFlushWritesInAFixedOrder(t *testing.T) {
	t.Parallel()

	runs := manyRuns(t)
	wantOrder := expectedOrder(runs)
	wantRecords := make([]store.Record, len(wantOrder))
	for i, r := range wantOrder {
		wantRecords[i] = want(r.start[0], 0, 0)
		wantRecords[i].EventTime = r.start[0].EventTime
		wantRecords[i].Through = r.second[0].EventTime
	}
	// The order by the deadline after the flush is another one, or the test shows nothing.
	byAfter := slices.Clone(wantRecords)
	slices.SortStableFunc(byAfter, func(a, b store.Record) int { return a.Through.Compare(b.Through) })
	if reflect.DeepEqual(byAfter, wantRecords) {
		t.Fatal("the runs are in the same order by their deadlines before and after the flush")
	}

	rng := rand.New(rand.NewPCG(5, 5))
	for rep := range 100 {
		shuffled := slices.Clone(runs)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		co := mustNew(t, ageFromTTL())
		if rep%2 == 0 {
			for _, r := range shuffled {
				feedAll(co, r.start)
				feedAll(co, r.second)
			}
		} else {
			for _, r := range shuffled {
				feedAll(co, r.start)
			}
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			for _, r := range shuffled {
				feedAll(co, r.second)
			}
		}
		got := co.Flush(at(minutes(18)), nil)
		if !reflect.DeepEqual(got, wantRecords) {
			for i := range min(len(got), len(wantRecords)) {
				if !reflect.DeepEqual(got[i], wantRecords[i]) {
					t.Fatalf("repetition %d: record %d is %+v, want %+v (%d records, want %d)",
						rep, i, got[i], wantRecords[i], len(got), len(wantRecords))
				}
			}
			t.Fatalf("repetition %d: %d records, want %d", rep, len(got), len(wantRecords))
		}
	}
}

// F6: a run that began before the horizon is continued where closeRun would continue
// it, by a new run that absorbs the next refresh.
func TestFlushContinuesARunThatBeganBeforeTheHorizon(t *testing.T) {
	t.Parallel()

	// With an interval longer than the TTL, a run's stored copy can lapse before its
	// last refresh.
	long := coalesce.Config{ExtendEvery: 20 * time.Minute, RunAgeFromTTL: true}
	for _, tc := range []struct {
		name        string
		cfg         coalesce.Config
		pulls       []int
		horizon     int
		now         int
		start, thru int
	}{
		{"at the last refresh", ageFromTTL(), []int{0, 5}, 3, 14, 5, 0},
		{"at the lapse of the stored copy", long, []int{0, 6, 12, 18}, 1, 18, 15, 18},
		{"at the horizon", long, []int{0, 6, 12, 18}, 16, 18, 16, 18},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			co := mustNew(t, tc.cfg)
			fp := host(t, "h")
			for _, m := range tc.pulls {
				b := []byte("a")
				co.Add(richPull(t, fp, minutes(m), b), nil)
				b[0] = 'Z'
			}
			co.SetHorizonAll(at(minutes(tc.horizon)))
			got := co.Flush(at(minutes(tc.now)), nil)
			if len(got) != 1 {
				t.Fatalf("%d records, want 1", len(got))
			}
			proto := richPull(t, fp, 0, []byte("a"))
			sameRecord(t, "the continuation", got[0], want(proto, tc.start, tc.thru))
		})
	}
}

func TestAfterAFlushTheContinuationOfARunAbsorbsTheNextRefresh(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*coalesce.Coalescer, identity.Fingerprint) {
		co := mustNew(t, ageFromTTL())
		fp := host(t, "h")
		feedAll(co, pulls(t, fp, 0, 5))
		co.SetHorizonAll(at(minutes(3)))
		if got := co.Flush(at(minutes(14)), nil); len(got) != 1 || !got[0].EventTime.Equal(at(minutes(5))) {
			t.Fatalf("flush wrote %s, want one record at 5m", spans(got))
		}
		return co, fp
	}
	t.Run("a refresh within the age bound is absorbed", func(t *testing.T) {
		t.Parallel()
		co, fp := setup(t)
		if got := co.Add(pull(t, fp, minutes(8), []byte("a")), nil); len(got) != 0 {
			t.Errorf("the refresh at 8m wrote %s, want nothing", spans(got))
		}
	})
	t.Run("a refresh past the bound starts a run without a closing record", func(t *testing.T) {
		t.Parallel()
		co, fp := setup(t)
		got := co.Add(pull(t, fp, minutes(13), []byte("a")), nil)
		if len(got) != 1 || !got[0].EventTime.Equal(at(minutes(13))) || !got[0].Through.IsZero() {
			t.Errorf("the refresh at 13m wrote %s, want only itself", spans(got))
		}
	})
	t.Run("a late refresh inside the old run is covered through it", func(t *testing.T) {
		t.Parallel()
		co, fp := setup(t)
		if got := co.Add(pull(t, fp, minutes(2), []byte("a")), nil); len(got) != 0 {
			t.Errorf("the late refresh at 2m wrote %s, want nothing", spans(got))
		}
	})
}

// F7: a run whose last refresh is before the horizon has nothing to write, and is not
// changed.
func TestFlushLeavesARunWhollyBeforeTheHorizonAlone(t *testing.T) {
	t.Parallel()

	fp := host(t, "h")
	flushed, plain := mustNew(t, ageFromTTL()), mustNew(t, ageFromTTL())
	for _, co := range []*coalesce.Coalescer{flushed, plain} {
		feedAll(co, pulls(t, fp, 0, 5))
		co.SetHorizonAll(at(minutes(10)))
	}
	if got := flushed.Flush(at(minutes(14)), nil); len(got) != 0 {
		t.Errorf("Flush wrote %s", spans(got))
	}
	if got := flushed.FlushAll(nil); len(got) != 0 {
		t.Errorf("FlushAll wrote %s", spans(got))
	}
	if flushed.Runs() != 1 {
		t.Errorf("%d runs, want the run kept", flushed.Runs())
	}
	for _, m := range []int{7, 12, 30} {
		r := pull(t, fp, minutes(m), []byte("a"))
		if a, b := flushed.Add(r, nil), plain.Add(r, nil); !reflect.DeepEqual(a, b) {
			t.Errorf("the refresh at %dm wrote %s after the flush and %s without", m, spans(a), spans(b))
		}
	}
}

// F8: with nothing absorbed there is nothing to write.
func TestFlushWritesNothingWhenNothingWasAbsorbed(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]coalesce.Config{"age from the TTL": ageFromTTL(), "every refresh extends": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			co := mustNew(t, cfg)
			feedAll(co, pulls(t, host(t, "h"), 0, 10, 20))
			feedAll(co, pulls(t, host(t, "g"), 0))
			if got := co.Flush(at(time.Hour), nil); len(got) != 0 {
				t.Errorf("Flush wrote %s", spans(got))
			}
			if got := co.FlushAll(nil); len(got) != 0 {
				t.Errorf("FlushAll wrote %s", spans(got))
			}
		})
	}
}

// F9: a second flush has nothing to write, for a run after the horizon and for one that
// began before it.
func TestAFlushIsIdempotent(t *testing.T) {
	t.Parallel()

	long := coalesce.Config{ExtendEvery: 20 * time.Minute, RunAgeFromTTL: true}
	for _, tc := range []struct {
		name    string
		cfg     coalesce.Config
		pulls   []int
		horizon int
		now     int
	}{
		{"after the horizon", ageFromTTL(), []int{0, 5}, 0, 14},
		{"before the horizon", long, []int{0, 6, 12, 18}, 1, 18},
	} {
		for _, all := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, flush all %t", tc.name, all), func(t *testing.T) {
				t.Parallel()
				co := mustNew(t, tc.cfg)
				feedAll(co, pulls(t, host(t, "h"), tc.pulls...))
				if tc.horizon > 0 {
					co.SetHorizonAll(at(minutes(tc.horizon)))
				}
				flush := func() []store.Record {
					if all {
						return co.FlushAll(nil)
					}
					return co.Flush(at(minutes(tc.now)), nil)
				}
				if got := flush(); len(got) != 1 {
					t.Fatalf("the first flush wrote %d records, want 1", len(got))
				}
				if got := flush(); len(got) != 0 {
					t.Errorf("the second flush wrote %s", spans(got))
				}
				if got := co.FlushAll(nil); len(got) != 0 {
					t.Errorf("FlushAll after it wrote %s", spans(got))
				}
				if got := co.Flush(at(time.Hour), nil); len(got) != 0 {
					t.Errorf("a later flush wrote %s", spans(got))
				}
			})
		}
	}
}

// F10: FlushAll writes every absorbed run once, whatever its deadline, in the order of
// a flush.
func TestFlushAllWritesEveryAbsorbedRunOnceInTheOrderOfAFlush(t *testing.T) {
	t.Parallel()

	runs := manyRuns(t)
	co, same := mustNew(t, ageFromTTL()), mustNew(t, ageFromTTL())
	for _, r := range runs {
		for _, c := range []*coalesce.Coalescer{co, same} {
			feedAll(c, r.start)
			feedAll(c, r.second)
		}
	}
	// Runs with nothing absorbed, and one far from its deadline.
	for i := range 4 {
		r := pull(t, host(t, "fresh"+strconv.Itoa(i)), minutes(i), []byte("a"))
		co.Add(r, nil)
		same.Add(r, nil)
	}
	far := pulls(t, host(t, "far"), 100, 105)
	feedAll(co, far)
	feedAll(same, far)

	got := co.FlushAll(nil)
	if len(got) != len(runs)+1 {
		t.Fatalf("FlushAll wrote %d records, want %d", len(got), len(runs)+1)
	}
	// The same as a flush that is late enough for every run.
	if want := same.Flush(at(24*time.Hour), nil); !reflect.DeepEqual(got, want) {
		t.Errorf("FlushAll wrote %s, and a late Flush %s", spans(got), spans(want))
	}
	if again := co.FlushAll(nil); len(again) != 0 {
		t.Errorf("a second FlushAll wrote %s", spans(again))
	}
	// The run that began at 100m is last by its deadline.
	if last := got[len(got)-1]; last.Subject != store.EntitySubject(host(t, "far")) {
		t.Errorf("the last record is for %v, want the run at 100m", last.Subject)
	}
}

// withSeed runs f with rapid's PRNG seed fixed, so that a property draws the same cases
// on every run. The tests that use it are not parallel, so no other rapid property is
// reading the flag.
func withSeed(t *testing.T, seed uint64, f func()) {
	t.Helper()
	fl := flag.Lookup("rapid.seed")
	if fl == nil {
		t.Fatal("rapid.seed is not registered")
	}
	old := fl.Value.String()
	if err := flag.Set("rapid.seed", strconv.FormatUint(seed, 10)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := flag.Set("rapid.seed", old); err != nil {
			t.Error(err)
		}
	}()
	f()
}

type flushConfig struct {
	name string
	cfg  coalesce.Config
}

var flushConfigs = []flushConfig{
	{"age from the TTL", ageFromTTL()},
	{"max age 30m", coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 30 * time.Minute}},
	{"interval longer than the TTL", coalesce.Config{ExtendEvery: 20 * time.Minute, RunAgeFromTTL: true}},
	{"every refresh extends", coalesce.Config{}},
}

// drawPulls draws the pulls of a few keys (a host and a producer), in event-time order:
// jittered pulls every 5 minutes, with silences, and now and then a new payload.
func drawPulls(t *rapid.T, tb testing.TB) (all []store.Record) {
	nKeys := rapid.IntRange(1, 5).Draw(t, "keys")
	for k := range nKeys {
		fp := host(tb, "k"+strconv.Itoa(k))
		prod := producer
		if rapid.Bool().Draw(t, "second producer") {
			prod = "pod-collector"
		}
		first := time.Duration(rapid.IntRange(0, 600).Draw(t, "start")) * time.Second
		n := rapid.IntRange(2, 20).Draw(t, "pulls")
		when := first
		payload := "a"
		for i := range n {
			if i > 0 {
				gap := minutes(5)
				gap += time.Duration(rapid.IntRange(-90, 90).Draw(t, "jitter")) * time.Second
				gap += time.Duration(rapid.SampledFrom([]int{0, 0, 0, 0, 0, 16, 25, 40}).Draw(t, "silence")) * time.Minute
				when += gap
				if rapid.IntRange(0, 19).Draw(t, "new payload") == 0 {
					payload += "x"
				}
			}
			r := heartbeat(tb, fp, prod, when, pullTTL)
			r.Payload = []byte(payload)
			all = append(all, r)
		}
	}
	slices.SortStableFunc(all, func(a, b store.Record) int { return a.EventTime.Compare(b.EventTime) })
	return all
}

// F11: for a key, every Through and the stored lapse never go backwards, with Add and
// Flush interleaved, whatever the clock the flush is called with.
func TestThroughAndTheStoredLapseOfAKeyNeverGoBackwards(t *testing.T) {
	for _, seed := range []uint64{11, 12} {
		withSeed(t, seed, func() {
			rapid.Check(t, func(rt *rapid.T) {
				fc := rapid.SampledFrom(flushConfigs).Draw(rt, "config")
				stream := drawPulls(rt, t)
				co := mustNew(t, fc.cfg)
				type key struct {
					s store.Subject
					p lifecycle.Producer
				}
				through := map[key]time.Time{}
				stored := map[key]time.Time{}
				take := func(rs []store.Record) {
					for _, r := range rs {
						k := key{r.Subject, r.Producer}
						if !r.Through.IsZero() {
							if r.Through.Before(through[k]) {
								rt.Fatalf("a Through of %s after one of %s", r.Through.Sub(base), through[k].Sub(base))
							}
							through[k] = r.Through
						}
						l := lapse([]store.Record{r})
						if l.Before(stored[k]) {
							rt.Fatalf("a stored lapse of %s after one of %s", l.Sub(base), stored[k].Sub(base))
						}
						stored[k] = l
					}
				}
				for _, r := range stream {
					take(co.Add(r, nil))
					// The ingest clock is at, or well ahead of, the event times.
					ahead := time.Duration(rapid.SampledFrom([]int{0, 0, 30, 400, 1200}).Draw(rt, "ahead")) * time.Second
					if rapid.Bool().Draw(rt, "flush now") {
						take(co.Flush(r.EventTime.Add(ahead), nil))
					}
				}
				take(co.FlushAll(nil))
			})
		})
	}
}

// F12: with Flush called at any period up to the lead, on a clock that advances with the
// event times, every run still being refreshed is stored to at least the sooner of its
// producer's deadline and the lead after the flush, and a graceful stop stores each to
// its producer's deadline.
func TestEveryRunIsStoredToTheSoonerOfItsDeadlineAndTheLeadAfterEachFlush(t *testing.T) {
	// A property that draws nothing to flush shows nothing: count what it reached.
	var flushRecords, afterHorizon int
	defer func() {
		if !t.Failed() && (flushRecords == 0 || afterHorizon == 0) {
			t.Errorf("the property wrote %d flush records, %d under a horizon: it shows too little", flushRecords, afterHorizon)
		}
	}()
	for _, seed := range []uint64{21, 22} {
		withSeed(t, seed, func() {
			rapid.Check(t, func(rt *rapid.T) {
				fc := rapid.SampledFrom(flushConfigs).Draw(rt, "config")
				stream := drawPulls(rt, t)
				period := time.Duration(rapid.SampledFrom([]int{7, 30, 59, 60}).Draw(rt, "period")) * time.Second
				if period > coalesce.FlushLead {
					rt.Fatalf("period %s is more than the lead", period)
				}
				horizonAt := -1 // the index of the pull before which the horizon moves
				if rapid.Bool().Draw(rt, "horizon") {
					horizonAt = rapid.IntRange(0, len(stream)-1).Draw(rt, "horizon at")
				}
				horizonBack := time.Duration(rapid.IntRange(0, 25).Draw(rt, "horizon back")) * time.Minute

				co := mustNew(t, fc.cfg)
				layer := layerOf(t, host(t, "k0"))
				type key struct {
					s store.Subject
					p lifecycle.Producer
				}
				last := map[key]time.Time{}
				stored := map[key][]store.Record{}
				seq := uint64(0)
				take := func(rs []store.Record, flushed bool) {
					for _, r := range rs {
						if h := co.Horizon(r.Layer); flushed && r.EventTime.Before(h) {
							rt.Fatalf("a flush wrote a record at %s, before the horizon %s", r.EventTime.Sub(base), h.Sub(base))
						}
						if h := co.Horizon(r.Layer); flushed && r.Seq == 0 {
							flushRecords++
							if !h.IsZero() {
								afterHorizon++
							}
						}
						seq++
						r.Seq = seq
						if err := r.Validate(); err != nil {
							rt.Fatalf("an output is invalid: %v", err)
						}
						k := key{r.Subject, r.Producer}
						stored[k] = append(stored[k], r)
					}
				}
				afterFlush := func(now time.Duration) {
					h := co.Horizon(layer)
					for k, l := range last {
						if l.Before(h) {
							continue
						}
						need := l.Add(pullTTL)
						if lead := at(now).Add(coalesce.FlushLead); lead.Before(need) {
							need = lead
						}
						if got := lapse(stored[k]); got.Before(need) {
							rt.Fatalf("after Flush(%s) a run last refreshed at %s is stored to %s, want at least %s (horizon %s)",
								now, l.Sub(base), got.Sub(base), need.Sub(base), h.Sub(base))
						}
					}
				}
				tick := period
				flushTo := func(limit time.Duration, inclusive bool) {
					for tick < limit || (inclusive && tick == limit) {
						take(co.Flush(at(tick), nil), true)
						afterFlush(tick)
						tick += period
					}
				}
				for i, r := range stream {
					flushTo(r.EventTime.Sub(base), false)
					if i == horizonAt {
						co.SetHorizonAll(r.EventTime.Add(-horizonBack))
					}
					take(co.Add(r, nil), false)
					last[key{r.Subject, r.Producer}] = r.EventTime
				}
				flushTo(stream[len(stream)-1].EventTime.Sub(base)+pullTTL+2*coalesce.FlushLead, true)

				take(co.FlushAll(nil), true)
				h := co.Horizon(layer)
				for k, l := range last {
					if l.Before(h) {
						continue
					}
					if got, want := lapse(stored[k]), l.Add(pullTTL); !got.Equal(want) {
						rt.Fatalf("after FlushAll a run last refreshed at %s is stored to %s, want %s", l.Sub(base), got.Sub(base), want.Sub(base))
					}
				}
			})
		})
	}
}

// The coalesced stream folds like the raw one with a flush interleaved, as it does
// without (see the properties in property_test.go).
func TestAfterARetentionTheFlushedStreamStillFoldsLikeTheRawOne(t *testing.T) {
	for _, seed := range []uint64{31, 32} {
		withSeed(t, seed, func() {
			rapid.Check(t, func(rt *rapid.T) {
				c, stream := drawStream(rt)
				ret := drawRetention(rt, c)
				if !ret.set {
					ret = retention{set: true, all: true, at: c.Start.Add(c.Duration / 2)}
				}
				cfg := coalesce.DefaultConfig()
				if rapid.Bool().Draw(rt, "fixedInterval") {
					cfg.ExtendTTLFraction = 0
					cfg.ExtendEvery = time.Duration(rapid.IntRange(1, 600).Draw(rt, "everySeconds")) * time.Second
				}
				cfg.RunMaxAge = min(time.Duration(rapid.IntRange(1, 30).Draw(rt, "maxAgeMinutes"))*time.Minute, max(time.Minute, c.Duration-c.HeartbeatInterval))
				period := time.Duration(rapid.SampledFrom([]int{10, 30}).Draw(rt, "period")) * time.Second
				check(rt, cfg, false, feedFlushing(rt, cfg, stream, ret, period, rapid.Bool().Draw(rt, "flush all")), ret, new(tally))
			})
		})
	}
}

// feedFlushing is feed with a flush at every period of the clock, which is the latest
// event time seen, and with a FlushAll at the end if all is set.
func feedFlushing(t fataler, cfg coalesce.Config, stream []store.Record, ret retention, period time.Duration, all bool) outcome {
	co, err := coalesce.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var o outcome
	told := false
	take := func(x store.Record) {
		if told && ret.refuses(x) {
			return
		}
		x.Seq = uint64(len(o.out) + 1)
		if err := x.Validate(); err != nil {
			t.Fatalf("an output is invalid: %v", err)
		}
		o.out = append(o.out, x)
	}
	tick := stream[0].EventTime.Add(period)
	for _, r := range stream {
		for tick.Before(r.EventTime) {
			for _, x := range co.Flush(tick, nil) {
				take(x)
			}
			tick = tick.Add(period)
		}
		if !ret.refuses(r) || !told {
			o.raw = append(o.raw, r)
		}
		for _, x := range co.Add(r, nil) {
			take(x)
		}
		if ret.set && !told && !r.EventTime.Before(ret.at) {
			told = true
			if ret.all {
				co.SetHorizonAll(ret.at)
			} else {
				co.SetHorizon(ret.layer, ret.at)
			}
		}
	}
	if all {
		for _, x := range co.FlushAll(nil) {
			take(x)
		}
	}
	return o
}

// F14: Prune does not forget a run whose absorbed refresh is yet to be written.
func TestPruneKeepsARunWithAnAbsorbedRefreshUntilItIsFlushed(t *testing.T) {
	t.Parallel()

	co := mustNew(t, ageFromTTL())
	feedAll(co, pulls(t, host(t, "h"), 0, 5))
	if n := co.Prune(at(minutes(21))); n != 0 || co.Runs() != 1 {
		t.Fatalf("Prune forgot %d runs, %d left, want the run kept", n, co.Runs())
	}
	got := co.Flush(at(minutes(21)), nil)
	if len(got) != 1 || got[0].Through.IsZero() || !got[0].Through.Equal(at(minutes(5))) {
		t.Fatalf("Flush wrote %s, want the extension through 5m", spans(got))
	}
	if n := co.Prune(at(minutes(21))); n != 1 || co.Runs() != 0 {
		t.Errorf("after the flush Prune forgot %d runs, %d left, want it forgotten", n, co.Runs())
	}
}

// F14b: a run whose last refresh is before the horizon has nothing to write, so Prune
// forgets it.
func TestPruneForgetsARunWhollyBeforeTheHorizon(t *testing.T) {
	t.Parallel()

	co := mustNew(t, ageFromTTL())
	feedAll(co, pulls(t, host(t, "h"), 0, 5))
	co.SetHorizonAll(at(minutes(10)))
	if n := co.Prune(at(minutes(21))); n != 1 || co.Runs() != 0 {
		t.Errorf("Prune forgot %d runs, %d left, want the run forgotten", n, co.Runs())
	}
}
