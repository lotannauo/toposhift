package svcgraph_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// at is the time of the kth flush.
func at(k int) time.Time { return t0.Add(time.Duration(k) * flush) }

// wantSighting checks that recs are exactly the records of one call of e at the
// instant tm: the client's service, the server's, and the edge, each an Observe with
// the expiry as TTL.
func wantSighting(t *testing.T, recs []store.Record, e edgeID, tm time.Time) {
	t.Helper()
	client, server := serviceFP(t, e.clientNS, e.client), serviceFP(t, e.serverNS, e.server)
	var want []store.Subject
	want = append(want, store.EntitySubject(client))
	if server != client {
		want = append(want, store.EntitySubject(server))
	}
	want = append(want, store.EdgeSubject(client, server, catalog.DependsOn))
	if len(recs) != len(want) {
		t.Fatalf("%d records, want %d for %s:%s", len(recs), len(want), e, describe(recs))
	}
	for i, r := range recs {
		switch {
		case r.Subject != want[i]:
			t.Errorf("record %d: subject %+v, want %+v", i, r.Subject, want[i])
		case !r.EventTime.Equal(tm):
			t.Errorf("record %d: at %s, want %s", i, r.EventTime, tm)
		case r.Kind != lifecycle.Observe || r.TTL != expiry || r.Producer != testProducer || r.EventTimeBasis != store.BasisObserved:
			t.Errorf("record %d: %v ttl %s producer %s basis %s", i, r.Kind, r.TTL, r.Producer, r.EventTimeBasis)
		case !r.Through.IsZero() || len(r.Payload) != 0 || r.Seq != 0 || r.Boot != "":
			t.Errorf("record %d: through %v payload %q seq %d boot %q", i, r.Through, r.Payload, r.Seq, r.Boot)
		}
	}
}

func none(t *testing.T, what string, r svcgraph.Result) {
	t.Helper()
	if len(r.Records) != 0 {
		t.Errorf("%s: %d records, want none:%s", what, len(r.Records), describe(r.Records))
	}
}

// The first flush is not a sighting, and neither is the second: a translator cannot
// tell a series new at the first flush from one the connector already had, and the
// warm-up is two flushes. A series new from the third flush on is a sighting.
func TestTheWarmUpIsTwoFlushes(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	a, b, c, d := edge("a", "b"), edge("c", "d"), edge("e", "f"), edge("g", "h")
	cn := newConn(connStart)
	cn.call(a, 5, t0.Add(-time.Minute))
	cn.call(b, 3, t0.Add(-time.Minute))

	none(t, "flush 0", send(tr, rcv(at(0)), cn.flush(at(0), 0)...))
	if got := tr.Series(); got != 2 {
		t.Errorf("series after flush 0: %d, want 2", got)
	}

	cn.call(c, 1, at(1).Add(-time.Second))
	r := send(tr, rcv(at(1)), cn.flush(at(1), 0)...)
	none(t, "flush 1, with a series new in it", r)
	if n := skipped(r, svcgraph.SkipWarmUp); n != 1 {
		t.Errorf("flush 1: %d series new during warm-up, want 1", n)
	}

	cn.call(d, 1, at(2).Add(-time.Second))
	pts := cn.flush(at(2), 0)
	r = send(tr, rcv(at(2)), pts...)
	// Exactly one sighting: d's. The series with state and no increase give none.
	var dAt time.Time
	for _, p := range pts {
		if attrOf(p, "client") == "g" {
			dAt = time.Unix(0, int64(p.TimeUnixNano)).UTC()
		}
	}
	wantSighting(t, r.Records, d, dAt)
}

func attrOf(p otlpmetrics.SumPoint, key string) string {
	for _, a := range p.Attributes {
		if a.Key == key && a.Value.StringValue != nil {
			return *a.Value.StringValue
		}
	}
	return ""
}

// The warm-up ends at the anchor plus two flushes, to the nanosecond: a new series
// one nanosecond before it is not a sighting, and one at it is.
func TestTheWarmUpEndsAtTheAnchorPlusTwoFlushes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		at   time.Duration
		want int
	}{
		{2*flush - time.Nanosecond, 0},
		{2 * flush, 3},
		{2*flush + time.Nanosecond, 3},
	} {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(edge("a", "b"), connStart, t0, 5))
		e := edge("c", "d")
		r := send(tr, rcv(t0.Add(tc.at)), counter(e, connStart, t0.Add(tc.at), 1))
		if len(r.Records) != tc.want {
			t.Errorf("a new series %s after the anchor: %d records, want %d", tc.at, len(r.Records), tc.want)
		}
	}
}

// A series that sends points five minutes apart makes the largest gap five minutes,
// and the warm-up two of those: a translator that counted two flushes would end it
// too soon.
func TestTheLargestGapLengthensTheWarmUp(t *testing.T) {
	t.Parallel()
	a := edge("a", "b")
	// The anchor is t0 and the largest gap five minutes: the warm-up ends at ten.
	for _, tc := range []struct {
		at   time.Duration
		want int
	}{
		{6 * time.Minute, 0}, {10*time.Minute - time.Nanosecond, 0}, {10 * time.Minute, 3},
	} {
		x := newTranslator(t)
		send(x, rcv(t0), counter(a, connStart, t0, 5))
		send(x, rcv(t0.Add(5*time.Minute)), counter(a, connStart, t0.Add(5*time.Minute), 5))
		e := edge("c", "d")
		r := send(x, rcv(t0.Add(tc.at)), counter(e, connStart, t0.Add(tc.at), 1))
		if len(r.Records) != tc.want {
			t.Errorf("a new series at %s: %d records, want %d", tc.at, len(r.Records), tc.want)
		}
	}
}

// warmed returns a translator past its warm-up, with the series a and b known, and
// the connector that made them.
func warmed(t *testing.T, opts ...func(*svcgraph.Config)) (*svcgraph.Translator, *conn) {
	t.Helper()
	tr := newTranslator(t, opts...)
	cn := newConn(connStart)
	cn.call(edge("a", "b"), 5, t0.Add(-time.Minute))
	cn.call(edge("c", "d"), 3, t0.Add(-time.Minute))
	for k := 0; k < 3; k++ {
		none(t, "warm-up", send(tr, rcv(at(k)), cn.flush(at(k), 0)...))
	}
	return tr, cn
}

// A rare edge, called once after the warm-up, is one sighting, and the flushes that
// re-export it for the next sixteen minutes are none. Its interval ends the expiry
// after the sighting, and says why.
func TestARareEdgeCalledOnceIsOneSighting(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	cn.call(e, 1, at(3).Add(-time.Second))
	var all []store.Record
	first := true
	var sightAt time.Time
	for k := 3; k < 3+20; k++ {
		pts := cn.flush(at(k), 0)
		r := send(tr, rcv(at(k)), pts...)
		if first {
			for _, p := range pts {
				if attrOf(p, "client") == "x" {
					sightAt = time.Unix(0, int64(p.TimeUnixNano)).UTC()
				}
			}
			wantSighting(t, r.Records, e, sightAt)
			first = false
		} else {
			none(t, "a re-export", r)
		}
		all = append(all, r.Records...)
	}
	edgeSubject := store.EdgeSubject(serviceFP(t, "", "x"), serviceFP(t, "", "y"), catalog.DependsOn)
	iv := existence(t, edgeSubject, all)
	if len(iv) != 1 || !iv[0].Start.Equal(sightAt) || !iv[0].End.Equal(sightAt.Add(expiry)) || iv[0].EndSource != lifecycle.EndLivenessExpiry {
		t.Errorf("the edge exists %+v, want [%s, %s) ending by liveness_expiry", iv, sightAt, sightAt.Add(expiry))
	}
}

// A series called once, idle for twenty minutes, and called once again: the connector
// removed it and made it again with the same start time and the same count. The count
// alone cannot tell, and the idle time can: two sightings.
func TestACallAfterTheSeriesWasRemovedIsAnotherSighting(t *testing.T) {
	t.Parallel()
	for _, quiet := range []time.Duration{20 * time.Minute, 3 * time.Hour} {
		tr, cn := warmed(t)
		e := edge("x", "y")
		cn.call(e, 1, at(3).Add(-time.Second))
		total := 0
		k := 3
		for ; at(k).Before(at(3).Add(quiet)); k++ {
			total += len(calls(send(tr, rcv(at(k)), cn.flush(at(k), 0)...).Records))
		}
		if total != 1 {
			t.Fatalf("quiet %s: %d sightings before the second call, want 1", quiet, total)
		}
		cn.call(e, 1, at(k).Add(-time.Second))
		again := 0
		for j := k; j < k+20; j++ {
			again += len(calls(send(tr, rcv(at(j)), cn.flush(at(j), 0)...).Records))
		}
		if again != 1 {
			t.Errorf("quiet %s: %d sightings of the second call, want 1", quiet, again)
		}
	}
}

// The same, with the count lower than before (case 4: the value fell), and with a
// restart of the connector (case 4: a later start time), which a continuation of the
// count would miss.
func TestAResetIsNotAContinuation(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	for i := 0; i < 4; i++ {
		cn.call(e, 3, at(3+i).Add(-time.Second))
		r := send(tr, rcv(at(3+i)), cn.flush(at(3+i), 0)...)
		if len(calls(r.Records)) != 1 {
			t.Fatalf("call %d: %d sightings, want 1", i, len(calls(r.Records)))
		}
	}
	// The value fell: the connector removed the series and made it again with one call.
	k := 7
	for ; at(k).Before(at(6).Add(20 * time.Minute)); k++ {
		send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
	}
	cn.call(e, 1, at(k).Add(-time.Second))
	r := send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
	if len(calls(r.Records)) != 1 {
		t.Errorf("the value fell from 12 to 1: %d sightings, want 1", len(calls(r.Records)))
	}
	// A restart: a later start time, and the count begins again at a value the old one
	// passed.
	cn.restart(at(k + 1).Add(-time.Minute))
	cn.call(e, 2, at(k+1).Add(-time.Second))
	r = send(tr, rcv(at(k+1)), cn.flush(at(k+1), 0)...)
	if len(calls(r.Records)) != 1 {
		t.Errorf("after a restart with a count of 2 (the series had 1): %d sightings, want 1", len(calls(r.Records)))
	}
	// A restart whose count is larger than before is a restart all the same.
	cn.restart(at(k + 2).Add(-time.Minute))
	cn.call(e, 50, at(k+2).Add(-time.Second))
	r = send(tr, rcv(at(k+2)), cn.flush(at(k+2), 0)...)
	if len(calls(r.Records)) != 1 {
		t.Errorf("after a restart with a count of 50: %d sightings, want 1", len(calls(r.Records)))
	}
	// And the flushes after it are not sightings.
	for j := 3; j < 6; j++ {
		none(t, "a re-export after the restart", send(tr, rcv(at(k+j)), cn.flush(at(k+j), 0)...))
	}
}

// A steady series, re-exported with the same count every minute, is no sighting
// however long it goes on, and a series that goes on being called is a sighting at
// each increase.
func TestPresenceIsNotASighting(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	for k := 3; k < 60; k++ {
		cn.call(edge("a", "b"), 0, at(k)) // kept alive without a call, so that its count stays
		if k%10 == 0 {
			cn.call(edge("a", "b"), 1, at(k).Add(-time.Second))
		}
		r := send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
		want := 0
		if k%10 == 0 {
			want = 1
		}
		if got := len(calls(r.Records)); got != want {
			t.Fatalf("flush %d: %d sightings, want %d", k, got, want)
		}
	}
}

// A delta point is a sighting if its value is above zero, and needs no state or
// warm-up.
func TestADeltaPointIsASightingIfItsValueIsAboveZero(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	e := edge("a", "b")
	d := func(at time.Time, v float64) otlpmetrics.SumPoint {
		p := counter(e, at.Add(-time.Minute), at, v)
		p.Temporality = otlpmetrics.Delta
		return p
	}
	r := send(tr, rcv(t0), d(t0, 3))
	wantSighting(t, r.Records, e, t0)
	none(t, "a delta of zero", send(tr, rcv(at(1)), d(at(1), 0)))
	wantSighting(t, send(tr, rcv(at(2)), d(at(2), 1)).Records, e, at(2))
	// The same time again is a sighting too: there is no state to say it is a retry.
	wantSighting(t, send(tr, rcv(at(2)), d(at(2), 1)).Records, e, at(2))
	if tr.Series() != 0 {
		t.Errorf("a delta point made state: %d series", tr.Series())
	}
}

// What a failed group leaves: the sighting of a request that fails is not lost when
// the collector sends it again, and the warm-up the failed group began is not
// counted.
func TestAFailedGroupLeavesTheCommittedState(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	cn.call(e, 1, at(3).Add(-time.Second))
	pts := cn.flush(at(3), 0)

	first := tr.Translate(resource(pts...), rcv(at(3)))
	if len(calls(first.Records)) != 1 {
		t.Fatalf("the first attempt: %d sightings, want 1", len(calls(first.Records)))
	}
	// Further requests of the same group see the pending state: the same points again
	// are a duplicate.
	none(t, "the same request again in the group", tr.Translate(resource(pts...), rcv(at(3))))
	tr.Rollback()
	// The collector sends the request again: it is diffed against the committed state.
	retry := tr.Translate(resource(pts...), rcv(at(3)))
	tr.Commit()
	if len(calls(retry.Records)) != 1 {
		t.Fatalf("the retry: %d sightings, want 1", len(calls(retry.Records)))
	}
	wantSighting(t, retry.Records, e, calls(first.Records)[0].at)
	// Once committed, the request again is a duplicate.
	none(t, "a duplicate after the commit", send(tr, rcv(at(3)), pts...))
}

// A group that fails before any point was committed: the translator is as it was
// made, the warm-up begins with the next group that is committed.
func TestAFailedFirstGroupBeginsNoWarmUp(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	a := edge("a", "b")
	tr.Translate(resource(counter(a, connStart, t0, 5)), rcv(t0))
	tr.Rollback()
	if tr.Series() != 0 {
		t.Errorf("series after a rollback: %d, want 0", tr.Series())
	}
	send(tr, rcv(at(1)), counter(a, connStart, at(1), 5)) // the warm-up begins here
	// A new series at the failed group's two flushes is still in warm-up.
	r := send(tr, rcv(at(2)), counter(a, connStart, at(2), 5), counter(edge("c", "d"), connStart, at(2), 1))
	none(t, "a new series 120 s after the failed group", r)
	// And one at the committed group's two flushes is not.
	r = send(tr, rcv(at(3)), counter(a, connStart, at(3), 5), counter(edge("e", "f"), connStart, at(3), 1))
	wantSighting(t, r.Records, edge("e", "f"), at(3))
}

// Commit promotes what the group changed, whatever order the groups come in, and a
// rollback after it does not undo it.
func TestACommitIsKeptByALaterRollback(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	cn.call(e, 1, at(3).Add(-time.Second))
	pts := cn.flush(at(3), 0)
	if len(calls(send(tr, rcv(at(3)), pts...).Records)) != 1 {
		t.Fatal("no sighting")
	}
	tr.Translate(resource(cn.flush(at(4), 0)...), rcv(at(4)))
	tr.Rollback()
	none(t, "the first request again", send(tr, rcv(at(3)), pts...))
}

// A point whose time is not after the series' last is a retry or a duplicate, and
// writes nothing to the state: the point that follows is diffed against the state
// that was there.
func TestAStalePointChangesNothing(t *testing.T) {
	t.Parallel()
	tr, _ := warmed(t)
	a := edge("a", "b")
	r := send(tr, rcv(at(3)), counter(a, connStart, at(3), 5)) // the same as the state
	none(t, "an unchanged point", r)
	// Same time, a lower value: not a reset, a duplicate.
	r = send(tr, rcv(at(3)), counter(a, connStart, at(3), 4))
	none(t, "a stale point", r)
	if n := skipped(r, svcgraph.SkipStale); n != 1 {
		t.Errorf("%d stale points, want 1", n)
	}
	// Had the stale point changed the value to 4, the next point at 5 would be an increase.
	none(t, "the next point", send(tr, rcv(at(4)), counter(a, connStart, at(4), 5)))
	// An earlier time than the last is stale too, with a higher value.
	r = send(tr, rcv(at(4)), counter(a, connStart, at(3).Add(30*time.Second), 9))
	none(t, "an earlier point with a higher value", r)
	wantSighting(t, send(tr, rcv(at(5)), counter(a, connStart, at(5), 6)).Records, a, at(5))
}

// A start time earlier than the series' is a second connector behind the producer:
// counted, not a sighting, and it leaves the state as it was. A start time later is a
// restart, one sighting.
func TestAnEarlierStartTimeIsASecondConnector(t *testing.T) {
	t.Parallel()
	tr, _ := warmed(t)
	a := edge("a", "b")
	other := connStart.Add(-time.Hour)
	r := send(tr, rcv(at(3)), counter(a, other, at(3), 100))
	none(t, "a point of the other connector", r)
	if n := skipped(r, svcgraph.SkipSecondConnector); n != 1 {
		t.Errorf("%d second-connector points, want 1", n)
	}
	// The state is the first connector's still. Had the other's point been taken
	// for a restart, the first connector's next point would be a later start time and a
	// sighting.
	none(t, "the first connector again", send(tr, rcv(at(4)), counter(a, connStart, at(4), 5)))
	wantSighting(t, send(tr, rcv(at(5)), counter(a, connStart, at(5), 6)).Records, a, at(5))
	// The other connector, again, is no sighting however large its count.
	none(t, "the other connector, larger", send(tr, rcv(at(6)), counter(a, other, at(6), 1000)))
	// A connector that started later is a restart: one sighting, at the first point.
	later := connStart.Add(30 * time.Minute)
	wantSighting(t, send(tr, rcv(at(7)), counter(a, later, at(7), 1)).Records, a, at(7))
	none(t, "and then the earlier one", send(tr, rcv(at(8)), counter(a, connStart, at(8), 7)))
}

// A case 5 sighting sets the last increase to its time, or every flush after it would be
// one: a series that was removed, made again, and is exported every minute.
func TestACaseFiveSightingMovesTheLastIncrease(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	cn.call(e, 1, at(3).Add(-time.Second))
	k := 3
	for ; at(k).Before(at(3).Add(25 * time.Minute)); k++ {
		send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
	}
	cn.call(e, 1, at(k).Add(-time.Second))
	got := 0
	for j := k; j < k+10; j++ {
		got += len(calls(send(tr, rcv(at(j)), cn.flush(at(j), 0)...).Records))
	}
	if got != 1 {
		t.Errorf("%d sightings of a series made again and exported ten times, want 1", got)
	}
}

// A series that is evicted comes back with no state, and is a sighting if its value is
// above zero: the translator is past its warm-up.
func TestAnEvictedSeriesComesBackWithNoState(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("x", "y")
	cn.call(e, 1, at(3).Add(-time.Second))
	send(tr, rcv(at(3)), cn.flush(at(3), 0)...)
	// The other series keep being exported by a connector that is called.
	for k := 4; k < 60; k++ {
		cn.call(edge("a", "b"), 1, at(k).Add(-time.Second))
		cn.call(edge("c", "d"), 1, at(k).Add(-time.Second))
		send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
	}
	if got := tr.Series(); got != 2 {
		t.Fatalf("%d series after the third was idle for an hour, want 2", got)
	}
	cn.call(e, 1, at(60).Add(-time.Second))
	r := send(tr, rcv(at(60)), cn.flush(at(60), 0)...)
	found := false
	for _, c := range calls(r.Records) {
		if c.from == serviceFP(t, "", "x") {
			found = true
		}
	}
	if !found {
		t.Errorf("the series came back after an hour and was not a sighting")
	}
}

// The table holds a bound number of series. A series with no state and no room is
// counted and not sighted; the warm-up begins again at it, so that when the series
// later gains room it is not taken for a call on its unchanged count.
func TestAnOverflowedSeriesIsNotSightedWhenItGainsARoom(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t, maxSeries(2))
	a, b, c := edge("a", "b"), edge("c", "d"), edge("e", "f")
	cn := newConn(connStart)
	cn.call(a, 5, t0.Add(-time.Minute)) // never called again: the connector removes it
	cn.call(b, 5, t0.Add(-time.Minute))
	sightingsOf := func(r svcgraph.Result, client string) int {
		n := 0
		for _, x := range calls(r.Records) {
			if x.from == serviceFP(t, "", client) {
				n++
			}
		}
		return n
	}
	var gained, overflowed int
	sighted := map[int]int{}
	for k := 0; k < 46; k++ {
		if k > 0 && k%10 == 0 {
			cn.call(b, 1, at(k).Add(-time.Second)) // b stays alive, and is sighted
		}
		if k >= 2 && k%10 == 2 {
			cn.call(c, 1, at(k).Add(-time.Second)) // c is new at 2, and called every ten minutes
		}
		r := send(tr, rcv(at(k)), cn.flush(at(k), 0)...)
		switch {
		case skipped(r, svcgraph.SkipSeriesOverflow) > 0:
			overflowed++
		case gained == 0 && skipped(r, svcgraph.SkipWarmUp) == 1:
			gained = k
		}
		sighted[k] = sightingsOf(r, "e")
	}
	// c is called at 2, 12, 22, 32 and 42. It has no room until a, which the connector
	// removed after flush 14, is evicted by the commit of flush 33: the table gains room at
	// flush 34, in a warm-up that its own overflow at flush 33 began. Its count is unchanged until the call at 42.
	if overflowed == 0 || gained != 34 {
		t.Fatalf("c overflowed at %d flushes and gained room at %d, want room at 34", overflowed, gained)
	}
	for k, n := range sighted {
		want := 0
		if k == 42 {
			want = 1
		}
		if n != want {
			t.Errorf("flush %d: %d sightings of c, want %d", k, n, want)
		}
	}
}

// A point more than five minutes ahead of the time its request was received is
// skipped and counted: it changes no state, so the next normal point is diffed against
// what was there.
func TestAPointAheadOfReceiptIsSkippedAndChangesNothing(t *testing.T) {
	t.Parallel()
	tr, _ := warmed(t)
	a := edge("a", "b")
	received := rcv(at(3))
	r := send(tr, received, counter(a, connStart, received.Add(otlpmetrics.MaxClockAhead+time.Second), 6))
	none(t, "a point ahead of receipt", r)
	if n := skipped(r, otlpmetrics.SkipClockAhead); n != 1 {
		t.Errorf("%d clock-ahead points, want 1", n)
	}
	// The next normal increase is a sighting. Had the far point been taken, this would
	// be earlier than the series' last point.
	wantSighting(t, send(tr, rcv(at(4)), counter(a, connStart, at(4), 6)).Records, a, at(4))
	// A point exactly five minutes ahead is accepted.
	edgeAt := rcv(at(5)).Add(otlpmetrics.MaxClockAhead)
	wantSighting(t, send(tr, rcv(at(5)), counter(a, connStart, edgeAt, 7)).Records, a, edgeAt)
	// A new series ahead of receipt does not begin a warm-up or make state.
	fresh := newTranslator(t)
	r = send(fresh, t0, counter(a, connStart, t0.Add(time.Hour), 1))
	none(t, "the first point, ahead", r)
	if fresh.Series() != 0 {
		t.Errorf("a point ahead of receipt made state")
	}
	send(fresh, rcv(at(10)), counter(a, connStart, at(10), 1))
	send(fresh, rcv(at(11)), counter(a, connStart, at(11), 1))
	r = send(fresh, rcv(at(12)), counter(a, connStart, at(12), 1), counter(edge("c", "d"), connStart, at(12), 1))
	wantSighting(t, r.Records, edge("c", "d"), at(12).Add(0))
}

// A point with no time is skipped and counted. With no receipt time given, no point is
// judged to be ahead.
func TestAPointWithNoTimeIsSkipped(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	p := counter(edge("a", "b"), connStart, t0, 1)
	p.TimeUnixNano = 0
	r := send(tr, rcv(t0), p)
	if n := skipped(r, otlpmetrics.SkipNoTime); n != 1 || tr.Series() != 0 {
		t.Errorf("%d points with no time, %d series", n, tr.Series())
	}
	p.TimeUnixNano = uint64(t0.UnixNano())
	p.StartTimeUnixNano = 1 << 63
	if n := skipped(send(tr, rcv(t0), p), otlpmetrics.SkipNoTime); n != 1 {
		t.Errorf("a start time out of range: %d skips, want 1", n)
	}
	if tr.Series() != 0 {
		t.Error("a point with an unusable start time made state")
	}
	far := counter(edge("a", "b"), connStart, t0.Add(24*time.Hour), 1)
	if skipped(send(tr, time.Time{}, far), otlpmetrics.SkipClockAhead) != 0 {
		t.Error("a point was judged ahead of a receipt time that was not given")
	}
}

// What the connector says that is not a call between two services is counted and
// writes nothing, and takes no part in any series' state.
func TestConnectionTypesThatAreNotCallsAreSkipped(t *testing.T) {
	t.Parallel()
	base := counter(edge("a", "b"), connStart, t0, 1)
	for _, tc := range []struct {
		name   string
		attrs  []string // key, value pairs
		reason string
	}{
		{"database", []string{"connection_type", "database"}, svcgraph.SkipConnectionDatabase},
		{"messaging", []string{"connection_type", "messaging_system"}, svcgraph.SkipConnectionMessaging},
		{"virtual node", []string{"connection_type", "virtual_node"}, svcgraph.SkipConnectionVirtualNode},
		{"unknown", []string{"connection_type", "grpc"}, svcgraph.SkipConnectionUnknown},
		{"absent", []string{"connection_type", ""}, svcgraph.SkipNoConnectionType},
		{"a virtual node label", []string{"virtual_node", "client"}, svcgraph.SkipVirtualNodeLabel},
	} {
		tr := newTranslator(t)
		kvs := dropKey(base.Attributes, "connection_type")
		if tc.name != "absent" {
			kvs = append(kvs, kv("connection_type", ""))
		}
		kvs = dropKey(kvs, tc.attrs[0])
		kvs = append(kvs, kv(tc.attrs[0], tc.attrs[1]))
		if tc.name == "absent" {
			kvs = dropKey(kvs, "connection_type")
		}
		// A warm translator, so that a point that reached the state would be a sighting.
		send(tr, rcv(t0), counter(edge("x", "y"), connStart, t0, 1))
		p := withAttrs(counter(edge("a", "b"), connStart, at(3), 1), kvs...)
		r := send(tr, rcv(at(3)), p)
		none(t, tc.name, r)
		if skipped(r, tc.reason) != 1 {
			t.Errorf("%s: skips %+v, want one %q", tc.name, r.Skipped, tc.reason)
		}
		if tr.Series() != 1 {
			t.Errorf("%s: %d series, want only the one that was a call", tc.name, tr.Series())
		}
	}
	// A connection_type given twice with different values is as good as none.
	tr := newTranslator(t)
	p := withAttrs(base, append(base.Attributes, kv("connection_type", "database"))...)
	if r := send(tr, rcv(t0), p); skipped(r, svcgraph.SkipNoConnectionType) != 1 {
		t.Errorf("a connection type given twice: %+v", r.Skipped)
	}
}

func dropKey(kvs []capture.KeyValue, key string) []capture.KeyValue {
	var out []capture.KeyValue
	for _, k := range kvs {
		if k.Key != key {
			out = append(out, k)
		}
	}
	return out
}

// The client's namespace is the client's and the server's the server's: a service with
// a namespace is not the one without, an empty namespace is none, and a blank or
// repeated one writes nothing.
func TestNamespacesBelongToTheirSide(t *testing.T) {
	t.Parallel()
	read := func(e edgeID) []store.Record {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(edge("x", "y"), connStart, t0, 1))
		return send(tr, rcv(at(3)), counter(e, connStart, at(3), 1)).Records
	}
	plain := edge("shop", "db")
	ns := edgeID{client: "shop", server: "db", clientNS: "prod"}
	srvNS := edgeID{client: "shop", server: "db", serverNS: "prod"}
	both := edgeID{client: "shop", server: "db", clientNS: "prod", serverNS: "prod"}
	wantSighting(t, read(plain), plain, at(3))
	wantSighting(t, read(ns), ns, at(3))
	wantSighting(t, read(srvNS), srvNS, at(3))
	wantSighting(t, read(both), both, at(3))
	// Four edges, four distinct clients or servers: a namespace is part of identity.
	seen := map[string]bool{}
	for _, e := range []edgeID{plain, ns, srvNS, both} {
		c := calls(read(e))
		seen[c[0].from.String()+">"+c[0].to.String()] = true
	}
	if len(seen) != 4 {
		t.Errorf("%d distinct edges for four combinations of namespace, want 4", len(seen))
	}
	// The client with no namespace is not given the server's.
	c := calls(read(srvNS))[0]
	if c.from != serviceFP(t, "", "shop") || c.to != serviceFP(t, "prod", "db") {
		t.Error("the client has the server's namespace")
	}
	// An empty namespace label is none.
	empty := withAttrs(counter(plain, connStart, at(3), 1), append(plain.attrs(), kv("client_service.namespace", ""))...)
	tr := newTranslator(t)
	send(tr, rcv(t0), counter(edge("x", "y"), connStart, t0, 1))
	wantSighting(t, send(tr, rcv(at(3)), empty).Records, plain, at(3))
	// A blank one, or one given twice with different values, writes nothing.
	for name, kvs := range map[string][]capture.KeyValue{
		"blank":    append(plain.attrs(), kv("client_service.namespace", " ")),
		"twice":    append(plain.attrs(), kv("server_service.namespace", "a"), kv("server_service.namespace", "b")),
		"not text": append(plain.attrs(), capture.KeyValue{Key: "server_service.namespace", Value: iv(1)}),
	} {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(edge("x", "y"), connStart, t0, 1))
		r := send(tr, rcv(at(3)), withAttrs(counter(plain, connStart, at(3), 1), kvs...))
		none(t, name, r)
		if skipped(r, svcgraph.SkipBadNamespace) != 1 {
			t.Errorf("%s: %+v", name, r.Skipped)
		}
	}
}

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

// A client or server that is absent, blank or repeated writes nothing.
func TestUnusableEndpointsWriteNothing(t *testing.T) {
	t.Parallel()
	for name, kvs := range map[string][]capture.KeyValue{
		"no client":    dropKey(edge("a", "b").attrs(), "client"),
		"no server":    dropKey(edge("a", "b").attrs(), "server"),
		"blank client": append(dropKey(edge("a", "b").attrs(), "client"), kv("client", "  ")),
		"empty server": append(dropKey(edge("a", "b").attrs(), "server"), kv("server", "")),
		"two clients":  append(edge("a", "b").attrs(), kv("client", "z")),
	} {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(edge("x", "y"), connStart, t0, 1))
		r := send(tr, rcv(at(3)), withAttrs(counter(edge("a", "b"), connStart, at(3), 1), kvs...))
		none(t, name, r)
		if skipped(r, svcgraph.SkipNoEndpoint) != 1 {
			t.Errorf("%s: %+v", name, r.Skipped)
		}
	}
}

// Both services of a call are asserted, because an edge is reported only while both its
// endpoints exist; the client is the edge's source, and the server its target. A call
// of a service to itself is its service once and an edge.
func TestBothServicesAreAssertedAndTheDirectionIsClientToServer(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	e := edge("front", "back")
	cn.call(e, 1, at(3).Add(-time.Second))
	r := send(tr, rcv(at(3)), cn.flush(at(3), 0)...)
	if got := entities(r.Records); len(got) != 2 || got[0] != serviceFP(t, "", "front") || got[1] != serviceFP(t, "", "back") {
		t.Errorf("entities %v, want front then back", got)
	}
	c := calls(r.Records)
	if len(c) != 1 || c[0].from != serviceFP(t, "", "front") || c[0].to != serviceFP(t, "", "back") {
		t.Errorf("the edge is %+v, want front to back", c)
	}
	self := edge("loop", "loop")
	cn.call(self, 1, at(4).Add(-time.Second))
	r = send(tr, rcv(at(4)), cn.flush(at(4), 0)...)
	wantSighting(t, r.Records, self, calls(r.Records)[0].at)
	if len(r.Records) != 2 {
		t.Errorf("a self-call gave %d records, want a service and an edge", len(r.Records))
	}
}

// The failed attribute makes two series of an edge, and a call of either is a sighting
// of the same edge.
func TestFailedAndSucceededCallsAreTwoSeriesOfOneEdge(t *testing.T) {
	t.Parallel()
	tr, cn := warmed(t)
	ok, bad := edge("front", "back"), edge("front", "back")
	bad.failed = true
	cn.call(ok, 1, at(3).Add(-time.Second))
	r := send(tr, rcv(at(3)), cn.flush(at(3), 0)...)
	if len(calls(r.Records)) != 1 {
		t.Fatalf("%d sightings, want 1", len(calls(r.Records)))
	}
	cn.call(bad, 1, at(4).Add(-time.Second))
	r = send(tr, rcv(at(4)), cn.flush(at(4), 0)...)
	c := calls(r.Records)
	if len(c) != 1 || c[0].from != serviceFP(t, "", "front") || c[0].to != serviceFP(t, "", "back") {
		t.Errorf("a failed call: %+v", c)
	}
	if tr.Series() != 4 {
		t.Errorf("%d series, want 4", tr.Series())
	}
}

// Points of another scope or metric, a gauge, a sum that is not monotonic and a sum with
// no temporality are counted.
func TestWhatIsNotTheCounterIsCounted(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	a := counter(edge("a", "b"), connStart, t0, 1)
	otherMetric, otherScope, nonMono, noTemp := a, a, a, a
	otherMetric.Metric = "traces_service_graph_request_failed_total"
	otherScope.Scope = "spanmetrics"
	nonMono.Monotonic = false
	noTemp.Temporality = otlpmetrics.TemporalityUnspecified
	res := resource(otherMetric, otherScope, nonMono, noTemp)
	res.DroppedNotSum, res.DroppedNonMonotonic = 2, 3
	r := tr.Translate(res, rcv(t0))
	tr.Commit()
	none(t, "not the counter", r)
	want := map[string]int{
		svcgraph.SkipNotCounter: 2, otlpmetrics.SkipNonMonotonic: 4, svcgraph.SkipNoTemporality: 1, otlpmetrics.SkipNotSum: 2,
	}
	for reason, n := range want {
		if skipped(r, reason) != n {
			t.Errorf("%q: %d, want %d (all: %+v)", reason, skipped(r, reason), n, r.Skipped)
		}
	}
	if tr.Series() != 0 {
		t.Errorf("%d series, want none", tr.Series())
	}
}

// Series are told apart by the resource's attributes: the same point on two resources
// that differ is two series, whatever order the attributes come in.
func TestTheResourceIsPartOfTheSeries(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	p := counter(edge("a", "b"), connStart, t0, 5)
	tr.Translate(otlpmetrics.Resource{Points: []otlpmetrics.SumPoint{p}}, rcv(t0))
	other := otlpmetrics.Resource{Attributes: p2attrs("k8s.cluster.name", "other"), Points: []otlpmetrics.SumPoint{p}}
	tr.Translate(other, rcv(t0))
	tr.Commit()
	if tr.Series() != 2 {
		t.Errorf("%d series, want 2", tr.Series())
	}
	// The order of attributes does not matter.
	r1 := otlpmetrics.Resource{Attributes: []capture.KeyValue{kv("a", "1"), kv("b", "2")}, Points: []otlpmetrics.SumPoint{p}}
	r2 := otlpmetrics.Resource{Attributes: []capture.KeyValue{kv("b", "2"), kv("a", "1")}, Points: []otlpmetrics.SumPoint{p}}
	tr2 := newTranslator(t)
	tr2.Translate(r1, rcv(t0))
	tr2.Translate(r2, rcv(t0))
	if tr2.Series() != 1 {
		t.Errorf("%d series for two orders of the same attributes, want 1", tr2.Series())
	}
}

func p2attrs(k, v string) []capture.KeyValue { return []capture.KeyValue{kv(k, v)} }

// R is fifteen minutes, the connector's cache loop and one flush: a point exactly R
// after the last increase is not yet a series that outlived its idle life, and one a
// nanosecond later is.
func TestCaseFiveBeginsAfterR(t *testing.T) {
	t.Parallel()
	r := idle + svcgraph.DefaultCacheLoop + svcgraph.DefaultFlush
	a := edge("a", "b")
	for _, tc := range []struct {
		after time.Duration
		want  int
	}{
		{r - time.Nanosecond, 0}, {r, 0}, {r + time.Nanosecond, 3},
	} {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(a, connStart, t0, 5))
		got := send(tr, rcv(t0.Add(tc.after)), counter(a, connStart, t0.Add(tc.after), 5))
		if len(got.Records) != tc.want {
			t.Errorf("a point %s after the last increase, count unchanged: %d records, want %d", tc.after, len(got.Records), tc.want)
		}
	}
	// R follows the connector's settings.
	tr := newTranslator(t, func(c *svcgraph.Config) { c.CacheLoop, c.Flush = 2*time.Minute, 30*time.Second })
	send(tr, rcv(t0), counter(a, connStart, t0, 5))
	longer := idle + 2*time.Minute + 30*time.Second
	if got := send(tr, rcv(t0.Add(longer)), counter(a, connStart, t0.Add(longer), 5)); len(got.Records) != 0 {
		t.Error("R does not follow the cache loop and the flush")
	}
	if got := send(tr, rcv(t0.Add(longer+time.Minute)), counter(a, connStart, t0.Add(longer+time.Minute), 5)); len(got.Records) != 3 {
		t.Error("a point after R is not a sighting")
	}
}

// A translator made anew, as the sequencer does after a panic, has no state and a new
// warm-up: the series it knew are not known, and a series new in its first two flushes
// is not a sighting.
func TestATranslatorMadeAnewWarmsUpAgain(t *testing.T) {
	t.Parallel()
	old, cn := warmed(t)
	if old.Series() != 2 {
		t.Fatalf("%d series, want 2", old.Series())
	}
	fresh := newTranslator(t)
	if fresh.Series() != 0 {
		t.Fatalf("a new translator has %d series", fresh.Series())
	}
	cn.call(edge("x", "y"), 1, at(3).Add(-time.Second))
	pts := cn.flush(at(3), 0)
	if len(calls(send(old, rcv(at(3)), pts...).Records)) != 1 {
		t.Fatal("the old translator did not sight the call")
	}
	none(t, "the new translator's first flush", send(fresh, rcv(at(3)), pts...))
}

// A gap of more than R between two points of a series is the connector having removed
// the series and made it again, not a flush: it does not lengthen the warm-up. A gap of
// R or less is counted, as any gap is.
func TestAGapLongerThanRDoesNotLengthenTheWarmUp(t *testing.T) {
	t.Parallel()
	r := idle + svcgraph.DefaultCacheLoop + svcgraph.DefaultFlush
	a := edge("a", "b")
	for _, tc := range []struct {
		gap  time.Duration
		want int // sightings of a series new two flushes after the point that ended the gap
	}{
		{r, 0},               // counted: the warm-up is two R long, from the first point
		{r + time.Minute, 1}, // not counted: still two flushes
	} {
		tr := newTranslator(t)
		send(tr, rcv(t0), counter(a, connStart, t0, 5))
		// The series is called again after the gap, as a series made again is (case 5).
		send(tr, rcv(t0.Add(tc.gap)), counter(a, connStart, t0.Add(tc.gap), 5))
		n := t0.Add(tc.gap + 2*flush)
		got := send(tr, rcv(n), counter(a, connStart, n, 5), counter(edge("c", "d"), connStart, n, 1))
		// The old series is a sighting too (case 5): count the new one's.
		sighted := 0
		for _, c := range calls(got.Records) {
			if c.from == serviceFP(t, "", "c") {
				sighted++
			}
		}
		if sighted != tc.want {
			t.Errorf("a gap of %s: %d sightings of a new series, want %d", tc.gap, sighted, tc.want)
		}
	}
}
