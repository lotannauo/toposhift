package coalesce_test

import (
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// These tests are for the age of a run bounded by its own extension interval
// (Config.RunAgeFromTTL) and for a record that carries a Through across the horizon.

// ageFromTTL is the setting: extend at most once per half a TTL, and bound each run's age
// by that interval.
func ageFromTTL() coalesce.Config {
	return coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
}

// at is the instant d after the start of the stream.
func at(d time.Duration) time.Time { return base.Add(d) }

// heartbeat is the observation of a subject by a producer, d into the stream, with the
// given TTL and the payload "a".
func heartbeat(t testing.TB, fp identity.Fingerprint, p lifecycle.Producer, d, ttl time.Duration) store.Record {
	t.Helper()
	return store.Record{
		Layer: layerOf(t, fp), Subject: store.EntitySubject(fp), Producer: p,
		EventTime: at(d), Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("a"),
	}
}

// spans prints the records as the offset of each, with ..through when it has one.
func spans(rs []store.Record) string {
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = r.EventTime.Sub(base).String()
		if !r.Through.IsZero() {
			parts[i] += ".." + r.Through.Sub(base).String()
		}
	}
	return strings.Join(parts, " ")
}

// heartbeats returns the heartbeats of one subject at each step from first up to, and not
// including, end.
func heartbeats(t testing.TB, fp identity.Fingerprint, ttl, first, step, end time.Duration) []store.Record {
	t.Helper()
	var rs []store.Record
	for d := first; d < end; d += step {
		rs = append(rs, heartbeat(t, fp, producer, d, ttl))
	}
	return rs
}

// feedAll gives the records to the coalescer in order and returns what it wrote.
func feedAll(co *coalesce.Coalescer, rs []store.Record) []store.Record {
	var out []store.Record
	for _, r := range rs {
		out = co.Add(r, out)
	}
	return out
}

// T1: at a steady cadence, every second refresh starts a new run and none extends one.
func TestWithTheAgeFromTheTTLEverySecondRefreshStartsARunAndNoRefreshExtendsOne(t *testing.T) {
	t.Parallel()

	co := mustNew(t, ageFromTTL())
	got := feedAll(co, heartbeats(t, node(t, "node-0"), 15*time.Minute, 0, 5*time.Minute, time.Hour))
	if want := "0s 10m0s 20m0s 30m0s 40m0s 50m0s"; spans(got) != want {
		t.Errorf("wrote %s, want %s", spans(got), want)
	}
	for _, r := range got {
		if !r.Through.IsZero() {
			t.Errorf("the record at %s carries a Through: at a steady cadence no refresh extends a run bounded by its extension interval", r.EventTime.Sub(base))
		}
	}
	if co.Runs() != 1 {
		t.Errorf("Runs = %d, want 1", co.Runs())
	}
}

// T2: a refresh that is not on the cadence. The bound is the run's own extension
// interval, not a number that happens to be longer: with RunMaxAge of ten minutes the
// refresh at eight minutes extends the run, which is why the option exists.
func TestJitterDoesNotLetARunBeExtendedWhenItsAgeIsBoundedByItsTTL(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	beats := func(ds ...time.Duration) []store.Record {
		var rs []store.Record
		for _, d := range ds {
			rs = append(rs, heartbeat(t, fp, producer, d*time.Minute, 15*time.Minute))
		}
		return rs
	}

	t.Run("the age from the TTL", func(t *testing.T) {
		t.Parallel()

		got := feedAll(mustNew(t, ageFromTTL()), beats(0, 4, 8))
		// The refresh at 8 minutes is older than 7.5: a new run at its own time.
		if want := "0s 8m0s"; spans(got) != want {
			t.Errorf("wrote %s, want %s", spans(got), want)
		}
	})
	t.Run("a bound of ten minutes", func(t *testing.T) {
		t.Parallel()

		cfg := coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute}
		got := feedAll(mustNew(t, cfg), beats(0, 4, 8))
		if want := "0s 0s..8m0s"; spans(got) != want {
			t.Errorf("wrote %s, want %s", spans(got), want)
		}
		// The bound is in force: the refresh at 12 minutes is older than ten.
		got = feedAll(mustNew(t, cfg), beats(0, 4, 8, 12))
		if want := "0s 0s..8m0s 12m0s"; spans(got) != want {
			t.Errorf("with a refresh at 12 minutes wrote %s, want %s", spans(got), want)
		}
	})
	t.Run("the age from the TTL, with a refresh at 12 minutes", func(t *testing.T) {
		t.Parallel()

		// The run begun at 8 is 4 minutes old at 12: absorbed.
		got := feedAll(mustNew(t, ageFromTTL()), beats(0, 4, 8, 12))
		if want := "0s 8m0s"; spans(got) != want {
			t.Errorf("wrote %s, want %s", spans(got), want)
		}
	})
}

// T3: the bound follows the TTL, so a producer of any cadence, at a steady cadence, gets
// a new run every two refreshes, and no record carries a Through.
func TestTheAgeFromTheTTLStartsARunEveryTwoRefreshesAtEachCadence(t *testing.T) {
	t.Parallel()

	const span = 2 * time.Hour
	tests := map[string]struct {
		cadence time.Duration
		every   time.Duration // between the starts of runs
		runs    int
	}{
		"5 minutes (bound 7.5 minutes)":   {5 * time.Minute, 10 * time.Minute, 12},
		"15 minutes (bound 22.5 minutes)": {15 * time.Minute, 30 * time.Minute, 4},
		"60 seconds (bound 90 seconds)":   {time.Minute, 2 * time.Minute, 60},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, ageFromTTL())
			got := feedAll(co, heartbeats(t, node(t, "node-0"), 3*tt.cadence, 0, tt.cadence, span))
			if len(got) != tt.runs {
				t.Fatalf("wrote %d records over %s, want %d: %s", len(got), span, tt.runs, spans(got))
			}
			for i, r := range got {
				if want := time.Duration(i) * tt.every; r.EventTime.Sub(base) != want || !r.Through.IsZero() {
					t.Errorf("record %d is %s, want a run at %s with no Through", i, spans([]store.Record{r}), want)
				}
			}
		})
	}
}

// With a missed pull, the stored copy of a run lapses before the refresh that starts the
// next run: the refresh at 7 minutes is absorbed, the one at 16 is past the bound and past
// the stored copy (0 plus the TTL of 15), so the run is closed first, by the record that
// carries a Through, as any run is. No refresh extends the run.
func TestWithTheAgeFromTheTTLARunWhoseStoredCopyLapsedIsClosedWithAThrough(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	var rs []store.Record
	for _, d := range []time.Duration{0, 7 * time.Minute, 16 * time.Minute} {
		rs = append(rs, heartbeat(t, fp, producer, d, 15*time.Minute))
	}
	got := feedAll(mustNew(t, ageFromTTL()), rs)
	if want := "0s 0s..7m0s 16m0s"; spans(got) != want {
		t.Fatalf("wrote %s, want %s", spans(got), want)
	}
	for i, r := range got {
		r.Seq = uint64(i + 1)
		if err := r.Validate(); err != nil {
			t.Errorf("record %d is invalid: %v", i, err)
		}
	}
}

// T4: runs are kept per subject and producer, so refreshes of others in between change
// nothing about a run.
func TestTheAgeFromTheTTLIsKeptPerSubjectAndProducer(t *testing.T) {
	t.Parallel()

	const ttl = 15 * time.Minute
	n0, n1 := node(t, "node-0"), node(t, "node-1")
	keys := []struct {
		fp     identity.Fingerprint
		p      lifecycle.Producer
		offset time.Duration // where its cadence starts, so the keys do not beat together
	}{
		{n0, "collector-a", 0},
		{n0, "collector-b", time.Minute},     // two producers of one subject
		{n1, "collector-a", 2 * time.Minute}, // two subjects of one producer
	}
	var stream []store.Record
	alone := make([][]store.Record, len(keys))
	for step := range 24 { // 24 rounds of 5 minutes: 2 hours
		for i, k := range keys {
			r := heartbeat(t, k.fp, k.p, k.offset+time.Duration(step)*5*time.Minute, ttl)
			stream = append(stream, r)
			alone[i] = append(alone[i], r)
		}
	}

	co := mustNew(t, ageFromTTL())
	byKey := map[int][]store.Record{}
	for _, r := range stream {
		for _, w := range co.Add(r, nil) {
			for i, k := range keys {
				if w.Subject == store.EntitySubject(k.fp) && w.Producer == k.p {
					byKey[i] = append(byKey[i], w)
				}
			}
		}
	}
	if co.Runs() != len(keys) {
		t.Errorf("Runs = %d, want %d", co.Runs(), len(keys))
	}
	for i, k := range keys {
		want := feedAll(mustNew(t, ageFromTTL()), alone[i])
		if got := spans(byKey[i]); got != spans(want) {
			t.Errorf("%s of subject %d, interleaved, wrote %s; alone it writes %s", k.p, i, got, spans(want))
		}
		if len(want) != 12 {
			t.Errorf("%s of subject %d alone wrote %d records, want a run every 10 minutes (12)", k.p, i, len(want))
		}
		for j, w := range byKey[i] {
			if wantAt := k.offset + time.Duration(j)*10*time.Minute; w.EventTime.Sub(base) != wantAt || !w.Through.IsZero() {
				t.Errorf("%s of subject %d: record %d is %s, want a run at %s", k.p, i, j, spans([]store.Record{w}), wantAt)
			}
		}
	}
}

// T5: a record with no TTL forms no run.
func TestWithTheAgeFromTheTTLARecordWithNoTTLFormsNoRun(t *testing.T) {
	t.Parallel()

	co := mustNew(t, ageFromTTL())
	fp := node(t, "node-0")
	var rs, got []store.Record
	for i := range 6 {
		r := heartbeat(t, fp, producer, time.Duration(i)*5*time.Minute, 0)
		rs = append(rs, r)
		got = co.Add(r, got)
		if co.Runs() != 0 {
			t.Fatalf("Runs = %d after a record with no TTL at %s, want 0", co.Runs(), r.EventTime.Sub(base))
		}
	}
	if !reflect.DeepEqual(got, rs) {
		t.Errorf("wrote %s, want the records as given: %s", spans(got), spans(rs))
	}
}

// The option is off unless it is set, and a coalescer without it is the one before.
func TestTheAgeFromTheTTLIsOffByDefault(t *testing.T) {
	t.Parallel()

	if coalesce.DefaultConfig().RunAgeFromTTL {
		t.Error("DefaultConfig has RunAgeFromTTL on")
	}
	// With the default, a 15-minute TTL's runs are bound at 30 minutes, not 7.5.
	got := feedAll(mustNew(t, coalesce.DefaultConfig()), heartbeats(t, node(t, "node-0"), 15*time.Minute, 0, 5*time.Minute, time.Hour))
	if want := "0s 0s..10m0s 0s..20m0s 30m0s 30m0s..40m0s 30m0s..50m0s"; spans(got) != want {
		t.Errorf("wrote %s, want %s", spans(got), want)
	}
}

// T6: a stream folds the same when coalesced with the age from the TTL, as it does for
// the other settings: existence ends early by less than the extension interval and has no
// hole, the boots match, and a clone collision is still a collision.
func TestWithTheAgeFromTheTTLTheCoalescedStreamStillFoldsLikeTheRawOne(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		c, stream := drawStream(t)
		cfg := coalesce.Config{RunAgeFromTTL: true}
		if rapid.Bool().Draw(t, "fixedInterval") {
			cfg.ExtendEvery = time.Duration(rapid.IntRange(1, 600).Draw(t, "everySeconds")) * time.Second
		} else {
			cfg.ExtendTTLFraction = rapid.SampledFrom([]float64{0.25, 0.5, 1}).Draw(t, "fraction")
		}
		ret := drawRetention(t, c)
		check(t, cfg, false, feed(t, cfg, stream, ret), ret, new(tally))
	})
}

// The same on the fixed stream, whose reach does not depend on how many cases rapid
// runs: hosts that reboot, clones, late refreshes and a retention, with runs that are
// absorbed and runs that are started by the bound.
func TestWithTheAgeFromTheTTLTheFixedStreamFoldsLikeTheRawOne(t *testing.T) {
	t.Parallel()

	c, stream := fixedStream(t)
	cfg := ageFromTTL()
	var tl tally
	ret := retention{set: true, all: true, at: c.Start.Add(c.Duration / 3)}
	for _, r := range []retention{{}, ret} {
		o := feed(t, cfg, stream, r)
		check(t, cfg, false, o, r, &tl)
		if o.absorbed == 0 || o.continuations == 0 {
			t.Errorf("%s: %d refreshes absorbed and %d runs continued: the stream shows nothing of the bound", r, o.absorbed, o.continuations)
		}
		if !r.set && o.extensions != 0 {
			t.Errorf("%d records carry a Through with no retention: no refresh extends a run bounded by its extension interval, and this stream never lets a stored copy lapse", o.extensions)
		}
		varied := feed(t, cfg, varyBasis(stream), r)
		check(t, cfg, false, varied, r, new(tally))
	}
	if tl.rebootsSeen == 0 || tl.collisions == 0 {
		t.Errorf("%d second boots and %d clone collisions in a stream asked to have both", tl.rebootsSeen, tl.collisions)
	}
}

// T8.
func TestAgeFromTheTTLIsValidatedAgainstTheOtherSettings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config coalesce.Config
		valid  bool
	}{
		"with a fraction":                    {coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}, true},
		"with the whole TTL":                 {coalesce.Config{ExtendTTLFraction: 1, RunAgeFromTTL: true}, true},
		"with a fixed interval":              {coalesce.Config{ExtendEvery: time.Minute, RunAgeFromTTL: true}, true},
		"with a bound":                       {coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: time.Hour, RunAgeFromTTL: true}, false},
		"with a bound and no interval":       {coalesce.Config{RunMaxAge: time.Hour, RunAgeFromTTL: true}, false},
		"with no extension interval":         {coalesce.Config{RunAgeFromTTL: true}, false},
		"with no interval, from the default": {coalesce.Config{RunAgeFromTTL: true, ExtendTTLFraction: 0, ExtendEvery: 0}, false},
		"off, with no interval and a bound":  {coalesce.Config{RunMaxAge: time.Hour}, true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			_, newErr := coalesce.New(tt.config)
			if tt.valid {
				if err != nil || newErr != nil {
					t.Fatalf("Validate = %v, New = %v; want a valid config", err, newErr)
				}
				return
			}
			if !errors.Is(err, coalesce.ErrConfig) || !errors.Is(newErr, coalesce.ErrConfig) {
				t.Fatalf("Validate = %v, New = %v; want both to wrap ErrConfig", err, newErr)
			}
		})
	}
}

func TestDescribeNamesTheAgeFromTheTTLOnlyWhenItIsOn(t *testing.T) {
	t.Parallel()

	on := ageFromTTL().Describe()
	want := map[string]string{
		"extend_ttl_fraction": "0.5", "extend_every": "0s", "run_max_age": "0s", "run_age_from_ttl": "true",
	}
	if !maps.Equal(on, want) {
		t.Errorf("Describe = %v, want %v", on, want)
	}
	if _, ok := coalesce.DefaultConfig().Describe()["run_age_from_ttl"]; ok {
		t.Error("Describe of the default names run_age_from_ttl")
	}
}

// The horizon.

// throughRecord is a record of a subject that already stands for a span, from start to
// through, with every field set that a copy could lose.
func throughRecord(t testing.TB, fp identity.Fingerprint, start, through time.Time) store.Record {
	t.Helper()
	return store.Record{
		Layer: layerOf(t, fp), Subject: store.EntitySubject(fp), Producer: producer,
		EventTime: start, Through: through, Kind: lifecycle.Observe, TTL: testTTL,
		Payload: []byte("x"), Boot: "b1", EventTimeBasis: store.BasisObjectField,
	}
}

// H1: it starts before the horizon and reaches it, so it starts at the horizon and keeps
// its Through, and still ends the run it was a record of.
func TestARecordWithAThroughThatStartsBeforeTheHorizonIsRetimedToIt(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	h := at(2 * time.Hour)
	co := mustNew(t, coalesce.Config{})
	co.SetHorizon(layerOf(t, fp), h)
	if out := co.Add(refresh(t, fp, 7300, "a"), nil); len(out) != 1 || co.Runs() != 1 {
		t.Fatalf("setting up a run wrote%s with %d runs", describe(out), co.Runs())
	}

	in := throughRecord(t, fp, h.Add(-time.Hour), h.Add(5*time.Minute))
	want := in
	want.EventTime = h
	out := co.Add(in, nil)
	if len(out) != 1 || !reflect.DeepEqual(out[0], want) {
		t.Errorf("wrote %+v, want one record %+v", out, want)
	}
	if co.Runs() != 0 {
		t.Errorf("Runs = %d, want the run of the key ended", co.Runs())
	}
}

// H2: a Through at the horizon is not before it.
func TestARecordWithAThroughAtTheHorizonIsRetimedToIt(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	h := at(time.Hour)
	co := mustNew(t, coalesce.Config{})
	co.SetHorizon(layerOf(t, fp), h)

	in := throughRecord(t, fp, h.Add(-time.Second), h)
	want := in
	want.EventTime = h
	out := co.Add(in, nil)
	if len(out) != 1 || !reflect.DeepEqual(out[0], want) {
		t.Errorf("wrote %+v, want one record %+v", out, want)
	}
	if out[0].Through != h {
		t.Errorf("the Through became %v, want the horizon %v", out[0].Through, h)
	}
}

// H3: a record wholly before the horizon is passed on as it is, and the run of its key,
// which the store still holds, goes on: the next refresh extends it.
func TestARecordWithAThroughWhollyBeforeTheHorizonIsPassedOnAndLeavesTheRunAlone(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	co := mustNew(t, coalesce.Config{})
	co.SetHorizon(layerOf(t, fp), offset(1000))

	got := co.Add(refresh(t, fp, 1200, "a"), nil)
	in := throughRecord(t, fp, offset(100), offset(999))
	late := co.Add(in, nil)
	if len(late) != 1 || !reflect.DeepEqual(late[0], in) {
		t.Errorf("wrote %+v, want the record as given %+v", late, in)
	}
	if co.Runs() != 1 {
		t.Errorf("Runs = %d after the record, want the run of the key to stay", co.Runs())
	}
	got = co.Add(refresh(t, fp, 1260, "a"), got)
	if want := " [1200 a] [1200..1260 a]"; describe(got) != want {
		t.Errorf("the refresh after it wrote%s, want%s", describe(got), want)
	}

	// The input is not touched.
	if !in.EventTime.Equal(offset(100)) || !in.Through.Equal(offset(999)) {
		t.Errorf("the caller's record became %v..%v", in.EventTime.Sub(base), in.Through.Sub(base))
	}
}

// H4: the horizon of the record's own layer is the one that counts.
func TestARecordWithAThroughIsRetimedOnlyByTheHorizonOfItsOwnLayer(t *testing.T) {
	t.Parallel()

	n, h := node(t, "node-0"), host(t, "host-0") // layers L2 and L1
	if layerOf(t, n) == layerOf(t, h) {
		t.Fatalf("both entities are in layer %s: the test shows nothing", layerOf(t, n))
	}
	horizon := at(time.Hour)
	for name, tt := range map[string]struct{ set, record identity.Fingerprint }{
		"a horizon on L2, a record in L1": {n, h},
		"a horizon on L1, a record in L2": {h, n},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, coalesce.Config{})
			co.SetHorizon(layerOf(t, tt.set), horizon)
			for _, through := range []time.Time{horizon.Add(5 * time.Minute), horizon.Add(-time.Minute)} {
				in := throughRecord(t, tt.record, horizon.Add(-time.Hour), through)
				out := co.Add(in, nil)
				if len(out) != 1 || !reflect.DeepEqual(out[0], in) {
					t.Errorf("with a Through at %v wrote %+v, want the record as given %+v", through.Sub(base), out, in)
				}
			}
		})
	}
}

// A record that starts at the horizon or later, or with no horizon told, is as given; and
// so is one with no Through, a delete or a record with no TTL, before the horizon: those
// are for the late path to refuse.
func TestOnlyARecordWithAThroughBeforeTheHorizonIsChangedByIt(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	h := at(time.Hour)
	del := heartbeat(t, fp, producer, 10*time.Minute, 0)
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	noTTL := heartbeat(t, fp, producer, 10*time.Minute, 0)
	for name, tt := range map[string]struct {
		horizon time.Time
		in      store.Record
	}{
		"no horizon told":          {time.Time{}, throughRecord(t, fp, h.Add(-time.Hour), h.Add(time.Minute))},
		"it starts at the horizon": {h, throughRecord(t, fp, h, h.Add(time.Minute))},
		"it starts after":          {h, throughRecord(t, fp, h.Add(time.Minute), h.Add(2*time.Minute))},
		"a delete before":          {h, del},
		"a record with no TTL":     {h, noTTL},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, coalesce.Config{})
			if !tt.horizon.IsZero() {
				co.SetHorizon(layerOf(t, fp), tt.horizon)
			}
			out := co.Add(tt.in, nil)
			if len(out) != 1 || !reflect.DeepEqual(out[0], tt.in) {
				t.Errorf("wrote %+v, want the record as given %+v", out, tt.in)
			}
			if co.Runs() != 0 {
				t.Errorf("Runs = %d, want 0", co.Runs())
			}
		})
	}
}
