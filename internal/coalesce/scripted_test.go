package coalesce_test

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// A coalesced stream may end a run's existence early (that is what a bounded
// extension interval trades away) but may never open a hole inside an interval of
// existence the producer's refreshes covered: a hole is a death and a rebirth that
// nothing happened, and every layout would store it as a version.
//
// These tests drive the coalescer with refreshes written out by hand, so each
// shape is exact: the refresh that continues a run after a retention, the one that
// changes the description, and the ones that follow a real silence.

const (
	testTTL  = 240 * time.Second
	producer = lifecycle.Producer("node-collector")
)

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// offset is the instant s seconds after the start of the stream.
func offset(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }

func fingerprint(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, value string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: value}})
	if err != nil {
		t.Fatalf("resolving %s %q: %v", typ, value, err)
	}
	return id.Fingerprint()
}

func node(t testing.TB, name string) identity.Fingerprint {
	t.Helper()
	return fingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, name)
}

func host(t testing.TB, name string) identity.Fingerprint {
	t.Helper()
	return fingerprint(t, catalog.Host, catalog.HostID, name)
}

func layerOf(t testing.TB, fp identity.Fingerprint) catalog.Layer {
	t.Helper()
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		t.Fatalf("unknown entity type %s", fp.Type())
	}
	return e.Layer()
}

// refresh is the observation of an entity by the node collector, s seconds into the
// stream, with the test TTL.
func refresh(t testing.TB, fp identity.Fingerprint, s int, payload string) store.Record {
	t.Helper()
	return store.Record{
		Layer: layerOf(t, fp), Subject: store.EntitySubject(fp), Producer: producer,
		EventTime: offset(s), Kind: lifecycle.Observe, TTL: testTTL, Payload: []byte(payload),
	}
}

// bootRefresh is refresh of a host that reports a boot ID.
func bootRefresh(t testing.TB, fp identity.Fingerprint, s int, payload, boot string) store.Record {
	t.Helper()
	r := refresh(t, fp, s, payload)
	r.Boot = boot
	return r
}

func mustNew(t testing.TB, c coalesce.Config) *coalesce.Coalescer {
	t.Helper()
	co, err := coalesce.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return co
}

// beat is a refresh offset seconds after the start of the stream, with a payload.
type beat struct {
	at      int
	payload string
}

// everyMinute is a refresh every 60 s from 0 to the offset.
func everyMinute(to int) []beat {
	return every(0, to, 60, "a")
}

// every is a beat each step seconds from first to last, with the payload.
func every(first, last, step int, payload string) []beat {
	var out []beat
	for at := first; at <= last; at += step {
		out = append(out, beat{at, payload})
	}
	return out
}

// lateBeats is a refresh every minute to 720 s, then one that arrives late at the
// given offset, then every minute to 1200 s.
func lateBeats(late int) []beat {
	beats := every(0, 720, 60, "a")
	beats = append(beats, beat{late, "a"})
	return append(beats, every(780, 1200, 60, "a")...)
}

// scenario feeds refreshes of one node to a coalescer in order, telling it a retention
// horizon after the refresh at the key's offset, and returns the raw assertions and
// what the coalescer wrote.
type scenario struct {
	fraction float64
	every    time.Duration // ExtendEvery, instead of a fraction
	maxAge   time.Duration // RunMaxAge
	beats    []beat
	// horizonAfter maps the offset of a refresh to the horizon (as an offset) the
	// store has retained to once that refresh has been seen.
	horizonAfter map[int]int
}

func (s scenario) run(t *testing.T) (raw, got []store.Record) {
	t.Helper()

	co := mustNew(t, coalesce.Config{ExtendTTLFraction: s.fraction, ExtendEvery: s.every, RunMaxAge: s.maxAge})
	fp := node(t, "node-0")
	for _, b := range s.beats {
		r := refresh(t, fp, b.at, b.payload)
		r.Seq = uint64(len(raw) + 1)
		raw = append(raw, r)
		for _, o := range co.Add(r, nil) {
			if o.Seq != 0 {
				t.Fatalf("the coalescer returned %s with Seq %d, want 0", describe([]store.Record{o}), o.Seq)
			}
			o.Seq = uint64(len(got) + 1)
			if err := o.Validate(); err != nil {
				t.Fatalf("the coalescer wrote an invalid record %s: %v", describe([]store.Record{o}), err)
			}
			if h := co.Horizon(o.Layer); o.EventTime.Before(h) && !r.EventTime.Before(h) {
				t.Fatalf("the coalescer wrote %s before the horizon %v for a refresh after it", describe([]store.Record{o}), h.Sub(base))
			}
			got = append(got, o)
		}
		if h, ok := s.horizonAfter[b.at]; ok {
			co.SetHorizonAll(offset(h))
		}
	}
	return raw, got
}

// describe prints records as [start..through payload], with the TTL when it is not
// the test one and the boot when there is one.
func describe(rs []store.Record) string {
	var out string
	for _, r := range rs {
		through := ""
		if !r.Through.IsZero() {
			through = fmt.Sprintf("..%d", int(r.Through.Sub(base)/time.Second))
		}
		extra := ""
		if r.TTL != testTTL {
			extra += fmt.Sprintf(" ttl=%s", r.TTL)
		}
		if r.Boot != "" {
			extra += " boot=" + r.Boot
		}
		if r.EventTimeBasis != store.BasisUnknown {
			extra += " basis=" + r.EventTimeBasis.String()
		}
		out += fmt.Sprintf(" [%d%s %s%s]", int(r.EventTime.Sub(base)/time.Second), through, r.Payload, extra)
	}
	return out
}

// timeline folds records and reports whether the subject exists at an instant.
func timeline(t testing.TB, rs []store.Record, p lifecycle.Policy) lifecycle.Timeline {
	t.Helper()
	as := make([]lifecycle.Assertion, len(rs))
	for i, r := range rs {
		as[i] = r.Assertion()
	}
	tl, err := lifecycle.Fold(as, p)
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

// holes lists the instants, on a grid, from `from` to the end where the subject
// exists in the raw stream and not in the coalesced one, and exists in it again
// later within the same interval of the raw stream; and the instants where the
// coalesced stream says it exists and the raw one does not.
func holes(t testing.TB, raw, got []store.Record, from, to int) (inside, late, differ []int) {
	t.Helper()
	tr, tg := timeline(t, raw, lifecycle.Policy{}), timeline(t, got, lifecycle.Policy{})
	missing := false
	for x := from; x < to; x += 5 {
		a, b := tr.AliveAt(offset(x)), tg.AliveAt(offset(x))
		switch {
		case !a:
			missing = false
			if b {
				late = append(late, x)
			}
		case !b:
			missing = true
		case missing:
			inside = append(inside, x)
			missing = false
		}
		if a && b {
			da, _ := tr.DescribeAt(offset(x))
			db, _ := tg.DescribeAt(offset(x))
			if !maps.Equal(da, db) {
				differ = append(differ, x)
			}
		}
	}
	return inside, late, differ
}

func TestACoalescedRunHasNoHoleWhereTheProducerHadNone(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		scenario
		from, to int
		// want is what the coalescer writes, as describe prints it: [start..through payload].
		want string
	}{
		// The refresh that follows a retention is a beat late: the run was last
		// written at its start, the refreshes at 60, 120 and 180 were absorbed, and
		// the stored deadline (240) passes before the refresh at 300 starts the run
		// that continues it. The producer was seen every 60 s and then at 300, always
		// within its TTL.
		"continued after a retention, a beat late": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "a"}},
			},
			from: 200, to: 560, want: " [0 a] [200 a] [300 a]",
		},
		// The same with the horizon behind the run's last extension, so that the
		// run can still be extended at its start and it is the description that
		// changes.
		"description changes a beat late": {
			scenario: scenario{
				fraction: 0.9,
				beats:    []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "b"}},
			},
			from: 0, to: 560, want: " [0 a] [0..180 a] [300 b]",
		},
		// The description changes after a retention that the run began before.
		"description changes a beat late after a retention": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "b"}},
			},
			from: 200, to: 560, want: " [0 a] [200 a] [300 b]",
		},
		// The retention is behind the last refresh, so the run goes on from that
		// refresh and not from the horizon.
		"continued after a retention, from the last refresh": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 150},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "a"}},
			},
			from: 150, to: 560, want: " [0 a] [180 a] [300 a]",
		},
		// An extension interval longer than the TTL lets the stored deadline pass
		// before the last refresh; the run goes on from the lapse.
		"stored deadline lapsed before the last refresh": {
			scenario: scenario{
				every: 400 * time.Second, horizonAfter: map[int]int{360: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {240, "a"}, {300, "a"}, {360, "a"}, {420, "a"}},
			},
			from: 200, to: 700, want: " [0 a] [240 a] [420 a]",
		},
		// A refresh older than the horizon is one the store refuses: nothing is
		// written to carry the run to it.
		"a refresh from before the horizon": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {190, "a"}},
			},
			from: 200, to: 440, want: " [0 a] [190 a]",
		},
		// The refresh comes after the stored deadline but before the horizon: the
		// store would refuse it, and a record at the horizon to carry the run there
		// would promise existence for longer than the producer did.
		"a refresh after the lapse and before the horizon": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 260},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {250, "a"}},
			},
			from: 260, to: 560, want: " [0 a] [250 a]",
		},
		// The run has been extended, so its stored deadline is later than the
		// first record's would give: the description changes within it, and
		// nothing is written but the extension and the new description.
		"description changes inside the stored deadline": {
			scenario: scenario{
				fraction: 0.5,
				beats:    []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "b"}},
			},
			from: 0, to: 560, want: " [0 a] [0..120 a] [300 b]",
		},
		// A refresh that arrives late, after a retention made its run start again, is
		// older than the new run and inside the span the old one covered: the old run
		// already stands for it, and passing it through as an assertion of its own
		// would replace the old run's deadline from its instant on, and open a hole
		// before the new run starts.
		"a late refresh the previous run covers": {
			scenario: scenario{
				fraction: 0.5, horizonAfter: map[int]int{600: 300},
				beats: lateBeats(400),
			},
			from: 300, to: 1500,
			want: " [0 a] [0..120 a] [0..240 a] [0..360 a] [0..480 a] [0..600 a] [660 a] [660..780 a] [660..900 a] [660..1020 a] [660..1140 a]",
		},
		// A late refresh after the previous run's last one is not covered by it, and
		// is passed through as before: it is a refresh the stream has not seen.
		"a late refresh after the previous run's last": {
			scenario: scenario{
				fraction: 0.5, horizonAfter: map[int]int{600: 300},
				beats: lateBeats(620),
			},
			from: 300, to: 1500,
			want: " [0 a] [0..120 a] [0..240 a] [0..360 a] [0..480 a] [0..600 a] [660 a] [620 a] [660..780 a] [660..900 a] [660..1020 a] [660..1140 a]",
		},
		// A run that has reached its greatest age is continued by a new run at the
		// refresh that finds it so, with the same description: one version more every
		// 1200 s, and no hole, since the stored deadline of the old run reaches the new
		// one's start.
		"a run at its greatest age": {
			scenario: scenario{
				fraction: 0.5, maxAge: 1200 * time.Second,
				beats: everyMinute(3000),
			},
			from: 0, to: 3300,
			want: " [0 a] [0..120 a] [0..240 a] [0..360 a] [0..480 a] [0..600 a] [0..720 a] [0..840 a] [0..960 a] [0..1080 a] [1200 a] [1200..1320 a] [1200..1440 a] [1200..1560 a] [1200..1680 a] [1200..1800 a] [1200..1920 a] [1200..2040 a] [1200..2160 a] [1200..2280 a] [2400 a] [2400..2520 a] [2400..2640 a] [2400..2760 a] [2400..2880 a] [2400..3000 a]",
		},
		// With an extension interval that leaves the stored deadline short of the next
		// run, the old run is carried to it first.
		"a run at its greatest age, with a short stored deadline": {
			scenario: scenario{
				fraction: 0.9, maxAge: 1380 * time.Second,
				beats: append(everyMinute(1140), beat{1380, "a"}),
			},
			from: 0, to: 1800,
			want: " [0 a] [0..240 a] [0..480 a] [0..720 a] [0..960 a] [0..1140 a] [1380 a]",
		},
		// A silence longer than the TTL is a real gap: the run ends at its stored
		// deadline, before the producer's own (that is the bounded extension's loss),
		// and nothing is written to cover it. Two records, as before.
		"a real silence is not covered": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {500, "a"}},
			},
			from: 200, to: 760, want: " [0 a] [500 a]",
		},
		// A refresh of the description before the current one arrives late, after the
		// stream moved on to another. The run before covers it, so it is absorbed, and
		// the current run goes on to the end: it is not replaced by a run in the past,
		// which would leave the next refresh of the current description to start a new
		// run without closing this one (a hole from 1380 to 1400).
		"a late refresh of the previous description": {
			scenario: scenario{
				fraction: 0.9,
				beats:    append(append([]beat{{0, "a"}, {60, "a"}, {120, "a"}}, every(180, 1200, 60, "b")...), beat{90, "a"}, beat{1400, "b"}, beat{1460, "b"}),
			},
			from: 0, to: 1700, want: " [0 a] [180 b] [180..420 b] [180..660 b] [180..900 b] [180..1140 b] [180..1400 b]",
		},
		// The stored deadline already covers the new run's start: nothing to add.
		"continued with no lag": {
			scenario: scenario{
				fraction: 0.9, horizonAfter: map[int]int{0: 30},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}},
			},
			from: 30, to: 400, want: " [0 a] [60 a]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			raw, got := tc.run(t)
			inside, late, differ := holes(t, raw, got, tc.from, tc.to)
			if len(inside) > 0 {
				t.Errorf("the coalesced stream has the subject missing at +%vs and existing again after it, which the raw stream does not (%d records for %d refreshes: %s)",
					inside, len(got), len(raw), describe(got))
			}
			if len(late) > 0 {
				t.Errorf("the coalesced stream has the subject existing at +%vs, which the raw stream says it does not: existence may end early and never late", late)
			}
			if len(differ) > 0 {
				t.Errorf("the coalesced stream describes the subject differently from the raw one at +%vs: %s", differ, describe(got))
			}
			if d := describe(got); d != tc.want {
				t.Errorf("the coalescer wrote%s, want%s", d, tc.want)
			}
		})
	}
}

// A late refresh of the description the stream has moved to is not one the previous
// run stands for, even inside the span it covered (it described something else): it
// passes through as its own assertion.
func TestALateRefreshOfTheNewDescriptionIsNotAbsorbed(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5})
	fp := node(t, "node-0")
	var got []store.Record
	for _, b := range []beat{{0, "a"}, {60, "a"}, {120, "a"}, {600, "b"}, {90, "b"}} {
		got = co.Add(refresh(t, fp, b.at, b.payload), got)
	}
	if want := " [0 a] [0..120 a] [600 b] [90 b]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
}

// A late refresh at exactly the first or the last instant the previous run covers is
// inside it.
func TestALateRefreshAtTheEdgesOfThePreviousRunIsAbsorbed(t *testing.T) {
	t.Parallel()

	for _, late := range []int{0, 120} {
		co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5})
		fp := node(t, "node-0")
		var got []store.Record
		for _, at := range []int{0, 60, 120, 600, late} { // the run restarts at 600; the last is late
			got = co.Add(refresh(t, fp, at, "a"), got)
		}
		if want := " [0 a] [0..120 a] [600 a]"; describe(got) != want {
			t.Errorf("late refresh at %d: wrote%s, want%s", late, describe(got), want)
		}
	}
}

// A late refresh just outside the previous run's span is not inside it.
func TestALateRefreshJustOutsideThePreviousRunIsNotAbsorbed(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5})
	fp := node(t, "node-0")
	var got []store.Record
	for _, at := range []int{10, 70, 130, 600, 9, 131} {
		got = co.Add(refresh(t, fp, at, "a"), got)
	}
	// The run before the current one spans 10 to 130 (the last refresh it saw).
	if want := " [10 a] [10..130 a] [600 a] [9 a] [131 a]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
}

// storeStream feeds a heartbeat of each node every step seconds to a coalescer and returns
// what the store keeps, as a store that has retained to h from the first refresh at or
// after it does: an output before h, once it has retained, is refused. told says
// whether the coalescer is told the horizon.
func storeStream(t *testing.T, cfg coalesce.Config, nodes []identity.Fingerprint, step, to, h int, told bool) []store.Record {
	t.Helper()

	co := mustNew(t, cfg)
	var kept []store.Record
	retained := false
	for at := 0; at <= to; at += step {
		for i, fp := range nodes {
			r := refresh(t, fp, at+i, "a")
			for _, o := range co.Add(r, nil) {
				if retained && o.EventTime.Before(offset(h)) {
					continue // refused: before the horizon
				}
				o.Seq = uint64(len(kept) + 1)
				kept = append(kept, o)
			}
			if !retained && !r.EventTime.Before(offset(h)) {
				retained = true
				if told {
					co.SetHorizonAll(offset(h))
				}
			}
		}
	}
	return kept
}

func TestARunThatStartedBeforeTheHorizonIsContinuedNotExtended(t *testing.T) {
	t.Parallel()

	const h = 600
	fp := node(t, "node-0")
	extendedBefore := func(rs []store.Record) (n int) {
		for _, r := range rs {
			if !r.Through.IsZero() && r.EventTime.Before(offset(h)) {
				n++
			}
		}
		return n
	}

	// Without the horizon, every heartbeat of a run that began at the start of
	// the stream re-asserts it at that start: the store would refuse all of them.
	// What the coalescer offers, before the store refuses any:
	co := mustNew(t, coalesce.Config{})
	var offered []store.Record
	for at := 0; at <= 1200; at += 60 {
		if at > h {
			offered = co.Add(refresh(t, fp, at, "a"), offered)
		} else {
			co.Add(refresh(t, fp, at, "a"), nil)
		}
	}
	if extendedBefore(offered) == 0 {
		t.Fatal("without the horizon, no extension was offered at a start before it: the test shows nothing")
	}

	// Told the horizon, the coalescer never offers one.
	co = mustNew(t, coalesce.Config{})
	told, extended := false, 0
	for at := 0; at <= 1200; at += 60 {
		for _, o := range co.Add(refresh(t, fp, at, "a"), nil) {
			if told && !o.Through.IsZero() && o.EventTime.Before(offset(h)) {
				t.Fatalf("after SetHorizon(%v), an extension was offered at %v", offset(h), o.EventTime)
			}
			if told && !o.Through.IsZero() {
				extended++
			}
		}
		if !told && !offset(at).Before(offset(h)) {
			told = true
			co.SetHorizonAll(offset(h))
		}
	}
	if extended == 0 {
		t.Error("after the horizon no run was extended at all: new runs are not being extended either")
	}
}

func TestRetentionDoesNotKillAHeartbeatingRun(t *testing.T) {
	t.Parallel()

	const h, to, step = 600, 1200, 60
	nodes := []identity.Fingerprint{node(t, "node-0"), node(t, "node-1"), node(t, "node-2")}
	alive := func(rs []store.Record, fp identity.Fingerprint, at int) bool {
		var own []store.Record
		for _, r := range rs {
			if r.Subject == store.EntitySubject(fp) {
				own = append(own, r)
			}
		}
		return timeline(t, own, lifecycle.Policy{}).AliveAt(offset(at))
	}

	// every heartbeat a record of its own: nothing coalesced, nothing retained
	var reference []store.Record
	for at := 0; at <= to; at += step {
		for i, fp := range nodes {
			r := refresh(t, fp, at+i, "a")
			r.Seq = uint64(len(reference) + 1)
			reference = append(reference, r)
		}
	}
	probes := []int{h, h + 7*60, to - 1}

	for _, extendEvery := range []time.Duration{0, 2 * time.Minute} {
		cfg := coalesce.Config{ExtendEvery: extendEvery}
		toldStream := storeStream(t, cfg, nodes, step, to, h, true)
		untoldStream := storeStream(t, cfg, nodes, step, to, h, false)
		for _, at := range probes {
			killed := 0
			for _, fp := range nodes {
				want, told, untold := alive(reference, fp, at), alive(toldStream, fp, at), alive(untoldStream, fp, at)
				switch {
				case told && !want:
					t.Fatalf("ExtendEvery %s at +%ds: %s is alive after retention but not in the uncoalesced stream", extendEvery, at, fp)
				case extendEvery == 0 && want && !told:
					t.Fatalf("ExtendEvery 0 at +%ds: %s was alive and the retained, told stream says dead", at, fp)
				}
				if want && !untold {
					killed++
				}
			}
			// The stream that is not told the horizon loses every run that began
			// before it, so by the end of the stream they are dead.
			if at > h+5*60 && extendEvery == 0 && killed == 0 {
				t.Errorf("at +%ds no run was killed by retention without the horizon: the test shows nothing", at)
			}
		}
	}
}

func TestARefreshWithAnotherBootStartsANewRun(t *testing.T) {
	t.Parallel()

	fp := host(t, "host-0")
	tests := map[string]struct {
		config coalesce.Config
		beats  []store.Record
		want   string
	}{
		// The stored deadline of the first boot (60 + 240) reaches the refresh of the
		// second: nothing to add between them.
		"every refresh extends": {
			config: coalesce.Config{},
			beats: []store.Record{
				bootRefresh(t, fp, 0, "a", "b1"), bootRefresh(t, fp, 60, "a", "b1"),
				bootRefresh(t, fp, 120, "a", "b2"), bootRefresh(t, fp, 180, "a", "b2"),
			},
			want: " [0 a boot=b1] [0..60 a boot=b1] [120 a boot=b2] [120..180 a boot=b2]",
		},
		// Refreshes 60 to 180 were absorbed, and the stored deadline (240) falls short of
		// the reboot seen at 300: the old boot is carried to its last refresh, with its
		// own boot ID, before the new one begins.
		"the closing record carries the old boot": {
			config: coalesce.Config{ExtendTTLFraction: 0.9},
			beats: []store.Record{
				bootRefresh(t, fp, 0, "a", "b1"), bootRefresh(t, fp, 60, "a", "b1"), bootRefresh(t, fp, 120, "a", "b1"),
				bootRefresh(t, fp, 180, "a", "b1"), bootRefresh(t, fp, 300, "a", "b2"),
			},
			want: " [0 a boot=b1] [0..180 a boot=b1] [300 a boot=b2]",
		},
		// A host that does not report a boot, then does, is a different description.
		"a boot appears": {
			config: coalesce.Config{},
			beats: []store.Record{
				refresh(t, fp, 0, "a"), bootRefresh(t, fp, 60, "a", "b1"), bootRefresh(t, fp, 120, "a", "b1"),
			},
			want: " [0 a] [60 a boot=b1] [60..120 a boot=b1]",
		},
		// The same boot twice in a row is one run, and a late refresh of the old boot,
		// inside the span the previous run covered, is absorbed only for that boot.
		"a late refresh is covered only by its own boot": {
			config: coalesce.Config{},
			beats: []store.Record{
				bootRefresh(t, fp, 0, "a", "b1"), bootRefresh(t, fp, 60, "a", "b1"), bootRefresh(t, fp, 120, "a", "b2"),
				bootRefresh(t, fp, 30, "a", "b1"), bootRefresh(t, fp, 30, "a", "b3"),
			},
			want: " [0 a boot=b1] [0..60 a boot=b1] [120 a boot=b2] [30 a boot=b3]",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, tt.config)
			var got []store.Record
			for _, r := range tt.beats {
				got = co.Add(r, got)
			}
			if d := describe(got); d != tt.want {
				t.Errorf("wrote%s, want%s", d, tt.want)
			}
			for i, o := range got {
				o.Seq = uint64(i + 1)
				if err := o.Validate(); err != nil {
					t.Errorf("record %d is invalid: %v", i, err)
				}
			}
		})
	}
}

// withBasis is r with the basis of its event time.
func withBasis(r store.Record, b store.EventTimeBasis) store.Record {
	r.EventTimeBasis = b
	return r
}

func TestAnExtensionAndTheRecordThatClosesARunCarryTheBasisItBeganWith(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	const (
		receipt = store.BasisReceipt
		object  = store.BasisObjectField
	)
	tests := map[string]struct {
		config  coalesce.Config
		horizon int // told after the last beat but one, or 0
		beats   []store.Record
		want    string
	}{
		"an extension carries the basis of its run": {
			config: coalesce.Config{},
			beats: []store.Record{
				withBasis(refresh(t, fp, 0, "a"), receipt), withBasis(refresh(t, fp, 60, "a"), receipt),
				withBasis(refresh(t, fp, 120, "a"), receipt),
			},
			want: " [0 a basis=receipt] [0..60 a basis=receipt] [0..120 a basis=receipt]",
		},
		// An unknown basis is a basis too: the output has none to show.
		"the unknown basis is unchanged": {
			config: coalesce.Config{},
			beats:  []store.Record{refresh(t, fp, 0, "a"), refresh(t, fp, 60, "a")},
			want:   " [0 a] [0..60 a]",
		},
		// Refreshes 60 to 180 were absorbed; the stored deadline (240) falls short of the
		// refresh seen at 300, whose time came from elsewhere. The old run is carried to
		// its last refresh with its own basis, then the new run begins with its.
		"a change of basis starts a run and the closing record carries the old basis": {
			config: coalesce.Config{ExtendTTLFraction: 0.9},
			beats: []store.Record{
				withBasis(refresh(t, fp, 0, "a"), receipt), withBasis(refresh(t, fp, 60, "a"), receipt),
				withBasis(refresh(t, fp, 120, "a"), receipt), withBasis(refresh(t, fp, 180, "a"), receipt),
				withBasis(refresh(t, fp, 300, "a"), object),
			},
			want: " [0 a basis=receipt] [0..180 a basis=receipt] [300 a basis=object_field]",
		},
		"going from unknown to a basis starts a run": {
			config: coalesce.Config{},
			beats: []store.Record{
				refresh(t, fp, 0, "a"), withBasis(refresh(t, fp, 60, "a"), receipt), withBasis(refresh(t, fp, 120, "a"), receipt),
			},
			want: " [0 a] [60 a basis=receipt] [60..120 a basis=receipt]",
		},
		// A late refresh of the old run, inside the span the previous run covered, is
		// absorbed only with the basis that run had.
		"a late refresh is covered only by its own basis": {
			config: coalesce.Config{},
			beats: []store.Record{
				withBasis(refresh(t, fp, 0, "a"), receipt), withBasis(refresh(t, fp, 60, "a"), receipt),
				withBasis(refresh(t, fp, 120, "a"), object),
				withBasis(refresh(t, fp, 30, "a"), receipt), withBasis(refresh(t, fp, 30, "a"), store.BasisObserved),
			},
			want: " [0 a basis=receipt] [0..60 a basis=receipt] [120 a basis=object_field] [30 a basis=observed]",
		},
		// The run began before the horizon, so it cannot be extended: the record that
		// carries it to the new run begins at its last refresh, with its basis.
		"a run continued across the horizon keeps its basis": {
			config:  coalesce.Config{ExtendTTLFraction: 0.9},
			horizon: 100,
			beats: []store.Record{
				withBasis(refresh(t, fp, 0, "a"), receipt), withBasis(refresh(t, fp, 60, "a"), receipt),
				withBasis(refresh(t, fp, 120, "a"), receipt), withBasis(refresh(t, fp, 180, "a"), receipt),
				withBasis(refresh(t, fp, 300, "a"), object),
			},
			want: " [0 a basis=receipt] [180 a basis=receipt] [300 a basis=object_field]",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, tt.config)
			var got []store.Record
			for i, r := range tt.beats {
				if tt.horizon != 0 && i == len(tt.beats)-1 {
					co.SetHorizonAll(offset(tt.horizon))
				}
				got = co.Add(r, got)
			}
			if d := describe(got); d != tt.want {
				t.Errorf("wrote%s, want%s", d, tt.want)
			}
			for i, o := range got {
				o.Seq = uint64(i + 1)
				if err := o.Validate(); err != nil {
					t.Errorf("record %d is invalid: %v", i, err)
				}
			}
		})
	}
}

// A record that is passed on as given keeps its own basis, and a run forgotten by
// Prune leaves nothing of the basis it had.
func TestRecordsPassedOnAndPrunedRunsKeepNoBasisOfAnotherRun(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	co := mustNew(t, coalesce.Config{})
	var got []store.Record
	got = co.Add(withBasis(refresh(t, fp, 0, "a"), store.BasisReceipt), got)
	del := store.Record{
		Layer: layerOf(t, fp), Subject: store.EntitySubject(fp), Producer: producer, EventTime: offset(30),
		Kind: lifecycle.Delete, EventTimeBasis: store.BasisObserved,
	}
	got = co.Add(del, got)
	long := withBasis(refresh(t, fp, 40, "a"), store.BasisObjectField)
	long.Through = offset(100)
	got = co.Add(long, got)
	if want := " [0 a basis=receipt] [30  ttl=0s basis=observed] [40..100 a basis=object_field]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}

	got = co.Add(withBasis(refresh(t, fp, 200, "a"), store.BasisReceipt), nil)
	co.Prune(offset(1000))
	got = co.Add(withBasis(refresh(t, fp, 1100, "a"), store.BasisObjectField), got)
	if want := " [200 a basis=receipt] [1100 a basis=object_field]"; describe(got) != want {
		t.Errorf("after Prune wrote%s, want%s", describe(got), want)
	}
}

func TestAHorizonIsPerLayer(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	n, h := node(t, "node-0"), host(t, "host-0") // layers L2 and L1
	if layerOf(t, n) == layerOf(t, h) {
		t.Fatalf("both entities are in layer %s: the test shows nothing", layerOf(t, n))
	}
	var got []store.Record
	for _, fp := range []identity.Fingerprint{n, h} {
		for _, at := range []int{0, 60} {
			got = co.Add(refresh(t, fp, at, "a"), got)
		}
	}
	co.SetHorizon(layerOf(t, h), offset(100))
	for _, fp := range []identity.Fingerprint{n, h} {
		got = co.Add(refresh(t, fp, 120, "a"), got)
	}
	// The node's layer has no horizon, so its run is extended at its start; the
	// host's run began before the horizon of its layer, so it is continued.
	want := " [0 a] [0..60 a] [0 a] [0..60 a] [0..120 a] [120 a]"
	if d := describe(got); d != want {
		t.Errorf("wrote%s, want%s", d, want)
	}
	if got, want := co.Horizon(layerOf(t, h)), offset(100); !got.Equal(want) {
		t.Errorf("Horizon of the host layer = %v, want %v", got, want)
	}
	if got := co.Horizon(layerOf(t, n)); !got.IsZero() {
		t.Errorf("Horizon of the node layer = %v, want none", got)
	}
}

func TestSetHorizonOnlyMovesForward(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	fp := node(t, "node-0")
	l := layerOf(t, fp)
	co.SetHorizon(l, offset(100))
	co.SetHorizon(l, offset(50))
	co.SetHorizon(l, time.Time{})
	if got := co.Horizon(l); !got.Equal(offset(100)) {
		t.Fatalf("Horizon = %v after setting 100 s, then 50 s, then none; want 100 s", got.Sub(base))
	}
	// A run that began at 60 s, before the horizon of 100 s and after the earlier,
	// lower one, is continued.
	var got []store.Record
	for _, at := range []int{60, 120} {
		got = co.Add(refresh(t, fp, at, "a"), got)
	}
	if want := " [60 a] [120 a]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
	co.SetHorizon(l, offset(200))
	if got := co.Horizon(l); !got.Equal(offset(200)) {
		t.Errorf("Horizon = %v after moving forward to 200 s", got.Sub(base))
	}
}

func TestSetHorizonAllSetsEveryLayer(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	co.SetHorizonAll(offset(100))
	for l := catalog.L0; l <= catalog.L3; l++ {
		if got := co.Horizon(l); !got.Equal(offset(100)) {
			t.Errorf("Horizon(%s) = %v, want 100 s", l, got.Sub(base))
		}
	}
	co.SetHorizonAll(offset(40))
	co.SetHorizon(catalog.L2, offset(30))
	if got := co.Horizon(catalog.L2); !got.Equal(offset(100)) {
		t.Errorf("Horizon(L2) = %v after moving back, want 100 s", got.Sub(base))
	}
	// A layer the catalog does not have holds no horizon and takes none.
	co.SetHorizon(catalog.Layer(0), offset(500))
	co.SetHorizon(catalog.Layer(9), offset(500))
	for _, l := range []catalog.Layer{0, 9} {
		if got := co.Horizon(l); !got.IsZero() {
			t.Errorf("Horizon(%s) = %v, want none", l, got)
		}
	}
}

// payloadAddress is where the first byte of a payload lives, or nil for an empty one.
func payloadAddress(p []byte) *byte {
	if len(p) == 0 {
		return nil
	}
	return unsafe.SliceData(p)
}

func TestTheRecordThatClosesARunSharesNoPayloadBytes(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.9})
	fp := node(t, "node-0")
	var (
		got     []store.Record
		inputs  []*byte
		beats   = []beat{{0, "aaaa"}, {60, "aaaa"}, {120, "aaaa"}, {180, "aaaa"}, {300, "bbbb"}}
		scratch = make([]byte, 4) // a producer's buffer, rewritten for every refresh
	)
	for _, b := range beats {
		copy(scratch, b.payload)
		fresh := []byte(b.payload) // and the slice of a refresh that owns its bytes
		r := refresh(t, fp, b.at, "")
		r.Payload = scratch
		if b.at%120 == 0 {
			r.Payload = fresh
		}
		inputs = append(inputs, payloadAddress(r.Payload))
		got = co.Add(r, got)
	}
	if want := " [0 aaaa] [0..180 aaaa] [300 bbbb]"; describe(got) != want {
		t.Fatalf("wrote%s, want%s", describe(got), want)
	}
	closing := got[1]
	if closing.Through.IsZero() {
		t.Fatalf("record 1 is %s, want the extension that closes the run", describe(got[1:2]))
	}

	// Scribble over the producer's buffer: the records must not change.
	for i := range scratch {
		scratch[i] = 'z'
	}
	if string(closing.Payload) != "aaaa" {
		t.Errorf("the closing record's payload became %q when the buffer was reused", closing.Payload)
	}
	p := payloadAddress(closing.Payload)
	if p == nil {
		t.Fatal("the closing record has no payload")
	}
	for i, in := range inputs {
		if in == p {
			t.Errorf("the closing record shares its payload with the input of refresh %d", i)
		}
	}
	for i, o := range got {
		if i != 1 && payloadAddress(o.Payload) == p {
			t.Errorf("the closing record shares its payload with output %d", i)
		}
	}
	// What the caller does to the closing record's bytes is not done to the run it
	// closed, which still stands for a refresh that arrives late.
	for i := range closing.Payload {
		closing.Payload[i] = 'y'
	}
	if out := co.Add(refresh(t, fp, 100, "aaaa"), nil); len(out) != 0 {
		t.Errorf("a late refresh of the closed run wrote%s, want nothing: the run before covers it", describe(out))
	}
	// And the next refresh of the old description is not mistaken for a new one: the
	// coalescer kept what the run said, not the producer's buffer.
	later := refresh(t, fp, 540, "bbbb")
	if out := co.Add(later, nil); len(out) != 1 || out[0].Through.IsZero() {
		t.Errorf("a refresh of the current description wrote%s, want an extension", describe(out))
	}
}

func TestTheCoalescerDoesNotKeepTheProducersBuffer(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	fp := node(t, "node-0")
	buf := make([]byte, 1)
	var got []store.Record
	for _, b := range []beat{{0, "a"}, {60, "a"}, {120, "b"}, {180, "b"}} {
		buf[0] = b.payload[0]
		r := refresh(t, fp, b.at, "")
		r.Payload = buf
		n := len(got)
		got = co.Add(r, got)
		// What the coalescer returned for this refresh is used before the buffer is.
		for i := n; i < len(got); i++ {
			got[i].Payload = slices.Clone(got[i].Payload)
		}
	}
	if want := " [0 a] [0..60 a] [120 b] [120..180 b]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
}

func TestPruneForgetsTheRunsPastTheirDeadline(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	a, b, c := node(t, "node-a"), node(t, "node-b"), node(t, "node-c")
	for _, in := range []struct {
		fp identity.Fingerprint
		at int
	}{{a, 0}, {a, 60}, {b, 0}, {b, 300}, {c, 600}} {
		co.Add(refresh(t, in.fp, in.at, "a"), nil)
	}
	// The producer's deadlines: a 300, b 540, c 840.
	if got := co.Runs(); got != 3 {
		t.Fatalf("Runs = %d, want 3", got)
	}
	for _, step := range []struct{ now, forgot int }{
		{now: 300, forgot: 0},  // a's deadline is now, not before it
		{now: 301, forgot: 1},  // a
		{now: 301, forgot: 0},  // nothing more
		{now: 540, forgot: 0},  // b's deadline is now
		{now: 1000, forgot: 2}, // b and c
		{now: 2000, forgot: 0}, // none left
	} {
		if got := co.Prune(offset(step.now)); got != step.forgot {
			t.Fatalf("Prune(+%ds) forgot %d runs, want %d", step.now, got, step.forgot)
		}
	}
	if got := co.Runs(); got != 0 {
		t.Errorf("Runs = %d after pruning everything, want 0", got)
	}
}

func TestARefreshAfterPruningStartsANewRun(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		pruneAt int
		want    string
	}{
		// Not pruned: the run is extended by a refresh that arrives late, inside its
		// producer's deadline (300).
		"before the deadline": {pruneAt: 300, want: " [0 a] [0..60 a] [0..120 a]"},
		// Pruned: the run is forgotten, and the same refresh is a run of its own.
		"after the deadline": {pruneAt: 301, want: " [0 a] [0..60 a] [120 a]"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			co := mustNew(t, coalesce.Config{})
			fp := node(t, "node-0")
			var got []store.Record
			for _, at := range []int{0, 60} {
				got = co.Add(refresh(t, fp, at, "a"), got)
			}
			co.Prune(offset(tc.pruneAt))
			got = co.Add(refresh(t, fp, 120, "a"), got)
			if d := describe(got); d != tc.want {
				t.Errorf("wrote%s, want%s", d, tc.want)
			}
		})
	}
}

func TestADeleteOrAWatchModeAssertionEndsTheRun(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	fp := node(t, "node-0")
	del := refresh(t, fp, 90, "")
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	watch := refresh(t, fp, 150, "a")
	watch.TTL = 0

	var got []store.Record
	for _, r := range []store.Record{
		refresh(t, fp, 0, "a"), refresh(t, fp, 60, "a"),
		del,
		refresh(t, fp, 120, "a"), // after the delete: a new run, not an extension of the old one
		watch,
		refresh(t, fp, 180, "a"),
	} {
		got = co.Add(r, got)
	}
	want := " [0 a] [0..60 a] [90  ttl=0s] [120 a] [150 a ttl=0s] [180 a]"
	if d := describe(got); d != want {
		t.Errorf("wrote%s, want%s", d, want)
	}
	if co.Runs() != 1 {
		t.Errorf("Runs = %d, want 1", co.Runs())
	}
}

func TestSeqIsIgnoredOnInputAndZeroOnOutput(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	fp := node(t, "node-0")
	del := refresh(t, fp, 300, "")
	del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
	var got []store.Record
	for i, r := range []store.Record{
		refresh(t, fp, 0, "a"), refresh(t, fp, 60, "a"), refresh(t, fp, 120, "b"), refresh(t, fp, 180, "b"), del,
	} {
		r.Seq = uint64(1000 - i) // descending, as no real stream has
		got = co.Add(r, got)
	}
	if len(got) != 5 {
		t.Fatalf("wrote%s, want 5 records", describe(got))
	}
	for i, o := range got {
		if o.Seq != 0 {
			t.Errorf("record %d has Seq %d, want 0", i, o.Seq)
		}
	}
}

func TestAddAppendsToWhatItIsGiven(t *testing.T) {
	t.Parallel()

	co := mustNew(t, coalesce.Config{})
	fp := node(t, "node-0")
	first := refresh(t, fp, 0, "a")
	out := co.Add(first, []store.Record{{Seq: 7}})
	out = co.Add(refresh(t, fp, 60, "a"), out)
	if len(out) != 3 || out[0].Seq != 7 || !out[1].EventTime.Equal(offset(0)) || !out[2].Through.Equal(offset(60)) {
		t.Errorf("Add did not append to out: %v", out)
	}
	// A refresh the run already stands for appends nothing, and leaves out as it is.
	if again := co.Add(refresh(t, fp, 30, "a"), out); len(again) != 3 {
		t.Errorf("a refresh the run covers appended %d records", len(again)-3)
	}
}

func TestEveryOutputIsValidAndNoneIsBeforeTheHorizon(t *testing.T) {
	t.Parallel()

	// A stream with every kind of step: reboots, a change of payload, a gap, an old
	// run continued by a bound, a retention and a late refresh.
	fp := host(t, "host-0")
	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute})
	boots := []string{"b1", "b1", "b1", "b2", "b2", "b2", "b2", "b2"}
	n := 0
	var got []store.Record
	check := func(r store.Record) {
		t.Helper()
		out := co.Add(r, nil)
		for _, o := range out {
			n++
			o.Seq = uint64(n)
			if err := o.Validate(); err != nil {
				t.Fatalf("the output %s for %s is invalid: %v", describe([]store.Record{o}), describe([]store.Record{r}), err)
			}
			if h := co.Horizon(o.Layer); !r.EventTime.Before(h) && o.EventTime.Before(h) {
				t.Fatalf("the output %s is before the horizon %v, for %s", describe([]store.Record{o}), h.Sub(base), describe([]store.Record{r}))
			}
		}
		got = append(got, out...)
	}
	for i := range 40 {
		boot := boots[min(i/5, len(boots)-1)]
		check(bootRefresh(t, fp, i*60, "a", boot))
		if i == 20 {
			co.SetHorizonAll(offset(i * 60 / 2))
		}
		if i == 30 {
			check(bootRefresh(t, fp, 30, "a", "b1")) // late
			check(bootRefresh(t, fp, 15*60, "a", "b2"))
		}
	}
	if len(got) == 0 {
		t.Fatal("nothing was written")
	}
}

// A record that already carries a Through is not a refresh: only the coalescer sets one,
// so it is passed on as given, and it ends the run, whose state it no longer matches.
// Extending or absorbing it would overwrite its Through with a shorter one.
func TestARecordThatAlreadyCarriesAThroughIsPassedOnAndEndsTheRun(t *testing.T) {
	t.Parallel()

	fp := node(t, "node-0")
	// Half the TTL is 120 s: the record at 30 is inside the extension interval of the
	// run begun at 0, so a refresh at 30 would be absorbed.
	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5})
	long := refresh(t, fp, 30, "a")
	long.Through = offset(600)

	var got []store.Record
	got = co.Add(refresh(t, fp, 0, "a"), got)
	got = co.Add(long, got)
	if co.Runs() != 0 {
		t.Errorf("Runs = %d after a record with a Through, want 0", co.Runs())
	}
	// With the run gone, the next refresh starts a new one instead of extending the old.
	got = co.Add(refresh(t, fp, 60, "a"), got)
	if want := " [0 a] [30..600 a] [60 a]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
	if !got[1].Through.Equal(offset(600)) {
		t.Errorf("the Through of the record became %v, want 600 s", got[1].Through.Sub(base))
	}

	// The same when every refresh would extend: the record keeps its Through.
	co = mustNew(t, coalesce.Config{})
	got = co.Add(refresh(t, fp, 0, "a"), nil)
	got = co.Add(long, got)
	if want := " [0 a] [30..600 a]"; describe(got) != want {
		t.Errorf("wrote%s, want%s", describe(got), want)
	}
}

// The record that closes a run is keyed inside the horizon of the run's own layer. A host
// (layer L1) run begun before an L1 horizon, with an extension interval longer than its
// TTL so that every later refresh is absorbed and its stored copy lapses, is closed by a
// payload change with a record at the horizon, not at its start; a node (layer L2) run
// told no horizon is closed at its start.
func TestTheRecordThatClosesARunIsKeyedInsideTheHorizonOfItsLayer(t *testing.T) {
	t.Parallel()

	h, n := host(t, "host-0"), node(t, "node-0")
	l1, l2 := layerOf(t, h), layerOf(t, n)
	if l1 == l2 || l1 != catalog.L1 {
		t.Fatalf("the host is in layer %s and the node in %s: the test shows nothing", l1, l2)
	}
	co := mustNew(t, coalesce.Config{ExtendEvery: 400 * time.Second})
	var got []store.Record
	for _, at := range []int{0, 60, 120, 180} {
		got = co.Add(refresh(t, h, at, "a"), got)
		got = co.Add(refresh(t, n, at, "a"), got)
	}
	co.SetHorizon(l1, offset(200))
	if !co.Horizon(catalog.L0).IsZero() || !co.Horizon(l2).IsZero() {
		t.Fatal("a horizon of the host's layer was held for another layer")
	}
	// A change of payload at 300: the stored copy of both runs lapsed at 240, and the
	// producer's refreshes reached 180, so both are carried to 300 first.
	got = co.Add(refresh(t, h, 300, "b"), got)
	got = co.Add(refresh(t, n, 300, "b"), got)

	var hostGot, nodeGot []store.Record
	for _, r := range got {
		if r.Layer == l1 {
			hostGot = append(hostGot, r)
		} else {
			nodeGot = append(nodeGot, r)
		}
	}
	// The host's run began before its layer's horizon (200), so it is continued, not
	// extended: by a record at the later of its last refresh (180) and the horizon, which
	// is the horizon. The node's layer has no horizon, so its run is extended at its start
	// through its last refresh.
	if want := " [0 a] [200 a] [300 b]"; describe(hostGot) != want {
		t.Errorf("the host's run: wrote%s, want%s", describe(hostGot), want)
	}
	if want := " [0 a] [0..180 a] [300 b]"; describe(nodeGot) != want {
		t.Errorf("the node's run: wrote%s, want%s", describe(nodeGot), want)
	}
}

// Absorbing refreshes can hide a clone collision for a while. Boot A is seen at 0 and
// again at 60 (absorbed: the run stands for it), and boot B is seen late, at 30. The raw
// stream shows A after B first appeared; the coalesced one does not until A is refreshed
// again. Boot sightings end early by less than the extension interval, as existence does.
func TestAbsorptionCanDelayACloneCollision(t *testing.T) {
	t.Parallel()

	fp := host(t, "host-0")
	policy := lifecycle.Policy{BootKey: lifecycle.BootID}
	beats := []store.Record{
		bootRefresh(t, fp, 0, "a", "A"), bootRefresh(t, fp, 60, "a", "A"), bootRefresh(t, fp, 30, "a", "B"),
	}
	co := mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5}) // 120 s
	feedAll := func(rs ...store.Record) []store.Record {
		var out []store.Record
		for _, r := range rs {
			out = co.Add(r, out)
		}
		for i := range out {
			out[i].Seq = uint64(i + 1)
		}
		return out
	}
	assign := func(rs []store.Record) []store.Record {
		rs = slices.Clone(rs)
		for i := range rs {
			rs[i].Seq = uint64(i + 1)
		}
		return rs
	}

	// Boot sightings of a run: A last seen at 60 in the raw stream, at 0 in the coalesced.
	rawBoots, _ := foldBoots(assign(beats[:2]), policy)
	gotBoots, _ := foldBoots(feedAll(beats[:2]...), policy)
	if len(rawBoots) != 1 || len(gotBoots) != 1 || !rawBoots[0].LastSeen.Equal(offset(60)) ||
		!gotBoots[0].LastSeen.Equal(offset(0)) || rawBoots[0].LastSeen.Sub(gotBoots[0].LastSeen) >= 120*time.Second {
		t.Errorf("boots: raw %v, coalesced %v; want A last seen at 60 s and at 0 s", rawBoots, gotBoots)
	}

	// The late B: a collision in the raw stream, none in the coalesced one.
	co = mustNew(t, coalesce.Config{ExtendTTLFraction: 0.5})
	if _, err := foldBoots(assign(beats), policy); !errors.Is(err, lifecycle.ErrCloneCollision) {
		t.Fatalf("the raw stream folds with %v, want a clone collision", err)
	}
	out := feedAll(beats...)
	if _, err := foldBoots(out, policy); err != nil {
		t.Fatalf("the coalesced stream folds with %v before A is refreshed again, want none (wrote%s)", err, describe(out))
	}
	// A is refreshed again: it is now seen after B first appeared, and the collision shows.
	next := bootRefresh(t, fp, 120, "a", "A")
	out = append(out, feedAll(next)...)
	for i := range out {
		out[i].Seq = uint64(i + 1)
	}
	if _, err := foldBoots(out, policy); !errors.Is(err, lifecycle.ErrCloneCollision) {
		t.Errorf("the coalesced stream folds with %v after A is refreshed, want a clone collision (wrote%s)", err, describe(out))
	}
}

func foldBoots(rs []store.Record, p lifecycle.Policy) ([]lifecycle.Boot, error) {
	tl, err := foldRecords(rs, p)
	return tl.Boots(), err
}
