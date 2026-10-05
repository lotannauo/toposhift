package workload

import (
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// A coalesced stream may end a run's existence early (that is what a bounded
// extension interval trades away) but may never open a hole inside an interval of
// existence the producer's refreshes covered: a hole is a death and a rebirth that
// nothing happened, and every layout would store it as a version.
//
// These tests drive the coalescer with refreshes written out by hand, so each
// shape is exact: the refresh that continues a run after a retention, the one that
// changes the description, and the ones that follow a real silence.

// beat is a refresh offset seconds after the start of the stream, with a payload.
type beat struct {
	at      int
	payload string
}

const testTTL = 240 * time.Second

// everyMinute is a refresh every 60 s from 0 to the offset.
func everyMinute(to int) []beat {
	var beats []beat
	for at := 0; at <= to; at += 60 {
		beats = append(beats, beat{at, "a"})
	}
	return beats
}

// lateBeats is a refresh every minute to 720 s, then one that arrives late at the
// given offset, then every minute to 1200 s.
func lateBeats(late int) []beat {
	var beats []beat
	for at := 0; at <= 720; at += 60 {
		beats = append(beats, beat{at, "a"})
	}
	beats = append(beats, beat{late, "a"})
	for at := 780; at <= 1200; at += 60 {
		beats = append(beats, beat{at, "a"})
	}
	return beats
}

func offset(base time.Time, s int) time.Time { return base.Add(time.Duration(s) * time.Second) }

// coalesceScenario feeds refreshes to a coalescer in order, telling it a retention
// horizon after the refresh at the key's offset, and returns the raw assertions and
// what the coalescer wrote.
type coalesceScenario struct {
	fraction float64
	every    time.Duration // ExtendEvery, instead of a fraction
	maxAge   time.Duration // RunMaxAge
	beats    []beat
	// horizonAfter maps the offset of a refresh to the horizon (as an offset) the
	// store has retained to once that refresh has been seen.
	horizonAfter map[int]int
}

func (s coalesceScenario) run(t *testing.T) (raw, got []engine.Record, base time.Time) {
	t.Helper()
	cfg := Tiny()
	cfg.CoalesceRuns, cfg.ExtendTTLFraction, cfg.ExtendEvery = true, s.fraction, s.every
	cfg.RunMaxAge = s.maxAge
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base = cfg.Start
	fp := g.cl.nodes[0]
	rec := func(b beat) engine.Record {
		return engine.Record{
			Layer: entityLayer(fp.Type()), Subject: engine.EntitySubject(fp), Producer: ProducerNodeCollector,
			EventTime: offset(base, b.at), Kind: lifecycle.Observe, TTL: testTTL, Payload: []byte(b.payload),
		}
	}
	seq := uint64(0)
	for _, b := range s.beats {
		r := rec(b)
		seq++
		r.Seq = seq
		raw = append(raw, r)
		for _, o := range coalesced(g, r) {
			o.Seq = uint64(len(got) + 1)
			if err := o.Validate(); err != nil {
				t.Fatalf("the coalescer wrote an invalid record %s: %v", describe(base, []engine.Record{o}), err)
			}
			if h := g.horizon; o.EventTime.Before(h) && !r.EventTime.Before(h) {
				t.Fatalf("the coalescer wrote %s before the horizon %v for a refresh after it", describe(base, []engine.Record{o}), h.Sub(base))
			}
			got = append(got, o)
		}
		if h, ok := s.horizonAfter[b.at]; ok {
			g.SetHorizon(offset(base, h))
		}
	}
	return raw, got, base
}

// coalesced is what the coalescer writes for one refresh.
func coalesced(g *Generator, r engine.Record) []engine.Record { return g.coalesce(r, nil) }

// timeline folds records and reports whether the subject exists at an instant.
func timeline(t *testing.T, rs []engine.Record) lifecycle.Timeline {
	t.Helper()
	as := make([]lifecycle.Assertion, len(rs))
	for i, r := range rs {
		as[i] = r.Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

// holes lists the instants, on a grid, from `from` to the end where the subject
// exists in the raw stream and not in the coalesced one, and exists in it again
// later within the same interval of the raw stream; and the instants where the
// coalesced stream says it exists and the raw one does not.
func holes(t *testing.T, raw, got []engine.Record, base time.Time, from, to int) (inside, late, differ []int) {
	t.Helper()
	tr, tg := timeline(t, raw), timeline(t, got)
	missing := false
	for x := from; x < to; x += 5 {
		at := offset(base, x)
		a, b := tr.AliveAt(at), tg.AliveAt(at)
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
			da, _ := tr.DescribeAt(at)
			db, _ := tg.DescribeAt(at)
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
		coalesceScenario
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
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "a"}},
			},
			from: 200, to: 560, want: " [0 a] [200 a] [300 a]",
		},
		// The same with the horizon behind the run's last extension, so that the
		// run can still be extended at its start and it is the description that
		// changes.
		"description changes a beat late": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9,
				beats:    []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "b"}},
			},
			from: 0, to: 560, want: " [0 a] [0..180 a] [300 b]",
		},
		// The description changes after a retention that the run began before.
		"description changes a beat late after a retention": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "b"}},
			},
			from: 200, to: 560, want: " [0 a] [200 a] [300 b]",
		},
		// The retention is behind the last refresh, so the run goes on from that
		// refresh and not from the horizon.
		"continued after a retention, from the last refresh": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 150},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {300, "a"}},
			},
			from: 150, to: 560, want: " [0 a] [180 a] [300 a]",
		},
		// An extension interval longer than the TTL lets the stored deadline pass
		// before the last refresh; the run goes on from the lapse.
		"stored deadline lapsed before the last refresh": {
			coalesceScenario: coalesceScenario{
				every: 400 * time.Second, horizonAfter: map[int]int{360: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {240, "a"}, {300, "a"}, {360, "a"}, {420, "a"}},
			},
			from: 200, to: 700, want: " [0 a] [240 a] [420 a]",
		},
		// A refresh older than the horizon is one the store refuses: nothing is
		// written to carry the run to it.
		"a refresh from before the horizon": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 200},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {190, "a"}},
			},
			from: 200, to: 440, want: " [0 a] [190 a]",
		},
		// The refresh comes after the stored deadline but before the horizon: the
		// store would refuse it, and a record at the horizon to carry the run there
		// would promise existence for longer than the producer did.
		"a refresh after the lapse and before the horizon": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{180: 260},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}, {180, "a"}, {250, "a"}},
			},
			from: 260, to: 560, want: " [0 a] [250 a]",
		},
		// The run has been extended, so its stored deadline is later than the
		// first record's would give: the description changes within it, and
		// nothing is written but the extension and the new description.
		"description changes inside the stored deadline": {
			coalesceScenario: coalesceScenario{
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
			coalesceScenario: coalesceScenario{
				fraction: 0.5, horizonAfter: map[int]int{600: 300},
				beats: lateBeats(400),
			},
			from: 300, to: 1500,
			want: " [0 a] [0..120 a] [0..240 a] [0..360 a] [0..480 a] [0..600 a] [660 a] [660..780 a] [660..900 a] [660..1020 a] [660..1140 a]",
		},
		// A late refresh after the previous run's last one is not covered by it, and
		// is passed through as before: it is a refresh the stream has not seen.
		"a late refresh after the previous run's last": {
			coalesceScenario: coalesceScenario{
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
			coalesceScenario: coalesceScenario{
				fraction: 0.5, maxAge: 1200 * time.Second,
				beats: everyMinute(3000),
			},
			from: 0, to: 3300,
			want: " [0 a] [0..120 a] [0..240 a] [0..360 a] [0..480 a] [0..600 a] [0..720 a] [0..840 a] [0..960 a] [0..1080 a] [1200 a] [1200..1320 a] [1200..1440 a] [1200..1560 a] [1200..1680 a] [1200..1800 a] [1200..1920 a] [1200..2040 a] [1200..2160 a] [1200..2280 a] [2400 a] [2400..2520 a] [2400..2640 a] [2400..2760 a] [2400..2880 a] [2400..3000 a]",
		},
		// With an extension interval that leaves the stored deadline short of the next
		// run, the old run is carried to it first.
		"a run at its greatest age, with a short stored deadline": {
			coalesceScenario: coalesceScenario{
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
			coalesceScenario: coalesceScenario{
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
			coalesceScenario: coalesceScenario{
				fraction: 0.9,
				beats:    append(append([]beat{{0, "a"}, {60, "a"}, {120, "a"}}, every(180, 1200, 60, "b")...), beat{90, "a"}, beat{1400, "b"}, beat{1460, "b"}),
			},
			from: 0, to: 1700, want: " [0 a] [180 b] [180..420 b] [180..660 b] [180..900 b] [180..1140 b] [180..1400 b]",
		},
		// The stored deadline already covers the new run's start: nothing to add.
		"continued with no lag": {
			coalesceScenario: coalesceScenario{
				fraction: 0.9, horizonAfter: map[int]int{0: 30},
				beats: []beat{{0, "a"}, {60, "a"}, {120, "a"}},
			},
			from: 30, to: 400, want: " [0 a] [60 a]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			raw, got, base := tc.run(t)
			inside, late, differ := holes(t, raw, got, base, tc.from, tc.to)
			if len(inside) > 0 {
				t.Errorf("the coalesced stream has the subject missing at +%vs and existing again after it, which the raw stream does not (%d records for %d refreshes: %s)",
					inside, len(got), len(raw), describe(base, got))
			}
			if len(late) > 0 {
				t.Errorf("the coalesced stream has the subject existing at +%vs, which the raw stream says it does not: existence may end early and never late", late)
			}
			if len(differ) > 0 {
				t.Errorf("the coalesced stream describes the subject differently from the raw one at +%vs: %s", differ, describe(base, got))
			}
			if d := describe(base, got); d != tc.want {
				t.Errorf("the coalescer wrote%s, want%s", d, tc.want)
			}
		})
	}
}

// every is a beat each step seconds from first to last, with the payload.
func every(first, last, step int, payload string) []beat {
	var out []beat
	for at := first; at <= last; at += step {
		out = append(out, beat{at, payload})
	}
	return out
}

func describe(base time.Time, rs []engine.Record) string {
	var out string
	for _, r := range rs {
		through := ""
		if !r.Through.IsZero() {
			through = fmt.Sprintf("..%d", int(r.Through.Sub(base)/time.Second))
		}
		ttl := ""
		if r.TTL != testTTL {
			ttl = fmt.Sprintf(" ttl=%s", r.TTL)
		}
		out += fmt.Sprintf(" [%d%s %s%s]", int(r.EventTime.Sub(base)/time.Second), through, r.Payload, ttl)
	}
	return out
}

// A late refresh of the description the stream has moved to is not one the previous
// run stands for, even inside the span it covered (it described something else): it
// passes through as its own assertion.
func TestALateRefreshOfTheNewDescriptionIsNotAbsorbed(t *testing.T) {
	t.Parallel()

	cfg := Tiny()
	cfg.CoalesceRuns, cfg.ExtendTTLFraction = true, 0.5
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fp := g.cl.nodes[0]
	rec := func(at int, payload string) engine.Record {
		return engine.Record{
			Layer: entityLayer(fp.Type()), Subject: engine.EntitySubject(fp), Producer: ProducerNodeCollector,
			EventTime: offset(cfg.Start, at), Kind: lifecycle.Observe, TTL: testTTL, Payload: []byte(payload),
		}
	}
	var got []engine.Record
	for _, r := range []engine.Record{rec(0, "a"), rec(60, "a"), rec(120, "a"), rec(600, "b"), rec(90, "b")} {
		got = append(got, coalesced(g, r)...)
	}
	if want := " [0 a] [0..120 a] [600 b] [90 b]"; describe(cfg.Start, got) != want {
		t.Errorf("wrote%s, want%s", describe(cfg.Start, got), want)
	}
}

// A late refresh at exactly the first or the last instant the previous run covers is
// inside it.
func TestALateRefreshAtTheEdgesOfThePreviousRunIsAbsorbed(t *testing.T) {
	t.Parallel()

	for _, late := range []int{0, 120} {
		cfg := Tiny()
		cfg.CoalesceRuns, cfg.ExtendTTLFraction = true, 0.5
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		fp := g.cl.nodes[0]
		rec := func(at int) engine.Record {
			return engine.Record{
				Layer: entityLayer(fp.Type()), Subject: engine.EntitySubject(fp), Producer: ProducerNodeCollector,
				EventTime: offset(cfg.Start, at), Kind: lifecycle.Observe, TTL: testTTL, Payload: []byte("a"),
			}
		}
		var got []engine.Record
		for _, at := range []int{0, 60, 120, 600, late} { // the run restarts at 600; the last is late
			got = append(got, coalesced(g, rec(at))...)
		}
		if want := " [0 a] [0..120 a] [600 a]"; describe(cfg.Start, got) != want {
			t.Errorf("late refresh at %d: wrote%s, want%s", late, describe(cfg.Start, got), want)
		}
	}
}
