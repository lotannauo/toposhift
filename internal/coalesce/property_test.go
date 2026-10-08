package coalesce_test

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestMain sets how many cases rapid runs for each property. The environment
// variable TOPOSHIFT_RAPID_CHECKS raises it, and a -rapid.checks given on the
// command line still wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", storetest.RapidChecks("30")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// The properties feed the raw refreshes of a generated cluster, in arrival order, to
// a coalescer, and fold what it returns with the lifecycle specification, beside the
// fold of the raw stream. A store that retains is modelled as the storage layers are:
// once told a horizon it refuses a record before it (in the layers it retains), and
// keeps every record it took before.

// drawConfig draws a valid config of a small cluster of refreshing producers, with
// hosts that reboot and clones that report old boots. It has no runs: the stream is
// the raw refreshes.
func drawConfig(t *rapid.T) storetest.Config {
	c := storetest.Tiny()
	c.Seed = rapid.Uint64().Draw(t, "seed")
	c.Duration = time.Duration(rapid.IntRange(20, 60).Draw(t, "minutes")) * time.Minute
	c.Racks = rapid.IntRange(1, 2).Draw(t, "racks")
	c.Hosts = rapid.IntRange(1, 4).Draw(t, "hosts")
	c.Pods = rapid.IntRange(1, 8).Draw(t, "pods")
	c.Services = rapid.IntRange(1, 3).Draw(t, "services")
	c.ChurnPerMinute = rapid.SampledFrom([]float64{0, 5, 20}).Draw(t, "churn")
	c.HeartbeatInterval = rapid.SampledFrom([]time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute}).Draw(t, "interval")
	c.TTLFactor = rapid.IntRange(1, 5).Draw(t, "ttlFactor")
	c.ConfirmProbability = rapid.SampledFrom([]float64{0, 0.5}).Draw(t, "confirm")
	c.LateProbability = rapid.SampledFrom([]float64{0, 0.3}).Draw(t, "late")
	c.LateMax = time.Duration(rapid.IntRange(1, 300).Draw(t, "lateMax")) * time.Second
	c.RebootProbability = rapid.SampledFrom([]float64{0, 0.2, 0.5}).Draw(t, "reboot")
	c.CloneProbability = 0
	if c.RebootProbability > 0 {
		c.CloneProbability = rapid.SampledFrom([]float64{0, 0.5, 1}).Draw(t, "clone")
	}
	c.Runs = false
	if err := c.Validate(); err != nil {
		t.Fatalf("drew an invalid config: %v", err)
	}
	return c
}

// retention says when the modelled store is told a horizon, and for which layers.
type retention struct {
	set   bool          // whether there is a retention at all
	all   bool          // every layer, or only layer
	layer catalog.Layer //
	at    time.Time     // the horizon
}

// refuses is whether the store, once it has retained, refuses the record.
func (r retention) refuses(rec store.Record) bool {
	return r.set && rec.EventTime.Before(r.at) && (r.all || rec.Layer == r.layer)
}

func (r retention) String() string {
	switch {
	case !r.set:
		return "no retention"
	case r.all:
		return fmt.Sprintf("every layer retained to %s", r.at.Format(time.TimeOnly))
	}
	return fmt.Sprintf("layer %s retained to %s", r.layer, r.at.Format(time.TimeOnly))
}

// drawRetention draws no retention, or a horizon in the first half of the stream that
// covers every layer or one of the layers of the refreshing producers.
func drawRetention(t *rapid.T, c storetest.Config) retention {
	kind := rapid.IntRange(0, 2).Draw(t, "retention")
	if kind == 0 {
		return retention{}
	}
	steps := rapid.SampledFrom([]int{4, 3, 2}).Draw(t, "horizonShare")
	r := retention{set: true, all: kind == 1, at: c.Start.Add(c.Duration / time.Duration(steps))}
	if !r.all {
		r.layer = rapid.SampledFrom([]catalog.Layer{catalog.L1, catalog.L2, catalog.L3}).Draw(t, "horizonLayer")
	}
	return r
}

// outcome is what the store keeps of a raw stream, and of what the coalescer made of
// it, when the store retains as the retention says, and how the coalescer behaved.
type outcome struct {
	raw, out []store.Record // kept by the store; out has its Seq assigned
	// extensions are the records returned with a Through; absorbed the refreshes that
	// returned nothing; continuations the refreshes of a run, inside its TTL, that
	// started a new run of the same description.
	extensions, absorbed, continuations int
}

type seen struct {
	ttl     time.Duration
	boot    string
	payload string
	last    time.Time
}

// feed runs the stream through a new coalescer, telling it the retention once the
// first record at or after the horizon has been seen, and checks what no stream may
// break: every output is valid once it has a Seq, and none is before the horizon of
// its layer for a refresh at or after it.
func feed(t fataler, cfg coalesce.Config, stream []store.Record, ret retention) outcome {
	co, err := coalesce.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var o outcome
	told := false
	last := map[struct {
		s store.Subject
		p lifecycle.Producer
	}]seen{}
	for _, r := range stream {
		outs := co.Add(r, nil)

		if r.Kind == lifecycle.Observe && r.TTL > 0 {
			key := struct {
				s store.Subject
				p lifecycle.Producer
			}{r.Subject, r.Producer}
			prev, had := last[key]
			same := had && prev.ttl == r.TTL && prev.boot == r.Boot && prev.payload == string(r.Payload)
			switch {
			case len(outs) == 0:
				o.absorbed++
			case same && r.EventTime.After(prev.last) && !r.EventTime.After(prev.last.Add(prev.ttl)) &&
				outs[len(outs)-1].Through.IsZero() && outs[len(outs)-1].EventTime.Equal(r.EventTime):
				o.continuations++
			}
			if !same || r.EventTime.After(prev.last) {
				last[key] = seen{r.TTL, r.Boot, string(r.Payload), r.EventTime}
			}
		}

		if !ret.refuses(r) || !told {
			o.raw = append(o.raw, r)
		}
		for _, x := range outs {
			if x.Seq != 0 {
				t.Fatalf("an output has Seq %d, want 0", x.Seq)
			}
			if !x.Through.IsZero() {
				o.extensions++
			}
			if h := co.Horizon(x.Layer); x.EventTime.Before(h) && !r.EventTime.Before(h) {
				t.Fatalf("output at %s is before the horizon %s of its layer, for a refresh at %s",
					x.EventTime.Format(time.TimeOnly), h.Format(time.TimeOnly), r.EventTime.Format(time.TimeOnly))
			}
			if told && ret.refuses(x) {
				continue
			}
			x.Seq = uint64(len(o.out) + 1)
			if err := x.Validate(); err != nil {
				t.Fatalf("an output is invalid: %v", err)
			}
			o.out = append(o.out, x)
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
	return o
}

// subjects lists the subjects of the records, in order of first appearance, and
// groups the records by subject, keeping their order.
func subjects(rs []store.Record) ([]store.Subject, map[store.Subject][]store.Record) {
	var order []store.Subject
	by := map[store.Subject][]store.Record{}
	for _, r := range rs {
		if _, ok := by[r.Subject]; !ok {
			order = append(order, r.Subject)
		}
		by[r.Subject] = append(by[r.Subject], r)
	}
	return order, by
}

func foldRecords(rs []store.Record, p lifecycle.Policy) (lifecycle.Timeline, error) {
	as := make([]lifecycle.Assertion, len(rs))
	for i, r := range rs {
		as[i] = r.Assertion()
	}
	return lifecycle.Fold(as, p)
}

// policyOf is the policy a store opened for these streams uses: boots are told apart for
// hosts.
func policyOf(s store.Subject) lifecycle.Policy {
	if s.Kind == store.SubjectEntity && s.A.Type() == catalog.Host {
		return lifecycle.Policy{BootKey: lifecycle.BootID}
	}
	return lifecycle.Policy{}
}

// clip is the intervals from h on.
func clip(ivs []lifecycle.Interval, h time.Time) []lifecycle.Interval {
	var out []lifecycle.Interval
	for _, iv := range ivs {
		if !iv.Open() && !iv.End.After(h) {
			continue
		}
		if iv.Start.Before(h) {
			iv.Start = h
		}
		out = append(out, iv)
	}
	return out
}

func describeIntervals(ivs []lifecycle.Interval) string {
	out := ""
	for _, iv := range ivs {
		end := "open"
		if !iv.Open() {
			end = iv.End.Format(time.TimeOnly) + " (" + iv.EndSource.String() + ")"
		}
		out += fmt.Sprintf(" [%s, %s)", iv.Start.Format(time.TimeOnly), end)
	}
	return out
}

// fataler is what feed and check need of a test, so that a rapid case and a plain
// test both fit.
type fataler interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// tally counts what check found in the streams it was given: the second boots of hosts
// and the clone collisions. A property whose generator never produced them would pass
// with nothing to say, so TestTheGeneratedStreamsReachRebootsAndCollisions pins that on
// a fixed stream, outside rapid, where it cannot depend on how many cases ran.
type tally struct {
	rebootsSeen, collisions int
}

// check folds the raw and the coalesced stream, subject by subject, and compares them.
// With exact, existence must be the same intervals (the same ends too) and boots the
// same sightings; without it, the coalesced intervals match the raw ones one for one,
// start with them, and end no later and earlier by less than the extension interval,
// and the boots match in ID and first sighting.
func check(t fataler, cfg coalesce.Config, exact bool, o outcome, ret retention, tl *tally) {
	order, rawBy := subjects(o.raw)
	_, outBy := subjects(o.out)
	for _, s := range order {
		raw, out := rawBy[s], outBy[s]
		// The horizon matters to a subject if its layer is retained.
		var from time.Time
		if ret.set && (ret.all || raw[0].Layer == ret.layer) {
			from = ret.at
		}
		name := "entity " + string(s.A.Type())
		if s.Kind == store.SubjectEdge {
			name = fmt.Sprintf("%s %s", s.Relation, s.A.Type())
		}

		rawTL, rawErr := foldRecords(raw, policyOf(s))
		outTL, outErr := foldRecords(out, policyOf(s))
		if rawErr != nil || outErr != nil {
			if !errors.Is(rawErr, lifecycle.ErrCloneCollision) {
				t.Fatalf("%s: folding the raw stream: %v", name, rawErr)
			}
			tl.collisions++
			if !errors.Is(outErr, lifecycle.ErrCloneCollision) {
				t.Fatalf("%s: the raw stream has a clone collision (%v) and the coalesced one has %v", name, rawErr, outErr)
			}
			if exact {
				var a, b *lifecycle.CloneCollisionError
				if !errors.As(rawErr, &a) || !errors.As(outErr, &b) || *a != *b {
					t.Fatalf("%s: the clone collision differs: raw %v, coalesced %v", name, rawErr, outErr)
				}
			}
			continue
		}

		// Boots.
		rb, ob := rawTL.Boots(), outTL.Boots()
		if len(rb) != len(ob) {
			t.Fatalf("%s: raw boots %v, coalesced boots %v", name, rb, ob)
		}
		for i := range rb {
			if rb[i].ID != ob[i].ID || !rb[i].FirstSeen.Equal(ob[i].FirstSeen) || (exact && !rb[i].LastSeen.Equal(ob[i].LastSeen)) {
				t.Fatalf("%s: raw boots %v, coalesced boots %v", name, rb, ob)
			}
		}
		tl.rebootsSeen += max(0, len(rb)-1)

		// Existence.
		ri, oi := clip(rawTL.Existence(), from), clip(outTL.Existence(), from)
		if len(ri) != len(oi) {
			t.Fatalf("%s, %s: raw existence%s, coalesced%s", name, ret, describeIntervals(ri), describeIntervals(oi))
		}
		var extend time.Duration
		for _, r := range raw {
			extend = max(extend, cfg.ExtensionInterval(r.TTL))
		}
		for i := range ri {
			a, b := ri[i], oi[i]
			bad := !a.Start.Equal(b.Start) || a.Open() != b.Open()
			if !bad && !a.Open() {
				short := a.End.Sub(b.End)
				bad = short < 0 || (exact && short != 0) || (!exact && short > 0 && short >= extend)
			}
			if bad || (exact && a.EndSource != b.EndSource && !a.Open()) {
				t.Fatalf("%s, %s, extension interval %s: raw existence%s, coalesced%s", name, ret, extend, describeIntervals(ri), describeIntervals(oi))
			}
		}
	}
}

// drawStream draws a config and builds its stream.
func drawStream(t *rapid.T) (storetest.Config, []store.Record) {
	c := drawConfig(t)
	g, err := storetest.NewGenerator(c)
	if err != nil {
		t.Fatal(err)
	}
	return c, g.All()
}

// With every refresh extending and no bound, the coalesced stream is the raw one in
// another form: the same existence, the same boots, and the same clone collisions.
func TestWithNothingAbsorbedTheCoalescedStreamFoldsLikeTheRawOne(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		c, stream := drawStream(t)
		o := feed(t, coalesce.Config{}, stream, retention{})
		if o.extensions == 0 {
			t.Fatalf("no extension in %d records: the property shows nothing (hosts %d, interval %s, duration %s)",
				len(stream), c.Hosts, c.HeartbeatInterval, c.Duration)
		}
		if o.absorbed != 0 {
			t.Fatalf("%d refreshes were absorbed with nothing to absorb them", o.absorbed)
		}
		check(t, coalesce.Config{}, true, o, retention{}, new(tally))
	})
}

// Absorbing refreshes within an extension interval, with or without a bound on the age
// of a run, ends existence early by less than that interval and never opens a hole.
func TestAnExtensionIntervalOnlyEndsExistenceEarly(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		c, stream := drawStream(t)
		cfg := coalesce.Config{}
		switch rapid.IntRange(0, 2).Draw(t, "config") {
		case 0:
			cfg.ExtendTTLFraction = 0.5
		case 1:
			cfg = coalesce.DefaultConfig()
			cfg.RunMaxAge = time.Duration(rapid.IntRange(1, 30).Draw(t, "maxAgeMinutes")) * time.Minute
		default:
			cfg.ExtendEvery = time.Duration(rapid.IntRange(1, 600).Draw(t, "everySeconds")) * time.Second
		}
		if cfg.RunMaxAge > c.Duration-c.HeartbeatInterval {
			cfg.RunMaxAge = max(time.Minute, c.Duration-c.HeartbeatInterval)
		}
		o := feed(t, cfg, stream, retention{})
		if o.extensions+o.absorbed+o.continuations == 0 {
			t.Fatalf("the coalescer extended, absorbed and continued nothing in %d records: the property shows nothing", len(stream))
		}
		if cfg.ExtendTTLFraction > 0 && c.TTLFactor >= 3 && (cfg.RunMaxAge == 0 || cfg.RunMaxAge > c.HeartbeatInterval) && o.absorbed == 0 {
			t.Fatalf("a fraction of a TTL of %d intervals absorbed no refresh", c.TTLFactor)
		}
		if cfg.RunMaxAge > 0 {
			if o.continuations == 0 {
				t.Fatalf("a bound of %s (duration %s, interval %s) continued no run", cfg.RunMaxAge, c.Duration, c.HeartbeatInterval)
			}
		}
		check(t, cfg, false, o, retention{}, new(tally))
	})
}

// checkBound feeds the stream to a coalescer, telling it the retention, and checks that no
// extension is asserted at a run's start the bound or more before its Through. It
// returns how many extensions it saw.
func checkBound(t fataler, cfg coalesce.Config, stream []store.Record, ret retention) int {
	co, err := coalesce.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	extensions, told := 0, false
	for _, r := range stream {
		for _, x := range co.Add(r, nil) {
			if x.Through.IsZero() {
				continue
			}
			extensions++
			if age := x.Through.Sub(x.EventTime); age >= cfg.RunMaxAge {
				t.Fatalf("an extension at %s reaches %s, %s later, which is the bound %s or more",
					x.EventTime.Format(time.TimeOnly), x.Through.Format(time.TimeOnly), age, cfg.RunMaxAge)
			}
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
	return extensions
}

// With a bound, an extension is never asserted at a run's start more than the bound
// before its Through.
func TestNoExtensionReachesTheBound(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		c, stream := drawStream(t)
		cfg := coalesce.Config{
			ExtendTTLFraction: rapid.SampledFrom([]float64{0, 0.5, 1}).Draw(t, "fraction"),
			RunMaxAge:         time.Duration(rapid.IntRange(1, 30).Draw(t, "maxAgeMinutes")) * time.Minute,
		}
		cfg.RunMaxAge = min(cfg.RunMaxAge, max(time.Minute, c.Duration-c.HeartbeatInterval))
		checkBound(t, cfg, stream, drawRetention(t, c))
	})
}

// fixedStream is a stream that does not depend on rapid: four hosts that reboot, with
// clones, refreshing every minute for forty minutes.
func fixedStream(t *testing.T) (storetest.Config, []store.Record) {
	t.Helper()
	c := storetest.Tiny()
	c.Seed, c.Duration = 7, 40*time.Minute
	c.Hosts, c.Pods, c.Services = 4, 8, 2
	c.HeartbeatInterval, c.TTLFactor = time.Minute, 4
	c.LateProbability, c.ConfirmProbability = 0.3, 0.5
	c.RebootProbability, c.CloneProbability = 0.06, 0.5
	g, err := storetest.NewGenerator(c)
	if err != nil {
		t.Fatal(err)
	}
	return c, g.All()
}

// The properties above draw their streams; this one pins, on a fixed stream and with
// no dependence on how many cases rapid runs, that those streams can reach what the
// properties are about: extensions under a bound, hosts that reboot, and clone
// collisions that survive coalescing.
func TestTheGeneratedStreamsReachRebootsAndCollisions(t *testing.T) {
	t.Parallel()

	c, stream := fixedStream(t)
	for name, cfg := range map[string]coalesce.Config{
		"nothing absorbed": {},
		"the default":      coalesce.DefaultConfig(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var tl tally
			ret := retention{set: true, all: true, at: c.Start.Add(c.Duration / 3)}
			for _, r := range []retention{{}, ret} {
				o := feed(t, cfg, stream, r)
				check(t, cfg, cfg == coalesce.Config{}, o, r, &tl)
			}
			if tl.rebootsSeen == 0 || tl.collisions == 0 {
				t.Errorf("%d second boots and %d clone collisions in a stream asked to have both", tl.rebootsSeen, tl.collisions)
			}
		})
	}
	bounded := coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute}
	if n := checkBound(t, bounded, stream, retention{}); n == 0 {
		t.Error("no extension under a bound in the fixed stream: the bound property shows nothing")
	}
}

// A store that retains, told so, refuses nothing the coalescer was able to avoid, and
// from the horizon on the coalesced stream still folds like the raw one.
func TestAfterARetentionTheCoalescedStreamStillFoldsLikeTheRawOne(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		c, stream := drawStream(t)
		ret := drawRetention(t, c)
		if !ret.set {
			ret = retention{set: true, all: true, at: c.Start.Add(c.Duration / 2)}
		}
		exact := rapid.Bool().Draw(t, "exact")
		cfg := coalesce.Config{}
		if !exact {
			cfg = coalesce.DefaultConfig()
			if rapid.Bool().Draw(t, "fixedInterval") {
				// An interval that may outlast the TTL, so that the store's copy of a run
				// lapses before its last refresh and a closing record has to be written.
				cfg.ExtendTTLFraction = 0
				cfg.ExtendEvery = time.Duration(rapid.IntRange(1, 600).Draw(t, "everySeconds")) * time.Second
			}
			cfg.RunMaxAge = min(time.Duration(rapid.IntRange(1, 30).Draw(t, "maxAgeMinutes"))*time.Minute, max(time.Minute, c.Duration-c.HeartbeatInterval))
		}
		o := feed(t, cfg, stream, ret)
		if o.continuations == 0 {
			t.Fatalf("%s: no run was continued: the property shows nothing", ret)
		}
		check(t, cfg, exact, o, ret, new(tally))
	})
}
