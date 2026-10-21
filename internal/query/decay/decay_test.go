package decay_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// at is t0 plus d.
func at(d time.Duration) time.Time { return t0.Add(d) }

const (
	ttl6h  = 6 * time.Hour // X = 3h and h = 45m under the default policy
	minute = time.Minute
	hour   = time.Hour
)

func evid(p string, event, through time.Time, ttl time.Duration) interval.Evidence {
	return interval.Evidence{Producer: lifecycle.Producer(p), EventTime: event, Through: through, TTL: ttl}
}

func evidL(p string, event, through, latest time.Time, ttl time.Duration) interval.Evidence {
	e := evid(p, event, through, ttl)
	e.Latest = latest
	return e
}

func TestPolicy(t *testing.T) {
	p := decay.DefaultPolicy()
	if p.Version != 1 || p.ExtendFraction != 0.5 || p.HalfLifeOfX != 0.25 {
		t.Errorf("DefaultPolicy() = %+v", p)
	}
	if err := p.Validate(); err != nil {
		t.Errorf("DefaultPolicy() is invalid: %v", err)
	}
	if x, h := p.Extension(ttl6h), p.HalfLife(ttl6h); x != 3*hour || h != 45*minute {
		t.Errorf("at a 6h TTL: X = %s, h = %s; want 3h and 45m", x, h)
	}
	if x, h := p.Extension(15*minute), p.HalfLife(15*minute); x != 7*minute+30*time.Second || h != 112500*time.Millisecond {
		t.Errorf("at a 15m TTL: X = %s, h = %s", x, h)
	}
	for name, bad := range map[string]decay.Policy{
		"no fraction":        {Version: 1, HalfLifeOfX: 0.25},
		"fraction above one": {Version: 1, ExtendFraction: 1.01, HalfLifeOfX: 0.25},
		"negative fraction":  {Version: 1, ExtendFraction: -0.5, HalfLifeOfX: 0.25},
		"NaN fraction":       {Version: 1, ExtendFraction: math.NaN(), HalfLifeOfX: 0.25},
		"no half-life":       {Version: 1, ExtendFraction: 0.5},
		"infinite half-life": {Version: 1, ExtendFraction: 0.5, HalfLifeOfX: math.Inf(1)},
		"NaN half-life":      {Version: 1, ExtendFraction: 0.5, HalfLifeOfX: math.NaN()},
	} {
		if err := bad.Validate(); !errors.Is(err, decay.ErrPolicy) {
			t.Errorf("%s: Validate() = %v, want an error wrapping ErrPolicy", name, err)
		}
		if _, ok := decay.Of([]interval.Evidence{evid("a", t0, time.Time{}, ttl6h)}, at(5*hour), bad); ok {
			t.Errorf("%s: Of gave a recency under an invalid policy", name)
		}
	}
}

func TestOf(t *testing.T) {
	p := decay.DefaultPolicy()
	for _, c := range []struct {
		name string
		last []interval.Evidence
		at   time.Time
		ok   bool
		prod string
		seen time.Time
		sil  time.Duration
		conf float64
	}{
		{name: "no evidence", at: at(hour)},
		{
			name: "a silence inside X shows nothing, however late in it",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)},
			at:   at(3*hour - time.Nanosecond), ok: true, prod: "a", seen: t0, sil: 0, conf: 1,
		},
		{
			name: "a silence of exactly X is still none",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)},
			at:   at(3 * hour), ok: true, prod: "a", seen: t0, sil: 0, conf: 1,
		},
		{
			name: "one half-life past X halves it",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)},
			at:   at(3*hour + 45*minute), ok: true, prod: "a", seen: t0, sil: 45 * minute, conf: 0.5,
		},
		{
			name: "two half-lives past X quarter it",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)},
			at:   at(3*hour + 90*minute), ok: true, prod: "a", seen: t0, sil: 90 * minute, conf: 0.25,
		},
		{
			name: "the end of a member's life, a TTL after the sighting, is 2^-4",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)},
			at:   at(ttl6h - time.Nanosecond), ok: true, prod: "a", seen: t0, sil: 3*hour - time.Nanosecond,
			conf: math.Exp2(-4 * float64(3*hour-time.Nanosecond) / float64(3*hour)),
		},
		{
			name: "a Through moves the last instant, and X still applies after it",
			last: []interval.Evidence{evid("a", t0, at(10*hour), ttl6h)},
			at:   at(14 * hour), ok: true, prod: "a", seen: at(10 * hour), sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "a Through inside X of the instant shows nothing",
			last: []interval.Evidence{evid("a", t0, at(10*hour), ttl6h)},
			at:   at(11 * hour), ok: true, prod: "a", seen: at(10 * hour), sil: 0, conf: 1,
		},
		{
			name: "a Through before the event time is no Through",
			last: []interval.Evidence{evid("a", at(2*hour), at(hour), ttl6h)},
			at:   at(7 * hour), ok: true, prod: "a", seen: at(2 * hour), sil: 2 * hour, conf: math.Exp2(-120.0 / 45.0),
		},
		{
			name: "a Through after the instant is clipped to it",
			last: []interval.Evidence{evid("a", t0, at(10*hour), ttl6h)},
			at:   at(5 * hour), ok: true, prod: "a", seen: at(5 * hour), sil: 0, conf: 1,
		},
		{
			name: "an event time after the instant is clipped to it",
			last: []interval.Evidence{evid("a", at(5*hour), time.Time{}, ttl6h)},
			at:   at(hour), ok: true, prod: "a", seen: at(hour), sil: 0, conf: 1,
		},
		{
			name: "a run starting exactly at the instant",
			last: []interval.Evidence{evid("a", at(hour), time.Time{}, ttl6h)},
			at:   at(hour), ok: true, prod: "a", seen: at(hour), sil: 0, conf: 1,
		},
		{
			name: "a later last instant among the producer's records moves the last instant, and X still applies",
			last: []interval.Evidence{evidL("a", at(hour), at(2*hour), at(10*hour), ttl6h)},
			at:   at(14 * hour), ok: true, prod: "a", seen: at(10 * hour), sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "a last instant among the records earlier than the deciding record's own changes nothing",
			last: []interval.Evidence{evidL("a", at(5*hour), time.Time{}, at(2*hour), ttl6h)},
			at:   at(9 * hour), ok: true, prod: "a", seen: at(5 * hour), sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "a later last instant is clipped to the instant",
			last: []interval.Evidence{evidL("a", t0, time.Time{}, at(10*hour), ttl6h)},
			at:   at(5 * hour), ok: true, prod: "a", seen: at(5 * hour), sil: 0, conf: 1,
		},
		{
			name: "no TTL, no recency",
			last: []interval.Evidence{evid("a", t0, time.Time{}, 0)},
			at:   at(100 * hour),
		},
		{
			name: "a producer with no TTL among the live ones: no recency, not the other's",
			last: []interval.Evidence{evid("a", at(9*hour), time.Time{}, ttl6h), evid("b", t0, time.Time{}, 0)},
			at:   at(10 * hour),
		},
		{
			name: "the same, in the other order",
			last: []interval.Evidence{evid("b", t0, time.Time{}, 0), evid("a", at(9*hour), time.Time{}, ttl6h)},
			at:   at(10 * hour),
		},
		{
			name: "two producers: the one seen more recently, whatever the order",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h), evid("b", at(hour), time.Time{}, ttl6h)},
			at:   at(5 * hour), ok: true, prod: "b", seen: at(hour), sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "the same, reversed",
			last: []interval.Evidence{evid("b", at(hour), time.Time{}, ttl6h), evid("a", t0, time.Time{}, ttl6h)},
			at:   at(5 * hour), ok: true, prod: "b", seen: at(hour), sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "producers of different TTLs: the smaller bound, not the later sighting",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h), evid("b", at(hour), time.Time{}, hour)},
			at:   at(4 * hour), ok: true, prod: "a", seen: t0, sil: hour, conf: math.Exp2(-60.0 / 45.0),
		},
		{
			name: "equal bounds of zero: the later sighting",
			last: []interval.Evidence{evid("a", t0, time.Time{}, ttl6h), evid("b", at(hour), time.Time{}, ttl6h)},
			at:   at(2 * hour), ok: true, prod: "b", seen: at(hour), sil: 0, conf: 1,
		},
		{
			name: "equal in everything: the smaller name",
			last: []interval.Evidence{evid("b", t0, time.Time{}, ttl6h), evid("a", t0, time.Time{}, ttl6h)},
			at:   at(2 * hour), ok: true, prod: "a", seen: t0, sil: 0, conf: 1,
		},
		{
			name: "a TTL too short to have an extension interval: any silence has no confidence",
			last: []interval.Evidence{evid("a", t0, time.Time{}, time.Nanosecond)},
			at:   at(time.Second), ok: true, prod: "a", seen: t0, sil: time.Second, conf: 0,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := decay.Of(c.last, c.at, p)
			if ok != c.ok {
				t.Fatalf("ok = %t, want %t (%+v)", ok, c.ok, got)
			}
			if !ok {
				if got != (decay.Recency{}) {
					t.Errorf("no recency, but %+v", got)
				}
				return
			}
			if string(got.Producer) != c.prod || !got.LastSeen.Equal(c.seen) || got.SilentAtLeast != c.sil || math.Abs(got.Confidence-c.conf) > 1e-12 {
				t.Errorf("got %+v, want producer %s, seen %s, silent %s, confidence %g", got, c.prod, c.seen, c.sil, c.conf)
			}
		})
	}
}

// Of is a function of its arguments: it changes neither the evidence nor, being
// pure, anything else, and the same call gives the same answer.
func TestOfLeavesItsArgumentsAlone(t *testing.T) {
	last := []interval.Evidence{evid("b", at(hour), at(2*hour), ttl6h), evid("a", t0, time.Time{}, ttl6h)}
	want := append([]interval.Evidence(nil), last...)
	r1, ok1 := decay.Of(last, at(9*hour), decay.DefaultPolicy())
	r2, ok2 := decay.Of(last, at(9*hour), decay.DefaultPolicy())
	if !ok1 || !ok2 || r1 != r2 {
		t.Errorf("two calls: %+v %t, %+v %t", r1, ok1, r2, ok2)
	}
	for i := range last {
		if last[i] != want[i] {
			t.Errorf("Of changed its evidence: %+v", last)
		}
	}
}

// RFC 2439's composability, with nothing stored: once the silence is past X,
// waiting a further d multiplies the confidence by 2^(-d/h), however d is cut
// in two.
func TestOfComposes(t *testing.T) {
	p := decay.DefaultPolicy()
	last := []interval.Evidence{evid("a", t0, time.Time{}, ttl6h)}
	h := float64(p.HalfLife(ttl6h))
	base := at(3*hour + 10*minute) // past X
	m, ok := decay.Of(last, base, p)
	if !ok || m.SilentAtLeast != 10*minute {
		t.Fatalf("base: %+v %t", m, ok)
	}
	for _, split := range [][2]time.Duration{{0, 90 * minute}, {10 * minute, 80 * minute}, {45 * minute, 45 * minute}, {89 * minute, minute}} {
		d1, d2 := split[0], split[1]
		f1, _ := decay.Of(last, base.Add(d1), p)
		f12, _ := decay.Of(last, base.Add(d1+d2), p)
		if got, want := f12.Confidence/f1.Confidence, math.Exp2(-float64(d2)/h); math.Abs(got-want) > 1e-12 {
			t.Errorf("f(f(m, %s), %s)/f(m, %s) = %g, want %g", d1, d2, d1, got, want)
		}
		if got, want := f12.Confidence, m.Confidence*math.Exp2(-float64(d1+d2)/h); math.Abs(got-want) > 1e-12 {
			t.Errorf("f(m, %s) = %g, want f(m, 0) x 2^(-%s/h) = %g", d1+d2, got, d1+d2, want)
		}
	}
}
