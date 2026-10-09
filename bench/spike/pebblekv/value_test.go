package pebblekv_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func TestLocate(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		t     time.Time
		ns    int64
		where pebblekv.Where
	}{
		"the epoch":        {engine.MinEventTime, 0, pebblekv.Inside},
		"before the epoch": {engine.MinEventTime.Add(-time.Nanosecond), 0, pebblekv.Before},
		"year 1":           {time.Time{}, 0, pebblekv.Before},
		"the last instant": {engine.MaxEventTime, math.MaxInt64, pebblekv.Inside},
		"after it":         {engine.MaxEventTime.Add(time.Nanosecond), math.MaxInt64, pebblekv.After},
		"year 9999":        {time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), math.MaxInt64, pebblekv.After},
		"an ordinary time": {time.Unix(1_700_000_000, 5).UTC(), 1_700_000_000*1e9 + 5, pebblekv.Inside},
	} {
		ns, where := pebblekv.Locate(tc.t)
		if ns != tc.ns || where != tc.where {
			t.Errorf("%s: Locate = %d, %d; want %d, %d", name, ns, where, tc.ns, tc.where)
		}
	}
	if got := pebblekv.Time(1_700_000_000*1e9 + 5); !got.Equal(time.Unix(1_700_000_000, 5)) || got.Location() != time.UTC {
		t.Errorf("Time = %v", got)
	}
}

// The value encoding is a stored format, so these bytes are a contract with data
// on disk: if one changes, the format changed, and that needs a new format byte
// and a migration, never a regenerated expectation.
func TestValueGoldenVectors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		v    pebblekv.Value
		hex  string
	}{
		{"observe, payload", pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, Payload: []byte("ab")}, "010105006162"},
		{"observe, seq past 2^32, TTL a minute", pebblekv.Value{Seq: 1 << 32, Kind: lifecycle.Observe, TTL: time.Minute}, "0101808080801080b09dc2df01"},
		{
			"run, largest seq below 2^63, through set, binary payload",
			pebblekv.Value{
				Seq: 1<<63 - 1, Kind: lifecycle.Observe, TTL: 5 * time.Minute,
				HasThrough: true, Through: 1_700_000_000_000_000_000 + 90*1e9, Payload: []byte{0, 0xFF},
			},
			"0105ffffffffffffffff7f80f092cbdd088088d4d4b2a2e7cb1700ff",
		},
		{"delete", pebblekv.Value{Seq: 7, Kind: lifecycle.Delete}, "01020700"},
		{"a through at the epoch is still a through", pebblekv.Value{Seq: 3, Kind: lifecycle.Observe, TTL: time.Second, HasThrough: true}, "0105038094ebdc0300"},
		{
			"every field at its largest",
			pebblekv.Value{Seq: math.MaxUint64, Kind: lifecycle.Observe, TTL: math.MaxInt64, HasThrough: true, Through: math.MaxInt64},
			"0105ffffffffffffffffff01ffffffffffffffff7fffffffffffffffff7f",
		},
	} {
		got := hex.EncodeToString(tc.v.Append(nil))
		if got != tc.hex {
			t.Errorf("%s: encoded %s, want %s", tc.name, got, tc.hex)
		}
		raw, _ := hex.DecodeString(tc.hex)
		back, err := pebblekv.DecodeValue(raw)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if back.Seq != tc.v.Seq || back.Kind != tc.v.Kind || back.TTL != tc.v.TTL || back.HasThrough != tc.v.HasThrough ||
			back.Through != tc.v.Through || !bytes.Equal(back.Payload, tc.v.Payload) {
			t.Errorf("%s: decoded %+v, want %+v", tc.name, back, tc.v)
		}
	}
}

func TestValueRejects(t *testing.T) {
	t.Parallel()
	good := pebblekv.Value{Seq: 9, Kind: lifecycle.Observe, TTL: time.Second, HasThrough: true, Through: 5}.Append(nil)
	for name, b := range map[string][]byte{
		"empty":                 nil,
		"header only":           {1},
		"another format":        {2, 1, 9, 0},
		"kind zero":             {1, 0, 9, 0},
		"kind three":            {1, 3, 9, 0},
		"an unknown flag":       {1, 0b1000_0001, 9, 0},
		"a basis beyond four":   {1, 0b0101_0001, 9, 0},
		"no sequence":           {1, 1},
		"a truncated sequence":  {1, 1, 0x80},
		"no TTL":                {1, 1, 9},
		"a TTL beyond int64":    append([]byte{1, 1, 9}, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01),
		"a flagged missing end": {1, 0b101, 9, 0},
		"a truncated through":   good[:len(good)-1],
	} {
		if _, err := pebblekv.DecodeValue(b); !errors.Is(err, pebblekv.ErrValue) {
			t.Errorf("%s: DecodeValue(%x) err = %v, want ErrValue", name, b, err)
		}
	}
}

func TestValueRoundTripsAnyRecord(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		kind := rapid.SampledFrom([]lifecycle.Kind{lifecycle.Observe, lifecycle.Delete}).Draw(t, "kind")
		v := pebblekv.Value{Seq: rapid.Uint64().Draw(t, "seq"), Kind: kind}
		if kind == lifecycle.Observe {
			v.TTL = time.Duration(rapid.Int64Range(0, math.MaxInt64).Draw(t, "ttl"))
			v.Payload = rapid.SliceOfN(rapid.Byte(), 0, 40).Draw(t, "payload")
			if rapid.Bool().Draw(t, "hasThrough") {
				v.HasThrough, v.Through = true, rapid.Int64Range(0, math.MaxInt64).Draw(t, "through")
			}
		}
		back, err := pebblekv.DecodeValue(v.Append(nil))
		if err != nil {
			t.Fatal(err)
		}
		if back.Seq != v.Seq || back.Kind != v.Kind || back.TTL != v.TTL || back.HasThrough != v.HasThrough ||
			back.Through != v.Through || !bytes.Equal(back.Payload, v.Payload) {
			t.Fatalf("decoded %+v, want %+v", back, v)
		}
	})
}

// FromRecord keeps a Through of exactly the event time, which is a run of one,
// distinct from no Through at all.
func TestFromRecordKeepsThrough(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0).UTC()
	r := engine.Record{Seq: 4, Kind: lifecycle.Observe, EventTime: at, TTL: time.Minute, Payload: []byte("x")}
	if v := pebblekv.FromRecord(r); v.HasThrough {
		t.Errorf("a record with no Through got one: %+v", v)
	}
	r.Through = at
	if v := pebblekv.FromRecord(r); !v.HasThrough || v.Through != at.UnixNano() {
		t.Errorf("a record whose Through equals its event time lost it: %+v", v)
	}
}

// aliveByVersions is the liveness rule the layouts apply, restated as the
// lifecycle fold sees it: for each producer, the newest assertion at or before t
// (by event time, then sequence) decides, and Value.Holds says whether an
// Observe still stands at t. The subject is alive if any producer's does.
func aliveByVersions(as []lifecycle.Assertion, t time.Time) bool {
	type newest struct {
		a  lifecycle.Assertion
		ok bool
	}
	byProducer := map[lifecycle.Producer]newest{}
	for _, a := range as {
		if a.EventTime.After(t) {
			continue
		}
		cur := byProducer[a.Producer]
		if !cur.ok || a.EventTime.After(cur.a.EventTime) || (a.EventTime.Equal(cur.a.EventTime) && a.Seq > cur.a.Seq) {
			byProducer[a.Producer] = newest{a, true}
		}
	}
	for _, n := range byProducer {
		r := engine.Record{Kind: n.a.Kind, EventTime: n.a.EventTime, Seq: n.a.Seq, TTL: n.a.TTL, Through: n.a.Through}
		if pebblekv.FromRecord(r).Holds(n.a.EventTime.UnixNano(), t.UnixNano()) {
			return true
		}
	}
	return false
}

// The rule is checked against the lifecycle fold itself on random histories, not
// trusted by copy: several producers, assertions at the same instant, deletes,
// TTLs, runs with a Through, and instants on, just before and just after every
// event time and deadline.
func TestHoldsAgreesWithTheFold(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0).UTC()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 10).Draw(t, "assertions")
		as := make([]lifecycle.Assertion, n)
		for i := range as {
			a := lifecycle.Assertion{
				Producer:  rapid.SampledFrom([]lifecycle.Producer{"a", "b", "c"}).Draw(t, "producer"),
				EventTime: base.Add(time.Duration(rapid.IntRange(0, 60).Draw(t, "at")) * time.Second),
				Seq:       uint64(i + 1),
				Kind:      rapid.SampledFrom([]lifecycle.Kind{lifecycle.Observe, lifecycle.Observe, lifecycle.Delete}).Draw(t, "kind"),
			}
			if a.Kind == lifecycle.Observe {
				a.TTL = time.Duration(rapid.SampledFrom([]int{0, 0, 1, 5, 20, 90}).Draw(t, "ttl")) * time.Second
				if rapid.Bool().Draw(t, "run") {
					a.Through = a.EventTime.Add(time.Duration(rapid.IntRange(0, 40).Draw(t, "through")) * time.Second)
				}
			}
			as[i] = a
		}
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		var probes []time.Time
		for _, a := range as {
			for _, x := range []time.Time{a.EventTime, a.Through, a.Through.Add(a.TTL), a.EventTime.Add(a.TTL)} {
				if x.IsZero() {
					continue
				}
				probes = append(probes, x.Add(-time.Nanosecond), x, x.Add(time.Nanosecond))
			}
		}
		for _, p := range probes {
			if got, want := aliveByVersions(as, p), tl.AliveAt(p); got != want {
				t.Fatalf("at %s: by versions %v, by the fold %v\nassertions: %+v", p.Format(time.RFC3339Nano), got, want, as)
			}
		}
	})
}

func TestHoldsAtTheEdges(t *testing.T) {
	t.Parallel()
	obs := func(ttl time.Duration, through int64) pebblekv.Value {
		return pebblekv.Value{Kind: lifecycle.Observe, TTL: ttl, HasThrough: through != 0, Through: through}
	}
	const e = int64(1000)
	for name, tc := range map[string]struct {
		v    pebblekv.Value
		t    int64
		want bool
	}{
		"watch mode holds":                     {obs(0, 0), e + 1e12, true},
		"watch mode holds at the last instant": {obs(0, 0), math.MaxInt64, true},
		"a TTL holds up to the deadline":       {obs(10, 0), e + 9, true},
		"and not at it":                        {obs(10, 0), e + 10, false},
		"a run's deadline counts from Through": {obs(10, e+100), e + 109, true},
		"and not at it, either":                {obs(10, e+100), e + 110, false},
		"a Through before the event is moot":   {obs(10, e-5), e + 9, true},
		"a delete holds nothing":               {pebblekv.Value{Kind: lifecycle.Delete}, e, false},
		"a deadline at the last instant":       {obs(10, math.MaxInt64-10), math.MaxInt64 - 1, true},
		"which has passed at it":               {obs(10, math.MaxInt64-10), math.MaxInt64, false},
	} {
		if got := tc.v.Holds(e, tc.t); got != tc.want {
			t.Errorf("%s: Holds = %v, want %v", name, got, tc.want)
		}
	}
}
