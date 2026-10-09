package pebblekv

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func TestLocate(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		t     time.Time
		ns    int64
		where Where
	}{
		"the epoch":        {store.MinEventTime, 0, Inside},
		"before the epoch": {store.MinEventTime.Add(-time.Nanosecond), 0, Before},
		"year 1":           {time.Time{}, 0, Before},
		"the last instant": {store.MaxEventTime, math.MaxInt64, Inside},
		"after it":         {store.MaxEventTime.Add(time.Nanosecond), math.MaxInt64, After},
		"year 9999":        {time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), math.MaxInt64, After},
		"an ordinary time": {time.Unix(1_700_000_000, 5).UTC(), 1_700_000_000*1e9 + 5, Inside},
	} {
		ns, where := Locate(tc.t)
		if ns != tc.ns || where != tc.where {
			t.Errorf("%s: Locate = %d, %d; want %d, %d", name, ns, where, tc.ns, tc.where)
		}
	}
	if got := Time(1_700_000_000*1e9 + 5); !got.Equal(time.Unix(1_700_000_000, 5)) || got.Location() != time.UTC {
		t.Errorf("Time = %v", got)
	}
}

func sameValue(a, b Value) bool {
	return a.Seq == b.Seq && a.Kind == b.Kind && a.TTL == b.TTL && a.HasThrough == b.HasThrough &&
		a.Through == b.Through && a.Boot == b.Boot && a.Basis == b.Basis && bytes.Equal(a.Payload, b.Payload)
}

// The value encoding is a stored format, so these bytes are a contract with data
// on disk: if one changes, the format changed, and that needs a new format byte
// and a migration, never a regenerated expectation. The first group is what the
// spike wrote, which a value without a boot must still encode to, byte for byte.
func TestValueGoldenVectors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		v    Value
		hex  string
	}{
		{"observe, payload", Value{Seq: 5, Kind: lifecycle.Observe, Payload: []byte("ab")}, "010105006162"},
		{"observe, seq past 2^32, TTL a minute", Value{Seq: 1 << 32, Kind: lifecycle.Observe, TTL: time.Minute}, "0101808080801080b09dc2df01"},
		{
			"run, largest seq below 2^63, through set, binary payload",
			Value{
				Seq: 1<<63 - 1, Kind: lifecycle.Observe, TTL: 5 * time.Minute,
				HasThrough: true, Through: 1_700_000_000_000_000_000 + 90*1e9, Payload: []byte{0, 0xFF},
			},
			"0105ffffffffffffffff7f80f092cbdd088088d4d4b2a2e7cb1700ff",
		},
		{"delete", Value{Seq: 7, Kind: lifecycle.Delete}, "01020700"},
		{"a through at the epoch is still a through", Value{Seq: 3, Kind: lifecycle.Observe, TTL: time.Second, HasThrough: true}, "0105038094ebdc0300"},
		{
			"every field at its largest",
			Value{Seq: math.MaxUint64, Kind: lifecycle.Observe, TTL: math.MaxInt64, HasThrough: true, Through: math.MaxInt64},
			"0105ffffffffffffffffff01ffffffffffffffff7fffffffffffffffff7f",
		},

		// With a boot: flag bit 3, then the boot as a varint length and its bytes,
		// after the Through and before the payload.
		{"boot, payload, no through", Value{Seq: 5, Kind: lifecycle.Observe, Boot: "b1", Payload: []byte("ab")}, "010905000262316162"},
		{"boot, no payload, no through", Value{Seq: 1, Kind: lifecycle.Observe, Boot: "x"}, "010901000178"},
		{
			"boot and through, no payload",
			Value{Seq: 3, Kind: lifecycle.Observe, TTL: time.Second, HasThrough: true, Boot: "boot-1"},
			"010d038094ebdc030006626f6f742d31",
		},
		{
			"boot, through and binary payload",
			Value{Seq: 9, Kind: lifecycle.Observe, HasThrough: true, Through: 5, Boot: "uuid", Payload: []byte{0, 0xFF}},
			"010d090005047575696400ff",
		},
		{
			"a boot of 256 bytes takes a two byte length",
			Value{Seq: 1, Kind: lifecycle.Observe, Boot: strings.Repeat("a", 256), Payload: []byte{1}},
			"010901008002" + strings.Repeat("61", 256) + "01",
		},

		// With an event-time basis: bits 4 to 6 of the flags byte. A basis of zero
		// costs nothing, which the vectors above show.
		{"basis 1 (an object field)", Value{Seq: 5, Kind: lifecycle.Observe, Basis: 1, Payload: []byte("ab")}, "011105006162"},
		{"basis 2 (observed), on a delete", Value{Seq: 7, Kind: lifecycle.Delete, Basis: 2}, "01220700"},
		{"basis 3 (receipt)", Value{Seq: 1, Kind: lifecycle.Observe, Basis: 3}, "01310100"},
		{"basis 4 (producer event)", Value{Seq: 1, Kind: lifecycle.Observe, Basis: 4}, "01410100"},
		{"basis 4 on a delete", Value{Seq: 7, Kind: lifecycle.Delete, Basis: 4}, "01420700"},
		{"basis 2 with a boot", Value{Seq: 1, Kind: lifecycle.Observe, Basis: 2, Boot: "x"}, "012901000178"},
		{
			"basis 3 with a through and a binary payload",
			Value{Seq: 9, Kind: lifecycle.Observe, Basis: 3, HasThrough: true, Through: 5, Payload: []byte{0, 0xFF}},
			"013509000500ff",
		},
		{
			"basis 4 with a boot, a through and a payload",
			Value{Seq: 3, Kind: lifecycle.Observe, TTL: time.Second, Basis: 4, HasThrough: true, Boot: "boot-1", Payload: []byte("p")},
			"014d038094ebdc030006626f6f742d3170",
		},
		{
			"basis 1 with a boot, a through and a payload",
			Value{Seq: 3, Kind: lifecycle.Observe, TTL: time.Second, Basis: 1, HasThrough: true, Boot: "boot-1", Payload: []byte("p")},
			"011d038094ebdc030006626f6f742d3170",
		},
	} {
		got := hex.EncodeToString(tc.v.Append(nil))
		if got != tc.hex {
			t.Errorf("%s: encoded %s, want %s", tc.name, got, tc.hex)
		}
		raw, _ := hex.DecodeString(tc.hex)
		back, err := DecodeValue(raw)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if !sameValue(back, tc.v) {
			t.Errorf("%s: decoded %+v, want %+v", tc.name, back, tc.v)
		}
	}
}

// Every record that passes validation is a value that passes Check, and comes
// back from its encoding unchanged: the panic in Append cannot be reached from a
// valid record. The streams are fixed, with every feature of the generator on.
func TestEveryValidRecordIsAValueThatCanBeKept(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 6; seed++ {
		c := storetest.Tiny()
		c.Seed = seed
		c.Runs = true
		c.RebootProbability, c.CloneProbability = 0.2, 0.5
		c.LateProbability, c.ConfirmProbability = 0.2, 0.5
		g, err := storetest.NewGenerator(c)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		records := g.All()
		for b := store.BasisUnknown; b <= store.BasisProducerEvent; b++ {
			r := records[0]
			r.EventTimeBasis = b
			records = append(records, r)
		}
		for _, r := range records {
			if err := r.Validate(); err != nil {
				t.Fatalf("seed %d: the generator made an invalid record: %v", seed, err)
			}
			v := FromRecord(r)
			if err := v.Check(); err != nil {
				t.Fatalf("seed %d: Check of a valid record's value: %v\n%+v", seed, err, r)
			}
			back, err := DecodeValue(v.Append(nil))
			if err != nil || !sameValue(back, v) {
				t.Fatalf("seed %d: the value did not survive its encoding: %+v, %v", seed, back, err)
			}
		}
	}
}

// A value that would be stored as something else, or that could not be read back,
// is never encoded: the basis would be masked to its low three bits, a kind of 4
// would set the Through bit.
func TestAppendRefusesWhatItCannotKeep(t *testing.T) {
	t.Parallel()
	ok := Value{Seq: 1, Kind: lifecycle.Observe}
	if err := ok.Check(); err != nil {
		t.Fatalf("Check of a good value = %v", err)
	}
	bad := map[string]Value{
		"basis 5":        {Seq: 1, Kind: lifecycle.Observe, Basis: 5},
		"basis 8":        {Seq: 1, Kind: lifecycle.Observe, Basis: 8},
		"basis 12":       {Seq: 1, Kind: lifecycle.Observe, Basis: 12},
		"kind 0":         {Seq: 1},
		"kind 3":         {Seq: 1, Kind: 3},
		"kind 4":         {Seq: 1, Kind: 4},
		"negative TTL":   {Seq: 1, Kind: lifecycle.Observe, TTL: -1},
		"negative reach": {Seq: 1, Kind: lifecycle.Observe, HasThrough: true, Through: -1},
		"a stray reach":  {Seq: 1, Kind: lifecycle.Observe, Through: 5},
		"a long boot":    {Seq: 1, Kind: lifecycle.Observe, Boot: strings.Repeat("a", store.MaxBootLen+1)},
	}
	for name, v := range bad {
		if err := v.Check(); !errors.Is(err, ErrValue) {
			t.Errorf("%s: Check = %v, want ErrValue", name, err)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: Append did not panic", name)
				}
			}()
			v.Append(nil)
		}()
	}
}

func TestValueRejects(t *testing.T) {
	t.Parallel()
	good := Value{Seq: 9, Kind: lifecycle.Observe, TTL: time.Second, HasThrough: true, Through: 5}.Append(nil)
	withBoot := Value{Seq: 9, Kind: lifecycle.Observe, Boot: "abc", Payload: []byte("p")}.Append(nil)
	cases := map[string][]byte{
		"empty":                    nil,
		"header only":              {1},
		"another format":           {2, 1, 9, 0},
		"kind zero":                {1, 0, 9, 0},
		"kind three":               {1, 3, 9, 0},
		"no sequence":              {1, 1},
		"a truncated sequence":     {1, 1, 0x80},
		"no TTL":                   {1, 1, 9},
		"a TTL beyond int64":       append([]byte{1, 1, 9}, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01),
		"a flagged missing end":    {1, 0b101, 9, 0},
		"a truncated through":      good[:len(good)-1],
		"a flagged missing boot":   {1, 0b1001, 9, 0},
		"a truncated boot length":  {1, 0b1001, 9, 0, 0x80},
		"a boot longer than value": {1, 0b1001, 9, 0, 5, 'a', 'b'},
		"a boot of length zero":    {1, 0b1001, 9, 0, 0, 'p'},
		"a boot length beyond int": append([]byte{1, 0b1001, 9, 0}, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01),
		"a truncated boot":         withBoot[:6],
		"a through flagged before the boot but missing": {1, 0b1101, 9, 0},
	}
	// Basis values 5, 6 and 7 are refused, as the three flag bits 4 to 6 hold them,
	// alone and beside a boot.
	for basis := byte(5); basis <= 7; basis++ {
		n := string(rune('0' + basis))
		cases["basis "+n] = []byte{1, 1 | basis<<4, 9, 0}
		cases["basis "+n+" beside a boot"] = []byte{1, 1 | 0b1000 | basis<<4, 9, 0, 1, 'a'}
	}
	// Bit 7 is reserved, to say that a second flags byte follows, and is refused
	// alone, beside each basis and beside a boot, in a value that is well formed
	// otherwise, so the flag is the only thing wrong with it.
	cases["reserved flag bit 7"] = []byte{1, 1 | 0x80, 9, 0}
	for basis := byte(1); basis <= 4; basis++ {
		cases["reserved flag bit 7 beside basis "+string(rune('0'+basis))] = []byte{1, 1 | 0x80 | basis<<4, 9, 0}
	}
	cases["reserved flag bit 7 beside a boot"] = []byte{1, 1 | 0b1000 | 0x80, 9, 0, 1, 'a'}
	cases["kind three with a basis"] = []byte{1, 3 | 0b01_0000, 9, 0}
	cases["kind three with basis 4"] = []byte{1, 3 | 0b100_0000, 9, 0}
	cases["kind zero with a basis"] = []byte{1, 0b10_0000, 9, 0}
	cases["kind zero with basis 4"] = []byte{1, 0b100_0000, 9, 0}
	cases["a boot of 257 bytes"] = append([]byte{1, 0b1001, 9, 0, 0x81, 0x02}, bytes.Repeat([]byte{'a'}, 257)...)
	for name, b := range cases {
		if _, err := DecodeValue(b); !errors.Is(err, ErrValue) {
			t.Errorf("%s: DecodeValue(%x) err = %v, want ErrValue", name, b, err)
		}
	}
}

// A boot of exactly store.MaxBootLen bytes is a value; one byte more is not, so a
// stored value cannot carry a boot the store would refuse. (The golden vectors
// hold the 256 byte case; this holds the edge itself, written by Append, and the
// refusal of one byte more.)
func TestTheLongestBootIsTheStoresLimit(t *testing.T) {
	t.Parallel()
	if store.MaxBootLen != 256 {
		t.Fatalf("store.MaxBootLen = %d: the vectors assume 256", store.MaxBootLen)
	}
	for n, wantOK := range map[int]bool{1: true, 255: true, 256: true, 257: false, 1000: false} {
		v := Value{Seq: 1, Kind: lifecycle.Observe, Boot: strings.Repeat("b", n), Payload: []byte("p")}
		if !wantOK {
			// Append refuses it (TestAppendRefusesWhatItCannotKeep), and DecodeValue
			// refuses the bytes of one (TestValueRejects).
			if err := v.Check(); !errors.Is(err, ErrValue) {
				t.Errorf("a boot of %d bytes passed Check: err = %v, want ErrValue", n, err)
			}
			continue
		}
		back, err := DecodeValue(v.Append(nil))
		if err != nil || !sameValue(back, v) {
			t.Errorf("a boot of %d bytes: %+v, %v", n, back, err)
		}
	}
}

// A value written before the boot flag existed decodes with no boot, and an
// empty Boot encodes to those same bytes: there is one encoding of a value
// without a boot.
func TestAnEmptyBootIsNotWritten(t *testing.T) {
	t.Parallel()
	v := Value{Seq: 5, Kind: lifecycle.Observe, Payload: []byte("ab")}
	if got := v.Append(nil); !bytes.Equal(got, []byte{1, 1, 5, 0, 'a', 'b'}) {
		t.Errorf("a value with no boot encodes to %x", got)
	}
	back, err := DecodeValue([]byte{1, 1, 5, 0, 'a', 'b'})
	if err != nil || back.Boot != "" || string(back.Payload) != "ab" {
		t.Errorf("DecodeValue = %+v, %v", back, err)
	}
}

func TestValueRoundTripsAnyRecord(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		kind := rapid.SampledFrom([]lifecycle.Kind{lifecycle.Observe, lifecycle.Delete}).Draw(t, "kind")
		v := Value{Seq: rapid.Uint64().Draw(t, "seq"), Kind: kind, Basis: rapid.Uint8Range(0, 4).Draw(t, "basis")}
		if kind == lifecycle.Observe {
			v.TTL = time.Duration(rapid.Int64Range(0, math.MaxInt64).Draw(t, "ttl"))
			v.Payload = rapid.SliceOfN(rapid.Byte(), 0, 40).Draw(t, "payload")
			v.Boot = rapid.StringN(0, 40, 80).Draw(t, "boot")
			if rapid.Bool().Draw(t, "hasThrough") {
				v.HasThrough, v.Through = true, rapid.Int64Range(0, math.MaxInt64).Draw(t, "through")
			}
		}
		back, err := DecodeValue(v.Append(nil))
		if err != nil {
			t.Fatal(err)
		}
		if !sameValue(back, v) {
			t.Fatalf("decoded %+v, want %+v", back, v)
		}
	})
}

// FromRecord carries the clock that stamped the event time, and the encoding
// keeps it, for every basis there is.
func TestFromRecordCarriesTheEventTimeBasis(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0).UTC()
	for b := store.BasisUnknown; b <= store.BasisProducerEvent; b++ {
		r := store.Record{Seq: 4, Kind: lifecycle.Observe, EventTime: at, EventTimeBasis: b, Payload: []byte("x")}
		v := FromRecord(r)
		if v.Basis != uint8(b) {
			t.Errorf("FromRecord dropped basis %v: got %d", b, v.Basis)
		}
		back, err := DecodeValue(v.Append(nil))
		if err != nil || back.Basis != uint8(b) {
			t.Errorf("basis %v did not survive the encoding: %+v, %v", b, back, err)
		}
	}
}

// FromRecord keeps a Through of exactly the event time, which is a run of one,
// distinct from no Through at all, and keeps the boot.
func TestFromRecordKeepsThroughAndBoot(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0).UTC()
	r := store.Record{Seq: 4, Kind: lifecycle.Observe, EventTime: at, TTL: time.Minute, Payload: []byte("x")}
	if v := FromRecord(r); v.HasThrough || v.Boot != "" {
		t.Errorf("a record with no Through and no boot got one: %+v", v)
	}
	r.Through = at
	if v := FromRecord(r); !v.HasThrough || v.Through != at.UnixNano() {
		t.Errorf("a record whose Through equals its event time lost it: %+v", v)
	}
	r.Boot = "boot-7"
	v := FromRecord(r)
	if v.Boot != "boot-7" {
		t.Errorf("FromRecord lost the boot: %+v", v)
	}
	back, err := DecodeValue(v.Append(nil))
	if err != nil || back.Boot != "boot-7" || string(back.Payload) != "x" {
		t.Errorf("the boot did not survive the encoding: %+v, %v", back, err)
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
		r := store.Record{Kind: n.a.Kind, EventTime: n.a.EventTime, Seq: n.a.Seq, TTL: n.a.TTL, Through: n.a.Through}
		if FromRecord(r).Holds(n.a.EventTime.UnixNano(), t.UnixNano()) {
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
	obs := func(ttl time.Duration, through int64) Value {
		return Value{Kind: lifecycle.Observe, TTL: ttl, HasThrough: through != 0, Through: through}
	}
	const e = int64(1000)
	for name, tc := range map[string]struct {
		v    Value
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
		"a delete holds nothing":               {Value{Kind: lifecycle.Delete}, e, false},
		"a deadline at the last instant":       {obs(10, math.MaxInt64-10), math.MaxInt64 - 1, true},
		"which has passed at it":               {obs(10, math.MaxInt64-10), math.MaxInt64, false},
	} {
		if got := tc.v.Holds(e, tc.t); got != tc.want {
			t.Errorf("%s: Holds = %v, want %v", name, got, tc.want)
		}
	}
}
