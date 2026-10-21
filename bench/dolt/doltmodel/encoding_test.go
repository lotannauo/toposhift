package doltmodel_test

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// testingT is what fpHash needs of a *testing.T or a *rapid.T.
type testingT interface {
	Helper()
	Fatal(args ...any)
}

func fpHash(t testingT, typ catalog.EntityType, hash [identity.FingerprintBytes]byte) identity.Fingerprint {
	t.Helper()
	f, err := identity.FingerprintFromHash(typ, hash)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// checkFingerprintEncoding is what an encoding of fingerprints must be: the bytes pinned
// (a separator that is not the zero byte is another format), and the bytewise order of the
// encodings the contract's order: a type that is a prefix of another sorts first, the type
// decides before the hash, and hashes differing in the first or the last byte decide.
func checkFingerprintEncoding(enc func(identity.Fingerprint) []byte) error {
	var h [identity.FingerprintBytes]byte
	for i := range h {
		h[i] = byte(0xf0 + i)
	}
	f, err := identity.FingerprintFromHash("k8s.pod", h)
	if err != nil {
		return err
	}
	if got, want := enc(f), append([]byte("k8s.pod\x00"), h[:]...); !bytes.Equal(got, want) {
		return fmt.Errorf("encoding %x, want %x", got, want)
	}

	var lo, hi, firstDiffers, lastDiffers [identity.FingerprintBytes]byte
	for i := range hi {
		hi[i] = 0xff
	}
	firstDiffers[0] = 1
	lastDiffers[identity.FingerprintBytes-1] = 1
	mk := func(typ catalog.EntityType, hash [identity.FingerprintBytes]byte) identity.Fingerprint {
		f, err := identity.FingerprintFromHash(typ, hash)
		if err != nil {
			panic(err)
		}
		return f
	}
	fps := []identity.Fingerprint{
		mk("k8s.pod", lo), mk("k8s.pod", lastDiffers), mk("k8s.pod", firstDiffers), mk("k8s.pod", hi),
		mk("k8s.pod", lo), // equal to the first
		mk("k8s", hi), mk("k8s", lo),
		mk("k8s.pod.x", lo), mk("k8s0pod", lo), mk("k8s_pod", lo), mk("k8s.podx", lo),
		mk("service", hi), mk("service.instance", lo),
	}
	for _, a := range fps {
		for _, b := range fps {
			got := bytes.Compare(enc(a), enc(b))
			if want := engine.CompareFingerprints(a, b); got != want {
				return fmt.Errorf("encodings of %s and %s compare %d, engine.CompareFingerprints %d", a, b, got, want)
			}
		}
	}
	return nil
}

func TestEncodedFingerprintsAreTheTypeAZeroByteAndTheHashAndSortAsTheContractDoes(t *testing.T) {
	if err := checkFingerprintEncoding(doltmodel.EncodeFingerprint); err != nil {
		t.Fatal(err)
	}
}

// TestFingerprintEncodingMutantsAreCaught: another separator, none, or the type's length in
// front are encodings that look alike, and the check refuses each.
func TestFingerprintEncodingMutantsAreCaught(t *testing.T) {
	with := func(sep []byte) func(identity.Fingerprint) []byte {
		return func(f identity.Fingerprint) []byte {
			h := f.Hash()
			return append(append([]byte(f.Type()), sep...), h[:]...)
		}
	}
	for name, enc := range map[string]func(identity.Fingerprint) []byte{
		"separator 0xff": with([]byte{0xff}),
		"separator '/'":  with([]byte{'/'}),
		"separator 0x01": with([]byte{1}),
		"no separator":   with(nil),
		"text form":      func(f identity.Fingerprint) []byte { return []byte(f.String()) },
		"hash before type": func(f identity.Fingerprint) []byte {
			h := f.Hash()
			return append(append(h[:0:0], h[:]...), append([]byte{0}, f.Type()...)...)
		},
		"length then type": func(f identity.Fingerprint) []byte {
			h := f.Hash()
			return append(append([]byte{byte(len(f.Type()))}, f.Type()...), h[:]...)
		},
	} {
		if err := checkFingerprintEncoding(enc); err == nil {
			t.Errorf("%s passed", name)
		}
	}
}

func TestEncodedFingerprintsSortAsTheContractDoesRandomly(t *testing.T) {
	names := []catalog.EntityType{"a", "a.b", "a.b.c", "a_b", "ab", "b", "k8s.pod", "k8s.pod.x"}
	draw := func(t *rapid.T, label string) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		// Few distinct bytes, so that equal prefixes of hashes are common.
		for i := range h {
			h[i] = byte(rapid.SampledFrom([]int{0, 1, 255}).Draw(t, label))
		}
		return fpHash(t, rapid.SampledFrom(names).Draw(t, label+" type"), h)
	}
	rapid.Check(t, func(t *rapid.T) {
		a, b := draw(t, "a"), draw(t, "b")
		if got, want := doltmodel.CompareEncoded(doltmodel.EncodeFingerprint(a), doltmodel.EncodeFingerprint(b)), engine.CompareFingerprints(a, b); got != want {
			t.Fatalf("%s and %s: encodings compare %d, the contract %d", a, b, got, want)
		}
		back, err := doltmodel.DecodeFingerprint(doltmodel.EncodeFingerprint(a))
		if err != nil || back != a {
			t.Fatalf("%s decoded as %s (%v)", a, back, err)
		}
	})
}

// TestTheTextFormOfAFingerprintIsNotTheContractsOrder is why fingerprints are ordered in
// Go by their encoding or by the contract's comparison and never by their text, which a
// database column of text would: a type that is a prefix of another sorts after it by its
// text form (a colon follows the type, and is above the dot).
func TestTheTextFormOfAFingerprintIsNotTheContractsOrder(t *testing.T) {
	var h [identity.FingerprintBytes]byte
	short, long := fpHash(t, "k8s", h), fpHash(t, "k8s.pod", h)
	if engine.CompareFingerprints(short, long) >= 0 {
		t.Fatal("the contract orders a prefix type first")
	}
	if short.String() <= long.String() {
		t.Fatalf("the text forms %q and %q order as the contract does, so sorting by text would be unobserved", short, long)
	}
	fps := []identity.Fingerprint{long, short, fpHash(t, "k8s_pod", h), fpHash(t, "k8s0", h)}
	doltmodel.SortFingerprints(fps)
	if !slices.IsSortedFunc(fps, engine.CompareFingerprints) || fps[0] != short {
		t.Fatalf("SortFingerprints: %v", fps)
	}
}

func TestDecodeFingerprintRefusesWhatIsNotOne(t *testing.T) {
	good := doltmodel.EncodeFingerprint(fp("k8s.pod", 7))
	bad := map[string][]byte{
		"empty":         nil,
		"hash only":     good[len(good)-identity.FingerprintBytes:],
		"no separator":  bytes.ReplaceAll(good, []byte{0}, []byte{'x'}),
		"short hash":    good[:len(good)-1],
		"empty type":    append([]byte{0}, good[len("k8s.pod")+1:]...),
		"invalid type":  append([]byte("K8S POD\x00"), good[len("k8s.pod")+1:]...),
		"two separator": append([]byte("k8s.pod\x00\x00"), good[len("k8s.pod")+1:]...),
	}
	for name, b := range bad {
		if f, err := doltmodel.DecodeFingerprint(b); err == nil {
			t.Errorf("%s: decoded as %s", name, f)
		}
	}
}

// checkSeqEncoding pins an encoding of sequence numbers at the edges of the range, where a
// plain int64 conversion fails (the stored value of the smallest sequence number is the
// smallest BIGINT, and of the largest the largest), and requires the stored order to be
// the order of the numbers.
func checkSeqEncoding(enc func(uint64) int64, dec func(int64) uint64) error {
	for _, c := range []struct {
		seq  uint64
		want int64
	}{
		{0, math.MinInt64},
		{1, math.MinInt64 + 1},
		{1<<63 - 1, -1},
		{1 << 63, 0},
		{1<<63 + 1, 1},
		{math.MaxUint64, math.MaxInt64},
	} {
		if got := enc(c.seq); got != c.want {
			return fmt.Errorf("EncodeSeq(%d) = %d, want %d", c.seq, got, c.want)
		}
		if back := dec(c.want); back != c.seq {
			return fmt.Errorf("DecodeSeq(%d) = %d, want %d", c.want, back, c.seq)
		}
	}
	seqs := []uint64{0, 1, 2, 1<<32 - 1, 1 << 32, 1<<63 - 2, 1<<63 - 1, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64}
	for i, a := range seqs {
		for j, b := range seqs {
			ea, eb := enc(a), enc(b)
			if (ea < eb) != (i < j) || (ea == eb) != (i == j) {
				return fmt.Errorf("seq %d and %d: stored %d and %d are out of order", a, b, ea, eb)
			}
		}
	}
	return nil
}

func TestSequenceNumbersKeepTheirOrderWithTheSignBitFlipped(t *testing.T) {
	if err := checkSeqEncoding(doltmodel.EncodeSeq, doltmodel.DecodeSeq); err != nil {
		t.Fatal(err)
	}
}

// TestSequenceEncodingMutantsAreCaught: a plain conversion keeps the order of the lower
// half and puts the upper half first; flipping a different bit, or complementing, are other
// ways to get it wrong.
func TestSequenceEncodingMutantsAreCaught(t *testing.T) {
	for name, enc := range map[string]struct {
		enc func(uint64) int64
		dec func(int64) uint64
	}{
		"a plain int64 conversion": {func(s uint64) int64 { return int64(s) }, func(v int64) uint64 { return uint64(v) }},
		"the complement":           {func(s uint64) int64 { return int64(^s) }, func(v int64) uint64 { return ^uint64(v) }},
		"the wrong bit flipped":    {func(s uint64) int64 { return int64(s ^ 1<<62) }, func(v int64) uint64 { return uint64(v) ^ 1<<62 }},
		"saturating at the middle": {func(s uint64) int64 { return int64(min(s, 1<<63-1)) }, func(v int64) uint64 { return uint64(v) }},
	} {
		if err := checkSeqEncoding(enc.enc, enc.dec); err == nil {
			t.Errorf("%s passed", name)
		}
	}
}

func TestSequenceNumberEncodingRoundTripsRandomly(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, b := rapid.Uint64().Draw(t, "a"), rapid.Uint64().Draw(t, "b")
		if doltmodel.DecodeSeq(doltmodel.EncodeSeq(a)) != a {
			t.Fatalf("%d does not round trip", a)
		}
		if (a < b) != (doltmodel.EncodeSeq(a) < doltmodel.EncodeSeq(b)) {
			t.Fatalf("%d and %d change order", a, b)
		}
	})
}

// TestInstantsKeepTheirNanosecondsAndAreNeverADatetime: the stored time is the Unix
// nanoseconds of an int64, so two instants one nanosecond apart stay two, which a DATETIME,
// with its microseconds, would merge, and the last representable instant fits.
func TestInstantsKeepTheirNanosecondsAndAreNeverADatetime(t *testing.T) {
	base := time.Date(2026, 10, 21, 3, 4, 5, 123_456_789, time.UTC)
	for _, ts := range []time.Time{base, base.Add(1), base.Add(-1), engine.MinEventTime, engine.MaxEventTime} {
		ns := doltmodel.Nanos(ts)
		if back := doltmodel.FromNanos(ns); !back.Equal(ts) || back.Location() != time.UTC {
			t.Errorf("%s comes back as %s", ts.Format(time.RFC3339Nano), back.Format(time.RFC3339Nano))
		}
	}
	if doltmodel.Nanos(base.Add(1)) == doltmodel.Nanos(base) {
		t.Error("two instants a nanosecond apart are one")
	}
	if got := doltmodel.Nanos(engine.MaxEventTime); got != math.MaxInt64 {
		t.Errorf("the last instant is %d", got)
	}
	// What a microsecond column would store merges them.
	if !base.Truncate(time.Microsecond).Equal(base.Add(1).Truncate(time.Microsecond)) {
		t.Error("the premise of this test: a microsecond truncation merges the instants")
	}
}

func TestKindIsOneCharacter(t *testing.T) {
	if doltmodel.EncodeKind(lifecycle.Observe) != "o" || doltmodel.EncodeKind(lifecycle.Delete) != "d" {
		t.Fatal("the kinds are o and d")
	}
}

// TestThePayloadCapacityIsTheLargestPayloadPlusPaddingRoundedUpTo256: the registered
// capacity is pinned, and is what the rule gives for the registered inputs, the plans'
// payloads of at most 96 bytes with no padding.
func TestThePayloadCapacityIsTheLargestPayloadPlusPaddingRoundedUpTo256(t *testing.T) {
	if doltmodel.RegisteredPayloadCapacity != 256 {
		t.Fatalf("the registered capacity is %d: changing it is a new registration", doltmodel.RegisteredPayloadCapacity)
	}
	for _, c := range []struct{ max, pad, want int }{
		{96, 0, 256}, {0, 0, 0}, {1, 0, 256}, {256, 0, 256}, {257, 0, 512}, {96, 360, 512}, {96, 160, 256}, {96, 161, 512},
	} {
		if got := doltmodel.CapacityFor(c.max, c.pad); got != c.want {
			t.Errorf("CapacityFor(%d, %d) = %d, want %d", c.max, c.pad, got, c.want)
		}
	}
	w := workload.Tiny()
	w.PayloadMax, w.PayloadPad = 96, 360
	if got := doltmodel.WorkloadPayload(w); got != 456 {
		t.Errorf("a workload's payload is %d", got)
	}
	for name, cfg := range map[string]workload.Config{"tiny": workload.Tiny(), "small": workload.Small()} {
		if got := doltmodel.CapacityFor(doltmodel.WorkloadPayload(cfg), 0); got > doltmodel.RegisteredPayloadCapacity {
			t.Errorf("the %s workload's payloads (%d) do not fit the registered capacity", name, got)
		}
	}
}

func TestClampInstant(t *testing.T) {
	for _, c := range []struct {
		name string
		at   time.Time
		want int64
		ok   bool
	}{
		{"the epoch", time.Unix(0, 0), 0, true},
		{"one nanosecond before the epoch", time.Unix(0, -1), 0, false},
		{"far before the epoch", time.Unix(-1<<40, 0), 0, false},
		{"year 1", time.Time{}, 0, false},
		{"the last instant", engine.MaxEventTime, math.MaxInt64, true},
		{"one nanosecond past it", engine.MaxEventTime.Add(1), math.MaxInt64, true},
		{"9999-12-31", time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC), math.MaxInt64, true},
		{"an ordinary instant", time.Unix(1_700_000_000, 5), 1_700_000_000_000_000_005, true},
	} {
		got, ok := doltmodel.ClampInstant(c.at)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: ClampInstant = %d, %v; want %d, %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

func TestClampWindow(t *testing.T) {
	end := engine.MaxEventTime
	for _, c := range []struct {
		name     string
		from, to time.Time
		in, out  []int64 // event times inside the window and outside it
		empty    bool
	}{
		{
			name: "an ordinary window", from: time.Unix(10, 0), to: time.Unix(20, 0),
			in: []int64{10e9, 20e9 - 1}, out: []int64{10e9 - 1, 20e9, math.MaxInt64},
		},
		{name: "from before the epoch", from: time.Unix(-1<<40, 0), to: time.Unix(0, 5), in: []int64{0, 4}, out: []int64{5}},
		{
			name: "to past the range holds the last instant", from: time.Unix(10, 0), to: end.Add(time.Hour),
			in: []int64{10e9, math.MaxInt64}, out: []int64{10e9 - 1},
		},
		{
			name: "to exactly the last instant leaves it out", from: time.Unix(10, 0), to: end,
			in: []int64{10e9, math.MaxInt64 - 1}, out: []int64{math.MaxInt64},
		},
		{name: "both past the range", from: end.Add(time.Second), to: end.Add(time.Hour), empty: true},
		{name: "from the last instant to past it", from: end, to: end.Add(time.Hour), in: []int64{math.MaxInt64}, out: []int64{math.MaxInt64 - 1}},
		{name: "empty", from: time.Unix(10, 0), to: time.Unix(10, 0), empty: true},
		{name: "backwards", from: time.Unix(20, 0), to: time.Unix(10, 0), empty: true},
		{name: "both before the epoch", from: time.Unix(-20, 0), to: time.Unix(-10, 0), empty: true},
		{name: "to exactly the epoch", from: time.Unix(-20, 0), to: time.Unix(0, 0), empty: true},
	} {
		b := doltmodel.ClampWindow(c.from, c.to)
		if b.Empty != c.empty {
			t.Errorf("%s: empty = %v", c.name, b.Empty)
		}
		for _, ns := range c.in {
			if !b.Contains(ns) {
				t.Errorf("%s: %d is not in the window", c.name, ns)
			}
		}
		for _, ns := range c.out {
			if b.Contains(ns) {
				t.Errorf("%s: %d is in the window", c.name, ns)
			}
		}
	}
}
