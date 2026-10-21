package pgmodel_test

import (
	"bytes"
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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

// TestEncodedFingerprintsSortAsTheContractDoes pins the order a database's ORDER BY
// gives the bytea column: a type that is a prefix of another sorts first (the zero
// byte separator is below every byte of a name), the type decides before the hash,
// and hashes differing in the first or the last byte decide.
func TestEncodedFingerprintsSortAsTheContractDoes(t *testing.T) {
	var lo, hi, firstDiffers, lastDiffers [identity.FingerprintBytes]byte
	for i := range hi {
		hi[i] = 0xff
	}
	firstDiffers[0] = 1
	lastDiffers[identity.FingerprintBytes-1] = 1
	fps := []identity.Fingerprint{
		fpHash(t, "k8s.pod", lo), fpHash(t, "k8s.pod", lastDiffers), fpHash(t, "k8s.pod", firstDiffers), fpHash(t, "k8s.pod", hi),
		fpHash(t, "k8s.pod", lo), // equal to the first
		fpHash(t, "k8s", hi), fpHash(t, "k8s", lo),
		fpHash(t, "k8s.pod.x", lo), fpHash(t, "k8s0pod", lo), fpHash(t, "k8s_pod", lo), fpHash(t, "k8s.podx", lo),
		fpHash(t, "service", hi), fpHash(t, "service.instance", lo),
	}
	for _, a := range fps {
		for _, b := range fps {
			got := bytes.Compare(pgmodel.EncodeFingerprint(a), pgmodel.EncodeFingerprint(b))
			if want := engine.CompareFingerprints(a, b); got != want {
				t.Errorf("encodings of %s and %s compare %d, engine.CompareFingerprints %d", a, b, got, want)
			}
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
		if got, want := pgmodel.CompareEncoded(pgmodel.EncodeFingerprint(a), pgmodel.EncodeFingerprint(b)), engine.CompareFingerprints(a, b); got != want {
			t.Fatalf("%s and %s: encodings compare %d, the contract %d", a, b, got, want)
		}
		back, err := pgmodel.DecodeFingerprint(pgmodel.EncodeFingerprint(a))
		if err != nil || back != a {
			t.Fatalf("%s decoded as %s (%v)", a, back, err)
		}
	})
}

func TestDecodeFingerprintRefusesWhatIsNotOne(t *testing.T) {
	good := pgmodel.EncodeFingerprint(fp("k8s.pod", 7))
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
		if f, err := pgmodel.DecodeFingerprint(b); err == nil {
			t.Errorf("%s: decoded as %s", name, f)
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
		got, ok := pgmodel.ClampInstant(c.at)
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
		b := pgmodel.ClampWindow(c.from, c.to)
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
