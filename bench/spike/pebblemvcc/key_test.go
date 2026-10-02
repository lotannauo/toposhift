package pebblemvcc

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// fingerprintOf is a fingerprint whose hash is first, first+1, ...: a value to
// reason about by hand.
func fingerprintOf(typ catalog.EntityType, first byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	for i := range h {
		h[i] = first + byte(i)
	}
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func testFingerprint(_ testing.TB, typ catalog.EntityType, first byte) identity.Fingerprint {
	return fingerprintOf(typ, first)
}

func testEngine() *Engine { return &Engine{ids: pebblekv.Default} }

// The key bytes are a stored format. If one of these changes, the layout changed:
// that needs a new format tag and a migration, never a regenerated expectation.
func TestKeyGoldenVectors(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := testFingerprint(t, catalog.K8sPod, 0x00)
	node := testFingerprint(t, catalog.K8sNode, 0x10)

	edge := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "kubelet",
		Kind: lifecycle.Observe,
	}
	keys, err := e.roachKeys(edge)
	if err != nil {
		t.Fatal(err)
	}
	const (
		podID, nodeID = "0007", "0006"
		podHash       = "000102030405060708090a0b0c0d0e0f"
		nodeHash      = "101112131415161718191a1b1c1d1e1f"
		producer      = "6b7562656c6574" // kubelet
	)
	// layer 3 | pod | forward | node | scheduled_on | producer, and its reverse.
	wantForward := "03" + podID + podHash + "01" + nodeID + nodeHash + "0002" + producer
	wantReverse := "03" + nodeID + nodeHash + "02" + podID + podHash + "0002" + producer
	if got := hex.EncodeToString(keys[0]); got != wantForward {
		t.Errorf("forward roach key = %s, want %s", got, wantForward)
	}
	if got := hex.EncodeToString(keys[1]); got != wantReverse {
		t.Errorf("reverse roach key = %s, want %s", got, wantReverse)
	}

	entity := engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(pod), Producer: "kubelet", Kind: lifecycle.Observe}
	keys, err = e.roachKeys(entity)
	if err != nil || len(keys) != 1 {
		t.Fatalf("roachKeys(entity) = %x, %v", keys, err)
	}
	if got, want := hex.EncodeToString(keys[0]), "03"+podID+podHash+"00"+producer; got != want {
		t.Errorf("entity roach key = %s, want %s", got, want)
	}

	// The engine key adds the sentinel, the version and its length: wall is the
	// event time plus one, so the epoch is wall 1, and a logical part makes the
	// version twelve bytes.
	for _, tc := range []struct {
		name         string
		wall         uint64
		logical      uint32
		wantVersion  string
		wantTerminal string
	}{
		{"the epoch, ordinal 1", wallOf(0), 1, "0000000000000001" + "00000001", "0d"},
		{"the last instant, ordinal 1", wallOf(math.MaxInt64), 1, "8000000000000000" + "00000001", "0d"},
		{"an ordinary instant, ordinal 3", wallOf(1_700_000_000_000_000_005), 3, "17979cfe362a0006" + "00000003", "0d"},
		{"the seek form: the largest ordinal", wallOf(1), math.MaxUint32, "0000000000000002" + "ffffffff", "0d"},
		{"an ordinal of zero is the short form", wallOf(1), 0, "0000000000000002", "09"},
	} {
		got := hex.EncodeToString(versionKey(keys[0], tc.wall, tc.logical))
		want := hex.EncodeToString(keys[0]) + "00" + tc.wantVersion + tc.wantTerminal
		if got != want {
			t.Errorf("%s: engine key = %s, want %s", tc.name, got, want)
		}
	}
	if got, want := hex.EncodeToString(metaKey(pebblekv.MetaLastSeq)), "006c6173745365710"+"0"; got != want {
		t.Errorf("meta key = %s, want %s", got, want)
	}
}

func TestRoachKeysRejectWhatCannotBeStored(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := testFingerprint(t, catalog.K8sPod, 0)
	stranger := testFingerprint(t, "nonesuch", 0)
	for name, subject := range map[string]engine.Subject{
		"a source of an unknown type":    engine.EdgeSubject(stranger, pod, catalog.ScheduledOn),
		"a target of an unknown type":    engine.EdgeSubject(pod, stranger, catalog.ScheduledOn),
		"an unknown relation":            engine.EdgeSubject(pod, pod, "nonesuch"),
		"an entity of an unknown type":   engine.EntitySubject(stranger),
		"a subject that is neither kind": {Kind: 9},
	} {
		if _, err := e.roachKeys(engine.Record{Layer: catalog.L2, Subject: subject, Producer: "p"}); err == nil {
			t.Errorf("%s was given keys", name)
		}
	}
}

// Encoded order is tuple order: roach key ascending, then wall descending, then
// logical descending, whatever the bytes in the roach key and at both ends of the
// time range. This is what every seek in the engine leans on.
func TestEngineKeysOrderLikeTheirTuples(t *testing.T) {
	t.Parallel()
	walls := []uint64{1, 2, 255, 256, 1 << 32, 1<<63 - 1, 1 << 63}
	logicals := []uint32{1, 2, 3, 255, 256, 1 << 24, math.MaxUint32 - 1, math.MaxUint32}
	type k struct {
		roach   []byte
		wall    uint64
		logical uint32
	}
	rapid.Check(t, func(t *rapid.T) {
		roaches := rapid.SliceOfN(rapid.SliceOfN(rapid.SampledFrom([]byte{0, 1, 2, 0xFE, 0xFF}), 1, 5), 1, 4).Draw(t, "roach keys")
		var ks []k
		for _, r := range roaches {
			for i := range rapid.IntRange(1, 6).Draw(t, "versions") {
				_ = i
				ks = append(ks, k{r, rapid.SampledFrom(walls).Draw(t, "wall"), rapid.SampledFrom(logicals).Draw(t, "logical")})
			}
		}
		byTuple := slices.Clone(ks)
		slices.SortFunc(byTuple, func(a, b k) int {
			if c := bytes.Compare(a.roach, b.roach); c != 0 {
				return c
			}
			switch {
			case a.wall != b.wall:
				if a.wall > b.wall {
					return -1
				}
				return 1
			case a.logical != b.logical:
				if a.logical > b.logical {
					return -1
				}
				return 1
			}
			return 0
		})
		byEngine := slices.Clone(ks)
		slices.SortFunc(byEngine, func(a, b k) int {
			return cockroachkvs.Compare(versionKey(a.roach, a.wall, a.logical), versionKey(b.roach, b.wall, b.logical))
		})
		for i := range byTuple {
			if !bytes.Equal(byTuple[i].roach, byEngine[i].roach) || byTuple[i].wall != byEngine[i].wall || byTuple[i].logical != byEngine[i].logical {
				t.Fatalf("position %d: by tuple %+v, by engine key %+v", i, byTuple[i], byEngine[i])
			}
		}
		for _, x := range ks {
			roach, wall, logical, err := split(versionKey(x.roach, x.wall, x.logical))
			if err != nil || !bytes.Equal(roach, x.roach) || wall != x.wall || logical != x.logical {
				t.Fatalf("split(versionKey(%+v)) = %x, %d, %d, %v", x, roach, wall, logical, err)
			}
		}
	})
}

// A seek for the newest version at or before an instant must use the largest
// logical part. This pins both halves: the right seek key lands on the newest
// ordinal at the instant, and a seek key with logical 0 would skip every version
// there (the short form sorts after all of them).
func TestAtOrBeforeLandsOnTheNewestVersionAtTheInstant(t *testing.T) {
	t.Parallel()
	roach := []byte{3, 0, 7, 9}
	versions := []struct {
		wall    uint64
		logical uint32
	}{{10, 1}, {10, 2}, {10, 3}, {8, 1}, {5, 1}, {5, 2}}
	var keys [][]byte
	for _, v := range versions {
		keys = append(keys, versionKey(roach, v.wall, v.logical))
	}
	slices.SortFunc(keys, cockroachkvs.Compare)
	firstAtOrAfter := func(target []byte) int {
		i, _ := slices.BinarySearchFunc(keys, target, cockroachkvs.Compare)
		return i
	}
	for _, tc := range []struct {
		at         uint64
		wall       uint64
		logical    uint32
		wantsNoKey bool
	}{
		{10, 10, 3, false}, {11, 10, 3, false}, {9, 8, 1, false}, {8, 8, 1, false}, {7, 5, 2, false}, {5, 5, 2, false}, {4, 0, 0, true},
	} {
		i := firstAtOrAfter(atOrBefore(roach, tc.at))
		if tc.wantsNoKey {
			if i != len(keys) {
				t.Errorf("at %d: landed on %x, want nothing", tc.at, keys[i])
			}
			continue
		}
		if i == len(keys) {
			t.Errorf("at %d: landed past every version, want (%d,%d)", tc.at, tc.wall, tc.logical)
			continue
		}
		_, wall, logical, err := split(keys[i])
		if err != nil || wall != tc.wall || logical != tc.logical {
			t.Errorf("at %d: landed on (%d,%d), want (%d,%d)", tc.at, wall, logical, tc.wall, tc.logical)
		}
	}
	// The wrong form, for contrast: logical 0 lands past the whole instant.
	i := firstAtOrAfter(versionKey(roach, 10, 0))
	if i == len(keys) {
		t.Fatal("a seek with logical 0 at wall 10 lands past every version; the hazard this test guards against has changed")
	}
	if _, wall, _, _ := split(keys[i]); wall != 8 {
		t.Errorf("a seek with logical 0 at wall 10 lands on wall %d; the hazard this test guards against has changed", wall)
	}
}

// Iterator bounds and range-delete endpoints must be valid engine keys, and must
// fall where the comment on each says.
func TestBoundsAreValidEngineKeysInTheRightPlace(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := testFingerprint(t, catalog.K8sPod, 0)
	node := testFingerprint(t, catalog.K8sNode, 0)
	pre, ok := e.prefixOf(catalog.L2, pod, byte(engine.Forward))
	if !ok {
		t.Fatal("no prefix")
	}
	lo, hi := prefixBounds(pre)
	for _, b := range [][]byte{lo, hi} {
		if _, _, ok := cockroachkvs.DecodeEngineKey(b); !ok {
			t.Errorf("bound %x is not an engine key", b)
		}
	}
	rec := engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p"}
	keys, _ := e.roachKeys(rec)
	in := versionKey(keys[0], wallOf(0), 1)
	last := versionKey(keys[0], wallOf(math.MaxInt64), math.MaxUint32)
	// Every version of a key under the prefix is inside its bounds.
	for _, key := range [][]byte{in, last} {
		if cockroachkvs.Compare(key, lo) <= 0 || cockroachkvs.Compare(key, hi) >= 0 {
			t.Errorf("key %x is not strictly inside the bounds of its own prefix", key)
		}
	}
	// A key of the same entity in the other direction, or another entity, is not.
	other, _ := e.prefixOf(catalog.L2, pod, byte(engine.Reverse))
	otherKey := versionKey(append(other, 1, 2, 3), wallOf(5), 1)
	if cockroachkvs.Compare(otherKey, lo) > 0 && cockroachkvs.Compare(otherKey, hi) < 0 {
		t.Errorf("a reverse key %x is inside the forward prefix's bounds", otherKey)
	}
	// endOfVersions is past every version of the key and not past the next key.
	end := endOfVersions(keys[0])
	if _, _, ok := cockroachkvs.DecodeEngineKey(end); !ok {
		t.Errorf("endOfVersions %x is not an engine key", end)
	}
	if cockroachkvs.Compare(last, end) >= 0 || cockroachkvs.Compare(in, end) >= 0 {
		t.Error("a version of the key is not before endOfVersions")
	}
	next := versionKey(append(slices.Clone(keys[0]), 0), wallOf(math.MaxInt64), math.MaxUint32)
	if cockroachkvs.Compare(end, next) > 0 {
		t.Error("endOfVersions is after the first possible longer key")
	}
	dlo, dhi := dataBounds()
	for _, key := range [][]byte{in, last} {
		if cockroachkvs.Compare(key, dlo) <= 0 || cockroachkvs.Compare(key, dhi) >= 0 {
			t.Errorf("data key %x is outside the data bounds", key)
		}
	}
	if cockroachkvs.Compare(metaKey(pebblekv.MetaFormat), dlo) >= 0 {
		t.Error("a meta key is inside the data bounds")
	}
}

// The comparer's own consistency check, on the prefixes and suffixes this layout
// actually produces.
func TestComparerAcceptsOurKeys(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := testFingerprint(t, catalog.K8sPod, 0)
	node := testFingerprint(t, catalog.K8sNode, 3)
	var prefixes, suffixes [][]byte
	for _, r := range []engine.Record{
		{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "kubelet"},
		{Layer: catalog.L2, Subject: engine.EntitySubject(pod), Producer: "p"},
	} {
		keys, err := e.roachKeys(r)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			prefixes = append(prefixes, append(slices.Clone(k), 0))
		}
	}
	for _, w := range []uint64{wallOf(0), wallOf(7), wallOf(math.MaxInt64)} {
		for _, l := range []uint32{1, 2, math.MaxUint32} {
			key := versionKey([]byte("x"), w, l)
			suffixes = append(suffixes, key[cockroachkvs.Split(key):])
		}
	}
	if err := pebble.CheckComparer(&cockroachkvs.Comparer, prefixes, suffixes); err != nil {
		t.Fatal(err)
	}
}
