package pebblelog

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
	"testing"

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

func testEngine() *Engine { return &Engine{ids: pebblekv.Default} }

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The key bytes are a stored format. If one of these changes, the layout changed:
// that needs a new format tag and a migration, never a regenerated expectation.
// The expectations were computed independently of the code.
func TestKeyGoldenVectors(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod, node := fingerprintOf(catalog.K8sPod, 0x00), fingerprintOf(catalog.K8sNode, 0x10)
	const (
		podKey  = "0007000102030405060708090a0b0c0d0e0f"
		nodeKey = "0006101112131415161718191a1b1c1d1e1f"
	)

	edge := engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "kubelet", Kind: lifecycle.Observe}
	sides, err := e.sidesOf(edge)
	if err != nil || len(sides) != 2 {
		t.Fatalf("sidesOf = %v, %v", sides, err)
	}
	// layer 3, the entity, the direction; and the reference: peer, relation, producer.
	if got, want := hex.EncodeToString(sides[0].prefix), "03"+podKey+"01"; got != want {
		t.Errorf("forward prefix = %s, want %s", got, want)
	}
	if got, want := hex.EncodeToString(sides[1].prefix), "03"+nodeKey+"02"; got != want {
		t.Errorf("reverse prefix = %s, want %s", got, want)
	}
	if got, want := hex.EncodeToString(sides[0].ref), nodeKey+"0002"+"6b7562656c6574"; got != want {
		t.Errorf("forward reference = %s, want %s", got, want)
	}
	if got, want := hex.EncodeToString(sides[1].ref), podKey+"0002"+"6b7562656c6574"; got != want {
		t.Errorf("reverse reference = %s, want %s", got, want)
	}
	entity := engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(pod), Producer: "kubelet", Kind: lifecycle.Observe}
	sides, err = e.sidesOf(entity)
	if err != nil || len(sides) != 1 {
		t.Fatalf("sidesOf(entity) = %v, %v", sides, err)
	}
	if got, want := hex.EncodeToString(sides[0].prefix), "03"+podKey+"00"; got != want {
		t.Errorf("entity prefix = %s, want %s", got, want)
	}
	if got, want := hex.EncodeToString(sides[0].ref), "6b7562656c6574"; got != want {
		t.Errorf("entity reference = %s, want %s", got, want)
	}

	fwd := mustHex(t, "03"+podKey+"01")
	for name, tc := range map[string]struct {
		key  []byte
		want string
	}{
		"a record at the epoch, seq 1":        {recordKey(fwd, 0, 1), "03" + podKey + "01" + "ffffffffffffffff" + "fffffffffffffffe" + "00"},
		"the last instant, the largest seq":   {recordKey(fwd, math.MaxInt64, 1<<63-1), "03" + podKey + "01" + "8000000000000000" + "8000000000000000" + "00"},
		"an ordinary record":                  {recordKey(fwd, 1_700_000_000_000_000_005, 5), "03" + podKey + "01" + "e8686301c9d5fffa" + "fffffffffffffffa" + "00"},
		"a baseline, with no sequence number": {stampKey(fwd, 1_700_000_000_000_000_000, kindBaseline), "03" + podKey + "01" + "e8686301c9d5ffff" + "ffffffffffffffff" + "02"},
		"a checkpoint, with none":             {stampKey(fwd, 1_700_000_000_000_000_000, kindCheckpoint), "03" + podKey + "01" + "e8686301c9d5ffff" + "ffffffffffffffff" + "01"},
		"the seek key for an instant":         {seekKey(fwd, 1_700_000_000_000_000_005), "03" + podKey + "01" + "e8686301c9d5fffa" + "0000000000000000" + "00"},
		"the largest Seq a record can have":   {recordKey(fwd, 0, math.MaxUint64), "03" + podKey + "01" + "ffffffffffffffff" + "0000000000000000" + "00"},
		"the metadata key of the last number": {metaKey(pebblekv.MetaLastSeq), "006c617374536571"},
	} {
		if got := hex.EncodeToString(tc.key); got != tc.want {
			t.Errorf("%s: key = %s, want %s", name, got, tc.want)
		}
	}
}

func TestSidesRejectWhatCannotBeStored(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := fingerprintOf(catalog.K8sPod, 0)
	stranger := fingerprintOf("nonesuch", 0)
	for name, subject := range map[string]engine.Subject{
		"a source of an unknown type":    engine.EdgeSubject(stranger, pod, catalog.ScheduledOn),
		"a target of an unknown type":    engine.EdgeSubject(pod, stranger, catalog.ScheduledOn),
		"an unknown relation":            engine.EdgeSubject(pod, pod, "nonesuch"),
		"an entity of an unknown type":   engine.EntitySubject(stranger),
		"a subject that is neither kind": {Kind: 9},
	} {
		if _, err := e.sidesOf(engine.Record{Layer: catalog.L2, Subject: subject, Producer: "p"}); err == nil {
			t.Errorf("%s was given keys", name)
		}
	}
}

// Encoded order is time order, newest first, and within an instant highest Seq
// first, with a checkpoint after the records at its instant and the baseline after
// the checkpoint: whatever the bytes, at both ends of the time range and of the
// sequence numbers. Every seek and every range delete in the engine leans on it.
func TestKeysOrderNewestFirst(t *testing.T) {
	t.Parallel()
	prefix := []byte("0123456789012345678\x01")
	nss := []int64{0, 1, 255, 256, 1 << 32, math.MaxInt64 - 1, math.MaxInt64}
	seqs := []uint64{1, 2, 255, 256, 1 << 32, 1<<63 - 1, 1 << 63, math.MaxUint64 - 1, math.MaxUint64}
	type k struct {
		ns   int64
		seq  uint64 // 0 for a checkpoint or the baseline
		kind byte
	}
	rapid.Check(t, func(t *rapid.T) {
		var ks []k
		for range rapid.IntRange(2, 12).Draw(t, "keys") {
			ns := rapid.SampledFrom(nss).Draw(t, "ns")
			switch rapid.IntRange(0, 3).Draw(t, "what") {
			case 0:
				ks = append(ks, k{ns, 0, kindCheckpoint})
			case 1:
				ks = append(ks, k{ns, 0, kindBaseline})
			default:
				ks = append(ks, k{ns, rapid.SampledFrom(seqs).Draw(t, "seq"), kindRecord})
			}
		}
		key := func(x k) []byte {
			if x.kind == kindRecord {
				return recordKey(prefix, x.ns, x.seq)
			}
			return stampKey(prefix, x.ns, x.kind)
		}
		// By meaning: later event time first; at one instant a record before a
		// stamp, a higher Seq first, a checkpoint before the baseline.
		rank := func(x k) (int, uint64) {
			if x.kind == kindRecord {
				return 0, math.MaxUint64 - x.seq
			}
			return int(x.kind), 0
		}
		byMeaning := slices.Clone(ks)
		slices.SortFunc(byMeaning, func(a, b k) int {
			switch {
			case a.ns != b.ns:
				if a.ns > b.ns {
					return -1
				}
				return 1
			}
			ra, sa := rank(a)
			rb, sb := rank(b)
			switch {
			case ra != rb:
				return ra - rb
			case sa < sb:
				return -1
			case sa > sb:
				return 1
			}
			return 0
		})
		byBytes := slices.Clone(ks)
		slices.SortFunc(byBytes, func(a, b k) int { return bytes.Compare(key(a), key(b)) })
		if !slices.Equal(byMeaning, byBytes) {
			t.Fatalf("by meaning %+v, by bytes %+v", byMeaning, byBytes)
		}
		for _, x := range ks {
			p, ns, seq, kind, err := parseKey(key(x))
			if err != nil || !bytes.Equal(p, prefix) || ns != x.ns || seq != x.seq || kind != x.kind {
				t.Fatalf("parseKey(%+v) = %x, %d, %d, %d, %v", x, p, ns, seq, kind, err)
			}
		}
	})
}

func TestParseKeyRejects(t *testing.T) {
	t.Parallel()
	prefix := []byte("0123456789012345678\x01")
	good := recordKey(prefix, 5, 7)
	flip := func(i int, b byte) []byte {
		k := slices.Clone(good)
		k[i] = b
		return k
	}
	for name, key := range map[string][]byte{
		"too short":                    good[:len(good)-1],
		"too long":                     append(slices.Clone(good), 0),
		"a negative event time":        flip(prefixLen, 0x00),
		"a record with no sequence":    recordKey(prefix, 5, 0),
		"an unknown kind":              flip(keyLen-1, 9),
		"a checkpoint with a sequence": flip(keyLen-1, kindCheckpoint),
		"a baseline with a sequence":   flip(keyLen-1, kindBaseline),
	} {
		if _, _, _, _, err := parseKey(key); err == nil {
			t.Errorf("%s was parsed", name)
		}
	}
}

// The seek key for an instant sits between the keys at that instant and later
// ones; the bounds and the delete range built from it contain exactly what they
// say.
func TestSeekAndBoundsFallWhereTheyShould(t *testing.T) {
	t.Parallel()
	e := testEngine()
	pod := fingerprintOf(catalog.K8sPod, 0)
	fwd, _ := e.prefixOf(catalog.L2, pod, byte(engine.Forward))
	rev, _ := e.prefixOf(catalog.L2, pod, byte(engine.Reverse))
	ent, _ := e.prefixOf(catalog.L2, pod, dirEntity)
	other, _ := e.prefixOf(catalog.L2, fingerprintOf(catalog.K8sPod, 1), byte(engine.Forward))

	x := int64(1_000_000)
	seek := seekKey(fwd, x)
	for name, tc := range map[string]struct {
		key     []byte
		atOrNew bool // at or after the seek key: event time at or before x
	}{
		"a record at the instant, highest seq": {recordKey(fwd, x, math.MaxUint64), true},
		"a record at the instant, seq 1":       {recordKey(fwd, x, 1), true},
		"a checkpoint at the instant":          {stampKey(fwd, x, kindCheckpoint), true},
		"the baseline at the instant":          {stampKey(fwd, x, kindBaseline), true},
		"a record a nanosecond earlier":        {recordKey(fwd, x-1, math.MaxUint64), true},
		"a record at the epoch":                {recordKey(fwd, 0, 1), true},
		"a record a nanosecond later":          {recordKey(fwd, x+1, 1), false},
		"a record at the last instant":         {recordKey(fwd, math.MaxInt64, 1), false},
	} {
		if got := bytes.Compare(tc.key, seek) >= 0; got != tc.atOrNew {
			t.Errorf("%s: at or after the seek key = %v, want %v", name, got, tc.atOrNew)
		}
	}
	lo, hi := prefixBounds(fwd)
	inside := func(k []byte) bool { return bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 }
	for _, k := range [][]byte{recordKey(fwd, 0, 1), recordKey(fwd, math.MaxInt64, math.MaxUint64), stampKey(fwd, 0, kindBaseline), stampKey(fwd, math.MaxInt64, kindCheckpoint), seekKey(fwd, 0)} {
		if !inside(k) {
			t.Errorf("key %x is outside its own prefix's bounds", k)
		}
	}
	for _, k := range [][]byte{recordKey(rev, 5, 1), recordKey(ent, 5, 1), recordKey(other, 5, 1), metaKey(pebblekv.MetaFormat)} {
		if inside(k) {
			t.Errorf("key %x is inside another prefix's bounds", k)
		}
	}
	dlo, dhi := dataBounds()
	for _, k := range [][]byte{recordKey(fwd, 0, 1), recordKey(ent, math.MaxInt64, 1)} {
		if bytes.Compare(k, dlo) < 0 || bytes.Compare(k, dhi) >= 0 {
			t.Errorf("data key %x is outside the data bounds", k)
		}
	}
	if bytes.Compare(metaKey(pebblekv.MetaFormat), dlo) >= 0 {
		t.Error("a meta key is inside the data bounds")
	}
	// The last prefix of the last layer is inside the data bounds, and nothing
	// in the successor of a prefix can be a key of the prefix.
	if bytes.Compare(prefixSucc(fwd), recordKey(fwd, math.MaxInt64, 1)) <= 0 {
		t.Error("the successor of a prefix is not after its keys")
	}
}
