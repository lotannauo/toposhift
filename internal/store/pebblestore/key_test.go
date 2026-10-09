package pebblestore

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
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

// keyer is a store that can build keys and nothing else.
func keyer() *Store { return &Store{ids: pebblekv.Default} }

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSidesRejectWhatCannotBeStored(t *testing.T) {
	t.Parallel()
	s := keyer()
	pod := fingerprintOf(catalog.K8sPod, 0)
	stranger := fingerprintOf("nonesuch", 0)
	for name, subject := range map[string]store.Subject{
		"a source of an unknown type":    store.EdgeSubject(stranger, pod, catalog.ScheduledOn),
		"a target of an unknown type":    store.EdgeSubject(pod, stranger, catalog.ScheduledOn),
		"an unknown relation":            store.EdgeSubject(pod, pod, "nonesuch"),
		"an entity of an unknown type":   store.EntitySubject(stranger),
		"a subject that is neither kind": {Kind: 9},
	} {
		if _, err := s.sidesOf(store.Record{Layer: catalog.L2, Subject: subject, Producer: "p"}); err == nil {
			t.Errorf("%s was given keys", name)
		}
	}
}

// Encoded order is time order, newest first, and within an instant highest Seq
// first, with a checkpoint after the records at its instant and the baseline after
// the checkpoint: whatever the bytes, at both ends of the time range and of the
// sequence numbers. Every seek and every range delete in the store leans on it.
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
	s := keyer()
	pod := fingerprintOf(catalog.K8sPod, 0)
	fwd, _ := s.prefixOf(catalog.L2, pod, byte(store.Forward))
	rev, _ := s.prefixOf(catalog.L2, pod, byte(store.Reverse))
	ent, _ := s.prefixOf(catalog.L2, pod, dirEntity)
	other, _ := s.prefixOf(catalog.L2, fingerprintOf(catalog.K8sPod, 1), byte(store.Forward))

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

// The data keyspace is the keys whose first byte is a layer: the bounds are the
// ones the benchmarks' digests and the dbhash command use, written there as
// numbers because that code cannot import this package.
func TestTheDataKeyspaceIsTheFrozenOne(t *testing.T) {
	t.Parallel()
	lo, hi := dataBounds()
	if !bytes.Equal(lo, []byte{0x01}) || !bytes.Equal(hi, []byte{0x05}) {
		t.Errorf("dataBounds() = [%x, %x), want [01, 05)", lo, hi)
	}
	if keyLen != 37 || prefixLen != 20 {
		t.Errorf("keyLen = %d, prefixLen = %d: the digests and dbhash assume 37 and 20", keyLen, prefixLen)
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if b := pebblekv.LayerByte(l); b != byte(l) {
			t.Errorf("layer %s is key byte %d, want %d", l, b, byte(l))
		}
	}
}
