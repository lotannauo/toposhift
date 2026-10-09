package pebblestore

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The key bytes are a stored format. If one of these changes, the layout changed:
// that needs a new format tag and a migration, never a regenerated expectation.
// The expectations were computed independently of the code.
func TestKeyGoldenVectors(t *testing.T) {
	t.Parallel()
	s := keyer()
	pod, node := fingerprintOf(catalog.K8sPod, 0x00), fingerprintOf(catalog.K8sNode, 0x10)
	const (
		podKey  = "0007000102030405060708090a0b0c0d0e0f"
		nodeKey = "0006101112131415161718191a1b1c1d1e1f"
	)

	edge := store.Record{Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "kubelet", Kind: lifecycle.Observe}
	sides, err := s.sidesOf(edge)
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
	entity := store.Record{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "kubelet", Kind: lifecycle.Observe}
	sides, err = s.sidesOf(entity)
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

// The value encodings are stored formats: if one of these changes, the format
// changed, and that needs a new format byte and a migration, never a regenerated
// expectation. The expectations were computed independently of the code.
func TestRecordValueGoldenVectors(t *testing.T) {
	t.Parallel()
	const (
		nodeKey = "0006101112131415161718191a1b1c1d1e1f"
		kubelet = "6b7562656c6574"
	)
	edgeRef := mustHex(t, nodeKey+"0002"+kubelet)
	for name, tc := range map[string]struct {
		dir  byte
		ref  []byte
		v    pebblekv.Value
		want string
	}{
		"an edge's observation": {
			1, edgeRef,
			pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, Payload: []byte("ab")},
			"1b" + nodeKey + "0002" + kubelet + "010105006162",
		},
		"an entity's delete": {
			dirEntity, mustHex(t, kubelet),
			pebblekv.Value{Seq: 7, Kind: lifecycle.Delete},
			"07" + kubelet + "01020700",
		},
		"an observation with a boot": {
			1, edgeRef,
			pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, Boot: "boot-a", Payload: []byte("ab")},
			"1b" + nodeKey + "0002" + kubelet + "0109050006626f6f742d616162",
		},
		"an observation with a boot and the producer's own clock": {
			1, edgeRef,
			pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, Boot: "boot-a", Basis: uint8(store.BasisProducerEvent), Payload: []byte("ab")},
			"1b" + nodeKey + "0002" + kubelet + "0149050006626f6f742d616162",
		},
		"a delete stamped by the collector's clock": {
			dirEntity, mustHex(t, kubelet),
			pebblekv.Value{Seq: 7, Kind: lifecycle.Delete, Basis: uint8(store.BasisObserved)},
			"07" + kubelet + "01220700",
		},
	} {
		got := appendRecordValue(nil, tc.ref, tc.v)
		if hex.EncodeToString(got) != tc.want {
			t.Errorf("%s: encoded %x, want %s", name, got, tc.want)
		}
		ref, v, err := decodeRecordValue(tc.dir, got)
		if err != nil || !bytes.Equal(ref, tc.ref) || v.Seq != tc.v.Seq || v.Kind != tc.v.Kind || v.Boot != tc.v.Boot || v.Basis != tc.v.Basis || !bytes.Equal(v.Payload, tc.v.Payload) {
			t.Errorf("%s: decoded %x, %+v, %v", name, ref, v, err)
		}
	}
}

func goldenEntries(t *testing.T) []entry {
	t.Helper()
	node := mustHex(t, "0006101112131415161718191a1b1c1d1e1f"+"0002"+"6b7562656c6574")
	pod := mustHex(t, "0007000102030405060708090a0b0c0d0e0f"+"0002"+"61")
	return []entry{
		{ref: node, eventNs: 1_700_000_000_000_000_000, value: pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, TTL: time.Minute}},
		{ref: pod, eventNs: 1_699_999_999_000_000_000, value: pebblekv.Value{
			Seq: 9, Kind: lifecycle.Observe, HasThrough: true, Through: 1_700_000_000_000_000_000, Payload: []byte("dropped"),
		}},
	}
}

func TestStampGoldenVectors(t *testing.T) {
	t.Parallel()
	entries := goldenEntries(t)
	horizon := time.Unix(1_700_000_000, 0).UTC()
	bootEntry := entry{
		ref: mustHex(t, "6b7562656c6574"), eventNs: 1_699_999_990_000_000_000,
		value: pebblekv.Value{Seq: 3, Kind: lifecycle.Observe, Boot: "boot-a", Basis: uint8(store.BasisObserved), Payload: []byte("dropped")},
	}
	for name, tc := range map[string]struct {
		s    stamp
		want string
	}{
		"a baseline with two entries, given out of order": {
			stamp{kind: kindBaseline, through: 1000, w: 1000, horizon: horizon, entries: []entry{entries[1], entries[0]}},
			"01020000000000000003e800000000000003e80f010000000edce5e80000000000ffff021b0006101112131415161718191a1b1c1d1e1f00026b7562656c65748080a8b1e39fe7cb170901010580b09dc2df01150007000102030405060708090a0b0c0d0e0f00026180ecbcd4df9fe7cb170d010509008080a8b1e39fe7cb17",
		},
		"an empty checkpoint": {
			stamp{kind: kindCheckpoint, foldVersion: foldVersion, through: 77, w: 70},
			"010101000000000000004d00000000000000460000",
		},
		"a checkpoint with one entry, its payload dropped": {
			stamp{kind: kindCheckpoint, foldVersion: foldVersion, through: 77, w: 70, entries: entries[1:]},
			"010101000000000000004d000000000000004600" + "01" + "150007000102030405060708090a0b0c0d0e0f00026180ecbcd4df9fe7cb170d010509008080a8b1e39fe7cb17",
		},
		"a baseline whose entry keeps its boot and its basis": {
			stamp{kind: kindBaseline, foldVersion: foldVersion, through: 9, w: 9, horizon: horizon, entries: []entry{bootEntry}},
			"010201000000000000000900000000000000090f010000000edce5e80000000000ffff01076b7562656c657480b8f890be9fe7cb170b0129030006626f6f742d61",
		},
	} {
		got, err := appendStamp(nil, tc.s)
		if err != nil || hex.EncodeToString(got) != tc.want {
			t.Errorf("%s: encoded %x, %v; want %s", name, got, err, tc.want)
			continue
		}
		back, err := decodeStamp(got)
		if err != nil || back.kind != tc.s.kind || back.foldVersion != tc.s.foldVersion || back.through != tc.s.through || back.w != tc.s.w ||
			!back.horizon.Equal(tc.s.horizon) || len(back.entries) != len(tc.s.entries) {
			t.Errorf("%s: decoded %+v, %v", name, back, err)
		}
		for _, en := range back.entries {
			if len(en.value.Payload) != 0 {
				t.Errorf("%s: an entry kept its payload", name)
			}
		}
	}
}

// The meta keys and values are stored too. The expectations were computed
// independently of the code; the horizons are the instant exactly, in time.Time's
// binary form, then the sequence number.
func TestMetaGoldenVectors(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		got  []byte
		want string
	}{
		"the key of the format":            {metaKey(pebblekv.MetaFormat), "00666f726d6174"},
		"the key of the last number":       {metaKey(pebblekv.MetaLastSeq), "006c617374536571"},
		"the key of the first horizon":     {metaKey(pebblekv.HorizonMetaName(catalog.L0)), "00686f72697a6f6e2f31"},
		"the key of the last horizon":      {metaKey(pebblekv.HorizonMetaName(catalog.L3)), "00686f72697a6f6e2f34"},
		"the key of the boot key":          {metaKey(metaBootKey), "00626f6f744b6579"},
		"the key of the Pebble version":    {metaKey(metaPebble), "00706562626c65"},
		"the key of the checkpoints flag":  {metaKey(metaCheckpoints), "00636865636b706f696e7473"},
		"the format tag":                   {[]byte(formatTag), "746f706f73686966742f73746f72652f6c6f673b6b65793d313b7265636f72643d313b7374616d703d313b6d6574613d31"},
		"a last number":                    {pebblekv.EncodeSeq(258), "0000000000000102"},
		"the boot key of the hosts":        {[]byte(lifecycle.BootID), "746f706f2e686f73742e626f6f742e6964"},
		"the Pebble version and format":    {pebbleTag("v2.1.7", pebble.FormatValueSeparation), "706562626c653d76322e312e373b666d763d3234"},
		"the format major version is 24":   {[]byte{byte(pebble.FormatValueSeparation)}, "18"},
		"a horizon at the epoch, seq 0":    {mustEncodeHorizon(t, store.MinEventTime, 0), "010000000e7791f70000000000ffff0000000000000000"},
		"a horizon inside the range":       {mustEncodeHorizon(t, time.Unix(1_700_000_000, 0).UTC(), 77), "010000000edce5e80000000000ffff000000000000004d"},
		"a horizon after the end of range": {mustEncodeHorizon(t, store.MaxEventTime.Add(time.Hour), 3), "01000000109d53821432f2d7ffffff0000000000000003"},
	} {
		if got := hex.EncodeToString(tc.got); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

func mustEncodeHorizon(t *testing.T, h time.Time, seq uint64) []byte {
	t.Helper()
	b, err := pebblekv.EncodeLayerHorizon(h, seq)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
