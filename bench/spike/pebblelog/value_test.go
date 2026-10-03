package pebblelog

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

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
	} {
		got := appendRecordValue(nil, tc.ref, tc.v)
		if hex.EncodeToString(got) != tc.want {
			t.Errorf("%s: encoded %x, want %s", name, got, tc.want)
		}
		ref, v, err := decodeRecordValue(tc.dir, got)
		if err != nil || !bytes.Equal(ref, tc.ref) || v.Seq != tc.v.Seq || v.Kind != tc.v.Kind || !bytes.Equal(v.Payload, tc.v.Payload) {
			t.Errorf("%s: decoded %x, %+v, %v", name, ref, v, err)
		}
	}
}

func TestRecordValueRejects(t *testing.T) {
	t.Parallel()
	ref := mustHex(t, "0006101112131415161718191a1b1c1d1e1f"+"0002"+"6b")
	good := appendRecordValue(nil, ref, pebblekv.Value{Seq: 1, Kind: lifecycle.Observe})
	for name, tc := range map[string]struct {
		dir byte
		b   []byte
	}{
		"empty":                          {1, nil},
		"a reference longer than it is":  {1, []byte{200, 1, 2}},
		"an edge reference with no peer": {1, appendRecordValue(nil, []byte("short"), pebblekv.Value{Seq: 1, Kind: lifecycle.Observe})},
		"a reference with no value":      {1, good[:1+len(ref)]},
		"a damaged value":                {1, append(slices.Clone(good[:1+len(ref)]), 9, 9)},
	} {
		if _, _, err := decodeRecordValue(tc.dir, tc.b); err == nil {
			t.Errorf("%s was decoded", name)
		}
	}
}

func goldenEntries(t *testing.T) []Entry {
	t.Helper()
	node := mustHex(t, "0006101112131415161718191a1b1c1d1e1f"+"0002"+"6b7562656c6574")
	pod := mustHex(t, "0007000102030405060708090a0b0c0d0e0f"+"0002"+"61")
	return []Entry{
		{Ref: node, EventNs: 1_700_000_000_000_000_000, Value: pebblekv.Value{Seq: 5, Kind: lifecycle.Observe, TTL: time.Minute}},
		{Ref: pod, EventNs: 1_699_999_999_000_000_000, Value: pebblekv.Value{
			Seq: 9, Kind: lifecycle.Observe, HasThrough: true, Through: 1_700_000_000_000_000_000, Payload: []byte("dropped"),
		}},
	}
}

func TestStampGoldenVectors(t *testing.T) {
	t.Parallel()
	entries := goldenEntries(t)
	horizon := time.Unix(1_700_000_000, 0).UTC()
	for name, tc := range map[string]struct {
		s    Stamp
		want string
	}{
		"a baseline with two entries, given out of order": {
			Stamp{Kind: kindBaseline, Through: 1000, W: 1000, Horizon: horizon, Entries: []Entry{entries[1], entries[0]}},
			"01020000000000000003e800000000000003e80f010000000edce5e80000000000ffff021b0006101112131415161718191a1b1c1d1e1f00026b7562656c65748080a8b1e39fe7cb170901010580b09dc2df01150007000102030405060708090a0b0c0d0e0f00026180ecbcd4df9fe7cb170d010509008080a8b1e39fe7cb17",
		},
		"an empty checkpoint": {
			Stamp{Kind: kindCheckpoint, FoldVersion: FoldVersion, Through: 77, W: 70},
			"010101000000000000004d00000000000000460000",
		},
		"a checkpoint with one entry, its payload dropped": {
			Stamp{Kind: kindCheckpoint, FoldVersion: FoldVersion, Through: 77, W: 70, Entries: entries[1:]},
			"010101000000000000004d000000000000004600" + "01" + "150007000102030405060708090a0b0c0d0e0f00026180ecbcd4df9fe7cb170d010509008080a8b1e39fe7cb17",
		},
	} {
		got, err := appendStamp(nil, tc.s)
		if err != nil || hex.EncodeToString(got) != tc.want {
			t.Errorf("%s: encoded %x, %v; want %s", name, got, err, tc.want)
			continue
		}
		back, err := decodeStamp(got)
		if err != nil || back.Kind != tc.s.Kind || back.FoldVersion != tc.s.FoldVersion || back.Through != tc.s.Through || back.W != tc.s.W ||
			!back.Horizon.Equal(tc.s.Horizon) || len(back.Entries) != len(tc.s.Entries) {
			t.Errorf("%s: decoded %+v, %v", name, back, err)
		}
		for _, en := range back.Entries {
			if len(en.Value.Payload) != 0 {
				t.Errorf("%s: an entry kept its payload", name)
			}
		}
		// Entries come back sorted by reference, whatever order they went in.
		if !slices.IsSortedFunc(back.Entries, func(a, b Entry) int { return slices.Compare(a.Ref, b.Ref) }) {
			t.Errorf("%s: entries are not sorted", name)
		}
	}
}

func TestStampRejects(t *testing.T) {
	t.Parallel()
	good, err := appendStamp(nil, Stamp{Kind: kindBaseline, Through: 5, W: 5, Horizon: time.Unix(1_700_000_000, 0).UTC(), Entries: goldenEntries(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeStamp(good); err != nil {
		t.Fatal(err)
	}
	for n := range good {
		if _, err := decodeStamp(good[:n]); !errors.Is(err, pebblekv.ErrValue) {
			t.Errorf("a stamp cut to %d bytes decoded, or failed with %v", n, err)
		}
	}
	for name, b := range map[string][]byte{
		"another format":     append([]byte{2}, good[1:]...),
		"trailing bytes":     append(slices.Clone(good), 0),
		"a damaged horizon":  mustHex(t, "0102000000000000000500000000000000050200ff0000"),
		"a huge entry count": mustHex(t, "010200000000000000000500000000000000050000"+"ffffffffff0f"),
	} {
		if _, err := decodeStamp(b); err == nil {
			t.Errorf("%s was decoded", name)
		}
	}
}
