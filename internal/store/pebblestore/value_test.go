package pebblestore

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

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
		"a basis no clock has":           {1, append(slices.Clone(good[:1+len(ref)]), 1, 0x51, 1, 0)},
		"a reserved kind":                {1, append(slices.Clone(good[:1+len(ref)]), 1, 0x03, 1, 0)},
		"the extension bit":              {1, append(slices.Clone(good[:1+len(ref)]), 1, 0x81, 1, 0)},
	} {
		if _, _, err := decodeRecordValue(tc.dir, tc.b); err == nil {
			t.Errorf("%s was decoded", name)
		}
	}
}

// A record's boot and the basis of its event time survive the value the store
// writes and the record a read gives back.
func TestAValueKeepsTheBootAndTheBasis(t *testing.T) {
	t.Parallel()
	for basis := store.BasisUnknown; basis <= store.BasisProducerEvent; basis++ {
		r := store.Record{Seq: 9, Kind: lifecycle.Observe, Boot: "b-1", EventTimeBasis: basis}
		v := pebblekv.FromRecord(r)
		ref, back, err := decodeRecordValue(dirEntity, appendRecordValue(nil, []byte("p"), v))
		if err != nil || string(ref) != "p" || back.Boot != "b-1" || store.EventTimeBasis(back.Basis) != basis {
			t.Errorf("basis %s: decoded %q, %+v, %v", basis, ref, back, err)
		}
	}
}
