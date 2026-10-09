package pebblestore

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

func TestStampRoundTripsInOrder(t *testing.T) {
	t.Parallel()
	entries := goldenEntries(t)
	b, err := appendStamp(nil, stamp{kind: kindBaseline, through: 1, w: 1, horizon: time.Unix(1_700_000_000, 0).UTC(), entries: []entry{entries[1], entries[0]}})
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeStamp(b)
	if err != nil {
		t.Fatal(err)
	}
	// Entries come back sorted by reference, whatever order they went in.
	if !slices.IsSortedFunc(back.entries, func(a, b entry) int { return slices.Compare(a.ref, b.ref) }) {
		t.Error("entries are not sorted")
	}
}

func TestStampRejects(t *testing.T) {
	t.Parallel()
	good, err := appendStamp(nil, stamp{kind: kindBaseline, through: 5, w: 5, horizon: time.Unix(1_700_000_000, 0).UTC(), entries: goldenEntries(t)})
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
