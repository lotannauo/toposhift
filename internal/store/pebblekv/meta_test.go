package pebblekv

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// metaKey is a meta key under the bytewise layout: the zero byte is left to keys
// that are not data.
func metaKey(name string) []byte { return append([]byte{0}, name...) }

func TestMetaRoundTrips(t *testing.T) {
	t.Parallel()
	kv := bytewise(t, vfs.NewMem(), "db", Config{})
	defer func() { _ = kv.Close() }()

	if v, err := kv.GetMeta(metaKey(MetaLastSeq)); err != nil || v != nil {
		t.Fatalf("GetMeta on a new database = %x, %v", v, err)
	}
	if seq, err := DecodeSeq(nil); err != nil || seq != 0 {
		t.Errorf("DecodeSeq(nil) = %d, %v; want 0", seq, err)
	}
	if h, err := DecodeHorizon(nil); err != nil || !h.IsZero() {
		t.Errorf("DecodeHorizon(nil) = %v, %v; want the zero time", h, err)
	}
	for _, seq := range []uint64{0, 1, 1 << 32, 1<<63 + 5} {
		back, err := DecodeSeq(EncodeSeq(seq))
		if err != nil || back != seq {
			t.Errorf("seq %d round-trips to %d, %v", seq, back, err)
		}
	}
	for _, h := range []time.Time{
		time.Unix(1_700_000_000, 7).UTC(), {}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(0, 0).UTC(),
	} {
		raw, err := EncodeHorizon(h)
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodeHorizon(raw)
		if err != nil || !back.Equal(h) {
			t.Errorf("horizon %v round-trips to %v, %v", h, back, err)
		}
	}
	if _, err := DecodeSeq([]byte{1, 2}); !errors.Is(err, ErrValue) {
		t.Errorf("a short sequence number decoded: %v", err)
	}
	if _, err := DecodeHorizon([]byte{1, 2}); !errors.Is(err, ErrValue) {
		t.Errorf("a short horizon decoded: %v", err)
	}

	// The format check writes on a new database, accepts the same format and
	// refuses another.
	key := metaKey(MetaFormat)
	if err := kv.CheckFormat(key, []byte("layout-x/1")); err != nil {
		t.Fatal(err)
	}
	if err := kv.CheckFormat(key, []byte("layout-x/1")); err != nil {
		t.Errorf("the same format was refused: %v", err)
	}
	if err := kv.CheckFormat(key, []byte("layout-y/1")); err == nil {
		t.Error("another layout's format was accepted")
	}
	if err := kv.CheckFormat(key, []byte("layout-x/2")); err == nil {
		t.Error("another version of the format was accepted")
	}
	if v, err := kv.GetMeta(key); err != nil || string(v) != "layout-x/1" {
		t.Errorf("GetMeta = %q, %v", v, err)
	}
}

func TestHorizonMetaName(t *testing.T) {
	t.Parallel()
	for l, want := range map[catalog.Layer]string{catalog.L0: "horizon/1", catalog.L1: "horizon/2", catalog.L2: "horizon/3", catalog.L3: "horizon/4"} {
		if got := HorizonMetaName(l); got != want {
			t.Errorf("HorizonMetaName(%s) = %q, want %q", l, got, want)
		}
	}
}

// The layer horizon is a stored form: a time in time.Time's binary form and then
// the sequence number as 8 bytes, big endian. These bytes are a contract with data
// on disk.
func TestLayerHorizonGoldenVectors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		at   time.Time
		seq  uint64
		hex  string
	}{
		{"the zero time, no sequence", time.Time{}, 0, "01000000000000000000000000ffff0000000000000000"},
		{"the epoch in UTC, sequence 7", time.Unix(0, 0).UTC(), 7, "010000000e7791f70000000000ffff0000000000000007"},
	} {
		raw, err := EncodeLayerHorizon(tc.at, tc.seq)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := hex.EncodeToString(raw); got != tc.hex {
			t.Errorf("%s: encoded %s, want %s", tc.name, got, tc.hex)
		}
	}
}

func TestLayerHorizonRoundTrips(t *testing.T) {
	t.Parallel()
	local := time.FixedZone("x", 5*3600+30*60)
	for _, tc := range []struct {
		name string
		at   time.Time
		seq  uint64
	}{
		{"the zero time", time.Time{}, 0},
		{"an ordinary instant", time.Unix(1_700_000_000, 7).UTC(), 41},
		{"in another zone", time.Unix(1_700_000_000, 7).In(local), 1 << 40},
		{"the largest sequence number", time.Unix(0, 1<<62).UTC(), 1<<64 - 1},
		{"year 9999", time.Date(9999, 12, 31, 23, 59, 59, 999_999_999, time.UTC), 3},
	} {
		raw, err := EncodeLayerHorizon(tc.at, tc.seq)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		at, seq, err := DecodeLayerHorizon(raw)
		if err != nil || !at.Equal(tc.at) || seq != tc.seq {
			t.Errorf("%s: round-trips to %v, %d, %v; want %v, %d", tc.name, at, seq, err, tc.at, tc.seq)
		}
		// The sequence number is the last 8 bytes whatever the length of the time.
		if !bytes.Equal(raw[len(raw)-8:], EncodeSeq(tc.seq)) {
			t.Errorf("%s: the last 8 bytes %x are not the sequence number", tc.name, raw[len(raw)-8:])
		}
	}
	if at, seq, err := DecodeLayerHorizon(nil); err != nil || !at.IsZero() || seq != 0 {
		t.Errorf("DecodeLayerHorizon(nil) = %v, %d, %v; want the zero time and 0", at, seq, err)
	}
	good, _ := EncodeLayerHorizon(time.Unix(5, 0).UTC(), 9)
	for name, b := range map[string][]byte{
		"empty":                    {},
		"shorter than a sequence":  {1, 2, 3},
		"a sequence and no time":   EncodeSeq(9),
		"a truncated time":         good[1:],
		"the spike's form, no seq": good[:len(good)-8],
		"a time of the wrong form": append(bytes.Repeat([]byte{0xFF}, 15), EncodeSeq(1)...),
	} {
		if _, _, err := DecodeLayerHorizon(b); !errors.Is(err, ErrValue) {
			t.Errorf("%s: DecodeLayerHorizon(%x) err = %v, want ErrValue", name, b, err)
		}
	}
}
