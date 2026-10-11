package store_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// A cluster name is bounded by the producer name it becomes part of: the format by
// the longest producer name a reader accepts, the limit by the longest a store
// writes. The prefix is the same length for both producers that carry one.
func TestClusterNameBoundsFollowTheProducerBounds(t *testing.T) {
	t.Parallel()
	if len("k8scluster/") != len("k8sobjects/") {
		t.Fatalf("the producer prefixes differ in length: %d and %d", len("k8scluster/"), len("k8sobjects/"))
	}
	if got, want := store.MaxClusterNameFormat, store.MaxProducerFormat-len("k8sobjects/"); got != want {
		t.Errorf("MaxClusterNameFormat = %d, want %d", got, want)
	}
	if got, want := store.MaxClusterNameLen, store.MaxProducerLen-len("k8sobjects/"); got != want {
		t.Errorf("MaxClusterNameLen = %d, want %d", got, want)
	}
	if store.MaxClusterNameLen < 1 || store.MaxClusterNameLen >= store.MaxClusterNameFormat {
		t.Errorf("the limit %d must be at least 1 and below the format's %d", store.MaxClusterNameLen, store.MaxClusterNameFormat)
	}
	// A producer built from the longest name a store writes is a producer a store writes.
	for _, prefix := range []string{"k8scluster/", "k8sobjects/"} {
		p := prefix + strings.Repeat("a", store.MaxClusterNameLen)
		if err := store.WritableProducer(lifecycle.Producer(p)); err != nil {
			t.Errorf("%s with the longest cluster name: %v", prefix, err)
		}
		p = prefix + strings.Repeat("a", store.MaxClusterNameFormat)
		if err := store.ValidProducer(lifecycle.Producer(p)); err != nil {
			t.Errorf("%s with the longest cluster name of the format: %v", prefix, err)
		}
	}
}

func TestValidClusterName(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   string
		ok   bool
	}{
		{"one byte", "a", true},
		{"every kind of byte", "aZ09._:/-", true},
		{"the limit", strings.Repeat("a", store.MaxClusterNameLen), true},
		{"over the limit, within the format", strings.Repeat("a", store.MaxClusterNameLen+1), true},
		{"the format's longest", strings.Repeat("a", store.MaxClusterNameFormat), true},
		{"empty", "", false},
		{"over the format", strings.Repeat("a", store.MaxClusterNameFormat+1), false},
		{"a space", "a b", false},
		{"a leading space", " ab", false},
		{"a newline", "a\n", false},
		{"a NUL", "a\x00b", false},
		{"non-ASCII", "café", false},
		{"an at sign", "a@b", false},
		{"a backslash", `a\b`, false},
		{"a comma", "a,b", false},
		{"a slash is allowed", "a/b", true},
	} {
		err := store.ValidClusterName(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("%s: ValidClusterName(%q) = %v, want ok=%v", tc.name, tc.in, err, tc.ok)
		}
		if err != nil && !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v does not wrap ErrInvalid", tc.name, err)
		}
	}
}

func TestCheckClusterNameLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   string
		ok   bool
	}{
		{"one byte", "a", true},
		{"the limit", strings.Repeat("a", store.MaxClusterNameLen), true},
		{"one over the limit", strings.Repeat("a", store.MaxClusterNameLen+1), false},
		{"the format's longest", strings.Repeat("a", store.MaxClusterNameFormat), false},
		{"over the format", strings.Repeat("a", store.MaxClusterNameFormat+1), false},
		{"empty", "", false},
		{"a bad byte within the limit", "a b", false},
	} {
		err := store.CheckClusterNameLimit(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("%s: CheckClusterNameLimit(%q) = %v, want ok=%v", tc.name, tc.in, err, tc.ok)
		}
		if err != nil && !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v does not wrap ErrInvalid", tc.name, err)
		}
	}
}

// Every length the format allows round trips, the limit's neighbours and the
// format's longest included, and none set is its own value.
func TestClusterNameCodecRoundTrips(t *testing.T) {
	t.Parallel()
	for n := 1; n <= store.MaxClusterNameFormat; n++ {
		name := strings.Repeat("x", n)
		b := store.AppendClusterName(nil, name, true)
		if want := append([]byte{1, byte(n)}, name...); !bytes.Equal(b, want) {
			t.Fatalf("length %d encodes as %x, want %x", n, b, want)
		}
		got, set, err := store.DecodeClusterName(b)
		if err != nil || !set || got != name {
			t.Fatalf("length %d decodes as %q, %v, %v", n, got, set, err)
		}
	}
	none := store.AppendClusterName(nil, "", false)
	if !bytes.Equal(none, []byte{1, 0}) {
		t.Errorf("none set encodes as %x, want 0100", none)
	}
	// With set false the name is ignored.
	if got := store.AppendClusterName(nil, "ignored", false); !bytes.Equal(got, none) {
		t.Errorf("none set with a name encodes as %x, want %x", got, none)
	}
	if name, set, err := store.DecodeClusterName(none); err != nil || set || name != "" {
		t.Errorf("none set decodes as %q, %v, %v", name, set, err)
	}
	// It appends to what is there.
	if got := store.AppendClusterName([]byte{9}, "ab", true); !bytes.Equal(got, []byte{9, 1, 2, 'a', 'b'}) {
		t.Errorf("append to a prefix gives %x", got)
	}
}

// The decoder holds the format's bound, not the limit's: a name a store would not
// write today still reads, and one byte over the format does not.
func TestClusterNameDecoderIsBoundByTheFormat(t *testing.T) {
	t.Parallel()
	for _, n := range []int{store.MaxClusterNameLen, store.MaxClusterNameLen + 1, store.MaxClusterNameFormat - 1, store.MaxClusterNameFormat} {
		name := strings.Repeat("n", n)
		got, set, err := store.DecodeClusterName(store.AppendClusterName(nil, name, true))
		if err != nil || !set || got != name {
			t.Errorf("a name of %d bytes decodes as %q, %v, %v", n, got, set, err)
		}
	}
	over := store.AppendClusterName(nil, strings.Repeat("n", store.MaxClusterNameFormat+1), true)
	if _, _, err := store.DecodeClusterName(over); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a name one byte over the format decodes as %v, want a refusal", err)
	}
}

func TestClusterNameDecoderRefuses(t *testing.T) {
	t.Parallel()
	good := store.AppendClusterName(nil, "prod", true)
	long := strings.Repeat("a", store.MaxClusterNameFormat)
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"unknown leading byte 0", append([]byte{0}, good[1:]...)},
		{"unknown leading byte 2", append([]byte{2}, good[1:]...)},
		{"unknown leading byte 0xff", append([]byte{0xff}, good[1:]...)},
		{"the version byte alone", []byte{1}},
		{"a length cut short", []byte{1, 0x80}},
		{"a name cut short", good[:len(good)-1]},
		{"a name missing", good[:2]},
		{"a trailing byte", append(bytes.Clone(good), 0)},
		{"a trailing byte after none set", []byte{1, 0, 0}},
		{"a length one over the format", append([]byte{1, byte(store.MaxClusterNameFormat + 1)}, []byte(long+"a")...)},
		{"a two-byte length over the format", append(binary.AppendUvarint([]byte{1}, 300), bytes.Repeat([]byte("a"), 300)...)},
		{"a length that overflows", append([]byte{1}, bytes.Repeat([]byte{0xff}, 10)...)},
		{"a non-shortest length of a name", append([]byte{1, 0x84, 0x00}, "prod"...)},
		{"a non-shortest length of none", []byte{1, 0x80, 0x00}},
		{"a space in the name", append([]byte{1, 4}, "pr d"...)},
		{"a NUL in the name", append([]byte{1, 4}, "pr\x00d"...)},
		{"a non-ASCII byte in the name", append([]byte{1, 2}, "\xc3\xa9"...)},
		{"a comma in the name", append([]byte{1, 4}, "pr,d"...)},
	} {
		name, set, err := store.DecodeClusterName(tc.in)
		if err == nil {
			t.Errorf("%s: %x decodes as %q, %v; want a refusal", tc.name, tc.in, name, set)
			continue
		}
		if !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v does not wrap ErrInvalid", tc.name, err)
		}
		if name != "" || set {
			t.Errorf("%s: a refusal returned %q, %v", tc.name, name, set)
		}
	}
}

// Whatever the decoder takes, the encoder writes back to the same bytes: there is
// one encoding of each value.
func FuzzClusterNameCodec(f *testing.F) {
	f.Add([]byte{1, 0})
	f.Add(store.AppendClusterName(nil, "prod", true))
	f.Add(store.AppendClusterName(nil, strings.Repeat("a", store.MaxClusterNameFormat), true))
	f.Add([]byte{1, 0x80, 0x00})
	f.Add([]byte{2, 4, 'p', 'r', 'o', 'd'})
	f.Fuzz(func(t *testing.T, b []byte) {
		name, set, err := store.DecodeClusterName(b)
		if err != nil {
			if name != "" || set {
				t.Fatalf("a refusal returned %q, %v", name, set)
			}
			return
		}
		if !set && name != "" {
			t.Fatalf("none set returned the name %q", name)
		}
		if set {
			if err := store.ValidClusterName(name); err != nil {
				t.Fatalf("decoded the invalid name %q: %v", name, err)
			}
		}
		if again := store.AppendClusterName(nil, name, set); !bytes.Equal(again, b) {
			t.Fatalf("%x decodes as %q, %v and encodes as %x", b, name, set, again)
		}
	})
}
