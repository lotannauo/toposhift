package identity

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// build assembles an encoding by hand so a test can break exactly one rule.
func build(typ string, entries ...string) string {
	b := appendHeader(nil, catalog.EntityType(typ), len(entries))
	out := string(b)
	for _, e := range entries {
		out += e
	}
	return out
}

func entryString(name, val string) string {
	return string(appendEntry(nil, catalog.AttributeKey(name), value{kind: catalog.KindString, s: val}))
}

func entryInt(name string, n int64) string {
	return string(appendEntry(nil, catalog.AttributeKey(name), value{kind: catalog.KindInt, i: n}))
}

func entryTime(name string, sec int64, ns uint32) string {
	return string(appendEntry(nil, catalog.AttributeKey(name), value{kind: catalog.KindTime, i: sec, ns: ns}))
}

// TestDecodeRejectsEveryNonCanonicalForm checks that the decoder accepts
// exactly what the encoder produces: each case breaks one rule of the
// encoding and must be refused.
func TestDecodeRejectsEveryNonCanonicalForm(t *testing.T) {
	t.Parallel()

	ok := build("host", entryString("host.id", "h1"))
	if _, _, err := decode(ok); err != nil {
		t.Fatalf("the baseline encoding was rejected: %v", err)
	}

	// An entry whose tag byte, right after the name "host.id", is 0x09.
	good := entryString("host.id", "h1")
	badTag := good[:4+len("host.id")] + "\x09" + good[4+len("host.id")+1:]

	withVersion := func(v byte) string {
		b := []byte(ok)
		b[len(magic)] = v
		return string(b)
	}

	tests := map[string]string{
		"empty":                     "",
		"wrong magic":               "toposhift/identitx" + ok[len(magic):],
		"truncated magic":           ok[:5],
		"version 0":                 withVersion(0),
		"version 2":                 withVersion(2),
		"truncated after header":    ok[:len(magic)+1],
		"trailing byte":             ok + "x",
		"truncated payload":         ok[:len(ok)-1],
		"entry count too large":     magic + "\x01\x00\x00\x00\x04host\xff\xff\xff\xff",
		"invalid type name":         build("Host", entryString("host.id", "h1")),
		"invalid attribute name":    build("host", entryString("Host ID", "h1")),
		"unsorted entries":          build("process", entryInt("process.pid", 1), entryString("host.id", "h")),
		"repeated entry":            build("host", entryString("host.id", "a"), entryString("host.id", "b")),
		"empty string value":        build("host", entryString("host.id", "")),
		"whitespace-only value":     build("host", entryString("host.id", " \t")),
		"invalid UTF-8 value":       build("host", entryString("host.id", "a\xffb")),
		"unknown tag":               build("host", badTag),
		"nanoseconds too large":     build("process", entryTime("process.creation.time", 1, 1_000_000_000)),
		"zero time":                 build("process", entryTime("process.creation.time", zeroTimeUnix, 0)),
		"oversize string length":    build("host", string(appendEntry(nil, "host.id", value{kind: catalog.KindString, s: string(make([]byte, MaxValueLen+1))}))),
		"oversize type name length": magic + "\x01\x00\x00\x01\x00",
	}
	for name, enc := range tests {
		_, _, err := decode(enc)
		if !errors.Is(err, ErrNonCanonical) {
			t.Errorf("%s: err = %v, want ErrNonCanonical", name, err)
		}
	}
}

// TestDecodeReturnsWhatWasEncoded is the bijection checked from the encoder's
// side: a decoded encoding re-encodes to the same bytes.
func TestDecodeReturnsWhatWasEncoded(t *testing.T) {
	t.Parallel()

	enc := build("process",
		entryString("host.id", "i-0abc"),
		entryTime("process.creation.time", 1700558734, 853_000_000),
		entryInt("process.pid", -5),
	)
	typ, entries, err := decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	re := string(appendHeader(nil, typ, len(entries)))
	for _, e := range entries {
		re += string(appendEntry(nil, e.name, e.v))
	}
	if re != enc {
		t.Errorf("re-encoded bytes differ:\n got %x\nwant %x", re, enc)
	}
	if got := entries[1].v.native().(time.Time); got.Unix() != 1700558734 || got.Nanosecond() != 853_000_000 {
		t.Errorf("decoded time = %v", got)
	}
}

// FuzzDecodeBijection checks, on arbitrary bytes and without any catalog,
// that the decoder never panics and that whatever it accepts re-encodes to
// the identical bytes: every identity has exactly one encoding.
func FuzzDecodeBijection(f *testing.F) {
	f.Add(build("host", entryString("host.id", "h1")))
	f.Add(build("process", entryString("host.id", "h"), entryTime("process.creation.time", -1, 5), entryInt("process.pid", 9)))
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		typ, entries, err := decode(s)
		if err != nil {
			return
		}
		re := string(appendHeader(nil, typ, len(entries)))
		for _, e := range entries {
			re += string(appendEntry(nil, e.name, e.v))
		}
		if re != s {
			t.Fatalf("accepted a non-canonical encoding:\n in %x\nout %x", s, re)
		}
	})
}
