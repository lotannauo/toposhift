package identity

import (
	"encoding/binary"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// entry is one decoded (name, value) pair.
type entry struct {
	name catalog.AttributeKey
	v    value
}

// minEntrySize is the smallest an encoded entry can be: a length, a one-byte
// name, a tag and an 8-byte payload. It bounds the entry count against the
// bytes actually present, so a hostile count cannot force a large allocation.
const minEntrySize = 4 + 1 + 1 + 8

// zeroTimeUnix is the Unix seconds of the zero time.Time, which convertTime
// rejects as unset and so must never appear in an encoding.
var zeroTimeUnix = time.Time{}.Unix()

func nonCanonical(format string, args ...any) error {
	return fail(ErrNonCanonical, format, args...)
}

// reader is a cursor over an encoded identity. Each method reports failure
// by returning false and leaves the cursor unusable.
type reader struct {
	s   string
	off int
}

func (r *reader) take(n int) (string, bool) {
	if n < 0 || n > len(r.s)-r.off {
		return "", false
	}
	out := r.s[r.off : r.off+n]
	r.off += n
	return out, true
}

func (r *reader) u32() (uint32, bool) {
	b, ok := r.take(4)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32([]byte(b)), true
}

func (r *reader) u64() (uint64, bool) {
	b, ok := r.take(8)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64([]byte(b)), true
}

// name reads a length-prefixed name of at most catalog.MaxNameLen bytes.
func (r *reader) name() (string, bool) {
	n, ok := r.u32()
	if !ok || n > catalog.MaxNameLen {
		return "", false
	}
	return r.take(int(n))
}

// decode parses a canonical encoding, accepting exactly the byte strings the
// encoder can produce: so decode(b) succeeding means b is the one encoding of
// what it returns. It knows nothing about any catalog; the resolver checks
// the result against one.
func decode(canonical string) (catalog.EntityType, []entry, error) {
	r := &reader{s: canonical}

	if head, ok := r.take(len(magic) + 1); !ok || head[:len(magic)] != magic {
		return "", nil, nonCanonical("missing magic prefix")
	} else if head[len(magic)] != EncodingVersion {
		return "", nil, nonCanonical("unsupported encoding version %d", head[len(magic)])
	}

	typeName, ok := r.name()
	if !ok {
		return "", nil, nonCanonical("bad entity type length")
	}
	typ := catalog.EntityType(typeName)
	if !typ.Valid() {
		return "", nil, nonCanonical("invalid entity type %q", typeName)
	}

	count, ok := r.u32()
	if !ok || uint64(count) > uint64(len(r.s)-r.off)/minEntrySize {
		return "", nil, nonCanonical("entry count %d does not fit the remaining bytes", count)
	}

	entries := make([]entry, 0, count)
	prev := ""
	for range count {
		name, ok := r.name()
		if !ok || !catalog.AttributeKey(name).Valid() {
			return "", nil, nonCanonical("bad attribute name")
		}
		if len(entries) > 0 && name <= prev {
			return "", nil, nonCanonical("attribute %q is out of order or repeated", name)
		}
		prev = name

		v, err := readValue(r)
		if err != nil {
			return "", nil, nonCanonical("attribute %q: %v", name, err)
		}
		entries = append(entries, entry{name: catalog.AttributeKey(name), v: v})
	}
	if r.off != len(r.s) {
		return "", nil, nonCanonical("%d trailing bytes", len(r.s)-r.off)
	}
	return typ, entries, nil
}

func readValue(r *reader) (value, error) {
	tag, ok := r.take(1)
	if !ok {
		return value{}, errors.New("truncated")
	}
	switch tag[0] {
	case tagString:
		n, ok := r.u32()
		if !ok || n > MaxValueLen {
			return value{}, errors.New("bad string length")
		}
		s, ok := r.take(int(n))
		if !ok {
			return value{}, errors.New("truncated string")
		}
		if !utf8.ValidString(s) {
			return value{}, errors.New("string is not valid UTF-8")
		}
		if strings.TrimSpace(s) == "" {
			return value{}, errors.New("string is empty or whitespace-only")
		}
		return value{kind: catalog.KindString, s: s}, nil
	case tagInt:
		u, ok := r.u64()
		if !ok {
			return value{}, errors.New("truncated int")
		}
		return value{kind: catalog.KindInt, i: int64(u)}, nil
	case tagTime:
		sec, ok := r.u64()
		if !ok {
			return value{}, errors.New("truncated time")
		}
		ns, ok := r.u32()
		if !ok || ns > 999_999_999 {
			return value{}, errors.New("bad nanoseconds")
		}
		if int64(sec) == zeroTimeUnix && ns == 0 {
			return value{}, errors.New("the zero time is treated as unset")
		}
		return value{kind: catalog.KindTime, i: int64(sec), ns: ns}, nil
	}
	return value{}, errors.New("unknown value tag")
}
