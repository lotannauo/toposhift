package identity

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// MaxValueLen is the longest a string identity value may be, in bytes. It
// bounds work and memory on untrusted input; identifiers are short.
const MaxValueLen = 4096

// Sentinel errors wrapped by the errors this package returns, so callers can
// tell the kind of violation with [errors.Is].
var (
	ErrUnknownType  = errors.New("unknown entity type")
	ErrMissing      = errors.New("required attribute is missing")
	ErrEmpty        = errors.New("value is empty or whitespace-only")
	ErrUnregistered = errors.New("not an identifying attribute of this entity type")
	ErrDuplicateKey = errors.New("attribute given more than once")
	ErrType         = errors.New("value has the wrong type for this attribute")
	ErrFloat        = errors.New("floating-point values are banned from identity")
	ErrNotScalar    = errors.New("value is not a scalar")
	ErrTooLong      = errors.New("value is too long")
	ErrNonCanonical = errors.New("not a canonical identity encoding")
	ErrCollision    = errors.New("fingerprint collision")
	ErrFingerprint  = errors.New("malformed fingerprint")
)

// Attr is one attribute of an entity as a producer asserts it. Value is
// untrusted: see the package documentation for what each kind of key accepts.
type Attr struct {
	Key   catalog.AttributeKey
	Value any
}

// AttrError describes a problem with one attribute, or with the entity type
// when Key is empty. It unwraps to one of the sentinel errors.
type AttrError struct {
	Type   catalog.EntityType
	Key    catalog.AttributeKey
	Err    error
	Detail string
}

func (e *AttrError) Error() string {
	var b strings.Builder
	b.WriteString("identity")
	if e.Type != "" {
		fmt.Fprintf(&b, ": entity %q", e.Type)
	}
	if e.Key != "" {
		fmt.Fprintf(&b, ": attribute %q", e.Key)
	}
	b.WriteString(": ")
	b.WriteString(e.Err.Error())
	if e.Detail != "" {
		b.WriteString(": ")
		b.WriteString(e.Detail)
	}
	return b.String()
}

func (e *AttrError) Unwrap() error { return e.Err }

func fail(err error, format string, args ...any) *AttrError {
	return &AttrError{Err: err, Detail: fmt.Sprintf(format, args...)}
}

// value is a converted identity value. Only the fields of its kind are set.
type value struct {
	kind catalog.Kind
	s    string // KindString
	i    int64  // KindInt; for KindTime, seconds since the Unix epoch
	ns   uint32 // KindTime: nanoseconds, 0 to 999999999
}

// convert changes v to the given kind or says why it cannot. The caller adds
// the entity type and key to the returned error.
func convert(kind catalog.Kind, v any) (value, *AttrError) {
	switch kind {
	case catalog.KindString:
		return convertString(v)
	case catalog.KindInt:
		return convertInt(v)
	case catalog.KindTime:
		return convertTime(v)
	}
	return value{}, fail(ErrType, "unsupported kind %s", kind)
}

func convertString(v any) (value, *AttrError) {
	s, ok := v.(string)
	if !ok {
		return value{}, reject(catalog.KindString, v)
	}
	if len(s) > MaxValueLen {
		return value{}, fail(ErrTooLong, "%d bytes, at most %d", len(s), MaxValueLen)
	}
	if !utf8.ValidString(s) {
		return value{}, fail(ErrType, "string is not valid UTF-8")
	}
	return value{kind: catalog.KindString, s: s}, nil
}

func convertInt(v any) (value, *AttrError) {
	var n int64
	switch x := v.(type) {
	case int:
		n = int64(x)
	case int8:
		n = int64(x)
	case int16:
		n = int64(x)
	case int32:
		n = int64(x)
	case int64:
		n = x
	case uint:
		return fromUint(uint64(x))
	case uint8:
		n = int64(x)
	case uint16:
		n = int64(x)
	case uint32:
		n = int64(x)
	case uint64:
		return fromUint(x)
	case string:
		if !canonicalDecimal(x) {
			return value{}, fail(ErrType, "%q is not a canonical decimal integer", x)
		}
		parsed, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return value{}, fail(ErrType, "%q does not fit in 64 bits", x)
		}
		n = parsed
	default:
		return value{}, reject(catalog.KindInt, v)
	}
	return value{kind: catalog.KindInt, i: n}, nil
}

func fromUint(u uint64) (value, *AttrError) {
	if u > math.MaxInt64 {
		return value{}, fail(ErrType, "%d does not fit in 64 bits signed", u)
	}
	return value{kind: catalog.KindInt, i: int64(u)}, nil
}

// canonicalDecimal reports whether s is "0", or an optional minus sign then
// digits with no leading zero.
func canonicalDecimal(s string) bool {
	digits := strings.TrimPrefix(s, "-")
	if digits == "" || (s == "-0") {
		return false
	}
	if digits[0] == '0' {
		return digits == "0" && s == "0"
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
}

func convertTime(v any) (value, *AttrError) {
	var t time.Time
	switch x := v.(type) {
	case time.Time:
		t = x
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, x)
		if err != nil {
			return value{}, fail(ErrType, "%q is not an RFC 3339 timestamp", x)
		}
		t = parsed
	default:
		return value{}, reject(catalog.KindTime, v)
	}
	if t.IsZero() {
		return value{}, fail(ErrEmpty, "the zero time is treated as unset")
	}
	return value{kind: catalog.KindTime, i: t.Unix(), ns: uint32(t.Nanosecond())}, nil
}

// reject classifies a value of the wrong Go type for a key of the wanted kind.
func reject(want catalog.Kind, v any) *AttrError {
	if v == nil {
		return fail(ErrNotScalar, "nil for a %s attribute", want)
	}
	if _, ok := v.(time.Time); ok {
		return fail(ErrType, "time given for a %s attribute", want)
	}
	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.Float32, reflect.Float64:
		return fail(ErrFloat, "%s given for a %s attribute", t, want)
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Struct, reflect.Pointer,
		reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fail(ErrNotScalar, "%s given for a %s attribute", t, want)
	default:
		return fail(ErrType, "%s given for a %s attribute", t, want)
	}
}
