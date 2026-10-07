package capture

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Int64 is a 64-bit signed integer that OTLP/JSON writes as a decimal string
// and reads as a decimal string or an integer number.
type Int64 int64

// Uint64 is a 64-bit unsigned integer that OTLP/JSON writes as a decimal
// string and reads as a decimal string or an integer number.
type Uint64 uint64

// MarshalJSON writes the value as a decimal string, "0" included.
func (i Int64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatInt(int64(i), 10) + `"`), nil
}

// MarshalJSON writes the value as a decimal string, "0" included.
func (u Uint64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(u), 10) + `"`), nil
}

// UnmarshalJSON reads a JSON string of a decimal integer or a JSON number, as
// proto3 JSON does: the integral forms with a fraction or an exponent (1e3,
// 1.0, "1E3") are accepted, parsed exactly, with no floating point. It refuses
// a value that is not integral (1.5, 1e-3), a value out of range, a leading
// "+", the empty string, an exponent whose magnitude is above 40
// (zero included, so no input can make the parser build a huge number), and
// every other JSON type. Refusals wrap [ErrFormat]. JSON null leaves the value
// unchanged, as it does for any [json.Unmarshaler].
func (i *Int64) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		return nil
	}
	n, err := parseSigned(b, 64)
	if err != nil {
		return err
	}
	*i = Int64(n)
	return nil
}

// UnmarshalJSON reads like [Int64.UnmarshalJSON], and refuses negative values,
// "-0" included.
func (u *Uint64) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		return nil
	}
	n, err := parseUnsigned(b, 64)
	if err != nil {
		return err
	}
	*u = Uint64(n)
	return nil
}

// maxExponent is the largest exponent magnitude an integer's input form may
// have. A 64-bit integer has at most 20 digits, so a larger exponent either
// overflows or is not an integer unless the mantissa is hundreds of digits
// long, which no producer writes.
const maxExponent = 40

func isNull(b []byte) bool { return string(bytes.TrimSpace(b)) == "null" }

// integerText returns the text of a JSON string or number token.
func integerText(b []byte) (string, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return "", errors.New("missing integer")
	}
	switch c := b[0]; {
	case c == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return "", err
		}
		return s, nil
	case c == '-' || ('0' <= c && c <= '9'):
		return string(b), nil
	default:
		return "", fmt.Errorf("want a decimal string or an integer number, got %s", snippet(b))
	}
}

func snippet(b []byte) string {
	const limit = 24
	if len(b) > limit {
		return string(b[:limit]) + "..."
	}
	return string(b)
}

var (
	errNotIntegral = errors.New("not an integer")
	errExponent    = errors.New("exponent too large")
)

func integerError(s string, err error) error {
	return formatErrorf(0, err, "invalid integer %s: %v", snippetQuoted(s), err)
}

func snippetQuoted(s string) string {
	const limit = 24
	if len(s) > limit {
		return strconv.Quote(s[:limit]) + "..."
	}
	return strconv.Quote(s)
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

// maxDigits is the length of the largest 64-bit magnitude, 1<<64 - 1.
const maxDigits = 20

// parseIntegral reads the JSON number grammar (a leading "-", digits, an
// optional fraction, an optional exponent), except that leading zeros are
// allowed and a leading "+" is not. It returns the sign and the magnitude as
// decimal digits without leading zeros ("" for zero), or an error if the value
// is not an integer, has an exponent beyond [maxExponent], or has more than 20
// digits. The arithmetic is on digits, so it allocates at most the digits of
// the input.
func parseIntegral(s string) (neg bool, mag string, err error) {
	i := 0
	if i < len(s) && s[i] == '-' {
		neg = true
		i++
	}
	scan := func() string {
		start := i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		return s[start:i]
	}
	intPart := scan()
	if intPart == "" {
		return false, "", strconv.ErrSyntax
	}
	var frac string
	if i < len(s) && s[i] == '.' {
		i++
		if frac = scan(); frac == "" {
			return false, "", strconv.ErrSyntax
		}
	}
	var expDigits string
	expNeg := false
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			expNeg = s[i] == '-'
			i++
		}
		if expDigits = scan(); expDigits == "" {
			return false, "", strconv.ErrSyntax
		}
	}
	if i != len(s) {
		return false, "", strconv.ErrSyntax
	}
	exp := 0
	if expDigits != "" {
		expDigits = strings.TrimLeft(expDigits, "0")
		if len(expDigits) > 2 {
			return false, "", errExponent
		}
		for _, c := range []byte(expDigits) {
			exp = exp*10 + int(c-'0')
		}
		if exp > maxExponent {
			return false, "", errExponent
		}
		if expNeg {
			exp = -exp
		}
	}
	digits := intPart
	if frac != "" {
		digits += frac
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return neg, "", nil
	}
	e := exp - len(frac) // the value is digits * 10^e
	if e >= 0 {
		if len(digits)+e > maxDigits {
			return false, "", strconv.ErrRange
		}
		return neg, digits + strings.Repeat("0", e), nil
	}
	k := -e
	if k > len(digits) || strings.Trim(digits[len(digits)-k:], "0") != "" {
		return false, "", errNotIntegral
	}
	digits = digits[:len(digits)-k]
	if len(digits) > maxDigits {
		return false, "", strconv.ErrRange
	}
	return neg, digits, nil
}

func parseSigned(b []byte, bits int) (int64, error) {
	s, err := integerText(b)
	if err != nil {
		return 0, err
	}
	neg, mag, err := parseIntegral(s)
	if err != nil {
		return 0, integerError(s, err)
	}
	if mag == "" {
		return 0, nil
	}
	if neg {
		mag = "-" + mag
	}
	n, err := strconv.ParseInt(mag, 10, bits)
	if err != nil {
		return 0, integerError(s, errors.Unwrap(err))
	}
	return n, nil
}

func parseUnsigned(b []byte, bits int) (uint64, error) {
	s, err := integerText(b)
	if err != nil {
		return 0, err
	}
	neg, mag, err := parseIntegral(s)
	if err != nil {
		return 0, integerError(s, err)
	}
	if neg {
		return 0, integerError(s, strconv.ErrSyntax)
	}
	if mag == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(mag, 10, bits)
	if err != nil {
		return 0, integerError(s, errors.Unwrap(err))
	}
	return n, nil
}

// HexBytes is a byte string that OTLP/JSON writes as hex: the encoding of
// trace and span IDs, which are not base64.
type HexBytes []byte

// MarshalJSON writes lowercase hex. A capture re-encoded from the typed model
// can therefore differ in case from the input; byte-exact round trips use
// [Line.Raw].
func (h HexBytes) MarshalJSON() ([]byte, error) {
	out := make([]byte, 0, 2+hex.EncodedLen(len(h)))
	out = append(out, '"')
	out = hex.AppendEncode(out, h)
	return append(out, '"'), nil
}

// UnmarshalJSON reads hex of either case. The empty string is empty bytes; an
// odd length or a character that is not a hex digit is an error. JSON null
// leaves the value unchanged.
func (h *HexBytes) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("hex bytes: %w", err)
	}
	if s == "" {
		*h = nil
		return nil
	}
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("hex bytes: %w", err)
	}
	*h = decoded
	return nil
}

// AnyValue is the OTLP value union. At most one field is set; none set is the
// empty value, written as {}. The fields are pointers so that a set zero value
// (an empty string, false, 0) is told from an unset one.
type AnyValue struct {
	StringValue *string
	BoolValue   *bool
	IntValue    *Int64
	DoubleValue *float64
	ArrayValue  *ArrayValue
	KVListValue *KeyValueList
	BytesValue  *[]byte
}

// ArrayValue is the payload of an array [AnyValue].
type ArrayValue struct {
	Values []AnyValue `json:"values,omitempty"`
}

// KeyValueList is the payload of a key-value list [AnyValue]. Order and
// repeated keys are kept.
type KeyValueList struct {
	Values []KeyValue `json:"values,omitempty"`
}

// anyKind is one member of the union, in the order they are checked and
// reported.
type anyKind struct {
	key string
	set func(*AnyValue) bool
}

var anyKinds = [...]anyKind{
	{"stringValue", func(v *AnyValue) bool { return v.StringValue != nil }},
	{"boolValue", func(v *AnyValue) bool { return v.BoolValue != nil }},
	{"intValue", func(v *AnyValue) bool { return v.IntValue != nil }},
	{"doubleValue", func(v *AnyValue) bool { return v.DoubleValue != nil }},
	{"arrayValue", func(v *AnyValue) bool { return v.ArrayValue != nil }},
	{"kvlistValue", func(v *AnyValue) bool { return v.KVListValue != nil }},
	{"bytesValue", func(v *AnyValue) bool { return v.BytesValue != nil }},
}

func (v *AnyValue) setKinds() []string {
	var keys []string
	for _, k := range anyKinds {
		if k.set(v) {
			keys = append(keys, k.key)
		}
	}
	return keys
}

// MarshalJSON writes the one set field. Two set fields are an error. A
// double that is NaN or infinite is written as "NaN", "Infinity" or
// "-Infinity".
func (v AnyValue) MarshalJSON() ([]byte, error) {
	keys := v.setKinds()
	switch len(keys) {
	case 0:
		return []byte("{}"), nil
	case 1:
	default:
		return nil, fmt.Errorf("any value has more than one kind set: %s", strings.Join(keys, ", "))
	}
	var payload any
	switch keys[0] {
	case "stringValue":
		payload = *v.StringValue
	case "boolValue":
		payload = *v.BoolValue
	case "intValue":
		payload = *v.IntValue
	case "doubleValue":
		payload = doubleJSON(*v.DoubleValue)
	case "arrayValue":
		payload = v.ArrayValue
	case "kvlistValue":
		payload = v.KVListValue
	case "bytesValue":
		payload = base64.StdEncoding.EncodeToString(*v.BytesValue)
	}
	body, err := marshalNoEscape(payload)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(keys[0])+len(body)+5)
	out = append(out, '{', '"')
	out = append(out, keys[0]...)
	out = append(out, '"', ':')
	out = append(out, body...)
	return append(out, '}'), nil
}

// doubleJSON returns f, or its proto3 JSON string for NaN and the infinities.
func doubleJSON(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	default:
		return f
	}
}

// UnmarshalJSON reads one value. Keys are matched exactly; unknown keys are
// ignored; a key whose value is null counts as absent. Two different kinds in
// one object are an error; the same key twice is allowed and the last one
// wins, as in [encoding/json]. JSON null leaves the value unchanged.
func (v *AnyValue) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		return nil
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(b, &members); err != nil {
		return fmt.Errorf("any value: %w", err)
	}
	var out AnyValue
	var present []string
	for _, k := range anyKinds {
		raw, ok := members[k.key]
		if !ok || isNull(raw) {
			continue
		}
		present = append(present, k.key)
		if err := out.decodeKind(k.key, raw); err != nil {
			return fmt.Errorf("%s: %w", k.key, err)
		}
	}
	if len(present) > 1 {
		return fmt.Errorf("any value has more than one kind: %s", strings.Join(present, ", "))
	}
	*v = out
	return nil
}

func (v *AnyValue) decodeKind(key string, raw json.RawMessage) error {
	switch key {
	case "stringValue":
		v.StringValue = new(string)
		return json.Unmarshal(raw, v.StringValue)
	case "boolValue":
		v.BoolValue = new(bool)
		return json.Unmarshal(raw, v.BoolValue)
	case "intValue":
		v.IntValue = new(Int64)
		return json.Unmarshal(raw, v.IntValue)
	case "doubleValue":
		f, err := parseDouble(raw)
		if err != nil {
			return err
		}
		v.DoubleValue = &f
	case "arrayValue":
		v.ArrayValue = new(ArrayValue)
		return json.Unmarshal(raw, v.ArrayValue)
	case "kvlistValue":
		v.KVListValue = new(KeyValueList)
		return json.Unmarshal(raw, v.KVListValue)
	case "bytesValue":
		decoded, err := parseBase64(raw)
		if err != nil {
			return err
		}
		v.BytesValue = &decoded
	}
	return nil
}

func parseDouble(raw json.RawMessage) (float64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		switch s {
		case "NaN":
			return math.NaN(), nil
		case "Infinity":
			return math.Inf(1), nil
		case "-Infinity":
			return math.Inf(-1), nil
		}
		return parseDoubleString(s)
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0, err
	}
	return f, nil
}

// parseDoubleString reads a double written as a string, which proto3 JSON
// allows besides "NaN", "Infinity" and "-Infinity": a decimal number, with an
// optional sign, fraction and exponent. Other spellings that strconv accepts
// (hexadecimal, underscores, "inf", "nan") and values out of range are errors.
func parseDoubleString(s string) (float64, error) {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !isDigit(c) && !strings.ContainsRune("+-.eE", rune(c)) {
			return 0, fmt.Errorf("invalid double %s", snippetQuoted(s))
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid double %s: %w", snippetQuoted(s), errors.Unwrap(err))
	}
	return f, nil
}

// parseBase64 reads standard base64, padded or not, or its URL-safe
// alphabet, padded or not. The result is never nil.
func parseBase64(raw json.RawMessage) ([]byte, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	var firstErr error
	for _, enc := range [...]*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
	} {
		decoded, err := enc.DecodeString(s)
		if err == nil {
			if decoded == nil {
				decoded = []byte{}
			}
			return decoded, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, fmt.Errorf("invalid base64: %w", firstErr)
}

// marshalNoEscape is [json.Marshal] without escaping <, > and &, which
// collectors do not escape either. It has no trailing newline.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// parseInt32 reads a proto3 JSON 32-bit integer: a number or a decimal
// string. Absent and null are zero.
func parseInt32(raw json.RawMessage) (int32, error) {
	if len(raw) == 0 || isNull(raw) {
		return 0, nil
	}
	n, err := parseSigned(raw, 32)
	return int32(n), err
}

func parseUint32(raw json.RawMessage) (uint32, error) {
	if len(raw) == 0 || isNull(raw) {
		return 0, nil
	}
	n, err := parseUnsigned(raw, 32)
	return uint32(n), err
}
