package capture_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

func TestInt64Decode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: `"10"`, want: 10},
		{in: `10`, want: 10},
		{in: `-10`, want: -10},
		{in: `"-9223372036854775808"`, want: math.MinInt64},
		{in: `-9223372036854775808`, want: math.MinInt64},
		{in: `"9223372036854775807"`, want: math.MaxInt64},
		{in: `"0"`, want: 0},
		{in: `"1.5"`, wantErr: true},
		{in: `1.5`, wantErr: true},
		{in: `1e3`, want: 1000},
		{in: `1E3`, want: 1000},
		{in: `1e+3`, want: 1000},
		{in: `"1e3"`, want: 1000},
		{in: `1.0`, want: 1},
		{in: `"1.0"`, want: 1},
		{in: `1.50e1`, want: 15},
		{in: `15e-1`, wantErr: true},
		{in: `10e-1`, want: 1},
		{in: `100e-2`, want: 1},
		{in: `-2.5e1`, want: -25},
		{in: `0e5`, want: 0},
		{in: `0.0`, want: 0},
		{in: `-0`, want: 0},
		{in: `-0.0e3`, want: 0},
		{in: `1e-3`, wantErr: true},
		{in: `0.5`, wantErr: true},
		{in: `1.05e1`, wantErr: true},
		{in: `"007"`, want: 7},
		{in: `9.223372036854775807e18`, want: math.MaxInt64},
		{in: `-9.223372036854775808e18`, want: math.MinInt64},
		{in: `9.223372036854775808e18`, wantErr: true},
		{in: `1e19`, wantErr: true},
		{in: `"1e40"`, wantErr: true},
		{in: `1e41`, wantErr: true},
		{in: `0e41`, wantErr: true},
		{in: `1e-41`, wantErr: true},
		{in: `1e0040`, wantErr: true},
		{in: `1e00040000`, wantErr: true},
		{in: `1e999999999999999999999`, wantErr: true},
		{in: `"1."`, wantErr: true},
		{in: `".5"`, wantErr: true},
		{in: `"1e"`, wantErr: true},
		{in: `"1e+"`, wantErr: true},
		{in: `"1.e3"`, wantErr: true},
		{in: `"--1"`, wantErr: true},
		{in: `"+1"`, wantErr: true},
		{in: `"+1e3"`, wantErr: true},
		{in: `""`, wantErr: true},
		{in: `" 1"`, wantErr: true},
		{in: `"1 "`, wantErr: true},
		{in: `"1_0"`, wantErr: true},
		{in: `"0x10"`, wantErr: true},
		{in: `"9223372036854775808"`, wantErr: true},
		{in: `"-9223372036854775809"`, wantErr: true},
		{in: `9223372036854775808`, wantErr: true},
		{in: `"abc"`, wantErr: true},
		{in: `true`, wantErr: true},
		{in: `{}`, wantErr: true},
		{in: `[1]`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			var got capture.Int64
			err := json.Unmarshal([]byte(tc.in), &got)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if err == nil && int64(got) != tc.want {
				t.Errorf("Unmarshal(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestUint64Decode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: `"10"`, want: 10},
		{in: `10`, want: 10},
		{in: `"18446744073709551615"`, want: math.MaxUint64},
		{in: `18446744073709551615`, want: math.MaxUint64},
		{in: `"0"`, want: 0},
		{in: `"18446744073709551616"`, wantErr: true},
		{in: `18446744073709551616`, wantErr: true},
		{in: `"-1"`, wantErr: true},
		{in: `-1`, wantErr: true},
		{in: `-0`, wantErr: true},
		{in: `"1.5"`, wantErr: true},
		{in: `1.5`, wantErr: true},
		{in: `1e3`, want: 1000},
		{in: `"1E3"`, want: 1000},
		{in: `1.0`, want: 1},
		{in: `1e19`, want: 10000000000000000000},
		{in: `1.8446744073709551615e19`, want: math.MaxUint64},
		{in: `1.8446744073709551616e19`, wantErr: true},
		{in: `1e20`, wantErr: true},
		{in: `1e41`, wantErr: true},
		{in: `-0.0`, wantErr: true},
		{in: `-0e3`, wantErr: true},
		{in: `"-0"`, wantErr: true},
		{in: `-1e0`, wantErr: true},
		{in: `1e-3`, wantErr: true},
		{in: `"+1"`, wantErr: true},
		{in: `""`, wantErr: true},
		{in: `"abc"`, wantErr: true},
		{in: `true`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			var got capture.Uint64
			err := json.Unmarshal([]byte(tc.in), &got)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if err == nil && uint64(got) != tc.want {
				t.Errorf("Unmarshal(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestIntegerNull pins what null does: like an absent field, it leaves the
// value as it was.
func TestIntegerNull(t *testing.T) {
	t.Parallel()
	var i capture.Int64
	if err := json.Unmarshal([]byte(`null`), &i); err != nil || i != 0 {
		t.Errorf("Int64 from null = %d, %v; want 0, nil", i, err)
	}
	i = 7
	if err := json.Unmarshal([]byte(`null`), &i); err != nil || i != 7 {
		t.Errorf("Int64 set to 7 then null = %d, %v; want 7, nil", i, err)
	}
	var u capture.Uint64
	if err := json.Unmarshal([]byte(`null`), &u); err != nil || u != 0 {
		t.Errorf("Uint64 from null = %d, %v; want 0, nil", u, err)
	}
	u = 7
	if err := json.Unmarshal([]byte(`null`), &u); err != nil || u != 7 {
		t.Errorf("Uint64 set to 7 then null = %d, %v; want 7, nil", u, err)
	}
}

func TestIntegerEncode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   any
		want string
	}{
		{capture.Int64(10), `"10"`},
		{capture.Int64(0), `"0"`},
		{capture.Int64(-1), `"-1"`},
		{capture.Int64(math.MinInt64), `"-9223372036854775808"`},
		{capture.Uint64(0), `"0"`},
		{capture.Uint64(math.MaxUint64), `"18446744073709551615"`},
	}
	for _, tc := range tests {
		got, err := json.Marshal(tc.in)
		if err != nil || string(got) != tc.want {
			t.Errorf("Marshal(%v) = %s, %v; want %s", tc.in, got, err, tc.want)
		}
	}
}

// TestIntegerOmitEmpty checks that a zero 64-bit field is left out of a
// record and that a non-zero one is a string.
func TestIntegerOmitEmpty(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(capture.LogRecord{})
	if err != nil || string(got) != `{}` {
		t.Errorf("Marshal(LogRecord{}) = %s, %v; want {}", got, err)
	}
	got, err = json.Marshal(capture.LogRecord{TimeUnixNano: 5})
	if err != nil || string(got) != `{"timeUnixNano":"5"}` {
		t.Errorf("Marshal(time 5) = %s, %v", got, err)
	}
}

func TestHexBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    []byte
		wantErr bool
	}{
		{in: `"5b8efff7"`, want: []byte{0x5b, 0x8e, 0xff, 0xf7}},
		{in: `"5B8EFFF7"`, want: []byte{0x5b, 0x8e, 0xff, 0xf7}},
		{in: `"5b8EfFf7"`, want: []byte{0x5b, 0x8e, 0xff, 0xf7}},
		{in: `""`, want: nil},
		{in: `"abc"`, wantErr: true},
		{in: `"a"`, wantErr: true},
		{in: `"zz"`, wantErr: true},
		{in: `"0x00"`, wantErr: true},
		{in: `"00 "`, wantErr: true},
		{in: `5`, wantErr: true},
		{in: `true`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			var got capture.HexBytes
			err := json.Unmarshal([]byte(tc.in), &got)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if len(got) != len(tc.want) || (len(got) > 0 && !reflect.DeepEqual([]byte(got), tc.want)) {
				t.Errorf("Unmarshal(%s) = %x, want %x", tc.in, []byte(got), tc.want)
			}
			out, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if want := strings.ToLower(tc.in); string(out) != want {
				t.Errorf("Marshal = %s, want lowercase %s", out, want)
			}
		})
	}
}

func TestAnyValueRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in string }{
		{"empty", `{}`},
		{"string", `{"stringValue":"some string"}`},
		{"empty string", `{"stringValue":""}`},
		{"bool true", `{"boolValue":true}`},
		{"bool false", `{"boolValue":false}`},
		{"int", `{"intValue":"10"}`},
		{"int zero", `{"intValue":"0"}`},
		{"int negative", `{"intValue":"-9223372036854775808"}`},
		{"double", `{"doubleValue":637.704}`},
		{"double zero", `{"doubleValue":0}`},
		{"array", `{"arrayValue":{"values":[{"stringValue":"many"},{"intValue":"1"},{}]}}`},
		{"empty array", `{"arrayValue":{}}`},
		{"kvlist", `{"kvlistValue":{"values":[{"key":"k","value":{"boolValue":true}},{"key":"k","value":{}}]}}`},
		{"empty kvlist", `{"kvlistValue":{}}`},
		{"bytes", `{"bytesValue":"AQID"}`},
		{"empty bytes", `{"bytesValue":""}`},
		{"nested", `{"arrayValue":{"values":[{"kvlistValue":{"values":[{"key":"a","value":{"arrayValue":{"values":[{"doubleValue":1.5}]}}}]}}]}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var v capture.AnyValue
			if err := json.Unmarshal([]byte(tc.in), &v); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.in, err)
			}
			out, err := json.Marshal(v)
			if err != nil || string(out) != tc.in {
				t.Errorf("Marshal = %s, %v; want %s", out, err, tc.in)
			}
		})
	}
}

func TestAnyValueTypedFields(t *testing.T) {
	t.Parallel()
	var v capture.AnyValue
	if err := json.Unmarshal([]byte(`{"intValue":10}`), &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if v.IntValue == nil || *v.IntValue != 10 {
		t.Errorf("IntValue = %v, want 10 (a number is accepted)", v.IntValue)
	}
	v = capture.AnyValue{}
	if err := json.Unmarshal([]byte(`{"stringValue":""}`), &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if v.StringValue == nil || *v.StringValue != "" {
		t.Errorf("StringValue = %v, want a set empty string", v.StringValue)
	}
	v = capture.AnyValue{}
	if err := json.Unmarshal([]byte(`{}`), &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(v, capture.AnyValue{}) {
		t.Errorf("{} = %+v, want the empty value", v)
	}
}

func TestAnyValueDecodeErrors(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in string }{
		{"two kinds", `{"stringValue":"a","boolValue":true}`},
		{"two kinds, one empty", `{"stringValue":"","intValue":"0"}`},
		{"three kinds", `{"stringValue":"a","boolValue":true,"intValue":"1"}`},
		{"array and kvlist", `{"arrayValue":{},"kvlistValue":{}}`},
		{"nested two kinds", `{"arrayValue":{"values":[{"stringValue":"a","boolValue":true}]}}`},
		{"string of wrong type", `{"stringValue":1}`},
		{"bool of wrong type", `{"boolValue":"true"}`},
		{"bad int", `{"intValue":"1.5"}`},
		{"double word", `{"doubleValue":"inf"}`},
		{"double lowercase nan", `{"doubleValue":"nan"}`},
		{"double hex", `{"doubleValue":"0x1p-2"}`},
		{"double underscore", `{"doubleValue":"1_000"}`},
		{"double empty string", `{"doubleValue":""}`},
		{"double padded string", `{"doubleValue":" 1.5"}`},
		{"double string out of range", `{"doubleValue":"1e999"}`},
		{"double string garbage", `{"doubleValue":"1.2.3"}`},
		{"double bool", `{"doubleValue":true}`},
		{"bad base64", `{"bytesValue":"!!!"}`},
		{"not an object", `"x"`},
		{"array", `[]`},
		{"array not an object", `{"arrayValue":[]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var v capture.AnyValue
			if err := json.Unmarshal([]byte(tc.in), &v); err == nil {
				t.Errorf("Unmarshal(%s) = %+v, want an error", tc.in, v)
			}
		})
	}
}

// TestAnyValueEncodeTwoKinds checks the encoder refuses what the decoder
// refuses.
func TestAnyValueEncodeTwoKinds(t *testing.T) {
	t.Parallel()
	s, b := "a", true
	if out, err := json.Marshal(capture.AnyValue{StringValue: &s, BoolValue: &b}); err == nil {
		t.Errorf("Marshal of two kinds = %s, want an error", out)
	}
}

func TestAnyValueQuirks(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in, want string }{
		{"unknown key", `{"stringValue":"a","futureValue":1}`, `{"stringValue":"a"}`},
		{"only unknown keys", `{"futureValue":1}`, `{}`},
		{"same kind twice, last wins", `{"stringValue":"a","stringValue":"b"}`, `{"stringValue":"b"}`},
		{"null kind is absent", `{"stringValue":null,"intValue":"1"}`, `{"intValue":"1"}`},
		{"keys are matched exactly", `{"StringValue":"a"}`, `{}`},
		{"unknown key in an array element", `{"arrayValue":{"values":[{"x":1}],"y":2}}`, `{"arrayValue":{"values":[{}]}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var v capture.AnyValue
			if err := json.Unmarshal([]byte(tc.in), &v); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.in, err)
			}
			if out, err := json.Marshal(v); err != nil || string(out) != tc.want {
				t.Errorf("Marshal = %s, %v; want %s", out, err, tc.want)
			}
		})
	}
}

func TestAnyValueSpecialDoubles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in    string
		check func(float64) bool
	}{
		{`"NaN"`, math.IsNaN},
		{`"Infinity"`, func(f float64) bool { return math.IsInf(f, 1) }},
		{`"-Infinity"`, func(f float64) bool { return math.IsInf(f, -1) }},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			in := `{"doubleValue":` + tc.in + `}`
			var v capture.AnyValue
			if err := json.Unmarshal([]byte(in), &v); err != nil {
				t.Fatalf("Unmarshal(%s): %v", in, err)
			}
			if v.DoubleValue == nil || !tc.check(*v.DoubleValue) {
				t.Fatalf("DoubleValue = %v", v.DoubleValue)
			}
			out, err := json.Marshal(v)
			if err != nil || string(out) != in {
				t.Errorf("Marshal = %s, %v; want %s", out, err, in)
			}
		})
	}
}

func TestAnyValueBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in string
		want     []byte
	}{
		{"padded", `"AQI="`, []byte{1, 2}},
		{"unpadded", `"AQI"`, []byte{1, 2}},
		{"no padding needed", `"AQID"`, []byte{1, 2, 3}},
		{"standard alphabet", `"+/8="`, []byte{0xfb, 0xff}},
		{"url alphabet padded", `"-_8="`, []byte{0xfb, 0xff}},
		{"url alphabet unpadded", `"-_8"`, []byte{0xfb, 0xff}},
		{"empty", `""`, []byte{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var v capture.AnyValue
			if err := json.Unmarshal([]byte(`{"bytesValue":`+tc.in+`}`), &v); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if v.BytesValue == nil || !reflect.DeepEqual(*v.BytesValue, tc.want) {
				t.Errorf("BytesValue = %v, want %v", v.BytesValue, tc.want)
			}
		})
	}
	// Output is standard base64 with padding.
	out, err := json.Marshal(capture.AnyValue{BytesValue: &[]byte{0xfb, 0xff}})
	if err != nil || string(out) != `{"bytesValue":"+/8="}` {
		t.Errorf("Marshal = %s, %v", out, err)
	}
}

func TestAnyValueDoubleStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want float64
	}{
		{`"1.5"`, 1.5},
		{`"-2"`, -2},
		{`"1e3"`, 1000},
		{`"1E-2"`, 0.01},
		{`"0"`, 0},
		{`"5."`, 5},
		{`".5"`, 0.5},
	}
	for _, tc := range tests {
		in := `{"doubleValue":` + tc.in + `}`
		var v capture.AnyValue
		if err := json.Unmarshal([]byte(in), &v); err != nil {
			t.Errorf("Unmarshal(%s): %v", in, err)
			continue
		}
		if v.DoubleValue == nil || *v.DoubleValue != tc.want {
			t.Errorf("Unmarshal(%s) = %v, want %v", in, v.DoubleValue, tc.want)
		}
		// It is written back as a number.
		if out, err := json.Marshal(v); err != nil || strings.Contains(string(out), `:"`) {
			t.Errorf("Marshal = %s, %v; want a number", out, err)
		}
	}
}

// TestIntegerErrorsAreFormatErrors checks that every refusal of an integer,
// the exponent bound included, wraps ErrFormat.
func TestIntegerErrorsAreFormatErrors(t *testing.T) {
	t.Parallel()
	for _, in := range []string{`1e41`, `"1e999999999999"`, `1.5`, `"abc"`, `""`, `"+1"`} {
		var i capture.Int64
		err := json.Unmarshal([]byte(in), &i)
		if err == nil || !errors.Is(err, capture.ErrFormat) {
			t.Errorf("Int64 from %s: error = %v, want one wrapping ErrFormat", in, err)
		}
		var u capture.Uint64
		if err := json.Unmarshal([]byte(in), &u); err == nil || !errors.Is(err, capture.ErrFormat) {
			t.Errorf("Uint64 from %s: error = %v, want one wrapping ErrFormat", in, err)
		}
	}
}

// TestIntegerHostileInputs feeds inputs that would be expensive if parsed by
// building the number: they must be refused quickly and without growing.
func TestIntegerHostileInputs(t *testing.T) {
	t.Parallel()
	zeros := strings.Repeat("0", 1_000_000)
	for _, in := range []string{
		`1e` + strings.Repeat("9", 1_000_000),
		`"1e` + strings.Repeat("9", 1_000_000) + `"`,
		`0.` + zeros + `1e40`,
		`1` + zeros,
		`1.` + zeros + `1`,
		`1` + zeros + `e-40`,
	} {
		var i capture.Int64
		if err := json.Unmarshal([]byte(in), &i); err == nil {
			t.Errorf("Int64 accepted a hostile input of %d bytes as %d", len(in), i)
		}
	}
	// An exponent of 2^64+1 would wrap to 1 in 64-bit arithmetic.
	var i capture.Int64
	if err := json.Unmarshal([]byte(`1e18446744073709551617`), &i); err == nil || !errors.Is(err, capture.ErrFormat) {
		t.Errorf("1e18446744073709551617 = %d, %v; want an error", i, err)
	}
	if err := json.Unmarshal([]byte(`1e000000000000000000001`), &i); err != nil || i != 10 {
		t.Errorf("1e000000000000000000001 = %d, %v; want 10", i, err)
	}
	var big capture.Int64
	if err := json.Unmarshal([]byte(`1e19`), &big); err == nil || !errors.Is(err, capture.ErrFormat) {
		t.Errorf("Int64 from 1e19: error = %v, want a range error wrapping ErrFormat", err)
	}
	// A long mantissa that is integral and fits is fine.
	if err := json.Unmarshal([]byte(`1`+zeros[:30]+`e-30`), &i); err != nil || i != 1 {
		t.Errorf("1 followed by 30 zeros, e-30 = %d, %v; want 1", i, err)
	}
	if err := json.Unmarshal([]byte(`0.`+zeros[:10]+`e5`), &i); err != nil || i != 0 {
		t.Errorf("0.0000000000e5 = %d, %v; want 0", i, err)
	}
	if err := json.Unmarshal([]byte(`"`+strings.Repeat("0", 100)+`7"`), &i); err != nil || i != 7 {
		t.Errorf("leading zeros = %d, %v; want 7", i, err)
	}
}
