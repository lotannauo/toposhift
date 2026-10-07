package capture_test

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// Generators produce values in the form decoding gives back: absent slices
// are nil, bytes are non-nil, hex IDs are nil or non-empty, and doubles are
// never NaN (NaN is not equal to itself; it has its own test).

func genAnyValue(depth int) *rapid.Generator[capture.AnyValue] {
	return rapid.Custom(func(t *rapid.T) capture.AnyValue {
		kinds := 5
		if depth > 0 {
			kinds = 7
		}
		var v capture.AnyValue
		switch rapid.IntRange(0, kinds).Draw(t, "kind") {
		case 0:
			// the empty value
		case 1:
			s := rapid.String().Draw(t, "string")
			v.StringValue = &s
		case 2:
			b := rapid.Bool().Draw(t, "bool")
			v.BoolValue = &b
		case 3:
			i := capture.Int64(rapid.Int64().Draw(t, "int"))
			v.IntValue = &i
		case 4:
			f := rapid.OneOf(
				rapid.Float64Range(-1e300, 1e300),
				rapid.SampledFrom([]float64{math.Inf(1), math.Inf(-1), 0, math.SmallestNonzeroFloat64, math.MaxFloat64}),
			).Draw(t, "double")
			v.DoubleValue = &f
		case 5:
			if depth == 0 {
				b := append([]byte{}, rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(t, "bytes")...)
				v.BytesValue = &b
				break
			}
			a := &capture.ArrayValue{}
			if n := rapid.IntRange(0, 3).Draw(t, "array len"); n > 0 {
				a.Values = rapid.SliceOfN(genAnyValue(depth-1), n, n).Draw(t, "array")
			}
			v.ArrayValue = a
		case 6:
			l := &capture.KeyValueList{}
			if n := rapid.IntRange(0, 3).Draw(t, "kvlist len"); n > 0 {
				l.Values = rapid.SliceOfN(genKeyValue(depth-1), n, n).Draw(t, "kvlist")
			}
			v.KVListValue = l
		default:
			b := append([]byte{}, rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(t, "bytes")...)
			v.BytesValue = &b
		}
		return v
	})
}

func genKeyValue(depth int) *rapid.Generator[capture.KeyValue] {
	return rapid.Custom(func(t *rapid.T) capture.KeyValue {
		return capture.KeyValue{
			Key:   rapid.String().Draw(t, "key"),
			Value: genAnyValue(depth).Draw(t, "value"),
		}
	})
}

func genAttrs() *rapid.Generator[[]capture.KeyValue] {
	return rapid.Custom(func(t *rapid.T) []capture.KeyValue {
		n := rapid.IntRange(0, 3).Draw(t, "attrs")
		if n == 0 {
			return nil
		}
		return rapid.SliceOfN(genKeyValue(3), n, n).Draw(t, "attrs")
	})
}

func genID(size int) *rapid.Generator[capture.HexBytes] {
	return rapid.Custom(func(t *rapid.T) capture.HexBytes {
		if !rapid.Bool().Draw(t, "has id") {
			return nil
		}
		return rapid.SliceOfN(rapid.Byte(), size, size).Draw(t, "id")
	})
}

// genTime is mostly small, so that ties and zero times happen.
func genTime() *rapid.Generator[capture.Uint64] {
	return rapid.OneOf(
		rapid.Just(capture.Uint64(0)),
		rapid.Map(rapid.Uint64Range(1, 6), func(u uint64) capture.Uint64 { return capture.Uint64(u) }),
		rapid.Map(rapid.Uint64(), func(u uint64) capture.Uint64 { return capture.Uint64(u) }),
	)
}

func genRecord() *rapid.Generator[capture.LogRecord] {
	return rapid.Custom(func(t *rapid.T) capture.LogRecord {
		r := capture.LogRecord{
			TimeUnixNano:           genTime().Draw(t, "time"),
			ObservedTimeUnixNano:   genTime().Draw(t, "observed"),
			SeverityNumber:         rapid.Int32().Draw(t, "severity"),
			SeverityText:           rapid.String().Draw(t, "severity text"),
			Attributes:             genAttrs().Draw(t, "attributes"),
			DroppedAttributesCount: rapid.Uint32().Draw(t, "dropped"),
			Flags:                  rapid.Uint32().Draw(t, "flags"),
			TraceID:                genID(16).Draw(t, "trace id"),
			SpanID:                 genID(8).Draw(t, "span id"),
			EventName:              rapid.String().Draw(t, "event name"),
		}
		if rapid.Bool().Draw(t, "has body") {
			body := genAnyValue(3).Draw(t, "body")
			r.Body = &body
		}
		return r
	})
}

// genEntityRefs is nothing or compact JSON, which is what the encoder gives
// back.
func genEntityRefs() *rapid.Generator[json.RawMessage] {
	return rapid.SampledFrom([]json.RawMessage{
		nil,
		json.RawMessage(`[]`),
		json.RawMessage(`[{"type":"k8s.pod","idKeys":["k8s.pod.uid"]}]`),
		json.RawMessage(`[{"type":"host","idKeys":["host.id"],"descriptionKeys":["host.name"]},{"type":"x<&>"}]`),
	})
}

func genLogs() *rapid.Generator[capture.LogsData] {
	return rapid.Custom(func(t *rapid.T) capture.LogsData {
		var d capture.LogsData
		for range rapid.IntRange(1, 3).Draw(t, "resources") {
			rl := capture.ResourceLogs{
				Resource: capture.Resource{
					Attributes:             genAttrs().Draw(t, "resource attributes"),
					DroppedAttributesCount: rapid.Uint32().Draw(t, "resource dropped"),
					EntityRefs:             genEntityRefs().Draw(t, "entity refs"),
				},
				SchemaURL: rapid.String().Draw(t, "schema url"),
			}
			for range rapid.IntRange(0, 3).Draw(t, "scopes") {
				sl := capture.ScopeLogs{
					Scope: capture.InstrumentationScope{
						Name:                   rapid.String().Draw(t, "scope name"),
						Version:                rapid.String().Draw(t, "scope version"),
						Attributes:             genAttrs().Draw(t, "scope attributes"),
						DroppedAttributesCount: rapid.Uint32().Draw(t, "scope dropped"),
					},
					SchemaURL: rapid.String().Draw(t, "scope schema url"),
				}
				for range rapid.IntRange(0, 3).Draw(t, "records") {
					sl.LogRecords = append(sl.LogRecords, genRecord().Draw(t, "record"))
				}
				rl.ScopeLogs = append(rl.ScopeLogs, sl)
			}
			d.ResourceLogs = append(d.ResourceLogs, rl)
		}
		return d
	})
}

func TestPropertyAnyValueRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		v := genAnyValue(3).Draw(t, "value")
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var back capture.AnyValue
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if !reflect.DeepEqual(v, back) {
			t.Fatalf("round trip of %s changed the value", raw)
		}
		again, err := json.Marshal(back)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatalf("encoding is not stable: %s then %s (%v)", raw, again, err)
		}
	})
}

func TestPropertyLogsRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		d := genLogs().Draw(t, "logs")
		raw, err := capture.EncodeLogs(d)
		if err != nil {
			t.Fatalf("EncodeLogs: %v", err)
		}
		if bytes.ContainsAny(raw, "\n\r") {
			t.Fatalf("encoding has a line break: %q", raw)
		}
		back, err := capture.DecodeLogs(raw)
		if err != nil {
			t.Fatalf("DecodeLogs(%s): %v", raw, err)
		}
		if !reflect.DeepEqual(d, back) {
			t.Fatalf("round trip of %s changed the value", raw)
		}
		again, err := capture.EncodeLogs(back)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatalf("encoding is not stable (%v)", err)
		}
	})
}

func TestPropertyWriteThenRead(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ds := rapid.SliceOfN(genLogs(), 0, 5).Draw(t, "lines")
		var out bytes.Buffer
		w := newWriter(t, &out, capture.Logs)
		var want [][]byte
		for _, d := range ds {
			raw, err := capture.EncodeLogs(d)
			if err != nil {
				t.Fatalf("EncodeLogs: %v", err)
			}
			want = append(want, raw)
			if err := w.WriteLogs(d); err != nil {
				t.Fatalf("WriteLogs: %v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		written := bytes.Clone(out.Bytes())
		lines, err := readAll(capture.NewReader(&out, capture.ReaderOptions{}))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(lines) != len(want) {
			t.Fatalf("read %d lines, wrote %d", len(lines), len(want))
		}
		var rewritten bytes.Buffer
		w2 := newWriter(t, &rewritten, capture.Logs)
		for i, l := range lines {
			if !bytes.Equal(l.Raw, want[i]) || l.Number != i+1 || l.Signal != capture.Logs {
				t.Fatalf("line %d = %+v, want raw %s", i, l, want[i])
			}
			if err := w2.WriteRaw(l.Raw); err != nil {
				t.Fatalf("WriteRaw: %v", err)
			}
		}
		if err := w2.Flush(); err != nil || !bytes.Equal(rewritten.Bytes(), written) {
			t.Fatalf("rewriting the raw lines changed the file (%v)", err)
		}
	})
}

// refKey is the fallback order's key, computed here independently of the
// package: event time, with observed time standing in when it is 0, then
// observed time, then position (the line's index in the slice, and the
// indexes within the line).
func refKey(r capture.LogRef) [6]uint64 {
	tm := r.Time
	if tm == 0 {
		tm = r.Observed
	}
	return [6]uint64{tm, r.Observed, uint64(r.Index), uint64(r.Resource), uint64(r.Scope), uint64(r.Record)}
}

func keyLess(a, b [6]uint64) bool { return slices.Compare(a[:], b[:]) < 0 }

func TestPropertyFallbackOrderIsTotalAndStable(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ds := rapid.SliceOfN(genLogs(), 1, 4).Draw(t, "lines")
		var out bytes.Buffer
		w := newWriter(t, &out, capture.Logs)
		total := 0
		for _, d := range ds {
			for _, rl := range d.ResourceLogs {
				for _, sl := range rl.ScopeLogs {
					total += len(sl.LogRecords)
				}
			}
			if err := w.WriteLogs(d); err != nil {
				t.Fatalf("WriteLogs: %v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		file := bytes.Clone(out.Bytes())
		lines, err := readAll(capture.NewReader(bytes.NewReader(file), capture.ReaderOptions{}))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// The generated records have no sequence attribute, except by
		// chance of a generated key; drop that case.
		for _, d := range ds {
			if hasSeqKey(d) {
				t.Skip("generated a record with the sequence key")
			}
		}
		refs, err := capture.OrderLogs(lines)
		if err != nil {
			t.Fatalf("OrderLogs: %v", err)
		}
		if len(refs) != total {
			t.Fatalf("%d refs for %d records", len(refs), total)
		}
		// Strictly increasing in the key: no two refs tie, so the order is
		// total, and it is the one the reference computes.
		for i := 1; i < len(refs); i++ {
			if !keyLess(refKey(refs[i-1]), refKey(refs[i])) {
				t.Fatalf("refs %d and %d are not in strictly increasing order: %+v, %+v", i-1, i, refs[i-1], refs[i])
			}
		}
		// Stable under re-reading: the same lines in the same order give the
		// same refs.
		again, err := readAll(capture.NewReader(bytes.NewReader(file), capture.ReaderOptions{}))
		if err != nil {
			t.Fatalf("read again: %v", err)
		}
		refs2, err := capture.OrderLogs(again)
		if err != nil || !reflect.DeepEqual(refs, refs2) {
			t.Fatalf("order changed on re-reading (%v)", err)
		}
		// With the lines in another order, the records are the same and the
		// order differs only among records equal in time and observed time.
		slices.Reverse(again)
		refs3, err := capture.OrderLogs(again)
		if err != nil {
			t.Fatalf("OrderLogs of reversed lines: %v", err)
		}
		timeKey := func(r capture.LogRef) [2]uint64 { k := refKey(r); return [2]uint64{k[0], k[1]} }
		type place struct{ line, res, scope, record int }
		places := func(refs []capture.LogRef) []place {
			var out []place
			for _, r := range refs {
				out = append(out, place{r.Line, r.Resource, r.Scope, r.Record})
			}
			slices.SortFunc(out, func(a, b place) int {
				return slices.Compare([]int{a.line, a.res, a.scope, a.record}, []int{b.line, b.res, b.scope, b.record})
			})
			return out
		}
		for i := range refs {
			if timeKey(refs[i]) != timeKey(refs3[i]) {
				t.Fatalf("time order changed with the lines reversed at %d", i)
			}
		}
		if !reflect.DeepEqual(places(refs), places(refs3)) {
			t.Fatalf("reversing the lines changed which records there are")
		}
	})
}

func hasSeqKey(d capture.LogsData) bool {
	for _, rl := range d.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, r := range sl.LogRecords {
				for _, a := range r.Attributes {
					if a.Key == capture.SeqKey {
						return true
					}
				}
			}
		}
	}
	return false
}

func TestPropertyStampedOrderRestoresDocumentOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ds := rapid.SliceOfN(genLogs(), 1, 4).Draw(t, "lines")
		next := counter(rapid.Int64Range(-1000, 1000).Draw(t, "start"), rapid.Int64Range(1, 5).Draw(t, "step"))
		var order [][4]int
		for i := range ds {
			for j, rl := range ds[i].ResourceLogs {
				for k, sl := range rl.ScopeLogs {
					for l := range sl.LogRecords {
						order = append(order, [4]int{i + 1, j, k, l})
					}
				}
			}
			capture.StampLogs(&ds[i], next)
		}
		lines := toLines(t, ds)
		perm := rapid.Permutation(lines).Draw(t, "arrival order")
		refs, err := capture.OrderLogs(perm)
		if err != nil {
			t.Fatalf("OrderLogs: %v", err)
		}
		if got := positions(refs); !reflect.DeepEqual(got, order) && (len(got) != 0 || len(order) != 0) {
			t.Fatalf("order = %v, want %v", got, order)
		}
	})
}
