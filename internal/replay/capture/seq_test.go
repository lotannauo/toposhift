package capture_test

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// shape describes a capture to build: per line, per resource, per scope, the
// records' event and observed times.
type timing struct{ time, observed uint64 }

func buildLogs(shape [][][][]timing) []capture.LogsData {
	var out []capture.LogsData
	for _, resources := range shape {
		var d capture.LogsData
		for _, scopes := range resources {
			var rl capture.ResourceLogs
			for _, records := range scopes {
				var sl capture.ScopeLogs
				for _, tm := range records {
					sl.LogRecords = append(sl.LogRecords, capture.LogRecord{
						TimeUnixNano:         capture.Uint64(tm.time),
						ObservedTimeUnixNano: capture.Uint64(tm.observed),
						Attributes:           []capture.KeyValue{{Key: "other", Value: str("x")}},
					})
				}
				rl.ScopeLogs = append(rl.ScopeLogs, sl)
			}
			d.ResourceLogs = append(d.ResourceLogs, rl)
		}
		out = append(out, d)
	}
	return out
}

// fataler is what both *testing.T and *rapid.T offer.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// toLines encodes each value as a line, numbered from 1.
func toLines(t fataler, ds []capture.LogsData) []capture.Line {
	t.Helper()
	lines := make([]capture.Line, len(ds))
	for i, d := range ds {
		raw, err := capture.EncodeLogs(d)
		if err != nil {
			t.Fatalf("EncodeLogs: %v", err)
		}
		lines[i] = capture.Line{Number: i + 1, Signal: capture.Logs, Raw: raw}
	}
	return lines
}

func positions(refs []capture.LogRef) [][4]int {
	out := make([][4]int, len(refs))
	for i, r := range refs {
		out[i] = [4]int{r.Line, r.Resource, r.Scope, r.Record}
	}
	return out
}

// twoLines is a capture of two lines: line 1 has two resources with 2 and 1
// scopes, line 2 one resource. Its document order is the order of positions.
func twoLines() []capture.LogsData {
	return buildLogs([][][][]timing{
		{{{{}, {}}, {{}}}, {{{}}}},
		{{{{}, {}, {}}}},
	})
}

var twoLinesOrder = [][4]int{
	{1, 0, 0, 0},
	{1, 0, 0, 1},
	{1, 0, 1, 0},
	{1, 1, 0, 0},
	{2, 0, 0, 0},
	{2, 0, 0, 1},
	{2, 0, 0, 2},
}

func counter(start, step int64) func() int64 {
	n := start - step
	return func() int64 { n += step; return n }
}

func TestStampLogsSetsSeqOnEveryRecordInOrder(t *testing.T) {
	t.Parallel()
	ds := twoLines()
	next := counter(100, 1)
	for i := range ds {
		capture.StampLogs(&ds[i], next)
	}
	var got []int64
	for _, d := range ds {
		for _, rl := range d.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, r := range sl.LogRecords {
					n := 0
					for _, a := range r.Attributes {
						if a.Key == capture.SeqKey {
							n++
							if a.Value.IntValue == nil {
								t.Fatalf("seq is not an intValue: %+v", a.Value)
							}
							got = append(got, int64(*a.Value.IntValue))
						}
					}
					if n != 1 {
						t.Errorf("record has %d seq attributes", n)
					}
					if len(r.Attributes) != 2 || r.Attributes[0].Key != "other" {
						t.Errorf("attributes = %+v, want the existing one kept first and seq appended", r.Attributes)
					}
				}
			}
		}
	}
	if want := []int64{100, 101, 102, 103, 104, 105, 106}; !slices.Equal(got, want) {
		t.Errorf("seq values = %v, want %v", got, want)
	}
}

func TestStampLogsReplacesExisting(t *testing.T) {
	t.Parallel()
	old, older := capture.Int64(1), capture.Int64(2)
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{LogRecords: []capture.LogRecord{{
		Attributes: []capture.KeyValue{
			{Key: "a", Value: str("1")},
			{Key: capture.SeqKey, Value: capture.AnyValue{IntValue: &old}},
			{Key: "b", Value: str("2")},
			{Key: capture.SeqKey, Value: capture.AnyValue{IntValue: &older}},
		},
	}}}}}}}
	capture.StampLogs(&d, counter(50, 1))
	attrs := d.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes
	if len(attrs) != 3 || attrs[0].Key != "a" || attrs[1].Key != capture.SeqKey || attrs[2].Key != "b" {
		t.Fatalf("attributes = %+v, want a, seq, b", attrs)
	}
	if got := attrs[1].Value.IntValue; got == nil || *got != 50 {
		t.Errorf("seq = %v, want 50", got)
	}
	if old != 1 || older != 2 {
		t.Errorf("stamping changed the values it replaced")
	}
}

func TestStampLogsCallsNextOncePerRecord(t *testing.T) {
	t.Parallel()
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{}, {LogRecords: []capture.LogRecord{{}, {}}}}}, {}}}
	calls := 0
	capture.StampLogs(&d, func() int64 { calls++; return 1 })
	if calls != 2 {
		t.Errorf("next called %d times, want 2", calls)
	}
}

func stampAll(ds []capture.LogsData, next func() int64) {
	for i := range ds {
		capture.StampLogs(&ds[i], next)
	}
}

func TestOrderLogsStampedIsDocumentOrder(t *testing.T) {
	t.Parallel()
	ds := twoLines()
	stampAll(ds, counter(1, 1))
	refs, err := capture.OrderLogs(toLines(t, ds))
	if err != nil {
		t.Fatalf("OrderLogs: %v", err)
	}
	if got := positions(refs); !reflect.DeepEqual(got, twoLinesOrder) {
		t.Errorf("order = %v, want %v", got, twoLinesOrder)
	}
	for i, r := range refs {
		if !r.HasSeq || r.Seq != int64(i+1) {
			t.Errorf("ref %d = %+v, want seq %d", i, r, i+1)
		}
	}
}

func TestOrderLogsShuffledLinesRestoreStampedOrder(t *testing.T) {
	t.Parallel()
	ds := twoLines()
	stampAll(ds, counter(1, 1))
	lines := toLines(t, ds)
	for _, perm := range [][]int{{1, 0}, {0, 1}} {
		shuffled := []capture.Line{lines[perm[0]], lines[perm[1]]}
		refs, err := capture.OrderLogs(shuffled)
		if err != nil {
			t.Fatalf("OrderLogs: %v", err)
		}
		if got := positions(refs); !reflect.DeepEqual(got, twoLinesOrder) {
			t.Errorf("order of lines %v = %v, want %v", perm, got, twoLinesOrder)
		}
	}
}

// TestOrderLogsSeqBeatsTime has times that point the other way from the
// sequence: the sequence decides.
func TestOrderLogsSeqBeatsTime(t *testing.T) {
	t.Parallel()
	ds := buildLogs([][][][]timing{
		{{{{time: 10}, {time: 20}}}},
		{{{{time: 30}, {time: 5}}}},
	})
	// Capture order: line 2's records, then line 1's, last record first.
	seqs := map[[4]int]int64{
		{2, 0, 0, 1}: 1, {2, 0, 0, 0}: 2, {1, 0, 0, 1}: 3, {1, 0, 0, 0}: 4,
	}
	for i := range ds {
		k := 0
		capture.StampLogs(&ds[i], func() int64 {
			s := seqs[[4]int{i + 1, 0, 0, k}]
			k++
			return s
		})
	}
	refs, err := capture.OrderLogs(toLines(t, ds))
	if err != nil {
		t.Fatalf("OrderLogs: %v", err)
	}
	want := [][4]int{{2, 0, 0, 1}, {2, 0, 0, 0}, {1, 0, 0, 1}, {1, 0, 0, 0}}
	if got := positions(refs); !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

func TestOrderLogsSeqErrors(t *testing.T) {
	t.Parallel()
	one, two := capture.Int64(1), capture.Int64(2)
	seqAttr := func(v *capture.Int64) capture.KeyValue {
		return capture.KeyValue{Key: capture.SeqKey, Value: capture.AnyValue{IntValue: v}}
	}
	line := func(records ...capture.LogRecord) capture.LogsData {
		return capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{LogRecords: records}}}}}
	}
	withSeq := func(v *capture.Int64) capture.LogRecord {
		return capture.LogRecord{Attributes: []capture.KeyValue{seqAttr(v)}}
	}
	s := "1"
	tests := []struct {
		name     string
		ds       []capture.LogsData
		contains string
	}{
		{"duplicate within a line", []capture.LogsData{line(withSeq(&one), withSeq(&one))}, "repeats"},
		{"duplicate across lines", []capture.LogsData{line(withSeq(&one)), line(withSeq(&two), withSeq(&one))}, "repeats"},
		{"mixed in one line", []capture.LogsData{line(withSeq(&one), capture.LogRecord{})}, "stamp every record or none"},
		{"mixed across lines", []capture.LogsData{line(withSeq(&one)), line(capture.LogRecord{})}, "stamp every record or none"},
		{"not an intValue", []capture.LogsData{line(capture.LogRecord{Attributes: []capture.KeyValue{{Key: capture.SeqKey, Value: str(s)}}})}, "intValue"},
		{"two on a record", []capture.LogsData{line(capture.LogRecord{Attributes: []capture.KeyValue{seqAttr(&one), seqAttr(&two)}})}, "more than one"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs, err := capture.OrderLogs(toLines(t, tc.ds))
			if err == nil {
				t.Fatalf("OrderLogs = %v, want an error", refs)
			}
			if !errors.Is(err, capture.ErrFormat) || !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error = %q, want one wrapping ErrFormat that mentions %q", err, tc.contains)
			}
		})
	}
}

func TestOrderLogsRefusesOtherLines(t *testing.T) {
	t.Parallel()
	lines := []capture.Line{{Number: 4, Signal: capture.Metrics, Raw: []byte(metricsLine)}}
	_, err := capture.OrderLogs(lines)
	if err == nil || !errors.Is(err, capture.ErrFormat) || !strings.HasPrefix(err.Error(), "line 4: ") {
		t.Errorf("OrderLogs on a metrics line: %v", err)
	}
}

func TestOrderLogsEmpty(t *testing.T) {
	t.Parallel()
	for _, lines := range [][]capture.Line{nil, toLines(t, []capture.LogsData{{}, {ResourceLogs: []capture.ResourceLogs{{}}}})} {
		refs, err := capture.OrderLogs(lines)
		if err != nil || len(refs) != 0 {
			t.Errorf("OrderLogs = %v, %v; want nothing", refs, err)
		}
	}
}

func TestOrderLogsFallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		shape [][][][]timing
		want  [][4]int
	}{
		{
			"by time",
			[][][][]timing{{{{{time: 30}, {time: 10}, {time: 20}}}}},
			[][4]int{{1, 0, 0, 1}, {1, 0, 0, 2}, {1, 0, 0, 0}},
		},
		{
			"time zero falls back to observed time",
			[][][][]timing{{{{{time: 20, observed: 1}, {observed: 10}, {observed: 30}, {time: 25}}}}},
			[][4]int{{1, 0, 0, 1}, {1, 0, 0, 0}, {1, 0, 0, 3}, {1, 0, 0, 2}},
		},
		{
			"observed time breaks a tie in time",
			[][][][]timing{{{{{time: 5, observed: 9}, {time: 5, observed: 3}}}}},
			[][4]int{{1, 0, 0, 1}, {1, 0, 0, 0}},
		},
		{
			"a record with time zero and observed zero comes first",
			[][][][]timing{{{{{time: 1}, {}}}}},
			[][4]int{{1, 0, 0, 1}, {1, 0, 0, 0}},
		},
		{
			"a record with time zero ties with an equal explicit time, then observed decides",
			[][][][]timing{{{{{time: 7, observed: 8}, {observed: 7}}}}},
			[][4]int{{1, 0, 0, 1}, {1, 0, 0, 0}},
		},
		{
			"ties go by line, resource, scope, record",
			[][][][]timing{
				{{{{time: 1}}, {{time: 1}}}, {{{time: 1}}}},
				{{{{time: 1}}}},
			},
			[][4]int{{1, 0, 0, 0}, {1, 0, 1, 0}, {1, 1, 0, 0}, {2, 0, 0, 0}},
		},
		{
			"earlier line later in time",
			[][][][]timing{{{{{time: 9}}}}, {{{{time: 2}}}}},
			[][4]int{{2, 0, 0, 0}, {1, 0, 0, 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs, err := capture.OrderLogs(toLines(t, buildLogs(tc.shape)))
			if err != nil {
				t.Fatalf("OrderLogs: %v", err)
			}
			if got := positions(refs); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("order = %v, want %v", got, tc.want)
			}
			for _, r := range refs {
				if r.HasSeq {
					t.Errorf("ref %+v has a sequence", r)
				}
			}
		})
	}
}

// TestOrderLogsFallbackIsIndependentOfLineOrder shuffles the lines of an
// unstamped capture whose records differ in time: the order does not change,
// because line numbers travel with the lines and position is only the last
// tie-break.
func TestOrderLogsFallbackIsIndependentOfLineOrder(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	shape := [][][][]timing{
		{{{{time: 3}, {time: 1}}, {{observed: 2}}}},
		{{{{time: 4}}}, {{{time: 3, observed: 1}}}},
		{{{{time: 2}, {time: 2}}}},
	}
	lines := toLines(t, buildLogs(shape))
	base, err := capture.OrderLogs(lines)
	if err != nil {
		t.Fatalf("OrderLogs: %v", err)
	}
	for range 20 {
		shuffled := slices.Clone(lines)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		got, err := capture.OrderLogs(shuffled)
		if err != nil || !reflect.DeepEqual(positions(got), positions(base)) {
			t.Fatalf("shuffled order = %v, %v; want %v", positions(got), err, positions(base))
		}
	}
}

func TestOrderLogsIndexIsPositionInSlice(t *testing.T) {
	t.Parallel()
	lines := toLines(t, buildLogs([][][][]timing{
		{{{{time: 3}}}},
		{{{{time: 1}, {time: 2}}}},
		{{{{time: 0, observed: 4}}}},
	}))
	// Give the lines numbers unrelated to their positions.
	lines[0].Number, lines[1].Number, lines[2].Number = 70, 5, 31
	refs, err := capture.OrderLogs(lines)
	if err != nil {
		t.Fatalf("OrderLogs: %v", err)
	}
	type at struct{ index, line, record int }
	var got []at
	for _, r := range refs {
		got = append(got, at{r.Index, r.Line, r.Record})
		if lines[r.Index].Number != r.Line {
			t.Errorf("ref %+v: lines[%d] is line %d", r, r.Index, lines[r.Index].Number)
		}
	}
	want := []at{{1, 5, 0}, {1, 5, 1}, {0, 70, 0}, {2, 31, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("refs = %v, want %v", got, want)
	}
}

// TestOrderLogsRepeatedLineNumbers has two lines with the same number, as the
// files of a rotated capture have: the order is still total and
// deterministic, ties go by the position of the line in the slice, and a ref
// maps back to its line by Index.
func TestOrderLogsRepeatedLineNumbers(t *testing.T) {
	t.Parallel()
	same := [][][][]timing{
		{{{{time: 5, observed: 1}, {time: 5, observed: 1}}}},
		{{{{time: 5, observed: 1}}, {{time: 2}}}},
	}
	lines := toLines(t, buildLogs(same))
	lines[0].Number, lines[1].Number = 1, 1
	check := func(lines []capture.Line) []capture.LogRef {
		t.Helper()
		refs, err := capture.OrderLogs(lines)
		if err != nil {
			t.Fatalf("OrderLogs: %v", err)
		}
		type pos struct{ index, res, scope, record int }
		seen := map[pos]bool{}
		for i, r := range refs {
			p := pos{r.Index, r.Resource, r.Scope, r.Record}
			if seen[p] {
				t.Errorf("position %+v appears twice", p)
			}
			seen[p] = true
			if r.Line != 1 || lines[r.Index].Number != 1 {
				t.Errorf("ref %+v does not map to a line numbered 1", r)
			}
			if i > 0 && keyLess(refKey(r), refKey(refs[i-1])) {
				t.Errorf("refs %d and %d out of order", i-1, i)
			}
		}
		return refs
	}
	refs := check(lines)
	got := positions2(refs)
	// time 2 first; then the three records of time 5, line 0's two before line 1's.
	want := [][4]int{{1, 0, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 1}, {1, 0, 0, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	if again := check(lines); !reflect.DeepEqual(again, refs) {
		t.Errorf("a second ordering differs")
	}
	// Given in the other order, the ties go the other way: that is the only
	// way the input order matters.
	swapped := check([]capture.Line{lines[1], lines[0]})
	want = [][4]int{{0, 0, 1, 0}, {0, 0, 0, 0}, {1, 0, 0, 0}, {1, 0, 0, 1}}
	if got := positions2(swapped); !reflect.DeepEqual(got, want) {
		t.Errorf("swapped order = %v, want %v", got, want)
	}
}

// positions2 is positions by Index rather than line number.
func positions2(refs []capture.LogRef) [][4]int {
	out := make([][4]int, len(refs))
	for i, r := range refs {
		out[i] = [4]int{r.Index, r.Resource, r.Scope, r.Record}
	}
	return out
}

func TestLogRefs(t *testing.T) {
	t.Parallel()
	const raw = `{"resourceLogs":[` +
		`{"scopeLogs":[{"logRecords":[` +
		`{"timeUnixNano":"7","observedTimeUnixNano":9,"body":{"stringValue":"x"},"attributes":[{"key":"a","value":{"kvlistValue":{"values":[{"key":"toposhift.capture.seq","value":{"intValue":"99"}}]}}},{"key":"toposhift.capture.seq","value":{"intValue":"41"}}]},` +
		`{"observedTimeUnixNano":"3"}]},{}]},` +
		`{"scopeLogs":[{"logRecords":[{"attributes":[{"key":"b","value":{"stringValue":"y"}},{"key":"toposhift.capture.seq","value":{"intValue":42}}]}]}]}]}`
	refs, err := capture.LogRefs(capture.Line{Number: 12, Signal: capture.Logs, Raw: []byte(raw)}, 4)
	if err != nil {
		t.Fatalf("LogRefs: %v", err)
	}
	want := []capture.LogRef{
		{Index: 4, Line: 12, Resource: 0, Scope: 0, Record: 0, Seq: 41, HasSeq: true, Time: 7, Observed: 9},
		{Index: 4, Line: 12, Resource: 0, Scope: 0, Record: 1, Observed: 3},
		{Index: 4, Line: 12, Resource: 1, Scope: 0, Record: 0, Seq: 42, HasSeq: true},
	}
	if !reflect.DeepEqual(refs, want) {
		t.Errorf("LogRefs =\n%+v\nwant\n%+v", refs, want)
	}
}

func TestLogRefsErrors(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, raw, contains string }{
		{"metrics", metricsLine, "not logs"},
		{"invalid", `{"resourceLogs":[}`, "invalid JSON"},
		{"not an object", `[]`, "not a JSON object"},
		{"seq not an int", `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"attributes":[{"key":"toposhift.capture.seq","value":{"stringValue":"1"}}]}]}]}]}`, "intValue"},
		{"two seqs", `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"attributes":[{"key":"toposhift.capture.seq","value":{"intValue":"1"}},{"key":"toposhift.capture.seq","value":{"intValue":"2"}}]}]}]}]}`, "more than one"},
		{"seq out of range", `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"attributes":[{"key":"toposhift.capture.seq","value":{"intValue":"9223372036854775808"}}]}]}]}]}`, "invalid integer"},
		{"time not an integer", `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"x"}]}]}]}`, "invalid integer"},
		{"wrong type", `{"resourceLogs":{}}`, "logs:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := capture.LogRefs(capture.Line{Number: 6, Raw: []byte(tc.raw)}, 0)
			if err == nil || !errors.Is(err, capture.ErrFormat) {
				t.Fatalf("LogRefs error = %v, want one wrapping ErrFormat", err)
			}
			if !strings.HasPrefix(err.Error(), "line 6: ") || !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error = %q, want line 6 and %q", err, tc.contains)
			}
		})
	}
}

// TestLogRefsReadsOnlyWhatOrderNeeds pins that a line whose bodies and other
// attributes are not valid typed values still gives refs: DecodeLogs is what
// validates them.
func TestLogRefsReadsOnlyWhatOrderNeeds(t *testing.T) {
	t.Parallel()
	const raw = `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1","severityNumber":"x","traceId":"zz",` +
		`"body":{"stringValue":"a","intValue":"1"},"attributes":[{"key":"k","value":{"doubleValue":"nope"}}]}]}]}]}`
	line := capture.Line{Number: 1, Raw: []byte(raw)}
	refs, err := capture.LogRefs(line, 0)
	if err != nil || len(refs) != 1 || refs[0].Time != 1 {
		t.Errorf("LogRefs = %+v, %v", refs, err)
	}
	if _, err := capture.DecodeLogs(line.Raw); err == nil {
		t.Errorf("DecodeLogs accepted the line")
	}
}

// TestLogRefsMatchOrderLogs checks OrderLogs is LogRefs and SortRefs.
func TestLogRefsMatchOrderLogs(t *testing.T) {
	t.Parallel()
	ds := twoLines()
	lines := toLines(t, ds)
	var refs []capture.LogRef
	for i, l := range lines {
		r, err := capture.LogRefs(l, i)
		if err != nil {
			t.Fatalf("LogRefs: %v", err)
		}
		refs = append(refs, r...)
	}
	if err := capture.SortRefs(refs); err != nil {
		t.Fatalf("SortRefs: %v", err)
	}
	want, err := capture.OrderLogs(lines)
	if err != nil || !reflect.DeepEqual(refs, want) {
		t.Errorf("SortRefs of LogRefs = %v, OrderLogs = %v (%v)", positions(refs), positions(want), err)
	}
}

// TestStreamingOrder streams a capture, keeps only the refs, sorts them, and
// reads the lines again by offset.
func TestStreamingOrder(t *testing.T) {
	t.Parallel()
	ds := buildLogs([][][][]timing{
		{{{{time: 30}, {time: 10}}}},
		{{{{time: 20}}}, {{{time: 5}}}},
		{{{{time: 25}}}},
	})
	var file bytes.Buffer
	w := newWriter(t, &file, capture.Logs)
	for _, d := range ds {
		if err := w.WriteLogs(d); err != nil {
			t.Fatalf("WriteLogs: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data := file.Bytes()

	r := capture.NewReader(bytes.NewReader(data), capture.ReaderOptions{})
	var refs []capture.LogRef
	var offsets []int64
	for {
		line, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		lr, err := capture.LogRefs(line, len(offsets))
		if err != nil {
			t.Fatalf("LogRefs: %v", err)
		}
		offsets = append(offsets, line.Offset)
		refs = append(refs, lr...)
	}
	if err := capture.SortRefs(refs); err != nil {
		t.Fatalf("SortRefs: %v", err)
	}
	var times []uint64
	for _, ref := range refs {
		times = append(times, ref.Time)
		// Read the line again by its offset.
		line, err := capture.NewReader(io.NewSectionReader(bytes.NewReader(data), offsets[ref.Index], int64(len(data))-offsets[ref.Index]), capture.ReaderOptions{}).Next()
		if err != nil {
			t.Fatalf("re-read at %d: %v", offsets[ref.Index], err)
		}
		d, err := capture.DecodeLogs(line.Raw)
		if err != nil {
			t.Fatalf("DecodeLogs: %v", err)
		}
		got := d.ResourceLogs[ref.Resource].ScopeLogs[ref.Scope].LogRecords[ref.Record]
		if uint64(got.TimeUnixNano) != ref.Time {
			t.Errorf("ref %+v re-read as time %d", ref, got.TimeUnixNano)
		}
	}
	if want := []uint64{5, 10, 20, 25, 30}; !slices.Equal(times, want) {
		t.Errorf("times in order = %v, want %v", times, want)
	}
}

func TestSortRefs(t *testing.T) {
	t.Parallel()
	refs := []capture.LogRef{
		{Index: 2, Line: 3, Time: 5},
		{Index: 0, Line: 1, Time: 9},
		{Index: 1, Line: 2, Time: 0, Observed: 7},
		{Index: 1, Line: 2, Record: 1, Time: 5},
	}
	if err := capture.SortRefs(refs); err != nil {
		t.Fatalf("SortRefs: %v", err)
	}
	var got [][2]int
	for _, r := range refs {
		got = append(got, [2]int{r.Index, r.Record})
	}
	if want := [][2]int{{1, 1}, {2, 0}, {1, 0}, {0, 0}}; !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	if err := capture.SortRefs(nil); err != nil {
		t.Errorf("SortRefs(nil) = %v", err)
	}

	stamped := []capture.LogRef{
		{Index: 0, HasSeq: true, Seq: 30, Time: 1},
		{Index: 1, HasSeq: true, Seq: -4, Time: 9},
		{Index: 2, HasSeq: true, Seq: 12},
	}
	if err := capture.SortRefs(stamped); err != nil {
		t.Fatalf("SortRefs stamped: %v", err)
	}
	if stamped[0].Seq != -4 || stamped[1].Seq != 12 || stamped[2].Seq != 30 {
		t.Errorf("stamped order = %+v", stamped)
	}
}

func TestSortRefsErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		refs     []capture.LogRef
		contains []string
	}{
		{
			"repeated seq",
			[]capture.LogRef{{Index: 0, Line: 1, HasSeq: true, Seq: 3}, {Index: 1, Line: 2, HasSeq: true, Seq: 3}},
			[]string{"repeats", "line 2: "},
		},
		{
			"first differing record, none then one",
			[]capture.LogRef{
				{Index: 0, Line: 10},
				{Index: 0, Line: 10, Record: 1},
				{Index: 1, Line: 11, Resource: 2, Scope: 3, Record: 4, HasSeq: true, Seq: 1},
				{Index: 2, Line: 12, HasSeq: true, Seq: 2},
			},
			[]string{"line 11: ", "resource 2, scope 3, record 4 (index 1) has " + capture.SeqKey, "first record (line 10, index 0, resource 0, scope 0, record 0) has none", "stamp every record or none"},
		},
		{
			"first differing record, one then none",
			[]capture.LogRef{
				{Index: 0, Line: 10, HasSeq: true, Seq: 1},
				{Index: 0, Line: 10, Record: 1, HasSeq: true, Seq: 2},
				{Index: 3, Line: 13, Record: 7},
				{Index: 4, Line: 14, HasSeq: true, Seq: 3},
				{Index: 5, Line: 15},
			},
			[]string{"line 13: ", "record 7 (index 3) has no " + capture.SeqKey, "first record (line 10, index 0, resource 0, scope 0, record 0) has one"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := capture.SortRefs(tc.refs)
			if err == nil || !errors.Is(err, capture.ErrFormat) {
				t.Fatalf("SortRefs error = %v, want one wrapping ErrFormat", err)
			}
			for _, want := range tc.contains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
		})
	}
}

// TestOrderLogsMixedNamesFirstDifference checks the error of a capture where
// stamping stops partway names the record where it does.
func TestOrderLogsMixedNamesFirstDifference(t *testing.T) {
	t.Parallel()
	ds := buildLogs([][][][]timing{
		{{{{}, {}}}},
		{{{{}, {}}}},
		{{{{}}}},
	})
	stampAll(ds[:1], counter(1, 1))
	// Line 1 is stamped, lines 2 and 3 are not: the first record that
	// differs is line 2's first.
	_, err := capture.OrderLogs(toLines(t, ds))
	if err == nil {
		t.Fatalf("OrderLogs accepted a capture stamped in part")
	}
	for _, want := range []string{"line 2: ", "(index 1) has no " + capture.SeqKey, "first record (line 1, index 0, resource 0, scope 0, record 0) has one"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}
