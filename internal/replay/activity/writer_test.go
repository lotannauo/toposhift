package activity_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestWriteRefusals(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	node := fingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	host := fingerprint(t, catalog.Host, catalog.HostID, "h")
	first := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: epoch, Seq: 10, Kind: lifecycle.Observe,
	}
	second := first
	second.Seq = 20
	mutate := func(f func(*store.Record)) store.Record { r := first; r.Seq = 15; f(&r); return r }

	for _, tc := range []struct {
		name string
		rec  store.Record
	}{
		{"empty producer", mutate(func(r *store.Record) { r.Producer = "" })},
		{"entity with a target", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Subject.B = node })},
		{"edge without a relation", mutate(func(r *store.Record) { r.Subject.Relation = "" })},
		{"event time before 1970", mutate(func(r *store.Record) { r.EventTime = time.Unix(-1, 0) })},
		{"entity in another layer than its type's", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Layer = catalog.L1 })},
		{"event time basis 5", mutate(func(r *store.Record) { r.EventTimeBasis = 5 })},
		{"event time basis 255", mutate(func(r *store.Record) { r.EventTimeBasis = 255 })},
		{"negative TTL", mutate(func(r *store.Record) { r.TTL = -time.Second })},
		{"delete with a payload", mutate(func(r *store.Record) { r.Kind, r.Payload = lifecycle.Delete, []byte("x") })},
		{"through before the event time", mutate(func(r *store.Record) { r.Through = r.EventTime.Add(-time.Second) })},
		{"deadline beyond the representable range", mutate(func(r *store.Record) { r.EventTime, r.TTL = store.MaxEventTime, time.Second })},
		{"boot on an edge", mutate(func(r *store.Record) { r.Boot = "b1" })},
		{"boot on a delete", mutate(func(r *store.Record) {
			r.Subject = store.EntitySubject(host)
			r.Layer = catalog.L1
			r.Kind, r.Boot = lifecycle.Delete, "b1"
		})},
		{"boot on a pod", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Boot = "b1" })},
		{"boot on a node", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(node); r.Boot = "b1" })},
		{"blank boot", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(host); r.Layer = catalog.L1; r.Boot = "  \t" })},
		{"boot that is not UTF-8", mutate(func(r *store.Record) { r.Subject = store.EntitySubject(host); r.Layer = catalog.L1; r.Boot = "b\xff" })},
		{"boot of 257 bytes", mutate(func(r *store.Record) {
			r.Subject = store.EntitySubject(host)
			r.Layer = catalog.L1
			r.Boot = strings.Repeat("b", store.MaxBootLen+1)
		})},
		{"producer over MaxText", mutate(func(r *store.Record) { r.Producer = lifecycle.Producer(strings.Repeat("p", activity.MaxText+1)) })},
		{"relation over MaxText", mutate(func(r *store.Record) {
			r.Subject.Relation = catalog.RelationType(strings.Repeat("r", activity.MaxText+1))
		})},
		{"producer that is not UTF-8", mutate(func(r *store.Record) { r.Producer = "bad\xff" })},
		{"seq equal to the previous", mutate(func(r *store.Record) { r.Seq = first.Seq })},
		{"seq below the previous", mutate(func(r *store.Record) { r.Seq = first.Seq - 1 })},
		{"seq zero", mutate(func(r *store.Record) { r.Seq = 0 })},
		{"payload over the limit", mutate(func(r *store.Record) { r.Payload = make([]byte, activity.MaxPayload+1) })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			w, err := activity.NewWriter(&buf, activity.WriterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Write(first); err != nil {
				t.Fatal(err)
			}
			if err := w.Write(tc.rec); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("Write(%s) = %v, want an error wrapping store.ErrInvalid", tc.name, err)
			}
			// The writer is still usable, and the refused record left no trace.
			if err := w.Write(second); err != nil {
				t.Fatalf("Write of a valid record after a refusal: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			requireSame(t, []store.Record{first, second}, readFile(t, buf.Bytes(), activity.ReaderOptions{}))
		})
	}

	t.Run("seq zero as the first record", func(t *testing.T) {
		t.Parallel()
		w, err := activity.NewWriter(io.Discard, activity.WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		zero := first
		zero.Seq = 0
		if err := w.Write(zero); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Write(seq 0) = %v, want an error wrapping store.ErrInvalid", err)
		}
	})
}

func TestWriterOptionsRefused(t *testing.T) {
	t.Parallel()

	over := activity.WriterOptions{RowGroupBytes: activity.MaxWriterRowGroupBytes + 1}
	if _, err := activity.NewWriter(io.Discard, activity.WriterOptions{RowGroupBytes: activity.MaxWriterRowGroupBytes}); err != nil {
		t.Errorf("NewWriter at the largest RowGroupBytes = %v, want it accepted", err)
	}
	for _, opts := range []activity.WriterOptions{{RowGroupRows: -1}, {RowGroupBytes: -1}, over} {
		if _, err := activity.NewWriter(io.Discard, opts); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("NewWriter(%+v) = %v, want an error wrapping store.ErrInvalid", opts, err)
		}
	}
}

func TestWriterAfterClose(t *testing.T) {
	t.Parallel()

	w, err := activity.NewWriter(io.Discard, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recs := sample(t, 2)
	if err := w.Write(recs[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(recs[1]); !errors.Is(err, activity.ErrClosed) {
		t.Errorf("Write after Close = %v, want an error wrapping ErrClosed", err)
	}
	if err := w.Close(); !errors.Is(err, activity.ErrClosed) {
		t.Errorf("second Close = %v, want an error wrapping ErrClosed", err)
	}
}

// The writer does not close the writer it was given.
type closeRecorder struct {
	bytes.Buffer
	closed bool
}

func (c *closeRecorder) Close() error { c.closed = true; return nil }

func TestCloseLeavesTheSinkOpen(t *testing.T) {
	t.Parallel()

	var sink closeRecorder
	w, err := activity.NewWriter(&sink, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.closed {
		t.Error("Close closed the underlying writer")
	}
}

// failingWriter fails once limit bytes have been written; the write that
// crosses the limit is partial.
type failingWriter struct {
	limit, written int
	err            error
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.written+len(p) <= f.limit {
		f.written += len(p)
		return len(p), nil
	}
	n := max(f.limit-f.written, 0)
	f.written += n
	return n, f.err
}

func TestWriterSinkFailure(t *testing.T) {
	t.Parallel()

	recs := sample(t, 40)
	opts := activity.WriterOptions{RowGroupRows: 5}
	size := len(writeFile(t, recs, opts))
	sinkErr := errors.New("disk full")

	limits := []int{0, 1, 3, 4, 5, 8, 100, size / 4, size / 2, size - 100, size - 9, size - 1}
	for _, limit := range limits {
		t.Run(fmt.Sprintf("fails after %d bytes", limit), func(t *testing.T) {
			t.Parallel()
			var first error
			note := func(where string, err error) {
				t.Helper()
				if err == nil {
					return
				}
				if !errors.Is(err, sinkErr) {
					t.Errorf("%s returned %v, which is not the sink's error", where, err)
				}
				if first == nil {
					first = err
				} else if !errors.Is(err, first) {
					t.Errorf("%s returned %v, want the first error %v again", where, err, first)
				}
			}

			w, err := activity.NewWriter(&failingWriter{limit: limit, err: sinkErr}, opts)
			note("NewWriter", err)
			if err != nil {
				if limit >= 4 {
					t.Fatalf("NewWriter failed at limit %d, after the magic bytes: %v", limit, err)
				}
				return
			}
			for _, r := range recs {
				note("Write", w.Write(r))
				if first != nil {
					break
				}
			}
			note("Close", w.Close())
			if first == nil {
				t.Fatalf("a sink that fails after %d bytes of a %d byte file caused no error", limit, size)
			}
			// Everything after the failure returns the first error.
			note("Write after the failure", w.Write(recs[0]))
			note("Close after the failure", w.Close())
		})
	}
}

func TestEmptyFile(t *testing.T) {
	t.Parallel()

	data := writeFile(t, nil, activity.WriterOptions{})
	r, err := openFile(data, activity.ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := r.Info(), (activity.Info{Version: 1}); got != want {
		t.Errorf("Info() = %+v, want %+v", got, want)
	}
	for range 2 {
		if rec, err := r.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("Next = %+v, %v, want io.EOF", rec, err)
		}
	}

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	kv := footerMetadata(pf)
	// The digest of nothing is the SHA-256 of the empty input.
	want := map[string]string{
		"toposhift.activity.version":       "1",
		"toposhift.activity.records":       "0",
		"toposhift.activity.group_digests": "",
		"toposhift.activity.digest":        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
	for k, v := range want {
		if kv[k] != v {
			t.Errorf("footer metadata %s = %q, want %q", k, kv[k], v)
		}
	}
	for _, k := range []string{"toposhift.activity.min_seq", "toposhift.activity.max_seq"} {
		if v, ok := kv[k]; ok {
			t.Errorf("footer metadata %s = %q in an empty file, want it absent", k, v)
		}
	}
	if pf.NumRowGroups() != 0 {
		t.Errorf("an empty file has %d row groups, want 0", pf.NumRowGroups())
	}
}

func footerMetadata(pf *file.Reader) map[string]string {
	kv := map[string]string{}
	for _, e := range pf.MetaData().KeyValueMetadata() {
		if e.Value != nil {
			kv[e.Key] = *e.Value
		}
	}
	return kv
}

func TestFileProperties(t *testing.T) {
	t.Parallel()

	recs := sample(t, 20)
	recs[0].Seq = 1
	recs[19].Seq = 1<<63 + 5
	data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: 20})
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	kv := footerMetadata(pf)
	if kv["toposhift.activity.version"] != "1" || kv["toposhift.activity.records"] != "20" ||
		kv["toposhift.activity.min_seq"] != "1" || kv["toposhift.activity.max_seq"] != "9223372036854775813" {
		t.Errorf("footer metadata = %v", kv)
	}
	if d := kv["toposhift.activity.digest"]; len(d) != len("sha256:")+64 || !strings.HasPrefix(d, "sha256:") || d != strings.ToLower(d) {
		t.Errorf("digest = %q, want sha256: and 64 lowercase hex digits", d)
	}

	if pf.NumRowGroups() != 1 {
		t.Fatalf("%d row groups, want 1", pf.NumRowGroups())
	}
	rg := pf.MetaData().RowGroup(0)
	if sc := rg.SortingColumns(); len(sc) != 1 || sc[0].ColumnIdx != 0 || sc[0].Descending {
		t.Errorf("sorting columns = %+v, want ascending by column 0 (seq)", sc)
	}
	for i := range rg.NumColumns() {
		cc, err := rg.ColumnChunk(i)
		if err != nil {
			t.Fatal(err)
		}
		if cc.Compression() != compress.Codecs.Zstd {
			t.Errorf("column %d is compressed with %v, want zstd", i, cc.Compression())
		}
	}

	// The sequence number column reports unsigned statistics.
	cc, err := rg.ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := cc.Statistics()
	if err != nil {
		t.Fatal(err)
	}
	i64, ok := stats.(*metadata.Int64Statistics)
	if !ok {
		t.Fatalf("seq statistics are %T", stats)
	}
	if got := uint64(i64.Max()); got != 1<<63+5 {
		t.Errorf("seq maximum in the statistics = %d, want %d", got, uint64(1<<63+5))
	}
	if got := i64.Min(); got != 1 {
		t.Errorf("seq minimum in the statistics = %d, want 1", got)
	}
	if pf.MetaData().Schema.Column(0).LogicalType().String() != "Int(bitWidth=64, isSigned=false)" {
		t.Errorf("seq has logical type %v", pf.MetaData().Schema.Column(0).LogicalType())
	}

	// The payload column carries neither a dictionary nor statistics.
	pc, err := rg.ColumnChunk(11)
	if err != nil {
		t.Fatal(err)
	}
	if pc.HasDictionaryPage() {
		t.Error("the payload column is dictionary encoded")
	}
	if set, _ := pc.StatsSet(); set {
		t.Error("the payload column has statistics")
	}
}

func TestWriteIsDeterministic(t *testing.T) {
	t.Parallel()

	recs := sample(t, 100)
	opts := activity.WriterOptions{RowGroupRows: 13}
	a, b := writeFile(t, recs, opts), writeFile(t, recs, opts)
	if !bytes.Equal(a, b) {
		t.Errorf("the same records and options gave files of %d and %d bytes that differ", len(a), len(b))
	}
}

// The writer keeps a copy of the payload: the caller may reuse its slice.
func TestWriterCopiesPayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recs := sample(t, 2)
	scratch := []byte("first payload")
	recs[0].Payload = scratch
	if err := w.Write(recs[0]); err != nil {
		t.Fatal(err)
	}
	copy(scratch, "XXXXXXXXXXXXX")
	if err := w.Write(recs[1]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := readFile(t, buf.Bytes(), activity.ReaderOptions{})
	if string(got[0].Payload) != "first payload" {
		t.Errorf("payload read back as %q, want %q", got[0].Payload, "first payload")
	}
}

func TestLargestPayload(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	payload := make([]byte, activity.MaxPayload)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	recs := []store.Record{
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "p", EventTime: epoch, Seq: 1, Kind: lifecycle.Observe, Payload: payload},
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "p", EventTime: epoch, Seq: 2, Kind: lifecycle.Observe},
	}
	got := readFile(t, writeFile(t, recs, activity.WriterOptions{}), activity.ReaderOptions{})
	requireSame(t, recs, got)
}

func TestMaxRecordFields(t *testing.T) {
	t.Parallel()

	// Times at both ends of the range and the longest TTL that fits.
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	recs := []store.Record{
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "p", EventTime: store.MinEventTime, Seq: 1, Kind: lifecycle.Observe, TTL: time.Duration(math.MaxInt64)},
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "p", EventTime: store.MaxEventTime, Seq: 2, Kind: lifecycle.Observe, Through: store.MaxEventTime},
	}
	for i, r := range recs {
		if err := r.Validate(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	requireSame(t, recs, readFile(t, writeFile(t, recs, activity.WriterOptions{}), activity.ReaderOptions{}))
}

// The longest producer, relation and boot id are accepted and read back.
func TestTextAtTheLimit(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	node := fingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	host := fingerprint(t, catalog.Host, catalog.HostID, "h")
	recs := []store.Record{
		{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.RelationType(strings.Repeat("r", activity.MaxText))),
			Producer: lifecycle.Producer(strings.Repeat("p", activity.MaxText)), EventTime: epoch, Seq: 1, Kind: lifecycle.Observe,
		},
		{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "cloud", EventTime: epoch, Seq: 2, Kind: lifecycle.Observe,
			Boot: strings.Repeat("b", store.MaxBootLen),
		},
	}
	requireSame(t, recs, readFile(t, writeFile(t, recs, activity.WriterOptions{}), activity.ReaderOptions{}))
}
