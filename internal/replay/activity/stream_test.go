package activity_test

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// countingWriter keeps the bytes it receives.
type countingWriter struct {
	buf bytes.Buffer
}

func (c *countingWriter) Write(p []byte) (int, error) { return c.buf.Write(p) }

func TestRowGroupsAreWrittenAsTheyFill(t *testing.T) {
	t.Parallel()

	const magic = 4
	var sink countingWriter
	w, err := activity.NewWriter(&sink, activity.WriterOptions{RowGroupRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if sink.buf.Len() != magic {
		t.Errorf("after NewWriter the sink holds %d bytes, want only the %d magic bytes", sink.buf.Len(), magic)
	}
	recs := sample(t, 250)
	for i, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
		switch n := i + 1; {
		case n < 100 && sink.buf.Len() != magic:
			t.Fatalf("after %d records the sink holds %d bytes, want only the magic bytes until a row group is full", n, sink.buf.Len())
		case n == 100 && sink.buf.Len() == magic:
			t.Fatalf("after 100 records the sink still holds only the magic bytes, want the first row group written")
		}
	}
	// Two row groups are out; the third waits for Close.
	afterTwo := sink.buf.Len()
	if afterTwo <= magic {
		t.Errorf("after 250 records the sink holds %d bytes, want more than the magic bytes", afterTwo)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.buf.Len() <= afterTwo {
		t.Errorf("Close added nothing to the %d bytes already written", afterTwo)
	}
	r, err := openFile(sink.buf.Bytes(), activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Info().RowGroups; got != 3 {
		t.Errorf("Info().RowGroups = %d, want 3", got)
	}
}

// groupSizes returns the number of rows and the uncompressed size of each row
// group, from the footer.
func groupSizes(tb fataler, data []byte) (rows, bytesIn []int64) {
	tb.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		tb.Fatalf("reading the footer: %v", err)
	}
	for i := range pf.NumRowGroups() {
		rg := pf.MetaData().RowGroup(i)
		rows = append(rows, rg.NumRows())
		bytesIn = append(bytesIn, rg.TotalByteSize())
	}
	return rows, bytesIn
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func TestRowGroupBytesLimit(t *testing.T) {
	t.Parallel()

	const limit = 64 << 10
	rng := rand.New(rand.NewPCG(1, 2))
	recs := sample(t, 300)
	var payloads int
	for i := range recs {
		if recs[i].Kind == lifecycle.Observe {
			recs[i].Payload = randomBytes(rng, 1<<10)
			payloads += len(recs[i].Payload)
		}
	}
	data := writeFile(t, recs, activity.WriterOptions{RowGroupBytes: limit})
	requireSame(t, recs, readFile(t, data, activity.ReaderOptions{}))

	rows, sizes := groupSizes(t, data)
	if want := payloads / limit; len(rows) < want {
		t.Errorf("%d row groups for %d bytes of payload with a limit of %d, want at least %d", len(rows), payloads, limit, want)
	}
	for i, size := range sizes {
		if size > 2*limit {
			t.Errorf("row group %d (%d rows) is %d bytes, want at most twice the limit of %d", i, rows[i], size, limit)
		}
	}
}

func TestRecordLargerThanTheLimitHasItsOwnRowGroup(t *testing.T) {
	t.Parallel()

	recs := sample(t, 7)
	for i := range recs {
		recs[i].Payload, recs[i].Kind, recs[i].Through, recs[i].Boot = nil, lifecycle.Observe, recs[i].EventTime, ""
		recs[i].Subject = store.EntitySubject(recs[0].Subject.A)
		recs[i].Layer = catalog.L2
	}
	recs[3].Payload = make([]byte, 10_000)
	data := writeFile(t, recs, activity.WriterOptions{RowGroupBytes: 2000})
	requireSame(t, recs, readFile(t, data, activity.ReaderOptions{}))

	rows, _ := groupSizes(t, data)
	if want := []int64{3, 1, 3}; !slices.Equal(rows, want) {
		t.Errorf("row groups hold %v rows, want %v: the large record alone in its own", rows, want)
	}
}

// countingReaderAt counts the bytes asked of it.
type countingReaderAt struct {
	data      []byte
	requested int64
	calls     int
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.requested += int64(len(p))
	c.calls++
	return bytes.NewReader(c.data).ReadAt(p, off)
}

func TestReaderStreams(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		records      int
		payload      int
		rowGroupRows int
		batchRows    int
	}{
		{"ten row groups", 2000, 512, 200, 0},
		{"forty row groups, small batches", 4000, 2048, 100, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(3, 4))
			recs := sample(t, tc.records)
			for i := range recs {
				if recs[i].Kind == lifecycle.Observe {
					recs[i].Payload = randomBytes(rng, tc.payload)
				}
			}
			data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: tc.rowGroupRows})
			src := &countingReaderAt{data: data}
			r, err := activity.NewReader(src, int64(len(data)), activity.ReaderOptions{BatchRows: tc.batchRows})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Next(); err != nil {
				t.Fatal(err)
			}
			t.Logf("after NewReader and one Next: %d of %d bytes requested in %d calls", src.requested, len(data), src.calls)
			if limit := int64(len(data)) * 40 / 100; src.requested >= limit {
				t.Errorf("after NewReader and one Next, %d of %d bytes were requested (%d calls), want under 40%%", src.requested, len(data), src.calls)
			}
			got, err := drain(r)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.records-1 {
				t.Errorf("read %d more records, want %d", len(got), tc.records-1)
			}
		})
	}
}

// A boot id counts toward the buffered size of a row group like the payload.
func TestBootCountsTowardTheRowGroupBytes(t *testing.T) {
	t.Parallel()

	host := fingerprint(t, catalog.Host, catalog.HostID, "h")
	var recs []store.Record
	for i := range 20 {
		recs = append(recs, store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "p", EventTime: epoch, Seq: uint64(i + 1), Kind: lifecycle.Observe,
			Boot: strings.Repeat("b", store.MaxBootLen),
		})
	}
	// A record is about 380 bytes with its boot id and about 120 without.
	data := writeFile(t, recs, activity.WriterOptions{RowGroupBytes: 2000})
	requireSame(t, recs, readFile(t, data, activity.ReaderOptions{}))

	rows, _ := groupSizes(t, data)
	for i, n := range rows {
		if n > 6 {
			t.Errorf("row group %d holds %d rows, want at most 6 of about 380 bytes in 2000", i, n)
		}
	}
}
