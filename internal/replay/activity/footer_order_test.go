package activity_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// fallbackRows is how many host observations, each with a boot id of
// fallbackBootLen distinct bytes, fill a row group past the Parquet library's
// dictionary page limit of 1 MiB. The boot column then falls back from
// dictionary encoding to plain encoding in the middle of a column chunk, so
// that the chunk's data pages use two encodings.
const (
	fallbackRows    = 6000
	fallbackBootLen = 250
)

// fallbackRecords returns n host observations with distinct boot ids that
// fall back to plain encoding in groups of fallbackRows.
func fallbackRecords(tb fataler, n int) []store.Record {
	tb.Helper()
	host := fingerprint(tb, catalog.Host, catalog.HostID, "host-1")
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "collector-a",
			EventTime: epoch, Seq: uint64(i + 1), Kind: lifecycle.Observe,
			Boot: fmt.Sprintf("%0*d", fallbackBootLen, i),
		}
		if err := recs[i].Validate(); err != nil {
			tb.Fatalf("record %d is not valid: %v", i, err)
		}
	}
	return recs
}

// dataEncodings counts the encodings the data pages of a column chunk use, as
// its footer entry says.
func dataEncodings(cc *metadata.ColumnChunkMetaData) int {
	n := 0
	for _, s := range cc.EncodingStats() {
		if s.PageType == file.PageTypeDataPage {
			n++
		}
	}
	return n
}

// The footer of a column chunk lists, in encoding_stats, how many pages of each
// type and encoding it has. The Parquet library builds that list from a Go map,
// so a chunk that has pages of two encodings (a dictionary column that falls
// back to plain encoding) used to get its two entries in a different order from
// one write to the next, and two files of the same records differed in the
// footer. The writer sorts the list before the footer is written.
func TestFooterIsWrittenInOneOrder(t *testing.T) {
	t.Parallel()

	recs := fallbackRecords(t, 2*fallbackRows)
	opts := activity.WriterOptions{RowGroupRows: fallbackRows}

	first := writeFile(t, recs, opts)

	// The fixture must do what its comment says, or the test proves nothing:
	// some column chunk has data pages of two encodings.
	pf, err := file2reader(first)
	if err != nil {
		t.Fatal(err)
	}
	if pf.NumRowGroups() != 2 {
		t.Fatalf("%d row groups, want 2", pf.NumRowGroups())
	}
	fallbacks := 0
	for g := range pf.NumRowGroups() {
		rg := pf.MetaData().RowGroup(g)
		for c := range rg.NumColumns() {
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			if dataEncodings(cc) > 1 {
				fallbacks++
			}
			assertSortedStats(t, g, c, cc)
		}
	}
	if fallbacks == 0 {
		t.Fatal("no column chunk of the fixture has data pages of two encodings; the fixture no longer exercises the footer's encoding list")
	}

	// Go draws a new map iteration order for every range, so writes in one
	// process are enough to see the order vary: a chunk with two entries gets
	// either order with equal chance, and 24 writes agree by chance with
	// probability 2^-23 for one such chunk.
	for i := range 24 {
		again := writeFile(t, recs, opts)
		if !bytes.Equal(first, again) {
			t.Fatalf("write %d of the same records and options gave a file of %d bytes that differs from the first, of %d", i+2, len(again), len(first))
		}
	}

	// Every file the writer wrote before still reads, and so does this one.
	got := readFile(t, first, activity.ReaderOptions{})
	requireSame(t, recs, got)
}

// assertSortedStats fails the test unless the footer entry of the chunk lists
// its encoding_stats by page type, then encoding.
func assertSortedStats(t *testing.T, group, column int, cc *metadata.ColumnChunkMetaData) {
	t.Helper()
	stats := cc.EncodingStats()
	for i := 1; i < len(stats); i++ {
		a, b := stats[i-1], stats[i]
		dictA, dictB := a.PageType == file.PageTypeDictionaryPage, b.PageType == file.PageTypeDictionaryPage
		if (dictB && !dictA) || (dictA == dictB && a.Encoding >= b.Encoding) {
			t.Errorf("row group %d, column %d: encoding_stats entry %d (%v, %v) is not before entry %d (%v, %v)",
				group, column, i-1, a.PageType, a.Encoding, i, b.PageType, b.Encoding)
		}
	}
}
