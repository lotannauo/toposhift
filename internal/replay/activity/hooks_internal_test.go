package activity

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// Test-only hooks. They let the tests build files that the public writer
// refuses to write: records out of order or not valid, rows whose text is not
// what the schema allows, a footer that disagrees with the rows. The
// exported names exist only in the test build of the package.

// RecordCost is the decoded size every record counts for in a row group.
const RecordCost = recordCost

// Unchecked makes w skip the checks of Write. The content digest is still
// computed over what is written.
func Unchecked(w *Writer) { w.unchecked = true }

// Tamper makes w pass the footer metadata to f before it is written.
func Tamper(w *Writer, f func(kv map[string]string)) { w.tamper = f }

// RawRow is one row in the forms of the columns, written as given.
type RawRow struct {
	Seq         uint64
	EventNS     int64
	Layer       string
	SubjectKind string
	Source      string
	Target      *string // nil is null
	Relation    *string // nil is null
	Producer    string
	Kind        string
	TTLNS       int64
	ThroughNS   *int64 // nil is null
	Payload     []byte
	Boot        *string // nil is null
	Basis       int32   // the number of the event time basis; 0 is unknown
}

// WriteRaw buffers a row exactly as given, with none of the checks of Write.
func WriteRaw(w *Writer, r RawRow) error {
	if w.err != nil {
		return w.err
	}
	row := row{canonRow: canonRow{
		seq:         r.Seq,
		eventNS:     r.EventNS,
		layer:       r.Layer,
		subjectKind: r.SubjectKind,
		source:      r.Source,
		producer:    r.Producer,
		kind:        r.Kind,
		ttlNS:       r.TTLNS,
		payload:     r.Payload,
		basis:       r.Basis,
	}}
	if r.Target != nil {
		row.hasTarget, row.target = true, *r.Target
	}
	if r.Relation != nil {
		row.hasRelation, row.relation = true, *r.Relation
	}
	if r.Boot != nil {
		row.hasBoot, row.boot = true, *r.Boot
	}
	if r.ThroughNS != nil {
		row.hasThrough, row.throughNS = true, *r.ThroughNS
	}
	return w.add(&row)
}

// fakePages serves a fixed sequence of pages to checkedPages.
type fakePages struct {
	file.PageReader
	pages []file.Page
	at    int
}

func (f *fakePages) Next() bool               { f.at++; return f.at <= len(f.pages) }
func (f *fakePages) Page() file.Page          { return f.pages[f.at-1] }
func (f *fakePages) Err() error               { return nil }
func (f *fakePages) Close() error             { return nil }
func (f *fakePages) SetMaxPageHeaderSize(int) {}

// TestCheckedPages tests the check on dictionary pages on pages made up for the
// purpose: a file in which a dictionary page comes after another page cannot
// be written by the library.
func TestCheckedPages(t *testing.T) {
	t.Parallel()

	dict := func(values int32, size int) file.Page {
		return file.NewDictionaryPage(memory.NewBufferBytes(make([]byte, size)), values, parquet.Encodings.Plain)
	}
	data := func() file.Page {
		return file.NewDataPageV1(memory.NewBufferBytes(make([]byte, 8)), 1, parquet.Encodings.RLEDict, parquet.Encodings.RLE, parquet.Encodings.RLE, 8)
	}
	for _, tc := range []struct {
		name  string
		pages []file.Page
		ok    bool
	}{
		{"a dictionary and a data page", []file.Page{dict(10, 40), data()}, true},
		{"a data page only", []file.Page{data()}, true},
		{"a dictionary of exactly the values it holds", []file.Page{dict(10, 40)}, true},
		{"a dictionary that claims one value too many", []file.Page{dict(11, 40)}, false},
		{"a dictionary that claims a billion values", []file.Page{dict(1<<30, 40)}, false},
		{"a dictionary that claims a negative number of values", []file.Page{dict(-1, 40)}, false},
		{"a dictionary after a data page", []file.Page{data(), dict(10, 40)}, false},
		{"two dictionaries", []file.Page{dict(10, 40), dict(10, 40)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &checkedPages{PageReader: &fakePages{pages: tc.pages}, group: 0, column: "payload", minValue: 4}
			for p.Next() {
			}
			if err := p.Err(); (err == nil) != tc.ok {
				t.Errorf("Err() = %v, want an error: %v", err, !tc.ok)
			}
		})
	}
}

// TestMinEncodedValue pins the smallest size of one dictionary value per type.
func TestMinEncodedValue(t *testing.T) {
	t.Parallel()

	for typ, want := range map[parquet.Type]int{parquet.Types.ByteArray: 4, parquet.Types.Int64: 8} {
		if got := minEncodedValue(typ); got != want {
			t.Errorf("minEncodedValue(%s) = %d, want %d", typ, got, want)
		}
	}
}
