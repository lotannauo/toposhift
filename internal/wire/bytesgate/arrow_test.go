package bytesgate_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
	"github.com/lotannauo/toposhift/internal/wire/bytesgate"
)

// goldens are the documents of package wire's golden files: between them, every
// section the format has, including the ones the fixtures leave empty.
func goldens(t testing.TB) map[string]wire.Document {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "testdata", "golden", "*.bin"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no golden files: %v", err)
	}
	out := map[string]wire.Document{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		d, err := wire.Decode(b)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[filepath.Base(f)] = d
	}
	return out
}

// streamsOf reads the streams of Arrow bytes, handing each stream's first batch
// to fn with the stream's position.
func streamsOf(t testing.TB, b []byte, fn func(i int, rec arrow.RecordBatch)) int {
	t.Helper()
	r := bytes.NewReader(b)
	n := 0
	for r.Len() > 0 {
		sr, err := ipc.NewReader(r, ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			t.Fatalf("stream %d: %v", n, err)
		}
		batches := 0
		for sr.Next() {
			batches++
			fn(n, sr.RecordBatch())
		}
		if err := sr.Err(); err != nil {
			t.Fatalf("stream %d: %v", n, err)
		}
		if batches != 1 {
			t.Fatalf("stream %d has %d batches, not one", n, batches)
		}
		sr.Release()
		n++
	}
	return n
}

// encoders are the two ways of writing entity references.
var encoders = map[string]func(wire.Document) ([]byte, error){
	"arrow": bytesgate.Arrow, "dictionaries": bytesgate.ArrowDictionaries,
}

// allDocs are the small fixtures and the golden documents.
func allDocs(t testing.TB) map[string]wire.Document {
	docs := build(t, small())
	for name, d := range goldens(t) {
		docs[name] = d
	}
	return docs
}

// A document is twelve streams that an Arrow reader accepts, one for each
// section, holding one row for each thing the section lists.
func TestArrowIsTwelveStreamsOfRows(t *testing.T) {
	docs := allDocs(t)
	for name, d := range docs {
		for mode, write := range encoders {
			b, err := write(d)
			if err != nil {
				t.Fatalf("%s %s: %v", name, mode, err)
			}
			sg := d.Subgraph
			s := sg.Sections
			want := []int{
				1, len(sg.Entities), len(sg.Elements), len(sg.Keyframes), len(sg.Deltas), 1, 1,
				len(s.Unattributed), len(s.Unmapped), len(s.Suppressed), len(s.Pods), 1,
			}
			infos, err := bytesgate.Streams(b)
			if err != nil || len(infos) != len(want) {
				t.Fatalf("%s %s: %d streams, %v", name, mode, len(infos), err)
			}
			var bytesSum, meta int
			for i, info := range infos {
				if info.Rows != want[i] || info.Batches != 1 {
					t.Errorf("%s %s: stream %d has %d rows in %d batches, want %d in 1", name, mode, i, info.Rows, info.Batches, want[i])
				}
				if info.Meta <= 0 || info.Meta > info.Bytes {
					t.Errorf("%s %s: stream %d has %d metadata bytes of %d", name, mode, i, info.Meta, info.Bytes)
				}
				bytesSum += info.Bytes
				meta += info.Meta
			}
			if bytesSum != len(b) {
				t.Errorf("%s %s: the streams hold %d of %d bytes", name, mode, bytesSum, len(b))
			}
			if got, err := bytesgate.Metadata(b); err != nil || got != meta {
				t.Errorf("%s %s: metadata %d, %v, want %d", name, mode, got, err, meta)
			}
		}
	}
}

// A document rebuilt from its Arrow streams is the document that was written:
// nothing the format carries is left out and nothing is added. It is checked for
// both ways of referring to entities, on the fixtures and on the golden
// documents.
func TestArrowRebuildsTheDocument(t *testing.T) {
	for name, d := range allDocs(t) {
		for mode, write := range encoders {
			b, err := write(d)
			if err != nil {
				t.Fatalf("%s %s: %v", name, mode, err)
			}
			requireSame(t, name+" "+mode, d, rebuild(t, b))
		}
	}
}

// The bytes of Arrow are a function of the document.
func TestArrowIsDeterministic(t *testing.T) {
	for name, d := range allDocs(t) {
		for mode, write := range encoders {
			first, err := write(d)
			if err != nil {
				t.Fatalf("%s %s: %v", name, mode, err)
			}
			for range 3 {
				if again, _ := write(d); !bytes.Equal(first, again) {
					t.Errorf("%s %s: two writes, two byte strings", name, mode)
				}
			}
		}
	}
}

// The body of a stream is not compressed: the bytes are at least what the
// fixed-width columns of the keyframes, the deltas and the elements take.
func TestArrowIsNotCompressed(t *testing.T) {
	for name, d := range build(t, small()) {
		b, err := bytesgate.Arrow(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		sg := d.Subgraph
		floor := 14*len(sg.Deltas) + 3*len(sg.Elements)
		for _, k := range sg.Keyframes {
			floor += 4 * len(k.Elements)
		}
		if len(b) < floor {
			t.Errorf("%s: %d bytes, below the %d the columns take", name, len(b), floor)
		}
	}
}

// An entity reference is an index into the entities in Arrow, and a column of
// the dictionary type, whose dictionary holds each entity once, in
// ArrowDictionaries.
func TestArrowReferencesAreIndicesOrDictionaries(t *testing.T) {
	d := build(t, small())["a"]
	for mode, write := range encoders {
		b, err := write(d)
		if err != nil {
			t.Fatal(err)
		}
		streamsOf(t, b, func(i int, rec arrow.RecordBatch) {
			if i != 2 {
				return
			}
			switch col := rec.Column(0).(type) {
			case *array.Uint32:
				if mode != "arrow" {
					t.Errorf("%s: a column of indices", mode)
				}
			case *array.Dictionary:
				if mode != "dictionaries" {
					t.Errorf("%s: a dictionary column", mode)
				}
				if col.Dictionary().Len() >= col.Len() || col.Dictionary().Len() == 0 {
					t.Errorf("a column of %d rows has a dictionary of %d", col.Len(), col.Dictionary().Len())
				}
			default:
				t.Errorf("%s: a column of %T", mode, col)
			}
		})
	}
}

func TestArrowRefusesWhatTheFormatRefuses(t *testing.T) {
	for mode, write := range encoders {
		if _, err := write(wire.Document{}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s, an empty document: %v", mode, err)
		}
	}
}

func listLen(col arrow.Array, row int) int {
	lo, hi := col.(*array.List).ValueOffsets(row)
	return int(hi - lo)
}

// The nested lists hold what the document's lists hold: the quarantine's entities
// and edges, the spans and candidates of every unattributed entry and the lists
// of every pod-level search, in the golden documents that have them.
func TestArrowNestedListsCarryTheDocument(t *testing.T) {
	checked := 0
	for name, d := range goldens(t) {
		b, err := bytesgate.Arrow(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		sg := d.Subgraph
		streamsOf(t, b, func(i int, rec arrow.RecordBatch) {
			switch i {
			case 5:
				if got := listLen(rec.Column(0), 0); got != len(sg.Quarantine.Entities) {
					t.Errorf("%s: %d quarantined entities, want %d", name, got, len(sg.Quarantine.Entities))
				}
				if got := listLen(rec.Column(1), 0); got != len(sg.Quarantine.Edges) {
					t.Errorf("%s: %d quarantined edges, want %d", name, got, len(sg.Quarantine.Edges))
				}
				checked += len(sg.Quarantine.Entities) + len(sg.Quarantine.Edges)
			case 7:
				for row, u := range sg.Sections.Unattributed {
					if listLen(rec.Column(5), row) != len(u.Spans) || listLen(rec.Column(6), row) != len(u.Candidates) {
						t.Errorf("%s: unattributed entry %d differs", name, row)
					}
					checked += len(u.Spans) + len(u.Candidates)
				}
			case 10:
				for row, p := range sg.Sections.Pods {
					if listLen(rec.Column(2), row) != len(p.Quarantined) || listLen(rec.Column(3), row) != len(p.Suppressed) {
						t.Errorf("%s: pod search %d differs", name, row)
					}
					checked += len(p.Quarantined) + len(p.Suppressed)
				}
			}
		})
	}
	if checked == 0 {
		t.Error("the golden documents have no nested list to check")
	}
}

// leaves names the columns of a type that hold values, nested ones by their path.
func leaves(prefix string, t arrow.DataType) []string {
	switch x := t.(type) {
	case *arrow.ListType:
		return leaves(prefix, x.Elem())
	case *arrow.StructType:
		var out []string
		for _, f := range x.Fields() {
			name := f.Name
			if prefix != "" {
				name = prefix + "." + f.Name
			}
			out = append(out, leaves(name, f.Type)...)
		}
		return out
	}
	return []string{prefix}
}

func under(prefix, names string) []string {
	var out []string
	for _, n := range strings.Fields(names) {
		out = append(out, prefix+"."+n)
	}
	return out
}

// The columns of each stream are the ones written out here, from the description
// of the format: a column the writer adds, or drops, or renames, is a difference.
// The reader above reads the columns it knows, so an added one is not otherwise
// seen.
func TestArrowColumnsAreTheOnesTheFormatWrites(t *testing.T) {
	const (
		element = "a b relation derived_kind detail subject class transitions paths " +
			"pieces.start pieces.end pieces.before pieces.end_reason pieces.producers pieces.ends.path pieces.ends.leg pieces.ends.reason"
		collision  = "collision.stale_boot collision.observed_at collision.newer_boot collision.newer_first_seen"
		truncation = "nodes hosts members instances seed_pods depth frontier.depth frontier.dropped per_via.via per_via.mechanism per_via.dropped " +
			"results pairs members_dropped suppressed_dropped"
		suppressed = "a b derived_kind detail reason coverage_nodes coverage_frontier class transitions spans.start spans.end spans.before"
	)
	cat := func(parts ...[]string) []string { return slices.Concat(parts...) }
	words := func(s string) []string { return strings.Fields(s) }
	want := [][]string{
		words("format_major format_minor basis token from to policy_subgraph policy_interval policy_coresidency policy_extract " +
			"policy_blastradius policy_graphdiff identity_encoding catalog_ids rank query_kind seeds options.name options.value asof"),
		words("fingerprint boundary"),
		words(element),
		words("at elements"),
		words("at element op reason"),
		cat(words("entities.entity entities.layer"), under("entities", collision), under("edges.element", element),
			words("edges.side edges.layer"), under("edges", collision)),
		words(truncation),
		words("seed_pod subject reason relation target spans.start spans.end spans.before candidates"),
		words("pod spans.start spans.end spans.before"),
		words(suppressed),
		cat(words("seed"), under("truncated", truncation), words("quarantined.kind quarantined.a quarantined.b quarantined.relation"),
			under("suppressed", suppressed)),
		words("anchor judged frontier_nodes"),
	}
	d := allDocs(t)["extraction.bin"]
	for mode, write := range encoders {
		b, err := write(d)
		if err != nil {
			t.Fatal(err)
		}
		streamsOf(t, b, func(i int, rec arrow.RecordBatch) {
			got := leaves("", arrow.StructOf(rec.Schema().Fields()...))
			if !slices.Equal(got, want[i]) {
				t.Errorf("%s: the columns of stream %d are\n%v\nwant\n%v", mode, i, got, want[i])
			}
		})
	}
}
