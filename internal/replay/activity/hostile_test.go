package activity_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// multiGroupFile is a valid file of 40 records in six row groups.
func multiGroupFile(tb fataler) ([]byte, []store.Record) {
	tb.Helper()
	recs := sample(tb, 40)
	return writeFile(tb, recs, activity.WriterOptions{RowGroupRows: 7}), recs
}

// failure opens and reads the file and returns the error that stopped it. The
// test fails if the file reads without an error, or if the error does not wrap
// activity.ErrFormat.
func failure(tb fataler, data []byte, opts activity.ReaderOptions) error {
	tb.Helper()
	r, err := openFile(data, opts)
	if err == nil {
		_, err = drain(r)
	}
	if err == nil {
		tb.Fatalf("a file that should be refused read without an error")
	}
	if !errors.Is(err, activity.ErrFormat) {
		tb.Fatalf("error %v does not wrap ErrFormat", err)
	}
	return err
}

func TestNotParquet(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(5, 6))
	random := randomBytes(rng, 1024)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"zero bytes", nil},
		{"the magic only", []byte("PAR1")},
		{"the magic twice", []byte("PAR1PAR1")},
		{"random bytes", random},
		{"random bytes between magics", append(append([]byte("PAR1"), random...), "PAR1"...)},
		{"random bytes with a footer length and the magic", append(append([]byte{}, random...), 0xff, 0xff, 0xff, 0x7f, 'P', 'A', 'R', '1')},
		{"a line of JSON", []byte(`{"resourceLogs":[{"scopeLogs":[]}]}` + "\n")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := openFile(tc.data, activity.ReaderOptions{}); !errors.Is(err, activity.ErrFormat) {
				t.Errorf("NewReader = %v, want an error wrapping ErrFormat", err)
			}
		})
	}
}

func TestTruncated(t *testing.T) {
	t.Parallel()

	data, _ := multiGroupFile(t)
	var lengths []int
	for k := range 64 {
		lengths = append(lengths, len(data)*k/64)
	}
	for j := 1; j <= 16; j++ {
		lengths = append(lengths, len(data)-j)
	}
	for _, n := range lengths {
		_ = failure(t, data[:n], activity.ReaderOptions{})
	}
}

// specColumn is one column of the schema, written out here independently of
// the package: the test fails if the package's idea of the schema drifts.
type specColumn struct {
	name    string
	typ     parquet.Type
	logical schema.LogicalType
	rep     parquet.Repetition
}

func spec() []specColumn {
	req, opt := parquet.Repetitions.Required, parquet.Repetitions.Optional
	i32, i64, bytesType := parquet.Types.Int32, parquet.Types.Int64, parquet.Types.ByteArray
	u64, s64, s32 := schema.NewIntLogicalType(64, false), schema.NewIntLogicalType(64, true), schema.NewIntLogicalType(32, true)
	str := schema.StringLogicalType{}
	return []specColumn{
		{"seq", i64, u64, req},
		{"event_time_ns", i64, s64, req},
		{"layer", bytesType, str, req},
		{"subject_kind", bytesType, str, req},
		{"source", bytesType, str, req},
		{"target", bytesType, str, opt},
		{"relation", bytesType, str, opt},
		{"producer", bytesType, str, req},
		{"kind", bytesType, str, req},
		{"ttl_ns", i64, s64, req},
		{"through_ns", i64, s64, opt},
		{"payload", bytesType, nil, req},
		{"boot", bytesType, str, opt},
		{"event_time_basis", i32, s32, req},
	}
}

// schemaFile writes a file with the given columns, two rows of placeholder
// values and footer metadata that is consistent with a file of two records.
func schemaFile(tb fataler, cols []specColumn) []byte {
	tb.Helper()
	fields := make(schema.FieldList, len(cols))
	for i, c := range cols {
		var n *schema.PrimitiveNode
		var err error
		if c.logical != nil {
			n, err = schema.NewPrimitiveNodeLogical(c.name, c.rep, c.logical, c.typ, -1, -1)
		} else {
			n, err = schema.NewPrimitiveNode(c.name, c.rep, c.typ, -1, -1)
		}
		if err != nil {
			tb.Fatalf("column %s: %v", c.name, err)
		}
		fields[i] = n
	}
	root, err := schema.NewGroupNode("activity", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		tb.Fatal(err)
	}
	var buf bytes.Buffer
	fw, err := file.NewParquetWriterWithError(&buf, root)
	if err != nil {
		tb.Fatal(err)
	}
	rg, err := fw.AppendRowGroupChecked()
	if err != nil {
		tb.Fatal(err)
	}
	for _, c := range cols {
		cw, err := rg.NextColumn()
		if err != nil {
			tb.Fatal(err)
		}
		var def []int16
		if c.rep == parquet.Repetitions.Optional {
			def = []int16{1, 1}
		}
		switch w := cw.(type) {
		case *file.Int32ColumnChunkWriter:
			_, err = w.WriteBatch([]int32{1, 2}, def, nil)
		case *file.Int64ColumnChunkWriter:
			_, err = w.WriteBatch([]int64{1, 2}, def, nil)
		case *file.ByteArrayColumnChunkWriter:
			_, err = w.WriteBatch([]parquet.ByteArray{[]byte("a"), []byte("b")}, def, nil)
		default:
			tb.Fatalf("unexpected column writer %T", cw)
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	if err := rg.Close(); err != nil {
		tb.Fatal(err)
	}
	for k, v := range map[string]string{
		"toposhift.activity.version":       "1",
		"toposhift.activity.records":       "2",
		"toposhift.activity.min_seq":       "1",
		"toposhift.activity.max_seq":       "2",
		"toposhift.activity.group_digests": strings.Repeat("0", 64),
		"toposhift.activity.digest":        specFileDigest([]string{strings.Repeat("0", 64)}),
	} {
		if err := fw.AppendKeyValueMetadata(k, v); err != nil {
			tb.Fatal(err)
		}
	}
	if err := fw.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func TestOtherSchemas(t *testing.T) {
	t.Parallel()

	// The control: a file with the schema of the specification is opened. If
	// it were refused, the refusals below would prove nothing.
	if _, err := openFile(schemaFile(t, spec()), activity.ReaderOptions{}); err != nil {
		t.Fatalf("a file with the schema of format version 1 was refused at the footer: %v", err)
	}

	edit := func(f func(c []specColumn) []specColumn) []specColumn { return f(spec()) }
	for _, tc := range []struct {
		name string
		cols []specColumn
	}{
		{"a column missing", edit(func(c []specColumn) []specColumn { return append(c[:9], c[10:]...) })},
		{"the last column missing", edit(func(c []specColumn) []specColumn { return c[:12] })},
		{"event_time_basis as an int64", edit(func(c []specColumn) []specColumn {
			c[13].typ, c[13].logical = parquet.Types.Int64, schema.NewIntLogicalType(64, true)
			return c
		})},
		{"event_time_basis optional", edit(func(c []specColumn) []specColumn { c[13].rep = parquet.Repetitions.Optional; return c })},
		{"event_time_basis unsigned", edit(func(c []specColumn) []specColumn { c[13].logical = schema.NewIntLogicalType(32, false); return c })},
		{"the last column missing, which is the basis", edit(func(c []specColumn) []specColumn { return c[:13] })},
		{"boot required", edit(func(c []specColumn) []specColumn { c[12].rep = parquet.Repetitions.Required; return c })},
		{"boot as an integer", edit(func(c []specColumn) []specColumn { c[12].typ, c[12].logical = parquet.Types.Int64, nil; return c })},
		{"boot without the string annotation", edit(func(c []specColumn) []specColumn { c[12].logical = nil; return c })},
		{"a column renamed", edit(func(c []specColumn) []specColumn { c[11].name = "body"; return c })},
		{"seq as INT32", edit(func(c []specColumn) []specColumn { c[0].typ, c[0].logical = parquet.Types.Int32, nil; return c })},
		{"seq as signed", edit(func(c []specColumn) []specColumn { c[0].logical = schema.NewIntLogicalType(64, true); return c })},
		{"seq without an annotation", edit(func(c []specColumn) []specColumn { c[0].logical = nil; return c })},
		{"event time as a timestamp", edit(func(c []specColumn) []specColumn {
			c[1].logical = schema.NewTimestampLogicalType(true, schema.TimeUnitNanos)
			return c
		})},
		{"an extra column", edit(func(c []specColumn) []specColumn {
			return append(c, specColumn{"extra", parquet.Types.Int64, nil, parquet.Repetitions.Required})
		})},
		{"payload optional", edit(func(c []specColumn) []specColumn { c[11].rep = parquet.Repetitions.Optional; return c })},
		{"target required", edit(func(c []specColumn) []specColumn { c[5].rep = parquet.Repetitions.Required; return c })},
		{"layer without the string annotation", edit(func(c []specColumn) []specColumn { c[2].logical = nil; return c })},
		{"the first two columns swapped", edit(func(c []specColumn) []specColumn { c[0], c[1] = c[1], c[0]; return c })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := schemaFile(t, tc.cols)
			_, err := openFile(data, activity.ReaderOptions{})
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), "schema") {
				t.Errorf("NewReader = %v, want an error wrapping ErrFormat that names the schema", err)
			}
		})
	}
}

// tamperedFile writes records with the footer metadata changed by f.
func tamperedFile(tb fataler, recs []store.Record, f func(kv map[string]string)) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 7})
	if err != nil {
		tb.Fatal(err)
	}
	activity.Tamper(w, f)
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// tamperedFileRows writes records in row groups of the given size with the
// footer metadata changed by f.
func tamperedFileRows(tb fataler, recs []store.Record, rowGroupRows int, f func(kv map[string]string)) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: rowGroupRows})
	if err != nil {
		tb.Fatal(err)
	}
	activity.Tamper(w, f)
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func TestBadFooterMetadata(t *testing.T) {
	t.Parallel()

	const (
		version = "toposhift.activity.version"
		records = "toposhift.activity.records"
		minSeq  = "toposhift.activity.min_seq"
		maxSeq  = "toposhift.activity.max_seq"
		digest  = "toposhift.activity.digest"
		groups  = "toposhift.activity.group_digests"
	)
	other := "sha256:" + strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name    string
		records int
		tamper  func(kv map[string]string)
		want    string // in the error
	}{
		{"no version", 20, func(kv map[string]string) { delete(kv, version) }, "version"},
		{"version 2", 20, func(kv map[string]string) { kv[version] = "2" }, `"2"`},
		{"version 1.0", 20, func(kv map[string]string) { kv[version] = "1.0" }, `"1.0"`},
		{"one record too many", 20, func(kv map[string]string) { kv[records] = "21" }, "records"},
		{"one record too few", 20, func(kv map[string]string) { kv[records] = "19" }, "records"},
		{"no record count", 20, func(kv map[string]string) { delete(kv, records) }, "records"},
		{"record count not a number", 20, func(kv map[string]string) { kv[records] = "twenty" }, "records"},
		{"record count with a sign", 20, func(kv map[string]string) { kv[records] = "+20" }, "records"},
		{"record count with leading zeros", 20, func(kv map[string]string) { kv[records] = "020" }, "records"},
		{"min_seq too high", 20, func(kv map[string]string) { kv[minSeq] = "4" }, "min_seq"},
		{"min_seq too low", 20, func(kv map[string]string) { kv[minSeq] = "2" }, "min_seq"},
		{"max_seq too high", 20, func(kv map[string]string) { kv[maxSeq] = "61" }, "max_seq"},
		{"max_seq too low", 20, func(kv map[string]string) { kv[maxSeq] = "57" }, "max_seq"},
		{"min_seq above max_seq", 20, func(kv map[string]string) { kv[minSeq] = "61" }, "min_seq"},
		{"min_seq missing", 20, func(kv map[string]string) { delete(kv, minSeq) }, "min_seq"},
		{"max_seq missing", 20, func(kv map[string]string) { delete(kv, maxSeq) }, "max_seq"},
		{"max_seq beyond 64 bits", 20, func(kv map[string]string) { kv[maxSeq] = "18446744073709551616" }, "max_seq"},
		{"min_seq present with zero records", 0, func(kv map[string]string) { kv[minSeq] = "1" }, "min_seq"},
		{"max_seq present with zero records", 0, func(kv map[string]string) { kv[maxSeq] = "1" }, "max_seq"},
		{"group digests missing", 20, func(kv map[string]string) { delete(kv, groups) }, "group_digests"},
		{"one group digest too few", 20, func(kv map[string]string) { kv[groups] = kv[groups][:strings.LastIndex(kv[groups], ",")] }, "group_digests"},
		{"one group digest too many", 20, func(kv map[string]string) { kv[groups] += "," + kv[groups][:64] }, "group_digests"},
		{"group digests empty", 20, func(kv map[string]string) { kv[groups] = "" }, "group_digests"},
		{"a group digest in uppercase", 20, func(kv map[string]string) { kv[groups] = strings.ToUpper(kv[groups]) }, "group_digests"},
		{"group digests in another order", 20, func(kv map[string]string) {
			parts := strings.Split(kv[groups], ",")
			parts[0], parts[1] = parts[1], parts[0]
			kv[groups] = strings.Join(parts, ",")
		}, "digest"},
		{"group digests that give another file digest", 20, func(kv map[string]string) {
			kv[groups] = strings.Repeat("ab", 32) + kv[groups][64:]
		}, "digest"},
		{"digest of other content", 20, func(kv map[string]string) { kv[digest] = other }, "digest"},
		{"digest of other content in an empty file", 0, func(kv map[string]string) { kv[digest] = other }, "digest"},
		{"digest missing", 20, func(kv map[string]string) { delete(kv, digest) }, "digest"},
		{"digest without its prefix", 20, func(kv map[string]string) { kv[digest] = strings.Repeat("ab", 32) }, "digest"},
		{"digest of another algorithm", 20, func(kv map[string]string) { kv[digest] = "sha1:" + strings.Repeat("ab", 20) }, "digest"},
		{"digest in uppercase", 20, func(kv map[string]string) { kv[digest] = "sha256:" + strings.Repeat("AB", 32) }, "digest"},
		{"digest too short", 20, func(kv map[string]string) { kv[digest] = other[:len(other)-1] }, "digest"},
		{"digest not hex", 20, func(kv map[string]string) { kv[digest] = "sha256:" + strings.Repeat("xy", 32) }, "digest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := tamperedFile(t, sample(t, tc.records), tc.tamper)
			err := failure(t, data, activity.ReaderOptions{})
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

func TestFileWithoutActivityMetadata(t *testing.T) {
	t.Parallel()

	// Any other Parquet file with the right columns is not an activity file.
	data := tamperedFile(t, sample(t, 3), func(kv map[string]string) { clear(kv) })
	if err := failure(t, data, activity.ReaderOptions{}); !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q does not name the missing version", err)
	}
}

func TestBadRows(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p").String()
	node := fingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n").String()
	upper := func(fp string) string {
		typ, hash, _ := strings.Cut(fp, ":")
		return typ + ":" + strings.ToUpper(hash)
	}
	str := func(s string) *string { return &s }
	ns := func(n int64) *int64 { return &n }
	at := epoch.UnixNano()

	good := activity.RawRow{
		Seq: 1, EventNS: at, Layer: "L2", SubjectKind: "edge", Source: pod, Target: str(node), Relation: str("scheduled_on"),
		Producer: "k8s", Kind: "observe",
	}
	entity := good
	entity.SubjectKind, entity.Target, entity.Relation = "entity", nil, nil
	hostEntity := entity
	hostEntity.Layer, hostEntity.Source = "L1", fingerprint(t, catalog.Host, catalog.HostID, "h").String()

	// Each case is the row that follows a good one with Seq 1.
	for _, tc := range []struct {
		name string
		base activity.RawRow
		edit func(r *activity.RawRow)
		want string // in the error
	}{
		{"a good edge (control)", good, func(r *activity.RawRow) {}, ""},
		{"a good entity (control)", entity, func(r *activity.RawRow) {}, ""},
		{"a basis of 4, the largest (control)", good, func(r *activity.RawRow) { r.Basis = 4 }, ""},
		{"a basis of 5", good, func(r *activity.RawRow) { r.Basis = 5 }, "event_time_basis"},
		{"a basis of 255", good, func(r *activity.RawRow) { r.Basis = 255 }, "event_time_basis"},
		{"a basis of 256, which is 0 in a byte", good, func(r *activity.RawRow) { r.Basis = 256 }, "event_time_basis"},
		{"a negative basis", good, func(r *activity.RawRow) { r.Basis = -1 }, "event_time_basis"},
		{"the largest basis a column holds", good, func(r *activity.RawRow) { r.Basis = 1<<31 - 1 }, "event_time_basis"},
		{"a host with a boot (control)", hostEntity, func(r *activity.RawRow) { r.Boot = str("boot-1") }, ""},
		{"a host with the longest boot (control)", hostEntity, func(r *activity.RawRow) { r.Boot = str(strings.Repeat("b", store.MaxBootLen)) }, ""},
		{"a boot of 257 bytes", hostEntity, func(r *activity.RawRow) { r.Boot = str(strings.Repeat("b", store.MaxBootLen+1)) }, "boot"},
		{"a boot on a pod", entity, func(r *activity.RawRow) { r.Boot = str("boot-1") }, "boot"},
		{"a boot on an edge", good, func(r *activity.RawRow) { r.Boot = str("boot-1") }, "boot"},
		{"a boot on a delete", hostEntity, func(r *activity.RawRow) { r.Kind, r.Boot = "delete", str("boot-1") }, "boot"},
		{"a blank boot", hostEntity, func(r *activity.RawRow) { r.Boot = str("  \t") }, "boot"},
		{"a present but empty boot", hostEntity, func(r *activity.RawRow) { r.Boot = str("") }, "boot"},
		{"a boot that is not UTF-8", hostEntity, func(r *activity.RawRow) { r.Boot = str("b\xff") }, "boot"},
		{"a producer of the longest length (control)", good, func(r *activity.RawRow) { r.Producer = strings.Repeat("p", activity.MaxText) }, ""},
		{"a producer over MaxText", good, func(r *activity.RawRow) { r.Producer = strings.Repeat("p", activity.MaxText+1) }, "producer"},
		{"a relation of the longest length (control)", good, func(r *activity.RawRow) { r.Relation = str(strings.Repeat("r", activity.MaxText)) }, ""},
		{"a relation over MaxText", good, func(r *activity.RawRow) { r.Relation = str(strings.Repeat("r", activity.MaxText+1)) }, "relation"},
		{"a source that is far too long", good, func(r *activity.RawRow) { r.Source = strings.Repeat("s", 4096) }, "source"},
		{"a delete with a payload", good, func(r *activity.RawRow) { r.Kind, r.Payload = "delete", []byte("x") }, "payload"},
		{"an entity with a target", entity, func(r *activity.RawRow) { r.Target = str(node) }, "target"},
		{"an entity with a relation", entity, func(r *activity.RawRow) { r.Relation = str("part_of") }, "relation"},
		{"an edge without a target", good, func(r *activity.RawRow) { r.Target = nil }, "target"},
		{"an edge without a relation", good, func(r *activity.RawRow) { r.Relation = nil }, "relation"},
		{"an edge with an empty relation", good, func(r *activity.RawRow) { r.Relation = str("") }, "relation"},
		{"uppercase hex in the source", good, func(r *activity.RawRow) { r.Source = upper(pod) }, "source"},
		{"uppercase hex in the target", good, func(r *activity.RawRow) { r.Target = str(upper(node)) }, "target"},
		{"a source that is not a fingerprint", good, func(r *activity.RawRow) { r.Source = "pod-1" }, "source"},
		{"an empty source", good, func(r *activity.RawRow) { r.Source = "" }, "source"},
		{"layer L9", good, func(r *activity.RawRow) { r.Layer = "L9" }, "layer"},
		{"layer in lowercase", good, func(r *activity.RawRow) { r.Layer = "l2" }, "layer"},
		{"an empty layer", good, func(r *activity.RawRow) { r.Layer = "" }, "layer"},
		{"kind update", good, func(r *activity.RawRow) { r.Kind = "update" }, "kind"},
		{"kind in uppercase", good, func(r *activity.RawRow) { r.Kind = "Observe" }, "kind"},
		{"subject kind node", good, func(r *activity.RawRow) { r.SubjectKind = "node" }, "subject_kind"},
		{"through before the event time", good, func(r *activity.RawRow) { r.ThroughNS = ns(at - 1) }, "through"},
		{"a delete with a through time", good, func(r *activity.RawRow) { r.Kind, r.ThroughNS = "delete", ns(at+1) }, "through"},
		{"an event time before 1970", good, func(r *activity.RawRow) { r.EventNS = -1 }, "range"},
		{"a negative TTL", good, func(r *activity.RawRow) { r.TTLNS = -1 }, "ttl_ns"},
		{"a TTL whose deadline is out of range", good, func(r *activity.RawRow) { r.EventNS, r.TTLNS = 1<<63-1, 1 }, "deadline"},
		{"a delete with a TTL", good, func(r *activity.RawRow) { r.Kind, r.TTLNS = "delete", 1 }, "TTL"},
		{"an entity in another layer than its type's", entity, func(r *activity.RawRow) { r.Layer = "L1" }, "layer"},
		{"an empty producer", good, func(r *activity.RawRow) { r.Producer = "" }, "producer"},
		{"a producer that is not UTF-8", good, func(r *activity.RawRow) { r.Producer = "a\xffb" }, "producer"},
		{"a payload over the limit", good, func(r *activity.RawRow) { r.Payload = make([]byte, activity.MaxPayload+1) }, "payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bad := tc.base
			bad.Seq = 2
			tc.edit(&bad)

			var buf bytes.Buffer
			// One row per row group: the good row is returned, the bad one is not.
			w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 1})
			if err != nil {
				t.Fatal(err)
			}
			first := good
			for _, r := range []activity.RawRow{first, bad} {
				if err := activity.WriteRaw(w, r); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			if tc.want == "" {
				if got := readFile(t, buf.Bytes(), activity.ReaderOptions{}); len(got) != 2 {
					t.Errorf("read %d records, want 2", len(got))
				}
				return
			}
			r, err := openFile(buf.Bytes(), activity.ReaderOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := drain(r)
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %v, want one wrapping ErrFormat that names %q", err, tc.want)
			}
			if len(got) != 1 {
				t.Errorf("read %d records before the error, want 1: the bad row's group must not be returned", len(got))
			}
		})
	}
}

func TestSeqMustRise(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p").String()
	row := func(seq uint64) activity.RawRow {
		return activity.RawRow{Seq: seq, EventNS: epoch.UnixNano(), Layer: "L2", SubjectKind: "entity", Source: pod, Producer: "k8s", Kind: "observe"}
	}
	for _, tc := range []struct {
		name         string
		rowGroupRows int
		seqs         []uint64
	}{
		{"equal in one row group", 10, []uint64{1, 2, 2, 3}},
		{"below in one row group", 10, []uint64{1, 5, 3, 7}},
		{"equal across a row group boundary", 2, []uint64{1, 2, 2, 3}},
		{"below across a row group boundary", 2, []uint64{1, 5, 3, 7}},
		{"zero first", 10, []uint64{0, 1, 2}},
		{"zero after a row group boundary", 2, []uint64{1, 2, 0, 3}},
		{"falling from the largest", 3, []uint64{1 << 63, 1<<64 - 1, 1 << 63}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: tc.rowGroupRows})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range tc.seqs {
				if err := activity.WriteRaw(w, row(s)); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			err = failure(t, buf.Bytes(), activity.ReaderOptions{})
			if !strings.Contains(err.Error(), "seq") {
				t.Errorf("error %q does not name seq", err)
			}
		})
	}
}

// The writer's own refusals can be switched off to prove the reader checks the
// same rules by itself.
func TestUncheckedWriterIsStillRefusedByTheReader(t *testing.T) {
	t.Parallel()

	recs := sample(t, 6)
	recs[3], recs[2] = recs[2], recs[3] // out of order
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(recs[0]); err != nil {
		t.Fatal(err)
	}
	activity.Unchecked(w)
	for _, r := range recs[1:] {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = failure(t, buf.Bytes(), activity.ReaderOptions{})
}

func TestRowGroupOverTheLimit(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(7, 8))
	recs := sample(t, 100)
	for i := range recs {
		if recs[i].Kind == lifecycle.Observe {
			recs[i].Payload = randomBytes(rng, 1024)
		}
	}
	data := writeFile(t, recs, activity.WriterOptions{}) // one row group of over 50 KiB
	r, err := openFile(data, activity.ReaderOptions{MaxRowGroupBytes: 4096})
	if err != nil {
		if !errors.Is(err, activity.ErrFormat) {
			t.Fatalf("NewReader = %v", err)
		}
	} else {
		var got []store.Record
		got, err = drain(r)
		if len(got) != 0 {
			t.Errorf("%d records were returned from a row group over the limit, want it refused before decoding", len(got))
		}
	}
	if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), "4096") {
		t.Errorf("error %v, want one wrapping ErrFormat that names the limit of 4096", err)
	}

	// The same file reads with a limit that fits.
	requireSame(t, recs, readFile(t, data, activity.ReaderOptions{MaxRowGroupBytes: 1 << 20}))
}

func TestCorruptionSweep(t *testing.T) {
	t.Parallel()

	const groupRows = 9
	rng := rand.New(rand.NewPCG(9, 10))
	recs := sample(t, 60)
	for i := range recs {
		if recs[i].Kind == lifecycle.Observe {
			recs[i].Payload = randomBytes(rng, 5+i)
		}
	}
	data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: groupRows})

	const variants = 600
	var errs, identical, recovered, wrong int
	for i := range variants {
		v := bytes.Clone(data)
		switch i % 3 {
		case 0:
			v = v[:rng.IntN(len(v))]
		case 1:
			for range 4 {
				v[rng.IntN(len(v))] ^= byte(1 + rng.IntN(255))
			}
		case 2:
			for range 3 {
				v[len(v)-1-rng.IntN(600)] ^= byte(1 + rng.IntN(255))
			}
		}

		r, err := openFile(v, activity.ReaderOptions{BatchRows: 1 + rng.IntN(20)})
		var got []store.Record
		if err == nil {
			got, err = drain(r)
		}
		if err != nil {
			if !errors.Is(err, activity.ErrFormat) {
				t.Errorf("variant %d: error %v does not wrap ErrFormat", i, err)
				wrong++
				continue
			}
			errs++
			if strings.Contains(err.Error(), "Parquet library failed") {
				recovered++
			}
			// What was returned before the error is whole, verified row groups
			// and the original records.
			if len(got)%groupRows != 0 || len(got) >= len(recs) {
				t.Errorf("variant %d: %d records were returned before the error %q, want whole row groups of %d", i, len(got), err, groupRows)
				wrong++
			} else if !prefixOf(recs, got) {
				t.Errorf("variant %d: the %d records returned before the error %q are not the original ones", i, len(got), err)
				wrong++
			}
			continue
		}
		// A file that reads without an error must be the original records.
		if len(got) != len(recs) || !prefixOf(recs, got) {
			t.Errorf("variant %d: read %d records without an error, want exactly the original %d", i, len(got), len(recs))
			wrong++
			continue
		}
		identical++
	}
	t.Logf("%d variants: %d refused (%d of them by the recovered panic of the Parquet library), %d read as exactly the original records, %d wrong",
		variants, errs, recovered, identical, wrong)
	if errs+identical+wrong != variants {
		t.Errorf("counted %d + %d + %d of %d variants", errs, identical, wrong, variants)
	}
	if errs < variants/2 {
		t.Errorf("only %d of %d damaged files were refused", errs, variants)
	}
}

// prefixOf reports whether got is the first len(got) of want.
func prefixOf(want, got []store.Record) bool {
	if len(got) > len(want) {
		return false
	}
	for i := range got {
		if !sameRecord(want[i], got[i]) {
			return false
		}
	}
	return true
}

// A row group whose values differ from what its digest says is refused as a
// whole: none of its records is returned. The groups before it are, and are
// the original records. Here every record of the damaged group is valid, so
// only the digest can tell.
func TestGroupDigestMismatchRefusesTheWholeGroup(t *testing.T) {
	t.Parallel()

	const groupRows = 7
	recs := sample(t, 40) // six row groups: 7, 7, 7, 7, 7, 5
	original := writeFile(t, recs, activity.WriterOptions{RowGroupRows: groupRows})
	pf, err := file2reader(original)
	if err != nil {
		t.Fatal(err)
	}
	kv := footerMetadata(pf)

	for k := range 6 {
		t.Run(fmt.Sprintf("group %d", k), func(t *testing.T) {
			t.Parallel()
			// The same file, but one record of group k has another producer,
			// and the footer is the footer of the original.
			changed := slices.Clone(recs)
			changed[k*groupRows+1].Producer = "another-collector"
			if err := changed[k*groupRows+1].Validate(); err != nil {
				t.Fatal(err)
			}
			data := tamperedFileRows(t, changed, groupRows, func(m map[string]string) {
				m["toposhift.activity.group_digests"] = kv["toposhift.activity.group_digests"]
				m["toposhift.activity.digest"] = kv["toposhift.activity.digest"]
			})
			r, err := openFile(data, activity.ReaderOptions{BatchRows: 3})
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			got, err := drain(r)
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), fmt.Sprintf("row group %d", k)) {
				t.Fatalf("error %v, want one wrapping ErrFormat that names row group %d", err, k)
			}
			if len(got) != k*groupRows {
				t.Errorf("%d records were returned, want exactly the %d of the %d verified row groups before the damaged one", len(got), k*groupRows, k)
			}
			if !prefixOf(recs, got) {
				t.Error("the records returned before the error are not the original ones")
			}
			if _, again := r.Next(); !errors.Is(again, err) {
				t.Errorf("Next after the error = %v, want the same error", again)
			}
		})
	}
}

// A byte flipped in a column chunk of row group k is refused without any record
// of group k being returned.
func TestFlippedColumnByteRefusesTheWholeGroup(t *testing.T) {
	t.Parallel()

	const groupRows = 7
	recs := sample(t, 40)
	original := writeFile(t, recs, activity.WriterOptions{RowGroupRows: groupRows})
	pf, err := file2reader(original)
	if err != nil {
		t.Fatal(err)
	}
	var refused, identical int
	for k := range pf.NumRowGroups() {
		for c := range 13 {
			cc, err := pf.MetaData().RowGroup(k).ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			start := cc.DataPageOffset()
			if cc.HasDictionaryPage() && cc.DictionaryPageOffset() > 0 {
				start = min(start, cc.DictionaryPageOffset())
			}
			for _, at := range []int64{start + cc.TotalCompressedSize()/2, start + cc.TotalCompressedSize() - 1} {
				v := bytes.Clone(original)
				v[at] ^= 0x5a
				r, err := openFile(v, activity.ReaderOptions{})
				var got []store.Record
				if err == nil {
					got, err = drain(r)
				}
				if err == nil {
					if len(got) != len(recs) || !prefixOf(recs, got) {
						t.Errorf("group %d column %d byte %d: read without an error and not as the original", k, c, at)
					}
					identical++
					continue
				}
				refused++
				if !errors.Is(err, activity.ErrFormat) {
					t.Errorf("group %d column %d byte %d: error %v does not wrap ErrFormat", k, c, at, err)
				}
				if len(got) != k*groupRows || !prefixOf(recs, got) {
					t.Errorf("group %d column %d byte %d: %d records returned before the error %q, want the %d of the groups before it",
						k, c, at, len(got), err, k*groupRows)
				}
			}
		}
	}
	t.Logf("%d flipped bytes refused, %d read as the original", refused, identical)
	if refused == 0 {
		t.Error("no flipped byte was refused")
	}
}

// The Parquet library panics on some damaged files. A flipped bit in the first
// bytes of a file (a page header, the start of the first column chunk) is enough
// for some of them; the reader must turn each panic into an error.
func TestLibraryPanicsAreRecovered(t *testing.T) {
	t.Parallel()

	data, recs := multiGroupFile(t)
	var recovered, refused int
	for p := range 600 {
		v := bytes.Clone(data)
		v[p] ^= 0x80
		r, err := openFile(v, activity.ReaderOptions{})
		if err == nil {
			_, err = drain(r)
		}
		if err == nil {
			// A flip the file does not show must leave the records as they were.
			if got := readFile(t, v, activity.ReaderOptions{}); len(got) != len(recs) || !prefixOf(recs, got) {
				t.Fatalf("flipping a bit of byte %d changed the records and was not refused", p)
			}
			continue
		}
		if !errors.Is(err, activity.ErrFormat) {
			t.Fatalf("flipping a bit of byte %d: error %v does not wrap ErrFormat", p, err)
		}
		refused++
		if strings.Contains(err.Error(), "Parquet library failed") {
			recovered++
		}
	}
	t.Logf("%d flipped bits refused, %d of them by the recovered panic of the Parquet library", refused, recovered)
	if recovered == 0 {
		t.Errorf("the Parquet library did not panic on any of the first 600 bytes flipped, as it did when this test was written. " +
			"A new version of arrow-go or klauspost/compress may have changed the bytes of the file, or fixed the panic: " +
			"choose other positions that make it panic, so that removing the recover is still caught; if it no longer panics at all, reconsider the recover boundary")
	}
}

// failingReaderAt serves reads from data until it has served limit of them.
type failingReaderAt struct {
	data  []byte
	limit int
	calls int
	err   error
}

func (f *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if f.calls >= f.limit {
		return 0, f.err
	}
	f.calls++
	return bytes.NewReader(f.data).ReadAt(p, off)
}

func TestFailingReaderAt(t *testing.T) {
	t.Parallel()

	data, recs := multiGroupFile(t)
	ioErr := errors.New("connection reset")
	for limit := 0; limit < 80; limit++ {
		t.Run(fmt.Sprintf("after %d reads", limit), func(t *testing.T) {
			src := &failingReaderAt{data: data, limit: limit, err: ioErr}
			r, err := activity.NewReader(src, int64(len(data)), activity.ReaderOptions{BatchRows: 3})
			var got []store.Record
			if err == nil {
				got, err = drain(r)
			}
			if err != nil {
				if !errors.Is(err, activity.ErrFormat) {
					t.Fatalf("error %v does not wrap ErrFormat", err)
				}
				if limit == 0 && !strings.Contains(err.Error(), "connection reset") {
					t.Errorf("error %q lost the cause", err)
				}
				return
			}
			requireSame(t, recs, got)
		})
	}

	// A source that fails at once.
	if _, err := activity.NewReader(&failingReaderAt{data: data, err: io.ErrUnexpectedEOF}, int64(len(data)), activity.ReaderOptions{}); !errors.Is(err, activity.ErrFormat) {
		t.Errorf("NewReader over a source that fails = %v, want an error wrapping ErrFormat", err)
	}
}

// bombFileWith writes a file of one row group of the given number of rows, every
// row an observation of one entity by one producer with the given payload. The
// footer is consistent, but the group digest is not that of the rows: a reader
// that gets as far as the digest has decoded the whole group. Everything but
// seq repeats, so dictionary and run-length encoding make the file small
// however many rows it declares, and seq is delta encoded.
// bomb describes a file for bombFileWith. The functions give the value of a row;
// a nil one repeats a constant. With distinct set, every row is the observation
// of another host by another producer with another boot id, and the payloads
// are the function's.
type bomb struct {
	rows     int
	payload  func(i int) []byte
	distinct bool
	pageSize int64 // the data page size of the writer; 0 is the library's
	plainPay bool  // write the payloads without a dictionary
	// dictionaryColumn, "payload" or "ttl_ns", is written with a dictionary of
	// any size, of distinct values, and nothing is compressed, so that the bytes
	// of the pages can be patched.
	dictionaryColumn string
}

func bombFileWith(tb fataler, b bomb) []byte {
	tb.Helper()
	rows := b.rows
	cols := spec()
	fields := make(schema.FieldList, len(cols))
	for i, c := range cols {
		var n *schema.PrimitiveNode
		var err error
		if c.logical != nil {
			n, err = schema.NewPrimitiveNodeLogical(c.name, c.rep, c.logical, c.typ, -1, -1)
		} else {
			n, err = schema.NewPrimitiveNode(c.name, c.rep, c.typ, -1, -1)
		}
		if err != nil {
			tb.Fatal(err)
		}
		fields[i] = n
	}
	root, err := schema.NewGroupNode("activity", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		tb.Fatal(err)
	}
	opts := []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryFor("seq", false),
		parquet.WithEncodingFor("seq", parquet.Encodings.DeltaBinaryPacked),
	}
	if b.pageSize > 0 {
		opts = append(opts, parquet.WithDictionaryFor("payload", !b.plainPay), parquet.WithDataPageSize(b.pageSize))
	}
	if b.dictionaryColumn != "" {
		opts = append(opts, parquet.WithCompression(compress.Codecs.Uncompressed), parquet.WithDictionaryPageSizeLimit(64<<20),
			parquet.WithDictionaryCostFallbackFor(b.dictionaryColumn, false))
	}
	props := parquet.NewWriterProperties(opts...)
	var buf bytes.Buffer
	fw, err := file.NewParquetWriterWithError(&buf, root, file.WithWriterProps(props))
	if err != nil {
		tb.Fatal(err)
	}
	rg, err := fw.AppendRowGroupChecked()
	if err != nil {
		tb.Fatal(err)
	}

	pod := fingerprint(tb, catalog.K8sPod, catalog.K8sPodUID, "bomb").String()
	seqs := make([]int64, rows)
	same := make([]int64, rows)
	for i := range seqs {
		seqs[i], same[i] = int64(i+1), epoch.UnixNano()
	}
	repeat := func(s []byte) []parquet.ByteArray {
		out := make([]parquet.ByteArray, rows)
		for i := range out {
			out[i] = s
		}
		return out
	}
	zeros := make([]int16, rows)
	ones := make([]int16, rows)
	for i := range ones {
		ones[i] = 1
	}
	each := func(f func(i int) string) []parquet.ByteArray {
		out := make([]parquet.ByteArray, rows)
		for i := range out {
			out[i] = []byte(f(i))
		}
		return out
	}
	layer, source, producer := "L2", pod, "p"
	for i, c := range cols {
		cw, err := rg.NextColumn()
		if err != nil {
			tb.Fatal(err)
		}
		switch w := cw.(type) {
		case *file.Int32ColumnChunkWriter:
			_, err = w.WriteBatch(make([]int32, rows), nil, nil)
		case *file.Int64ColumnChunkWriter:
			switch c.name {
			case "seq":
				_, err = w.WriteBatch(seqs, nil, nil)
			case "through_ns":
				_, err = w.WriteBatch(nil, zeros, nil)
			case "ttl_ns":
				ttls := make([]int64, rows)
				if b.dictionaryColumn == "ttl_ns" {
					copy(ttls, seqs) // distinct
				}
				_, err = w.WriteBatch(ttls, nil, nil)
			default:
				_, err = w.WriteBatch(same, nil, nil)
			}
		case *file.ByteArrayColumnChunkWriter:
			switch c.name {
			case "layer":
				if b.distinct {
					_, err = w.WriteBatch(repeat([]byte("L1")), nil, nil)
				} else {
					_, err = w.WriteBatch(repeat([]byte(layer)), nil, nil)
				}
			case "subject_kind":
				_, err = w.WriteBatch(repeat([]byte("entity")), nil, nil)
			case "source":
				if b.distinct {
					_, err = w.WriteBatch(each(func(i int) string { return fmt.Sprintf("host:%032x", i) }), nil, nil)
				} else {
					_, err = w.WriteBatch(repeat([]byte(source)), nil, nil)
				}
			case "target", "relation":
				_, err = w.WriteBatch(nil, zeros, nil)
			case "boot":
				if b.distinct {
					_, err = w.WriteBatch(each(func(i int) string { return fmt.Sprintf("boot-%d", i) }), ones, nil)
				} else {
					_, err = w.WriteBatch(nil, zeros, nil)
				}
			case "producer":
				if b.distinct {
					_, err = w.WriteBatch(each(func(i int) string { return fmt.Sprintf("producer-%d", i) }), nil, nil)
				} else {
					_, err = w.WriteBatch(repeat([]byte(producer)), nil, nil)
				}
			case "kind":
				_, err = w.WriteBatch(repeat([]byte("observe")), nil, nil)
			case "payload":
				out := make([]parquet.ByteArray, rows)
				for i := range out {
					out[i] = b.payload(i)
				}
				_, err = w.WriteBatch(out, nil, nil)
			}
		default:
			tb.Fatalf("column %d: unexpected writer %T", i, cw)
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	if err := rg.Close(); err != nil {
		tb.Fatal(err)
	}
	zero := strings.Repeat("0", 64)
	for k, v := range map[string]string{
		"toposhift.activity.version":       "1",
		"toposhift.activity.records":       fmt.Sprint(rows),
		"toposhift.activity.min_seq":       "1",
		"toposhift.activity.max_seq":       fmt.Sprint(rows),
		"toposhift.activity.group_digests": zero,
		"toposhift.activity.digest":        specFileDigest([]string{zero}),
	} {
		if err := fw.AppendKeyValueMetadata(k, v); err != nil {
			tb.Fatal(err)
		}
	}
	if err := fw.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// allocated returns the bytes allocated while f runs.
func allocated(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A small file can declare a row group that decodes to far more than the limit:
// repeated values cost almost nothing encoded. The reader must refuse such a
// group having allocated little more than the limit, not the size the file
// claims.
//
// The test is not parallel: it measures the allocation of the whole process,
// which other tests running at the same time would disturb.
func TestRowGroupThatDecodesToMoreThanTheLimit(t *testing.T) {
	const limit = 8 << 20
	same := func(n int) func(int) []byte {
		p := bytes.Repeat([]byte("x"), n)
		return func(int) []byte { return p }
	}
	// distinct payloads of the given size that still compress well
	different := func(n int) func(int) []byte {
		return func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8), byte(i >> 16)}, n/3+1)[:n] }
	}
	for _, tc := range []struct {
		name       string
		bomb       bomb
		allocLimit uint64 // what the refusal may allocate
		decodes    uint64 // roughly what decoding the group would allocate
	}{
		// 100,000 records at 256 bytes are over the limit before anything is read.
		{"too many rows", bomb{rows: 100_000, payload: same(0)}, 4 << 20, 200 << 20},
		// One batch of 4096 payloads of 256 KiB is a gigabyte.
		{"one batch of large payloads", bomb{rows: 10_000, payload: same(256 << 10)}, 8 * limit, 1 << 30},
		// Each batch is small, the group is not: 30,000 payloads of 1000 bytes are 30 MB.
		{"many small payloads", bomb{rows: 30_000, payload: same(1000)}, 8 * limit, 120 << 20},
		// Values just small enough to be copied into chunks, 4096 of them in a
		// batch: 60 MB for a limit of 8 MiB.
		{"values just under the chunk threshold", bomb{rows: 10_000, payload: same(15 << 10)}, 4 * limit, 150 << 20},
		// Every row another host, producer and boot id: each distinct text costs its
		// length and a map entry, so 30,000 rows pass the limit with no payload at all.
		{"distinct hosts, producers and boots", bomb{rows: 30_000, payload: same(0), distinct: true}, 8 * limit, 150 << 20},
		// Thousands of pages of a few values each: the batch spans them all.
		{"many small pages", bomb{rows: 20_000, payload: different(400), pageSize: 1024, plainPay: true}, 8 * limit, 100 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bombFileWith(t, tc.bomb)
			if len(data) > 2<<20 {
				t.Fatalf("the test file is %d bytes, want a small one", len(data))
			}
			var got []store.Record
			var err error
			n := allocated(func() {
				var r *activity.Reader
				if r, err = openFile(data, activity.ReaderOptions{MaxRowGroupBytes: limit}); err == nil {
					got, err = drain(r)
				}
			})
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), fmt.Sprint(limit)) {
				t.Fatalf("error %v, want one wrapping ErrFormat that names the limit of %d", err, limit)
			}
			if len(got) != 0 {
				t.Errorf("%d records were returned from a row group over the limit", len(got))
			}
			t.Logf("refusing the group allocated %d bytes (limit %d; decoding it would take about %d)", n, tc.allocLimit, tc.decodes)
			if n > tc.allocLimit {
				t.Errorf("refusing the group allocated %d bytes, want at most %d (decoding it would take %d)", n, tc.allocLimit, tc.decodes)
			}
		})
	}
}

// dictionaryBomb is a file whose payload column has a dictionary page that
// claims more values than its bytes can hold, and a data page whose first index
// is below the claim and far beyond the real dictionary. The column is a byte
// array (payload, four bytes a value at the least) or an int64 (ttl_ns, eight). The Parquet library
// allocates for the largest index it sees, from the claim, before it looks at
// the dictionary: this is how a few kilobytes make it allocate gigabytes.
// The file is written honestly and the two headers are then patched.
func dictionaryBomb(tb fataler, column string, index int) []byte {
	tb.Helper()
	const (
		honest = 9000      // distinct payloads, so a dictionary of 9000 values
		claim  = 1_048_575 // the largest claim that takes as many header bytes as 9000
		width  = 20        // bits of an index that can reach the claim
	)
	data := bombFileWith(tb, bomb{
		rows: honest, dictionaryColumn: column,
		payload: func(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) },
	})
	pf, err := file2reader(data)
	if err != nil {
		tb.Fatal(err)
	}
	cc, err := pf.MetaData().RowGroup(0).ColumnChunk(index)
	if err != nil {
		tb.Fatal(err)
	}
	if !cc.HasDictionaryPage() {
		tb.Fatalf("the %s column has no dictionary page", column)
	}

	// The dictionary page header: replace the number of values.
	zigzag := func(n int) []byte { return binary.AppendUvarint(nil, uint64(n)<<1) }
	at := int(cc.DictionaryPageOffset())
	window := data[at : at+64]
	k := bytes.Index(window, zigzag(honest))
	if k < 0 || len(zigzag(honest)) != len(zigzag(claim)) {
		tb.Fatalf("cannot find the number of values in the dictionary page header %x", window)
	}
	copy(window[k:], zigzag(claim))

	// The data page: the first index of the bit-packed run, at the bit width
	// 14 of a dictionary of 9000, is set to the largest value of the wider
	// width 20 that the claim allows, and the width byte follows. The run is
	// then read at the wrong width, which only matters after the allocation.
	first := make([]byte, 0, 16)
	var bits uint64
	for v := range 4 {
		bits |= uint64(v) << (14 * v)
	}
	for range 7 {
		first = append(first, byte(bits))
		bits >>= 8
	}
	dataAt := int(cc.DataPageOffset())
	region := data[dataAt : dataAt+256]
	p := bytes.Index(region, first)
	if p < 3 {
		tb.Fatalf("cannot find the indexes of the data page in %x", region)
	}
	w := bytes.LastIndexByte(region[:p], 14)
	if w < 0 {
		tb.Fatalf("cannot find the bit width before the indexes in %x", region[:p])
	}
	region[w] = width
	last := claim - 1
	region[p] = byte(last)
	region[p+1] = byte(last >> 8)
	region[p+2] = region[p+2]&0xf0 | byte(last>>16)&0x0f
	return data
}

// The library sizes a dictionary from the number of values its page header
// claims and allocates for any index below that, before it compares the claim
// with the page. A dictionary page that cannot hold the values it claims is
// refused before the library sees it.
//
// The test is not parallel: it measures the allocation of the whole process.
func TestDictionaryPageThatClaimsTooManyValues(t *testing.T) {
	for _, tc := range []struct {
		column string
		index  int
		bytes  int // what the library would allocate for the claimed dictionary, for the message
	}{
		{"payload", 11, 25 << 20},
		{"ttl_ns", 9, 8 << 20},
	} {
		t.Run(tc.column, func(t *testing.T) {
			data := dictionaryBomb(t, tc.column, tc.index)
			var got []store.Record
			var err error
			n := allocated(func() {
				var r *activity.Reader
				if r, err = openFile(data, activity.ReaderOptions{}); err == nil {
					got, err = drain(r)
				}
			})
			t.Logf("a file of %d bytes: error %v, %d bytes allocated", len(data), err, n)
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), "column "+tc.column) ||
				!strings.Contains(err.Error(), "dictionary page") || !strings.Contains(err.Error(), "claims") {
				t.Fatalf("error %v, want one wrapping ErrFormat that says the dictionary page of %s claims more values than it holds", err, tc.column)
			}
			if len(got) != 0 {
				t.Errorf("%d records were returned", len(got))
			}
			// The records slice for 9,000 rows is 2 MB of what is allocated.
			if n > 6<<20 {
				t.Errorf("refusing the file allocated %d bytes, want under 6 MiB (the library would allocate about %d MB more for the claimed dictionary)", n, tc.bytes>>20)
			}
		})
	}
}

// The reader lets the Parquet library read a page of at most MaxPayload and
// 2 MiB, however large the row group limit: a writer closes a page after about
// 1 MiB. A page of 40 MB is refused by the library's limit, not decoded.
func TestPageOverThePageLimit(t *testing.T) {
	t.Parallel()

	payload := func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 20<<10) }
	data := bombFileWith(t, bomb{rows: 1000, payload: payload, pageSize: 64 << 20, plainPay: true})
	err := failure(t, data, activity.ReaderOptions{})
	if !strings.Contains(err.Error(), "uncompressed page size") {
		t.Errorf("error %q, want the library's refusal of a page over the limit", err)
	}
}

// decodedSize is the size the reader counts for the records of one row group:
// 256 bytes a record, the payloads, and each distinct producer, relation, boot
// id and fingerprint text once, with the cost of its entry in the map that holds
// it. Like the reader, it keeps one set of fingerprint texts and one of names.
// entryCost is the 96 bytes the package documentation gives for the entry of a
// text in its map. It is written out here, not read from the package.
const entryCost = 96

func decodedSize(recs []store.Record) int64 {
	fingerprints, names := map[string]bool{}, map[string]bool{}
	once := func(set map[string]bool, s string) int64 {
		if set[s] {
			return 0
		}
		set[s] = true
		return int64(len(s)) + entryCost
	}
	var n int64
	for _, r := range recs {
		n += activity.RecordCost + int64(len(r.Payload))
		n += once(fingerprints, r.Subject.A.String())
		if r.Subject.Kind == store.SubjectEdge {
			n += once(fingerprints, r.Subject.B.String()) + once(names, string(r.Subject.Relation))
		}
		n += once(names, string(r.Producer))
		if r.Boot != "" {
			n += once(names, r.Boot)
		}
	}
	return n
}

func TestRecordCostCoversARecord(t *testing.T) {
	t.Parallel()

	if size := reflect.TypeFor[store.Record]().Size(); size > activity.RecordCost {
		t.Errorf("a store.Record is %d bytes, more than the %d the reader counts for it", size, activity.RecordCost)
	}
}

// The limit is exact: a row group is read with a limit equal to its decoded
// size and refused with one byte less.
func TestDecodedLimitIsExact(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(11, 12))
	recs := sample(t, 60)
	for i := range recs {
		if recs[i].Kind == lifecycle.Observe {
			recs[i].Payload = randomBytes(rng, 300)
		}
	}
	data := writeFile(t, recs, activity.WriterOptions{})
	size := decodedSize(recs)

	got := readFile(t, data, activity.ReaderOptions{MaxRowGroupBytes: size})
	requireSame(t, recs, got)

	r, err := openFile(data, activity.ReaderOptions{MaxRowGroupBytes: size - 1})
	if err == nil {
		var read []store.Record
		read, err = drain(r)
		if len(read) != 0 {
			t.Errorf("%d records returned from a row group over the limit", len(read))
		}
	}
	if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), fmt.Sprintf("limit of %d", size-1)) {
		t.Errorf("error %v, want one wrapping ErrFormat that names the limit of %d", err, size-1)
	}
}

// A row group the writer closes at its limit is always read with a reader limit
// of 5.1 times that: the decoded size is under 5.1 times the buffered size.
// The records are the worst cases: the smallest records, and records that are
// mostly distinct text.
func TestWriterGroupsFitTheReaderLimit(t *testing.T) {
	t.Parallel()

	const groupBytes = 64 << 10
	rack := fingerprint(t, catalog.Rack, catalog.RackID, "r")
	host := fingerprint(t, catalog.Host, catalog.HostID, "h")
	rng := rand.New(rand.NewPCG(13, 14))
	var worst float64
	for _, tc := range []struct {
		name  string
		count int
		make  func(i int) store.Record
	}{
		{"deletes of one entity", 4000, func(i int) store.Record {
			return store.Record{Layer: catalog.L0, Subject: store.EntitySubject(rack), Producer: "p", EventTime: epoch, Kind: lifecycle.Delete}
		}},
		{"deletes of edges between distinct entities, distinct producers and relations", 4000, func(i int) store.Record {
			var from, to [identity.FingerprintBytes]byte
			binary.BigEndian.PutUint64(from[:], uint64(i))
			binary.BigEndian.PutUint64(to[:], uint64(i)+1<<40)
			a, _ := identity.FingerprintFromHash("a", from) // the shortest type name there is
			b, _ := identity.FingerprintFromHash("a", to)
			return store.Record{
				Layer: catalog.L1, Subject: store.EdgeSubject(a, b, catalog.RelationType(fmt.Sprint(i))),
				Producer: lifecycle.Producer(fmt.Sprint(i)), EventTime: epoch, Kind: lifecycle.Delete,
			}
		}},
		{"observations of distinct hosts with distinct producers and boots", 4000, func(i int) store.Record {
			h := fingerprint(t, catalog.Host, catalog.HostID, fmt.Sprint(i))
			return store.Record{
				Layer: catalog.L1, Subject: store.EntitySubject(h), Producer: lifecycle.Producer(fmt.Sprint(i)),
				EventTime: epoch, Kind: lifecycle.Observe, Boot: fmt.Sprint(i),
			}
		}},
		{"deletes of edges", 4000, func(i int) store.Record {
			return store.Record{Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "p", EventTime: epoch, Kind: lifecycle.Delete}
		}},
		{"observations with distinct producers", 4000, func(i int) store.Record {
			return store.Record{Layer: catalog.L0, Subject: store.EntitySubject(rack), Producer: lifecycle.Producer(fmt.Sprintf("producer-%d", i)), EventTime: epoch, Kind: lifecycle.Observe}
		}},
		{"observations with payloads", 400, func(i int) store.Record {
			return store.Record{Layer: catalog.L0, Subject: store.EntitySubject(rack), Producer: "p", EventTime: epoch, Kind: lifecycle.Observe, Payload: randomBytes(rng, 700)}
		}},
		{"observations with one large payload each", 30, func(i int) store.Record {
			return store.Record{Layer: catalog.L0, Subject: store.EntitySubject(rack), Producer: "p", EventTime: epoch, Kind: lifecycle.Observe, Payload: randomBytes(rng, 40<<10)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recs []store.Record
			for i := range tc.count {
				r := tc.make(i)
				r.Seq = uint64(i + 1)
				recs = append(recs, r)
			}
			data := writeFile(t, recs, activity.WriterOptions{RowGroupBytes: groupBytes})
			got := readFile(t, data, activity.ReaderOptions{MaxRowGroupBytes: groupBytes * 51 / 10})
			requireSame(t, recs, got)

			// The ratio of what the reader counts to what the writer buffers.
			ratio := float64(decodedSize(recs)) / float64(bufferedSize(recs))
			t.Logf("decoded size is %.2f times the buffered size", ratio)
			worst = max(worst, ratio)
		})
	}
	if worst < 4 || worst >= 5.1 {
		t.Errorf("the worst ratio of decoded to buffered size is %.2f, want from 4 to under 5.1: the bound of the documentation is meant to be tight", worst)
	}
}

// bufferedSize is the size the writer counts for the records of a row group:
// 64 bytes a record and the length of each of its texts and its payload.
func bufferedSize(recs []store.Record) int64 {
	var n int64
	for _, r := range recs {
		kind := "entity"
		if r.Subject.Kind == store.SubjectEdge {
			kind = "edge"
		}
		n += 64 + int64(len(r.Layer.String())+len(kind)+len(r.Subject.A.String())+len(r.Subject.B.String())+
			len(r.Subject.Relation)+len(r.Producer)+len(r.Kind.String())+len(r.Payload)+len(r.Boot))
	}
	return n
}

// The footer's first and last Seq are checked against the rows of the group
// that holds them, before that group's records are returned.
func TestSeqBoundsAreCheckedBeforeTheGroupIsReturned(t *testing.T) {
	t.Parallel()

	const groupRows = 7
	recs := sample(t, 40) // six row groups; seq 3 to 120
	for _, tc := range []struct {
		name   string
		key    string
		value  string
		groups int // row groups that are returned before the error
	}{
		{"min_seq too high", "toposhift.activity.min_seq", "6", 0},
		{"min_seq too low", "toposhift.activity.min_seq", "2", 0},
		{"max_seq too high", "toposhift.activity.max_seq", "121", 5},
		{"max_seq too low", "toposhift.activity.max_seq", "117", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := tamperedFile(t, recs, func(kv map[string]string) { kv[tc.key] = tc.value })
			r, err := openFile(data, activity.ReaderOptions{})
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			got, err := drain(r)
			if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), tc.key[len("toposhift.activity."):]) {
				t.Fatalf("error %v, want one wrapping ErrFormat that names %s", err, tc.key)
			}
			if want := tc.groups * groupRows; len(got) != want {
				t.Errorf("%d records were returned, want %d: the group that disagrees with the footer must not be returned", len(got), want)
			}
		})
	}
}
