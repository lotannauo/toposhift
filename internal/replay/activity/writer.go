package activity

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/lotannauo/toposhift/internal/store"
)

// MaxPayload is the largest payload a record in an activity file may carry.
const MaxPayload = 16 << 20

// ErrClosed is wrapped by the error a Writer returns after Close.
var ErrClosed = errors.New("activity writer closed")

const (
	// DefaultRowGroupRows is the row group size in records when
	// [WriterOptions.RowGroupRows] is zero.
	DefaultRowGroupRows = 1 << 16
	// DefaultRowGroupBytes is the buffered size at which a row group is
	// closed when [WriterOptions.RowGroupBytes] is zero.
	DefaultRowGroupBytes = 32 << 20
)

// MaxWriterRowGroupBytes is the largest [WriterOptions.RowGroupBytes]. The
// reader's default limit on a row group, [DefaultMaxRowGroupBytes], is eight
// times this: the decoded size of a row group the writer closes at the limit is
// under 5.1 times its buffered size (see the package documentation), so a file
// written with any allowed options reads with the default reader options.
const MaxWriterRowGroupBytes = 32 << 20

// perRecordBytes is what a record counts for in a row group's buffered size,
// before the lengths of its strings and payload.
const perRecordBytes = 64

// WriterOptions configures a [Writer].
type WriterOptions struct {
	// RowGroupRows is the number of records in a row group. Zero means
	// [DefaultRowGroupRows].
	RowGroupRows int
	// RowGroupBytes closes a row group once its buffered size reaches it.
	// Zero means [DefaultRowGroupBytes]; more than [MaxWriterRowGroupBytes] is
	// refused. A record larger than this gets a row group of its own.
	RowGroupBytes int64
}

// sink passes bytes to the caller's writer, hides the writer's Close from
// the Parquet library, and keeps the first error, which the library does not
// reliably report as it was.
type sink struct {
	w   io.Writer
	err error
}

func (s *sink) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.err = err
	}
	return n, err
}

// Writer writes an activity file. It is not safe for concurrent use.
type Writer struct {
	sink *sink
	fw   *file.Writer
	dig  *digester

	maxRows  int
	maxBytes int64

	// The row group being filled, one slice per column. The optional
	// columns hold only their present values, with a definition level (1
	// present, 0 null) for every row.
	n                                      int
	size                                   int64
	basis                                  []int32
	seq, event, ttl, through               []int64
	layer, subject, source, producer, kind []parquet.ByteArray
	payload, target, relation              []parquet.ByteArray
	boot                                   []parquet.ByteArray
	targetDef, relationDef, throughDef     []int16
	bootDef                                []int16
	groupSums                              []string // digest of each row group written, in order
	last, minSeq, maxSeq                   uint64
	records                                int64
	err                                    error // the first failure; returned by every later call
	closed                                 bool
	unchecked                              bool                    // set only by tests: skip the checks of Write
	tamper                                 func(map[string]string) // set only by tests: change the footer metadata
}

// NewWriter starts an activity file on w. Nothing is written until the first
// row group is full or Close is called, except the file's leading magic
// bytes.
func NewWriter(w io.Writer, opts WriterOptions) (*Writer, error) {
	if opts.RowGroupRows < 0 || opts.RowGroupBytes < 0 {
		return nil, fmt.Errorf("activity: negative row group size in %+v: %w", opts, store.ErrInvalid)
	}
	if opts.RowGroupBytes > MaxWriterRowGroupBytes {
		return nil, fmt.Errorf("activity: RowGroupBytes %d is over the limit of %d: %w", opts.RowGroupBytes, MaxWriterRowGroupBytes, store.ErrInvalid)
	}
	if opts.RowGroupRows == 0 {
		opts.RowGroupRows = DefaultRowGroupRows
	}
	if opts.RowGroupBytes == 0 {
		opts.RowGroupBytes = DefaultRowGroupBytes
	}
	root, err := buildSchema()
	if err != nil {
		return nil, err
	}
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryFor("payload", false),
		parquet.WithStatsFor("payload", false),
		parquet.WithSortingColumns([]parquet.SortingColumn{{ColumnIdx: colSeq}}),
	)
	s := &sink{w: w}
	fw, err := file.NewParquetWriterWithError(s, root, file.WithWriterProps(props))
	if err != nil {
		if s.err != nil {
			err = s.err
		}
		return nil, fmt.Errorf("activity: starting the file: %w", err)
	}
	return &Writer{
		sink:     s,
		fw:       fw,
		dig:      newDigester(),
		maxRows:  opts.RowGroupRows,
		maxBytes: opts.RowGroupBytes,
	}, nil
}

// fail keeps the first error and returns it wrapped with what was being done.
func (w *Writer) fail(doing string, err error) error {
	if w.err == nil {
		if w.sink.err != nil {
			err = w.sink.err
		}
		w.err = fmt.Errorf("activity: %s: %w", doing, err)
	}
	return w.err
}

// Write appends one record. It refuses, with an error wrapping
// [store.ErrInvalid] and writing nothing, a record that [store.Record.Validate]
// refuses, whose Seq is not above the previous record's, whose payload exceeds
// [MaxPayload], whose producer or relation is longer than [MaxText] bytes, or
// whose producer, relation or boot id is not valid UTF-8 (a Parquet string must
// be). A refused record leaves the writer usable.
//
// Once a write has failed, the file is incomplete and must be discarded; every
// later call returns that first error.
func (w *Writer) Write(r store.Record) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("activity: write: %w", ErrClosed)
	}
	if !w.unchecked {
		if err := w.check(r); err != nil {
			return err
		}
	}
	row := toRow(r)
	return w.add(&row)
}

// check applies the rules of Write.
func (w *Writer) check(r store.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Seq <= w.last {
		return fmt.Errorf("record seq %d is not above the previous record's %d: %w", r.Seq, w.last, store.ErrInvalid)
	}
	if len(r.Payload) > MaxPayload {
		return fmt.Errorf("record seq %d: payload of %d bytes is over the limit of %d: %w", r.Seq, len(r.Payload), MaxPayload, store.ErrInvalid)
	}
	if len(r.Producer) > MaxText || len(r.Subject.Relation) > MaxText {
		return fmt.Errorf("record seq %d: producer and relation are at most %d bytes: %w", r.Seq, MaxText, store.ErrInvalid)
	}
	if !utf8.ValidString(string(r.Producer)) || !utf8.ValidString(string(r.Subject.Relation)) || !utf8.ValidString(r.Boot) {
		return fmt.Errorf("record seq %d: producer, relation and boot must be valid UTF-8: %w", r.Seq, store.ErrInvalid)
	}
	return nil
}

// row is a record as the columns hold it. A target or relation that is not
// present is null.
type row struct {
	canonRow
	hasTarget, hasRelation bool
}

func subjectKindName(k store.SubjectKind) string {
	switch k {
	case store.SubjectEntity:
		return "entity"
	case store.SubjectEdge:
		return "edge"
	}
	return "SubjectKind(" + strconv.Itoa(int(k)) + ")"
}

// toRow converts a record to the text and integer forms of the schema.
func toRow(r store.Record) row {
	out := row{canonRow: canonRow{
		seq:         r.Seq,
		eventNS:     r.EventTime.UnixNano(),
		basis:       int32(r.EventTimeBasis),
		layer:       r.Layer.String(),
		subjectKind: subjectKindName(r.Subject.Kind),
		source:      r.Subject.A.String(),
		producer:    string(r.Producer),
		kind:        r.Kind.String(),
		ttlNS:       int64(r.TTL),
		payload:     r.Payload,
	}}
	if r.Subject.Kind == store.SubjectEdge {
		out.hasTarget, out.hasRelation = true, true
		out.target = r.Subject.B.String()
		out.relation = string(r.Subject.Relation)
	}
	if r.Boot != "" {
		out.hasBoot, out.boot = true, r.Boot
	}
	if !r.Through.IsZero() {
		out.hasThrough = true
		out.throughNS = r.Through.UnixNano()
	}
	return out
}

// interned holds the values the enumeration columns repeat in every row, so
// that the buffer does not allocate them again.
var interned = func() map[string]parquet.ByteArray {
	m := make(map[string]parquet.ByteArray)
	for _, s := range []string{"L0", "L1", "L2", "L3", "entity", "edge", "observe", "delete"} {
		m[s] = parquet.ByteArray(s)
	}
	return m
}()

func byteArray(s string) parquet.ByteArray {
	if b, ok := interned[s]; ok {
		return b
	}
	return parquet.ByteArray(s)
}

// add buffers one row, closing the row group before it if the row would
// overflow the size limit, and after it if the group is full.
func (w *Writer) add(r *row) error {
	size := int64(perRecordBytes + len(r.layer) + len(r.subjectKind) + len(r.source) + len(r.target) +
		len(r.relation) + len(r.producer) + len(r.kind) + len(r.payload) + len(r.boot))
	if w.n > 0 && w.size+size > w.maxBytes {
		if err := w.flush(); err != nil {
			return err
		}
	}

	w.seq = append(w.seq, int64(r.seq))
	w.event = append(w.event, r.eventNS)
	w.basis = append(w.basis, r.basis)
	w.layer = append(w.layer, byteArray(r.layer))
	w.subject = append(w.subject, byteArray(r.subjectKind))
	w.source = append(w.source, byteArray(r.source))
	if r.hasTarget {
		w.target = append(w.target, byteArray(r.target))
		w.targetDef = append(w.targetDef, 1)
	} else {
		w.targetDef = append(w.targetDef, 0)
	}
	if r.hasRelation {
		w.relation = append(w.relation, byteArray(r.relation))
		w.relationDef = append(w.relationDef, 1)
	} else {
		w.relationDef = append(w.relationDef, 0)
	}
	w.producer = append(w.producer, byteArray(r.producer))
	w.kind = append(w.kind, byteArray(r.kind))
	w.ttl = append(w.ttl, r.ttlNS)
	if r.hasThrough {
		w.through = append(w.through, r.throughNS)
		w.throughDef = append(w.throughDef, 1)
	} else {
		w.throughDef = append(w.throughDef, 0)
	}
	if r.hasBoot {
		w.boot = append(w.boot, byteArray(r.boot))
		w.bootDef = append(w.bootDef, 1)
	} else {
		w.bootDef = append(w.bootDef, 0)
	}
	// The payload is copied: the caller may reuse its slice.
	w.payload = append(w.payload, parquet.ByteArray(bytes.Clone(r.payload)))
	w.n++
	w.size += size

	w.dig.add(&r.canonRow)
	if w.records == 0 || r.seq < w.minSeq {
		w.minSeq = r.seq
	}
	if w.records == 0 || r.seq > w.maxSeq {
		w.maxSeq = r.seq
	}
	w.last = r.seq
	w.records++

	if w.n >= w.maxRows || w.size >= w.maxBytes {
		return w.flush()
	}
	return nil
}

// flush writes the buffered rows as one row group and empties the buffer.
func (w *Writer) flush() error {
	if w.n == 0 {
		return nil
	}
	rg, err := w.fw.AppendRowGroupChecked()
	if err != nil {
		return w.fail("starting a row group", err)
	}
	for i := range numColumns {
		cw, err := rg.NextColumn()
		if err != nil {
			return w.fail("starting column "+columns[i].name, err)
		}
		switch c := cw.(type) {
		case *file.Int32ColumnChunkWriter:
			_, err = c.WriteBatch(w.basis, nil, nil)
		case *file.Int64ColumnChunkWriter:
			var values []int64
			var def []int16
			switch i {
			case colSeq:
				values = w.seq
			case colEventTime:
				values = w.event
			case colTTL:
				values = w.ttl
			case colThrough:
				values, def = w.through, w.throughDef
			}
			_, err = c.WriteBatch(values, def, nil)
		case *file.ByteArrayColumnChunkWriter:
			var values []parquet.ByteArray
			var def []int16
			switch i {
			case colLayer:
				values = w.layer
			case colSubjectKind:
				values = w.subject
			case colSource:
				values = w.source
			case colTarget:
				values, def = w.target, w.targetDef
			case colRelation:
				values, def = w.relation, w.relationDef
			case colProducer:
				values = w.producer
			case colKind:
				values = w.kind
			case colPayload:
				values = w.payload
			case colBoot:
				values, def = w.boot, w.bootDef
			}
			_, err = c.WriteBatch(values, def, nil)
		default:
			err = fmt.Errorf("unexpected column writer %T", cw)
		}
		if err != nil {
			return w.fail("writing column "+columns[i].name, err)
		}
	}
	if err := rg.Close(); err != nil {
		return w.fail("closing a row group", err)
	}

	w.groupSums = append(w.groupSums, w.dig.sumHex())
	w.dig.reset()
	w.n, w.size = 0, 0
	w.seq, w.event, w.ttl, w.through, w.basis = w.seq[:0], w.event[:0], w.ttl[:0], w.through[:0], w.basis[:0]
	for _, s := range []*[]parquet.ByteArray{&w.layer, &w.subject, &w.source, &w.producer, &w.kind, &w.payload, &w.target, &w.relation, &w.boot} {
		clear(*s)
		*s = (*s)[:0]
	}
	w.targetDef, w.relationDef, w.throughDef, w.bootDef = w.targetDef[:0], w.relationDef[:0], w.throughDef[:0], w.bootDef[:0]
	return nil
}

// Close writes the last row group and the footer, with the metadata the
// package documentation lists. It does not close the underlying writer. A file
// is complete only after Close returns nil.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("activity: close: %w", ErrClosed)
	}
	w.closed = true
	if err := w.flush(); err != nil {
		return err
	}

	kv := map[string]string{
		keyVersion: strconv.Itoa(FormatVersion),
		keyRecords: strconv.FormatInt(w.records, 10),
		keyDigest:  fileDigest(w.groupSums),
		keyGroups:  strings.Join(w.groupSums, ","),
	}
	if w.records > 0 {
		kv[keyMinSeq] = strconv.FormatUint(w.minSeq, 10)
		kv[keyMaxSeq] = strconv.FormatUint(w.maxSeq, 10)
	}
	if w.tamper != nil {
		w.tamper(kv)
	}
	for _, k := range slices.Sorted(maps.Keys(kv)) {
		if err := w.fw.AppendKeyValueMetadata(k, kv[k]); err != nil {
			return w.fail("writing the footer metadata", err)
		}
	}
	if err := w.fw.Close(); err != nil {
		return w.fail("writing the footer", err)
	}
	return nil
}
