package activity

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// ErrFormat is wrapped by every error that reports an activity file this
// package cannot read: not Parquet, another schema or version, metadata that
// disagrees with the rows, a record that is not valid, sequence numbers that
// do not rise, or a file damaged in any way the reader can see.
var ErrFormat = errors.New("invalid activity file")

const (
	// DefaultBatchRows is the number of rows decoded per column at a time when
	// [ReaderOptions.BatchRows] is zero.
	DefaultBatchRows = 4096
	// DefaultMaxRowGroupBytes is the largest row group the reader decodes when
	// [ReaderOptions.MaxRowGroupBytes] is zero.
	DefaultMaxRowGroupBytes = 256 << 20
)

// recordCost is what every record of a row group counts for in the decoded
// size of the group, before the length of its payload and of the strings that
// are not already held for the group. It is an upper bound on the in-memory size
// of a [store.Record] (232 bytes on a 64-bit machine) with room for the slice
// that holds it.
// The reader refuses a row group whose decoded size passes
// [ReaderOptions.MaxRowGroupBytes]; see the package documentation for how that
// relates to what the writer buffers.
const recordCost = 256

// MaxText is the longest producer or relation, in bytes, that a record in an
// activity file may carry. (A fingerprint text is at most
// [catalog.MaxNameLen] + 33 bytes, and a boot id at most [store.MaxBootLen].)
// [Writer.Write] refuses a longer one and the reader refuses a file that holds
// one, so that no text field can make a row group larger than the limit the
// reader holds it to.
const MaxText = 64 << 10

// entryCost is what each distinct text a row group holds once (a producer, a
// relation, a boot id or a fingerprint) counts for in the decoded size of the
// group, besides its length: the cost of its entry in the map that holds it.
const entryCost = 96

// maxFingerprintText is the longest fingerprint text: a type name, a colon and
// 32 hex digits.
const maxFingerprintText = catalog.MaxNameLen + 1 + 2*identity.FingerprintBytes

// pageLimit is the largest page, compressed or uncompressed, that the reader
// lets the Parquet library read: a data page the writer closes holds about 1 MiB
// of values and the one value that took it over, and the largest value is a
// payload of [MaxPayload]. It is lower than [ReaderOptions.MaxRowGroupBytes]
// when that is large.
const pageLimit = MaxPayload + 2<<20

// ReaderOptions configures a [Reader].
type ReaderOptions struct {
	// BatchRows is the number of rows decoded per column at a time. Zero
	// means [DefaultBatchRows].
	BatchRows int
	// MaxRowGroupBytes is the largest row group the reader will decode: by
	// the encoded sizes in the footer, and by the decoded size of its records
	// (see the package documentation). Zero means [DefaultMaxRowGroupBytes].
	MaxRowGroupBytes int64
}

// Info describes the file, from its footer.
type Info struct {
	// Version is the format version.
	Version int
	// Records is the number of records in the file.
	Records int64
	// MinSeq and MaxSeq are the first and the last sequence number. Both are
	// zero when Records is zero.
	MinSeq, MaxSeq uint64
	// RowGroups is the number of row groups.
	RowGroups int
}

// internedFP is a fingerprint parsed once for a row group, with its text.
type internedFP struct {
	fp   identity.Fingerprint
	text string
}

// group is the row group being decoded.
type group struct {
	index     int
	remaining int64 // rows of the group not yet decoded into a batch
	cols      [numColumns]file.ColumnChunkReader
}

// Reader reads an activity file record by record. It is not safe for
// concurrent use.
type Reader struct {
	fr     *file.Reader
	props  *parquet.ReaderProperties
	info   Info
	digest string // from the footer

	batchRows int
	maxGroup  int64

	nextGroup int
	cur       *group

	// The row group being returned: decoded, checked and verified against its
	// digest as a whole before the first of its records is returned.
	recs []store.Record
	at   int

	// The decoded batch: for each column the values of up to batchRows
	// rows. The optional columns are spread over the rows (a null row holds
	// nothing) and keep their definition levels.
	n    int
	ints [numColumns][]int64
	i32  [numColumns][]int32
	strs [numColumns][]parquet.ByteArray
	defs [numColumns][]int16

	// What the row group being decoded has cost so far, and the values it holds
	// once for all its records.
	cost      int64
	fps       map[string]internedFP
	names     map[string]string
	chunk     []byte // the chunk the byte array values of a batch are copied into
	chunkSize int
	held      int64 // bytes allocated for the copies of the batch, all columns

	groupWant []string // digests of the row groups, from the footer
	groupGot  []string // digests of the row groups verified so far

	rows    int64 // rows decoded so far
	first   uint64
	prev    uint64
	dig     *digester // of the row group being decoded
	err     error     // sticky; io.EOF once the file has been read to its end
	closed  bool
	groupAt int // row group of the batch being decoded, for messages
}

// NewReader opens the activity file held by r, of the given size. It reads and
// checks the footer: the schema, the version and the metadata. Records are
// decoded one row group at a time as Next asks for them.
func NewReader(r io.ReaderAt, size int64, opts ReaderOptions) (*Reader, error) {
	if opts.BatchRows < 0 || opts.MaxRowGroupBytes < 0 {
		return nil, fmt.Errorf("activity: negative reader option in %+v: %w", opts, store.ErrInvalid)
	}
	if opts.BatchRows == 0 {
		opts.BatchRows = DefaultBatchRows
	}
	if opts.MaxRowGroupBytes == 0 {
		opts.MaxRowGroupBytes = DefaultMaxRowGroupBytes
	}
	rd := &Reader{
		chunkSize: int(min(64<<10, max(opts.MaxRowGroupBytes/64, 256))),
		batchRows: opts.BatchRows, maxGroup: opts.MaxRowGroupBytes, dig: newDigester(),
		fps: make(map[string]internedFP), names: make(map[string]string),
	}
	if err := rd.guard("opening the file", func() error { return rd.open(r, size) }); err != nil {
		return nil, err
	}
	return rd, nil
}

// formatErr builds an error that wraps [ErrFormat].
func formatErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrFormat, fmt.Sprintf(format, args...))
}

// libraryErr wraps an error from the Parquet library, keeping it reachable.
func libraryErr(doing string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrFormat, doing, err)
}

// guard runs f, turning a panic in the Parquet library into an error. The
// library panics on some damaged files (a nil pointer in the decoder of a
// corrupted page, for one), and a file is data from outside.
func (r *Reader) guard(doing string, f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = formatErr("the Parquet library failed on this file (%s): %v", doing, p)
		}
	}()
	return f()
}

// open reads the footer and checks everything the footer says.
func (r *Reader) open(src io.ReaderAt, size int64) error {
	// The footer is 4 bytes of length and the closing magic; with the opening
	// magic, no Parquet file is shorter than 12 bytes.
	if size < 12 {
		return formatErr("a file of %d bytes is too short to be Parquet", size)
	}
	var head [4]byte
	if _, err := src.ReadAt(head[:], 0); err != nil {
		return libraryErr("reading the first bytes", err)
	}
	if string(head[:]) != "PAR1" {
		return formatErr("the file does not start with the Parquet magic bytes")
	}
	props := parquet.NewReaderProperties(nil)
	props.BufferedStreamEnabled = true
	// A page cannot be larger than the row group that holds it, nor than the
	// largest page the writer closes.
	props.MaxCompressedPageSize = min(r.maxGroup, pageLimit)
	props.MaxUncompressedPageSize = min(r.maxGroup, pageLimit)
	r.props = props
	fr, err := file.NewParquetReader(io.NewSectionReader(src, 0, size), file.WithReadProps(props))
	if err != nil {
		return libraryErr("reading the footer", err)
	}
	r.fr = fr
	return r.checkFooter(fr.MetaData())
}

// checkFooter checks the schema and the key-value metadata against the row
// groups, and fills in the file's Info.
func (r *Reader) checkFooter(md *metadata.FileMetaData) error {
	if md.IsSetEncryptionAlgorithm() {
		return formatErr("the file is encrypted")
	}
	if err := checkSchema(md.Schema); err != nil {
		return formatErr("schema: %v", err)
	}

	kv := make(map[string]string)
	for _, e := range md.KeyValueMetadata() {
		if e == nil || !strings.HasPrefix(e.Key, keyPrefix) {
			continue
		}
		if _, dup := kv[e.Key]; dup {
			return formatErr("footer metadata %s appears twice", e.Key)
		}
		if e.Value == nil {
			return formatErr("footer metadata %s has no value", e.Key)
		}
		kv[e.Key] = *e.Value
	}

	version, ok := kv[keyVersion]
	if !ok {
		return formatErr("footer metadata %s is missing: not an activity file", keyVersion)
	}
	if version != strconv.Itoa(FormatVersion) {
		return formatErr("format version %q is not supported (this package reads version %d)", version, FormatVersion)
	}
	r.info.Version = FormatVersion

	records, err := parseDecimal(kv, keyRecords)
	if err != nil {
		return err
	}
	if records > 1<<62 {
		return formatErr("footer metadata %s is %d, which is not a record count", keyRecords, records)
	}
	r.info.Records = int64(records)

	if md.NumRows != r.info.Records {
		return formatErr("footer metadata %s says %d records, the footer counts %d rows", keyRecords, records, md.NumRows)
	}
	var total int64
	groups := md.NumRowGroups()
	for i := range groups {
		n := md.RowGroup(i).NumRows()
		if n <= 0 || n > 1<<62 {
			return formatErr("row group %d has %d rows", i, n)
		}
		total += n
		if total > 1<<62 {
			return formatErr("the row groups hold more rows than any file can")
		}
	}
	if total != r.info.Records {
		return formatErr("footer metadata %s says %d records, the row groups hold %d", keyRecords, records, total)
	}
	r.info.RowGroups = groups

	minSeq, hasMin := kv[keyMinSeq]
	maxSeq, hasMax := kv[keyMaxSeq]
	switch {
	case r.info.Records == 0 && (hasMin || hasMax):
		return formatErr("footer metadata has %s or %s, but the file holds no records", keyMinSeq, keyMaxSeq)
	case r.info.Records > 0 && (!hasMin || !hasMax):
		return formatErr("footer metadata %s and %s are required when the file holds records", keyMinSeq, keyMaxSeq)
	case r.info.Records > 0:
		if r.info.MinSeq, err = parseDecimal(kv, keyMinSeq); err != nil {
			return err
		}
		if r.info.MaxSeq, err = parseDecimal(kv, keyMaxSeq); err != nil {
			return err
		}
		if r.info.MinSeq > r.info.MaxSeq {
			return formatErr("footer metadata %s %s is above %s %s", keyMinSeq, minSeq, keyMaxSeq, maxSeq)
		}
	}

	r.digest, ok = kv[keyDigest]
	if !ok || !validDigest(r.digest) {
		return formatErr("footer metadata %s is missing or is not sha256: and 64 lowercase hex digits", keyDigest)
	}
	list, ok := kv[keyGroups]
	if !ok {
		return formatErr("footer metadata %s is missing", keyGroups)
	}
	listed := 0
	if list != "" {
		listed = strings.Count(list, ",") + 1
	}
	if listed != groups {
		return formatErr("footer metadata %s lists %d digests for %d row groups", keyGroups, listed, groups)
	}
	if listed > 0 {
		r.groupWant = strings.Split(list, ",")
	}
	for i, g := range r.groupWant {
		if !validGroupDigest(g) {
			return formatErr("footer metadata %s: the digest of row group %d is not 64 lowercase hex digits", keyGroups, i)
		}
	}
	if want := fileDigest(r.groupWant); want != r.digest {
		return formatErr("footer metadata %s is %s, but the row group digests in %s give %s", keyDigest, r.digest, keyGroups, want)
	}
	return nil
}

// parseDecimal reads a footer value that must be an unsigned decimal number
// in its shortest form.
func parseDecimal(kv map[string]string, key string) (uint64, error) {
	s, ok := kv[key]
	if !ok {
		return 0, formatErr("footer metadata %s is missing", key)
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != s {
		return 0, formatErr("footer metadata %s is %q, not a decimal number", key, s)
	}
	return v, nil
}

// Info describes the file, from its footer.
func (r *Reader) Info() Info { return r.info }

// ErrReaderClosed is wrapped by the error Next returns after Close.
var ErrReaderClosed = errors.New("activity reader closed")

// Close releases the reader. It does not close the underlying reader. Next
// after Close returns an error wrapping [ErrReaderClosed].
func (r *Reader) Close() error {
	r.closed = true
	r.recs = nil
	if r.cur != nil {
		_ = r.guard("closing", func() error { r.closeGroup(); return nil })
	}
	return nil
}

func (r *Reader) closeGroup() {
	if r.cur == nil {
		return
	}
	for _, c := range r.cur.cols {
		if cl, ok := c.(io.Closer); ok {
			_ = cl.Close()
		}
	}
	r.cur = nil
}

// Next returns the next record, and io.EOF after the last. Every record has
// passed [store.Record.Validate] and has a Seq above the one before it.
//
// The reader decodes one row group at a time, and checks every row of it and
// the group's content digest before it returns the group's first record. A
// record Next returns therefore belongs to a verified row group: if a group is
// damaged, Next returns an error and none of that group's records. The groups
// before it have already been returned, and a caller that stops early has
// verified only the groups it consumed. At the end, before io.EOF, Next checks
// the row count, the first and last Seq and the digest of the file against the
// footer. After any error other than io.EOF, Next keeps returning that error.
func (r *Reader) Next() (store.Record, error) {
	if r.closed {
		return store.Record{}, fmt.Errorf("activity: next: %w", ErrReaderClosed)
	}
	if r.err != nil {
		return store.Record{}, r.err
	}
	if r.at >= len(r.recs) {
		if r.nextGroup >= r.info.RowGroups {
			r.err = r.finish()
			return store.Record{}, r.err
		}
		if err := r.guard("decoding", r.loadGroup); err != nil {
			r.err, r.recs = err, nil
			return store.Record{}, err
		}
	}
	rec := r.recs[r.at]
	r.recs[r.at] = store.Record{}
	r.at++
	return rec, nil
}

// finish checks the file as a whole once every row has been read.
func (r *Reader) finish() error {
	// Everything checked here has already been verified for each row group
	// before its records were returned, so these checks cannot fail today. They
	// stay as the last line of defence against a later change to loadGroup.
	if r.rows != r.info.Records {
		return formatErr("read %d rows, the footer says %d records", r.rows, r.info.Records)
	}
	if r.rows > 0 {
		if r.first != r.info.MinSeq {
			return formatErr("the first seq is %d, the footer says %s is %d", r.first, keyMinSeq, r.info.MinSeq)
		}
		if r.prev != r.info.MaxSeq {
			return formatErr("the last seq is %d, the footer says %s is %d", r.prev, keyMaxSeq, r.info.MaxSeq)
		}
	}
	if got := fileDigest(r.groupGot); got != r.digest {
		return formatErr("the content digest is %s, the footer says %s: the file was changed or damaged", got, r.digest)
	}
	return io.EOF
}

// loadGroup decodes the next row group completely, batch by batch, checking
// every row, and verifies the group's digest against the footer. Only then are
// its records available to Next.
func (r *Reader) loadGroup() error {
	i := r.nextGroup
	clear(r.recs)
	r.recs, r.at = r.recs[:0], 0
	if err := r.openGroup(i); err != nil {
		return err
	}
	r.nextGroup++
	// One allocation for the records of the group: the rows are already capped
	// at the limit over recordCost, so this is at most the limit.
	r.recs = slices.Grow(r.recs[:0], int(r.cur.remaining))
	r.dig.reset()
	r.cost = 0
	clear(r.fps)
	clear(r.names)
	for r.cur.remaining > 0 {
		if err := r.readBatch(); err != nil {
			return err
		}
		for j := range r.n {
			rec, err := r.decode(j)
			if err != nil {
				return err
			}
			r.recs = append(r.recs, rec)
			r.rows++
		}
	}
	r.closeGroup()
	// The footer's totals, checked for the group that holds them before any of
	// its records is returned.
	if i == 0 && r.recs[0].Seq != r.info.MinSeq {
		return formatErr("row group 0: the first seq is %d, the footer says %s is %d", r.recs[0].Seq, keyMinSeq, r.info.MinSeq)
	}
	if r.nextGroup == r.info.RowGroups {
		if last := r.recs[len(r.recs)-1].Seq; last != r.info.MaxSeq {
			return formatErr("row group %d: the last seq is %d, the footer says %s is %d", i, last, keyMaxSeq, r.info.MaxSeq)
		}
		if r.rows != r.info.Records {
			return formatErr("row group %d is the last, after %d rows; the footer says %d records", i, r.rows, r.info.Records)
		}
	}
	got := r.dig.sumHex()
	if got != r.groupWant[i] {
		return formatErr("row group %d: the content digest is %s, the footer says %s: the row group was changed or damaged", i, got, r.groupWant[i])
	}
	r.groupGot = append(r.groupGot, got)
	return nil
}

// openGroup checks a row group's sizes against the limit, before any of its
// data is read, and opens a reader on each of its columns.
func (r *Reader) openGroup(i int) error {
	rg := r.fr.RowGroup(i)
	md := rg.MetaData()
	if md.NumColumns() != numColumns {
		return formatErr("row group %d has %d columns, want %d", i, md.NumColumns(), numColumns)
	}
	rows := md.NumRows()
	if rows > r.maxGroup/recordCost {
		return formatErr("row group %d has %d rows, which decode to more than the limit of %d bytes (ReaderOptions.MaxRowGroupBytes) at %d bytes a record", i, rows, r.maxGroup, recordCost)
	}
	if size := md.TotalByteSize(); size < 0 || size > r.maxGroup {
		return formatErr("row group %d is %d bytes, over the limit of %d bytes (ReaderOptions.MaxRowGroupBytes)", i, size, r.maxGroup)
	}
	for c := range numColumns {
		cc, err := md.ColumnChunk(c)
		if err != nil {
			return libraryErr(fmt.Sprintf("row group %d, column %s", i, columns[c].name), err)
		}
		if n := cc.TotalCompressedSize(); n < 0 || n > r.maxGroup {
			return formatErr("row group %d, column %s: %d compressed bytes, over the limit of %d bytes (ReaderOptions.MaxRowGroupBytes)", i, columns[c].name, n, r.maxGroup)
		}
		if n := cc.TotalUncompressedSize(); n < 0 || n > r.maxGroup {
			return formatErr("row group %d, column %s: %d uncompressed bytes, over the limit of %d bytes (ReaderOptions.MaxRowGroupBytes)", i, columns[c].name, n, r.maxGroup)
		}
		if cc.Type() != columns[c].physical || cc.NumValues() != rows || cc.CryptoMetadata() != nil {
			return formatErr("row group %d, column %s: the chunk has type %s, %d values and %d rows", i, columns[c].name, cc.Type(), cc.NumValues(), rows)
		}
	}

	g := &group{index: i, remaining: rows}
	r.cur = g
	for c := range numColumns {
		pr, err := rg.GetColumnPageReader(c)
		if err != nil {
			return libraryErr(fmt.Sprintf("row group %d, column %s", i, columns[c].name), err)
		}
		// The library builds the column reader around the pages it is given.
		// NewColumnReader is deprecated for use outside the library because it
		// leaves the buffer pool to the caller; the pool here is the file
		// reader's own, as rg.Column passes it. It is the only way to have each
		// page checked before the library acts on it.
		//nolint:staticcheck // see above
		g.cols[c] = file.NewColumnReader(r.fr.MetaData().Schema.Column(c), &checkedPages{
			PageReader: pr, group: i, column: columns[c].name, minValue: minEncodedValue(columns[c].physical),
		}, r.props.Allocator(), r.fr.BufferPool())
	}
	return nil
}

// minEncodedValue is the fewest bytes one value of a dictionary page takes in
// the PLAIN encoding the library requires of it: a length of 4 bytes for a byte
// array, 8 for an int64.
func minEncodedValue(t parquet.Type) int {
	switch t {
	case parquet.Types.Int64, parquet.Types.Double:
		return 8
	}
	return 4
}

// checkedPages wraps the pages of a column chunk and refuses a dictionary page
// the library must not be given. The library sizes its dictionary from the
// number of values in the page header, a claim it never compares with the size
// of the page, and allocates for any index below it: one data page index near
// 2^31 and a header claiming that many values make it allocate tens of GiB, and
// running out of memory is not an error that can be recovered. So a dictionary
// page must hold at least the bytes its values need, be the first page of the
// chunk, and be the only one.
type checkedPages struct {
	file.PageReader
	group    int
	column   string
	minValue int
	pages    int // pages returned so far
	err      error
}

func (p *checkedPages) Next() bool {
	if p.err != nil || !p.PageReader.Next() {
		return false
	}
	if pg := p.Page(); pg != nil {
		if pg.Type() == file.PageTypeDictionaryPage {
			if p.pages > 0 {
				p.err = formatErr("row group %d, column %s: a dictionary page after %d other pages", p.group, p.column, p.pages)
				return false
			}
			if p.err = p.checkDictionary(pg); p.err != nil {
				return false
			}
		}
		p.pages++
	}
	return true
}

// GetDictionaryPage is the library's other way to the dictionary page, from the
// offset in the footer.
func (p *checkedPages) GetDictionaryPage() (*file.DictionaryPage, error) {
	pg, err := p.PageReader.GetDictionaryPage()
	if err == nil && pg != nil {
		err = p.checkDictionary(pg)
	}
	return pg, err
}

func (p *checkedPages) Err() error {
	if p.err != nil {
		return p.err
	}
	return p.PageReader.Err()
}

func (p *checkedPages) checkDictionary(pg file.Page) error {
	n, size := int64(pg.NumValues()), int64(len(pg.Data()))
	if n < 0 || n > size/int64(p.minValue) {
		return formatErr("row group %d, column %s: a dictionary page of %d bytes claims %d values, which take at least %d bytes each",
			p.group, p.column, size, n, p.minValue)
	}
	return nil
}

// readBatch decodes the next batch of the current row group, all columns in
// step.
func (r *Reader) readBatch() error {
	g := r.cur
	m := int(min(int64(r.batchRows), g.remaining))
	r.held, r.chunk = 0, nil
	for c := range numColumns {
		name := columns[c].name
		var def []int16
		if columns[c].rep == repOptional {
			r.defs[c] = grow(r.defs[c], m)
			def = r.defs[c]
		}
		var total int64
		var nvals int
		var err error
		switch cr := g.cols[c].(type) {
		case *file.Int32ColumnChunkReader:
			r.i32[c] = grow(r.i32[c], m)
			total, nvals, err = cr.ReadBatch(int64(m), r.i32[c], def, nil)
			if err == nil && total < int64(m) {
				err = cr.Err()
			}
		case *file.Int64ColumnChunkReader:
			r.ints[c] = grow(r.ints[c], m)
			total, nvals, err = cr.ReadBatch(int64(m), r.ints[c], def, nil)
			if err == nil && total < int64(m) {
				// ReadBatch ends quietly at a page it could not read; the
				// reason is the column's.
				err = cr.Err()
			}
		case *file.ByteArrayColumnChunkReader:
			r.strs[c] = grow(r.strs[c], m)
			total, nvals, err = r.readBytes(cr, c, m, def)
		default:
			return formatErr("row group %d, column %s: unexpected column reader %T", g.index, name, g.cols[c])
		}
		if err != nil {
			return libraryErr(fmt.Sprintf("row group %d, column %s, after row %d of the file", g.index, name, r.rows), err)
		}
		if total != int64(m) {
			return formatErr("row group %d, column %s: %d values where the row group has %d more rows (after row %d of the file)", g.index, name, total, m, r.rows)
		}
		if def == nil {
			if nvals != m {
				return formatErr("row group %d, column %s: %d values for %d rows", g.index, name, nvals, m)
			}
			continue
		}
		present := 0
		for _, d := range def {
			switch d {
			case 0:
			case 1:
				present++
			default:
				return formatErr("row group %d, column %s: definition level %d", g.index, name, d)
			}
		}
		if present != nvals {
			return formatErr("row group %d, column %s: %d values for %d non-null rows", g.index, name, nvals, present)
		}
		// Spread the packed values over their rows, from the last row back,
		// so that the packing never overwrites a value not yet moved.
		if columns[c].physical == typeInt64 {
			spread(r.ints[c], def, nvals)
		} else {
			spread(r.strs[c], def, nvals)
		}
	}
	g.remaining -= int64(m)
	r.n, r.groupAt = m, g.index
	return nil
}

// readBytes reads m rows of a BYTE_ARRAY column into r.strs[c] and copies the
// values into memory the reader owns, in fixed chunks. It reads page by page
// and refuses a batch whose copies would pass the limit before it makes them, so
// that a dictionary page repeated over thousands of rows cannot make the library
// or this package allocate more than the limit. (The library's ReadBatch would
// clone every value first, whatever its size.)
func (r *Reader) readBytes(cr *file.ByteArrayColumnChunkReader, c, m int, def []int16) (int64, int, error) {
	vals := r.strs[c]
	var total int64
	var nvals int
	for total < int64(m) {
		var d []int16
		if def != nil {
			d = def[total:]
		}
		t, n, err := cr.ReadBatchInPage(int64(m)-total, vals[nvals:], d, nil)
		if err != nil {
			return total, nvals, err
		}
		if t == 0 && n == 0 {
			break
		}
		for k := nvals; k < nvals+n; k++ {
			cp, ok := r.copyValue(vals[k])
			if !ok {
				return total, nvals, formatErr("row group %d, column %s: the values of %d rows are over the limit of %d bytes (ReaderOptions.MaxRowGroupBytes), after row %d of the file",
					r.cur.index, columns[c].name, m, r.maxGroup, r.rows)
			}
			vals[k] = cp
		}
		total += t
		nvals += n
	}
	return total, nvals, nil
}

// copyValue copies a value the library owns into the chunk being filled, or
// into a copy of its own if it is large. It counts the memory it allocates
// toward the limit for the batch, and reports false, allocating nothing, if
// that would pass it.
func (r *Reader) copyValue(v []byte) ([]byte, bool) {
	n := len(v)
	if n == 0 {
		return nil, true
	}
	if n > r.chunkSize/4 {
		if r.held+int64(n) > r.maxGroup {
			return nil, false
		}
		r.held += int64(n)
		return bytes.Clone(v), true
	}
	if cap(r.chunk)-len(r.chunk) < n {
		if r.held+int64(r.chunkSize) > r.maxGroup {
			return nil, false
		}
		r.held += int64(r.chunkSize)
		r.chunk = make([]byte, 0, r.chunkSize)
	}
	start := len(r.chunk)
	r.chunk = append(r.chunk, v...)
	return r.chunk[start:len(r.chunk):len(r.chunk)], true
}

func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

// spread moves the first nvals entries of vals, which are the values of the
// rows whose definition level is 1, to the positions of those rows.
func spread[T any](vals []T, def []int16, nvals int) {
	var zero T
	k := nvals
	for j := len(def) - 1; j >= 0; j-- {
		if def[j] == 1 {
			k--
			vals[j] = vals[k]
		} else {
			vals[j] = zero
		}
	}
}

// rowErr builds a format error for the row being decoded.
func (r *Reader) rowErr(col int, format string, args ...any) error {
	return formatErr("row group %d, row %d of the file, column %s: %s", r.groupAt, r.rows, columns[col].name, fmt.Sprintf(format, args...))
}

// decode turns row i of the batch into a record, checking everything the file
// promises. The batch holds the library's buffers, so every string and slice
// of the record is a copy.
func (r *Reader) decode(i int) (store.Record, error) {
	var rec store.Record
	var cr canonRow

	if err := r.charge(recordCost); err != nil {
		return rec, err
	}
	cr.seq = uint64(r.ints[colSeq][i])
	if cr.seq <= r.prev {
		return rec, r.rowErr(colSeq, "seq %d is not above the previous row's %d", cr.seq, r.prev)
	}
	cr.eventNS = r.ints[colEventTime][i]
	basis := r.i32[colEventTimeBasis][i]
	if basis < 0 || basis > int32(store.BasisProducerEvent) {
		return rec, r.rowErr(colEventTimeBasis, "%d is not the number of an event time basis (0 to %d)", basis, store.BasisProducerEvent)
	}
	cr.basis = basis
	cr.ttlNS = r.ints[colTTL][i]
	cr.hasThrough = r.defs[colThrough][i] == 1
	if cr.hasThrough {
		cr.throughNS = r.ints[colThrough][i]
	}

	// The enumerations.
	var ok bool
	var layer catalog.Layer
	if layer, cr.layer, ok = parseLayer(r.strs[colLayer][i]); !ok {
		return rec, r.rowErr(colLayer, "%q is not L0, L1, L2 or L3", r.strs[colLayer][i])
	}
	var subjectKind store.SubjectKind
	if subjectKind, cr.subjectKind, ok = parseSubjectKind(r.strs[colSubjectKind][i]); !ok {
		return rec, r.rowErr(colSubjectKind, "%q is not entity or edge", r.strs[colSubjectKind][i])
	}
	var kind lifecycle.Kind
	if kind, cr.kind, ok = parseKind(r.strs[colKind][i]); !ok {
		return rec, r.rowErr(colKind, "%q is not observe or delete", r.strs[colKind][i])
	}

	// The null pattern: an entity has no target and no relation, an edge has both.
	hasTarget, hasRelation := r.defs[colTarget][i] == 1, r.defs[colRelation][i] == 1
	isEdge := subjectKind == store.SubjectEdge
	if hasTarget != isEdge {
		return rec, r.rowErr(colTarget, "a %s row has a target only if it is an edge", cr.subjectKind)
	}
	if hasRelation != isEdge {
		return rec, r.rowErr(colRelation, "a %s row has a relation only if it is an edge", cr.subjectKind)
	}

	// The text columns. A value that repeats within the row group is held
	// once.
	src, err := r.fingerprint(colSource, r.strs[colSource][i])
	if err != nil {
		return rec, err
	}
	cr.source = src.text
	var target internedFP
	if isEdge {
		if target, err = r.fingerprint(colTarget, r.strs[colTarget][i]); err != nil {
			return rec, err
		}
		cr.target = target.text
		if cr.relation, err = r.name(colRelation, r.strs[colRelation][i], MaxText); err != nil {
			return rec, err
		}
	}
	if cr.producer, err = r.name(colProducer, r.strs[colProducer][i], MaxText); err != nil {
		return rec, err
	}

	// The boot id: on an observation of a host only, never empty, at most
	// store.MaxBootLen bytes. The host rule and the length are checks that
	// Record.Validate also makes, kept for a clearer message and so that a long
	// text is refused before it is interned and counted; they are redundant for
	// what the reader accepts. A present but empty boot is refused here because
	// it would otherwise read back as none.
	var boot string
	if r.defs[colBoot][i] == 1 {
		if kind != lifecycle.Observe || isEdge || src.fp.Type() != catalog.Host {
			return rec, r.rowErr(colBoot, "only an observation of a host carries a boot id")
		}
		if len(r.strs[colBoot][i]) == 0 {
			return rec, r.rowErr(colBoot, "a present boot id is empty")
		}
		if boot, err = r.name(colBoot, r.strs[colBoot][i], store.MaxBootLen); err != nil {
			return rec, err
		}
		cr.hasBoot, cr.boot = true, boot
	}
	payload := r.strs[colPayload][i]
	if len(payload) > MaxPayload {
		return rec, r.rowErr(colPayload, "payload of %d bytes is over the limit of %d", len(payload), MaxPayload)
	}
	cr.payload = payload
	if err := r.charge(int64(len(payload))); err != nil {
		return rec, err
	}
	if cr.ttlNS < 0 {
		return rec, r.rowErr(colTTL, "negative TTL %d", cr.ttlNS)
	}

	rec = store.Record{
		Layer:          layer,
		Subject:        store.Subject{Kind: subjectKind, A: src.fp, B: target.fp, Relation: catalog.RelationType(cr.relation)},
		Producer:       lifecycle.Producer(cr.producer),
		EventTime:      time.Unix(0, cr.eventNS).UTC(),
		Seq:            cr.seq,
		Kind:           kind,
		TTL:            time.Duration(cr.ttlNS),
		Payload:        bytes.Clone(payload),
		Boot:           boot,
		EventTimeBasis: store.EventTimeBasis(basis),
	}
	if cr.hasThrough {
		rec.Through = time.Unix(0, cr.throughNS).UTC()
	}
	if len(rec.Payload) == 0 {
		rec.Payload = nil
	}
	if err := rec.Validate(); err != nil {
		return store.Record{}, fmt.Errorf("%w: row group %d, row %d of the file: %w", ErrFormat, r.groupAt, r.rows, err)
	}

	r.dig.add(&cr)
	if r.rows == 0 {
		r.first = cr.seq
	}
	r.prev = cr.seq
	return rec, nil
}

// charge adds n bytes to the decoded size of the row group, and refuses the
// group once that passes the limit. It is called before the memory is
// allocated.
func (r *Reader) charge(n int64) error {
	r.cost += n
	if r.cost > r.maxGroup {
		return formatErr("row group %d: the decoded records are over the limit of %d bytes (ReaderOptions.MaxRowGroupBytes), after row %d of the file",
			r.groupAt, r.maxGroup, r.rows)
	}
	return nil
}

// fingerprint parses the fingerprint in b, once for each distinct text in the
// row group.
func (r *Reader) fingerprint(col int, b []byte) (internedFP, error) {
	if len(b) > maxFingerprintText {
		return internedFP{}, r.rowErr(col, "text of %d bytes is not a fingerprint", len(b))
	}
	if e, ok := r.fps[string(b)]; ok {
		return e, nil
	}
	if err := r.charge(int64(len(b)) + entryCost); err != nil {
		return internedFP{}, err
	}
	s := string(b)
	fp, err := identity.ParseFingerprint(s)
	if err != nil {
		return internedFP{}, r.rowErr(col, "%v", err)
	}
	e := internedFP{fp: fp, text: s}
	r.fps[s] = e
	return e, nil
}

// name returns the text in b, a producer or a relation, held once for each
// distinct value in the row group.
func (r *Reader) name(col int, b []byte, limit int) (string, error) {
	if len(b) > limit {
		return "", r.rowErr(col, "text of %d bytes is over the limit of %d", len(b), limit)
	}
	if s, ok := r.names[string(b)]; ok {
		return s, nil
	}
	if err := r.charge(int64(len(b)) + entryCost); err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", r.rowErr(col, "not valid UTF-8")
	}
	s := string(b)
	r.names[s] = s
	return s, nil
}

func parseLayer(b []byte) (catalog.Layer, string, bool) {
	switch string(b) {
	case "L0":
		return catalog.L0, "L0", true
	case "L1":
		return catalog.L1, "L1", true
	case "L2":
		return catalog.L2, "L2", true
	case "L3":
		return catalog.L3, "L3", true
	}
	return 0, "", false
}

func parseSubjectKind(b []byte) (store.SubjectKind, string, bool) {
	switch string(b) {
	case "entity":
		return store.SubjectEntity, "entity", true
	case "edge":
		return store.SubjectEdge, "edge", true
	}
	return 0, "", false
}

func parseKind(b []byte) (lifecycle.Kind, string, bool) {
	switch string(b) {
	case "observe":
		return lifecycle.Observe, "observe", true
	case "delete":
		return lifecycle.Delete, "delete", true
	}
	return 0, "", false
}
