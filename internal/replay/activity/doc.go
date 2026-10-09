// Package activity writes and reads activity replay files: the records a
// store's Write takes, after identity resolution, in ascending Seq, as
// Parquet.
//
// An activity file is a sequence of [store.Record] values, each keyed by the
// fingerprints of the entities it is about, in strictly ascending Seq (the
// ingest sequence number). That is the order a store ingests them in, so every
// storage engine can replay the same file. Parquet is the container because
// DuckDB, Iceberg datasets and Raphtory's Python loaders read it natively.
// [Writer] and [Reader] stream: neither holds a whole file in memory.
//
// # Schema
//
// Format version 1 has fourteen columns, in this order, in a flat schema:
//
//	 #  column            physical    annotation           repetition  holds
//	 0  seq               INT64       integer 64 unsigned  required    Record.Seq
//	 1  event_time_ns     INT64       integer 64 signed    required    EventTime.UnixNano()
//	 2  layer             BYTE_ARRAY  string               required    "L0" to "L3"
//	 3  subject_kind      BYTE_ARRAY  string               required    "entity" or "edge"
//	 4  source            BYTE_ARRAY  string               required    Subject.A: the entity, or the edge's source
//	 5  target            BYTE_ARRAY  string               optional    Subject.B; null for an entity
//	 6  relation          BYTE_ARRAY  string               optional    Subject.Relation; null for an entity
//	 7  producer          BYTE_ARRAY  string               required    Record.Producer
//	 8  kind              BYTE_ARRAY  string               required    "observe" or "delete"
//	 9  ttl_ns            INT64       integer 64 signed    required    TTL in nanoseconds; 0 for none
//	10  through_ns        INT64       integer 64 signed    optional    Through.UnixNano(); null when Through is zero
//	11  payload           BYTE_ARRAY  none                 required    Payload; empty for none
//	12  boot              BYTE_ARRAY  string               optional    Record.Boot; null when there is none
//	13  event_time_basis  INT32       integer 32 signed    required    Record.EventTimeBasis: 0 to 4
//
// The reasons for these choices:
//
//   - Times are plain nanoseconds, not a Parquet timestamp type. Every reader
//     keeps an integer exactly, while some convert UTC-adjusted nanosecond
//     timestamps to microseconds. Both time axes, event time and ingest
//     sequence, stay integers. A reader converts with its own function; in
//     DuckDB that is make_timestamp_ns where the version has it, otherwise
//     to_timestamp(event_time_ns / 1e9), which keeps microseconds only.
//   - Fingerprints are their text form, a type and 32 lowercase hex digits
//     ("k8s.pod:" and the digits; see [identity.Fingerprint.String]), read back
//     with the strict [identity.ParseFingerprint]. The text is readable and
//     joinable in DuckDB and usable as a node identifier by graph loaders.
//     Dictionary encoding keeps the repetition cheap.
//   - Enumerations are strings, not the Go constants' numbers: [catalog.L0] is
//     1, which an outside reader would misread as layer 1.
//   - source and target name the two ends of an edge, so a graph loader can map
//     them to its source and destination columns directly.
//   - boot is the boot id of the machine a host observation was made in. Clone
//     collisions (two live boots of one host) can be detected only if the boot id
//     survives the replay, so it travels with the record. Only an observation of
//     a host carries one: a delete, an edge and any other entity never do, and
//     it is never blank, valid UTF-8 and at most [store.MaxBootLen] bytes. The
//     column is last so that the order of the others does not change. The format
//     is not frozen yet: the column was added to version 1 before any file of the
//     format had been kept, so there is no version 2.
//   - event_time_basis says which clock stamped the event time (see
//     [store.EventTimeBasis]). It is a number, 0 for unknown to 4 for the
//     producer's own event time, unlike the other enumerations: the numbering
//     is the store's and is part of the format. The reader refuses any other
//     number, and the column is last so that the order of the others does not
//     change.
//   - producer and relation are at most [MaxText] bytes each, so that no text
//     field can make a row group larger than the reader is willing to hold.
//
// A [store.Record] read back has EventTime and Through in UTC, Through zero when
// the column is null, and a nil Payload when the stored payload is empty. A null
// boot is an empty Record.Boot; a present boot is never empty.
//
// # Footer metadata and the content digest
//
// The footer carries these key-value pairs:
//
//	toposhift.activity.version        "1"
//	toposhift.activity.records        the record count, in decimal
//	toposhift.activity.min_seq        the first Seq, in decimal (only when records > 0)
//	toposhift.activity.max_seq        the last Seq, in decimal (only when records > 0)
//	toposhift.activity.group_digests  the digest of each row group, in row group order:
//	                                  64 lowercase hex digits each, separated by commas
//	                                  (empty when the file has no row group)
//	toposhift.activity.digest         "sha256:" and 64 lowercase hex digits
//
// Parquet pages carry no checksum in the library's writer, so a flipped byte
// inside a data page can decode to another value without any error. The digests
// close that gap. The digest of a row group is the SHA-256 of the canonical
// encoding of its records in file order, where the encoding of one record is,
// with every integer big-endian and every string or byte field as a 4-byte
// length and then its bytes:
//
//	seq (8 bytes), event_time_ns (8, two's complement), layer, subject_kind,
//	source, target (empty when null), relation (empty when null), producer,
//	kind (all strings), ttl_ns (8), one byte 1 or 0 for whether through is
//	present, through_ns (8, 0 when absent), payload (4-byte length, then bytes),
//	one byte 1 or 0 for whether boot is present, boot (a string, empty when absent),
//	event_time_basis (4 bytes)
//
// The digest of the file is derived from the digests of its row groups: it is
// the SHA-256 of their raw 32-byte values, concatenated in file order. So it
// also depends on how the records are divided into row groups, and the reader
// checks it against the listed group digests as soon as it opens the file. A
// file without a row group has the digest of nothing.
//
// These encodings are part of format version 1. An empty file is valid: a
// schema, no row group, a count of zero, no min_seq or max_seq, an empty list of
// group digests, and the digest of nothing. The same records and options always
// give the same bytes, but only for the same versions of the Parquet library
// (apache/arrow-go) and of the zstd library it uses (klauspost/compress):
// nothing in the file depends on the clock or on randomness. Each row group
// declares itself sorted ascending by seq.
//
// # Guarantees
//
// A file this package writes has unique, strictly ascending Seq, and every
// record in it passes [store.Record.Validate]. [Writer.Write] refuses a record
// that does not, with an error wrapping [store.ErrInvalid], before anything is
// buffered; it also refuses a record whose Seq does not rise, whose payload is
// over [MaxPayload], whose producer or relation is over [MaxText] bytes, or whose
// producer, relation or boot id is not valid UTF-8 (a Parquet string must be).
// The reader does not trust the writer: it verifies all of it and refuses a file
// otherwise, with an error wrapping [ErrFormat]. It checks the
// footer (the schema column by column, the version, the metadata against the row
// groups, the file digest against the group digests), every row (the null
// pattern, each field's form, ascending Seq, [store.Record.Validate]) and, at the
// end, the row count, the first and last Seq and the file digest.
//
// The reader returns records only from verified row groups. It decodes a row
// group completely, checks every row of it, its digest against the footer and
// the footer's totals that concern it (the first Seq of the first group against
// min_seq, and the last Seq and the row count of the last group against max_seq
// and records), and only then returns the first of its records. If a group is
// damaged, [Reader.Next] returns an error and none of that group's records. The
// groups before it have already been returned (they were verified), so a
// consumer that streams records into a store has applied exactly the verified
// groups when the error comes. A caller that stops before the end has verified
// only the groups it consumed. The end of the file repeats the checks of the
// totals before io.EOF. A file that is read to its end without an error holds
// exactly the records that were written.
//
// # Streaming and memory
//
// The writer buffers one row group in column slices and writes it when it holds
// [WriterOptions.RowGroupRows] records or its buffered size reaches
// [WriterOptions.RowGroupBytes]; a record counts for 64 bytes plus the lengths
// of its strings and payload, and one record larger than the limit gets a row
// group of its own. Memory is bounded by about one row group plus the library's
// encoder buffers, whatever the file size. After a failed write the file is
// incomplete and must be discarded; every later call returns the first error.
//
// The reader decodes one row group at a time and, inside it,
// [ReaderOptions.BatchRows] rows per column, with all fourteen columns in step.
// It keeps the decoded records of the group until they have been returned. Two
// limits bound the group, both [ReaderOptions.MaxRowGroupBytes], called L here.
// The encoded sizes in the footer (the group's and every column chunk's
// compressed and uncompressed size) are checked before any of the group is read,
// and so is the number of rows, at 256 bytes a record. While the group is
// decoded the reader keeps a running total of its decoded size: 256 bytes for
// every record, the length of its payload, and for every producer, relation,
// boot id and fingerprint text, held once for the group however often it
// repeats, its length and 96 bytes for its entry in the map that holds it. The
// group is refused with an error naming the limit as soon as the total passes L,
// before the memory is allocated. Producer and relation are limited to [MaxText]
// bytes, a boot id to [store.MaxBootLen], a fingerprint text to its longest
// form and a payload to [MaxPayload].
//
// What the reader can be made to hold is therefore a small multiple of L, plus
// the buffers of the Parquet library:
//
//   - the records of the group, at most L, in a slice allocated once from the row
//     count (which is at most L/256) so that it never grows by copying;
//   - one batch of byte array values, copied in chunks of 64 KiB (or L/64 if
//     that is less) and counted by the chunk, at most L across all columns. The
//     library's own batch reader clones every value before this package can look
//     at it, so it is not used for them;
//   - the pages the library holds. The reader limits a page, compressed or not,
//     to MaxPayload and 2 MiB (18 MiB, or L if that is less): the writer closes
//     a page after about 1 MiB of values and the one value that took it over. A
//     byte array column holds at most the page of its dictionary (the decoded
//     entries point into it) and the entries themselves, 24 bytes for each value
//     of at least 4, so 6 pages more; and a compressed data page and the same
//     page decompressed. That is 9 pages, 162 MiB, for each of the 9 byte array
//     columns, 1.42 GiB. An integer column holds a dictionary page and its
//     entries (the same size, at 4 or 8 bytes a value) and two data pages: 4
//     pages, 72 MiB, for each of the 5, 360 MiB. This memory is the library's; the
//     package limits it by the page size only and does not count it in L, so
//     the pages total at most 1.8 GiB.
//
// The dictionary needs a check of its own. The library takes the number of
// values of a dictionary page from the page header and allocates for any index a
// data page uses below it, without comparing it with the size of the page, so
// that a few kilobytes claiming 2^31 values and using one large index would
// have it allocate tens of GiB, an out of memory error that no recover catches.
// Every page of every column chunk therefore passes through a check before the
// library sees it: a dictionary page must be the first page of its chunk, appear
// once, and hold the bytes its claimed number of values needs at the least (4
// for a byte array or an int32, 8 for an int64). The file footer is decoded by the library
// before any limit of this package applies, and its thrift decoding checks the
// length of a list only against its own message limit of 100 MB, a global
// setting that cannot be passed in when opening a file: a footer of a few bytes
// can briefly make the library allocate up to about 800 MB for a list of
// pointers. This is documented, not limited.
//
// With the default L of 256 MiB, then, the reader of a hostile file holds up to
// about 2 L plus 1.8 GiB; a file written by this package holds a group under 5.1
// times its buffered size and pages of the writer's own size, a small fraction
// of that.
//
// The writer's buffered size and the reader's decoded size are related, so that
// a row group the writer closes normally can always be read. The writer counts
// 64 bytes and the lengths of the strings and the payload for a record (the
// boot id included). The reader counts 256 bytes, the payload, and the distinct
// texts with 96 bytes each. The worst case is a record with short texts that are
// all distinct. A fingerprint text is a type name, a colon and 32 hex digits, at
// least 34 bytes with a one-character type name; with the type name host it is
// 37. For a one-character type name, the observation of a host with a one-byte
// boot id is 115 bytes buffered (118 for host) and 580 decoded (583); an edge
// delete with four distinct texts is 146 buffered and 710 decoded; the delete of
// an entity, at 113 bytes the smallest record, is 483 decoded. The largest
// ratio, 5.04 for the host observation, is under 5.1, so the decoded size of a
// group is under 5.1 S for a buffered size S. A group closed at
// [WriterOptions.RowGroupBytes], which is at most [MaxWriterRowGroupBytes]
// (32 MiB), decodes to under 164 MiB, within the default L of 256 MiB. A record
// larger than the limit is a group of its own, which decodes to under 16.2 MiB:
// a payload of [MaxPayload], two texts of [MaxText], and a few hundred bytes. A
// reader that sets a lower L must leave at least 5.1 times the RowGroupBytes the
// files were written with, and room for a record with a payload of [MaxPayload].
//
// # Damaged files
//
// The Parquet library panics on some damaged files: a nil pointer in the decoder
// of a corrupted page is one. A file is data from outside, so every call into the
// library on the read path runs behind a recover that turns a panic into an error
// wrapping [ErrFormat], and a reader that recovered a panic stays broken. The
// write path has no such boundary: it handles data this package built, and a
// panic there is a bug to see.
//
// # The library
//
// The package uses the low-level packages of apache/arrow-go (parquet,
// parquet/file, parquet/schema, parquet/metadata and parquet/compress) and not
// pqarrow. The pqarrow package imports Arrow Flight, which links gRPC and
// protobuf into every binary that imports it; the low-level packages do not. All
// of it is pure Go, so toposhift still builds with cgo off. Compression is zstd.
//
// # Checking with other readers
//
// testdata/duckdb_check.sql is a manual round trip through DuckDB: it describes
// the golden file's columns, shows its footer metadata and rows, and has DuckDB
// write the rows to a new Parquet file and compare the two with EXCEPT. Run it
// from the repository root with
//
//	duckdb -c ".read internal/replay/activity/testdata/duckdb_check.sql"
//
// For Raphtory, split the file with DuckDB into edge observations and edge
// deletions (subject_kind = 'edge', by kind), load both into a PersistentGraph,
// the graph kind that keeps deletions, the first with load_edges_from_parquet and
// the second with load_edge_deletions_from_parquet, with source source,
// destination target and layer relation, and compare the edge count with
// DuckDB's. Raphtory reads an integer time as epoch milliseconds, so pass
// event_time_ns // 1_000_000 (computed in the DuckDB split), or a timestamp
// column, or treat the values only as ordering ticks. Entity rows are left out
// of the split, and ttl_ns and through_ns are dropped: Raphtory has no use for
// them. None of this has been run against an installed Raphtory; check the
// function and parameter names against the version in use.
package activity
