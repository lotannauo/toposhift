// Package capture reads and writes captures of OpenTelemetry collector
// output: OTLP JSON Lines, the format the collector's file exporter writes
// and its OTLP JSON file receiver reads.
//
// # Format
//
// A capture is a UTF-8 text file with one JSON object per line. The line
// separator is "\n" (a "\r\n" ending is refused), empty lines are refused,
// there is no byte order mark, and the preferred extension is "jsonl". Each
// line is a TracesData, MetricsData or LogsData message in the OTLP/JSON
// encoding, so its single top-level key that matters is "resourceSpans",
// "resourceMetrics" or "resourceLogs". A file holds exactly one of the three. The specification gives no ordering
// guarantee, in particular none that timestamps increase, so a capture
// either carries [SeqKey] on every log record or is ordered by the fallback
// rule of [SortRefs] before replay. [OrderLogs] does this for lines held in
// memory; [LogRefs] and [SortRefs] do it for a capture streamed from disk,
// keeping only a small [LogRef] per record and reading the lines again by
// [Line.Offset].
//
// # What is typed
//
// [Reader] and [Writer] move lines. A [Line] keeps the bytes it was read
// from, so writing [Line.Raw] again reproduces the file byte for byte. Only
// logs have a typed model ([LogsData], [DecodeLogs], [EncodeLogs]); metrics
// and traces lines are detected and kept raw.
//
// The typed model follows the OTLP/JSON rules: lowerCamelCase keys, 64-bit
// integers as decimal strings (strings or numbers accepted on input),
// enumerations and other 32-bit integers as numbers, trace and span IDs as
// hex, other bytes as base64, and fields with default values omitted on
// output. Unknown fields are ignored on input, so decoding and encoding is
// lossy for them. The signal of a line is found by exact key match; typed
// decoding of the remaining fields is case-insensitive about key names
// because encoding/json is.
package capture
