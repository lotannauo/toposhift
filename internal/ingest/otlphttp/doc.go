// Package otlphttp receives OpenTelemetry logs over OTLP/HTTP: an
// [http.Handler] that serves POST /v1/logs, decodes the body, and hands it to
// a [Sink] as the module's OTLP JSON model, [capture.LogsData]. Binding a
// listener and serving are not its business; the caller mounts the handler.
//
// # One input type
//
// A request body is protobuf (Content-Type application/x-protobuf) or JSON
// (application/json), and either may be gzip-compressed (Content-Encoding:
// gzip). A JSON body is decoded with [capture.DecodeLogs], so it must be a logs
// message, the same rule as a line of a capture file, except that a body that is
// only {} is the empty request and is accepted, as the empty protobuf body is;
// a body of another signal (resourceMetrics) is refused. A protobuf body is
// decoded and converted to the same model, so the rest of ingest has one input
// type and the same data sent either way reaches the sink as an equal value.
// Fields the model does not name are dropped in both encodings. A protobuf
// string-table index (string_value_strindex, key_strindex) is refused, because
// the logs signal carries no table to resolve it against, and a trace or span
// ID of the wrong size is refused, as in JSON.
//
// # Nesting
//
// Values nest (an array or a key-value list holds values), and what a nested
// body costs is not its size: decoding JSON gets slower faster than the body
// grows as it nests deeper, so a body of under 100 KB nested a few thousand
// deep takes seconds to decode, and as long again to encode. So the depth is
// limited, in both encodings, before it can be expensive. A JSON body whose brackets nest
// deeper than 160 is refused by one linear scan before it is decoded, and a
// protobuf body is decoded with a recursion limit of the same size. A decoded
// batch with a value that nests deeper than 32 (a scalar is 1, and each array or
// key-value list adds 1) is refused in both encodings. All of these are 400. A
// Kubernetes object nests about 13 values deep, and 53 levels of JSON. A body
// that is nested as deeply as these limits allow decodes at about the speed of
// real records of its size.
//
// # No gRPC in the build
//
// The OTLP/HTTP request is the protobuf message ExportLogsServiceRequest. This
// package does not import the packages that define it, because the generated
// code of that service (go.opentelemetry.io/proto/otlp/collector/...) imports
// gRPC, and the product's build must not contain gRPC. It decodes the body as
// go.opentelemetry.io/proto/otlp/logs/v1.LogsData instead. The two messages
// are wire-compatible: each is a single field 1, repeated ResourceLogs, so
// the same bytes decode as either. The tests encode both shapes and check that
// they decode to the same value.
//
// # Statuses
//
// The statuses follow the OTLP/HTTP specification:
//
//   - 200 with an empty ExportLogsServiceResponse: zero bytes for protobuf and
//     {} for JSON. A request is accepted whole or refused whole; there is no
//     partial success.
//   - 400 for a body that does not decode (including a bad or truncated gzip
//     stream).
//   - 404 for a path other than /v1/logs, and 405, with Allow: POST, for a
//     method other than POST.
//   - 413 for a body above [Options.MaxBodyBytes], measured twice against the
//     same limit: the bytes on the wire, and the bytes after decompression. The
//     decompressed bytes are counted as they are produced and decompression
//     stops at the limit, so a small gzip stream that inflates to gigabytes
//     costs at most the limit, in time and in memory.
//   - 415 for a Content-Type that is neither protobuf nor JSON, or a
//     Content-Encoding that is neither gzip nor identity.
//   - 503 with Retry-After when the sink returns [ErrBusy]; the client is
//     expected to send the same request again later.
//   - 500 for any other sink error. The text of the error stays in the server.
//
// A refusal with a status body carries a google.rpc.Status (code and message)
// in the encoding of the request: protobuf, or JSON for a JSON request. 404
// and 405 are about the route, not about an export, and have a plain text body.
//
// # Why the limit is 64 MiB
//
// [DefaultMaxBodyBytes] is 64 MiB, after decompression. The OTLP/HTTP
// specification says a client must not retry a 413, so a request refused for
// its size is lost. The request that is large by nature is a full pull of the
// Kubernetes objects receiver: one log record per object of a kind, which the
// collector may send as one batch. A pod record is about 13 KB of JSON (the
// median of the pod records of a simulated cluster), so a pull of 300 pods is
// already above 4 MiB, and the pull of a cluster of a few thousand pods is tens
// of megabytes. The limit is set so that a full pull of a cluster fits in one
// request.
//
// # What a request costs
//
// The limit is a limit on bytes, and the cost of a request is a multiple of
// them, which a deployment has to budget. As measured, a request allocates about
// 13 times its body for protobuf and about 17 times for JSON, so one request of
// 64 MiB takes 0.8 to 1.1 GB. The body is read whole, and the decoded batch is
// held whole until the sink returns. JSON decodes several times slower than
// protobuf. The OpenTelemetry collector sends protobuf by default, so the
// protobuf figures are the ones to plan on; JSON is for senders that cannot do
// otherwise. A body is read as it arrives, with a buffer
// that starts small and doubles: a length a client declares is not trusted to
// size anything.
//
// The limit applies to the bytes on the wire as well as to the bytes after
// decompression, whichever encoding the sender uses.
//
// # What the server has to provide
//
// This package is a handler. It limits one request and nothing across
// requests. The lane that serves it must bound the requests in flight, by a
// count or by the bytes they hold, so that a number of large requests cannot
// together take the memory of the machine; it must set read-header and read
// timeouts, so that a client that sends nothing, or sends a body slowly, does
// not hold a connection and a buffer for ever; and it must decide what a body
// read that is too slow gets (the handler answers 400 if the connection fails
// mid-body, and cannot tell slow from broken).
package otlphttp
