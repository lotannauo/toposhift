// Package ingest is the entry of collector output into the store. Its
// subpackages are of two kinds: receivers, which take collector output off the
// wire and hand it on as the module's OTLP JSON model, and translators, which
// turn one kind of collector record into store records without a sequence
// number. The sequencer that orders the records and writes them to the store is
// separate.
//
// A translator translates and nothing else. It takes one log record at a time,
// keeps only what it needs to tell a changed description from an unchanged one,
// and returns the records the record stands for. It never reads a clock: every
// time it returns comes from the record it was given. Two translators given the
// same records in the same order return the same records.
//
// A receiver decodes and nothing else. It checks that a request is well formed
// and within its limits, converts it to one input type, and gives it to a sink.
// It does not look inside the records, and it keeps no state between requests.
//
//   - otlphttp receives OpenTelemetry logs over OTLP/HTTP, as protobuf or JSON,
//     as an http.Handler with no gRPC in the build.
//   - k8sobjects translates the records of the Kubernetes objects receiver: watch
//     events and list items for pods and nodes.
package ingest
