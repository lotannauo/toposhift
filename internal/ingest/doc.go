// Package ingest is the entry of collector output into the store. Each
// subpackage turns one kind of collector record into store records without a
// sequence number; the sequencer that orders them and writes them to the store
// is separate.
//
// The subpackages translate and nothing else. A translator takes one log record
// at a time, keeps only what it needs to tell a changed description from an
// unchanged one, and returns the records the record stands for. It never reads a
// clock: every time it returns comes from the record it was given. Two
// translators given the same records in the same order return the same records.
//
//   - k8sobjects translates the records of the Kubernetes objects receiver: watch
//     events and list items for pods and nodes.
package ingest
