// Package xtmodel is an in-memory model of the XTDB tables the XTDB baseline stores a
// stream in, with no database server and no driver.
//
// XTDB 2 is bitemporal: every document has a valid time, which the writer chooses,
// and a system time, which the database assigns to each transaction. The baseline
// maps toposhift's two axes onto them natively. System time is ingest: a batch is one
// transaction, at a system time that is a function of the batch's ordinal
// ([SystemTime]), so a read of the world as of an ingest token is a read FOR
// SYSTEM_TIME AS OF the batch the token ended. Valid time is event time, and what it
// carries is each producer's lifecycle segments: from an Observe until the same
// producer's next assertion about the subject, or its deadline, whichever comes first.
//
// The segments are written by a [Planner], which turns a batch into the transaction
// that makes the documents say so; the database decides nothing. A [Model] is a
// bitemporal store that applies a planner's transactions with the semantics XTDB is
// taken to have for the statements the planner uses (an insert over a range of valid
// time overwrites that range, a portion delete removes it, and what a statement
// replaces stays readable at the system times before it; see "Unverified" below) and
// answers the engine contract's reads with the queries the baseline will send. So
// that:
//
//   - the planner is checked against the reference engine, by the spike's own
//     conformance test and by comparing the segments with [lifecycle.Fold], with no
//     server;
//   - a server adapter can be checked against the Model, statement for statement;
//   - the number of statements a stream takes is a function of the stream, the batch
//     boundaries and the planner, and can be counted anywhere ([Counts]).
//
// # Tables
//
// seg_f and seg_r hold the segments of edges, one document per edge, producer and
// direction; ent holds those of entities; log_f, log_r and log_e hold every record,
// one document each, for Window (which returns every stored record, overwritten ones
// too) and for a read at a token that no system time names; meta holds one document
// per batch, the sequence number it ended at, from which the token to system time map
// is rebuilt when an engine is opened again.
//
// # Reads
//
// A neighbors or alive read at an instant is a read of the segment tables FOR
// VALID_TIME AS OF the instant, with FOR SYSTEM_TIME AS OF the batch's system time
// when the token ended a batch and no clause for the newest. A token inside a batch,
// or between two, has no system time: the read is answered from the log tables by
// folding in Go with the lifecycle specification, counted separately
// ([ReadCounts]). Window reads the log tables over all of valid time.
//
// # Times
//
// The model holds nanoseconds. XTDB's temporal columns are microseconds, so the planner
// counts the records that have a boundary off a whole microsecond
// ([Counts].OffMicrosecond), which a server adapter cannot write and refuses; the plans of
// the spike have none.
//
// # Assumed of the node
//
// The model is exact about XTDB only in the sense that it assumes the following of it, and
// package xtengine's tests (TestProbe...) ask a live node about each of them and fail if it
// behaves otherwise; what each was found to do is recorded in that package's documentation.
//
//   - a transaction may be given a system time earlier than the server's clock
//     ([BaseSystemTime]); if not, the base becomes the server's clock at the first
//     batch, with the same offsets;
//   - temporal columns hold microseconds;
//   - statements in one transaction apply in order, so a later statement over an
//     overlapping valid-time range wins;
//   - a state in the middle of a transaction leaves no system-time history;
//   - an INSERT on an existing _id overwrites only its valid-time range;
//   - a portion delete of an absent document is a no-op;
//   - FOR SYSTEM_TIME AS OF sys includes the transaction committed at sys;
//   - a read with no clause sees the newest transaction once the commit has returned.
//
// If statements are not applied in order, the line the planner paints ([merge]) can be
// written as disjoint statements instead, one insert or delete per stretch, which need
// no ordering: that is the fallback for the server adapter.
//
// # Retention
//
// Not mapped. XTDB's only removal is ERASE, which removes a document for all valid and
// system time, so one document's old valid-time portions cannot be dropped while its
// later ones stay. [Model.Retain] records the horizon, refuses later writes before it
// (the contract) and frees nothing, which the contract allows.
package xtmodel
