// Package duckmodel is an in-memory model of the Parquet files the DuckDB baseline
// reads, with no DuckDB, no driver and no file: what each arm's files hold, which rows
// a read of them finds, and the answer that follows. It is the specification the SQL of
// a DuckDB engine is held to, row for row, and it is checked against the reference
// engine by the spike's own conformance test, so that a SQL statement that differs from
// the model is a statement that differs from the contract.
//
// # The files
//
// A record file holds the records of a stream, one row each: the columns of an activity
// file (layer, subject kind, source, target, relation, producer, event time, ingest
// sequence number, kind, TTL, through time, payload), with the two fingerprints as their
// text ([Text]). One copy is ordered by source and one by target, so that a forward and
// a reverse read each find their rows as a prefix. An interval file holds, for each
// subject, the maximal runs of existence [lifecycle.Fold] derives from those records, as
// [IntervalRow]s. The model keeps a subject's records and, when asked, its intervals;
// a row's place in a file's order is not part of what it answers.
//
// # The read rule
//
// A subject holds at an instant t under a snapshot token S if, for some producer, the
// newest record with event time at or before t and Seq at or below S, ties at one
// instant going to the higher Seq, is an observation that has not lapsed: its TTL is
// zero, or the later of its event time and its through time, plus the TTL, is after t.
// That is the lifecycle specification's rule for one producer's reference, read off
// the records without a fold; [Model] answers every read of the records arm with it.
//
// An edge is alive if its subject holds, and a neighbor read finds the edges of the
// layer that touch the entity at the given end. Window reads the records themselves.
//
// # Two arms
//
// [ArmRecords] answers every read from the records, which is what a record file, and
// DuckDB's own tables loaded from one, can do for every token. [ArmIntervals] is the
// interval file with the record files behind it: a read as of the newest token is
// answered from the subject's intervals, and a read as of an older token, which an
// interval file cannot answer (an interval has lost which records made it), and every
// Window, from the records. An interval file answers neither a late event nor an
// append without being rewritten, so the arm refolds a subject when its records change.
//
// # Fingerprint order
//
// The text of a fingerprint does not sort as [engine.CompareFingerprints] does: a type
// that is the prefix of another, followed by a byte below the colon (the dot of
// "k8s.pod" is one), sorts after its longer neighbor as text and before it as a
// fingerprint. An answer is therefore never ordered by the database: [NeighborsFromText]
// parses what a statement returns and orders it as the contract does.
//
// # Retention
//
// A retention at a horizon keeps the records at or after it, and for each subject and
// producer the last record before it, the baseline that stands for the history it
// replaces. [Filter] is that rule over a whole stream, for a builder that writes a file
// from the stream; [Model.Retain] is the same rule applied to what the model holds.
package duckmodel
