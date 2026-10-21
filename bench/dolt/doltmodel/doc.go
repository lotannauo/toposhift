// Package doltmodel is an in-memory model of the tables the Dolt baseline stores a stream
// in, and of the commits that version them, with no database server and no driver.
//
// The baseline puts the skeptic's question to Dolt in the shape the skeptic gives it: keep
// a current-state table, commit after every batch, read any past state with AS OF, and
// there is history for free. Answering the plan's questions about the past needs more than
// that table, because it holds each producer's newest record and nothing before it, so the
// design also keeps an append-only log of every record. This package is that design as
// data and rules, so that:
//
//   - the rules are checked against the reference engine by the spike's own conformance
//     test, with no server;
//   - a server engine can be checked against the model, row for row ([Tables], [StateAt]);
//   - the counts of rows a stream writes, replaces and deletes, and of the reads each table
//     answers, are functions of the stream and the questions, and can be had anywhere.
//
// # Tables and encodings
//
// The edge log has a row per record and direction, keyed by (layer, direction, a, peer,
// relation, producer, event time, seq); the entity log a row per record, keyed by
// (fingerprint, producer, event time, seq). The state tables hold, per producer and subject,
// the newest record by (event time, seq) and the deadline at which it stops holding the
// subject: none for an Observe without a TTL, the later of its event time and Through plus
// the TTL for one with a TTL, and its event time for a Delete. One meta row says the highest
// sequence number committed, the retention horizon and the schema version. See [LogRow],
// [StateRow] and [Meta], and [Schema] for the columns and keys as data. The entity tables
// hold the layer as a value column outside the key, and a read filters on it.
//
// Fingerprints are their type name, a zero byte and their 16 hash bytes, so that bytewise
// order is engine.CompareFingerprints ([EncodeFingerprint]); times are Unix nanoseconds in
// a BIGINT, never a DATETIME ([Nanos]); sequence numbers have their sign bit flipped, so a
// signed comparison keeps the order of the uint64 ([EncodeSeq]); and a result that a
// database column of fingerprints would sort is sorted in Go, never by their text form.
//
// # Writes, commits and retention
//
// [Model.Write] validates the whole batch, then inserts its records in the logs, replaces
// the state rows whose record is newer than the stored one (the writer's in-memory map of
// each key's newest record decides, and a late record changes no state), and, in the arm
// with commits, makes a commit before the sequence number rises. Every commit, batch and
// retention alike, has an ordinal counted from 1, a message ("seq <last>" or "retain
// <horizon> seq <last>") and the synthetic date [CommitDate] of its ordinal, so that history
// is a function of the stream. A token maps to the newest commit made at it, the retention's
// when one follows its batch.
//
// [Model.Retain] deletes the log rows before the horizon but the newest of each key (the
// baseline), and the state rows whose deadline is at or before it, and commits. Earlier
// commits still hold what it deleted, so history truncation is not modelled and no bytes
// are freed by it in the arm with commits.
//
// # Reads
//
// A neighbors, batched or existence read is answered by one rule, the same whatever the age
// of the question ([PathKey]). The state table is read as the commit at the read's token left
// it. Each state key, a subject in the direction asked and one producer, is then settled by
// its row if the row's event time is at or before the instant (the row is the key's newest
// record, so it is the newest at or before the instant), and answered from the log, the newest
// record at or before the instant with a sequence number at or below the token, if the row is
// after it. A key with no row holds nothing from the horizon on. A token that names no commit
// (one inside a batch, or any but the newest in the arm without commits) has no state to read,
// every key is answered from the log, and the read is counted on its own: none is expected of
// a plan's reads in the arm with commits. Windows always read the log. [Model.Paths] counts,
// for each kind of read, the keys settled each way and the reads that the state as of a commit
// answered alone. The rule and the log path agree for every token and every instant from the
// horizon on, which the per-key-equals-log property tests.
//
// [CommitTime] is the diagnostic arm: the state as the newest commit whose frontier is at
// or before the instant left it, the shortcut of reading a commit's place in history as an
// event time. It exists to count the answers that shortcut gets wrong.
//
// # Unverified about Dolt
//
// The model decides nothing about how Dolt behaves, and these facts the design relies on
// are not verified here. The server engine (package doltengine) checks each against Dolt 2.4.2
// and records the results in its package documentation:
//
//   - [U] that a window function (ROW_NUMBER over a partition) is available and correct in
//     the pinned version, for the log path's SQL; otherwise the same rule runs over the
//     ordered rows in Go;
//   - [U] that AS OF is accepted on one table reference beside another in one statement,
//     otherwise the state and the log are read by two statements in one read-only
//     transaction;
//   - [U] that JSON_TABLE with FOR ORDINALITY is available, for the batched read;
//   - [U] the syntax of an index hint, if the planner does not choose the event-time index
//     for a window;
//   - [U] whether a Dolt commit made inside the open transaction makes the SQL commit and the
//     Dolt commit one step; either way the sequence number rises only after both return;
//   - [U] that two loads of one stream, with the synthetic author and dates, end at the same
//     head commit hash, and that the bytes of a data directory are equal across loads;
//   - [U] that garbage collection frees nothing that an earlier commit references;
//   - [U] that primary keys this long are accepted and indexed: a fingerprint is
//     VARBINARY(MaxNameLen+1+16), a relation and a producer VARBINARY(255), and the log's
//     key is about 1 KB;
//   - [U] that DOLT_COMMIT accepts the author and the date as given, which the
//     determinism of the head commit hash depends on;
//   - [U] that AS OF '<hash>' returns a table exactly as that commit left it, the rows
//     deleted after it included;
//   - [U] that REPLACE INTO on the state key behaves as the model's replace;
//   - [U] that the token-to-commit map and the frontiers can be rebuilt from the commit log
//     and the meta row on reopening.
//
// A model commit's ID is the model's own, a hash of what the commit holds; it is not and
// does not predict a Dolt commit hash.
package doltmodel
