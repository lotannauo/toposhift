// Package xtengine is an [engine.Engine] over an XTDB 2.1 node, reached over its Postgres
// wire protocol with the pgx driver, so that the spike's reads can be asked of a database that
// has both time axes built in and checked against the reference engine.
//
// The mapping is that of package xtmodel, whose [xtmodel.Planner] turns each batch into the
// statements and whose [xtmodel.Model] is what this engine is held to: system time is
// ingest (a batch is one transaction, at a system time that is a function of its ordinal),
// and valid time carries each producer's lifecycle segments. The engine adds the SQL, the
// connections and what a node has that a model has not: another process, a wire, a clock of
// its own and an order on all its transactions.
//
// # Tables
//
// The documents, which package xtmodel names and this engine gives columns (the `_id` of a
// document is its first column and is a string, but for a meta document's, a bigint; see
// "The `_id` layout"):
//
//	seg_f, seg_r  layer, fp, peer, rel, producer, and a valid time: the stretch of valid time
//	              one producer's reference to an edge holds, read from the edge's source (seg_f)
//	              or target (seg_r); fp is the entity read from and peer the other end
//	ent           layer, fp, producer, and a valid time: the same for an entity
//	log_f, log_r  layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns,
//	log_e         payload: every record, one document each (log_e has no peer or rel), for
//	              Window and for a read at a token that no system time names
//	meta          kind, last_seq, sys_us, horizon_ns, past_end: one document per batch
//
// A fingerprint is its text form, a sequence number is stored with its top bit flipped (it is
// a uint64, and the conformance workloads cross 2^63), times of a log document are
// nanoseconds in a bigint and a segment's valid time is a timestamp, to the microsecond. A
// [Config.Prefix] goes before every table name, so that engines that share a node do not
// share tables.
//
// # Writes
//
// A batch is validated whole, planned, and stored by one transaction, opened with
// BEGIN READ WRITE WITH (SYSTEM_TIME = ...) at the base plus its ordinal in microseconds
// ([Config.Base], 2000-01-01 unless a test says otherwise), its statements in the planner's
// order, and the batch's meta document last. Ordinals count transactions, which are batches
// and retentions (see "Retention"). The planner, the counters, the token and the ordinal change
// only after the commit has returned and the batch's meta document has been read back at its
// own system time, so a batch the node refuses uses neither an ordinal nor a sequence number.
// If the document is not there, or the write has not finished by its deadline (five minutes),
// the outcome is unknown: the error is returned and the engine is not written to again, so
// that an indexer that has failed is found at the batch it failed on. A payload is sent as hex
// text, cast to a binary value, and never as bytes, because the driver writes bytes as quoted
// text and the node's indexer stops for good on a cast of text that is not hex. A batch with a
// boundary that is not a whole microsecond is refused whole with [engine.ErrInvalid] and
// counted ([Engine.Refused]).
//
// # Reads
//
// Every read is one BEGIN READ ONLY transaction on a connection of its own, so that its
// statements see one snapshot of whole committed batches. A neighbors or alive read at an
// instant is FOR VALID_TIME AS OF the instant (floored to a whole microsecond, which with
// every boundary on one gives the same answer) of the segment tables, and FOR SYSTEM_TIME AS
// OF the system time of the batch the token ended, unless the token is [engine.Latest]. A token at or
// above the last batch's but not Latest names that batch's system time too, so a batch that commits
// during the read is not in the answer. A token inside a batch, or before the first, names no system
// time: the log tables are read up to the token and folded in Go with the lifecycle
// specification ([ReadCounts].ClientFold). Window reads the log tables over all of valid time,
// as of a system time or, for a token inside a batch, up to the sequence number.
//
// # Reopening
//
// An engine opened over the tables an earlier one left finds, in the meta documents, the
// token, the ordinal of the last transaction, the system time each token names, the base of the
// system times and the retention horizon, and in the log tables the layer of each subject and
// what the planner remembers ([xtmodel.Rebuilder]). The base is kept with the node because a
// node orders all its transactions by system time: it refuses one at a system time before the last it
// committed, and an engine that opened with another base would be refused.
//
// # Retention
//
// Not mapped (see package xtmodel). Retain records the horizon and refuses later writes before
// it. A Retain that moves the horizon is a transaction of its own, at the system time of the
// next ordinal, that writes one meta document (kind retain): the horizon, and the sequence
// number the last batch ended at, unchanged. The token of that number then names the newest of
// the two transactions, so a read at it sees the state after the retention, which is the same
// data; and the horizon is on the node when Retain returns, so an engine opened afterwards has it,
// whether or not a batch followed. A Retain that does not move the horizon writes nothing.
//
// # What is established of XTDB 2.1.0, and how
//
// From the node's own code: the image the baseline pins carries the node's jar, whose
// Clojure sources are in it and whose Kotlin is read from its classes (revision
// ca78d5609dfa7393c465c1e1435a8f20fa3eb371).
//
//   - A transaction's system time is accepted unless it is before the system time of the
//     latest completed transaction: xtdb/indexer.clj, indexTx, compares it with that and
//     with nothing else, so a system time years before the node's clock is as good as one
//     after it. A transaction at an earlier time than the latest is aborted, and the abort
//     is written at the node's clock, which would put every later transaction at the
//     baseline's times before the latest: the engine never sends one, and a node is
//     started afresh for every run.
//   - COMMIT is synchronous: xtdb/pgwire.clj, cmd-commit, executes the transaction and
//     returns when it has been indexed, unless it was begun ASYNC. The grammar has no
//     COMMIT SYNC (xtdb.antlr.Sql$CommitStatementContext has the one keyword).
//   - The statements of a transaction are held by the connection until the commit, so a
//     rollback sends nothing to the node; the indexer applies them in the order given
//     (indexTx goes through the operations by index), an INSERT of a row with arguments
//     being a put over the row's valid time, and a DELETE a query of the rows the earlier
//     statements of the transaction made.
//   - The end of a portion delete is optional or null for the end of time: xtdb/sql.clj,
//     dml-stmt-valid-time-portion.
//   - Timestamps on the wire are microseconds: xtdb/pgwire/types.clj gives timestamptz the
//     type [:timestamp-tz :micro "UTC"].
//   - The local log is forced to disk after each write of messages, and nothing turns it
//     off: xtdb.api.log.LocalLog.writeMessages ends in FileChannel.force(true). If a
//     comparison with layout L is made, it is made with L synced.
//
// The `_id` layout. XT-iid, the layout the guide prefers, is not used. A UUID `_id` is
// used as the 16 bytes of the document's internal id and is not hashed (xtdb.util.Iid,
// getAsIid: a UUID is its bytes, a string, keyword or integer is truncated SHA-256),
// which XT-iid needs; but an `_id` range does not prune anything. The scan prunes the
// trie of a table by internal id only for a predicate that fixes the `_id` (`_id = x`, or
// the `_id`s a join finds; xtdb/operator/scan.clj, selects->iid-bytes and ->path-pred),
// and prunes pages by the minimum and maximum of a column's values only for numbers and
// temporal types: the metadata test of a comparison is true for a UUID (xtdb/expression/
// metadata.clj, the :_meta_double call is null for the :any types). So a range on a UUID
// reads every page of the table, and the layout XT-plain is registered: an `_id` that names
// the document, and reads that filter on the columns, whose blooms prune pages.
//
// # Found against a live node
//
// Each of these is a test that asks a live node and fails if it behaves otherwise
// (probe_server_test.go, TestProbe...), run against the image the baseline pins started by
// scripts/xt-server.sh. The node of the run these were found on was given less memory than
// the registered configuration (JVM options -Xmx2g -XX:MaxDirectMemorySize=2g, and a memory
// cache of 512 MiB in a copy of xtdb.yaml, through TOPOSHIFT_XTDB_JAVA_OPTS and
// TOPOSHIFT_XTDB_CONFIG), because other database containers shared the machine's VM. These are
// probes of behaviour, whose outcomes do not depend on those sizes, and no registered figure
// (no count, size or time) comes from them.
//
//   - A transaction opened at a system time in 2000 is accepted on a node whose clock reads
//     2026, and the version it makes has that system time to the microsecond. Opening it with
//     a TIMESTAMP literal, a string literal or an offset literal works; an argument does not
//     (BEGIN READ WRITE WITH (SYSTEM_TIME = $1) fails). A transaction at a system time before the
//     latest is aborted, as the source says (the node logged it).
//   - Statement order: in one transaction an insert over [10, 100), a portion delete of
//     [20, 30) and an insert over [25, 40) leave the document absent over [20, 25) and present
//     elsewhere, so statements apply in the order given; the versions it made carry the
//     transaction's system time and none is closed by it, so the states in the middle of a
//     transaction leave no history. An insert over an existing `_id` replaces only its
//     stretch of valid time, and as of the earlier system time the earlier value is read.
//   - A portion delete of a document that is not there, of a table that is not there, and one
//     with a null end, all commit and do what they say. A read of a table that was never
//     written is an empty answer. FOR SYSTEM_TIME AS OF the system time of a transaction sees
//     it, and as of a microsecond before does not. After a commit has returned, a read on
//     another connection sees it, every time (400 reads). COMMIT SYNC is a syntax error.
//   - Temporal columns are microseconds, and a boundary with finer digits is truncated to
//     the microsecond, silently: a segment [10.000000500, 11.000000900) is stored as
//     [10.000000, 11.000000). That is why a boundary off a whole microsecond is refused, and
//     not written. A write the node cannot plan (one with no `_id`) is refused when the commit
//     runs, with nothing written; one that the indexer cannot execute (a cast of text that is
//     not hex to a binary value) stops the node's ingestion for good, and the node
//     answers nothing after it (the commit that met it says "Ingestion stopped: ...", the log says
//     "Ingestion stopped for 'xtdb' database", and a later write's commit fails with "Parent job is
//     Cancelled"): every statement the engine sends is one the node accepts.
//   - Query modes. The driver's default (cached statements) and its describe mode fail every
//     write with "mismatched param and argument count": the node describes a write's
//     parameters as none. Its exec mode sends no parameter types, and a write needs them in the
//     parse message ("Missing types for args", a failure in the node). Only the simple
//     protocol works, and it is the registered mode. It writes every argument as a quoted
//     string, so a number is stored as text unless it is cast: the engine casts an integer to a
//     bigint and bytes (as hex text) to a binary value, and stores a bool as 0 or 1 because no
//     string casts to one. A timestamp as a quoted string is accepted for a valid-time bound
//     and for a FOR VALID_TIME or FOR SYSTEM_TIME clause. min() works on a bigint, not on text.
//   - A batched read is `fp IN (...)` with an argument for each entity. An array argument
//     (`= ANY($1)`, `UNNEST($1)`, the left join lateral over an ordinality) arrives as text
//     under the simple protocol and matches nothing, so those forms, which the guide proposed,
//     cannot be used in the registered mode.
//   - A read whose plan was made against fewer columns than the table has by the time it
//     runs fails with "cached plan must not change result type". It happens to a read that
//     races the first writes to a table, and the engine makes the read again (up to five
//     times).
//   - EXPLAIN ANALYZE reports, per operator, two durations and two integers (the page count
//     and the row count). The durations are not repeatable and the counts are 1 page and the
//     rows of tables of a few hundred documents; nothing was established that makes them a
//     read-work counter, so none is claimed ("no read-work counter" is the capability row).
//   - A UUID `_id` is accepted (as CAST(... AS UUID)), compared as a UUID, and found by a
//     range, a BETWEEN and an equality, with the answers the predicate gives; its cost was not
//     measured, and the source says why a range is not cheaper than a filter.
//   - A transaction that is rolled back leaves nothing, and a write with no `_id` fails at the
//     commit (not at the statement), after which the connection is usable.
//
// What the engine did against the node (TestServer...): the spike's conformance checks on a
// fixed workload with two retentions, and the scripted checks (write and read contracts,
// instants, producers, relations); the first gate on two tiny plans, with and without a
// retention, every answer equal to the reference engine's and the counts equal to the model's,
// no read answered by a client fold; the system time the node gave each batch equal to the one
// asked for; the ends of the range against the model; the spike's reopen check on the tiny
// workload with two retentions and three reopenings; and concurrent reads. Of the spike's
// checks, CheckExtremes is not run (its last record is not on a whole microsecond), and the
// full tier of the conformance test is not (the node's latency per record and per read
// makes it hours).
package xtengine
