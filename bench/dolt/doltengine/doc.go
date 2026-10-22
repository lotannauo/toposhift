// Package doltengine is an [engine.Engine] over a Dolt 2.4 server: the tables of package
// doltmodel, written in one transaction and one Dolt commit per batch and read with the
// rule the model implements, so that the spike's reads can be asked of a real Dolt and
// checked against the reference engine and, statement for statement, against the model.
//
// The baseline puts a skeptic's question to Dolt in the shape the skeptic gives it: keep a
// current-state table, commit after every batch, read any past state with AS OF. The
// state table holds each producer's newest record, so the design also keeps an append-only
// log of every record, because a correct answer at a past event time needs the records
// before the newest. This package is that design on a server. The model ([doltmodel]) is the
// pre-registered rule; this engine implements exactly it, and the tests hold it to it.
//
// # What a batch writes
//
// A batch is validated whole before anything is sent ([writer.validate]: each record valid,
// a producer and a payload fit their columns, no event time before the retention horizon,
// sequence numbers rising, a subject in one layer), so a refused batch sends nothing. Then,
// in one transaction on the writer's connection:
//
//   - a multi-row INSERT of every record into the log, an edge in both directions;
//   - a multi-row REPLACE INTO the state table of each key the batch touches whose newest
//     record in the batch is newer, by (event time, seq), than the stored row. The writer
//     knows the stored rows from an in-memory map of each key's newest record, rebuilt from
//     the state tables when the engine is opened again, so a late record changes no state
//     and costs no read;
//   - the meta row with the new token;
//   - CALL DOLT_COMMIT('-A', '-m', 'seq <last>', '--author', <synthetic>, '--date',
//     <synthetic>), in the arm with commits, which stages every table and returns the
//     commit's hash.
//
// Then the transaction ends, and the sequence number rises, only after both have returned
// ([Engine.LastSeq]). The author is neutral ([Author]) and the date of the n-th commit is
// [doltmodel.CommitDate] of n, a retention's commit counted like a batch's, so that the
// history of two loads of one stream is the same and they end at the same head commit hash.
// The engine keeps the hash of the newest commit at each token ([Engine.Commits]) and
// rebuilds the map from the server's commit log when it is opened again, refusing a log whose
// commits are not the stream's in message and date.
//
// [Engine.Retain] is one transaction too: the log rows before the horizon are deleted but
// the newest of each key, the state rows whose deadline is at or before the horizon are
// deleted (a Delete's row carries its event time as the deadline, so it goes), the horizon
// is set, and, in the arm with commits, a commit "retain <horizon> seq <last>" is made.
// Earlier commits still hold what it deleted, so nothing is freed by it while they do.
//
// # Reads
//
// A read is one statement in one read-only transaction, so it is of one snapshot. The state
// table is read as of the commit the token names: the current table if the token is at or
// above the newest sequence number, the newest commit at the token otherwise, and no state at
// all for a token that names none (inside a batch, or any but the newest in the arm without
// commits), when every key is answered from the log. Each state key, a subject in the
// direction asked and one producer, is then settled by its row if the row's event time is at
// or before the instant, and by the log if it is after: the newest record at or before the
// instant with a sequence number at or below the token, by ROW_NUMBER over the key, outer-joined
// so that a key with no such record is still a key and holds nothing. A key with no row holds
// nothing. The statement returns, for each peer and relation, how many keys the rows settled,
// how many it sent to the log and whether any holds, so that the answer and its counts are
// one read ([Engine.Paths] counts them as the model does). A batched read is one statement over
// JSON_TABLE of the fingerprints with FOR ORDINALITY. A window reads the log through the index
// on event time. Every list is sorted in Go, by the contract's order, never by the text form of
// a fingerprint.
//
// A read holds the engine's view of the database still for the whole of it: a batch or a
// retention takes the same lock, for writing, only for the step in which its Dolt commit
// returns, its transaction ends and the engine's memory moves. So a read at the newest token
// never sees a batch whose commit has not returned, and one at an older token reads that
// token's commit, whatever is being written.
//
// # The server
//
// scripts/server.yaml is the one source of every setting, and scripts/dolt-server.sh starts
// the digest-pinned image with it. [Open] refuses a server that is not Dolt 2.4.2 or whose
// observable settings differ from the file's ([CheckServer], [ObservableSettings]); a test
// holds the table of settings to the file. The driver sends prepared statements
// ([InterpolateParams] is false), one connection for the writer and one for the reader, and
// no statement is sent outside a transaction, because the server does not commit on its own.
//
// # Facts about Dolt, and the tests that check them
//
// Each was checked against the registered server (Dolt 2.4.2, the digest-pinned image, with
// scripts/server.yaml), [V] for a fact that held and [F] for one that did not. No fact that a
// registered rule depends on was falsified. The tests are the record, and run again in the
// server job of dolt.yml.
//
//   - [V] ROW_NUMBER over a partition, ordered newest first, numbers a tie at one instant by
//     the higher sequence number: TestFactWindowFunctionsAreAvailableAndCorrect;
//   - [V] AS OF is accepted on one table reference beside another, as "FROM t AS OF 'h' alias",
//     in a join, in a derived table that is outer-joined to a second table, and in a union,
//     and reads that commit's rows while the other table is read as it is now:
//     TestFactAsOfWorksBesideAnotherTableReference;
//   - [V] AS OF '<hash>' returns the rows deleted and replaced after that commit, none added
//     after it, and not the working set's uncommitted rows:
//     TestFactAsOfACommitIsTheTableAsThatCommitLeftIt;
//   - [V] REPLACE INTO replaces the whole row of a key, a NULL deadline included, and leaves
//     other keys alone: TestFactReplaceIntoReplacesTheRowOfTheKey;
//   - [V] JSON_TABLE with FOR ORDINALITY takes its document as a prepared statement's argument
//     and joins to a binary column through UNHEX, keeping ordinals, repeats, missing keys and
//     a trailing zero byte: TestFactJSONTableWithOrdinalityAndAPreparedArgument;
//   - [V] the registered tables, whose largest key is over a kilobyte, are created; a row of
//     the longest values is stored and read back; binary keys compare as bytes (a prefix sorts
//     first, a trailing zero byte is not padding): TestFactLongPrimaryKeysAreAcceptedAndOrderedByBytes,
//     and the scenarios "the longest keys" and "fingerprint order", which also compare every
//     table, in key order, with the model's;
//   - [V] DOLT_COMMIT takes the author and the date as given, and the date is the same
//     whatever the session's zone is when it is written with its zone:
//     TestFactACommitIsMadeAtTheGivenAuthorAndDate, TestFactTheDateIsNotTheServersZone. In
//     dolt_log the author is the "committer" column;
//   - [V, with a correction] the window read uses the index on event time with no hint when the
//     table is empty. EXPLAIN FORMAT=TREE shows it by its columns; a plain EXPLAIN returns no
//     rows in this version, and takes no arguments in a prepared statement:
//     TestFactTheWindowReadUsesTheEventTimeIndex. On a loaded table the planner chose the primary
//     key for some windows of a plan and the index for the others, and kept the primary key
//     when the statement named the index with FORCE INDEX or USE INDEX (both are accepted and
//     do not bind). The primary key's range for such a window is the subject's rows, with no
//     bound on the event time that a seek can use. The statement is left as it is, and
//     [Engine.ReadStatements] and [Engine.Explain] give the plan of each window as loaded, which
//     the loader's explain command records;
//   - [V] a reader sees none of an open transaction's rows, and a read-only transaction keeps
//     its snapshot while another connection commits:
//     TestFactReadersDoNotSeeAnOpenTransactionAndKeepTheirSnapshot;
//   - [V] a Dolt commit inside the open transaction ends the SQL transaction with it: another
//     connection sees the commit and its rows before the transaction's own COMMIT, and the
//     working set is clean after. The commit and the rows are one step, which is what the
//     engine's order (statements, then DOLT_COMMIT, then COMMIT) relies on; the engine does not
//     depend on it, because a read holds its view still while the step ends:
//     TestFactACommitInsideTheTransactionEndsWithIt;
//   - [V] prepared statements are accepted for every statement sent with arguments (a call
//     with none is sent as text): TestEveryStatementOfTheEngineRunsAsAPreparedStatement;
//   - [F, then V] two loads of one stream end at the same head commit hash only if the commit
//     the server makes when it creates a database is amended: it carries the server's time and
//     a system author, so the first version of the engine ended at different heads. The engine
//     now runs DOLT_COMMIT --amend with the synthetic author and the date of ordinal 0
//     (2000-01-01T00:00:00Z) and the message [InitMessage] before it creates the tables, and
//     the heads, and every commit's hash, author, date and message, are equal across loads:
//     TestTwoLoadsOfOneStreamEndAtTheSameHeadCommit;
//   - [F, then V] the retention's DELETE ... WHERE EXISTS (a correlated subquery over the same
//     table) is accepted and correct, but quadratic: it did not finish on a hundred thousand
//     log rows. It is now a multi-table DELETE joined on the whole key to the rows that
//     ROW_NUMBER numbers after the newest of their key before the horizon (a few seconds on
//     128,000 rows, the same rows deleted): the "retention" scenario and the plan tests with a
//     retention;
//   - [F, then V] GREATEST of two BIGINT columns plus a BIGINT is refused as "BIGINT value is
//     out of range" when the sum is exactly the largest BIGINT, which a record whose deadline
//     is the last representable instant has (the spike's "extremes" check found it, on the arm
//     without commits). The later of the two times is a CASE, which is exact:
//     the scenario "extremes" holds both arms to the model on such deadlines;
//   - [V] the token-to-commit map and the writer's state are rebuilt on opening from the commit
//     log (columns commit_hash, message, date, commit_order) and the tables, through
//     conformance's reopen check and the scenarios; the commit's frontier is not rebuilt (it
//     is not kept);
//   - [V, with a correction] behavior.autocommit in server.yaml sets the default of every new
//     session (@@autocommit is 0), not the global variable (@@global.autocommit stays 1), so
//     CheckServer reads the session's. behavior.dolt_transaction_commit and
//     listener.max_connections read as written;
//   - [V] the container configuration: the image's entrypoint takes exactly one *.yaml from
//     /etc/dolt/servercfg.d as the server's --config and exactly one *.json from
//     /etc/dolt/doltcfg.d as the global configuration (metrics.disabled is then true), and
//     DOLT_ROOT_HOST=% lets root reach the published port with no password. The entrypoint
//     says "Ready for connections" only once both are in place, which dolt-server.sh waits for.
//     Started with no network but loopback, the server starts, answers and commits, and
//     /proc/net/tcp shows only its listener and loopback sockets.
//
// The server deletes a dropped database's files only when it is purged
// (CALL dolt_purge_dropped_databases(), present in 2.4.2): [DropDatabase] purges, so the data
// directory of a test run is its live databases and not every one that was dropped. Over
// the scenarios, the plan tests, the mutants, the facts and the random streams it was 1 MB
// after each group, and at most 0.8 GB inside the control arm's conformance test, whose
// databases are dropped when it ends.
//
// Still unchecked, and not this package's: that garbage collection frees nothing an earlier
// commit references, and that the bytes of a data directory are equal across loads.
//
// # The diagnostic, statements, plans and garbage collection
//
// The frontier of each commit (the largest event time among the records at or below its
// token) is not kept in the database: the rule never reads it. The loader computes it from the
// stream it writes, and [CommitTime] reads the commit arm's database by it, as if a commit's
// place in history were an event time, which is the diagnostic arm; it is held to the model's
// ([doltmodel.CommitTime]) answer for answer. [Engine.ReadStatements] gives the statements a
// question sends (built by the functions the reads call) and [Engine.Explain] the server's
// plan for one, EXPLAIN taking no arguments, so they are written into it ([Literal]).
// [GC] runs garbage collection on a database, which the server documents as breaking every
// open connection of it, and so refuses ([ErrBusy]) a database that has any other connection
// open, as the processlist says.
//
// # What is not here
//
// Timings, the bytes of the data directory (the loader's command measures them), and the
// counters and digests of the registered plans are the loader's and the timed jobs'.
package doltengine
