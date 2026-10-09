// Package pebblestore is the store of layout L on Pebble: a log per entity,
// direction and layer, in Pebble's own bytewise key order, with the event time and
// the sequence number inverted in the key so the newest record is first. It
// implements [store.Store], and boots with no configuration beyond a directory
// ([DefaultOptions]).
//
// # What is stored
//
// Every record is stored, never overwritten, because a query pinned to an earlier
// snapshot token must see the records it would have seen. The key is the prefix
// (layer, entity fingerprint, direction) and then the inverted event time and the
// inverted Seq, so records at one instant are told apart by their Seq alone and a
// write needs no read. The peer, the relation and the producer are in the value.
// An edge is stored twice, under its source's forward prefix and its target's
// reverse prefix, payload and all, and an entity's existence once, under its own
// prefix with direction 0. A batch is one commit, with the new last sequence
// number in it. See key.go for the key and value.go and stamp.go for the values;
// the meta keys, which are outside the data keyspace, are in store.go and
// retain.go.
//
// The stored format is frozen: the key, the record value, the stamp and the meta
// are the ones named by the format tag, and the golden vectors in the tests are
// what changing any of them has to answer to.
//
// # How a read works
//
// A read at instant t and token asOf seeks to the newest record at or before t in
// one prefix and walks toward older ones. For each reference (peer, relation,
// producer) the first record the token sees decides it ([pebblekv.Value.Holds]);
// a subject is alive if any producer's reference holds. The walk ends at the
// retention baseline, which stands for everything before the horizon. With no
// checkpoints a read is as long as the history older than t in its prefix.
//
// The reader also understands checkpoints, derived summaries of a prefix's history
// that a read may stop at. The writer keeps a policy ([CheckpointOptions]) and
// writes them; see checkpoint.go for what keeps them true. They are never facts:
// losing one is harmless, and a stale one is a wrong answer, so a record that makes
// one untrue deletes it in the same commit.
//
// # Retention
//
// Retain(h) publishes the horizon, then rewrites each prefix that has history
// before h: it replays it, writes a baseline at h holding every reference that is
// still alive at h, and range-deletes everything older, the delete and the baseline
// in one commit. The baseline is not derived data: it is the only record of the
// state before h. With Config.SettleRetention the database is then flushed and
// waited on until its compactions have run. Retention is synchronous:
// [Synchronous] is the only [RetentionMode].
//
// The rewriting goes in key order, in chunks that end between prefixes (at 4 MiB
// or 20 ms, whichever comes first), each read through an iterator of its own and
// committed, not synced. The commit that publishes the horizons also writes a
// marker, the meta key "retain": the generation of the retention, the layers it
// moves with their horizons and last sequence numbers, where the rewriting
// resumes, and whether it is rewriting or only settling. Every chunk moves the
// resume key to the first key after its last prefix in the very commit that
// rewrote the prefixes before it, so that whatever a crash keeps of the log, a
// prefix before the resume key is rewritten whole and one at or after it is as it
// was. The marker is derived, never a fact; it is deleted when the retention ends,
// and only by the generation that wrote it. A store opened with a marker present
// finishes that retention before Open returns, from the resume key, and with the
// writer remembering nothing; a marker that disagrees with the horizons stored
// beside it is refused. Running a pass again over prefixes it has rewritten changes
// no key. A Retain with a later horizon while a marker is present starts again from
// the first key, under the next generation.
//
// # Quarantine
//
// A store opened with a policy that names a boot key reports two live boots of
// one host as a [store.QuarantineError], decided by folding everything the scope
// sees of the entity. Two interim rules make that correct and simple:
//   - Alive reads the whole history of the entity (its records and the baseline's
//     entries) and folds it, where without a boot key it stops at the first live
//     reference. Its cost grows with the entity's history.
//   - Retain does not shorten the prefix of an entity if a record it would discard
//     carries a boot (counted as "retain.prefixes_kept_for_boots"): the baseline
//     keeps no boot history, so the records stay, and every later collision is
//     judged against them. The history of a host with boots is never reclaimed.
//
// Because Alive folds the whole prefix, no read would use a checkpoint of an
// entity's own prefix, so under a boot key the checkpoint policy places none there.
// This is a choice of cost, not one of the rules above. Edge prefixes are
// unchanged, and a checkpoint that exists on an entity prefix (written by the
// instrument) is still deleted when a record makes it untrue.
//
// # Durability
//
// With Config.Sync, record commits and the commit that publishes a horizon are
// synced. The commits that write checkpoints, which are derived, and the commits
// that rewrite history below a published horizon are not: the log is replayed as a
// prefix, so a checkpoint is lost only together with everything committed after
// it, never while a later record that deleted it survives. Without it, no commit
// is. Pebble applies a batch to its memtable before it
// reports a failed sync, and replays a batch written to its log after a failed
// apply, so an error from a record commit does not say whether the batch is
// stored: the store sets LastSeq to what it shows, refuses further writes and
// retentions (reads continue), and leaves the outcome to the log's replay when it
// is reopened. Pebble itself ends the process on such an error (see package
// pebblekv), so this is the last line of defence.
//
// # Not here yet
//
// The asynchronous retainer (a lock released between chunks, a rewrite when a write
// touches a prefix the pass has not reached, a background settle), retention
// offsets and kept layers, and a boot history kept in baselines are later changes.
//
// Every layer has a horizon of its own ([Store.LayerHorizon]), stored under its own
// key and judged on its own by reads and writes, but with no offsets or kept layers
// they all move together.
package pebblestore
