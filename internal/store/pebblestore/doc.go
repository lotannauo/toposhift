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
// that a read may stop at; this version never writes one.
//
// # Retention
//
// Retain(h) publishes the horizon, then rewrites each prefix that has history
// before h in one commit: it replays it, writes a baseline at h holding every
// reference that is still alive at h, and range-deletes everything older. The
// baseline is not derived data: it is the only record of the state before h. With
// Config.SettleRetention the database is then flushed and waited on until its
// compactions have run. Retention is synchronous: [Synchronous] is the only
// [RetentionMode].
//
// # Quarantine
//
// A store opened with a policy that names a boot key reports two live boots of
// one host as a [store.QuarantineError], decided by folding everything the scope
// sees of the entity. Two rules make that correct and simple, and both are
// interim:
//   - Alive reads the whole history of the entity (its records and the baseline's
//     entries) and folds it, where without a boot key it stops at the first live
//     reference. Its cost grows with the entity's history.
//   - Retain does not shorten the prefix of an entity if a record it would discard
//     carries a boot (counted as "retain.prefixes_kept_for_boots"): the baseline
//     keeps no boot history, so the records stay, and every later collision is
//     judged against them. The history of a host with boots is never reclaimed.
//
// # Durability
//
// With Config.Sync, record commits and the commit that publishes a horizon are
// synced. The commits that rewrite history below a published horizon are not.
// Without it, no commit is. Pebble applies a batch to its memtable before it
// reports a failed sync, and replays a batch written to its log after a failed
// apply, so an error from a record commit does not say whether the batch is
// stored: the store sets LastSeq to what it shows, refuses further writes and
// retentions (reads continue), and leaves the outcome to the log's replay when it
// is reopened. Pebble itself ends the process on such an error (see package
// pebblekv), so this is the last line of defence.
//
// # Not here yet
//
// Checkpoints are never written, so the writer keeps no per-prefix state; the
// asynchronous retainer, retention offsets and kept layers, and a boot history
// kept in baselines are later changes.
//
// Every layer has a horizon of its own ([Store.LayerHorizon]), stored under its own
// key and judged on its own by reads and writes, but with no offsets or kept layers
// they all move together.
package pebblestore
