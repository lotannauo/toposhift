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
// what changing any of them has to answer to. The tag names stamp format 2, which
// added the baselines that carry a boot state (see Quarantine below); a database
// whose tag names format 1 holds none, is read as it is, and has its tag rewritten
// by the first Open that can write, before anything else is written.
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
// # Finding a producer's last assertion
//
// [Store.NewLookup] opens a [Lookup], one iterator for a group of calls of
// [Lookup.Last], which finds the newest record of one reference (a subject and a
// producer, exactly) outside [store.Store]. It reads one side only: the forward
// prefix of an edge's source, or the entity's own prefix, never the target's
// reverse prefix. It walks from the newest key, steps over checkpoints, which hold
// no payload, and stops at the first record of the reference, taking, of that
// instant's records, the latest [store.LastOf] and then the highest Seq; at the
// baseline a retention left, where the reference's entry gives its times with no
// payload ([store.LookupBaselineOnly]) or its absence ([store.LookupNone]); or at
// [LookupWalkBound] keys ([store.LookupExhausted]). It sees the database as it was
// when the Lookup was opened, and is closed before the group's write. Its counters
// are in [Recorder]. In a Background store whose pass has not reached the prefix,
// the records below the horizon are still there: the walk answers for the first of
// the reference's as the baseline the pass will write would, so the answer does not
// depend on how far the pass is.
//
// # The writer's memory
//
// The writer keeps, for the prefixes it touched most recently and within
// [Options.StateCacheBytes], what it needs to keep checkpoints true: the instants
// of a prefix's checkpoints and how much was written since the last. It knows the
// other prefixes by a filter of the stored prefixes (package prefixfilter), built
// by a scan of the data keyspace when a store opens over data and after each
// retention, beside the writes and without their lock, and by exact sets of the
// prefixes written since a scan began. A prefix in neither holds nothing, and its
// state is known without a read; any other prefix whose state is not kept is read
// when a write touches it. Before the first scan has installed a filter, every
// such prefix is read. Nothing of this is stored: a store opened again scans
// again. See prefixstate.go.
//
// # Retention
//
// Retain(h) publishes the horizon of each layer it moves (see Layers below), then
// rewrites each prefix of those layers that has history before its layer's horizon
// h: it replays it, writes a baseline at h holding every reference that is still
// alive at h, and range-deletes everything older, the delete and the baseline in one
// commit. The baseline is not derived data: it is the only record of the state
// before h. With Config.SettleRetention the database is then flushed and
// waited on until its compactions have run. That is the [Synchronous]
// [RetentionMode], the default: Retain returns when all of it is done. The
// [Background] mode is described below.
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
// # Background retention
//
// A store opened with [Background] does the first step of a retention under its
// lock, in milliseconds: the horizons and the marker are committed in one synced
// commit and published, a goroutine of the store is woken, and Retain returns. The
// goroutine takes the pass one chunk at a time, holding the lock for the chunk only
// and yielding between chunks, so writers go on. Reads take no lock, as always.
//
// A write to a prefix the pass has not reached (at or after the marker's resume
// key, in a layer the pass rewrites, and not yet rewritten by a write) first runs
// the pass's step on that prefix, in its own batch, before anything else the write
// does to it. The step reads only keys below the layer's horizon, which are frozen
// from the publication on, so the keys the batch leaves are the keys the pass would
// leave, and when the pass later reaches the prefix it finds nothing below the
// horizon to rewrite. A write that fails before its commit leaves the prefix to the
// pass. What the writer remembers of such a prefix is what a read of it finds once
// the batch is applied.
//
// A second Retain while a pass is pending starts a pass again from the first key at
// the new horizons, keeping the layers of the older pass that it does not move. Close
// stops the pass at a chunk boundary and waits for the goroutine; a store opened over
// a marker has the pass ready before Open returns and resumes it. A commit that fails
// in the background stops the pass and is latched: Write, Retain, Instrument.WaitRetained
// and Close return it, reads continue, and reopening the store finishes the pass.
// Instrument.WaitRetained returns when the pass is over.
//
// With Config.SettleRetention a Background pass settles after its last chunk, without
// holding the lock: a flush, then a compaction of the key ranges of each layer it
// moved, one range after another (178 per layer with the shipped catalog, cut on the
// first byte of the digest within each entity type; see [pebblekv.KV.CompactRanges]),
// until they are done or Config.SettleDeadline passes, which is counted, not an
// error. Writers go on meanwhile, and the marker stays, in its settling phase, until
// the settle is over; so Instrument.WaitRetained returns only after it, and a Close
// during it leaves the marker for the next Open, which settles again. The settle
// writes no key. It does not wait for the database to be at rest, as the
// Synchronous settle does. A Background store is opt-in: the default is
// [Synchronous]. Options.Observer is told when each phase of a retention ends.
//
// # Layers
//
// Every layer has a horizon of its own ([Store.LayerHorizon]), stored under its own
// meta key "horizon/<layer>" and judged on its own by reads and writes. Each has a
// retention offset ([Options.Offsets]) or is kept ([Options.Keep]), fixed when the
// store is opened: Retain(h) moves the horizon of a layer that is not kept to h less
// its offset, if that is later than the layer's own, and leaves the others, with
// their data, as they are. With every offset zero the layers move together. Neither
// option is stored: a store opened with other offsets or kept layers keeps every
// horizon it holds, and a horizon never moves backward.
//
// [Store.Horizon] is the horizon of the layer retained most recently, which the four
// horizons alone do not say, since two retentions with no write between them share
// a Seq. The commit of the horizons therefore also writes the meta key
// "horizon/last", holding that horizon in the encoding of a layer's. A store reads it
// back only if it equals the stored horizon of some layer and its Seq is the largest
// of them; otherwise (a database from before the key, or one a binary that does not
// write it has retained since) the Horizon is derived from the layers, as the one
// with the largest Seq and then the latest time, and the next Retain that moves a
// layer writes the key again.
//
// # Quarantine
//
// A store opened with a policy that names a boot key reports two live boots of
// one host as a [store.QuarantineError], decided by folding everything the scope
// sees of the entity, so that the answer is the fold of its whole history:
//   - Alive reads the whole prefix of the entity and folds it, where without a
//     boot key it stops at the first live reference: the records the token sees,
//     and the retention baseline, whose entries hold the references alive at the
//     horizon and whose boot state stands for the boots of the records before it.
//     Its cost grows with the history the prefix keeps, which retention bounds.
//   - Retain rewrites the prefix of an entity with boots as it rewrites any other,
//     and its baseline also holds the boot fold's state at the horizon, which
//     makes it a stamp of format 2 (see stamp.go): the boots seen before the
//     horizon, each with its first and last sighting, in order of first
//     appearance; the first collision, if there was one; and the sightings at or
//     after the horizon of the records it discards, which are the last instants of
//     runs that began before it. The sightings are the fold's own
//     ([lifecycle.Sightings]): per producer, the record with the highest Seq at
//     each instant, so every record that goes counts, not only the newest of its
//     reference. A baseline of a prefix with no boot is of format 1, byte for byte
//     what it was before format 2.
//
// A read seeds a [lifecycle.BootTracker] with the state and has it take the
// carried sightings and those of the records that stay, in order, which is the
// fold of the whole history from the horizon on. The Skew of the policy is applied
// as the state is folded, so it is stored when the database is created (the meta
// key "skew", absent for zero, as every database before the key has) and a store
// opened with another Skew is refused, as one opened with another boot key is.
//
// A summary of an entity's boots that a read could stop at (a key of kind 3) is
// deferred: Alive under a boot key folds the whole of the prefix that retention
// leaves. It is a cost, not a matter of correctness, and is revisited if hosts with
// many reboots make that fold too slow: known issue K5, which holds the tests that
// decide it (a bench of hosts with 1 to 1,000 reboots over 30 days read at several
// instants, against the read budget of their class, and the p99 of Alive on the
// worst host prefix against the query latency target).
//
// The boot list in a baseline is never shortened: a stale boot is judged against
// the boot that first appeared right after it, so every boot must stay, and the
// list grows by one entry per reboot for the life of the host. An entry is the
// boot ID with a length byte and two instants of 9 bytes: about 55 bytes for a
// 36-byte boot ID, the form of /proc/sys/kernel/random/boot_id.
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
// The product's default offsets are not set here: [DefaultOptions] has every offset
// zero and no layer kept.
package pebblestore
