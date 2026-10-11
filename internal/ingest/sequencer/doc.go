// Package sequencer commits the logs the receiver takes into the store. It is
// the receiver's sink: one goroutine, the loop, owns the translators (a
// k8sobjects and a k8scluster translator per cluster name, made on first use),
// the coalescer, the detector, and the store's writing. Nothing else writes to
// the store.
//
// # The 200 is the durability boundary
//
// A request is answered 200 only after the synced commit that holds its records
// has returned without error. There is no broker and no inbox: a request the
// sequencer cannot commit is answered 503, and the collector's queue keeps it
// and sends it again.
//
// # Groups
//
// The loop claims the requests waiting, in arrival order, up to
// [Config.MaxGroupRecords] OTLP log records, and commits them as one group in
// one synced write. A single request larger than that goes whole into one
// commit; a request is never split. The order inside a group:
//
//  1. The horizons of the layers are read and given to the coalescer, which
//     keeps only their moves forward. A retention can move them at any time.
//  2. Each request in turn: the cluster name of each resource is resolved (see
//     "Cluster names"); its namespace events go through the namespace pass and
//     the detector (see "The detector"); the rest of its log records are
//     translated by event name (entity.state and entity.delete to the cluster
//     receiver's translator, everything else to the objects receiver's), with
//     the producers k8sobjects/<cluster> and k8scluster/<cluster>; and every
//     store record is checked.
//  3. The records that passed are coalesced, in order, numbered contiguously
//     above the store's last sequence number, and written once, synced, with
//     the sequencer's own context, never a request's.
//
// Each request keeps the ingest instant it was claimed at, and only the cluster
// receiver's translator takes it, as the time it received the record, so that a
// rerun from memory translates alike. The detector, the coalescer's flush and
// the pruning take the ingest clock's present, which the sequencer never lets go
// backwards.
//
// # Partial success
//
// A translator error on a log record (no observed time, no usable time, an
// identifier that is not a valid key) rejects that log record, never the
// request. So does a store record that would fail on any retry: one that fails
// [store.Record.Validate], whose producer is over the store's write limit, or
// that lies before its layer's horizon where the coalescer would not re-time it.
// A log record is rejected if any of its store records is, and none of its
// records reaches the coalescer. The request is answered 200 with an OTLP
// partial_success counting the rejected log records; the collector logs it and
// does not send them again. Records the translators skip are not rejections.
//
// # Refusals
//
// A request is refused whole, with a 400 the collector does not retry, when:
//
//   - with a configured cluster name, a resource's k8s.cluster.name differs from
//     it (the message names both, quoted and bounded);
//   - a cluster name cannot be stored (over the store's limit of 21 bytes, or
//     outside [A-Za-z0-9._:/-]); the message advises a shorter name;
//   - with no configured name, a new cluster name would exceed
//     [Config.MaxClusters];
//   - the detector refuses one of its cluster names;
//   - translating it panicked; the message names the panic;
//   - the store refused its own records as invalid, when the group was written
//     one request at a time (see "Failed writes").
//
// # Cluster names
//
// With a configured cluster name every resource belongs to it, and a resource
// whose k8s.cluster.name differs refuses its request. Without one, a resource's
// cluster is its k8s.cluster.name (the last attribute of that key with a string
// value; an empty one is no name), or "default". Clients then choose the names,
// and [Config.MaxClusters] bounds the translators they can make: a request that
// brings a name beyond it is refused, saying so. A name's slot is freed at a
// flush tick once the name has had no request for three times the longer of the
// pull and collection intervals (an estimate); its translators are discarded,
// which costs that name the restart effects below when it returns. The
// detector's entries are bounded by its own pruning.
//
// # The detector
//
// The namespace events of a request (entity.state and entity.delete whose
// entity.type, read as the cluster receiver's translator reads it, is
// k8s.namespace) are translated first, through the cluster receiver's
// translator, so that its set of kube-system UIDs follows every sighting,
// refused requests included. Every sighting is observed by the detector, and
// then each cluster name of the request is checked. A refusal refuses the whole
// request with the detector's message, and the rest of the request is never
// translated, so nothing needs resetting. The detector's notes are logged. Its
// warmup is one collection interval and a sighting with no end vouches for
// three.
//
// # Failed writes
//
// A failed write must not leave the translators and the coalescer ahead of the
// store. The outcome follows the store's Write contract:
//
//   - Refused for the horizon (a retention moved one after it was read): the
//     translators and the coalescer are made anew, the horizons read again, and
//     the group rerun once from memory; the records now before a horizon are
//     rejected into their request's partial success. A second refusal answers
//     503 and resets again.
//   - Refused as invalid otherwise (a case the checks missed): it is
//     deterministic, so the group is not written again whole. After a reset each
//     request is rerun and written on its own; one the store refuses is answered
//     400 naming the error, with a reset before the next.
//   - Any other error: the last sequence number tells whether the batch landed.
//     If it did, every request gets 200. If not, every request gets 503 and the
//     translators and the coalescer are made anew. A closed store answers 503
//     with Retry-After: 30, and any other error 503 with Retry-After: 1.
//
// There is no sentinel for a store that has latched a failure: every write it
// refuses is answered 503, and it stays so until the process restarts. The
// operator restarts serve; automatic reopening is not built.
//
// A refusal of a request (400) is given whatever the write did. A flush's
// records, which no request waits on, are dropped when the store refuses them or
// did not store them, the coalescer is made anew, and the error is logged.
//
// # Restart effects
//
// Making the translators and the coalescer anew is what a restart of the
// process does, and costs what a restart costs; the detector is kept, because
// its state comes only from sightings. A snapshot and restore is not built: it
// would be more code for the same outcome. The effects are:
//
//   - the coalescer's gap: the absorbed refreshes of every run are lost, so a
//     run is stored only through its last extension, and the next refresh
//     starts a new run (one extra record per run);
//   - the first record of each UID is asserted again at its creation, with
//     Through its observation;
//   - the pending scheduled_on edges of both translators are lost, and come
//     back when the pod is next seen once its node is known (the next pull);
//   - the kube-system set is forgotten, apart from the UIDs whose events are in
//     the requests rerun. The delete of a forgotten UID then gives no sighting,
//     so a refusal it would have ended ends only when the UID lapses (three
//     collection intervals after its last state).
//
// A panic while translating a request makes anew only the translators of the
// cluster names it touched, and translates again from memory the requests of
// the group before it that share those names; the coalescer has seen nothing
// of the group yet and is kept. Observing their sightings again changes
// nothing.
//
// # Panics elsewhere
//
// Only a panic while a request is translated is recovered. A panic in the
// coalescer, in the store's Write or anywhere else in the loop is not: it ends
// the process. What was answered 200 before it is durable, because each 200
// followed a synced commit; what was not answered is sent again by the
// collector after the restart. Recovering it would not help: a request that
// makes the loop panic would panic it again on every retry, and recovery would
// only turn that crash loop into a loop of 503s, with the coalescer or the
// store left in a state no test describes.
//
// # Backpressure
//
// A request waits on a bounded queue ([Config.QueueLen]); when it is full the
// request is answered 503 with Retry-After: 1 at once. A request the loop has
// not claimed within [Config.MergeWait] is answered 503 by its own timer, which
// races the loop for an atomic claim: whichever claims it first acts, and the
// other does nothing. A request whose context is done before it is claimed is
// dropped. Once claimed, its outcome is its group's. So no request waits longer
// than MergeWait to be merged, however long a commit takes.
//
// # The flush tick
//
// At each tick ([coalesce.FlushPeriod]) the loop reads the horizons, writes what
// [coalesce.Coalescer.Flush] returns for the ingest clock's present as a group
// of its own, ordered with the requests, and prunes the coalescer, the detector
// and the idle cluster names. [Sequencer.Stop] refuses new requests with 503,
// finishes the requests already queued, writes what
// [coalesce.Coalescer.FlushAll] returns, and makes [Sequencer.Run] return.
//
// The coalescer is configured as ingest measured it: a run is re-asserted at
// most once per half its TTL, and its age is bounded by that interval
// (coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}). Its
// DefaultConfig, with a fixed 30-minute age bound, is never used.
//
// The sequencer never reads or writes the store's cursor meta key, and imports
// no broker.
package sequencer
