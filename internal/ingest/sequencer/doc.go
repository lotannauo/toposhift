// Package sequencer commits the logs the receiver takes into the store. It is
// the receiver's sink: one goroutine, the loop, owns the translators (a
// k8sobjects, a k8scluster and a nodeagent translator per cluster name, made on
// first use), the coalescer, the detector, and the store's writing. Nothing else
// writes to the store.
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
//     Then the group's lookup is opened (see "Resume").
//  2. Each request in turn: the cluster name of each resource is resolved (see
//     "Cluster names"); its namespace events go through the namespace pass and
//     the detector (see "The detector"); the rest of its log records are
//     translated by what they are (see "The node agents": a legacy entity event
//     is counted and translated by nobody; of the entity events, those of type
//     host go to the node agents' translator and the others to the cluster
//     receiver's; everything else goes to the objects receiver's), with the
//     producers nodeagent/<cluster>, k8scluster/<cluster> and
//     k8sobjects/<cluster>; the records the translators backdated are resumed;
//     and every store record is checked.
//  3. The lookup is closed. The records that passed are coalesced, in order,
//     numbered contiguously above the store's last sequence number, and written
//     once, synced, with the sequencer's own context, never a request's.
//
// Each request keeps the ingest instant it was claimed at, and the cluster
// receiver's and the node agents' translators take it, as the time they received
// the record, so that a rerun from memory translates alike. The detector, the coalescer's flush and
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
// brings a name beyond it is refused, saying so. Such a name's slot is freed at a
// flush tick once the name has had no request for three times the longest of the
// pull, collection and node intervals (an estimate); its translators are discarded,
// which costs that name the restart effects below when it returns: reads of the
// store for its first sightings (see "Resume"), not rewritten history. The slot
// of the configured cluster name is never freed, however long it is idle: there is
// only one, and its translators are kept. The detector's entries are bounded by
// its own pruning.
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
// # The node agents
//
// A node-level collector (deploy/otel-collector/nodeagent.yaml) reports a host
// entity event every node interval: eventName entity.state, entity.type host, the
// host's identity and description in the resource. The agents send only to the
// cluster collector, whose one logs pipeline, exporter and in-order queue carry
// their records with everything else, so one cluster has one stream. The
// sequencer routes by the record, in this order, after the namespace pass and the
// detector:
//
//  1. A legacy entity event is counted and translated by nobody. It is the form
//     the receivers write when the entity-events feature gate is off: no event
//     name, and attributes named otel.entity.*. A record with an otel.entity.*
//     attribute is legacy; a record with no entity event name and the scope
//     attribute otel.entity.event_as_log is legacy too. The current form carries
//     that scope attribute as well, so on a record with an entity event name it
//     decides nothing. Both receivers' records are checked, and the count
//     (sequencer.legacy_entity_events in GET /status) is warned about at most
//     once a minute for each cluster: a collector started without the gate.
//  2. An entity.state or entity.delete whose entity.type, read as the cluster
//     translator reads it, is host goes to the node agents' translator, with the
//     producer nodeagent/<cluster> and the ingest instant of the request as the
//     time received. A host delete is translated to nothing and counted.
//  3. Any other entity event goes to the cluster receiver's translator, and
//     everything else to the objects receiver's.
//
// [Config.NodeInterval] (serve: --node-interval, 5 minutes) is the interval the
// agents report at, and three times it is the TTL of every record of the node
// agents' producer, whatever interval an event carries. The counters of
// [Sequencer.NodeAgentStatus], which serve reports under "nodeagent" in GET
// /status, are named for what was done with a record: refused_* for a record or a
// part of one that was not written (no host id; no node uid, which writes the
// host and no edge; a part of a zone id that contains a slash), substituted_* for
// a record that was written with something put in place of what it gave (the
// receipt time for a time that was absent or too far ahead, the configured
// interval for another, no boot for a boot id that was absent or unusable), and
// skipped_* for a record set aside. They count the groups answered without error.
// The sequencer warns, at most once a minute for each kind and cluster, of host
// events with no usable host.id and with no node uid, with the likely cause.
//
// The status also holds, for each cluster that has had a host entity event, the
// ingest instant of the latest group that held one. At each flush tick a cluster
// whose latest is more than three node intervals old is marked silent and warned
// about, once for each time it goes silent: its hosts and runs_on edges lapse at three
// intervals, so the agents are down or cannot reach the collector, the collector
// no longer sends entity events, or --node-interval is wrong. The entry stays in
// the status for a day after the cluster's latest event, past the freeing of its
// slot, and then is dropped.
//
// The records of the node agents are ordinary refreshes: they are coalesced as the
// others are, and the translator is stateless in what it writes, so a reset
// changes nothing it writes. They count as another machine's clock for retention
// (see "Retention"), unless an agent runs on this host.
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
//   - A resume step that could not read the store because it or its lookup is
//     closed, or because the sequencer's context is done, fails the group in the
//     same way before anything is written: every request gets 503, the
//     translators and the coalescer are made anew, and nothing is counted.
//
// There is no sentinel for a store that has latched a failure: every write it
// refuses is answered 503, and it stays so until the process restarts. The
// operator restarts serve; automatic reopening is not built.
//
// A refusal of a request (400) is given whatever the write did. A flush's
// records, which no request waits on, are dropped when the store refuses them or
// did not store them, the coalescer is made anew, and the error is logged.
//
// A dropped flush loses the run extensions it held, so a stored run can lapse by
// its TTL before the next record of its producer re-asserts it. A reference can
// then be absent for at most one extension interval plus one pull interval (7.5
// and 5 minutes with the defaults; for the node agents' producer, their interval
// stands in for the pull's). It reads as a liveness expiry, never as a Delete:
// the producer was not heard from, and nothing says it ended. The next assertion
// restores the run.
//
// # Restart effects
//
// Making the translators and the coalescer anew is what a restart of the
// process does, and costs what a restart costs; the detector is kept, because
// its state comes only from sightings. A snapshot and restore of the translators
// is not built: the store already holds what they would restore, and the resume
// step reads it (see "Resume"). The effects are:
//
//   - the coalescer's gap: the absorbed refreshes of every run are lost, so a
//     run is stored only through its last extension, and the next refresh
//     starts a new run (one extra record per run);
//   - the first record of each UID is a first sighting again, which the
//     translators time at its creation with Through its observation, and the
//     resume step times from the store: one read per first sighting;
//   - the pending scheduled_on edges of both translators are lost, and come
//     back when the pod is next seen once its node is known (the next pull); an
//     edge that was never stored keeps its PodScheduled time;
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
// # Resume
//
// A translator that lost its state times the first record of each subject it
// sees again at the object's creation (an edge at its PodScheduled time), through
// the time it was observed. Written as is, each such record would be late on
// every prefix it touches, and would delete checkpoints that were still true, so
// that historical reads on busy prefixes would cost far more until retention
// removed them; answers would stay correct. The translators mark those records
// (see k8sobjects' and k8scluster's Result.Backdated: first sightings, and every
// edge resolved from a pending state), and the sequencer resumes them with
// [resume.Apply], per request, after both translation passes and before the
// checks and the coalescer, one producer at a time. A group opens one lookup,
// after the horizons are read and before the first request is translated, and
// closes it before the coalescer: the store's lookup reads a snapshot of the
// store as the previous commit left it. A rerun of the group (the horizon retry,
// the one-by-one split, the requests translated again after a panic) reads again
// from its own lookup; the reads are the same, since nothing was written.
//
// For each marked record, the producer's last stored assertion of the subject,
// matched by exact identity, decides (start is the record's event time as the
// translator gave it, obs its Through, the time it was observed, and last the
// stored record's Through if set, else its event time):
//
//	nothing stored                          unchanged (a genuine first sighting)          resume_new
//	a Delete at or after start              dropped                                       resume_dropped
//	a Delete before start                   unchanged (a new life)                        resume_new
//	an Observe with last at or before start unchanged                                     resume_new
//	an Observe, min(last, obs) below the    obs, no Through                               resume_unknown{below_horizon}
//	  layer's horizon
//	an Observe, description unchanged       raised to min(last, obs), its own payload,    resume_raised
//	                                        its Through kept while after the new time
//	an Observe, description changed         obs, no Through                               resume_changed (and
//	                                                                                      resume_changed_gap if obs
//	                                                                                      is past last plus the TTL)
//	the walk reached its bound              obs, no Through                               resume_unknown{exhausted}
//	only a baseline entry is left           obs, no Through                               resume_unknown{baseline_only}
//	the store could not be read             obs, no Through                               resume_unknown{error}
//	the store or lookup closed, or the      the group fails, 503, nothing counted         (none)
//	  sequencer's context done
//
// A raised record takes the stored record's basis, or the observed basis when the
// time came from a Through or was held at obs. The description is compared on
// what describes the subject (each translator's Comparable), never on what a
// translator derives from its own state: a scheduled_on edge is compared on its
// node_name alone, not on the resolution it carries. So when a node is first seen
// again after a reset without a usable creationTimestamp, its edges are still
// raised, and each writes its own new payload, whose node_interval_start differs
// from the stored one: a derived field of history changes. That is harmless; the
// answers do not read it.
//
// Every later record of the same producer and subject in the request that is
// timed before a resumed record is raised to its time and basis, as the
// translator's own clamp would have done (resume_mirror_raised), and the
// translator raises its own state to it, never lowering it, so that later
// requests are not timed before it either. The raised event time is derived from
// stored facts and is documented so; the description and the Through are the
// producer's. No bridge record is written.
//
// [Sequencer.ResumeStats] has the counts of the groups answered without error;
// serve reports them as the sequencer.resume_* counters of GET /status. The
// sequencer logs, at most once a minute each, a walk that reached its bound and a
// changed description after a silence longer than the TTL (warnings), and a
// store that could not be read (an error), each naming an example subject. With
// no [Config.Lookup] the step is off, and the records are written as the
// translators timed them; serve logs that as an error and its status says
// "disabled".
//
// Bridging: an unchanged object is bridged by its Through on the reset path,
// from its stored last assertion to its observation, even across a silence longer
// than its TTL; on the normal path, after an outage of the collector longer than
// the TTL, the reference lapses until the next record. That difference is benign,
// and rests on UID continuity: a Kubernetes object with a given UID exists
// continuously from its creation to its deletion.
//
// The node agents' translator marks nothing: it backdates no record, because a
// report says only that the host was seen at the instant of the report, and its
// Resumed and Comparable are the no-ops that let it stand where the step asks for
// a translator. The step skips any group with no marks, whatever the kind, so
// the node agents' records never reach it; a translator that later marked its
// records would reach [resume.Apply] with its own Comparable and no change here.
// After a reset its records are not late, and invalidate no checkpoint: each is
// later than any before it.
//
// What remains:
//
//   - a renamed producer (a new cluster name) finds nothing to resume from, and
//     its first records keep their source timing once;
//   - an edge never stored and still pending across a reset keeps its
//     PodScheduled time, one late record for that pod;
//   - an unknown answer (the walk bound, a baseline entry, a read error) takes
//     the observed time, counted, and never backdates a possibly changed
//     description;
//   - a description changed during a silence longer than the TTL leaves the
//     producer's reference absent from its stored lapse until the observed time;
//     only after data loss, or an outage of the collector itself, where it is
//     what the normal path does without a reset. It is counted.
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
// The coalescer is configured as ingest measured it, by [CoalesceConfig]: a run
// is re-asserted at most once per half its TTL, and its age is bounded by that
// interval. Its DefaultConfig, with a fixed 30-minute age bound, is never used,
// and pebblestore's seed-walk budget holds only under this configuration.
//
// The sequencer never reads or writes the store's cursor meta key, and imports
// no broker.
//
// # Retention
//
// With [Config.Retain] set, the loop retains the store once a day, between
// groups and flushes, so that nothing else calls Retain and no Write runs beside
// it. Every [RetainConfig.Every] (10 minutes in serve) a check reads the stored
// horizon of L2, the reference layer, plus its offset (h_stored; absent when L2
// was never retained) and the target, the ingest clock less the window W (30
// days in serve) truncated to the hour, in UTC. A retention is due once the
// target is at least a day past h_stored, so the store holds up to W plus a day:
// L2 up to 31 days and L1 up to 91. The rule keeps no schedule of its own: a
// restart finds the stored horizon where the last retention left it.
//
// The ingest clock is the clock under suspicion: a clock a year ahead would
// discard everything. The check therefore compares it with the clocks of other
// machines. After each write that committed (or that returned an error but whose
// batch the last sequence number shows stored), and after each refusal of a
// record as before a horizon, the newest event time of that batch among the
// records stamped by another machine's clock (basis observed or producer_event)
// replaces what was kept, with the monotonic uptime of the batch. Records on the
// receipt clock (this server's own, which always agrees with itself), on an
// object's own field or of unknown basis never count, and a batch with none
// leaves what was kept. At each check that time, moved forward by the uptime
// since its batch, is compared with the ingest clock: within a day they agree,
// further they disagree, and with no such batch since start the agreement is
// unknown. The latest batch counts, not the largest time ever seen, so a
// collector whose clock was ahead stops retention only while its batches are the
// latest; with mixed producers, a wrong one and a right one in alternate batches
// flip the state between disagree and agree, which is harmless, since the ingest
// clock is the one under suspicion and only agree retains in one large step. An
// idle server on a right clock stays in agreement, and one whose clock then jumps
// becomes disagree. A replay of old records reads disagree while it lasts, and
// the next fresh batch agrees again. Go's monotonic clock may not advance while
// the machine sleeps, so a host suspended for more than a day reads disagree
// until its next batch; retention is only delayed, and the supported deployment
// is a server. The guard protects only against producers on other hosts: a
// collector on the same host shares its clock, and agrees with it whatever it
// reads. The node agents' records are stamped by the clocks of the nodes the
// agents run on, which are other machines' unless an agent shares this host.
//
// The agreement data is read from the records the sequencer writes: the
// coalesced batch of each group and the output of each flush, not the records
// the translators gave. A coalesced record that extends, closes or continues a
// run carries an event time the coalescer computed (the run's start, a stored
// deadline, or a horizon), not the producer's latest reading. Under the bound on
// a run's age those times are minutes old, and the worst case is a spurious
// disagree, which retains nothing: the safe direction.
//
// The check, in order:
//
//   - retention stopped by an error: no call, and an error log at every check;
//   - disagree: no call, and a log at every check, at warning level for the first
//     day of uptime in disagreement, then at error level. A capped advance is not
//     made either: from W behind, gaining a day a day, it would pass true time
//     after about W days and then refuse the producers on true time;
//   - h_stored absent (a store never retained, empty or not): one call to the
//     target, logged at warning level, once the agreement is agree; until then
//     no call, and a warning at every check;
//   - not due: nothing;
//   - due, and the target at most two days past h_stored: one call to the target;
//   - further, and agree: one catch-up call to the target;
//   - further, and unknown: a capped call to h_stored plus two days, at most once
//     per day of uptime, logged at warning level. Uptime is monotonic, so a clock
//     that keeps jumping forward does not make a day pass at once. The limit is
//     kept in memory, so a crash loop resets it and may advance two days per
//     restart; the cap limits the damage of a wrong clock, it does not prevent
//     it.
//
// A call is made only when no pass is pending: a due check while one is pending
// is skipped, counted and logged at warning level, and at error level too once
// the pass has been pending for more than two hours. Pending is told by a
// goroutine that waits on [RetainConfig.Wait], started before the first check
// (the store may have resumed a pass at open, which no observer announces) and
// after each call; the pass is marked pending before the goroutine starts. Run
// cancels its context and waits for it before it returns, on a stop and on a
// cancel alike, so that no call of the store is in flight when the caller closes
// the store; its report goes to a buffered channel that nobody needs to read.
//
// Horizons never move backward: a clock behind the stored horizon is not due,
// and neither is a target equal to it. A smaller W is a catch-up, under the same
// guard; a larger W pauses retention until time catches up with it, and brings
// nothing back.
//
// An error from Retain, or from the wait other than the store's closing or the
// context's end, stops retention until a restart, with an error log at every
// check. A store that latched a failed commit refuses every write too, and
// ingest answers 503 until the restart; a validation error from Retain latches
// nothing, and ingest goes on while retention stays stopped. Automatic reopening
// is not built. A write refused for a horizon a retention moved is rerun once
// (see "Failed writes"): the horizons are read before each group and flush.
//
// Accepted limits:
//
//   - With disagreeing clocks, or during a replay of old records, nothing is
//     retained, and the store grows past W.
//   - Producers that send only receipt-basis records never give agreement: a
//     store never retained is then never retained, and a catch-up advances only
//     in capped steps. With the clock far ahead and only such producers, the
//     capped advance carries L2's horizon past true time after about W days;
//     later records on true time are refused, and the state becomes disagree
//     once they are seen.
//   - A deployment whose every producer runs on the server's own host (a single
//     machine, such as kind, k3s or a development box) has no second clock: its
//     records agree with the server's clock whatever that clock reads, and the
//     guard is void. A host clock that jumps a year ahead then agrees with
//     itself, and the first catch-up discards the whole real history in one
//     call.
//
// The cases:
//
//	no RetainConfig (serve --retain off)    no retention asked for; a pass already started completes
//	Retain or wait error, store latched     retention stopped, error at every check; ingest 503 until restart
//	Retain error, validation                retention stopped, error at every check; ingest goes on
//	refused write after a retention         the group is rerun once against the new horizons
//	clock behind the stored horizon         not due
//	disagree (clock far ahead, or a replay) nothing retained; warning for a day, then error, at every check
//	unknown, clock far ahead                capped: two days per day of uptime; warning
//	never retained, not agree               nothing retained; warning at every check
//	pass longer than a day                  skips logged; error after two hours pending
//	store closed or context ended           shutdown; no error
package sequencer
