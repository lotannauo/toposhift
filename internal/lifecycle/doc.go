// Package lifecycle is the specification of how producer assertions about one
// subject become that subject's existence over time, its merged description,
// and, for hosts, its boots.
//
// It is a pure function. [Fold] takes the assertions made about one subject
// and a [Policy] and returns a [Timeline]; nothing reads a clock, a disk or
// the network. It is the oracle that every storage implementation is tested
// against, so it favors being obviously correct over being fast. A subject is
// anything with a lifetime: an entity, or later an edge. The fold sees only
// one subject's assertions, so it never needs to know which.
//
// # Producers and assertions
//
// A [Producer] is a source of assertions: the collector watching one cluster,
// a node-level collector, an LLDP shim, a cloud poller, a webhook client. It is
// not the thing it describes: a host is an entity, the collector that reports
// it is the producer. Several producers can assert one subject, which is why
// the concept exists. It decides who still sees the subject, so that one
// producer's delete or silence cannot remove a subject another still sees, and
// who wins when descriptions disagree.
//
// A producer is a logical, stable name chosen by configuration, for example
// "k8sobjects/cluster-a", never a process instance ID. A reference that only
// an explicit Delete can end never lapses, so a collector that restarted
// under a fresh random name would leave its old references alive for ever.
// The empty producer is rejected rather than treated as anonymous, because
// merging all anonymous producers into one is a silent merge.
//
// An [Assertion] is one statement by one producer:
//
//   - [Observe] says the subject exists and gives the producer's complete
//     current description of it. It carries a TTL, the time within which the
//     producer promises to say so again. A TTL of zero makes no such promise
//     (watch mode): only a Delete ends the reference.
//   - [Delete] says the producer no longer sees the subject.
//
// An Observe may stand for a whole run of refreshes: its Through is the last
// of them, and its deadline is Through plus TTL.
//
// EventTime is when the statement was true. Seq is the ingest sequence number,
// unique within a fold; it orders assertions that share an event time, and it
// is the snapshot token: [Visible] keeps the assertions a query pinned to a
// token may see.
//
// # Existence and expiry
//
// Assertions are processed in (EventTime, Seq) order, so arrival order never
// matters: a late assertion lands where its event time puts it. Of several
// assertions by one producer at the same event time, the highest Seq wins and
// the others have no effect at all, not even on boots: it is how a
// store extends a run, by re-asserting it with a later Seq.
//
// Each producer holds at most one reference to the subject. At an instant x
// the reference is live if and only if that producer's latest assertion at or
// before x is an Observe and either its TTL is zero or x is before its
// deadline, EventTime plus TTL. A later Observe replaces the deadline; it does
// not extend the old one. A Delete releases only its own producer's
// reference, and a Delete when that producer holds none changes nothing. The
// subject exists at x if any producer's reference is live.
//
// Expiry is therefore not an event. It is what the definition says about an
// instant after a deadline with no later assertion: silence treated as
// absence once the grace period the producer itself declared has passed. It is
// derived when read, never written down, because a late observation inside the
// gap must be able to remove it, and a stored record that a late event can
// invalidate is not a fact. A store may cache the result and must treat it as
// rebuildable.
//
// [Timeline.Existence] returns the maximal runs of existence as half-open
// intervals [Start, End); runs that touch are one interval. An open-ended
// interval has a zero End. An interval that ends at a deadline can extend past
// the last observation: that end is an expectation derived from the TTL, not
// something anyone asserted.
//
// Each finite interval records how it ended ([EndSource]): [EndProducer] if a
// producer's Delete ended the last live reference, [EndLivenessExpiry] if its
// deadline passed. When both happen at the same instant the Delete wins, since
// an explicit statement outranks silence. The remaining sources are vocabulary
// for the store layer to author. The zero value means unknown and is never
// relabeled.
//
// # Description
//
// An Observe is the producer's complete description, as in OpenTelemetry
// entity state events, so an attribute the next Observe omits is gone. At
// instant x each producer with a live reference contributes its latest
// description, and [Timeline.DescribeAt] merges them attribute by attribute by
// a fixed authority order: the producer with the higher [Policy] rank wins,
// producers the policy does not list rank below all that it does, and equal
// ranks go to the lexicographically smaller producer name.
//
// Recency never decides. Last-writer-wins would let two disagreeing
// producers take turns at heartbeat cadence, so the answer would change every
// refresh while nothing changed. Authority applies only among producers whose
// reference is live: a high-ranked producer that goes quiet stops overriding.
// Equal-rank producers that disagree about an attribute are a configuration
// smell; the name tie-break only makes the answer stable, not right. A shared
// subject should carry only observer-independent attributes, and per-observer
// facts belong in separate relations.
//
// # Boots
//
// If the policy names a boot key ([BootID] for hosts), each distinct
// value seen in an Observe is one boot, with the first and last time it
// was seen. A reboot is a new boot of the same subject, and the same
// boot ID returning after a gap in existence is the same boot: a
// monitoring outage is not a reboot.
//
// Two boots live at once for one subject is a clone collision, reported as
// [ErrCloneCollision] with no timeline, never a merge. Boot IDs are ordered by
// first appearance, and by the ID itself when two first appear at one instant,
// so that sequence numbers, which a coalesced run does not preserve, never
// decide. Observing boot X at time t is a collision if the boot that
// first appeared after X did so more than [Policy.Skew] before t. With zero
// skew, any return to an older boot after a newer one appeared collides,
// except at the identical instant.
//
// This is a heuristic with two known limits, pinned by tests. It misses
// clones that report the same boot ID: a restore from a snapshot is not a
// boot, so clones restored from one snapshot, and containers reading the host
// kernel, share it. And it flags one machine reverted to an older snapshot
// after a reboot, because an old boot ID reappears. A second signal, such as
// conflicting concurrent host names from equal-rank producers, is future work.
//
// # Coalescing
//
// A heartbeating producer repeats the same Observe every interval, and storing
// each repeat would dwarf everything else. Dropping repeats cannot fix that:
// each refresh is what extends the deadline, so a run of beats can only shrink
// by about its TTL divided by its interval. What can stand for all of them is
// one assertion that says so: an Observe whose Through is its last refresh.
// [Coalesce] folds each run of unchanged refreshes into one such assertion.
// Existence, description, boots and the presence of a collision are
// identical for the original and the coalesced assertions.
//
// A store extends a run by re-asserting it at the same EventTime with a later
// Seq and a later Through. What is lost is only that refreshes inside a run are
// no longer individually visible to a query pinned to an earlier snapshot
// token: the as-known-at view of liveness has the granularity of a run's
// versions.
//
// # Clocks
//
// A deadline is the observation's own EventTime plus its TTL, never a receipt
// time, so replaying an observation cannot revive a subject. That makes the
// choice of event time the ingest layer's responsibility: a producer whose
// clock runs behind by more than its TTL would otherwise look permanently
// expired, so a pure refresh should carry the time it was received.
package lifecycle
