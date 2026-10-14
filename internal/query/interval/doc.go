// Package interval reads a window of a store as half-open intervals at one
// snapshot token, and joins paths by interval intersection.
//
// Everything it returns is derived: a pure function of the facts the store
// holds, computed above [store.Store] from its record reads. Nothing is written
// back, and the package has no write path.
//
// # Intervals, windows and instants
//
// An interval is [Start, End) of event time at nanosecond resolution, as in the
// lifecycle specification. A window is [From, To) with From before To; a window
// with From at or after To is refused as invalid. An instant t is the window
// [t, t+1ns), and its answer equals the store's point reads at t: an edge read
// raw has a piece containing t exactly when store.Store.Neighbors lists it at
// t. Two intervals that only touch, [a, b) and [b, c), do not overlap.
//
// # One token per answer
//
// An answer is read at one snapshot token. [store.Latest], and a token above
// store.Store.LastSeq, are resolved once, to LastSeq before the first read;
// every read of the answer uses that token, and [Result] names it. A batched
// read, [Reader.EdgesBatch] or [Reader.Entities], is one answer: one token for
// every fingerprint, and at most one existence read per distinct entity. The
// window is in event time only: what was known between two tokens is another
// query.
//
// # What is folded
//
// A subject's intervals in [From, To) are its lifecycle existence, folded from
// every visible record with an event time before To, from the start of the
// store's history, and then clipped to the window. So a late record changes
// the answer exactly where its event time puts it. Nothing after To is
// reported: an interval that reaches To ends there with no end source, even
// when a deadline after To is already known, because a later record could
// still move it. Before From only one bit is reported, [Piece].Before: whether
// the interval began before From.
//
// # Producers, gaps and ends
//
// An element's intervals are the union of every producer's reference, so a
// hand-off from one producer to another at one instant is one interval with no
// seam, and a gap of any length, one nanosecond included, is two intervals.
// Nothing is bridged or interpolated, and a change of description never splits
// an interval: intervals are existence. Each piece lists the producers whose
// reference was live at some instant of it.
//
// Every piece that ends inside the window says why: lifecycle.EndProducer (a
// Delete), lifecycle.EndLivenessExpiry (silence past the producer's own TTL)
// or lifecycle.EndCascade. So a gap that begins with an expiry reads as not
// knowing, and one that begins with a Delete as an absence. A piece that
// reaches To has lifecycle.EndUnknown, and only such a piece has it: a finite
// folded interval with no end source is refused as an error rather than shown.
//
// # The cascade
//
// By default an edge is reported only while the edge and both its endpoints
// exist: its intervals are intersected with each endpoint's existence, read in
// the endpoint's own entity layer (not the edge's) at the same token. An end
// cut by an endpoint is lifecycle.EndCascade; when the edge's own end falls at
// the same instant, the edge's own source wins. Before and the clipping are
// computed on the intersected interval. [Options].Raw reads an edge's own
// intervals, with no endpoint read.
//
// Only an edge with an interval of its own in the window has its endpoints
// read: one existence read per distinct endpoint of such an edge, at most, per
// call. An edge with none has no piece whatever its endpoints did, so the
// endpoints of an edge that ended before From, which a node's history is full
// of, are never read, and an error reading them (a retained endpoint layer
// among them) cannot fail the call.
//
// # Quarantine
//
// An entity the store quarantines (store.Store.Alive returns a
// [store.QuarantineError]) is an element marked [Element].Quarantine, with no
// pieces; it is shown, never dropped or guessed. An edge whose endpoint is
// quarantined keeps its own pieces, uncut, and is marked [Element].Uncascaded.
//
// # Descriptions
//
// A reader with [Options].Describe also gives each element its merged
// description at the window's two ends: [Element].AtFrom at From and
// [Element].AtLast at To-1ns, the window's last instant, each when a piece
// contains that instant (for an edge read with the cascade, a cut piece) and
// nil otherwise. A quarantined entity has neither. The description is the
// store's single payload attribute: a record's whole payload, one opaque
// value, never split into keys here.
//
// It is merged by rank and never by recency. Of the producers whose reference
// is live at the instant, a producer listed in [Options].Rank beats an
// unlisted one, a higher rank beats a lower one, and the smaller name breaks
// every remaining tie; the description is that producer's, the payload of its
// latest assertion at or before the instant. So a reader with no Rank ranks
// producers by name, and when the winner differs between the two ends the two
// payloads are two producers' descriptions, not one that changed.
//
// A reader that describes reads nothing more: it folds a second time, under
// Rank, the records of the subject the call has already read. Rank changes no
// existence: pieces, classes and transitions are the same with Describe on or
// off and under any Rank. Its cost is memory and CPU only: an entity read
// keeps each entity's records until the call ends, and an element with a
// piece at either end is folded twice, while the endpoints an edge read reads
// for the cascade keep no records.
//
// # A multigraph of subjects
//
// An element is one subject: an entity, or an edge (from, to, relation). Two
// relations on one pair are two elements, several producers on one edge are
// one element, and an edge that leaves and returns is one element with several
// pieces.
//
// # Views and classes
//
// [Union] is every element with at least one piece in the window,
// [Intersection] every element with one piece covering the whole window, and
// [At] every element with a piece containing an instant. All three keep the
// quarantined entities.
//
// With k pieces in the window, a = "the first starts at From" and b = "the last
// ends at To", the class is: none for k = 0; flapping for k of 2 or more; and
// for k = 1, stable if a and b, appeared if only b, disappeared if only a, and
// transient if neither. A piece that starts at From is never appeared, whether
// or not it began before. Each element also reports its transitions, the piece
// boundaries strictly inside (From, To), so a display can ask for more before
// it calls an element flapping. Classes and transitions are derived under
// [PolicyVersion].
//
// # Paths
//
// A path of two or more hops exists exactly where all its legs exist at once:
// the intersection of the legs' span lists, [Intersect] or [Along], a linear
// merge-join of sorted lists, with [Duration] for its total length. Each leg is
// read with the cascade, so its endpoints exist too. This package offers no
// other way to compose hops, and a union graph is never walked: two pods that
// were on one node at different times were never on it together. Nothing is
// inferred through a relation nobody asserted.
//
// # Edge layers
//
// A read of edges names no layer. Each stored relation is stored in one layer,
// catalog.Relation.Layer, and [EdgeLayers] lists, for an entity type and a
// direction, the layers of every relation that can touch it, so one read may
// cover several layers: the edges into a service are instance_of edges stored
// in L2 and depends_on edges stored in L3. Each layer is read once, at the one
// token, and the elements are merged. A stored edge read there whose relation
// is unregistered or derived, is stored outside its relation's layer, or
// connects types its relation does not allow is an error wrapping
// [store.ErrInvalid], never dropped: a writer has drifted from the catalog.
//
// A type that no relation reaches in a direction (topo.port, read either way)
// has no edge layer there: its edge read reads nothing and is empty. That is
// the right answer, not a read in the wrong layer: the catalog says no such
// edge can exist, and a drifted edge in any layer that is read fails the call.
//
// # Retention
//
// A read that touches a layer whose retention horizon is not zero is refused,
// before it reads that layer, with an error wrapping [ErrRetained] and so
// [errors.ErrUnsupported]. Every edge layer of a call is checked before its
// first read, and an endpoint's entity layer before that endpoint is read, by
// which time edge layers may have been read; either way the call then fails as
// a whole, with no partial answer, as it does for every other error. After a retention the state at the horizon may live
// in a baseline that no store read returns, so the intervals of a subject that
// was alive there cannot be rebuilt from records alone. The refusal lasts until
// the store has an interval read of its own. A retention that lands during a
// read is refused by the store itself, with store.ErrBeforeHorizon.
package interval
