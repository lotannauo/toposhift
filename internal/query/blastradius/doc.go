// Package blastradius derives what a failing entity reaches over a window: a
// walk in reverse along the catalog's failure directions, from one seed entity
// of any registered type, in which every path is checked by interval
// intersection and never by walking a union graph.
//
// Everything it returns is derived: a pure function of the facts a store holds
// at one token, the seed, the window and the policy ([PolicyVersion]),
// computed with package interval. Nothing is written back, and the package has
// no write path.
//
// # Direction
//
// For a relation whose propagation is down or up, the failure of an edge's
// "to" side degrades its "from" side. So the walk reads an entity's edges in
// reverse and follows each edge Y→X into it whose relation propagates, from X
// to Y: a failing host reaches the nodes that run on it, a failing service the
// services that depend on it, and never the other way. A service instance is
// part of its pod, so a walk reaches the instances of the pods it reaches. An
// edge of a relation that propagates nothing (instance_of) is read and counted
// but never followed, so the walk stops at the instance: it never crosses from
// a service instance to its service, or on to the service's callers. A derived
// relation is never stored, and is never followed.
//
// # Reach
//
// The seed's own reach is the whole window, and the seed is never its own
// member, even when a cycle of depends_on returns to it: an edge from the seed
// is never followed, so the seed is never a claimant of a placement either. A
// member Y is reached
// at an instant x of the window when some chain seed ← X1 ← … ← Y of at most
// [Policy].MaxDepth hops has every leg live at x. Each leg is read with the
// cascade, so both its endpoints exist at x too. Intervals that only touch do
// not overlap.
//
// A member's reach is the union, over its paths, of each path's own
// intersection of its legs. Intersection distributes over union, so this is
// exactly the instant-by-instant walk: a breadth-first walk at each instant
// over the edges live there. It is not a union-graph walk, which treats an edge
// with a piece anywhere in the window as live throughout it. A pod scheduled
// on a node only after the node left its host is not reached from the host,
// whatever a union graph of the window shows.
//
// The walk goes hop by hop. At each depth it reads the frontier's edges in
// reverse in one call, and carries each frontier entity's new reach one hop:
// the intersection of that new reach with the leg. An entity whose reach grew
// is in the next frontier. [Arrival].Spans is what one edge carried: the
// intersection of the parent's reach, by chains shorter than MaxDepth, with the
// leg. [Member].Reach is the union of a member's arrivals, and [Member].Depth
// the fewest hops by which any instant of it is reached. A frontier entity that
// [Policy].MaxFrontier leaves unexpanded carries none of its new reach onward.
//
// # Placement: one claim at a time
//
// For runs_on, and for located_in per target type, a from entity counts as on
// its target only at the instants at which it claims no other target of that
// relation and type, a claim of a quarantined target included. Over the other
// instants at which the target is reached the entity is listed in
// [Result].Unattributed, with the targets it claims there: it is placed on
// none of them, never on the newer, the older or both. A host's zone does not
// compete with its rack. The frontier's claimants are read forward, in one
// more call per depth, for their other claims. Local unions and subtractions of
// spans work only on the alternatives of one entity, never on two hops.
//
// The rule is not applied to scheduled_on, part_of or depends_on. A pod
// scheduled on two nodes at once, which only a disagreement between two
// producers can cause and which their TTLs bound, is reached from each: the
// check would cost one more forward read per pod per depth.
//
// # Quarantine
//
// An edge with a quarantined endpoint (interval's [interval.Element].Uncascaded)
// is never followed, and is listed once in [Result].Quarantined; the walk
// still succeeds. So a quarantined seed reaches nothing, and its edges are
// listed.
//
// # Membership, ranking and the policy
//
// Membership comes from existence alone; no decay is applied. Members are
// ranked by depth, then by reach duration, longest first, then fingerprint.
// Classes and transitions follow the class table of range reads over a
// member's reach. [Union], [Intersection] and [At] are the three views; an
// instant is the window [t, t+1ns).
//
// Members reached at MaxDepth are not expanded, and those with edges that
// could lead on are named in [Truncation].Depth. Each frontier is ranked by
// the duration of its new reach, then fingerprint, and those beyond
// MaxFrontier are not expanded and are named in [Truncation].Frontier with
// their depth; an entity whose type no edge can lead into is never expanded
// and takes no place in a frontier. Members beyond MaxMembers are counted in
// [Truncation].Members, never listed.
//
// # One token, and errors
//
// The first call resolves the window's token, and every later call passes it;
// [Result].Token names it. Arguments are checked before any store call. After
// that any error fails the whole walk, with no partial answer; a retained
// layer gives an error wrapping interval.ErrRetained.
//
// # Reads
//
// A walk makes one reverse call per depth it expands and one claims call per
// depth with a single-claim leg, and [Result].Reads counts them and their
// store reads: one Window per edge layer of each entity read, and one
// existence read (an Alive and, unless quarantined, an EntityWindow) per
// distinct endpoint of an edge with an interval of its own in the window, per
// call. The read budget is fixed in [BudgetDate] and the constants beside it.
//
// The Window reads are not bounded by the window: they read every edge record
// from the start of the store's history, so on a store that keeps a long
// history they grow with it. That lasts until the store has an interval read
// of its own.
package blastradius
