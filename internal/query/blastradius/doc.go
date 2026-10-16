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
// [Walker.Around] is the same walk from a pod's side: it finds the substrates a
// pod was on, and walks from each over the pod's time there (see "Around").
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
// that any error fails the whole walk, with no partial answer; a window that
// starts before a layer's retention horizon gives an error wrapping
// store.ErrBeforeHorizon.
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
// Each call reads a layer's edges, and an endpoint layer's existence, in one
// batched store read (store.Store.WindowSince) that is counted as one Window, or
// one EntityWindow, per entity. It returns the window's records and each
// reference's seed just before From, never the history before it, so its cost
// is the window and the store's walk to the seeds. Only pebblestore bounds that
// walk, and only under the conditions of its seed-walk budget (its seeded.go),
// among them ingest's coalescer configuration; memstore has no such bound.
//
// # Around
//
// [Walker.Around] takes a pod and a window and answers, for each substrate the
// pod was on, what that substrate's failure reaches while the pod was on it.
//
// The substrates come from an upward walk: from the pod, hop by hop, along the
// edges read forward whose relation propagates failure, at the one token, each
// leg read with the cascade. The single-claim table is the walk's own: a node
// counts as on a host, and a host as in a rack or in a zone, only at the
// instants it claims no other target of that relation and type, a claim of a
// quarantined target included, and the instants it loses while it is reached
// are listed in [AroundResult].Unattributed. The rule is not applied to
// scheduled_on: a pod scheduled on two nodes at once is on both. The claims are
// in the same forward read, so the upward walk needs no claims call, and it
// makes a call at a depth only if some entity of its frontier has a forward
// edge layer. An edge with a quarantined endpoint is never followed, so a
// quarantined pod has no substrate. [Substrate].On is the pod's time on the
// substrate: the union, over chains of at most MaxDepth hops, of the
// intersection of the legs' effective spans, and [Substrate].Depth the fewest
// hops. [Substrate].Vias are the upward legs into it.
//
// Each substrate is then walked as Impact walks it, at the same token, but
// started over the pod's time on it in place of the whole window, so a member's
// reach lies within that time; entities whose reach misses it are never
// expanded, and MaxFrontier ranks and cuts the restricted reach. The pod is
// neither a member nor expanded: no leg from it is followed, so its own
// containers and instances, reached only through it, are absent. Each
// [Substrate].Impact is labelled with the substrate, its Seed.
//
// Only a node and a host are walked by default. Every other substrate, a rack
// or a zone today and any type the catalog later places above a host, is found,
// listed and given its time and legs, but is walked only if
// [Options].ExpandFabric is set, because a walk from one reaches every host in
// it. Otherwise its Impact is the zero Result and [Substrate].Expanded is
// false. [Options].ExpandFabric is ignored by Impact. MaxFrontier bounds the
// number of substrates, and Around's cost is the sum over the substrates it
// expands.
//
// The reads of an Around are the upward walk's plus each walked substrate's,
// and the budget is fixed in [BudgetAroundDate] and the constants beside it.
// The rules of Around are new in version 1 of the policy: no answer of Impact
// changes.
package blastradius
