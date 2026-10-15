// Package coresidency derives which pods shared a place with a seed pod during
// a window: on one node, or on one host through two nodes whose placement on it
// is asserted. Every path is checked by interval intersection, never by walking
// a union graph.
//
// Everything it returns is derived: a pure function of the facts a store holds
// at one token, the window and the policy ([PolicyVersion]), computed with
// package interval. Nothing is written back, and the package has no write path.
//
// # Shared node
//
// A pod X other than the seed P was co-resident with P on node N over the
// intersection of P's and X's scheduled_on edges to N, both read with the
// cascade, so P, X and N exist throughout. Intervals that only touch do not
// overlap. Two pods that were on one node at different times were never on it
// together, whatever a union graph of the window shows.
//
// # Shared host, only through asserted placement
//
// A node is on host H at an instant when its runs_on edge to H is live there
// and it has no live runs_on to any other host. X was co-resident with P on H
// through nodes N1 and N2, N1 not N2, over the intersection of P's edge to N1,
// N1's time on H, X's edge to N2 and N2's time on H. One node is shared node
// only, never also shared host. A node with no live runs_on has no host, and
// is listed in [Result].Unattributed (NoHost) over the seed's time on it;
// nothing is inferred from a node's name, description or address. A node
// claiming two hosts at once has neither (SeveralHosts): never the newer, the
// older or both. A runs_on to a quarantined host is never lifted through, is
// listed in [Result].Quarantined, and still counts as a claim; when it is the
// node's only claim the node is unattributed (QuarantinedHost).
//
// # The seed's hosts
//
// [Result].SeedHosts reports, for each of the seed's nodes that was read, each
// host the node was on while the seed was there, with the spans of that time:
// the seed's time on the node alone, intersected with the node's time on the
// host alone. These are the first two legs of a shared-host path, with no read
// of their own. An instant is attributed to a host only where the seed has one
// node and that node has one live, unquarantined runs_on; every other instant
// of the seed's time on a node that was read is listed in [Result].Unattributed
// (SeveralNodes, NoHost, SeveralHosts, QuarantinedHost) and is in no entry of
// SeedHosts. The entries are ordered by node, then host, and are nil with
// [Options].NodeOnly. [Policy].MaxHosts does not cut them: it bounds the hosts
// whose nodes are read. A node over [Policy].MaxNodes is not read, and is
// listed in [Result].Truncated.
//
// The local union and subtraction of spans work only on the alternatives of
// one hop (the nodes of one pod, the hosts of one node). Every path is
// interval.Along of its legs.
//
// # Placement
//
// Placement comes only from scheduled_on edges: a pod with none is on no node,
// and a node name or address in its description is never used. While the seed
// is scheduled on two or more nodes at once, which only a disagreement between
// two producers resolving a node can cause, its node is unattributed
// (SeveralNodes) and those instants are left out of every path. The rule is
// applied to the seed only: a peer's own edges to other nodes are not read,
// which would cost one more read per peer, so a peer in that state is shown
// on the node the seed shares.
//
// # Overlaps
//
// Each entry has one or more overlap spans. [Overlap].Pieces names, for each
// leg, the piece of the leg that contains the span, which carries its
// producers. [Overlap].Ends lists every leg that ends at the span's end, with
// that piece's end source, so absence (a Delete) and not knowing (an expiry)
// reach the answer; a leg that goes on while its placement becomes ambiguous
// ends with Unattributed. A span that reaches the window's To has no ends.
// [Overlap].Before says the span starts at From and every leg's piece began
// before it.
//
// # Policy, ranking and the result budget
//
// An entry whose total overlap in the window is below [Policy].MinOverlap
// (1 s by default, the resolution of event times) is moved to
// [Result].Suppressed, never dropped, unless it starts before the window or
// reaches its end, where its true length is not known. Entries are ranked by
// total duration, then shared node before shared host, then peer. The caps of
// [Policy] bound the frontiers read and the entries returned; what they leave
// out is reported in [Result].Truncated. Suppressing peers co-resident with
// nearly everything is not done here: [Ubiquitous] is reserved for it.
//
// Classes and transitions follow the class table of range reads over an
// entry's spans. [Union], [Intersection] and [At] are the three views.
//
// # One token, and errors
//
// The first read resolves the window's token, and every later read uses it;
// [Result].Token names it. Arguments are checked before any store call. After
// that any error fails the whole query, with no partial answer; a retained
// layer gives an error wrapping interval.ErrRetained, and [Options].NodeOnly
// reads no L1 layer.
//
// # Reads
//
// A query makes two to six interval calls, and [Result].Reads counts them and
// their store reads: one Window per edge layer read, and one existence read
// (an Alive and, unless quarantined, an EntityWindow) per distinct endpoint of
// an edge with an interval of its own in the window, per call. The read budget
// of the cascade is fixed in [BudgetDate] and the constants beside it.
//
// The Window reads are not bounded by the window: they read every edge record
// from the start of the store's history, so on a store that keeps a long
// history they grow with it. That lasts until the store has an interval read
// of its own.
package coresidency
