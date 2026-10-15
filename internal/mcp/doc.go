// Package mcp is the core of toposhift's read-only tools for clients that ask
// questions of the topology graph: the tools as plain Go functions over a
// [store.Store], their input and output types, and the rules every answer
// follows. It imports no MCP protocol code; a transport adapter registers the
// tools of [Tools] over a [Source] and carries the types as they are.
//
// # What an answer is
//
// Every answer is read at one snapshot token. A [Source] resolves the token
// once per call, from the optional asof argument or the store's LastSeq, and
// passes it to every read of the call, the reads of the names included. The
// answer names the token as a decimal string, so no client rounds it, and says
// how to ask again for the same world.
//
// Every element of an answer says how its value was obtained, with these keys:
//
//   - basis is "fact" for a stored record returned as stored, and "derived" for
//     a value computed from records at a token under a policy version. Every
//     range-read element is derived.
//   - subject is "asserted" for an entity or a stored relation that a producer
//     asserted, and "derived" for anything no producer asserted: a reach, a
//     co-resident, a pair.
//   - rule names the derivation of a derived element.
//
// Every span carries end_reason next to its sentence: a value of the end
// vocabulary for a span that ends inside the window, and null for a span that
// reaches the window's end, because nothing after the window is reported.
//
// # Names, descriptions and what is withheld
//
// Every entity id is an object that carries the entity's name when its
// description has one. A name is derived from the description of the producer
// that wins under the store's ranks, and from a fixed table of keys
// ([NameTableVersion]); it is never taken from an identity attribute, from a
// losing producer or from a peer. A description or payload is printed only
// through a per-type table of allowed top-level keys ([PayloadTableVersion]).
// The table fails closed: a key it does not list is withheld, and the answer
// names it.
//
// # Refusals
//
// A failure is a [*Error] whose message is a plain sentence for the client.
// The sentences are chosen in a fixed order: cancellation, a closing store, a
// read before the retention horizon, a retained layer, an invalid argument, and
// last an internal error.
//
// # Not exposed yet
//
// The co_residents answer leaves out the package result's SeedHosts, the seed's
// time on each host while its node was on one host. It is deliberately not
// exposed yet; the unattributed placements that explain its gaps are.
//
// On a store whose layers have been retained, entity_history fails whole, like
// the other range tools, even though its stored records could still be read
// from the horizon on. A server whose range reads are off never meets this.
//
// # Read-only, and bounded
//
// A [Source] reads its store through a wrapper whose Write and Retain fail and
// whose Close does nothing. At most [DefaultMaxConcurrent] calls run at once,
// each has a deadline, and [Source.Drain] withdraws the store and waits for the
// calls in flight.
package mcp
