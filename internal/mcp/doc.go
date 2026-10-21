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
// ([NameTableVersion]); it is never taken from an identity attribute, except a
// service's, whose description is its identity, nor from a losing producer or
// from a peer. A description or payload is printed only through a per-type table
// of allowed top-level keys ([PayloadTableVersion]). The table fails closed: a key
// it does not list is withheld, and the answer names it.
//
// A name or a namespace is also bounded when it is printed, in every answer: one
// that is over 253 bytes, holds a control character (a newline and a tab among
// them, and U+2028 and U+2029), holds a format character (Unicode Cf: zero-width,
// bidi controls, tag characters) or holds an endpoint id's text is withheld. It
// is null, with a fixed note that holds none of it; in a description payload its
// key stays, as null, and is named among the withheld keys; and find_entities
// never matches it, and counts it as not printed. The same rule applies to the
// other allowed payload keys that hold text a sender chose: a service instance's
// version. The bound applies to what is printed, never to what is stored.
//
// # Finding ids by name
//
// Every other tool takes ids. [Source.FindEntities] goes from a name to ids: it
// lists the ids of one type from the store's listing read ([Lister]), which is
// outside [store.Store] and takes no token, and decides at the answer's token
// which of them existed in the window and whose name, read as every name is,
// contains the text. A call checks at most 256 ids, in id order, and says how to
// continue from the last one it checked, so a page at one token is exact. It reads
// only a name and a namespace, indexes nothing, and never lists a host or an
// endpoint; a store without the listing read refuses it in a sentence.
//
// # A service's context
//
// [Source.ServiceContext] serves the exploratory extraction of package extract
// for one service id over a window: the service's callers (a depends_on into
// it) and callees (a depends_on from it), one hop, and every other service that
// shared a pod, a node or a host with a pod of that hop. It takes an id and
// never a name, and a window and never an instant. It runs one extraction under
// the package's default policy and reads nothing else but the names, so every
// pair is the union over instances of the intersection of every leg, as the
// package derives it. A mapping that cannot be told is listed as unattributed,
// and a pod with no instance mapped singly to a service as unmapped, never
// guessed. The pairs carry the rules the wire uses (pair.shared_pod,
// pair.shared_node, pair.shared_host), and the policy carries the extraction's
// version. A service that shares a node with the seed pods on most of the
// frontier is suppressed as ubiquitous and listed apart, with its coverage. The
// caps of the extraction mark the answer incomplete, each with a sentence; the
// display limit and the hard cap give exact counts, and the hard cap drops the
// callers and callees last. The service is the only entity the request names:
// no endpoint id is printed.
//
// # Temporal subgraphs
//
// [Source.ScopeSubgraph], [Source.CoResidentsSubgraph],
// [Source.ServiceContextSubgraph] and [Source.BlastRadiusSubgraph] answer four
// questions with a temporal subgraph (package subgraph) instead of a list. They
// are not tools: no MCP client calls them, and no tool's answer changes. Each runs
// in the frame of every tool, with its slot, its deadline and its last check, and
// reads at one token: a token above the store's LastSeq is refused
// ([ErrFutureToken]), never clamped, because a restored store reuses tokens. The
// answer ([SubgraphAnswer]) carries the subgraph's wire JSON twin as a JSON
// value, labelled derived once, and beside it the names of the dictionary's
// entities, in its order, read as every name is, and the counts of what was
// withheld.
//
// An endpoint id the request did not name is withheld by building the subgraph
// again without every element that names it (subgraph.Without), so the ids, the
// keyframes and the deltas hold for what is printed, and each element taken out is
// counted. A dictionary that still names such an endpoint, which only a section
// could keep there, refuses the answer before it is written, and the last check
// scans the whole answer, the document included; no section of the four
// questions names an endpoint. A subgraph that cannot be built again fails the
// answer as an internal error, never with the subgraph from before. An answer
// over [MaxSubgraphAnswerBytes] is refused whole ([ErrAnswerTooLarge]), never cut.
//
// # Refusals
//
// A failure is a [*Error] whose message is a plain sentence for the client.
// The sentences are chosen in a fixed order: cancellation, a closing store, a
// read before the retention horizon, a description recorded before the horizon,
// an invalid argument, and last an internal error.
//
// # Not exposed yet
//
// The co_residents answer leaves out the package result's SeedHosts, the seed's
// time on each host while its node was on one host. It is deliberately not
// exposed yet; the unattributed placements that explain its gaps are.
//
// On a retained store, entity_history fails whole when a description at either
// end of its window was recorded before the horizon, though its stored records
// could still be read from the horizon on.
//
// # Read-only, and bounded
//
// A [Source] reads its store through a wrapper whose Write and Retain fail and
// whose Close does nothing. At most [DefaultMaxConcurrent] calls run at once,
// each has a deadline, and [Source.Drain] withdraws the store and waits for the
// calls in flight. The range tools run on memstore and pebblestore
// ([RangeReadsFor]); on any other store they answer that range answers are not
// available.
package mcp
