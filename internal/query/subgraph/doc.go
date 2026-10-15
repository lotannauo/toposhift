// Package subgraph builds the temporal subgraph of a window: the elements
// present in it, as keyframes, time-ordered invertible deltas and interval
// lists, with a transfer bound and a seek bound that hold without looking ahead
// in the window.
//
// Everything it returns is derived: a pure function of the elements it is given,
// which package interval read at one snapshot token, and of [PolicyVersion].
// Nothing is written back, and the package has no write path. [Build] is the
// builder, which takes a generic list of elements; [ReadScope] is the adapter
// that reads a scope through package interval and builds its subgraph. The
// wire's own enum tables, with the functions that map other packages' enums onto
// them, are in this package too: a wire value is never a cast of a Go constant.
//
// # What a subgraph holds
//
// An element is one subject: an entity, an edge (from, to, relation), or a
// derived subject. A subgraph has a dictionary of the entities its keys name, a
// list of its elements with their pieces, keyframes, and deltas. Existence is
// all it carries: descriptions inside the window, names and decay are not in
// version 1. The keyframe rule and the bounds are described at [Build].
//
// # Labels
//
// The basis of a subgraph is derived, stated once. An element's subject is
// asserted if and only if a producer asserts that subject itself: an entity, or
// an edge whose relation is stored. Its rule is "existence" then, and for a
// derived subject the rule that derives it. A piece's end reason is why it
// ended, as the lifecycle names it, or one of the reasons only a derived span
// has.
//
// A derived subject is keyed by [Key].Rule, one of the rules of the
// derived-kind table ([DerivedKinds]), by its two ends [Key].A and [Key].B,
// which are dictionary entries, and by [Key].Detail, the rest of its identity
// within the rule and the ends, which an adapter defines and which never holds an
// end. The rules are the MCP vocabulary's: co_residency.shared_node,
// co_residency.shared_host, pair.shared_pod, pair.shared_node,
// pair.shared_host and blast_radius.reach for derived keys, and same_as for an
// edge of that derived relation. A rule outside the table is refused.
//
// # Legs and ends
//
// A derived subject says what it is made of. [Source].Paths are its legs, one
// path for each way it holds, each leg the key of an edge that is an element:
// the subject holds while all the legs of some path do, and the builder checks
// that every piece lies inside the time at which the legs of some path exist,
// by interval intersection. A derived piece ends where an intersection or an
// ambiguity ends, which is [EndReasonReachEnded] and names no source of its own;
// for the rules that have legs of their own to name, the piece lists the legs
// that end where it does, each with its own end: a lifecycle reason, or
// [EndReasonUnattributed] when the leg goes on but its placement or mapping
// became ambiguous. A reach lists none. Nothing is inferred.
//
// # Sections
//
// What a query reports that is not an element travels in [Sections], whole: the
// cuts its caps made, the placements, mappings and claims it refused to guess,
// the pods with no mapped service, the entries it suppressed with their reason
// and coverage, what each pod-level search of an extraction cut, quarantined and
// suppressed, the anchor state and the ubiquity judgment. The builder never
// truncates them. An entity a section names is a dictionary entry, marked as a
// boundary when nothing read it.
//
// # Adapters
//
// [FromCoResidency], [FromExtraction] and [FromBlastRadius] carry the results
// of packages coresidency, extract and blastradius into a subgraph. Each result
// has no element for the existence of the entities it names, so each makes one
// call of package interval's Entities, at the result's own token, so that a write
// made since is not seen; its budget is fixed beside [BudgetDate]. A co-resident,
// a pair and a reach are derived elements keyed by their two ends; the legs, the
// seed's placement, and the edges read and not used because an endpoint is
// quarantined are elements as the interval package read them. An extraction's
// pod-level entries are not elements: a pair's paths run through their legs, and
// their cuts and quarantined edges travel as sections. Package blastradius's
// Around result has a different shape and is not carried here.
//
// # Quarantine
//
// A quarantined entity, and an edge with a quarantined endpoint, are in a static
// section with their collision and their own uncut pieces. They are in no
// keyframe and no delta, as graph diff lists them in no bucket and gives them no
// events.
//
// # Scope
//
// [ReadScope] follows graph diff's scope rule: the scope's entities, and every
// edge with an endpoint in it, one hop out. The peer at the far end of an edge
// is a dictionary entry marked as a boundary, with no existence element. A
// scope larger than [ScopePolicy].MaxScope is refused.
package subgraph
