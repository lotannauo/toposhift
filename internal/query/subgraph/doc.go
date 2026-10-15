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
// Derived subjects are keyed by [Key].Rule and [Key].Detail only, and carry no
// entity. For the adapters that derive them (co-residency, extraction and
// blast radius), the endpoint entities of a derived key, such as the two members
// of a co-resident pair, should be dictionary references and not text inside
// the opaque detail string, and the rule must be validated against the design's
// derived-kind table. Neither is done here.
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
