// Package graphdiff diffs a scope of the graph between two instants: what was
// created, deleted and changed, what appeared and disappeared in between, and
// how every disappearance came about.
//
// Everything it returns is derived: a pure function of the facts a store holds
// at one token, the scope, the two instants, the description ranks and the
// policy ([PolicyVersion]), computed with package interval. Nothing is written
// back, and the package has no write path.
//
// # The scope and the window
//
// A store cannot list its subjects, so a diff is of a scope the caller names:
// a set of entities, each read in its catalog layer, and every edge with an
// endpoint among them, read forward and reverse, one hop out. An edge between
// two scope entities is one element. The peers at the far end of an edge are
// read only for the cascade and are not diffed themselves.
//
// The instants t1 < t2 are read as the window W = [t1, t2+1ns), so the state
// at t2 includes the records at t2. An element is present at t1 when its first
// piece in W starts at t1, and present at t2 when its last piece ends at W's
// end. Presence is existence with the cascade: an edge is present only while
// it and both its endpoints exist.
//
// # Buckets
//
// The net buckets are exclusive: created (absent at t1, present at t2), deleted
// (present at t1, absent at t2) and changed (an entity present at both whose
// description differs). Everything else is unchanged and not listed. A deleted
// entry carries the end source of its last piece and that piece's end: a
// producer's Delete, a liveness expiry (silence past the producer's own TTL:
// not knowing, not an absence) or the cascade. Edges are diffed by existence
// only, never by payload.
//
// The transient bucket is separate: every element of class transient or
// flapping in W, whatever its net bucket, with its pieces and each piece's end
// source. An element can be deleted and transient at once.
//
// Two subjects are two elements, whatever their descriptions share: an identity
// change is a deletion and a creation, never paired.
//
// A quarantined scope entity, and an edge with a quarantined endpoint, is
// listed in [Result].Quarantined and in no bucket; the diff still succeeds.
// Quarantined elements have no events: their pieces are not cascaded
// existence.
//
// # Description changes
//
// An entity's description at an instant is the store's single payload
// attribute of the producer that wins under [Options].Rank there, as package
// interval merges it: by rank, never by recency. With no Rank the smallest
// producer name wins, so a caller passes the ranks the store was opened with.
//
// When a different producer wins at the two instants, the change is
// [ChangeSourceChanged], naming both: two producers' descriptions are not
// comparable, and they are never compared key by key. When one producer wins
// at both and the payload bytes are equal, nothing changed. When both payloads
// are JSON objects (an empty payload is the empty object), the top-level keys
// whose values differ, or which are on one side only, are listed, split into
// state keys and attribute keys by the state-key table of [PolicyVersion]; a
// key not in the table is an attribute. Values compare as decoded JSON values,
// so key order and spacing do not count, and a number compares by its text.
// Payloads that differ only in order or spacing are no change. When either is
// not a JSON object, the change is [ChangeOpaque].
//
// The state-key table of version 1 is phase, reason, conditions, containers,
// deletion_timestamp, host_ip, pod_ips, node_info and node_name (k8sobjects),
// and k8s.pod.phase, k8s.pod.status_reason, the five k8s.node.condition_ keys,
// container.status, container.status.reason, k8s.node.name and
// container.image.tag (k8scluster). In k8scluster's payloads k8s.node.name is
// also a node's own name, which cannot change under one node UID, so that
// meaning reports nothing.
//
// # The change log
//
// [Events] lists every piece boundary strictly inside W of the elements in the
// bucket lists: an appearance at each piece start after t1, and a
// disappearance with its end source at each piece end before W's end, ordered
// by time, then subject, then appearance first. Description changes are not in
// the log. When a list is truncated, the log lacks the boundaries of the
// elements that no list holds.
//
// # Counts and the result budget
//
// [Result].Counts holds the exact size of every bucket, entities and relations
// apart, since their ratio is the triage signal; counts are never truncated.
// Each list, the quarantined one included, is ordered by subject and holds at
// most [Policy].MaxListed entries, and [Result].Truncated counts the rest. A scope of more than
// [Policy].MaxScope distinct entities is refused, because a partial diff would
// mislead.
//
// # One token, and errors
//
// The first read resolves the token, and every later read uses it;
// [Result].Token names it. A diff is in event time only: what was known at two
// tokens is two diffs. Arguments are checked before any store call. After that
// any error fails the whole diff, with no partial answer; a retained layer
// gives an error wrapping interval.ErrRetained.
//
// # Reads
//
// A diff makes one to three interval calls, and [Result].Reads counts them and
// their store reads: one call for the scope's entities, then one per edge
// direction in which some scope entity has an edge layer; one Window per scope
// entity and edge layer; and one existence read (an Alive and, unless
// quarantined, an EntityWindow) per scope entity, plus, per edge call, one per
// distinct endpoint of an edge with an interval of its own in W. The read
// budget is fixed in [BudgetDate] and the constants beside it. The Window
// reads are not bounded by the window: they read every edge record from the
// start of the store's history, until the store has an interval read of its
// own.
package graphdiff
