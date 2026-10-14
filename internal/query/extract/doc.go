// Package extract derives the graph a range query renders for a service: its
// dependency hop, and every service that shared a pod, a node or a host with
// a pod of that hop during a window. It is one extraction with two seed
// strategies, which differ only in the seed pods they choose. Every path is
// checked by interval intersection, never by walking a union graph.
//
// Everything it returns is derived: a pure function of the facts a store holds
// at one token, the seed, the window and the policy ([PolicyVersion], carried
// with the versions of packages coresidency and interval), computed with
// package interval and package coresidency. Nothing is written back, and the
// package has no write path.
//
// # Seeds
//
// A seed is an [Exploratory] service, or an [Anchored] service, host and
// instant, as from a host-tagged event; the window is the context. Fingerprints
// are resolved by the caller: this package resolves no names and matches
// exactly.
//
// # Members, and direction
//
// The members are the seed service, its callers (a depends_on into it) and its
// callees (a depends_on from it), one hop only, each with its depends_on
// elements read with the cascade. A callee's failure hits the seed and the
// seed's failure hits its callers; that direction belongs to depends_on and to
// no placement mechanism, whose pairs say only that two services shared a
// place.
//
// # Instances and pods, only through edges
//
// A member's instances are its instance_of edges, and an instance's pods are its
// part_of edges to a k8s.pod. Nothing is mapped from a description, a label or
// a name. An instance part of two pods at once is unattributed over the overlap
// ([SeveralPods]), and so is an instance of two services at once
// ([SeveralServices]); neither pod or service counts for it there. A claim
// whose other end is quarantined still counts as a claim, but is never
// followed.
//
// # Seed pods
//
// For an exploratory seed, every pod of every member is a seed pod over the
// instants one of the member's instances is mapped singly to both, ranked by
// member and then by that time, up to [Policy].MaxSeedPods. For an anchored
// seed, the seed pods are found from the host side: the host's nodes at the
// instant, the pods on them at the instant, their instances, and the
// instances' services. A pod with an instance of a member, both mapped singly
// at the instant, is a candidate. Each candidate's Find (always with hosts)
// confirms it when its seed hosts attribute the pod to the anchor's host at the
// instant. The anchor is [OnHost] when a pod of the seed service is confirmed;
// the neighbours' candidates are then confirmed in turn. It is [NotOnHost] when
// no pod of the seed service is a candidate, and no Find runs. It is
// [HostUnattributed] when the seed service's candidates ran and none was
// confirmed, and nothing is expanded. Co-residency then expands from every seed
// pod over the whole window, exactly as for an exploratory seed.
//
// # Pairs
//
// A seed pod's co-residents come from its Find at the extraction's token.
// Each co-resident pod is mapped to services the same way, through its
// instances; one with no instance mapped singly to a service is listed in
// [Result].Unmapped over those instants, never guessed. A pair of a member and
// another service by one mechanism is the union, over every contribution, of
// the intersection of every leg: the member instance's two mapping legs, the
// pod-level entry, and the peer instance's two. A member is never paired with
// itself, and a pair of two members is reported once. Two instances part of
// one pod are a shared pod, which needs no placement read. Each span names the
// legs that end where it ends, for every contribution live just before. Before
// reads the legs' pieces, so a mapping ambiguity that ends exactly at From is
// not seen by it.
//
// # Ranking
//
// Pairs are ranked by total duration, descending, as co-residency ranks its
// entries and as the plan ranks by overlap; the mechanism only breaks ties
// (shared pod, then shared node, then shared host), then the rank of the
// members, then peer and member fingerprint. The order decides what
// [Policy].MaxPairs keeps. It is a derivation rule of [PolicyVersion]: the
// owner may choose another, and a change raises the version.
//
// # Ubiquity
//
// The frontier is the nodes the seed pods were placed on alone. A peer service
// covers a node of it when it has a shared-node contribution through that
// node. A peer that is not a member and covers at least
// UbiquityNum/UbiquityDen of the frontier is ubiquitous, and its pairs move to
// [Result].Suppressed with coresidency.Ubiquitous and their coverage, shown
// and never dropped. A member is never judged: the frontier is the members'
// own pods, so its coverage would refer back to itself. The rule is judged
// only for an exploratory seed with a frontier of at least UbiquityMinNodes
// nodes; otherwise [Ubiquity].Judged is false and nothing is suppressed for
// it. A global frequency is not computed: the store has no listing read.
//
// # One token, quarantine and errors
//
// The first read resolves the window's token, and every later read and every
// Find uses it; [Result].Token names it. Edges with a quarantined endpoint are
// left out of every derivation and listed in [Result].Quarantined (each Find's
// in its own result). Arguments are checked before any store call. After that
// any error fails the whole extraction, with no partial answer; a retained
// layer gives an error wrapping interval.ErrRetained. L3 is always read, and
// L1 unless [Options].NodeOnly.
//
// # Reads
//
// [Result].Reads counts the interval calls and their store reads, every
// Find's included: one Window per entity and layer read, and one existence read
// (an Alive and, unless quarantined, an EntityWindow) per distinct endpoint of
// an edge with an interval of its own in the window, per call. The read budget
// is fixed in [BudgetDate] and the constants beside it. The Window reads are
// not bounded by the window: they read every edge record from the start of the
// store's history, until the store has an interval read of its own.
package extract
