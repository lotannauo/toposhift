package extract

import (
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the defaults of
// DefaultPolicy and the rules the answer is derived by. Any change to either
// raises it, and every result carries it, beside the versions of package
// coresidency and package interval.
const PolicyVersion = 1

// The read budget of an extraction, fixed on 2026-10-14 before anything was
// measured. It changes only by a new dated comment beside this one, never by
// editing the date or a number in place. The unit is the existence read of
// package coresidency: one Alive and, for an entity that is not quarantined,
// one EntityWindow.
//
// The anchored reference cell is coresidency's reference node (110 live pods, a
// one-hour window ending at the newest instant, a 30-day store) on a host with
// that one node, a seed service with three callers and three callees, three
// containers and one instance per pod, and the seed and each neighbour with one
// pod on the node at the anchor's instant: seven seed pods. Its mapping from the
// host side is at most BudgetAnchorMapping existence reads, each Find at most
// coresidency.BudgetReferenceExistence, and the whole extraction at most
// BudgetAnchoredExistence. The exploratory reference cell is a service on 40
// hosts, each node like the anchored cell's: each seed pod costs at most
// BudgetPerSeedPod (its Find and the mapping of its co-residents), and the
// whole extraction at most BudgetExploratoryExistence. In both, the work is at
// most BudgetWorkFactor times the sum of the per-read work budgets. A cell over
// budget is reported as a finding (the per-pod fan-out then needs a batched
// store read), never fixed by raising a constant.
const (
	BudgetDate          = "2026-10-14"
	BudgetAnchorMapping = 1536 // A1 to A4: about 2 + 131 + 650 + 400
	BudgetPerSeedPod    = 2048 // one Find (at most 512) and its co-residents' mapping
	// BudgetAnchoredExistence is BudgetAnchorMapping and seven Finds of 512.
	BudgetAnchoredExistence = 5120
	// BudgetExploratoryExistence is DefaultPolicy's MaxSeedPods (64) seed pods
	// of BudgetPerSeedPod.
	BudgetExploratoryExistence = 131072
	BudgetWorkFactor           = 1.5
)

// Seed is where an extraction starts: an [Exploratory] or an [Anchored] value.
type Seed interface{ isSeed() }

// Exploratory seeds an extraction with a service and the window alone: every
// pod of the service and of its dependency neighbours is a seed pod, up to
// [Policy].MaxSeedPods.
type Exploratory struct {
	// Service is a service fingerprint, resolved by the caller.
	Service identity.Fingerprint
}

// Anchored seeds an extraction with a service, a host and an instant, as from a
// host-tagged event: the seed pods are the pods of the service and of its
// dependency neighbours that were on the host at At. The window is the context
// co-residency is then read over.
type Anchored struct {
	// Service is a service fingerprint and Host a host fingerprint, both
	// resolved by the caller.
	Service, Host identity.Fingerprint
	// At must be in the window.
	At time.Time
}

func (Exploratory) isSeed() {}
func (Anchored) isSeed()    {}

// Policy is the derivation policy of an extraction.
type Policy struct {
	// MaxMembers caps the members, the seed included; MaxInstances the instances
	// of each member; MaxSeedPods the seed pods of the query; MaxPairs the pairs
	// and, separately, the suppressed pairs. Each is at least 1.
	MaxMembers, MaxInstances, MaxSeedPods, MaxPairs int
	// A peer service is ubiquitous when it shares a node with the query's seed
	// pods on at least UbiquityNum/UbiquityDen of the query's frontier of nodes,
	// judged only on a frontier of at least UbiquityMinNodes nodes.
	// 0 <= UbiquityNum <= UbiquityDen, UbiquityDen >= 1 and UbiquityMinNodes >= 1.
	UbiquityNum, UbiquityDen, UbiquityMinNodes int
	// CoResidency is the policy of every Find. The zero value means
	// coresidency.DefaultPolicy(); any other is validated as given.
	CoResidency coresidency.Policy
}

// DefaultPolicy is version 1's defaults: 16 members, 64 instances per member,
// 64 seed pods and 4096 pairs; ubiquity at 4/5 of a frontier of at least 5
// nodes; and coresidency.DefaultPolicy(). They are placeholders until measured.
func DefaultPolicy() Policy {
	return Policy{
		MaxMembers: 16, MaxInstances: 64, MaxSeedPods: 64, MaxPairs: 4096,
		UbiquityNum: 4, UbiquityDen: 5, UbiquityMinNodes: 5,
		CoResidency: coresidency.DefaultPolicy(),
	}
}

func (p Policy) validate() error {
	c := p.CoResidency
	switch {
	case p.MaxMembers < 1, p.MaxInstances < 1, p.MaxSeedPods < 1, p.MaxPairs < 1:
		return fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	case p.UbiquityDen < 1, p.UbiquityNum < 0, p.UbiquityNum > p.UbiquityDen:
		return fmt.Errorf("the ubiquity fraction %d/%d is not in [0, 1]: %w", p.UbiquityNum, p.UbiquityDen, store.ErrInvalid)
	case p.UbiquityMinNodes < 1:
		return fmt.Errorf("the ubiquity minimum %d is below 1 node: %w", p.UbiquityMinNodes, store.ErrInvalid)
	case c.MinOverlap < 0:
		return fmt.Errorf("the co-residency minimum overlap %s is negative: %w", c.MinOverlap, store.ErrInvalid)
	case c.MaxNodes < 1, c.MaxHosts < 1, c.MaxPerVia < 1, c.MaxResults < 1:
		return fmt.Errorf("a co-residency cap of %+v is below 1: %w", c, store.ErrInvalid)
	}
	return nil
}

// Options configures one extraction.
type Options struct {
	// NodeOnly derives shared node and shared pod only: no host lift, and no
	// read of L1. An anchored seed needs its host's runs_on, so it refuses it.
	NodeOnly bool
	// Policy is the derivation policy. The zero Policy means DefaultPolicy().
	Policy Policy
}

// Role is a member's place in the dependency hop, a bit set: a service that both
// calls the seed and is called by it is a caller and a callee.
type Role uint8

const (
	// RoleSeed is the seed service.
	RoleSeed Role = 1 << iota
	// RoleCaller is a service that depends on the seed: the seed's failure hits
	// it.
	RoleCaller
	// RoleCallee is a service the seed depends on: its failure hits the seed.
	RoleCallee
)

// String names the roles, joined by "|": "seed", "caller", "callee", and
// "none" for no bit.
func (r Role) String() string {
	var parts []string
	for _, b := range []struct {
		bit  Role
		name string
	}{{RoleSeed, "seed"}, {RoleCaller, "caller"}, {RoleCallee, "callee"}} {
		if r&b.bit != 0 {
			parts = append(parts, b.name)
		}
	}
	if rest := r &^ (RoleSeed | RoleCaller | RoleCallee); rest != 0 {
		parts = append(parts, fmt.Sprintf("Role(%d)", uint8(rest)))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "|")
}

// Mechanism says how two services shared fate through placement. Between pairs
// of equal duration the order is the ranking: a shared pod is the strongest.
type Mechanism uint8

const (
	// MechanismNone is the zero value; no pair has it.
	MechanismNone Mechanism = iota
	// SharedPod is two instances part of one pod at once.
	SharedPod
	// SharedNode is a pod of each scheduled on one node at once.
	SharedNode
	// SharedHost is a pod of each on two distinct nodes at once, each node
	// placed on the one host by an asserted runs_on.
	SharedHost
)

// String names the mechanism: "none", "shared_pod", "shared_node" or
// "shared_host", and "Mechanism(n)" for any other value.
func (m Mechanism) String() string {
	switch m {
	case MechanismNone:
		return "none"
	case SharedPod:
		return "shared_pod"
	case SharedNode:
		return "shared_node"
	case SharedHost:
		return "shared_host"
	}
	return fmt.Sprintf("Mechanism(%d)", uint8(m))
}

// Member is the seed service or one of its dependency neighbours.
type Member struct {
	Service identity.Fingerprint
	Role    Role
	// Dependency is its depends_on elements to or from the seed, with the
	// cascade: the caller's then the callee's. It is empty for the seed.
	Dependency []interval.Element
	// Instances are the instance_of elements into it that were read, ordered by
	// subject: all of them (up to MaxInstances) for an exploratory seed; for an
	// anchored seed, the seed's own and those of the instances found on the
	// anchor's nodes.
	Instances []interval.Element
}

// Contribution is one path by which a pair's services shared a place. It is
// derived.
type Contribution struct {
	// MemberPod is the seed pod the path was found from, and MemberInst its
	// instance; PeerPod and PeerInst are the other side's. For a pair of two
	// members, MemberPod is a pod of either member.
	MemberPod, PeerPod   identity.Fingerprint
	MemberInst, PeerInst identity.Fingerprint
	// Via is the pod shared (SharedPod), or the node or host of the pod-level
	// entry.
	Via identity.Fingerprint
	// Entry indexes the CoResidents of the PodResults element whose Seed is
	// MemberPod; it is -1 for a shared pod, which has no pod-level entry.
	Entry int
	// Legs are the mapping legs, as the interval package reads them:
	// MemberInst→its service, MemberInst→MemberPod, PeerInst→PeerPod and
	// PeerInst→its service.
	Legs []interval.Element
	// Spans are the instants at which every mapping leg holds singly and, for a
	// shared node or host, the pod-level entry holds.
	Spans []interval.Span
}

// End is one reason a pair span ends inside the window.
type End struct {
	// Contribution indexes the pair's Contributions.
	Contribution int
	// Leg is 0 to 3 for the contribution's mapping legs, and 4 plus the leg of
	// the pod-level entry otherwise.
	Leg int
	// Pod is the pod-level entry's end, when Leg is 4 or more.
	Pod *coresidency.End
	// Source is the mapping leg piece's end source; lifecycle.EndUnknown when
	// Unattributed or when Pod is set.
	Source lifecycle.EndSource
	// Unattributed says the mapping leg goes on, but the instance became part of
	// several pods, or an instance of several services, here.
	Unattributed bool
}

// Span is one span of a pair. It is derived.
type Span struct {
	interval.Span
	// Before says the span starts at the window's From and some contribution
	// starting there has every leg's piece, and the pod-level overlap, begun
	// before From. It reads the legs' pieces, not the instants a leg holds
	// singly, so a mapping ambiguity (SeveralPods, SeveralServices) that ends
	// exactly at From is not seen by it.
	Before bool
	// Ends lists, for every contribution live just before End, each of its legs
	// that ends at End. It is empty exactly when End is the window's To.
	Ends []End
}

// Pair is a service-level co-residency: a member and a peer service that shared
// a place by one mechanism, the union over every contribution. It is derived.
type Pair struct {
	// Member is a member, and Peer the other service. A pair of two members is
	// reported once, with Member the one first in fingerprint order.
	Member, Peer identity.Fingerprint
	Mechanism    Mechanism
	// Spans are ascending, disjoint, never touching and never empty.
	Spans []Span
	// Vias holds, for each pod, node or host shared, the union of the spans of
	// the contributions through it.
	Vias          map[identity.Fingerprint][]interval.Span
	Contributions []Contribution
	// Duration is the total length of the spans.
	Duration    time.Duration
	Class       interval.Class
	Transitions int
	// Reason is coresidency.ReasonNone in Pairs, and coresidency.Ubiquitous in
	// Suppressed.
	Reason coresidency.Reason
	// Coverage is (covered, frontier nodes) of the pair's peer service when
	// ubiquity is judged and the peer is not a member; a member is never
	// judged, and a pair of two members keeps a zero Coverage.
	Coverage [2]int
}

// Spans returns the spans of a pair, as interval spans.
func Spans(p Pair) []interval.Span {
	out := make([]interval.Span, len(p.Spans))
	for i, s := range p.Spans {
		out[i] = s.Span
	}
	return out
}

// AnchorState says what an anchored seed found on its host.
type AnchorState uint8

const (
	// AnchorNone is an exploratory extraction.
	AnchorNone AnchorState = iota
	// OnHost is a pod of the seed service on the host at the anchor's instant,
	// confirmed by its Find.
	OnHost
	// NotOnHost is no pod of the seed service on the host's nodes at the
	// anchor's instant: no Find runs.
	NotOnHost
	// HostUnattributed is pods of the seed service on the host's nodes, none of
	// whose placement on the host is attributed at the instant: the node has no
	// host, several, or only a quarantined one, or the pod is on several nodes.
	// Nothing is expanded.
	HostUnattributed
)

// String names the state: "none", "on_host", "not_on_host" or
// "host_unattributed", and "AnchorState(n)" for any other value.
func (a AnchorState) String() string {
	switch a {
	case AnchorNone:
		return "none"
	case OnHost:
		return "on_host"
	case NotOnHost:
		return "not_on_host"
	case HostUnattributed:
		return "host_unattributed"
	}
	return fmt.Sprintf("AnchorState(%d)", uint8(a))
}

// UnattributedReason says why the extraction refuses a mapping over some spans.
type UnattributedReason uint8

const (
	// SeveralPods is an instance part of two or more pods at once.
	SeveralPods UnattributedReason = iota + 1
	// SeveralServices is an instance of two or more services at once.
	SeveralServices
)

// String names the reason: "several_pods" or "several_services", and
// "UnattributedReason(n)" for any other value.
func (r UnattributedReason) String() string {
	switch r {
	case SeveralPods:
		return "several_pods"
	case SeveralServices:
		return "several_services"
	}
	return fmt.Sprintf("UnattributedReason(%d)", uint8(r))
}

// Unattributed is a mapping or a placement the extraction refuses to guess.
type Unattributed struct {
	// SeedPod is the pod whose Find reported a placement; it is zero for the
	// extraction's own refusal of a mapping.
	SeedPod identity.Fingerprint
	// Subject is the instance (Mapping), or the Find's subject (Placement).
	Subject identity.Fingerprint
	Spans   []interval.Span
	// Mapping is set for the extraction's own refusals, and Placement for a
	// Find's.
	Mapping   UnattributedReason
	Placement coresidency.UnattributedReason
	// Candidates are the pods, services, nodes or hosts claimed, sorted.
	Candidates []identity.Fingerprint
}

// Unmapped is a co-resident pod with no instance mapped singly to a service over
// some spans of its co-residency. It is shown, never guessed from a
// description.
type Unmapped struct {
	Pod   identity.Fingerprint
	Spans []interval.Span
}

// Truncation reports what the policy's caps left out.
type Truncation struct {
	// Members, Instances and SeedPods were not used: over MaxMembers,
	// MaxInstances or MaxSeedPods. Each is sorted.
	Members, Instances, SeedPods []identity.Fingerprint
	// Pairs counts the pairs dropped by MaxPairs, and SuppressedDropped the
	// suppressed pairs.
	Pairs, SuppressedDropped int
}

// Ubiquity says whether the ubiquity rule was judged, and over how many nodes.
type Ubiquity struct {
	// Judged is false for an anchored seed, and for a frontier below
	// UbiquityMinNodes; nothing is then suppressed for ubiquity.
	Judged bool
	// FrontierNodes is the number of nodes the seed pods were placed on alone.
	FrontierNodes int
}

// Result is an extraction, read at one token. It is derived.
type Result struct {
	Token    uint64
	From, To time.Time
	Seed     Seed
	Anchor   AnchorState
	// Members are ranked: the seed first, then by the duration of the
	// dependency, then by fingerprint.
	Members []Member
	// Pairs are ranked by total duration, descending; then the mechanism
	// breaks ties (shared pod, shared node, shared host); then the rank of their
	// members, then peer and member fingerprint, as co-residency ranks its
	// entries. This order is a derivation rule of PolicyVersion 1, which a
	// change raises. Suppressed is ranked the same.
	Pairs      []Pair
	Suppressed []Pair
	// Unmapped is ordered by pod.
	Unmapped []Unmapped
	// Unattributed holds the extraction's own refusals, ordered by subject and
	// first span, then every Find's, in the order of PodResults.
	Unattributed []Unattributed
	// Quarantined holds the edges the extraction read but did not follow because
	// an endpoint is quarantined, ordered by subject. Each Find's own are in its
	// PodResults element.
	Quarantined []interval.Element
	Ubiquity    Ubiquity
	Truncated   Truncation
	// PodResults holds every Find that ran, in the order it ran: the seed pods
	// in rank order and, for an anchored seed, the candidates that were not
	// confirmed.
	PodResults []coresidency.Result
	// PolicyVersion is this package's; CoResidencyPolicy and IntervalPolicy are
	// coresidency.PolicyVersion and interval.PolicyVersion.
	PolicyVersion, CoResidencyPolicy, IntervalPolicy int
	// Reads counts every read request, every Find's included; each PodResults
	// element's Reads is that Find's share. StoreReads counts those that
	// reached the store: the extraction keeps the answer of a read it repeats
	// at its one token, up to a fixed size, so StoreReads is at most Reads
	// field by field, and its Calls is always 0.
	Reads, StoreReads coresidency.Reads
}
