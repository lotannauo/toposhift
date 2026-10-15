package subgraph

import (
	"fmt"

	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tables below are the wire's own, like those of tables.go: explicit
// values, a function that names every case of another package's enum and
// refuses the rest, and a golden test of every row. Coresidency's SharedNode is
// 1 and extract's is 2, so no Go constant is ever cast to a wire value.

// DerivedKind names a rule that derives a subject no producer asserted. The
// table does not use the catalog's relation ids: the catalog's only derived
// relation is same_as, and the other rules derive paths and reach, not
// relations. The strings are the rules of the MCP vocabulary, which labels the
// same subjects.
type DerivedKind uint8

const (
	// DerivedCoSharedNode is a pod that shared a node with the seed pod.
	DerivedCoSharedNode DerivedKind = 1
	// DerivedCoSharedHost is a pod that shared a host with the seed pod through
	// two nodes.
	DerivedCoSharedHost DerivedKind = 2
	// DerivedPairSharedPod is two services with instances in one pod.
	DerivedPairSharedPod DerivedKind = 3
	// DerivedPairSharedNode is two services with pods on one node.
	DerivedPairSharedNode DerivedKind = 4
	// DerivedPairSharedHost is two services with pods on one host.
	DerivedPairSharedHost DerivedKind = 5
	// DerivedReach is an entity a failing seed reaches.
	DerivedReach DerivedKind = 6
	// DerivedSameAs is the catalog's derived relation of that name. It is the
	// rule of an edge element of that relation, never of a derived key.
	DerivedSameAs DerivedKind = 7
)

// The rule strings of the derived kinds.
const (
	RuleCoSharedNode   = "co_residency.shared_node"
	RuleCoSharedHost   = "co_residency.shared_host"
	RulePairSharedPod  = "pair.shared_pod"
	RulePairSharedNode = "pair.shared_node"
	RulePairSharedHost = "pair.shared_host"
	RuleReach          = "blast_radius.reach"
	RuleSameAs         = "same_as"
)

type derivedRow struct {
	rule string
	// edge says the rule is of an edge element of a derived relation, not of a
	// derived key.
	edge bool
	// legEnds says a piece of an element of the rule that ends inside the window
	// lists the legs that end there; otherwise it lists none, because the span
	// ends where an intersection of reach ends and has no leg of its own to
	// name.
	legEnds bool
}

var derivedKinds = [...]derivedRow{
	DerivedCoSharedNode:   {rule: RuleCoSharedNode, legEnds: true},
	DerivedCoSharedHost:   {rule: RuleCoSharedHost, legEnds: true},
	DerivedPairSharedPod:  {rule: RulePairSharedPod, legEnds: true},
	DerivedPairSharedNode: {rule: RulePairSharedNode, legEnds: true},
	DerivedPairSharedHost: {rule: RulePairSharedHost, legEnds: true},
	DerivedReach:          {rule: RuleReach},
	DerivedSameAs:         {rule: RuleSameAs, edge: true},
}

// DerivedKinds lists the table, ascending by value.
func DerivedKinds() []DerivedKind {
	out := make([]DerivedKind, 0, len(derivedKinds)-1)
	for k := DerivedKind(1); int(k) < len(derivedKinds); k++ {
		out = append(out, k)
	}
	return out
}

// Valid reports whether k is a value of the table.
func (k DerivedKind) Valid() bool { return k >= 1 && int(k) < len(derivedKinds) }

// Rule is the rule string of the kind, and "DerivedKind(n)" outside the table.
func (k DerivedKind) Rule() string {
	if k.Valid() {
		return derivedKinds[k].rule
	}
	return fmt.Sprintf("DerivedKind(%d)", uint8(k))
}

func (k DerivedKind) String() string { return k.Rule() }

// DerivedKindOf returns the kind with rule r, if the table has one.
func DerivedKindOf(r string) (DerivedKind, bool) {
	for k := DerivedKind(1); int(k) < len(derivedKinds); k++ {
		if derivedKinds[k].rule == r {
			return k, true
		}
	}
	return 0, false
}

// Mechanism says how two things shared a place. Its values are not those of
// package coresidency or package extract, whose constants differ.
type Mechanism uint8

const (
	// MechanismSharedPod is two instances part of one pod at once.
	MechanismSharedPod Mechanism = 1
	// MechanismSharedNode is two pods on one node at once.
	MechanismSharedNode Mechanism = 2
	// MechanismSharedHost is two pods on two nodes of one host at once.
	MechanismSharedHost Mechanism = 3
)

// String names the mechanism: "shared_pod", "shared_node" or "shared_host", and
// "Mechanism(n)" for any other value.
func (m Mechanism) String() string {
	switch m {
	case MechanismSharedPod:
		return "shared_pod"
	case MechanismSharedNode:
		return "shared_node"
	case MechanismSharedHost:
		return "shared_host"
	}
	return fmt.Sprintf("Mechanism(%d)", uint8(m))
}

// Valid reports whether m is a value of the table.
func (m Mechanism) Valid() bool { return m >= MechanismSharedPod && m <= MechanismSharedHost }

// MechanismOfCoResidency maps package coresidency's mechanism to its wire
// value, naming every case. MechanismNone and any value outside the package's
// table are refused.
func MechanismOfCoResidency(m coresidency.Mechanism) (Mechanism, error) {
	switch m {
	case coresidency.SharedNode:
		return MechanismSharedNode, nil
	case coresidency.SharedHost:
		return MechanismSharedHost, nil
	}
	return 0, fmt.Errorf("subgraph: the co-residency mechanism %s has no wire value: %w", m, store.ErrInvalid)
}

// MechanismOfExtract maps package extract's mechanism to its wire value, naming
// every case. MechanismNone and any value outside the package's table are
// refused.
func MechanismOfExtract(m extract.Mechanism) (Mechanism, error) {
	switch m {
	case extract.SharedPod:
		return MechanismSharedPod, nil
	case extract.SharedNode:
		return MechanismSharedNode, nil
	case extract.SharedHost:
		return MechanismSharedHost, nil
	}
	return 0, fmt.Errorf("subgraph: the extraction mechanism %s has no wire value: %w", m, store.ErrInvalid)
}

// coRule is the derived kind of a co-resident of the mechanism.
func (m Mechanism) coRule() (DerivedKind, error) {
	switch m {
	case MechanismSharedNode:
		return DerivedCoSharedNode, nil
	case MechanismSharedHost:
		return DerivedCoSharedHost, nil
	}
	return 0, fmt.Errorf("subgraph: the mechanism %s has no co-resident rule: %w", m, store.ErrInvalid)
}

// pairRule is the derived kind of a pair of the mechanism.
func (m Mechanism) pairRule() (DerivedKind, error) {
	switch m {
	case MechanismSharedPod:
		return DerivedPairSharedPod, nil
	case MechanismSharedNode:
		return DerivedPairSharedNode, nil
	case MechanismSharedHost:
		return DerivedPairSharedHost, nil
	}
	return 0, fmt.Errorf("subgraph: the mechanism %s has no pair rule: %w", m, store.ErrInvalid)
}

// SuppressReason says why an entry is listed as suppressed and not shown.
type SuppressReason uint8

const (
	// SuppressedBelowMinOverlap is an entry whose total overlap is below the
	// policy's minimum, with both ends inside the window.
	SuppressedBelowMinOverlap SuppressReason = 1
	// SuppressedUbiquitous is a pair whose peer is co-resident with nearly
	// every node of the frontier.
	SuppressedUbiquitous SuppressReason = 2
)

// String names the reason: "below_min_overlap" or "ubiquitous", and
// "SuppressReason(n)" for any other value.
func (r SuppressReason) String() string {
	switch r {
	case SuppressedBelowMinOverlap:
		return "below_min_overlap"
	case SuppressedUbiquitous:
		return "ubiquitous"
	}
	return fmt.Sprintf("SuppressReason(%d)", uint8(r))
}

// Valid reports whether r is a value of the table.
func (r SuppressReason) Valid() bool {
	return r == SuppressedBelowMinOverlap || r == SuppressedUbiquitous
}

// SuppressReasonOf maps package coresidency's reason to its wire value. ReasonNone
// is the reason of an entry that is not suppressed, and is refused with any
// value outside the package's table.
func SuppressReasonOf(r coresidency.Reason) (SuppressReason, error) {
	switch r {
	case coresidency.BelowMinOverlap:
		return SuppressedBelowMinOverlap, nil
	case coresidency.Ubiquitous:
		return SuppressedUbiquitous, nil
	}
	return 0, fmt.Errorf("subgraph: the reason %s does not suppress an entry: %w", r, store.ErrInvalid)
}

// UnattributedReason says why a placement, a mapping or a claim is not
// attributed over some spans.
type UnattributedReason uint8

const (
	// UnattributedNoHost is a node of the seed with no live runs_on.
	UnattributedNoHost UnattributedReason = 1
	// UnattributedSeveralHosts is a node with live runs_on to two or more hosts.
	UnattributedSeveralHosts UnattributedReason = 2
	// UnattributedQuarantinedHost is a node whose only live runs_on is to a
	// quarantined host.
	UnattributedQuarantinedHost UnattributedReason = 3
	// UnattributedSeveralNodes is a pod scheduled on two or more nodes.
	UnattributedSeveralNodes UnattributedReason = 4
	// UnattributedSeveralPods is an instance part of two or more pods.
	UnattributedSeveralPods UnattributedReason = 5
	// UnattributedSeveralServices is an instance of two or more services.
	UnattributedSeveralServices UnattributedReason = 6
	// UnattributedSeveralClaims is a subject claiming two or more targets of one
	// relation and type while one of them is reached.
	UnattributedSeveralClaims UnattributedReason = 7
)

var unattributedNames = [...]string{
	UnattributedNoHost:          "no_host",
	UnattributedSeveralHosts:    "several_hosts",
	UnattributedQuarantinedHost: "quarantined_host",
	UnattributedSeveralNodes:    "several_nodes",
	UnattributedSeveralPods:     "several_pods",
	UnattributedSeveralServices: "several_services",
	UnattributedSeveralClaims:   "several_claims",
}

// Valid reports whether r is a value of the table.
func (r UnattributedReason) Valid() bool { return r >= 1 && int(r) < len(unattributedNames) }

// String names the reason as the MCP vocabulary does, and
// "UnattributedReason(n)" for any other value.
func (r UnattributedReason) String() string {
	if r.Valid() {
		return unattributedNames[r]
	}
	return fmt.Sprintf("UnattributedReason(%d)", uint8(r))
}

// UnattributedReasonOfCoResidency maps package coresidency's reason to its wire
// value, naming every case and refusing the rest.
func UnattributedReasonOfCoResidency(r coresidency.UnattributedReason) (UnattributedReason, error) {
	switch r {
	case coresidency.NoHost:
		return UnattributedNoHost, nil
	case coresidency.SeveralHosts:
		return UnattributedSeveralHosts, nil
	case coresidency.QuarantinedHost:
		return UnattributedQuarantinedHost, nil
	case coresidency.SeveralNodes:
		return UnattributedSeveralNodes, nil
	}
	return 0, fmt.Errorf("subgraph: the placement reason %s has no wire value: %w", r, store.ErrInvalid)
}

// UnattributedReasonOfExtract maps package extract's mapping reason to its wire
// value, naming every case and refusing the rest.
func UnattributedReasonOfExtract(r extract.UnattributedReason) (UnattributedReason, error) {
	switch r {
	case extract.SeveralPods:
		return UnattributedSeveralPods, nil
	case extract.SeveralServices:
		return UnattributedSeveralServices, nil
	}
	return 0, fmt.Errorf("subgraph: the mapping reason %s has no wire value: %w", r, store.ErrInvalid)
}

// AnchorState says what an anchored extraction found on its host. The zero
// value is [AnchorNone], an exploratory extraction and every other query.
type AnchorState uint8

const (
	// AnchorNone is a query with no anchor.
	AnchorNone AnchorState = 0
	// AnchorOnHost is a pod of the seed service on the host at the anchor's
	// instant, confirmed.
	AnchorOnHost AnchorState = 1
	// AnchorNotOnHost is no pod of the seed service on the host's nodes then.
	AnchorNotOnHost AnchorState = 2
	// AnchorHostUnattributed is pods of the seed service on the host's nodes,
	// none of whose placement on the host is attributed then.
	AnchorHostUnattributed AnchorState = 3
)

// String names the state: "none", "on_host", "not_on_host" or
// "host_unattributed", and "AnchorState(n)" for any other value.
func (a AnchorState) String() string {
	switch a {
	case AnchorNone:
		return "none"
	case AnchorOnHost:
		return "on_host"
	case AnchorNotOnHost:
		return "not_on_host"
	case AnchorHostUnattributed:
		return "host_unattributed"
	}
	return fmt.Sprintf("AnchorState(%d)", uint8(a))
}

// Valid reports whether a is a value of the table.
func (a AnchorState) Valid() bool { return a <= AnchorHostUnattributed }

// AnchorStateOf maps package extract's anchor state to its wire value, naming
// every case and refusing the rest.
func AnchorStateOf(a extract.AnchorState) (AnchorState, error) {
	switch a {
	case extract.AnchorNone:
		return AnchorNone, nil
	case extract.OnHost:
		return AnchorOnHost, nil
	case extract.NotOnHost:
		return AnchorNotOnHost, nil
	case extract.HostUnattributed:
		return AnchorHostUnattributed, nil
	}
	return 0, fmt.Errorf("subgraph: the anchor state %s has no wire value: %w", a, store.ErrInvalid)
}
