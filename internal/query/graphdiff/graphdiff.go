package graphdiff

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the defaults of
// DefaultPolicy, the state-key table and the rules a diff is derived by. Any
// change to one of them raises it, and every result carries it.
const PolicyVersion = 1

// The read budget of a diff, fixed on 2026-10-14 before anything was measured.
// It changes only by a new dated comment beside this one, never by editing the
// date or a number in place. An existence read is one Alive and, for an entity
// that is not quarantined, one EntityWindow.
//
// A diff of a scope of one node N makes at most 3 + D + C existence reads,
// where D is the number of distinct pods with a scheduled_on interval of their
// own to N in the window and C the number of distinct hosts N claims in it,
// never one per record and never one per pod of N's history before the
// window. Each existence read stays within the per-read work budget of the
// store's read gate, and the cascade of one scope entity costs at most
// BudgetWorkFactor times the sum of those budgets. At the reference cell,
// BudgetReferencePods live pods on one node, a one-hour diff ending at the
// newest instant and a 30-day store, a diff of that node makes at most
// BudgetReferenceExistence existence reads. A cell over budget is reported as
// a finding, never fixed by raising a constant.
const (
	BudgetDate               = "2026-10-14"
	BudgetWorkFactor         = 1.5
	BudgetReferencePods      = 110
	BudgetReferenceExistence = 256
)

// The read budget of a diff, revised on 2026-10-15, when has_address gave a node
// a second forward edge layer: the comment above stands, except that a diff of a
// scope of one node N makes at most 3 + D + C + A existence reads, where A is the
// number of distinct addresses N claims through has_address in the window, and D
// and C are as above. Every constant above is unchanged.

// Policy is the result budget of a diff.
type Policy struct {
	// MaxScope is the most distinct entities a scope may name; a larger scope
	// is refused, because a partial diff would mislead. At least 1.
	MaxScope int
	// MaxListed is the most entries each bucket list, and the quarantined
	// list, holds; the rest are counted in Result.Truncated, and every one is
	// in Result.Counts. At least 1.
	MaxListed int
}

// DefaultPolicy is version 1's defaults: a scope of at most 256 entities, and
// at most 4096 entries per bucket list. Both are placeholders until measured.
func DefaultPolicy() Policy { return Policy{MaxScope: 256, MaxListed: 4096} }

func (p Policy) validate() error {
	if p.MaxScope < 1 || p.MaxListed < 1 {
		return fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	}
	return nil
}

// Options configures one diff.
type Options struct {
	// Rank is the authority order of descriptions, as the store's producers
	// are ranked (lifecycle.Policy.Rank): a listed producer beats an unlisted
	// one, a higher rank beats a lower one, and the smaller name breaks every
	// remaining tie. With no Rank the smallest name wins, which is rarely the
	// store's order: a caller passes the map the store was opened with. Only
	// the changed bucket depends on it.
	Rank map[lifecycle.Producer]int
	// Policy is the result budget. The zero Policy means DefaultPolicy(); any
	// other is validated as given.
	Policy Policy
}

// ChangeKind says how an entity's description differs between the two
// instants. The zero value is ChangeNone.
type ChangeKind uint8

const (
	// ChangeNone is no change; no entry of Result.Changed has it.
	ChangeNone ChangeKind = iota
	// ChangeKeys is one producer's description at both instants, both JSON
	// objects, with at least one top-level key that differs.
	ChangeKeys
	// ChangeOpaque is one producer's description at both instants, with bytes
	// that differ and at least one of them not a JSON object.
	ChangeOpaque
	// ChangeSourceChanged is a different winning producer at the two instants.
	// Two producers' descriptions are not compared.
	ChangeSourceChanged
)

// String names the kind: "none", "keys", "opaque" or "source_changed", and
// "ChangeKind(n)" for any other value.
func (k ChangeKind) String() string {
	switch k {
	case ChangeNone:
		return "none"
	case ChangeKeys:
		return "keys"
	case ChangeOpaque:
		return "opaque"
	case ChangeSourceChanged:
		return "source_changed"
	}
	return fmt.Sprintf("ChangeKind(%d)", uint8(k))
}

// DescriptionChange is how an entity's merged description differs between the
// two instants. It is derived.
type DescriptionChange struct {
	Kind ChangeKind
	// StateKeys and AttributeKeys are the top-level keys that differ, split by
	// the state-key table and each sorted. Set for ChangeKeys only, and then at
	// least one is not empty.
	StateKeys, AttributeKeys []string
	// FromProducer and ToProducer are the winning producers at the two
	// instants: equal unless Kind is ChangeSourceChanged.
	FromProducer, ToProducer lifecycle.Producer
}

// Entry is one element in a bucket. It is derived.
type Entry struct {
	// Element is the element's pieces in the window, with their classes and
	// end sources, as package interval reads them. An entity's carries its
	// descriptions at the two instants where it exists there; an edge's
	// carries none.
	Element interval.Element
	// DeleteSource and DeletedAt are set in Result.Deleted only: the end
	// source and the end of the element's last piece, its last disappearance.
	DeleteSource lifecycle.EndSource
	DeletedAt    time.Time
	// Change is set in Result.Changed only.
	Change *DescriptionChange
}

// Count is a number of entities and a number of relations (edges), counted
// apart because their ratio is the triage signal.
type Count struct{ Entities, Relations int }

// Counts are the exact sizes of the buckets, never truncated.
type Counts struct{ Created, Deleted, Changed, Transient, Quarantined Count }

// Truncation counts, per list, the entries Policy.MaxListed left out.
type Truncation struct{ Created, Deleted, Changed, Transient, Quarantined int }

// Reads counts the store calls of one diff: the interval calls made, and the
// Window, Alive and EntityWindow calls they made.
type Reads struct{ Calls, Windows, Alive, EntityWindows int }

// Result is a diff of a scope between two instants, read at one token. It is
// derived.
type Result struct {
	// Token is the snapshot token every read used.
	Token uint64
	// T1 and T2 are the two instants; the window read is [T1, T2+1ns).
	T1, T2 time.Time
	// Scope is the scope's entities, once each and ordered by
	// store.CompareFingerprints.
	Scope []identity.Fingerprint
	// Created, Deleted and Changed are the net buckets, each exclusive of the
	// others. Transient is every element of class transient or flapping,
	// whatever its net bucket. Each list is ordered by subject: entities
	// before edges, then by store.CompareFingerprints of the entity or the
	// edge's source, then of its target, then by relation.
	Created, Deleted, Changed, Transient []Entry
	// Quarantined holds the quarantined scope entities and the edges read
	// with a quarantined endpoint, in the same order, capped like the bucket
	// lists. They are in no bucket.
	Quarantined []interval.Element
	Counts      Counts
	Truncated   Truncation
	// PolicyVersion is this package's; IntervalPolicy is
	// interval.PolicyVersion.
	PolicyVersion, IntervalPolicy int
	Reads                         Reads
}

// Event is one appearance or disappearance strictly inside a diff's window.
type Event struct {
	At      time.Time
	Subject store.Subject
	// Appear is true for an appearance, false for a disappearance.
	Appear bool
	// Source is how a disappearance came about: lifecycle.EndProducer,
	// lifecycle.EndLivenessExpiry or lifecycle.EndCascade. It is
	// lifecycle.EndUnknown for an appearance.
	Source lifecycle.EndSource
}

// stateKeys is the state-key table of policy version 1: a top-level key of a
// description is state if it is here, and an attribute otherwise. The first
// group is k8sobjects' keys, the second k8scluster's. Any change to it raises
// PolicyVersion. In k8scluster's payloads k8s.node.name is a pod's or a
// container's node, and also a node's own name, which cannot change under one
// node UID (Kubernetes names are immutable), so its second meaning reports
// nothing.
var stateKeys = map[string]bool{
	"phase":              true,
	"reason":             true,
	"conditions":         true,
	"containers":         true,
	"deletion_timestamp": true,
	"host_ip":            true,
	"pod_ips":            true,
	"node_info":          true,
	"node_name":          true,

	"k8s.pod.phase":                          true,
	"k8s.pod.status_reason":                  true,
	"k8s.node.condition_ready":               true,
	"k8s.node.condition_memory_pressure":     true,
	"k8s.node.condition_disk_pressure":       true,
	"k8s.node.condition_pid_pressure":        true,
	"k8s.node.condition_network_unavailable": true,
	"container.status":                       true,
	"container.status.reason":                true,
	"k8s.node.name":                          true,
	"container.image.tag":                    true,
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
