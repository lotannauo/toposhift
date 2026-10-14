package coresidency

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the defaults of
// DefaultPolicy and the rules the answer is derived by. Any change to either
// raises it, and every result carries it.
const PolicyVersion = 1

// The cascade's read budget, fixed on 2026-10-14 before anything was measured.
// It changes only by a new dated comment beside this one, never by editing the
// date or a number in place. An existence read is one Alive and, for an entity
// that is not quarantined, one EntityWindow.
//
// One reverse read of a node over a window makes at most 1 + D existence reads,
// where D is the number of distinct pods with an interval of their own on that
// node in the window. At the reference cell, BudgetReferencePods live pods on a
// node, a one-hour window ending at the newest instant and a 30-day store, the
// existence reads of one shared-node query are at most BudgetReferenceExistence,
// and the cascade of one hot-node read costs at most BudgetWorkFactor times the
// sum of the per-read work budgets of its existence reads. A cell over budget is
// reported as a finding, never fixed by raising a constant.
const (
	BudgetDate               = "2026-10-14"
	BudgetWorkFactor         = 1.5
	BudgetReferencePods      = 110
	BudgetReferenceExistence = 512
)

// Mechanism says how two pods shared a place.
type Mechanism uint8

const (
	// MechanismNone is the zero value; no entry has it.
	MechanismNone Mechanism = iota
	// SharedNode is two pods scheduled on one node at once.
	SharedNode
	// SharedHost is two pods on two distinct nodes at once, each node placed
	// on the one host by an asserted runs_on.
	SharedHost
)

// String names the mechanism: "none", "shared_node" or "shared_host", and
// "Mechanism(n)" for any other value.
func (m Mechanism) String() string {
	switch m {
	case MechanismNone:
		return "none"
	case SharedNode:
		return "shared_node"
	case SharedHost:
		return "shared_host"
	}
	return fmt.Sprintf("Mechanism(%d)", uint8(m))
}

// Reason says why an entry is suppressed.
type Reason uint8

const (
	// ReasonNone is every entry of CoResidents.
	ReasonNone Reason = iota
	// BelowMinOverlap is an entry whose total overlap inside the window is below
	// the policy's MinOverlap, with both ends inside the window.
	BelowMinOverlap
	// Ubiquitous is reserved for suppressing peers that are co-resident with
	// nearly everything. Nothing in this version produces it.
	Ubiquitous
)

// String names the reason: "none", "below_min_overlap" or "ubiquitous", and
// "Reason(n)" for any other value.
func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case BelowMinOverlap:
		return "below_min_overlap"
	case Ubiquitous:
		return "ubiquitous"
	}
	return fmt.Sprintf("Reason(%d)", uint8(r))
}

// UnattributedReason says why a placement is unattributed over some spans.
type UnattributedReason uint8

const (
	// NoHost is a node of the seed with no live runs_on.
	NoHost UnattributedReason = iota + 1
	// SeveralHosts is a node of the seed with two or more live runs_on to
	// different hosts.
	SeveralHosts
	// QuarantinedHost is a node of the seed whose only live runs_on is to a
	// quarantined host.
	QuarantinedHost
	// SeveralNodes is the seed with live scheduled_on edges to two or more
	// nodes.
	SeveralNodes
)

// String names the reason: "no_host", "several_hosts", "quarantined_host" or
// "several_nodes", and "UnattributedReason(n)" for any other value.
func (r UnattributedReason) String() string {
	switch r {
	case NoHost:
		return "no_host"
	case SeveralHosts:
		return "several_hosts"
	case QuarantinedHost:
		return "quarantined_host"
	case SeveralNodes:
		return "several_nodes"
	}
	return fmt.Sprintf("UnattributedReason(%d)", uint8(r))
}

// Policy is the derivation policy of a query.
type Policy struct {
	// MinOverlap is the least total overlap, inside the window, an entry needs to
	// be a co-resident; one with both ends inside the window and less is
	// suppressed. Never negative.
	MinOverlap time.Duration
	// MaxNodes caps each frontier of nodes, MaxHosts the frontier of hosts,
	// MaxPerVia the entries per (Via, Mechanism), and MaxResults the entries and,
	// separately, the suppressed entries. Each is at least 1.
	MaxNodes, MaxHosts, MaxPerVia, MaxResults int
}

// DefaultPolicy is version 1's defaults: a minimum overlap of 1 s, because
// event times have one-second resolution and a shorter overlap is an artefact of
// ordering, and caps of 64 nodes, 16 hosts, 1024 entries per via and 4096
// results, which are placeholders until measured.
func DefaultPolicy() Policy {
	return Policy{MinOverlap: time.Second, MaxNodes: 64, MaxHosts: 16, MaxPerVia: 1024, MaxResults: 4096}
}

func (p Policy) validate() error {
	switch {
	case p.MinOverlap < 0:
		return fmt.Errorf("the minimum overlap %s is negative: %w", p.MinOverlap, store.ErrInvalid)
	case p.MaxNodes < 1, p.MaxHosts < 1, p.MaxPerVia < 1, p.MaxResults < 1:
		return fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	}
	return nil
}

// Options configures one query.
type Options struct {
	// NodeOnly derives shared node only: no host lift, and no read of L1.
	NodeOnly bool
	// Policy is the derivation policy. The zero Policy means DefaultPolicy();
	// any other is validated as given.
	Policy Policy
}

// End is one reason an overlap span ends inside the window.
type End struct {
	// Leg is the index in CoResident.Legs of the leg that ends.
	Leg int
	// Source is the leg piece's own end source; lifecycle.EndUnknown when
	// Unattributed.
	Source lifecycle.EndSource
	// Unattributed says the leg goes on, but its placement became ambiguous
	// here: the seed on several nodes, or a node on several hosts.
	Unattributed bool
}

// Overlap is one span over which two pods shared a place. It is derived.
type Overlap struct {
	interval.Span
	// Before says the span starts at the window's From and every leg's piece
	// containing it began before From.
	Before bool
	// Pieces[i] indexes Legs[i].Pieces: the piece of leg i containing the span.
	Pieces []int
	// Ends lists, by leg, every leg that ends at End, with why. It is empty
	// exactly when End is the window's To.
	Ends []End
}

// CoResident is one path by which a peer shared a place with the seed. It is
// derived.
type CoResident struct {
	Mechanism Mechanism
	Peer      identity.Fingerprint
	// Nodes is [N] for a shared node, and [N1, N2], the seed's node then the
	// peer's, for a shared host.
	Nodes []identity.Fingerprint
	// Via is the node or the host shared.
	Via identity.Fingerprint
	// Legs is [seed→N, peer→N] for a shared node, and [seed→N1, N1→H, peer→N2,
	// N2→H] for a shared host, each as the interval package reads it.
	Legs []interval.Element
	// Overlaps are ascending, disjoint, never touching and never empty.
	Overlaps []Overlap
	// Duration is the total length of the overlaps.
	Duration    time.Duration
	Class       interval.Class
	Transitions int
	// Reason is ReasonNone in CoResidents, and why in Suppressed.
	Reason Reason
}

// Spans returns the spans of the entry's overlaps.
func (c CoResident) Spans() []interval.Span {
	out := make([]interval.Span, len(c.Overlaps))
	for i, o := range c.Overlaps {
		out[i] = o.Span
	}
	return out
}

// Unattributed is a placement this package refuses to guess.
type Unattributed struct {
	// Subject is a node of the seed, or the seed itself (SeveralNodes).
	Subject identity.Fingerprint
	Spans   []interval.Span
	Reason  UnattributedReason
	// Candidates are the hosts or nodes claimed over the spans, sorted.
	Candidates []identity.Fingerprint
}

// HostSpans is the seed's time on one node while that node is on one host: the
// seed is on Node alone, and Node's one live, unquarantined runs_on is to Host.
// It is derived.
type HostSpans struct {
	Node, Host identity.Fingerprint
	// Spans are ascending, disjoint, never touching and never empty, inside the
	// window.
	Spans []interval.Span
}

// ViaCount counts the entries of one (Via, Mechanism) dropped by MaxPerVia.
type ViaCount struct {
	Via       identity.Fingerprint
	Mechanism Mechanism
	Dropped   int
}

// Truncation reports what the result budget left out.
type Truncation struct {
	// Nodes and Hosts were not read: they were over MaxNodes or MaxHosts.
	Nodes, Hosts []identity.Fingerprint
	// PerVia counts the entries dropped by MaxPerVia.
	PerVia []ViaCount
	// Results counts the entries dropped by MaxResults, and SuppressedDropped
	// the suppressed entries.
	Results           int
	SuppressedDropped int
}

// Reads counts the store calls of one query: the interval calls made, and the
// Window, EntityWindow and Alive calls they made.
type Reads struct{ Calls, Windows, Alive, EntityWindows int }

// Result is a co-residency answer, read at one token. It is derived.
type Result struct {
	Token    uint64
	From, To time.Time
	Seed     identity.Fingerprint
	// Placement is the seed's scheduled_on edges to nodes, with the cascade.
	Placement []interval.Element
	// CoResidents are ranked: total duration descending, then shared node
	// before shared host, then peer, then the path's nodes and Via.
	CoResidents []CoResident
	Suppressed  []CoResident
	Truncated   Truncation
	// Unattributed is ordered by subject, then first span.
	Unattributed []Unattributed
	// SeedHosts is where the seed's attributed time on each of its nodes that was read
	// was attributed to a host, ordered by node, then host. An instant of the
	// seed's is in at most one entry: it is left out while the seed is on two or
	// more nodes, and while its node has no host, several, or only a quarantined
	// one, which Unattributed lists. It is nil with Options.NodeOnly, which reads
	// no host, and it is never cut by Policy.MaxHosts, which bounds only the
	// frontier read from the hosts.
	SeedHosts []HostSpans
	// Quarantined holds the edges read but not used because an endpoint is
	// quarantined, ordered by subject.
	Quarantined []interval.Element
	// PolicyVersion is this package's; IntervalPolicy is interval.PolicyVersion.
	PolicyVersion, IntervalPolicy int
	Reads                         Reads
}

// Union returns every entry.
func Union(cs []CoResident) []CoResident { return append([]CoResident(nil), cs...) }

// Intersection returns the entries with one overlap covering the whole window.
func Intersection(cs []CoResident) []CoResident {
	var out []CoResident
	for _, c := range cs {
		if c.Class == interval.ClassStable {
			out = append(out, c)
		}
	}
	return out
}

// At returns the entries with an overlap containing t. It trusts its caller to
// pass a t inside the window; AtChecked refuses one outside it.
func At(cs []CoResident, t time.Time) []CoResident {
	var out []CoResident
	for _, c := range cs {
		for _, o := range c.Overlaps {
			if o.Contains(t) {
				out = append(out, c)
				break
			}
		}
	}
	return out
}

// AtChecked is At over a result's co-residents, refusing a t outside
// [From, To) with an error wrapping store.ErrInvalid.
func AtChecked(res Result, t time.Time) ([]CoResident, error) {
	if t.Before(res.From) || !t.Before(res.To) {
		return nil, fmt.Errorf("coresidency: At: instant %s is outside the window [%s, %s): %w",
			formatTime(t), formatTime(res.From), formatTime(res.To), store.ErrInvalid)
	}
	return At(res.CoResidents, t), nil
}

// classOf is the class table of range reads over an entry's spans: none for
// none, flapping for two or more, and for one, by whether it starts at From and
// whether it ends at To. Transitions counts the boundaries strictly inside the
// window.
func classOf(ss []interval.Span, from, to time.Time) (interval.Class, int) {
	n := 0
	for _, s := range ss {
		if s.Start.After(from) {
			n++
		}
		if s.End.Before(to) {
			n++
		}
	}
	switch len(ss) {
	case 0:
		return interval.ClassNone, 0
	case 1:
	default:
		return interval.ClassFlapping, n
	}
	a, b := ss[0].Start.Equal(from), ss[0].End.Equal(to)
	switch {
	case a && b:
		return interval.ClassStable, n
	case b:
		return interval.ClassAppeared, n
	case a:
		return interval.ClassDisappeared, n
	}
	return interval.ClassTransient, n
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
