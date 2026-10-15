package blastradius

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the defaults of
// DefaultPolicy, the single-claim table and the rules the answer is derived by.
// Any change to one of them raises it, and every result carries it.
const PolicyVersion = 1

// The walk's read budget, fixed on 2026-10-14 before anything was measured. It
// changes only by a new dated comment beside this one, never by editing the
// date or a number in place. An existence read is one Alive and, for an entity
// that is not quarantined, one EntityWindow; a call is one interval call.
//
// The shape is exact: Windows is, per reverse call, the number of reverse edge
// layers of each distinct entity of the frontier and, per claims call, the
// number of forward edge layers of each distinct claimant; Alive is, per call,
// the number of distinct endpoints of the edges with an interval of their own
// in the window; EntityWindows is Alive less the quarantined among them. In one
// reverse call a node adds at most 1 + D existence reads, where D is the number
// of distinct pods with a scheduled_on interval to it in the window, never one
// per record or per pod of its history before the window.
//
// Each existence read stays within the read gate's per-read work budget, and
// one reverse call costs at most BudgetWorkFactor times the sum of those
// budgets over its existence reads. At the reference cell, a host with one node
// and BudgetReferencePods live pods on it with one container each, a one-hour
// window ending at the newest instant and a 30-day store, a walk from the host
// makes at most BudgetReferenceExistence existence reads and
// BudgetReferenceWindows Window reads. A cell over budget is reported as a
// finding, never fixed by raising a constant.
const (
	BudgetDate               = "2026-10-14"
	BudgetWorkFactor         = 1.5
	BudgetReferencePods      = 110
	BudgetReferenceExistence = 512
	BudgetReferenceWindows   = 256
)

// The read budget of Around, fixed on 2026-10-15 before anything was measured.
// It changes only by a new dated comment beside this one, never by editing the
// date or a number in place. Its shape is the walk's, above: Around's reads are
// its upward walk's plus every expanded substrate's walk, each counted as
// before; the upward walk's calls are one per upward depth with a frontier
// entity that has a forward edge layer, and it needs no claims call, because a
// forward read of an entity returns its claims.
//
// At the reference cell, a host with one node, BudgetReferencePods live pods on
// it with one container each, a one-hour window ending at the newest instant, a
// 30-day store, and the host in one rack and one zone, an Around from one of
// the pods makes at most BudgetAroundExistence existence reads and
// BudgetAroundWindows Window reads when racks and zones are listed and not
// expanded (the default), and at most BudgetAroundExpandedExistence and
// BudgetAroundExpandedWindows when Options.ExpandFabric expands them. A cell
// over budget is reported as a finding, never fixed by raising a constant.
// Walks from a rack or a zone over more than one host are not budgeted.
const (
	BudgetAroundDate              = "2026-10-15"
	BudgetAroundExistence         = 1024
	BudgetAroundWindows           = 512
	BudgetAroundExpandedExistence = 2048
	BudgetAroundExpandedWindows   = 1024
)

// Policy is the derivation policy of a walk.
type Policy struct {
	// MaxDepth is the most hops a path may have. Members reached at MaxDepth
	// are not expanded. MaxFrontier caps the entities expanded from each
	// depth, and MaxMembers the members listed. Each is at least 1.
	MaxDepth, MaxFrontier, MaxMembers int
}

// DefaultPolicy is version 1's defaults: a depth of 6, 1024 entities expanded
// per depth and 4096 members listed, which are placeholders until measured.
func DefaultPolicy() Policy { return Policy{MaxDepth: 6, MaxFrontier: 1024, MaxMembers: 4096} }

func (p Policy) validate() error {
	if p.MaxDepth < 1 || p.MaxFrontier < 1 || p.MaxMembers < 1 {
		return fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	}
	return nil
}

// Options configures one walk.
type Options struct {
	// Policy is the derivation policy. The zero Policy means DefaultPolicy();
	// any other is validated as given.
	Policy Policy
	// ExpandFabric makes Around walk from every substrate of a pod, not only
	// from its nodes and hosts: racks, zones and any other entity above them
	// are listed and not walked unless it is set. It is off by default, and
	// Impact ignores it.
	ExpandFabric bool
}

// singleClaim is the single-claim table of version 1: the relations a from
// entity counts as on its target through only while it claims no other target
// of that relation and type. They are the host and fabric placement relations.
// scheduled_on, part_of and depends_on are not in it.
func singleClaim(r catalog.RelationType) bool {
	return r == catalog.RunsOn || r == catalog.LocatedIn
}

// Arrival is impact reaching a member through one edge from a parent. It is
// derived.
type Arrival struct {
	// Parent is the entity the edge leads to: the seed, or a member one hop
	// nearer the seed.
	Parent      identity.Fingerprint
	Relation    catalog.RelationType
	Propagation catalog.Propagation
	// Leg is the edge Member→Parent, as package interval reads it, with the
	// cascade: its pieces carry their end sources and producers.
	Leg interval.Element
	// Spans is the intersection of the parent's reach by paths shorter than
	// MaxDepth (the whole window for the seed, or in an Around the seed's time
	// on the substrate) with the leg's effective spans:
	// its own, less the instants the member claims another target of a
	// single-claim relation. Ascending, disjoint, never touching, never empty.
	// The parent's reach counts only as far as it was expanded: the new reach of
	// a parent that MaxFrontier left unexpanded at a depth (Truncation.Frontier)
	// carries nothing onward, so its instants are in no arrival from it.
	Spans []interval.Span
}

// Member is one entity a failing seed reaches. It is derived.
type Member struct {
	Entity identity.Fingerprint
	// Depth is the fewest hops by which any instant of Reach is reached.
	Depth int
	// Reach is the instants of the window at which a path of at most MaxDepth
	// hops, every leg live at once, leads to the member from the seed. It is
	// the union of the arrivals' spans: ascending, disjoint, never touching and
	// never empty.
	Reach       []interval.Span
	Duration    time.Duration
	Class       interval.Class
	Transitions int
	// Arrivals is ordered by parent, then relation.
	Arrivals []Arrival
}

// Unattributed is a placement the walk refuses to guess: over Spans, Subject
// claimed two or more targets of type Target through Relation at once, while
// one of them was reached, so it is placed on none of them.
type Unattributed struct {
	Subject  identity.Fingerprint
	Relation catalog.RelationType
	Target   catalog.EntityType
	Spans    []interval.Span
	// Candidates are the targets claimed over the spans, sorted.
	Candidates []identity.Fingerprint
}

// FrontierCut names the entities reached at Depth that MaxFrontier left
// unexpanded.
type FrontierCut struct {
	Depth   int
	Dropped []identity.Fingerprint // sorted
}

// Truncation reports what the policy left out.
type Truncation struct {
	// Depth is the members reached at MaxDepth, with edges that could lead on,
	// that were not expanded; sorted.
	Depth []identity.Fingerprint
	// Frontier lists, by depth, the entities MaxFrontier left unexpanded.
	Frontier []FrontierCut
	// Members counts the members MaxMembers left out of the list.
	Members int
}

// Reads counts the store calls of one walk: the interval calls made, and the
// Window, EntityWindow and Alive calls they made.
type Reads struct{ Calls, Windows, Alive, EntityWindows int }

// Result is a blast-radius answer, read at one token. It is derived.
type Result struct {
	Token    uint64
	From, To time.Time
	Seed     identity.Fingerprint
	// Members are ranked: depth ascending, then reach duration descending, then
	// fingerprint.
	Members []Member
	// Unattributed is ordered by subject, relation, target, then first span.
	Unattributed []Unattributed
	// Quarantined holds, once each and ordered by subject, the edges read but
	// never followed because an endpoint is quarantined.
	Quarantined []interval.Element
	Truncated   Truncation
	// PolicyVersion is this package's; IntervalPolicy is interval.PolicyVersion.
	PolicyVersion, IntervalPolicy int
	Reads                         Reads
}

// Union returns every member.
func Union(ms []Member) []Member { return append([]Member(nil), ms...) }

// Intersection returns the members whose reach is the whole window.
func Intersection(ms []Member) []Member {
	var out []Member
	for _, m := range ms {
		if m.Class == interval.ClassStable {
			out = append(out, m)
		}
	}
	return out
}

// At returns the members whose reach contains t. It trusts its caller to pass
// a t inside the window; AtChecked refuses one outside it.
func At(ms []Member, t time.Time) []Member {
	var out []Member
	for _, m := range ms {
		for _, s := range m.Reach {
			if s.Contains(t) {
				out = append(out, m)
				break
			}
		}
	}
	return out
}

// AtChecked is At over a result's members, refusing a t outside [From, To)
// with an error wrapping store.ErrInvalid.
func AtChecked(res Result, t time.Time) ([]Member, error) {
	if t.Before(res.From) || !t.Before(res.To) {
		return nil, fmt.Errorf("blastradius: At: instant %s is outside the window [%s, %s): %w",
			formatTime(t), formatTime(res.From), formatTime(res.To), store.ErrInvalid)
	}
	return At(res.Members, t), nil
}

// classOf is the class table of range reads over a member's reach: none for
// none, flapping for two spans or more, and for one, by whether it starts at
// From and whether it ends at To. Transitions counts the boundaries strictly
// inside the window.
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
