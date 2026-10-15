package subgraph

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The existence read of an adapter's result, fixed on 2026-10-15 before
// anything was measured. It changes only by a new dated comment beside this one,
// never by editing the date or a number in place. A result of package
// coresidency, extract or blastradius has no element for the existence of the
// entities it names, so an adapter makes one call of package interval's
// Entities for the distinct entities its elements name, at the result's own
// token. That token is at or below the store's last sequence, so the call sees
// the world the result was read in, and a write made since is not seen. An
// existence read is one Alive and, for an entity that is not quarantined, one
// EntityWindow; so one call makes at most BudgetCalls interval call, and at
// most one Alive and one EntityWindow per distinct entity, which the cap
// [ReadPolicy].MaxEntities bounds.
//
// The counts, at each reference cell, of the distinct entities the call names:
//
//   - coresidency: a node with BudgetReferencePods (110) live pods on it, on one
//     host. The seed, its 109 peers, the node and its host: 112. A shared-node
//     query names the host only through a path through it, so the cell makes 111.
//     BudgetCoResidencyEntities is 128.
//   - blastradius: a host with one node and 110 pods with one container each. The
//     host, the node, 110 pods and 110 containers: 222. BudgetBlastEntities is 256.
//   - extract, anchored: coresidency's node on a host with that one node, and the
//     seed service with three callers and three callees: 7 services. The 110
//     pods on the node, one instance per pod (the containers are not named), the
//     node and the host: 7 + 110 + 110 + 2 = 229. BudgetAnchoredEntities is 1024.
//   - extract, exploratory: a service on 40 hosts, each node like the anchored
//     cell's. 40 nodes, 40 hosts, 40 * 110 pods and 40 * 110 instances, and the 7
//     services: 40 * 222 + 7 = 8887. BudgetExploratoryEntities is 9000.
//
// Only the first two are checked by a test; the two extraction budgets are fixed
// and not yet measured. A cell over budget is reported as a finding, never fixed
// by raising a constant.
const (
	BudgetDate                = "2026-10-15"
	BudgetCalls               = 1
	BudgetCoResidencyEntities = 128
	BudgetAnchoredEntities    = 1024
	BudgetExploratoryEntities = 9000
	BudgetBlastEntities       = 256
)

// defaultMaxReadEntities is the default cap on the distinct entities one read
// may name: a placeholder cap, above every budget, until it is measured. A
// result that names more is refused. It is not a budget: measuring a budget must
// not change which queries are refused, so the cap has a constant of its own.
const defaultMaxReadEntities = 16384

// ReadPolicy is the budget of an adapter's existence read.
type ReadPolicy struct {
	// MaxEntities is the most distinct entities the read may name; a result that
	// names more is refused, because a subgraph without the existence of its
	// entities would mislead. At least 1.
	MaxEntities int
}

// DefaultReadPolicy is version 1's default: a cap of 16384 entities, a
// placeholder until measured.
func DefaultReadPolicy() ReadPolicy { return ReadPolicy{MaxEntities: defaultMaxReadEntities} }

func (p ReadPolicy) resolve() (ReadPolicy, error) {
	if p == (ReadPolicy{}) {
		p = DefaultReadPolicy()
	}
	if p.MaxEntities < 1 {
		return p, fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	}
	return p, nil
}

// collector gathers what an adapter derives: the sources, each once, and the
// entities their keys name, which the existence read then asks for.
type collector struct {
	w     window
	from  time.Time
	to    time.Time
	srcs  map[Key]Source
	order []Key
	need  map[identity.Fingerprint]bool
}

func newCollector(from, to time.Time) (*collector, error) {
	w, err := newWindow(from, to)
	if err != nil {
		return nil, err
	}
	return &collector{
		w: w, from: from.UTC(), to: to.UTC(),
		srcs: map[Key]Source{}, need: map[identity.Fingerprint]bool{},
	}, nil
}

// add keeps a source once. A second source of the same key must be the same
// source: every read of a result is at one token, so two reads of a subject
// agree, and a difference is an error and never a pick.
func (c *collector) add(s Source) error {
	if old, ok := c.srcs[s.Key]; ok {
		if !sameSource(old, s) {
			return invalid("subgraph: two different reads of %s", s.Key)
		}
		return nil
	}
	c.srcs[s.Key] = s
	c.order = append(c.order, s.Key)
	c.name(s.Key.A, s.Key.B)
	return nil
}

// name records entities an element names, for the existence read.
func (c *collector) name(fps ...identity.Fingerprint) {
	for _, fp := range fps {
		if !fp.IsZero() {
			c.need[fp] = true
		}
	}
}

// element adds an interval element as an asserted source and returns its key.
func (c *collector) element(el interval.Element) (Key, error) {
	src, err := sourceOf(el)
	if err != nil {
		return Key{}, err
	}
	return src.Key, c.add(src)
}

// span is a span of the window as a section carries it.
func (c *collector) span(s interval.Span, before bool) (Span, error) {
	start, okS := c.w.rel(s.Start)
	end, okE := c.w.rel(s.End)
	if !okS || !okE {
		return Span{}, invalid("subgraph: the span [%s, %s) is outside the window", formatTime(s.Start), formatTime(s.End))
	}
	return Span{Start: start, End: end, Before: before, EndReason: c.endReason(end)}, nil
}

func (c *collector) spans(ss []interval.Span) ([]Span, error) {
	out := make([]Span, len(ss))
	for i, s := range ss {
		var err error
		if out[i], err = c.span(s, false); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// endReason is the end reason of a derived span ending at end: none at the
// window's To, and otherwise the end of an intersection or an ambiguity.
func (c *collector) endReason(end uint64) EndReason {
	if end == c.w.width() {
		return EndReasonUnknown
	}
	return EndReasonReachEnded
}

// piece is a derived piece over span s.
func (c *collector) piece(s interval.Span, before bool, ends []LegEnd) SourcePiece {
	slices.SortFunc(ends, func(a, b LegEnd) int {
		if a.Path != b.Path {
			return a.Path - b.Path
		}
		return a.Leg - b.Leg
	})
	reason := EndReasonReachEnded
	if !s.End.Before(c.to) {
		reason = EndReasonUnknown
	}
	return SourcePiece{Start: s.Start, End: s.End, Before: before, EndReason: reason, Ends: ends}
}

// read makes the existence read of the entities named, at the result's token,
// adds the elements it returns, and builds the subgraph.
func (c *collector) build(ctx context.Context, st store.Store, token uint64, sec Sections, p ReadPolicy) (Subgraph, error) {
	p, err := p.resolve()
	if err != nil {
		return Subgraph{}, err
	}
	fps := make([]identity.Fingerprint, 0, len(c.need))
	for fp := range c.need {
		fps = append(fps, fp)
	}
	slices.SortFunc(fps, store.CompareFingerprints)
	if len(fps) > p.MaxEntities {
		return Subgraph{}, fmt.Errorf("the result names %d distinct entities, over the cap of %d: %w", len(fps), p.MaxEntities, store.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Subgraph{}, err
	}
	got, els, err := interval.NewReader(st, interval.Options{}).Entities(ctx, fps, interval.Window{From: c.from, To: c.to, AsOf: token})
	if err != nil {
		return Subgraph{}, err
	}
	if got != token {
		return Subgraph{}, fmt.Errorf("the existence read used the token %d, not the result's %d", got, token)
	}
	for _, el := range els {
		if _, err := c.element(el); err != nil {
			return Subgraph{}, err
		}
	}
	in := Input{Token: token, From: c.from, To: c.to, Read: fps, Sections: sec}
	for _, k := range c.order {
		in.Sources = append(in.Sources, c.srcs[k])
	}
	return Build(in)
}

// legReason is why a leg of a derived span ends there: its own end source, or
// unattributed when the leg goes on and its placement or mapping became
// ambiguous. A leg that ends inside the window with no known source is refused,
// not relabeled.
func legReason(src lifecycle.EndSource, unattributed bool) (EndReason, error) {
	if unattributed {
		if src != lifecycle.EndUnknown {
			return 0, invalid("subgraph: a leg that goes on has the end source %s", src)
		}
		return EndReasonUnattributed, nil
	}
	r, err := EndReasonOf(src)
	if err != nil {
		return 0, err
	}
	if !r.lifecycleOnly() {
		return 0, invalid("subgraph: a leg ends inside the window with the end source %s", src)
	}
	return r, nil
}

// sameSource reports whether two reads of one subject agree.
func sameSource(a, b Source) bool {
	if a.Key != b.Key || a.Class != b.Class || a.Transitions != b.Transitions || len(a.Pieces) != len(b.Pieces) ||
		!sameQuarantine(a.Quarantine, b.Quarantine) || !sameQuarantine(a.Uncascaded, b.Uncascaded) ||
		len(a.Paths) != len(b.Paths) {
		return false
	}
	for i, p := range a.Pieces {
		q := b.Pieces[i]
		if !p.Start.Equal(q.Start) || !p.End.Equal(q.End) || p.Before != q.Before || p.EndReason != q.EndReason ||
			!slices.Equal(p.Producers, q.Producers) || !slices.Equal(p.Ends, q.Ends) {
			return false
		}
	}
	for i, p := range a.Paths {
		if !slices.Equal(p, b.Paths[i]) {
			return false
		}
	}
	return true
}

func sameQuarantine(a, b *store.QuarantineError) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Entity == b.Entity && a.Layer == b.Layer && a.Error() == b.Error()
}
