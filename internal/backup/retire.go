package backup

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// ErrNotRetirable is wrapped by the error Retire returns for a class that rule R
// refuses to retire: it holds an Observe with no TTL, or with a TTL above the
// margin less the clock bound, whose reference could be alive at the horizon with
// no record of it left in the backup. The refusal is for good: the file that
// caused it is never retired, so the class is not retired again.
var ErrNotRetirable = errors.New("backup: a class cannot be retired")

// ErrRetentionAhead is returned by Retire when the last retention logged was taken
// at a Seq the cutter has not been given yet. Records committed before that
// retention could still arrive with event times before its horizon, and rule R
// would not have checked them, so nothing is retired until they are added.
var ErrRetentionAhead = errors.New("backup: the last retention is ahead of the records added")

// Retirement says what one Retire did.
type Retirement struct {
	// Manifest is the manifest Retire wrote.
	Manifest Manifest
	// Files and Records count, per class, what it retired.
	Files   [NumClasses]int
	Records [NumClasses]int64
}

// classFiles returns the indexes in m.Files of each class's files, in Seq order.
func classFiles(m Manifest) [NumClasses][]int {
	var out [NumClasses][]int
	for i, f := range m.Files {
		if c, ok := parseClass(f.Class); ok {
			out[c] = append(out[c], i)
		}
	}
	return out
}

// cutOf returns the index of the cut that holds a file, or -1.
func cutOf(m Manifest, f File) int {
	for i, cut := range m.Cuts {
		if f.FirstSeq >= cut.FirstSeq && f.LastSeq <= cut.LastSeq {
			return i
		}
	}
	return -1
}

// atOrBefore reports whether x + margin <= h, without overflow.
func atOrBefore(x int64, margin time.Duration, h int64) bool {
	if h < math.MinInt64+int64(margin) {
		return false
	}
	return x <= h-int64(margin)
}

// decision is what rule R decides for one class.
type decision struct {
	n       int    // how many of the class's files, in Seq order, it retires
	refusal string // why the class is not retirable, or ""
	horizon int64  // the horizon it decided at
}

// decide applies rule R to every class of m.
//
// For a class l1, l2 or l3 whose layer is not kept, h is the layer's horizon in
// the last logged retention, and nothing is retired while it is zero. M is the
// margin.
//
//  1. Refusal: if a file of the class holds an Observe with no TTL, or one with a
//     TTL above M less MaxClockAhead, nothing of the class is retired, now or later.
//  2. Candidates: a file whose largest max(EventTime, Through) (its reach) plus M,
//     and whose last commit's clock plus M, are both at or before h.
//  3. The longest prefix P of the class's files, in Seq order, that are
//     candidates, shortened until (a) it ends at a cut boundary for the class, and
//     (b') the largest reach in P is at or before the smallest
//     MinShadowableObserveEventNS of the class's files it leaves, which, because
//     Retire cuts first, include every record the cutter holds.
//
// Why the answers stay equal. Take one class, the records P retired (by every
// retirement so far) and the records K kept, and the horizon h the live store
// holds. For a record r, reach_r = max(EventTime_r, Through_r) and
// span_r = reach_r - EventTime_r. The rule rests on six assumptions:
//
//   - A1 (a precondition): every record of one reference, a subject and a
//     producer, is in one class. The store validates an entity's layer against
//     the catalog, and hosts takes every host entity record. An edge's layer is
//     the caller's precondition (the catalog gives it by the relation); a restore
//     checks it only among the records the backup still holds, so it cannot see
//     an edge split across layers whose other half was retired.
//   - A2 (the cutter checks it, by the refusal): every Observe in the class has
//     0 < TTL < M.
//   - A3 (the cutter checks it, by the candidates): every record in P has
//     reach <= h - M.
//   - A4 (the cutter checks it, retiring prefixes): every Seq in P is below every
//     Seq in K.
//   - A5 (the cutter checks it, by condition (b')): every Observe D in K with
//     span_D > M - TTL_D has EventTime_D >= max reach(P).
//   - A6: every record the cutter receives after the decision has
//     EventTime >= h. The cutter checks its half: it decides only when it holds
//     every record through the Seq of the retention it decides on (Retire refuses
//     otherwise, with ErrRetentionAhead), so a record it receives later was
//     committed after the store's Retain. The rest is a precondition: the store's
//     Write refuses a record before h, and the h and the Seq logged are the
//     store's own, read with LayerHorizon and LastSeq after Retain returns.
//
// Lemma: for every reference and every instant x >= h, folding P and K, and
// folding K alone, give the same liveness and the same deciding record. The fold
// on [h, x] depends only on the latest assertion at or before h, in (EventTime,
// Seq) order, and on the assertions after it, which by A3 and A6 are all in K.
// If the latest assertion of P and K is in K, it is the latest of K. If it is a
// record A in P, it is not live at h: a Delete never is, and an Observe's
// deadline is at most h - M + TTL < h (A2). Every record of K at or before h then
// precedes A, so with K's higher Seqs (A4) its EventTime is below
// EventTime_A <= max reach(P) <= h - M. If the latest record D of K were live at
// some x >= h, D would be an Observe with reach_D + TTL_D > h, so
// span_D > M - TTL_D with EventTime_D < max reach(P), which A5 rules out. So
// neither is live until a later assertion, which both hold, and the Retain
// contract makes every read at or after h equal once both have retained to h.
// Earlier retirements are covered too: a record written after one has EventTime
// at or after the horizon it was decided at (A6), which is past every reach it
// retired. The wall condition and MaxClockAhead are not needed by the lemma; they
// are kept as a defence.
func (c *Cutter) decide(m Manifest) [NumClasses]decision {
	var out [NumClasses]decision
	byClass := classFiles(m)
	last, ok := m.lastRetention()
	for cl := range Class(NumClasses) {
		if !cl.retirable() && (cl != ClassHosts || !c.h.retireHosts) {
			continue
		}
		layer := cl.Layer()
		idx := byClass[cl]
		d := &out[cl]
		d.refusal = m.NotRetirable[cl]
		if d.refusal == "" && !c.h.noRefusal[cl] {
			d.refusal = c.refusal(m, idx)
		}
		if d.refusal != "" || !ok || m.Store.Keep[layer-catalog.L0] {
			continue
		}
		hz := last.Horizons[layer-catalog.L0]
		if hz.IsZero() {
			continue
		}
		h, margin := hz.TimeNS, m.Margin
		if c.h.zeroMargin[cl] {
			margin = 0
		}
		d.horizon = h
		n := 0
		for n < len(idx) {
			f := m.Files[idx[n]]
			if !atOrBefore(f.MaxReachNS, margin, h) || !atOrBefore(f.WallLastNS, margin, h) {
				break
			}
			n++
		}
		for ; n > 0; n-- {
			boundary := n == len(idx) || cutOf(m, m.Files[idx[n-1]]) != cutOf(m, m.Files[idx[n]])
			if !boundary {
				continue
			}
			if c.h.noMinEvent[cl] {
				break
			}
			reach, minShadowable := int64(math.MinInt64), int64(math.MaxInt64)
			for _, i := range idx[:n] {
				reach = max(reach, m.Files[i].MaxReachNS)
			}
			for _, i := range idx[n:] {
				minShadowable = min(minShadowable, m.Files[i].MinShadowableObserveEventNS)
			}
			if reach <= minShadowable {
				break
			}
		}
		d.n = n
	}
	return out
}

// refusal is the first reason a class's files give for refusing it, or "".
func (c *Cutter) refusal(m Manifest, idx []int) string {
	bound := m.Margin - m.MaxClockAhead
	for _, i := range idx {
		f := m.Files[i]
		switch {
		case f.TTL0Observes > 0:
			return fmt.Sprintf("%s holds Observes with no TTL (%d)", f.Name, f.TTL0Observes)
		case f.MaxObserveTTL > bound:
			return fmt.Sprintf("%s holds an Observe with a TTL of %s, above the margin %s less the clock bound %s",
				f.Name, f.MaxObserveTTL, m.Margin, m.MaxClockAhead)
		}
	}
	return ""
}

// Retire retires what rule R allows. It first cuts whenever a file holds records
// not yet cut, so that the rule sees every Observe the cutter has. It then writes
// the next generation's manifest without the retired files, and only then deletes
// them, and deletes the manifests older than the one before it. A failure at any
// step leaves the latest manifest whole: a failed write deletes nothing, and a
// file whose deletion failed is no longer named, and is deleted by the next
// Retire.
//
// Retire retires nothing, and returns an error wrapping ErrRetentionAhead, while
// the last retention logged was taken at a Seq above the last one added.
//
// A class that rule R refuses is reported by an error wrapping ErrNotRetirable,
// at every Retire, and its first reason is recorded in the manifest; the other
// classes are still retired, and the Retirement is returned with the error.
func (c *Cutter) Retire(ctx context.Context) (Retirement, error) {
	if c.failed != nil {
		return Retirement{}, c.failed
	}
	if !c.h.noCutBeforeRetire && (c.hasOpen() || len(c.pending) > 0) {
		if _, err := c.Cut(ctx); err != nil {
			return Retirement{}, err
		}
	}
	// The rule decides only when the cutter holds every record committed before
	// the last retention (A6).
	if n := len(c.retentions); n > 0 && c.retentions[n-1].Seq > c.last {
		return Retirement{}, fmt.Errorf("%w: it was logged at seq %d, and the records added end at seq %d",
			ErrRetentionAhead, c.retentions[n-1].Seq, c.last)
	}
	// The rule decides on the retention log as it is now, logged since the last
	// manifest or not.
	next := c.next(c.m.Seq)
	decided := c.decide(next)
	byClass := classFiles(next)
	var ret Retirement
	var refused []error
	drop := map[int]bool{}
	var retired []string
	for cl, d := range decided {
		if d.refusal != "" {
			if next.NotRetirable[cl] == "" {
				next.NotRetirable[cl] = d.refusal
			}
			refused = append(refused, fmt.Errorf("%w: class %s: %s", ErrNotRetirable, Class(cl), next.NotRetirable[cl]))
			continue
		}
		if d.n == 0 {
			continue
		}
		idx := byClass[cl][:d.n]
		lastFile := next.Files[idx[len(idx)-1]]
		r := &next.Retired[cl]
		r.ThroughSeq = lastFile.LastSeq
		r.ThroughCutSeq = next.Cuts[cutOf(next, lastFile)].LastSeq
		r.HorizonNS = d.horizon
		for _, i := range idx {
			drop[i] = true
			r.Records += next.Files[i].Records
			ret.Records[cl] += next.Files[i].Records
			retired = append(retired, next.Files[i].Name)
		}
		ret.Files[cl] = d.n
	}
	kept := next.Files[:0:0]
	for i, f := range next.Files {
		if !drop[i] {
			kept = append(kept, f)
		}
	}
	next.Files = kept
	cuts := next.Cuts[:0:0]
	for _, cut := range next.Cuts {
		if !exemptCut(next, cut) {
			cuts = append(cuts, cut)
		}
	}
	next.Cuts = cuts
	chain(&next)
	if err := c.writeManifest(ctx, next); err != nil {
		return Retirement{}, err
	}
	ret.Manifest = c.m
	c.garbage = append(c.garbage, retired...)
	var errs []error
	left := c.garbage[:0:0]
	for _, name := range c.garbage {
		if err := c.dst.Delete(ctx, name); err != nil {
			errs = append(errs, fmt.Errorf("backup: deleting the retired %s: %w", name, err))
			left = append(left, name)
		}
	}
	c.garbage = left
	if err := c.pruneManifests(ctx); err != nil {
		errs = append(errs, err)
	}
	return ret, errors.Join(append(refused, errs...)...)
}

// exemptCut reports whether every class a cut holds records of is retired
// through it, so that the cut holds no retained file.
func exemptCut(m Manifest, cut Cut) bool {
	for cl, n := range cut.Records {
		if n > 0 && cut.LastSeq > m.Retired[cl].ThroughCutSeq {
			return false
		}
	}
	return true
}

// pruneManifests deletes the manifests older than the one before the latest.
func (c *Cutter) pruneManifests(ctx context.Context) error {
	gens, err := manifestGenerations(ctx, c.dst, c.prefix)
	if err != nil {
		return err
	}
	var errs []error
	for _, g := range gens {
		if g < c.prevGen {
			if err := c.dst.Delete(ctx, ManifestName(c.prefix, g)); err != nil {
				errs = append(errs, fmt.Errorf("backup: deleting an old manifest: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

func (c *Cutter) hasOpen() bool {
	for _, of := range c.open {
		if of != nil && of.meta.Records > 0 {
			return true
		}
	}
	return false
}
