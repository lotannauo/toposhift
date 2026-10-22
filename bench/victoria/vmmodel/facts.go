package vmmodel

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrFactsDiffer marks two streams that do not describe the same existence.
var ErrFactsDiffer = errors.New("vmmodel: the two streams do not describe the same existence")

// ErrExtension marks a coalescer whose extension interval is longer than the window of a
// producer whose runs it extends: a run could then be re-asserted less often than the
// window a read gives it, and the coalesced stream would end it where the store's samples
// still hold it for longer than the check can bound.
var ErrExtension = errors.New("vmmodel: the coalescer's extension interval exceeds a producer's window")

// EverySampleSpec is the spec of the every-sample stream of a spec: the same workload,
// seed, start, batch size and retentions, with the ingest-side coalescer off (and the
// three settings that need it, ExtendEvery, ExtendTTLFraction and RunMaxAge, cleared),
// so that every refresh is a record of its own, which is what a metrics store sees.
//
// A spec with pins is read at the pinned prefixes only; the pins name a scenario that
// includes the coalescer, so they do not validate against this spec. The result has
// none, and the second value is the set of pinned entities: a record belongs to the
// projection if it touches one (the subject, or either end of an edge), the rule the
// reference engine's projection uses. It is nil for a spec without pins.
func EverySampleSpec(spec runner.Spec) (runner.Spec, map[identity.Fingerprint]struct{}) {
	var pinned map[identity.Fingerprint]struct{}
	if spec.Pins != nil {
		pinned = map[identity.Fingerprint]struct{}{}
		for _, pc := range spec.Pins.Classes {
			for _, list := range [][]identity.Fingerprint{pc.Records, pc.Extensions, pc.Median} {
				for _, fp := range list {
					pinned[fp] = struct{}{}
				}
			}
		}
	}
	spec.Pins, spec.FullStore = nil, false
	spec.Workload.CoalesceRuns = false
	spec.Workload.ExtendEvery = 0
	spec.Workload.ExtendTTLFraction = 0
	spec.Workload.RunMaxAge = 0
	return spec, pinned
}

// Touches reports whether a record is about an entity of the set: its subject, or either
// end of its edge.
func Touches(set map[identity.Fingerprint]struct{}, r engine.Record) bool {
	if _, ok := set[r.Subject.A]; ok {
		return true
	}
	if r.Subject.Kind == engine.SubjectEdge {
		_, ok := set[r.Subject.B]
		return ok
	}
	return false
}

// Projection returns a sink that gives each of the sinks only the records that touch an
// entity of the set, as the reference engine's projection does (see [Touches]), and every
// record if the set is nil. The converter and the worlds of a plan that is read at pinned
// prefixes are written through it.
func Projection(set map[identity.Fingerprint]struct{}, sinks ...runner.Sink) runner.Sink {
	return projection{set: set, sinks: sinks}
}

type projection struct {
	set   map[identity.Fingerprint]struct{}
	sinks []runner.Sink
}

func (p projection) Write(batch []engine.Record) error {
	if p.set != nil {
		kept := make([]engine.Record, 0, len(batch))
		for _, r := range batch {
			if Touches(p.set, r) {
				kept = append(kept, r)
			}
		}
		batch = kept
	}
	if len(batch) == 0 {
		return nil
	}
	for _, s := range p.sinks {
		if err := s.Write(batch); err != nil {
			return err
		}
	}
	return nil
}

func (p projection) Retain(h time.Time) error {
	for _, s := range p.sinks {
		if err := s.Retain(h); err != nil {
			return err
		}
	}
	return nil
}

// FactsReport is what [CheckSameFacts] found.
type FactsReport struct {
	// Subjects is the number of subjects compared. Identical of them have equal existence
	// intervals in the two streams, and EndsEarlier have the same intervals except that
	// some end is earlier in the coalesced stream, by less than the bound the extension
	// interval allows.
	Subjects, Identical, EndsEarlier int
	// MaxEarly is the largest amount any interval ends earlier in the coalesced stream.
	MaxEarly time.Duration
	// SmallestBand is the smallest [Registry.Band] of the subjects compared. The check is
	// per subject (an interval must end earlier by less than its own subject's band), so
	// MaxEarly above SmallestBand is not a failure: it is the amount of a subject with a
	// wide band, reported beside it.
	SmallestBand time.Duration
}

// SmallestBand returns the smallest band of the registry's subjects among those the map
// holds records of (zero if none is held by the registry).
func SmallestBand(reg *Registry, by map[engine.Subject][]engine.Record) time.Duration {
	var out time.Duration
	for s := range by {
		if b := reg.Band(s); b > 0 && (out == 0 || b < out) {
			out = b
		}
	}
	return out
}

// FactSink collects the records of a stream, per subject, without their payloads: what
// [CompareFacts] folds. It is a runner.Sink.
type FactSink struct {
	keep map[identity.Fingerprint]struct{}
	by   map[engine.Subject][]engine.Record
}

// NewFactSink returns a sink that keeps the records touching the entities of keep (see
// [Touches]), or every record if keep is nil.
func NewFactSink(keep map[identity.Fingerprint]struct{}) *FactSink {
	return &FactSink{keep: keep, by: map[engine.Subject][]engine.Record{}}
}

// Write implements runner.Sink.
func (s *FactSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		if s.keep != nil && !Touches(s.keep, r) {
			continue
		}
		r.Payload = nil
		s.by[r.Subject] = append(s.by[r.Subject], r)
	}
	return nil
}

// Retain implements runner.Sink and does nothing.
func (*FactSink) Retain(time.Time) error { return nil }

// Records returns the records kept, by subject.
func (s *FactSink) Records() map[engine.Subject][]engine.Record { return s.by }

// CheckSameFacts generates the stream of the spec twice, with the coalescer on and in its
// every-sample form ([EverySampleSpec]), and checks that the two describe the same
// existence for every subject the spec's projection holds (every subject, for a spec
// without pins). The comparison is by the reference's own rule, lifecycle.Fold, and it
// is exact except for the one thing the coalescer is documented to trade away: bounding
// how often a run is re-asserted (ExtendTTLFraction, ExtendEvery) can make an interval end
// earlier, never later, by less than the extension interval, and move nothing else.
//
// The plans' expected answers come from the coalesced stream and the store is written the
// every-sample one, so every comparison of the two rests on this. The check is made on
// the registry's windows: the bound of a subject is the fraction of the longest window
// among its refreshing holders (a producer in watch mode has no run to extend), or the
// fixed interval, and never a whole band of the subject ([Registry.Band]); a fixed
// interval longer than the window of a refreshing producer is refused first
// ([CheckExtension]). The spec's pins, if it has any, are the projection the store side
// is written through ([EverySampleSpec]), and its retentions are carried into the
// every-sample spec.
//
// The generator's ingest-side coalescer (bench/spike/workload/coalesce.go) takes no
// random numbers, so both streams share their events. CheckSameFacts fails if that is
// false: the subjects or their intervals then differ. (internal/lifecycle/coalesce.go is
// the specification's coalescing of assertions, not this one.)
func CheckSameFacts(ctx context.Context, spec runner.Spec, reg *Registry) (FactsReport, error) {
	every, pinned := EverySampleSpec(spec)
	if err := CheckExtension(spec.Workload, reg); err != nil {
		return FactsReport{}, err
	}
	a := NewFactSink(nil)
	if _, err := runner.Drive(ctx, spec, a); err != nil {
		return FactsReport{}, fmt.Errorf("vmmodel: the coalesced stream: %w", err)
	}
	b := NewFactSink(pinned)
	if _, err := runner.Drive(ctx, every, b); err != nil {
		return FactsReport{}, fmt.Errorf("vmmodel: the every-sample stream: %w", err)
	}
	rep, err := CompareFacts(a.Records(), b.Records(), func(s engine.Subject) time.Duration { return extensionBound(spec, reg, s) })
	rep.SmallestBand = SmallestBand(reg, b.Records())
	return rep, err
}

// CheckExtension refuses a coalescer whose fixed extension interval (ExtendEvery) is
// longer than the window of any producer whose runs it extends (the refreshing producers
// of the registry; one in watch mode has no run). The fraction of a run's own TTL
// (ExtendTTLFraction) cannot exceed a window, since it is at most one.
func CheckExtension(w workload.Config, reg *Registry) error {
	if !w.CoalesceRuns || w.ExtendEvery <= 0 {
		return nil
	}
	for _, h := range reg.Producers() {
		if !h.Watch() && w.ExtendEvery > h.Window {
			return fmt.Errorf("%w: ExtendEvery is %s and the window of producer %q is %s", ErrExtension, w.ExtendEvery, h.Producer, h.Window)
		}
	}
	return nil
}

// extensionBound is how much earlier than the every-sample stream an interval of the
// subject may end in the coalesced one: strictly less than this, and never as much as the
// subject's own band ([Registry.Band]).
func extensionBound(spec runner.Spec, reg *Registry, s engine.Subject) time.Duration {
	w := spec.Workload
	return ExtensionBound(w.CoalesceRuns, w.ExtendEvery, w.ExtendTTLFraction, reg, s)
}

// ExtensionBound is [CheckSameFacts]'s bound for a subject, from the coalescer's settings:
// the fixed interval or the fraction of the longest window of the subject's refreshing
// holders, capped at the subject's band. It is zero without a coalescer, so that every end
// must be equal.
func ExtensionBound(coalesce bool, every time.Duration, fraction float64, reg *Registry, s engine.Subject) time.Duration {
	var b time.Duration
	if coalesce {
		switch {
		case every > 0:
			b = every
		case fraction > 0:
			b = time.Duration(fraction * float64(reg.RefreshWindow(s)))
		}
	}
	if b > 0 {
		if band := reg.Band(s); band > 0 {
			b = min(b, band)
		}
	}
	return b
}

// CompareFacts folds the records of each subject of both streams and compares the
// existence. coalesced and everySample map a subject to its records; bound gives, for a
// subject, the amount (exclusive) an interval may end earlier in the coalesced stream.
// Subjects and intervals must correspond one to one, starts must be equal, and an end may
// be earlier by less than the bound and never later. It reports the first difference as
// an error that wraps [ErrFactsDiffer].
func CompareFacts(coalesced, everySample map[engine.Subject][]engine.Record, bound func(engine.Subject) time.Duration) (FactsReport, error) {
	var rep FactsReport
	subjects := make([]engine.Subject, 0, len(everySample))
	for s := range everySample {
		subjects = append(subjects, s)
	}
	for s := range coalesced {
		if _, ok := everySample[s]; !ok {
			return rep, fmt.Errorf("%w: %s is only in the coalesced stream", ErrFactsDiffer, describe(s))
		}
	}
	slices.SortFunc(subjects, compareSubjects)
	for _, s := range subjects {
		ca, ok := coalesced[s]
		if !ok {
			return rep, fmt.Errorf("%w: %s is only in the every-sample stream", ErrFactsDiffer, describe(s))
		}
		fa, err := foldRecords(ca)
		if err != nil {
			return rep, err
		}
		fb, err := foldRecords(everySample[s])
		if err != nil {
			return rep, err
		}
		early, err := compareIntervals(s, fa, fb, bound(s))
		if err != nil {
			return rep, err
		}
		rep.Subjects++
		if early == 0 {
			rep.Identical++
		} else {
			rep.EndsEarlier++
			rep.MaxEarly = max(rep.MaxEarly, early)
		}
	}
	return rep, nil
}

func compareSubjects(x, y engine.Subject) int {
	if x.Kind != y.Kind {
		return int(x.Kind) - int(y.Kind)
	}
	if c := engine.CompareFingerprints(x.A, y.A); c != 0 {
		return c
	}
	if c := engine.CompareFingerprints(x.B, y.B); c != 0 {
		return c
	}
	switch {
	case x.Relation < y.Relation:
		return -1
	case x.Relation > y.Relation:
		return 1
	}
	return 0
}

func foldRecords(recs []engine.Record) ([]lifecycle.Interval, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = lifecycle.Assertion{
			Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
			Kind: r.Kind, TTL: r.TTL, Through: r.Through,
		}
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, fmt.Errorf("vmmodel: folding: %w", err)
	}
	return tl.Existence(), nil
}

// compareIntervals compares the folded existence of one subject in the coalesced stream
// (a) and in the every-sample stream (b) and returns the largest amount an interval ends
// earlier in a.
func compareIntervals(s engine.Subject, a, b []lifecycle.Interval, bound time.Duration) (time.Duration, error) {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s: %s", ErrFactsDiffer, describe(s), fmt.Sprintf(format, args...))
	}
	if len(a) != len(b) {
		return 0, fail("%d intervals in the coalesced stream, %d in the every-sample stream", len(a), len(b))
	}
	var worst time.Duration
	for i := range a {
		x, y := a[i], b[i]
		if !x.Start.Equal(y.Start) {
			return 0, fail("interval %d starts at %s, and at %s", i, x.Start.Format(time.RFC3339Nano), y.Start.Format(time.RFC3339Nano))
		}
		if x.Open() != y.Open() {
			return 0, fail("interval %d is open in one stream and not in the other", i)
		}
		if x.Open() {
			continue
		}
		if x.End.After(y.End) {
			return 0, fail("interval %d ends at %s in the coalesced stream, after %s", i, x.End.Format(time.RFC3339Nano), y.End.Format(time.RFC3339Nano))
		}
		early := y.End.Sub(x.End)
		if early > 0 && early >= bound {
			return 0, fail("interval %d ends %s earlier in the coalesced stream, the bound is under %s", i, early, bound)
		}
		worst = max(worst, early)
	}
	return worst, nil
}
