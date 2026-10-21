package vmmodel

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrFactsDiffer marks two streams that do not describe the same existence.
var ErrFactsDiffer = errors.New("vmmodel: the two streams do not describe the same existence")

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

// FactsReport is what [CheckSameFacts] found.
type FactsReport struct {
	// Subjects is the number of subjects compared. Identical of them have equal existence
	// intervals in the two streams, and EndsEarlier have the same intervals except that
	// some end is earlier in the coalesced stream, by less than the bound the extension
	// interval allows.
	Subjects, Identical, EndsEarlier int
	// MaxEarly is the largest amount any interval ends earlier in the coalesced stream.
	MaxEarly time.Duration
}

// factSink collects the records of a stream, per subject, without their payloads.
type factSink struct {
	keep map[identity.Fingerprint]struct{}
	by   map[engine.Subject][]engine.Record
}

func (s *factSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		if s.keep != nil && !Touches(s.keep, r) {
			continue
		}
		r.Payload = nil
		s.by[r.Subject] = append(s.by[r.Subject], r)
	}
	return nil
}
func (*factSink) Retain(time.Time) error { return nil }

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
// among its holders.
//
// The generator's ingest-side coalescer (bench/spike/workload/coalesce.go) takes no
// random numbers, so both streams share their events. CheckSameFacts fails if that is
// false: the subjects or their intervals then differ. (internal/lifecycle/coalesce.go is
// the specification's coalescing of assertions, not this one.)
func CheckSameFacts(ctx context.Context, spec runner.Spec, reg *Registry) (FactsReport, error) {
	every, pinned := EverySampleSpec(spec)
	a := &factSink{by: map[engine.Subject][]engine.Record{}}
	if _, err := runner.Drive(ctx, spec, a); err != nil {
		return FactsReport{}, fmt.Errorf("vmmodel: the coalesced stream: %w", err)
	}
	b := &factSink{keep: pinned, by: map[engine.Subject][]engine.Record{}}
	if _, err := runner.Drive(ctx, every, b); err != nil {
		return FactsReport{}, fmt.Errorf("vmmodel: the every-sample stream: %w", err)
	}
	return CompareFacts(a.by, b.by, func(s engine.Subject) time.Duration { return extensionBound(spec, reg, s) })
}

// extensionBound is how much earlier than the every-sample stream an interval of the
// subject may end in the coalesced one: strictly less than this.
func extensionBound(spec runner.Spec, reg *Registry, s engine.Subject) time.Duration {
	w := spec.Workload
	if w.CoalesceRuns {
		if w.ExtendEvery > 0 {
			return w.ExtendEvery
		}
		if w.ExtendTTLFraction > 0 {
			return time.Duration(w.ExtendTTLFraction * float64(reg.SubjectWindow(s)))
		}
	}
	return 0
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
