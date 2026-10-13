package resume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Lookup finds a producer's last stored assertion of a subject. The pebblestore's
// and the memstore's lookups ([store.LookupRef] names them) are Lookups.
type Lookup interface {
	Last(ctx context.Context, ref store.LookupRef) (store.LastAssertion, error)
	Close() error
}

// Mark is a record a translator backdated: Records[Index] of its [Result].
type Mark struct {
	// Index is the record's position in [Result.Records].
	Index int
	// Handle is the translator's own, given back to [Resumer.Resumed]; nil for a
	// caller with no translator.
	Handle any
	// ObservedBasis is the basis of the record's Through, the time it was observed
	// at, which the record takes when it is re-timed to that time: observed for a
	// Kubernetes objects record, the record's own instant's for a cluster receiver
	// record. [store.BasisUnknown] is taken as [store.BasisObserved].
	ObservedBasis store.EventTimeBasis
}

// Result is the records of one producer for one request, in the order the
// translator emitted them, and the marks among them.
type Result struct {
	Records []store.Record
	Marks   []Mark
	// Dropped is set by [Apply]: the positions, in Records as given and in
	// ascending order, of the records it dropped. Apply removes them from Records,
	// and removes their marks and moves the others with their records.
	Dropped []int
}

// Resumer is told the final event time and basis of each marked record that is
// kept. It raises the translator's own state for the record's subject to them,
// and never lowers it.
type Resumer interface {
	Resumed(handle any, t time.Time, basis store.EventTimeBasis)
}

// Stats counts what [Apply] decided. Unknown is the sum of the four reasons after
// it.
type Stats struct {
	New, Raised, Changed, ChangedGap, Dropped                                     int64
	Unknown, UnknownExhausted, UnknownBaseline, UnknownBelowHorizon, UnknownError int64
	MirrorRaised                                                                  int64
	// ExhaustedExample, GapExample and ErrorExample are the first reference seen
	// for each warning, and FirstError the first error of a lookup that fell back,
	// for the caller to log: Apply logs nothing.
	ExhaustedExample, GapExample, ErrorExample store.LookupRef
	FirstError                                 error
}

// Add adds o's counts to s, and takes o's examples and error where s has none.
func (s *Stats) Add(o Stats) {
	s.New += o.New
	s.Raised += o.Raised
	s.Changed += o.Changed
	s.ChangedGap += o.ChangedGap
	s.Dropped += o.Dropped
	s.Unknown += o.Unknown
	s.UnknownExhausted += o.UnknownExhausted
	s.UnknownBaseline += o.UnknownBaseline
	s.UnknownBelowHorizon += o.UnknownBelowHorizon
	s.UnknownError += o.UnknownError
	s.MirrorRaised += o.MirrorRaised
	if isZeroRef(s.ExhaustedExample) {
		s.ExhaustedExample = o.ExhaustedExample
	}
	if isZeroRef(s.GapExample) {
		s.GapExample = o.GapExample
	}
	if isZeroRef(s.ErrorExample) {
		s.ErrorExample = o.ErrorExample
	}
	if s.FirstError == nil {
		s.FirstError = o.FirstError
	}
}

func isZeroRef(r store.LookupRef) bool { return r == store.LookupRef{} }

// Counters are the counts by name, as a caller reports them: "new", "raised",
// "changed", "changed_gap", "dropped", "unknown", "unknown.exhausted",
// "unknown.baseline_only", "unknown.below_horizon", "unknown.error" and
// "mirror_raised".
func (s Stats) Counters() map[string]int64 {
	return map[string]int64{
		"new": s.New, "raised": s.Raised, "changed": s.Changed, "changed_gap": s.ChangedGap,
		"dropped": s.Dropped, "unknown": s.Unknown, "unknown.exhausted": s.UnknownExhausted,
		"unknown.baseline_only": s.UnknownBaseline, "unknown.below_horizon": s.UnknownBelowHorizon,
		"unknown.error": s.UnknownError, "mirror_raised": s.MirrorRaised,
	}
}

// IsAbort says whether a lookup's error aborts [Apply]: the store or the lookup is
// closed, or the context is done.
func IsAbort(err error) bool {
	return errors.Is(err, store.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// answer is one mark's lookup.
type answer struct {
	ref   store.LookupRef
	start time.Time
	last  store.LastAssertion
	err   error // an error that falls back
	skip  bool  // not a backdated Observe: left as it is, with no lookup
}

// Apply resumes the marked records of res, as the package documentation says, in
// the order of their marks. horizon is each layer's retention horizon (zero for
// none), and described the translator's Comparable, applied to the record's
// payload and to the stored one (nil compares them raw). tr may be nil.
//
// Every lookup is made before any record changes. A lookup error for which
// [IsAbort] holds is returned at once, with res, tr and the counts untouched;
// every other error falls back for its mark alone. A mark whose record is not an
// Observe with a Through after its event time is left as it is, with no lookup,
// and counted as new. A mark that is out of range, or a second mark of one record,
// panics: it is a translator's error.
func Apply(ctx context.Context, lk Lookup, horizon func(catalog.Layer) time.Time, res *Result, tr Resumer, described func(store.Subject, []byte) []byte) (Stats, error) {
	var st Stats
	if len(res.Marks) == 0 {
		res.Dropped = nil
		return st, nil
	}
	answers := make([]answer, len(res.Marks))
	seen := make(map[int]bool, len(res.Marks))
	for i, m := range res.Marks {
		if m.Index < 0 || m.Index >= len(res.Records) || seen[m.Index] {
			panic(fmt.Sprintf("resume: mark %d names record %d of %d, or a record marked twice", i, m.Index, len(res.Records)))
		}
		seen[m.Index] = true
		r := res.Records[m.Index]
		a := answer{ref: store.LookupRef{Layer: r.Layer, Subject: r.Subject, Producer: r.Producer}, start: r.EventTime}
		if r.Kind != lifecycle.Observe || !r.Through.After(r.EventTime) {
			a.skip = true
			answers[i] = a
			continue
		}
		last, err := lk.Last(ctx, a.ref)
		switch {
		case err == nil:
			a.last = last
		case IsAbort(err):
			return Stats{}, fmt.Errorf("resume: %w", err)
		default:
			a.err = err
		}
		answers[i] = a
	}

	res.Dropped = nil
	type key struct {
		producer lifecycle.Producer
		subject  store.Subject
	}
	type floor struct {
		t     time.Time
		basis store.EventTimeBasis
	}
	floors := map[key]floor{}
	var drop []int
	for i, m := range res.Marks {
		a := answers[i]
		r := &res.Records[m.Index]
		observedBasis := m.ObservedBasis
		if observedBasis == store.BasisUnknown {
			observedBasis = store.BasisObserved
		}
		fallback := func() {
			st.Unknown++
			r.EventTime, r.Through, r.EventTimeBasis = r.Through, time.Time{}, observedBasis
		}
		switch {
		case a.skip:
			st.New++
		case a.err != nil:
			fallback()
			st.UnknownError++
			if st.FirstError == nil {
				st.FirstError, st.ErrorExample = a.err, a.ref
			}
		case a.last.Outcome == store.LookupNone:
			st.New++
		case a.last.Outcome == store.LookupExhausted:
			fallback()
			st.UnknownExhausted++
			if isZeroRef(st.ExhaustedExample) {
				st.ExhaustedExample = a.ref
			}
		case a.last.Outcome == store.LookupBaselineOnly:
			fallback()
			st.UnknownBaseline++
		case a.last.Outcome != store.LookupFound:
			// No lookup answers anything else without an error.
			fallback()
			st.UnknownError++
			if st.FirstError == nil {
				st.FirstError, st.ErrorExample = fmt.Errorf("resume: a lookup answered %s with no error", a.last.Outcome), a.ref
			}
		case a.last.Kind == lifecycle.Delete:
			if !a.start.After(a.last.EventTime) {
				st.Dropped++
				drop = append(drop, m.Index)
				continue
			}
			st.New++
		case !a.last.Last.After(a.start):
			st.New++
		default:
			obs := r.Through
			t, basis := a.last.Last, a.last.Basis
			switch {
			case obs.Before(t):
				t, basis = obs, observedBasis
			case a.last.LastFromThrough:
				basis = observedBasis
			}
			h := time.Time{}
			if horizon != nil {
				h = horizon(r.Layer)
			}
			switch {
			case !h.IsZero() && t.Before(h):
				fallback()
				st.UnknownBelowHorizon++
			case equalDescription(described, r.Subject, r.Payload, a.last.Payload):
				st.Raised++
				r.EventTime, r.EventTimeBasis = t, basis
				if !r.Through.After(t) {
					r.Through = time.Time{}
				}
			default:
				st.Changed++
				if a.last.TTL > 0 && obs.After(a.last.Last.Add(a.last.TTL)) {
					st.ChangedGap++
					if isZeroRef(st.GapExample) {
						st.GapExample = a.ref
					}
				}
				r.EventTime, r.Through, r.EventTimeBasis = obs, time.Time{}, observedBasis
			}
		}

		// An earlier resumed record of the subject is a floor, as for any later one.
		k := key{r.Producer, r.Subject}
		if f, ok := floors[k]; ok && r.EventTime.Before(f.t) {
			raiseTo(r, f.t, f.basis)
		}
		t, basis := r.EventTime, r.EventTimeBasis
		if f, ok := floors[k]; !ok || t.After(f.t) {
			floors[k] = floor{t, basis}
		}
		if tr != nil {
			tr.Resumed(m.Handle, t, basis)
		}
		for j := m.Index + 1; j < len(res.Records); j++ {
			o := &res.Records[j]
			if o.Producer == r.Producer && o.Subject == r.Subject && o.EventTime.Before(t) {
				raiseTo(o, t, basis)
				st.MirrorRaised++
			}
		}
	}
	if len(drop) > 0 {
		compact(res, drop)
	}
	return st, nil
}

// raiseTo gives a record a later event time and its basis, as a translator's clamp
// does, and keeps its Through only while that is still after the event time.
func raiseTo(r *store.Record, t time.Time, basis store.EventTimeBasis) {
	r.EventTime, r.EventTimeBasis = t, basis
	if !r.Through.After(t) {
		r.Through = time.Time{}
	}
}

func equalDescription(described func(store.Subject, []byte) []byte, sub store.Subject, a, b []byte) bool {
	if described == nil {
		return bytes.Equal(a, b)
	}
	return bytes.Equal(described(sub, a), described(sub, b))
}

// compact removes the dropped records, whose positions are given in any order,
// and their marks, moves the other marks with their records, and sets Dropped.
func compact(res *Result, drop []int) {
	gone := make(map[int]bool, len(drop))
	for _, i := range drop {
		gone[i] = true
	}
	newIndex := make([]int, len(res.Records))
	kept := res.Records[:0]
	for i, r := range res.Records {
		if gone[i] {
			newIndex[i] = -1
			res.Dropped = append(res.Dropped, i)
			continue
		}
		newIndex[i] = len(kept)
		kept = append(kept, r)
	}
	clear(res.Records[len(kept):])
	res.Records = kept
	marks := res.Marks[:0]
	for _, m := range res.Marks {
		if n := newIndex[m.Index]; n >= 0 {
			m.Index = n
			marks = append(marks, m)
		}
	}
	res.Marks = marks
}
