package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The fault classes of the seeded simulation. Class T faults are transparent: the
// answers must equal those of a run without faults. Class W faults allow a
// one-sided difference inside a window. Class R faults are outside the supported
// topology or beyond a bound the code states, and are only reported.
type dstClass string

const (
	dstT1  dstClass = "T1"  // restart
	dstT2  dstClass = "T2"  // crash, no final flush
	dstT3  dstClass = "T3"  // write refused as invalid
	dstT4  dstClass = "T4"  // write refused for the horizon, once
	dstT5  dstClass = "T5"  // write failed, not stored
	dstT6  dstClass = "T6"  // write failed, stored
	dstT7  dstClass = "T7"  // translation panic
	dstT8  dstClass = "T8"  // lookup abort
	dstT9  dstClass = "T9"  // silence of 30 minutes, configured name
	dstT10 dstClass = "T10" // silence of 10 minutes, unconfigured name
	dstT11 dstClass = "T11" // duplicate
	dstT12 dstClass = "T12" // drop
	dstT13 dstClass = "T13" // backlog in order
	dstT14 dstClass = "T14" // shuffle within a request
	dstT15 dstClass = "T15" // producer clock offset
	dstW1  dstClass = "W1"  // silence of 30 minutes, unconfigured name
	dstW2  dstClass = "W2"  // lookup read error
	dstW3  dstClass = "W3"  // a flush write that fails
	dstR1  dstClass = "R1"  // two adjacent requests swapped
	dstR2  dstClass = "R2"  // producer clock offset beyond the translators' bound
	dstR3  dstClass = "R3"  // backlog beyond the TTL
)

// dstJudged are the classes TestDST draws from.
var dstJudged = []dstClass{
	dstT1, dstT2, dstT3, dstT4, dstT5, dstT6, dstT7, dstT8, dstT9, dstT10, dstT11, dstT12, dstT13, dstT14, dstT15,
	dstW1, dstW2, dstW3,
}

// resetsTranslators says the fault makes the translators anew.
func (c dstClass) resetsTranslators() bool {
	switch c {
	case dstT1, dstT2, dstT3, dstT4, dstT5, dstT7, dstT8:
		return true
	}
	return false
}

func (c dstClass) isSilence() bool { return c == dstT9 || c == dstT10 || c == dstW1 }

// isReset says a dependent fault may follow: the translators lose their state.
func (c dstClass) isReset() bool { return c == dstT1 || c == dstT2 || c == dstW1 }

func (c dstClass) dependent() bool { return c == dstT8 || c == dstW2 }

// dstFault is one fault of a schedule: its class, the ingest instant it starts at
// and its parameters. The fields after offset are filled in by a run.
type dstFault struct {
	class dstClass
	start time.Time
	// dur is a silence's or a backlog's length.
	dur time.Duration
	// mode is, for T8 and W2, where the lookup fails; for T11 and T12, the kind of
	// request.
	mode int
	// multi says the pull the fault targets is delivered in a group of requests.
	multi bool
	// offset is the producer's clock offset of T15 and R2.
	offset time.Duration

	fired   bool
	firedAt time.Time
}

// span is how long the schedule keeps clear after the fault starts.
func (f dstFault) span() time.Duration {
	switch f.class {
	case dstT1, dstT2, dstT15, dstR2:
		return 0
	case dstT3, dstT4, dstT5, dstT6, dstT7, dstT8, dstW2:
		return 10 * time.Minute
	case dstT9, dstT10, dstW1, dstT13, dstR3:
		return f.dur
	case dstW3:
		return 25 * time.Minute
	}
	return 5 * time.Minute
}

// resetGuard is how long after the start of a fault that makes the translators
// anew a silence may not begin: at least one pull must have been answered since.
func (f dstFault) resetGuard() time.Duration {
	switch f.class {
	case dstT1, dstT2:
		return 6 * time.Minute
	case dstT3, dstT4, dstT5, dstT7, dstT8:
		return 20 * time.Minute
	}
	return 0
}

func (f dstFault) String() string {
	s := fmt.Sprintf("%s at %s", f.class, f.start.Sub(t0))
	if f.dur != 0 {
		s += fmt.Sprintf(" for %s", f.dur)
	}
	switch f.class {
	case dstT8, dstW2:
		s += fmt.Sprintf(" mode %d", f.mode)
	case dstT11, dstT12:
		s += " on a " + dstReqKind(f.mode).String()
	case dstT15, dstR2:
		s += fmt.Sprintf(" offset %s", f.offset)
	}
	if f.multi {
		s += " in a group"
	}
	if f.fired {
		s += fmt.Sprintf(" (fired at %s)", f.firedAt.Sub(t0))
	}
	return s
}

// dstSchedule is the faults of a seed, with the draws that are not faults.
type dstSchedule struct {
	idx    int
	draw   uint64
	faults []dstFault
	// configured says the seed runs with the cluster name configured.
	configured bool
	// k is the producer's clock offset of the whole seed.
	k time.Duration
	// grouped says the pull instant delivers its pull and report as one group, and
	// reportFirst that the report is first in it.
	grouped, reportFirst []bool
	// force lists the intervals that create a pod no watch event shows.
	force map[int]bool
	// noGroups and the faults are overridden by the tests of the harness itself.
	noGroups bool
}

type dstTemplate struct {
	classes []dstClass
	multi   bool
}

// dstTemplates are appended to the first fault of the seeds after the first
// full cycle of classes, so that the pairs the coverage test names occur.
var dstTemplates = []dstTemplate{
	{classes: []dstClass{dstT1, dstW2}},
	{classes: []dstClass{dstW1, dstT1}},
	{classes: []dstClass{dstT7}, multi: true},
	{classes: []dstClass{dstT3}, multi: true},
	{classes: []dstClass{dstT1, dstT8, dstT5}},
	{classes: []dstClass{dstT2, dstT2}},
	{classes: []dstClass{dstT2, dstW2}},
}

func dstSnap(d time.Duration) time.Duration { return d - d%dstStep }

// dstNewSchedule draws the schedule of seed idx. A report schedule draws one
// class R fault first.
func dstNewSchedule(idx int, report bool) *dstSchedule {
	draw := dstSeedBase + uint64(idx)
	r := rand.New(rand.NewPCG(draw, 0x7363686564))
	s := &dstSchedule{idx: idx, draw: draw, force: map[int]bool{}}
	s.grouped, s.reportFirst = make([]bool, dstPulls), make([]bool, dstPulls)
	for i := range dstPulls {
		s.grouped[i] = r.IntN(4) == 0
		s.reportFirst[i] = r.IntN(2) == 0
	}
	first := dstJudged[idx%len(dstJudged)]
	if report {
		first = []dstClass{dstR1, dstR2, dstR3}[idx%3]
	}
	classes := []dstClass{first}
	multi := false
	if tpl := idx - len(dstJudged); !report && tpl >= 0 && tpl < len(dstTemplates) {
		classes = append(classes, dstTemplates[tpl].classes...)
		multi = dstTemplates[tpl].multi
	} else {
		for n := 2 + r.IntN(3); len(classes) < n; {
			classes = append(classes, dstJudged[r.IntN(len(dstJudged))])
		}
	}
	// The cluster name is configured if the schedule holds T9, and unset if it holds
	// W1 or T10, whichever comes first.
	for _, c := range classes {
		if c == dstT9 {
			s.configured = true
			break
		}
		if c == dstW1 || c == dstT10 {
			break
		}
	}
	for i, c := range classes {
		switch {
		case s.configured && (c == dstW1 || c == dstT10):
			classes[i] = dstT9
		case !s.configured && c == dstT9:
			classes[i] = dstT10
		}
		if c := classes[i]; c.dependent() && (i == 0 || !classes[i-1].isReset()) {
			classes[i] = dstT1
		}
	}

	limit := dstEnd().Add(-30 * time.Minute)
	cursor := t0.Add(dstWarmup + time.Duration(r.IntN(11))*dstStep)
	for i, c := range classes {
		f := dstFault{class: c}
		switch c {
		case dstT8, dstW2:
			prev := s.faults[len(s.faults)-1]
			f.start = prev.start
			if prev.class == dstW1 {
				f.start = prev.start.Add(prev.dur)
			}
			if c == dstT8 {
				f.mode = r.IntN(3)
			} else {
				f.mode = r.IntN(2)
			}
		case dstT15, dstR2:
			f.start = t0
			f.offset = time.Duration(r.IntN(17)-8) * dstStep
			if f.offset == 0 {
				f.offset = dstStep
			}
			if c == dstR2 {
				f.offset = 10 * time.Minute * time.Duration(1-2*r.IntN(2))
			}
		default:
			f.start = cursor
			if i > 0 {
				gap := time.Duration(8+r.IntN(23)) * time.Minute
				if r.IntN(4) == 0 {
					gap = time.Duration(r.IntN(11)) * dstStep
				}
				f.start = cursor.Add(gap)
			}
			f.start = t0.Add(dstSnap(f.start.Sub(t0)))
		}
		switch c {
		case dstT9, dstW1:
			f.dur = 30 * time.Minute
		case dstT10:
			f.dur = 10 * time.Minute
		case dstT13:
			f.dur = time.Duration(5*(1+r.IntN(2))) * time.Minute
		case dstR3:
			f.dur = 20 * time.Minute
		case dstT11, dstT12:
			f.mode = r.IntN(3)
		}
		if c.isSilence() {
			for _, g := range s.faults {
				var end time.Time
				switch {
				case g.class == dstW1:
					end = g.start.Add(g.dur + 6*time.Minute)
				case g.class.resetsTranslators():
					end = g.start.Add(g.resetGuard())
				default:
					continue
				}
				if f.start.Before(end) {
					f.start = end
				}
			}
		}
		if c == dstT3 || c == dstT7 {
			if multi || r.IntN(2) == 0 {
				f.multi = s.pinToGroup(&f, multi && c == dstT7)
			}
		}
		if c != dstT15 && c != dstR2 && f.start.Add(f.span()).After(limit) {
			break
		}
		s.faults = append(s.faults, f)
		if c == dstT15 || c == dstR2 {
			s.k = f.offset
			continue
		}
		cursor = f.start.Add(f.span())
		switch c {
		case dstT3, dstT4, dstT5, dstT6, dstT7:
			s.forceAround(f)
		}
	}
	return s
}

// pinToGroup moves the start of the fault to just before the first pull instant,
// not before the fault's start, that delivers its requests as a group, and says
// whether there was one. A fault pinned to a group forces the interval before it
// to create a pod, so that the pull carries a first sighting.
func (s *dstSchedule) pinToGroup(f *dstFault, reportFirst bool) bool {
	from := int((f.start.Sub(t0) + dstPullEvery - 1) / dstPullEvery)
	for g := max(from, 2); g < dstPulls-8; g++ {
		if s.grouped[g] {
			f.start = t0.Add(time.Duration(g)*dstPullEvery - dstStep)
			if reportFirst {
				s.reportFirst[g] = true
			}
			s.force[g-1], s.force[g-2] = true, true
			return true
		}
	}
	return false
}

// forceAround forces the intervals around a fault that is armed on a pull to
// create a pod that only the pull shows.
func (s *dstSchedule) forceAround(f dstFault) {
	k := int(f.start.Sub(t0) / dstPullEvery)
	for j := k - 1; j <= k+2; j++ {
		if j >= 0 && j < dstPulls-1 {
			s.force[j] = true
		}
	}
}

func (s *dstSchedule) has(c dstClass) bool {
	return slices.ContainsFunc(s.faults, func(f dstFault) bool { return f.class == c })
}

func (s *dstSchedule) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "  schedule (draw %d, configured name %v, producer offset %s):\n", s.draw, s.configured, s.k)
	for _, f := range s.faults {
		fmt.Fprintf(&b, "    %s\n", f)
	}
	return b.String()
}

// dstPlan is what the schedule does to the requests, which does not depend on
// the run: who is silenced or dropped, held back, shuffled or sent twice.
type dstPlan struct {
	skip    map[int]bool
	at      map[int]time.Time
	shuffle map[int]bool
	dup     map[int]bool
}

func (s *dstSchedule) plan(w *dstWorld) *dstPlan {
	p := &dstPlan{skip: map[int]bool{}, at: map[int]time.Time{}, shuffle: map[int]bool{}, dup: map[int]bool{}}
	first := func(from time.Time, ok func(*dstReq) bool) *dstReq {
		for _, rq := range w.reqs {
			if !rq.at.Before(from) && !p.skip[rq.id] && ok(rq) {
				return rq
			}
		}
		return nil
	}
	for _, f := range s.faults {
		switch f.class {
		case dstT9, dstT10, dstW1:
			for _, rq := range w.reqs {
				if !rq.at.Before(f.start) && rq.at.Before(f.start.Add(f.dur)) {
					p.skip[rq.id] = true
				}
			}
		case dstT12:
			if rq := first(f.start, func(rq *dstReq) bool { return rq.kind == dstReqKind(f.mode) }); rq != nil {
				p.skip[rq.id] = true
			}
		case dstT13, dstR3:
			for _, rq := range w.reqs {
				if !rq.at.Before(f.start) && rq.at.Before(f.start.Add(f.dur)) && !p.skip[rq.id] {
					p.at[rq.id] = f.start.Add(f.dur)
				}
			}
		case dstT14:
			if rq := first(f.start, func(rq *dstReq) bool { return rq.kind == dstPullReq }); rq != nil {
				p.shuffle[rq.id] = true
			}
		case dstT11:
			if rq := first(f.start, func(rq *dstReq) bool { return rq.kind == dstReqKind(f.mode) }); rq != nil {
				p.dup[rq.id] = true
			}
		case dstR1:
			a := first(f.start, func(*dstReq) bool { return true })
			if a != nil {
				b := first(a.at, func(rq *dstReq) bool { return rq.id > a.id })
				if b != nil {
					p.at[a.id], p.at[b.id] = b.at, a.at
				}
			}
		}
	}
	return p
}

// deliverAt is the instant a request is first sent.
func (p *dstPlan) deliverAt(rq *dstReq) time.Time {
	if t, ok := p.at[rq.id]; ok {
		return t
	}
	return rq.at
}

// dstShuffled is the request with the records of its one resource permuted.
func dstShuffled(d capture.LogsData, draw uint64, id int) capture.LogsData {
	r := rand.New(rand.NewPCG(draw, uint64(id)))
	rl := d.ResourceLogs[0]
	recs := slices.Clone(rl.ScopeLogs[0].LogRecords)
	r.Shuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource:  rl.Resource,
		ScopeLogs: []capture.ScopeLogs{{LogRecords: recs}},
	}}}
}

// ---- the lookup that fails on demand ----

type dstLookupMode int

const (
	dstOpenAbort  dstLookupMode = iota // the opener's error wraps ErrClosed (T8)
	dstLastAbort                       // Last's error wraps ErrClosed (T8)
	dstLastCancel                      // Last's error wraps context.Canceled (T8)
	dstOpenError                       // the opener's error is a plain one (W2)
	dstLastError                       // Last's error is a plain one (W2)
)

func (m dstLookupMode) opener() bool { return m == dstOpenAbort || m == dstOpenError }
func (m dstLookupMode) abort() bool  { return m != dstOpenError && m != dstLastError }

func (m dstLookupMode) err() error {
	switch m {
	case dstOpenAbort:
		return fmt.Errorf("memstore: NewLookup: %w", store.ErrClosed)
	case dstLastAbort:
		return fmt.Errorf("memstore: Lookup.Last: %w", store.ErrClosed)
	case dstLastCancel:
		return fmt.Errorf("memstore: Lookup.Last: %w", context.Canceled)
	case dstOpenError:
		return errors.New("memstore: NewLookup: injected failure")
	}
	return errors.New("memstore: Lookup.Last: injected failure")
}

// dstLookupFire is one lookup fault that took effect: whether it aborted, and how
// many reads it failed.
type dstLookupFire struct {
	abort bool
	hits  int
	// opener says the opener failed, so that whether the group had marks is unknown.
	opener bool
}

// dstLookups opens the memstore's lookups for the sequencer. An armed fault fails
// the next lookup that it can: an opener fault the next opening, a read fault the
// first lookup that is asked for a reference.
type dstLookups struct {
	m     *memstore.Store
	mu    sync.Mutex
	armed bool
	mode  dstLookupMode
	fires []dstLookupFire
}

func (l *dstLookups) arm(m dstLookupMode) {
	l.mu.Lock()
	l.armed, l.mode = true, m
	l.mu.Unlock()
}

func (l *dstLookups) isArmed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.armed
}

// take returns the faults that took effect since the last call.
func (l *dstLookups) take() []dstLookupFire {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.fires
	l.fires = nil
	return out
}

func (l *dstLookups) open() (resume.Lookup, error) {
	l.mu.Lock()
	armed, mode := l.armed, l.mode
	if armed && mode.opener() {
		l.armed = false
		l.fires = append(l.fires, dstLookupFire{abort: mode.abort(), opener: true})
		l.mu.Unlock()
		return nil, mode.err()
	}
	l.mu.Unlock()
	lk, err := l.m.NewLookup()
	if err != nil {
		return nil, err
	}
	return &dstLookup{l: l, lk: lk, faulty: armed, mode: mode}, nil
}

type dstLookup struct {
	l      *dstLookups
	lk     *memstore.Lookup
	faulty bool
	mode   dstLookupMode
	hits   int
}

func (d *dstLookup) Last(ctx context.Context, ref store.LookupRef) (store.LastAssertion, error) {
	if d.faulty {
		d.hits++
		return store.LastAssertion{}, d.mode.err()
	}
	return d.lk.Last(ctx, ref)
}

func (d *dstLookup) Close() error {
	if d.faulty && d.hits > 0 {
		d.l.mu.Lock()
		d.l.armed = false
		d.l.fires = append(d.l.fires, dstLookupFire{abort: d.mode.abort(), hits: d.hits})
		d.l.mu.Unlock()
	}
	return d.lk.Close()
}
