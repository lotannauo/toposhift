package storetest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The checks of [store.Store.WindowSince]. Each answer is compared three ways: with
// the reference's, with the answer the contract defines, worked out here from every
// record the store accepted (the window by its bounds, the seeds by their
// definition), and, for the fold identity, by folding each subject's seeds and
// window records and folding its whole retained history, which must agree on the
// window. The window part must also be what Window or EntityWindow answers.

// sinceDirs are the directions WindowSince reads: edges both ways, and 0 for the
// entities' own existence.
var sinceDirs = []store.Direction{store.Forward, store.Reverse, 0}

// sinceDiff says how two answers of WindowSince differ, or returns "".
func sinceDiff(got, want []store.Since) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d answers, want %d", len(got), len(want))
	}
	for i := range want {
		if msg := seedsDiff(got[i].Seeds, want[i].Seeds); msg != "" {
			return fmt.Sprintf("answer %d: seeds: %s", i, msg)
		}
		if msg := diffRecords(got[i].Window, want[i].Window); msg != "" {
			return fmt.Sprintf("answer %d: window: %s", i, msg)
		}
	}
	return ""
}

func seedsDiff(got, want []store.Seed) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d seeds %s, want %d %s", len(got), describeSeeds(got), len(want), describeSeeds(want))
	}
	for i := range want {
		if got[i].PayloadKnown != want[i].PayloadKnown {
			return fmt.Sprintf("seed %d %s has PayloadKnown %v, want %v", i, describeSeeds(got[i:i+1]), got[i].PayloadKnown, want[i].PayloadKnown)
		}
		if msg := diffRecords([]store.Record{got[i].Record}, []store.Record{want[i].Record}); msg != "" {
			return fmt.Sprintf("seed %d: %s", i, msg)
		}
	}
	return ""
}

func describeSeeds(ss []store.Seed) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		r := s.Record
		parts[i] = fmt.Sprintf("%s by %s at %s seq %d", describeSubject(r.Subject), r.Producer, r.EventTime.Format(time.RFC3339Nano), r.Seq)
	}
	return "[" + strings.Join(parts, "; ") + "]"
}

func describeSubject(s store.Subject) string {
	if s.Kind == store.SubjectEntity {
		return "entity " + s.A.String()
	}
	return fmt.Sprintf("edge %s -[%s]-> %s", s.A, s.Relation, s.B)
}

// sinceSide is one side a record is read from: an entity's own existence (dir 0),
// or one direction of its edges.
type sinceSide struct {
	fp  identity.Fingerprint
	dir store.Direction
}

// sinceIndex holds every record a store accepted, by the sides it is read from, in
// the order written (ascending Seq).
type sinceIndex map[sinceSide][]store.Record

func newSinceIndex(written []store.Record) sinceIndex {
	idx := sinceIndex{}
	for _, r := range written {
		switch r.Subject.Kind {
		case store.SubjectEntity:
			k := sinceSide{r.Subject.A, 0}
			idx[k] = append(idx[k], r)
		case store.SubjectEdge:
			f, b := sinceSide{r.Subject.A, store.Forward}, sinceSide{r.Subject.B, store.Reverse}
			idx[f] = append(idx[f], r)
			idx[b] = append(idx[b], r)
		}
	}
	return idx
}

// visible are the records of one side in the scope, in (EventTime, Seq) order.
func (idx sinceIndex) visible(fp identity.Fingerprint, dir store.Direction, sc store.Scope) []store.Record {
	var out []store.Record
	for _, r := range idx[sinceSide{fp, dir}] {
		if r.Layer == sc.Layer && r.Seq <= sc.AsOf {
			out = append(out, r)
		}
	}
	store.SortRecords(out)
	return out
}

// seedInstant is the instant a seed must hold at: just before from, or from itself
// when from is the horizon h.
func seedInstant(from time.Time, h store.Horizon) time.Time {
	if !h.IsZero() && from.Equal(h.Time) {
		return from
	}
	return from.Add(-time.Nanosecond)
}

// holdsAt says whether a record, as the deciding one of its reference, holds at x.
func holdsAt(r store.Record, x time.Time) bool {
	last, _ := store.LastOf(r.EventTime, r.Through)
	return r.Kind == lifecycle.Observe && (r.TTL == 0 || x.Before(last.Add(r.TTL)))
}

// deciding returns, of records in (EventTime, Seq) order, the deciding record of
// each reference among those before the instant, in the order the references first
// appear.
func deciding(recs []store.Record, before time.Time) []store.Record {
	type ref struct {
		subject  store.Subject
		producer lifecycle.Producer
	}
	at := map[ref]int{}
	var out []store.Record
	for _, r := range recs {
		if !r.EventTime.Before(before) {
			break
		}
		k := ref{r.Subject, r.Producer}
		if i, ok := at[k]; ok {
			out[i] = r
			continue
		}
		at[k] = len(out)
		out = append(out, r)
	}
	return out
}

// want is the answer the contract defines for one fingerprint, from the records
// written; h is the horizon of the scope's layer.
func (idx sinceIndex) want(fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope, h store.Horizon, o store.SinceOptions) store.Since {
	var out store.Since
	if !from.Before(to) {
		return out
	}
	recs := idx.visible(fp, dir, sc)
	for _, r := range recs {
		if !r.EventTime.Before(from) && r.EventTime.Before(to) {
			r.Payload = slices.Clone(r.Payload)
			out.Window = append(out.Window, r)
		}
	}
	live := seedInstant(from, h)
	for _, r := range deciding(recs, from) {
		if !holdsAt(r, live) {
			continue
		}
		known := o.Payloads && (h.IsZero() || !r.EventTime.Before(h.Time))
		payload := r.Payload
		r.Payload = nil
		if known && len(payload) > 0 {
			r.Payload = slices.Clone(payload)
		}
		out.Seeds = append(out.Seeds, store.Seed{Record: r, PayloadKnown: known})
	}
	store.SortSeeds(out.Seeds)
	return out
}

// retained is the history a fold from the start of retained history sees: with a
// horizon h, the records at or after it, and of the records before it, for each
// reference, the deciding one if it holds at h (what a baseline stands for).
func retained(recs []store.Record, h store.Horizon) []store.Record {
	if h.IsZero() {
		return recs
	}
	out := slices.DeleteFunc(slices.Clone(recs), func(r store.Record) bool { return r.EventTime.Before(h.Time) })
	for _, r := range deciding(recs, h.Time) {
		if holdsAt(r, h.Time) {
			out = append(out, r)
		}
	}
	store.SortRecords(out)
	return out
}

// piece is one existence interval clipped to a window, as a range read reports
// it: whether it started before the window, and how it ended inside it.
type piece struct {
	start, end time.Time
	before     bool
	src        lifecycle.EndSource // set only when it ends inside the window
}

// clipped folds records (of one subject) with the zero policy and clips the
// existence to [from, to).
func clipped(recs []store.Record, from, to time.Time) ([]piece, lifecycle.Timeline, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, tl, err
	}
	var out []piece
	for _, iv := range tl.Existence() {
		if !iv.Start.Before(to) || (!iv.Open() && !iv.End.After(from)) {
			continue
		}
		p := piece{start: iv.Start, end: to, before: iv.Start.Before(from)}
		if p.before {
			p.start = from
		}
		if !iv.Open() && iv.End.Before(to) {
			p.end, p.src = iv.End, iv.EndSource
		}
		out = append(out, p)
	}
	return out, tl, nil
}

func samePieces(a, b []piece) bool {
	return slices.EqualFunc(a, b, func(x, y piece) bool {
		return x.start.Equal(y.start) && x.end.Equal(y.end) && x.before == y.before && x.src == y.src
	})
}

// foldIdentity checks the fold identity for one answer of one fingerprint: for each
// subject, the fold of its seeds and window records, and the fold of its history
// before to (all of it, or from the start of retained history when from is the
// horizon), must give the same existence at every probe in [from, to), in all and
// for each producer, and the same pieces of the window, with the same Before and
// end sources.
func foldIdentity(ans store.Since, history []store.Record, from, to time.Time, h store.Horizon) error {
	if !h.IsZero() && from.Equal(h.Time) {
		history = retained(history, h)
	}
	part := map[store.Subject][]store.Record{}
	whole := map[store.Subject][]store.Record{}
	var subjects []store.Subject
	add := func(m map[store.Subject][]store.Record, r store.Record) {
		if _, ok := part[r.Subject]; !ok {
			if _, ok := whole[r.Subject]; !ok {
				subjects = append(subjects, r.Subject)
			}
		}
		m[r.Subject] = append(m[r.Subject], r)
	}
	for _, s := range ans.Seeds {
		add(part, s.Record)
	}
	for _, r := range ans.Window {
		add(part, r)
	}
	for _, r := range history {
		if r.EventTime.Before(to) {
			add(whole, r)
		}
	}
	for _, sub := range subjects {
		if err := foldSubjectIdentity(sub, part[sub], whole[sub], from, to, h); err != nil {
			return err
		}
	}
	return nil
}

func foldSubjectIdentity(sub store.Subject, part, whole []store.Record, from, to time.Time, h store.Horizon) error {
	store.SortRecords(part)
	store.SortRecords(whole)
	// The instants where an answer can change: the window's ends, every record and
	// deadline in it and the instant before each, and the horizon.
	probes := []time.Time{from, to.Add(-time.Nanosecond)}
	for _, r := range append(slices.Clone(part), whole...) {
		last, _ := store.LastOf(r.EventTime, r.Through)
		for _, x := range []time.Time{r.EventTime, r.EventTime.Add(-time.Nanosecond), last.Add(r.TTL), last.Add(r.TTL - time.Nanosecond)} {
			if !x.Before(from) && x.Before(to) {
				probes = append(probes, x)
			}
		}
	}
	if !h.IsZero() {
		for _, x := range []time.Time{h.Time, h.Time.Add(-time.Nanosecond)} {
			if !x.Before(from) && x.Before(to) {
				probes = append(probes, x)
			}
		}
	}
	producers := map[lifecycle.Producer]bool{}
	for _, r := range append(slices.Clone(part), whole...) {
		producers[r.Producer] = true
	}
	check := func(who string, part, whole []store.Record) error {
		pp, ptl, err := clipped(part, from, to)
		if err != nil {
			return fmt.Errorf("folding the seeds and window of %s%s: %w", describeSubject(sub), who, err)
		}
		wp, wtl, err := clipped(whole, from, to)
		if err != nil {
			return fmt.Errorf("folding the history of %s%s: %w", describeSubject(sub), who, err)
		}
		for _, x := range probes {
			if ptl.AliveAt(x) != wtl.AliveAt(x) {
				return fmt.Errorf("%w: %s%s at %s: the seeds and window fold to alive %v, the history to %v",
					ErrMismatch, describeSubject(sub), who, x.Format(time.RFC3339Nano), ptl.AliveAt(x), wtl.AliveAt(x))
			}
		}
		if !samePieces(pp, wp) {
			return fmt.Errorf("%w: %s%s in [%s, %s): the seeds and window fold to %+v, the history to %+v",
				ErrMismatch, describeSubject(sub), who, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), pp, wp)
		}
		return nil
	}
	if err := check("", part, whole); err != nil {
		return err
	}
	for p := range producers {
		only := func(rs []store.Record) []store.Record {
			return slices.DeleteFunc(slices.Clone(rs), func(r store.Record) bool { return r.Producer != p })
		}
		if err := check(" by "+string(p), only(part), only(whole)); err != nil {
			return err
		}
	}
	return nil
}

// sinceCoverage counts what a run of the checks reached, so a test can hold the
// fixed workloads to reaching the cases the checks exist for.
type sinceCoverage struct {
	calls, seeds          int
	payloadKnown, unknown int // seeds that carried their payload, and that could not
	atHorizon             int // calls whose from is the horizon, with at least one seed
	deadlineAtFrom        int // seeds whose deadline is from itself
	deadlineOnHorizon     int // references the horizon rule left out at from == h
	beforeHorizon         int // seeds decided before the horizon
	refused               int // reads refused for starting before the horizon
	foldChecks            int // subjects checked by the fold identity
}

// sinceAsk is one WindowSince question.
type sinceAsk struct {
	fps      []identity.Fingerprint
	dir      store.Direction
	from, to time.Time
	sc       store.Scope
	o        store.SinceOptions
}

func (a sinceAsk) String() string {
	what := "edges " + a.dir.String()
	if a.dir == 0 {
		what = "own existence"
	}
	fp := fmt.Sprintf("%d fingerprints", len(a.fps))
	if len(a.fps) == 1 {
		fp = a.fps[0].String()
	}
	return fmt.Sprintf("WindowSince(%s, %s, [%s, %s), %s, payloads %v)", fp, what,
		a.from.Format(time.RFC3339Nano), a.to.Format(time.RFC3339Nano), scopeLabel(a.sc), a.o.Payloads)
}

// sinceRound asks the store and the reference the same WindowSince questions and
// checks both against the contract's answer.
type sinceRound struct {
	cand store.Store
	ref  *memstore.Store
	idx  sinceIndex
	cov  *sinceCoverage
	// fold says the fold identity is checked too.
	fold bool
}

// ask puts one question to both stores and checks the answers.
func (s sinceRound) ask(a sinceAsk) error {
	h := s.ref.LayerHorizon(a.sc.Layer)
	want := make([]store.Since, len(a.fps))
	for i, fp := range a.fps {
		want[i] = s.idx.want(fp, a.dir, a.from, a.to, a.sc, h, a.o)
	}
	refGot, err := s.ref.WindowSince(bg, a.fps, a.dir, a.from, a.to, a.sc, a.o)
	if err != nil {
		return fmt.Errorf("reference %s: %w", a, err)
	}
	if msg := sinceDiff(refGot, want); msg != "" {
		return fmt.Errorf("reference %s: %s (the reference disagrees with the contract)", a, msg)
	}
	got, err := s.cand.WindowSince(bg, a.fps, a.dir, a.from, a.to, a.sc, a.o)
	if err != nil {
		return fmt.Errorf("%s: %w", a, err)
	}
	scribbleSince(refGot)
	if msg := sinceDiff(got, want); msg != "" {
		return fmt.Errorf("%w: %s: %s", ErrMismatch, a, msg)
	}
	s.cov.calls++
	for i, fp := range a.fps {
		// The window is Window's or EntityWindow's.
		var win []store.Record
		if a.dir == 0 {
			win, err = s.cand.EntityWindow(bg, fp, a.from, a.to, a.sc)
		} else {
			win, err = s.cand.Window(bg, fp, a.dir, a.from, a.to, a.sc)
		}
		if err != nil {
			return fmt.Errorf("the window of %s: %w", a, err)
		}
		if a.from.Before(a.to) {
			if msg := diffRecords(got[i].Window, win); msg != "" {
				return fmt.Errorf("%w: %s answer %d: its window is not the store's own window: %s", ErrMismatch, a, i, msg)
			}
		}
		s.count(got[i], fp, a, h)
		if s.fold && a.from.Before(a.to) {
			s.cov.foldChecks += len(got[i].Seeds) + len(got[i].Window)
			if err := foldIdentity(got[i], s.idx.visible(fp, a.dir, a.sc), a.from, a.to, h); err != nil {
				return fmt.Errorf("%s answer %d: %w", a, i, err)
			}
		}
	}
	scribbleSince(got)
	return nil
}

// scribbleSince overwrites the payloads an answer returned, so a store that shares
// them with its own copy is found by the next read.
func scribbleSince(ans []store.Since) {
	for _, a := range ans {
		scribble(a.Window)
		for _, s := range a.Seeds {
			scribble([]store.Record{s.Record})
		}
	}
}

func (s sinceRound) count(ans store.Since, fp identity.Fingerprint, a sinceAsk, h store.Horizon) {
	s.cov.seeds += len(ans.Seeds)
	for _, sd := range ans.Seeds {
		switch {
		case sd.PayloadKnown:
			s.cov.payloadKnown++
		case a.o.Payloads:
			s.cov.unknown++
		}
		last, _ := store.LastOf(sd.Record.EventTime, sd.Record.Through)
		if sd.Record.TTL > 0 && last.Add(sd.Record.TTL).Equal(a.from) {
			s.cov.deadlineAtFrom++
		}
		if !h.IsZero() && sd.Record.EventTime.Before(h.Time) {
			s.cov.beforeHorizon++
		}
	}
	if !h.IsZero() && a.from.Equal(h.Time) {
		if len(ans.Seeds) > 0 {
			s.cov.atHorizon++
		}
		for _, r := range deciding(s.idx.visible(fp, a.dir, a.sc), a.from) {
			if holdsAt(r, a.from.Add(-time.Nanosecond)) && !holdsAt(r, a.from) {
				s.cov.deadlineOnHorizon++
			}
		}
	}
}

// refusal asks a window that starts one nanosecond before the horizon, and one at
// it under the token below the horizon's: both stores must refuse both.
func (s sinceRound) refusal(fps []identity.Fingerprint, layer catalog.Layer, h store.Horizon) error {
	asks := []sinceAsk{{fps: fps, from: h.Time.Add(-time.Nanosecond), to: h.Time.Add(time.Hour), sc: store.Current(layer)}}
	if h.Seq > 0 {
		asks = append(asks, sinceAsk{fps: fps, from: h.Time, to: h.Time.Add(time.Hour), sc: store.Scope{Layer: layer, AsOf: h.Seq - 1}})
	}
	for _, a := range asks {
		for _, dir := range sinceDirs {
			a.dir = dir
			if _, err := s.ref.WindowSince(bg, a.fps, a.dir, a.from, a.to, a.sc, a.o); !errors.Is(err, store.ErrBeforeHorizon) {
				return fmt.Errorf("reference %s with the horizon at %s (seq %d): want ErrBeforeHorizon, got %w", a, h.Time.Format(time.RFC3339Nano), h.Seq, orNil(err))
			}
			if _, err := s.cand.WindowSince(bg, a.fps, a.dir, a.from, a.to, a.sc, a.o); !errors.Is(err, store.ErrBeforeHorizon) {
				return fmt.Errorf("%w: %s with the horizon at %s (seq %d) must be refused with ErrBeforeHorizon, got %w", ErrMismatch, a, h.Time.Format(time.RFC3339Nano), h.Seq, orNil(err))
			}
			s.cov.refused++
		}
	}
	return nil
}

// CheckSince runs one workload through the store under test and the reference, as
// [Check] does, and after chunks of the stream, after each retention and at the
// end puts the same WindowSince questions to both: each sampled entity's edges in
// both directions and its own existence, in the layers it is in and one it is not,
// over windows whose start is the horizon, a record's instant, the nanosecond after
// it, a deadline and the nanosecond around it, under the latest token and earlier
// ones, with and without payloads, one fingerprint at a time and in a batch with
// repeats and an entity nothing is written about. Every answer must be the
// reference's, and both must be the answer the contract defines, worked out from
// the records written; the window part must be the store's own Window or
// EntityWindow; and the fold identity must hold. After a retention it also asks
// windows that start before the horizon, or at it under a token below its Seq,
// which both must refuse.
//
// cand must have been opened with w.Policy. The store is not closed.
func CheckSince(cand store.Store, w Workload, opts Options) error {
	_, err := checkSince(cand, w, opts)
	return err
}

func checkSince(cand store.Store, w Workload, opts Options) (sinceCoverage, error) {
	var cov sinceCoverage
	opts = opts.withDefaults()
	g, err := NewGenerator(w.Config)
	if err != nil {
		return cov, err
	}
	ref, err := memstore.Open(memstore.Options{Policy: w.Policy})
	if err != nil {
		return cov, err
	}
	defer func() { _ = ref.Close() }()
	stranger, err := strangerEntity()
	if err != nil {
		return cov, err
	}
	rng := rand.New(rand.NewPCG(w.Config.Seed+2, 0x5eed5))
	var horizons []time.Time
	for i, f := range opts.RetainAt {
		if f <= 0 || f >= 1 || (i > 0 && f <= opts.RetainAt[i-1]) {
			return cov, fmt.Errorf("storetest: RetainAt must be ascending fractions in (0, 1), got %v", opts.RetainAt)
		}
		horizons = append(horizons, g.Start().Add(time.Duration(f*float64(g.End().Sub(g.Start())))))
	}
	entities := g.Entities()
	var written []store.Record
	var horizon store.Horizon
	next, batches := 0, 0
	round := func(when string) error {
		if err := sinceCompare(cand, ref, written, entities, stranger, horizon, rng, opts, &cov); err != nil {
			return fmt.Errorf("%s: %w", when, err)
		}
		return nil
	}
	for {
		batch := g.Batch(1 + rng.IntN(opts.MaxBatch))
		if len(batch) == 0 {
			break
		}
		if !horizon.IsZero() {
			// Check offers these and requires both stores to refuse them; here
			// they are only left out.
			batch = slices.DeleteFunc(batch, func(r store.Record) bool { return r.EventTime.Before(horizon.Time) })
		}
		if len(batch) == 0 {
			continue
		}
		if err := writeBoth(cand, ref, batch); err != nil {
			return cov, err
		}
		written = append(written, batch...)
		batches++
		for next < len(horizons) && !batch[len(batch)-1].EventTime.Before(horizons[next]) {
			h := horizons[next]
			next++
			g.SetHorizon(h)
			for name, s := range map[string]store.Store{"store": cand, "reference": ref} {
				if err := s.Retain(bg, h); err != nil {
					return cov, fmt.Errorf("%s Retain(%s): %w", name, h.Format(time.RFC3339), err)
				}
			}
			horizon = ref.Horizon()
			if err := round(fmt.Sprintf("after Retain(%s)", h.Format(time.RFC3339))); err != nil {
				return cov, err
			}
		}
		if batches%opts.CheckEvery == 0 {
			if err := round(fmt.Sprintf("after %d records", len(written))); err != nil {
				return cov, err
			}
		}
	}
	return cov, round(fmt.Sprintf("at the end (%d records)", len(written)))
}

// sinceStarts picks the starts of windows for one side of an entity: the horizon
// (or the earliest instant), and around its records and their deadlines, none
// before floor.
func sinceStarts(recs []store.Record, floor time.Time, rng *rand.Rand, n int) []time.Time {
	out := []time.Time{floor}
	for attempts := 0; len(out) < n && len(recs) > 0 && attempts < 8*n; attempts++ {
		r := recs[rng.IntN(len(recs))]
		last, _ := store.LastOf(r.EventTime, r.Through)
		var x time.Time
		switch rng.IntN(5) {
		case 0:
			x = r.EventTime
		case 1:
			x = r.EventTime.Add(time.Nanosecond)
		case 2, 3:
			if r.TTL == 0 {
				continue
			}
			x = last.Add(r.TTL) // the deadline: the reference holds just before it
		default:
			x = r.EventTime.Add(time.Duration(rng.Int64N(int64(10 * time.Minute))))
		}
		if !x.Before(floor) {
			out = append(out, x)
		}
	}
	return out
}

// sinceEnds picks ends for a window that starts at from: the nanosecond after it,
// a few minutes on, and after everything.
func sinceEnds(from time.Time, rng *rand.Rand) []time.Time {
	return []time.Time{
		from.Add(time.Nanosecond),
		from.Add(time.Duration(1 + rng.Int64N(int64(15*time.Minute)))),
		store.MaxEventTime.Add(time.Nanosecond),
	}
}

func sinceCompare(cand store.Store, ref *memstore.Store, written []store.Record, entities []identity.Fingerprint,
	stranger identity.Fingerprint, horizon store.Horizon, rng *rand.Rand, opts Options, cov *sinceCoverage,
) error {
	if len(written) == 0 {
		return nil
	}
	s := sinceRound{cand: cand, ref: ref, idx: newSinceIndex(written), cov: cov, fold: true}
	floor := store.MinEventTime
	if !horizon.IsZero() {
		floor = horizon.Time
	}
	sample := make([]identity.Fingerprint, 0, opts.Entities)
	for range max(1, opts.Entities/2) {
		sample = append(sample, entities[rng.IntN(len(entities))])
	}
	for len(sample) < opts.Entities {
		r := written[len(written)-1-rng.IntN(min(len(written), 50))]
		end := r.Subject.A
		if r.Subject.Kind == store.SubjectEdge && rng.IntN(2) == 0 {
			end = r.Subject.B
		}
		sample = append(sample, end)
	}
	payloads := rng.IntN(2) == 0
	for _, fp := range sample {
		layers := probeLayers(ref, fp, rng)
		for _, dir := range sinceDirs {
			recs := s.idx.visible(fp, dir, store.Current(layers[0]))
			for _, from := range sinceStarts(recs, floor, rng, opts.Probes) {
				tokens := probeTokens(written, rng, horizon.Seq)
				for i, to := range sinceEnds(from, rng) {
					layer := layers[(i+rng.IntN(len(layers)))%len(layers)]
					payloads = !payloads
					a := sinceAsk{
						fps: []identity.Fingerprint{fp}, dir: dir, from: from, to: to,
						sc: store.Scope{Layer: layer, AsOf: tokens[(i+rng.IntN(len(tokens)))%len(tokens)]},
						o:  store.SinceOptions{Payloads: payloads},
					}
					if err := s.ask(a); err != nil {
						return err
					}
				}
			}
		}
	}

	// A batch, with a repeat and an entity nothing is written about, in each layer
	// the sample is in.
	fps := append(slices.Clone(sample), sample[0], stranger)
	rng.Shuffle(len(fps), func(i, j int) { fps[i], fps[j] = fps[j], fps[i] })
	layers := map[catalog.Layer]bool{}
	for _, fp := range sample {
		for _, l := range ref.Layers(fp) {
			layers[l] = true
		}
	}
	for _, l := range allLayers {
		if !layers[l] {
			continue
		}
		from := floor
		if rng.IntN(2) == 0 {
			from = written[rng.IntN(len(written))].EventTime
		}
		if from.Before(floor) {
			from = floor
		}
		for _, dir := range sinceDirs {
			a := sinceAsk{fps: fps, dir: dir, from: from, to: from.Add(10 * time.Minute), sc: store.Current(l), o: store.SinceOptions{Payloads: true}}
			if err := s.ask(a); err != nil {
				return err
			}
		}
	}
	if !horizon.IsZero() {
		return s.refusal(sample[:1], entityLayer(sample[0].Type()), horizon)
	}
	return nil
}

// CheckSinceContract checks WindowSince's refusals, empty cases and the scenarios a
// generated workload rarely reaches, on a fresh store opened with the zero policy:
//
//   - the arguments (an invalid layer or direction, a zero fingerprint among fps)
//     are refused with ErrInvalid before the context and the horizon; a cancelled
//     context with a context error; a window that starts before the horizon, or at
//     it under a token below its Seq, with ErrBeforeHorizon, even with no
//     fingerprints; empty fps give an empty answer, from >= to one empty Since per
//     fingerprint;
//   - the seeds are per reference: of two producers of one subject, the one whose
//     reference holds is a seed though the other deleted later;
//   - several records of one producer at one instant: the highest Seq decides, and
//     an earlier token sees the one it sees;
//   - a run extended past from by a later re-assertion at its start;
//   - a reference whose deadline is from itself is a seed (it holds just before
//     from), and another producer's record at from continues it;
//   - the records at from are the window's, never seeds;
//   - a late record before from decides once its token sees it;
//   - from at the horizon: the references the retention kept are the seeds, one
//     whose deadline is the horizon is not, and no seed before the horizon carries a
//     payload, while one at or after it does when asked;
//   - after Close, every call is refused with ErrClosed, even with invalid
//     arguments.
//
// Each answer is compared with the reference's and with the contract's, under every
// token, with and without payloads, and the fold identity is checked. It closes the
// store at the end.
func CheckSinceContract(st store.Store) error {
	f, err := newSinceFixture()
	if err != nil {
		return err
	}
	if err := f.refusals(st); err != nil {
		return err
	}
	sc, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.Sub(f.x).String() })
	if err != nil {
		return err
	}
	defer sc.close()
	if err := f.scenarios(sc); err != nil {
		return err
	}
	return f.closed(st)
}

// sinceFixture is the small world of CheckSinceContract: a node and three pods
// in L2, two producers, and the instant x everything is timed from.
type sinceFixture struct {
	node, p1, p2, p3 identity.Fingerprint
	x                time.Time
}

func newSinceFixture() (sinceFixture, error) {
	var f sinceFixture
	var err error
	if f.node, err = entityFP(catalog.K8sNode, catalog.K8sNodeUID, "since-node"); err != nil {
		return f, err
	}
	for i, p := range []*identity.Fingerprint{&f.p1, &f.p2, &f.p3} {
		if *p, err = entityFP(catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("since-pod-%d", i+1)); err != nil {
			return f, err
		}
	}
	f.x = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	return f, nil
}

func (f sinceFixture) at(d time.Duration) time.Time { return f.x.Add(d) }

func (f sinceFixture) edge(seq uint64, pod identity.Fingerprint, p lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, f.node, catalog.ScheduledOn), Producer: p,
		EventTime: at, Seq: seq, Kind: kind, TTL: ttl, EventTimeBasis: store.BasisObserved,
	}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "placement %d", seq)
	}
	return r
}

func (f sinceFixture) entity(seq uint64, fp identity.Fingerprint, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: "k8sobjects",
		EventTime: at, Seq: seq, Kind: kind, TTL: ttl, EventTimeBasis: store.BasisObjectField,
	}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "pod %d", seq)
	}
	return r
}

func (f sinceFixture) refusals(s store.Store) error {
	ok := sinceAsk{fps: []identity.Fingerprint{f.node}, dir: store.Reverse, from: f.x, to: f.x.Add(time.Hour), sc: store.Current(catalog.L2)}
	call := func(ctx context.Context, a sinceAsk) ([]store.Since, error) {
		return s.WindowSince(ctx, a.fps, a.dir, a.from, a.to, a.sc, a.o)
	}
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	bad := map[string]sinceAsk{}
	a := ok
	a.sc.Layer = catalog.L3 + 1
	bad["an invalid layer"] = a
	a = ok
	a.dir = store.Reverse + 1
	bad["the direction 3"] = a
	a = ok
	a.fps = []identity.Fingerprint{f.node, {}}
	bad["a zero fingerprint among fps"] = a
	for what, a := range bad {
		if _, err := call(bg, a); !errors.Is(err, store.ErrInvalid) || errors.Is(err, store.ErrBeforeHorizon) {
			return fmt.Errorf("%w: %s with %s: want ErrInvalid, got %w", ErrMismatch, a, what, orNil(err))
		}
		// Arguments come before the context.
		if _, err := call(cancelled, a); !errors.Is(err, store.ErrInvalid) {
			return fmt.Errorf("%w: %s with %s and a cancelled context: want ErrInvalid first, got %w", ErrMismatch, a, what, orNil(err))
		}
	}
	if _, err := call(cancelled, ok); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %s with a cancelled context: want context.Canceled, got %w", ErrMismatch, ok, orNil(err))
	}
	// The empty cases, before any horizon.
	for _, fps := range [][]identity.Fingerprint{nil, {}} {
		a := ok
		a.fps = fps
		got, err := call(bg, a)
		if err != nil || len(got) != 0 {
			return fmt.Errorf("%w: %s with no fingerprints = %d answers, %w; want an empty answer", ErrMismatch, a, len(got), orNil(err))
		}
	}
	return nil
}

// empty checks the empty cases once the store holds records: from >= to gives one
// empty answer per fingerprint, and empty fps an empty answer, which the horizon
// still refuses when from is before it.
func (f sinceFixture) empty(s store.Store, at time.Time, h store.Horizon) error {
	for _, to := range []time.Time{at, at.Add(-time.Hour)} {
		fps := []identity.Fingerprint{f.node, f.node, f.p1}
		got, err := s.WindowSince(bg, fps, store.Reverse, at, to, store.Current(catalog.L2), store.SinceOptions{Payloads: true})
		if err != nil || len(got) != 3 || slices.ContainsFunc(got, func(x store.Since) bool { return len(x.Seeds)+len(x.Window) > 0 }) {
			return fmt.Errorf("%w: WindowSince of 3 fingerprints over [%s, %s) = %v, %w; want three empty answers",
				ErrMismatch, at.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), got, orNil(err))
		}
	}
	if h.IsZero() {
		return nil
	}
	for _, fps := range [][]identity.Fingerprint{nil, {f.node}} {
		if _, err := s.WindowSince(bg, fps, 0, h.Time.Add(-time.Nanosecond), h.Time, store.Current(catalog.L2), store.SinceOptions{}); !errors.Is(err, store.ErrBeforeHorizon) {
			return fmt.Errorf("%w: WindowSince of %d fingerprints from just before the horizon: want ErrBeforeHorizon, got %w", ErrMismatch, len(fps), orNil(err))
		}
	}
	return nil
}

// closed checks that after Close every call is refused with ErrClosed, invalid
// arguments included.
func (f sinceFixture) closed(s store.Store) error {
	if err := s.Close(); err != nil {
		return err
	}
	for _, fps := range [][]identity.Fingerprint{{f.node}, {{}}, nil} {
		if _, err := s.WindowSince(bg, fps, store.Reverse, f.x, f.x.Add(time.Hour), store.Current(catalog.L2), store.SinceOptions{}); !errors.Is(err, store.ErrClosed) {
			return fmt.Errorf("%w: WindowSince of %d fingerprints after Close: want ErrClosed, got %w", ErrMismatch, len(fps), orNil(err))
		}
	}
	return nil
}

// askAll asks a question about fps under every token from 0 to beyond last, with
// and without payloads, in every direction, comparing with the reference and the
// contract and checking the fold identity.
func (f sinceFixture) askAll(sc *scenario, written []store.Record, fps []identity.Fingerprint, from, to time.Time, first, last uint64, cov *sinceCoverage) error {
	s := sinceRound{cand: sc.cand, ref: sc.ref, idx: newSinceIndex(written), cov: cov, fold: true}
	tokens := tokensAround(first, last)
	if !sc.hz.IsZero() {
		tokens = slices.DeleteFunc(tokens, func(t uint64) bool { return t < sc.hz.Seq })
	}
	for _, tok := range tokens {
		for _, dir := range sinceDirs {
			for _, p := range []bool{false, true} {
				a := sinceAsk{fps: fps, dir: dir, from: from, to: to, sc: store.Scope{Layer: catalog.L2, AsOf: tok}, o: store.SinceOptions{Payloads: p}}
				if err := s.ask(a); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f sinceFixture) scenarios(sc *scenario) error {
	var written []store.Record
	write := func(recs ...store.Record) error {
		if err := sc.write(recs...); err != nil {
			return err
		}
		written = append(written, recs...)
		return nil
	}
	var cov sinceCoverage
	m := time.Minute
	seq := uint64(seqBase)
	next := func() uint64 { seq++; return seq }
	first := seq + 1
	fps := []identity.Fingerprint{f.node, f.p1, f.p2, f.p3}

	// p1: two producers. k8sobjects observes with no TTL; kubelet observes with a
	// TTL and deletes later. Per reference, k8sobjects' is a seed after kubelet's
	// delete; per subject, the delete would decide.
	if err := write(
		f.edge(next(), f.p1, "k8sobjects", f.at(0), lifecycle.Observe, 0),
		f.edge(next(), f.p1, "kubelet", f.at(0), lifecycle.Observe, 3*m),
		f.entity(next(), f.p1, f.at(0), lifecycle.Observe, 0),
	); err != nil {
		return err
	}
	if err := write(f.edge(next(), f.p1, "kubelet", f.at(2*m), lifecycle.Delete, 0)); err != nil {
		return err
	}
	// p2: several records of one producer at one instant (an observe, a delete,
	// a re-observe), then a run extended past from by re-asserting it at its
	// start: the run begins at 4m with a TTL of 2m and Through 4m, and is
	// re-asserted with Through 9m, so from 10m sees it hold until 11m only under a
	// token that sees the extension.
	if err := write(
		f.edge(next(), f.p2, "kubelet", f.at(3*m), lifecycle.Observe, 0),
		f.edge(next(), f.p2, "kubelet", f.at(3*m), lifecycle.Delete, 0),
	); err != nil {
		return err
	}
	if err := write(f.edge(next(), f.p2, "kubelet", f.at(3*m), lifecycle.Observe, 0)); err != nil {
		return err
	}
	run := f.edge(next(), f.p2, "k8sobjects", f.at(4*m), lifecycle.Observe, 2*m)
	run.Through = f.at(4 * m)
	if err := write(run); err != nil {
		return err
	}
	run.Seq, run.Through, run.Payload = next(), f.at(9*m), []byte("extended")
	if err := write(run); err != nil {
		return err
	}
	// p3: a deadline at exactly 12m (observe at 9m, TTL 3m), and another
	// producer's record at 12m, which continues it: a window from 12m must see an
	// interval that started before it. A record at 12m by the first producer is the
	// window's, not a seed.
	if err := write(
		f.edge(next(), f.p3, "kubelet", f.at(9*m), lifecycle.Observe, 3*m),
		f.entity(next(), f.p3, f.at(9*m), lifecycle.Observe, 3*m),
	); err != nil {
		return err
	}
	if err := write(
		f.edge(next(), f.p3, "k8sobjects", f.at(12*m), lifecycle.Observe, 0),
		f.entity(next(), f.p3, f.at(12*m), lifecycle.Delete, 0),
	); err != nil {
		return err
	}
	// A late record: written last, timed 8m, it decides p1's kubelet reference for
	// a window from 10m once a token sees it.
	if err := write(f.edge(next(), f.p1, "kubelet", f.at(8*m), lifecycle.Observe, 5*m)); err != nil {
		return err
	}
	last := seq
	for _, from := range []time.Duration{0, 1 * m, 2 * m, 3 * m, 4 * m, 8 * m, 10*m + 1, 11 * m, 12 * m, 13 * m, 20 * m} {
		for _, to := range []time.Duration{from + 1, from + 3*m, time.Hour} {
			if err := f.askAll(sc, written, fps, f.at(from), f.at(to), first, last, &cov); err != nil {
				return err
			}
		}
	}
	if cov.deadlineAtFrom == 0 {
		return errors.New("storetest: the scenarios reached no seed whose deadline is the window's start")
	}

	// The horizon at 12m: p3's kubelet reference ends exactly there and is no seed
	// of a window from it; p1's k8sobjects reference and its late kubelet one, and
	// p2's run, are, without payloads; the records at 12m are the window's.
	if err := sc.retain(f.at(12 * m)); err != nil {
		return err
	}
	if err := write(f.edge(next(), f.p2, "kubelet", f.at(12*m), lifecycle.Delete, 0)); err != nil {
		return err
	}
	last = seq
	for _, from := range []time.Duration{12 * m, 12*m + 1, 13 * m} {
		for _, to := range []time.Duration{from + 1, time.Hour} {
			if err := f.askAll(sc, written, fps, f.at(from), f.at(to), first, last, &cov); err != nil {
				return err
			}
		}
	}
	if cov.deadlineOnHorizon == 0 || cov.atHorizon == 0 || cov.unknown == 0 {
		return fmt.Errorf("storetest: the scenario at the horizon reached too little: %+v", cov)
	}
	s := sinceRound{cand: sc.cand, ref: sc.ref, idx: newSinceIndex(written), cov: &cov}
	if err := s.refusal(fps, catalog.L2, sc.hz); err != nil {
		return err
	}
	return f.empty(sc.cand, f.at(13*m), sc.hz)
}

// readSince is the WindowSince read of [CheckConcurrentReads]: the nodes' reverse
// edges, pods' placements, or pods' own existence, in one batch, which must be one
// committed world as a whole. A quarter of them are pinned to LastSeq and must be
// the reference's at exactly that token. The reference is never retained, so the
// windows start a nanosecond after a whole second, where no horizon of the check
// falls, and ask no payloads: after the horizon the seeds then do not depend on the
// retention.
func (c *concurrent) readSince(rng *rand.Rand) error {
	a := sinceAsk{sc: store.Current(catalog.L2)}
	moving := c.hot.Load()
	pods := c.pods[:min(len(c.pods), 8)]
	if moving != nil {
		pods = append(slices.Clone(pods), *moving...)
	}
	switch rng.IntN(3) {
	case 0:
		a.dir, a.fps = store.Reverse, append(slices.Clone(c.nodes), c.nodes[0])
	case 1:
		a.dir, a.fps = store.Forward, slices.Clone(pods)
	default:
		a.dir, a.fps = 0, slices.Clone(pods)
		a.sc.Layer = entityLayer(catalog.K8sPod)
	}
	rng.Shuffle(len(a.fps), func(i, j int) { a.fps[i], a.fps[j] = a.fps[j], a.fps[i] })
	a.from = c.cfg.Start.Add(time.Duration(rng.IntN(int(c.cfg.Duration/time.Second)+600)) * time.Second)
	if now := c.clock.Load(); moving != nil && now != 0 {
		a.from = time.Unix(0, now)
	}
	a.from = a.from.Add(time.Nanosecond)
	a.to = a.from.Add(30 * time.Minute)
	ask := func(s store.Store, a sinceAsk) ([]store.Since, error) {
		return s.WindowSince(bg, a.fps, a.dir, a.from, a.to, a.sc, a.o)
	}

	if rng.IntN(4) == 0 {
		announced := c.announced.Load()
		a.sc.AsOf = c.cand.LastSeq()
		got, err := ask(c.cand, a)
		if err != nil {
			if errors.Is(err, store.ErrBeforeHorizon) && (c.before(a.from) || c.announced.Load() > announced) {
				return nil
			}
			return fmt.Errorf("concurrent pinned %s: %w", a, err)
		}
		want, err := ask(c.ref, a)
		if err != nil {
			return fmt.Errorf("reference: %w", err)
		}
		if msg := sinceDiff(got, want); msg != "" {
			return fmt.Errorf("%w: %s pinned to token %d: %s", ErrTorn, a, a.sc.AsOf, msg)
		}
		return nil
	}
	before := c.cand.LastSeq()
	got, err := ask(c.cand, a)
	after := c.cand.LastSeq()
	if err != nil {
		if errors.Is(err, store.ErrBeforeHorizon) && c.before(a.from) {
			return nil // a retention may have taken what was asked about
		}
		return fmt.Errorf("concurrent %s: %w", a, err)
	}
	candidates := c.worlds(before, after)
	for _, x := range candidates {
		w := a
		w.sc.AsOf = x
		want, err := ask(c.ref, w)
		if err != nil {
			return fmt.Errorf("reference: %w", err)
		}
		if sinceDiff(got, want) == "" {
			return nil
		}
	}
	return fmt.Errorf("%w: %s between tokens %d and %d (boundaries %v) matches none of them", ErrTorn, a, before, after, candidates)
}
