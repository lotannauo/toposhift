package sequencer_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// ---- the reads ----

// dstReads are the answers of one store at the grid instants: for each subject
// the world ever had, whether it is alive, and its neighbors (a node's reverse
// ones, a pod's and a container's forward ones), at the latest token.
type dstReads struct {
	at    []time.Time
	alive [][]bool
	neigh [][][]store.Neighbor
}

func dstKey(n store.Neighbor) string { return string(n.Relation) + ":" + n.Peer.String() }

func dstReadGrid(t testing.TB, mem *memstore.Store, w *dstWorld) *dstReads {
	t.Helper()
	ctx := context.Background()
	scope := store.Current(catalog.L2)
	g := &dstReads{at: dstGridInstants()}
	for _, tm := range g.at {
		alive := make([]bool, len(w.subs))
		neigh := make([][]store.Neighbor, len(w.subs))
		for i, sub := range w.subs {
			var err error
			if alive[i], err = mem.Alive(ctx, sub.fp, tm, scope); err != nil {
				t.Fatalf("Alive of %s at %s: %v", sub.name, tm.Sub(t0), err)
			}
			dir := store.Forward
			if sub.kind == "node" {
				dir = store.Reverse
			}
			ns, err := mem.Neighbors(ctx, sub.fp, dir, tm, scope)
			if err != nil {
				t.Fatalf("Neighbors of %s at %s: %v", sub.name, tm.Sub(t0), err)
			}
			slices.SortFunc(ns, func(a, b store.Neighbor) int { return strings.Compare(dstKey(a), dstKey(b)) })
			neigh[i] = ns
		}
		g.alive = append(g.alive, alive)
		g.neigh = append(g.neigh, neigh)
	}
	return g
}

// lines are the grid's answers as text, for the digest.
func (g *dstReads) lines(w *dstWorld) []string {
	var out []string
	for j, tm := range g.at {
		for i, sub := range w.subs {
			var keys []string
			for _, n := range g.neigh[j][i] {
				keys = append(keys, dstKey(n))
			}
			out = append(out, fmt.Sprintf("%s %s alive %v neighbors %v", tm.Sub(t0), sub.name, g.alive[j][i], keys))
		}
	}
	return out
}

func dstKeys(ns []store.Neighbor) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = dstKey(n)
	}
	return out
}

// dstMinus lists what a has that b has not; both are sorted.
func dstMinus(a, b []string) []string {
	var out []string
	for _, x := range a {
		if _, found := slices.BinarySearch(b, x); !found {
			out = append(out, x)
		}
	}
	return out
}

// ---- I2: no phantom ----

func dstAbs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// dstPlausible says a subject or edge the truth holds may be alive at t: not
// before it was born (less the producer's clock offset), and not later than a
// deletion no producer reported can leave it, which is the TTL, one pull and one
// tick.
func dstPlausible(tr dstTruth, t time.Time, k time.Duration) bool {
	if t.Before(tr.born.Add(-dstAbs(k))) {
		return false
	}
	return tr.died.IsZero() || t.Before(tr.died.Add(dstTTL+dstPullEvery+dstStep))
}

func dstNoPhantom(f *dstFailures, who string, w *dstWorld, g *dstReads) {
	for j, tm := range g.at {
		for i, sub := range w.subs {
			if g.alive[j][i] && !dstPlausible(sub.dstTruth, tm, w.k) {
				f.add("I2", "%s: %s %s is alive at %s, but it was born at %s and died at %s", who, sub.kind, sub.name, tm.Sub(t0), sub.born.Sub(t0), dstShowDied(sub.died))
			}
			for _, n := range g.neigh[j][i] {
				a, b := sub.fp, n.Peer
				if sub.kind == "node" {
					a, b = n.Peer, sub.fp
				}
				tr, ok := w.edgeOf(a, b, n.Relation)
				if !ok || !dstPlausible(tr, tm, w.k) {
					f.add("I2", "%s: %s %s has the neighbor %s at %s, the edge was born at %s and died at %s (known %v)", who, sub.kind, sub.name,
						dstKey(n), tm.Sub(t0), tr.born.Sub(t0), dstShowDied(tr.died), ok)
				}
			}
		}
	}
}

func dstShowDied(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return fmt.Sprint(t.Sub(t0))
}

// ---- D1, D2: the faulted run against the control ----

// dstWindow is a stretch in which the faulted run may differ from the control in
// one direction: extra says it may hold more, missing less.
type dstWindow struct {
	class          dstClass
	from, to       time.Time
	extra, missing bool
}

func dstWindows(faults []dstFault) []dstWindow {
	var out []dstWindow
	for _, f := range faults {
		switch f.class {
		case dstW1:
			// The first sightings after the silence are timed at their creation, which
			// for a pod created just before it is earlier than the silence.
			out = append(out, dstWindow{class: f.class, from: f.start.Add(-2 * dstPullEvery), to: f.start.Add(f.dur + dstPullEvery), extra: true})
		case dstW2:
			if f.fired {
				out = append(out, dstWindow{class: f.class, from: f.start.Add(-dstTTL), to: f.firedAt.Add(dstPullEvery), missing: true})
			}
		case dstW3:
			if f.fired {
				out = append(out, dstWindow{class: f.class, from: f.firedAt, to: f.firedAt.Add(20 * time.Minute), missing: true})
			}
		}
	}
	return out
}

// dstResets are the instants the translators lost their state: a restart, a crash,
// a refused or failed write, a panic, an abort, and the end of a silence that frees
// the slot.
func dstResets(faults []dstFault) []time.Time {
	var out []time.Time
	for _, f := range faults {
		switch {
		case f.class == dstW1:
			out = append(out, f.start.Add(f.dur))
		case f.class.resetsTranslators() && f.fired:
			out = append(out, f.firedAt)
		}
	}
	return out
}

// dstNearReset says the instant is within a TTL before a reset, or a TTL and a pull
// after it: where a subject first seen again after the reset may be timed apart from
// a run that never lost its state.
func dstNearReset(resets []time.Time, tm time.Time) bool {
	return slices.ContainsFunc(resets, func(r time.Time) bool {
		return !tm.Before(r.Add(-dstTTL)) && !tm.After(r.Add(dstTTL+dstPullEvery))
	})
}

// dstCompare compares the run's answers with the control's, and returns how many
// answers it compared, how many differed inside windows, and how many differed in
// a way that is explained.
//
// A subject or an edge that the cluster holds at the instant and that the control
// holds, and the run does not, is a loss (D1), unless the instant is inside a
// window that allows a loss (D2). What the run holds beyond the control is
// explained only if the cluster no longer holds it (the tail of a deletion, which
// lapses at the TTL, differs with what the translators knew), or if the instant is
// near a reset (after a restart the first record of an object is timed at its
// creation, earlier than a control that saw its container appear later), or inside
// a window that allows more. Every other extra is judged as D1.
func dstCompare(f *dstFailures, w *dstWorld, run, ctl *dstReads, wins []dstWindow, resets []time.Time, judged bool) (compared, inside, explained int) {
	for j, tm := range run.at {
		var in, missing, extra bool
		for _, wd := range wins {
			if !tm.Before(wd.from) && !tm.After(wd.to) {
				in = true
				missing = missing || wd.missing
				extra = extra || wd.extra
			}
		}
		near := extra || dstNearReset(resets, tm)
		check := "D1"
		if in {
			check = "D2"
		}
		for i, sub := range w.subs {
			compared += 2
			differ := false
			if ra, ca := run.alive[j][i], ctl.alive[j][i]; ra != ca {
				differ = true
				switch {
				case ra && (near || !sub.existsAt(tm)), !ra && !sub.existsAt(tm):
					explained++
				case judged && ((ra && !near) || (!ra && !missing)):
					f.add(check, "%s %s at %s: alive %v in the run, %v in the control, %v in the cluster", sub.kind, sub.name, tm.Sub(t0), ra, ca, sub.existsAt(tm))
				}
			}
			rk, ck := dstKeys(run.neigh[j][i]), dstKeys(ctl.neigh[j][i])
			for _, more := range dstMinus(rk, ck) {
				differ = true
				if near || !dstEdgeAlive(w, sub, run.neigh[j][i], more, tm) {
					explained++
				} else if judged {
					f.add(check, "%s %s at %s: the run holds the neighbor %s beyond the control, which has %v, outside a reset", sub.kind, sub.name, tm.Sub(t0), more, ck)
				}
			}
			for _, lost := range dstMinus(ck, rk) {
				differ = true
				if !dstEdgeAlive(w, sub, ctl.neigh[j][i], lost, tm) {
					explained++
				} else if !missing && judged {
					f.add(check, "%s %s at %s: the neighbor %s is missing from the run, which has %v, and is held by the control and by the cluster", sub.kind, sub.name, tm.Sub(t0), lost, rk)
				}
			}
			if differ && in {
				inside++
			}
		}
	}
	return compared, inside, explained
}

// dstEdgeAlive says the cluster holds the edge the neighbor key names, from the
// subject, at the instant.
func dstEdgeAlive(w *dstWorld, sub dstSub, ns []store.Neighbor, key string, tm time.Time) bool {
	for _, n := range ns {
		if dstKey(n) != key {
			continue
		}
		a, b := sub.fp, n.Peer
		if sub.kind == "node" {
			a, b = n.Peer, sub.fp
		}
		tr, ok := w.edgeOf(a, b, n.Relation)
		return ok && tr.existsAt(tm)
	}
	return false
}

var dstPhase = regexp.MustCompile(`phase":"([A-Za-z]+)"`)

// dstPhases compares each pod's phase, from the fold of its records, at every
// instant outside the windows where both runs hold the pod. A description that
// resume raised to a stored time when it had changed would show here.
func dstPhases(f *dstFailures, w *dstWorld, runRecs, ctlRecs []store.Record, grid []time.Time, wins []dstWindow, resets []time.Time) (compared int) {
	run, ctl := dstBySubject(runRecs), dstBySubject(ctlRecs)
	for _, sub := range w.subs {
		if sub.kind != "pod" {
			continue
		}
		es := store.EntitySubject(sub.fp)
		rt, rerr := dstTimeline(run[es])
		ct, cerr := dstTimeline(ctl[es])
		if rerr != nil || cerr != nil {
			f.add("D1", "folding the records of pod %s: %v, %v", sub.name, rerr, cerr)
			continue
		}
		for _, tm := range grid {
			if slices.ContainsFunc(wins, func(wd dstWindow) bool { return !tm.Before(wd.from) && !tm.After(wd.to) }) || dstNearReset(resets, tm) {
				continue
			}
			rd, rok := rt.DescribeAt(tm)
			cd, cok := ct.DescribeAt(tm)
			if !rok || !cok {
				continue
			}
			compared++
			if rp, cp := dstPhaseOf(rd), dstPhaseOf(cd); rp != cp {
				f.add("D1", "pod %s at %s: the run describes the phase %q, the control %q", sub.name, tm.Sub(t0), rp, cp)
			}
		}
	}
	return compared
}

func dstTimeline(recs []store.Record) (lifecycle.Timeline, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	return lifecycle.Fold(as, lifecycle.Policy{})
}

func dstPhaseOf(d lifecycle.Description) string {
	var out string
	for _, k := range slices.Sorted(maps.Keys(d)) {
		if m := dstPhase.FindStringSubmatch(fmt.Sprint(d[k].Value)); m != nil {
			out = m[1]
		}
	}
	return out
}

// ---- I4: the counters ----

func dstCheckCounters(f *dstFailures, r *dstRun, st resume.Stats) {
	if st.UnknownExhausted != 0 || st.UnknownBaseline != 0 || st.UnknownBelowHorizon != 0 {
		f.add("I4", "the resume step counted unknown answers it cannot meet here: %v", counters(st))
	}
	if st.Unknown != st.UnknownError {
		f.add("I4", "unknown %d is not the unknown for a read error %d", st.Unknown, st.UnknownError)
	}
	if st.UnknownError > 0 && !r.w2Fired {
		f.add("I4", "%d records fell back for a read error, and no read fault took effect", st.UnknownError)
	}
	if r.w2Hits > 0 && st.UnknownError == 0 {
		f.add("I4", "%d reads failed in a group, and none was counted as unknown for a read error", r.w2Hits)
	}
	if st.Dropped != 0 && !r.report {
		f.add("I4", "the resume step dropped %d records", st.Dropped)
	}
}

// ---- I5: lateness, a proxy for the checkpoints a late record deletes ----

type dstPrefix struct {
	fp   identity.Fingerprint
	kind int // 0 entity, 1 forward end of an edge, 2 reverse end
}

func dstPrefixes(rec store.Record) []dstPrefix {
	if rec.Subject.Kind == store.SubjectEntity {
		return []dstPrefix{{rec.Subject.A, 0}}
	}
	return []dstPrefix{{rec.Subject.A, 1}, {rec.Subject.B, 2}}
}

// dstLate is how late the records written after the warm-up were: each record's
// lateness on a prefix is the newest event time earlier writes committed there,
// less its own.
type dstLate struct {
	max        time.Duration
	over0      int
	overFive   int
	maxExample string
}

func dstLateness(batches [][]store.Record, at map[uint64]time.Time) dstLate {
	var out dstLate
	newest := map[dstPrefix]time.Time{}
	for _, b := range batches {
		if len(b) == 0 {
			continue
		}
		counted := !at[b[0].Seq].Before(t0.Add(dstWarmup))
		for _, rec := range b {
			for _, p := range dstPrefixes(rec) {
				late := newest[p].Sub(rec.EventTime)
				if late <= 0 || !counted {
					continue
				}
				out.over0++
				if late > 5*time.Minute {
					out.overFive++
				}
				if late > out.max {
					out.max, out.maxExample = late, show(rec)
				}
			}
		}
		for _, rec := range b {
			for _, p := range dstPrefixes(rec) {
				if rec.EventTime.After(newest[p]) {
					newest[p] = rec.EventTime
				}
			}
		}
	}
	return out
}

// dstCheckLateness judges the largest lateness of the run: the control's, or five
// minutes, whichever is more, and a pull interval for where a flush or a restart
// falls in the cycle. A silence that frees the slot is bridged by a record timed at
// the stored last assertion, which is late by the silence.
func dstCheckLateness(f *dstFailures, sd *dstSchedule, run, ctl dstLate) {
	bound := max(ctl.max, 5*time.Minute) + dstPullEvery
	for _, fl := range sd.faults {
		if fl.class == dstW1 {
			bound = max(bound, fl.dur+dstPullEvery+dstPullEvery)
		}
	}
	if run.max > bound {
		f.add("I5", "a record was written %s late (the control's latest %s, bound %s): %s", run.max, ctl.max, bound, run.maxExample)
	}
}

// ---- I6: deletes are true; an extra absence is a bounded liveness expiry ----

func dstBySubject(recs []store.Record) map[store.Subject][]store.Record {
	m := map[store.Subject][]store.Record{}
	for _, r := range recs {
		m[r.Subject] = append(m[r.Subject], r)
	}
	return m
}

func dstExistence(recs []store.Record) ([]lifecycle.Interval, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, err
	}
	return tl.Existence(), nil
}

// dstMaxAbsence is one extension interval and one pull interval.
const dstMaxAbsence = dstTTL/2 + dstPullEvery

func dstCheckDeletes(f *dstFailures, w *dstWorld, runRecs, ctlRecs []store.Record, wins []dstWindow) (absence time.Duration) {
	for _, rec := range runRecs {
		if rec.Kind != lifecycle.Delete {
			continue
		}
		var tr dstTruth
		var ok bool
		if rec.Subject.Kind == store.SubjectEntity {
			var i int
			if i, ok = w.bySub[rec.Subject.A]; ok {
				tr = w.subs[i].dstTruth
			}
		} else {
			tr, ok = w.edgeOf(rec.Subject.A, rec.Subject.B, rec.Subject.Relation)
		}
		if !ok || tr.died.IsZero() || rec.EventTime.Before(tr.died.Add(-dstAbs(w.k))) {
			f.add("I6", "a Delete was written for %s, which the cluster deleted at %s (known %v)", show(rec), dstShowDied(tr.died), ok)
		}
	}
	var w3 []dstWindow
	for _, wd := range wins {
		if wd.class == dstW3 {
			w3 = append(w3, wd)
		}
	}
	if len(w3) == 0 {
		return 0
	}
	run, ctl := dstBySubject(runRecs), dstBySubject(ctlRecs)
	subjects := make([]store.Subject, 0, len(run))
	for s := range run {
		subjects = append(subjects, s)
	}
	slices.SortFunc(subjects, func(a, b store.Subject) int {
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	})
	for _, s := range subjects {
		ri, err := dstExistence(run[s])
		if err != nil {
			f.add("I6", "folding the run's records of %v: %v", s, err)
			continue
		}
		ci, err := dstExistence(ctl[s])
		if err != nil {
			f.add("I6", "folding the control's records of %v: %v", s, err)
			continue
		}
		for n, r := range ri {
			if r.Open() || !slices.ContainsFunc(w3, func(wd dstWindow) bool { return !r.End.Before(wd.from) && !r.End.After(wd.to) }) {
				continue
			}
			k := slices.IndexFunc(ci, func(c lifecycle.Interval) bool {
				return !c.Start.After(r.End) && (c.Open() || c.End.After(r.End))
			})
			if k < 0 {
				continue
			}
			// The run's interval ended while the control's went on.
			if r.EndSource != lifecycle.EndLivenessExpiry {
				f.add("I6", "%v: the run's existence ended at %s by %s while the control's went on", s, r.End.Sub(t0), r.EndSource)
				continue
			}
			until := ci[k].End
			if n+1 < len(ri) && (until.IsZero() || ri[n+1].Start.Before(until)) {
				until = ri[n+1].Start
			}
			absence = max(absence, until.Sub(r.End))
			if until.IsZero() || until.Sub(r.End) > dstMaxAbsence {
				f.add("I6", "%v: absent from %s to %s, more than %s", s, r.End.Sub(t0), dstShowDied(until), dstMaxAbsence)
			}
		}
	}
	return absence
}

// ---- the digest ----

func dstRecLine(r store.Record) string {
	return fmt.Sprintf("%d|%v|%s|%s|%s|%d|%s|%s|%s|%s|%s|%s", r.Layer, r.Subject, r.Producer, r.EventTime.Format(time.RFC3339Nano), r.Kind, r.Seq,
		r.TTL, r.Through.Format(time.RFC3339Nano), r.Payload, r.Boot, r.EventTimeBasis, r.Subject.Relation)
}

func dstDigest(lines []string) string {
	h := sha256.New()
	for _, l := range lines {
		h.Write([]byte(l))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// dstFirstDifference names the first line two runs of one seed do not share.
func dstFirstDifference(a, b []string) string {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return fmt.Sprintf("line %d:\n first  %s\n second %s", i, a[i], b[i])
		}
	}
	return fmt.Sprintf("%d lines, then %d", len(a), len(b))
}
