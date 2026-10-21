package runner

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"text/tabwriter"
)

// The classes of a batch of the phase log: the gate's own (see [GateTrace]).
const (
	PhaseOutside  = "outside"
	PhaseSettling = "settling"
	PhaseInside   = "inside"
	PhaseUnpaced  = "unpaced"
	PhaseExcluded = "excluded"
)

// ClassifyPhaseBatch places a batch that began at startNs and took wallNs on the
// gate's clock, as the gate does while the build runs (gateTracker.batch), but
// from the windows the manifest holds when the build is over: a batch inside a
// window overlaps its publication to the end of its rewrite, one settling overlaps a
// settle and no window, and the rest are outside. In a paced build (PaceStartNs
// above 0) a batch that starts before the pacing is unpaced, and one that overlaps a
// window that started before it, or that window's settle, is excluded.
func ClassifyPhaseBatch(t *GateTrace, startNs, wallNs int64) string {
	s, e := startNs, startNs+wallNs
	paced := t.PaceStartNs != 0
	if paced && s < t.PaceStartNs {
		return PhaseUnpaced
	}
	in, settling, excluded := false, false, false
	for i := len(t.Windows) - 1; i >= 0; i-- {
		w := t.Windows[i]
		if w.SettleEndNs < s { // the windows are in order: the earlier ones end earlier
			break
		}
		unjudged := paced && w.StartNs < t.PaceStartNs
		switch {
		case w.StartNs <= e && s <= w.RewriteEndNs:
			if unjudged {
				excluded = true
				break
			}
			in = true
		case s <= w.SettleEndNs && e >= w.RewriteEndNs:
			if unjudged {
				excluded = true
				break
			}
			settling = true
		}
	}
	switch {
	case excluded:
		return PhaseExcluded
	case in:
		return PhaseInside
	case settling:
		return PhaseSettling
	}
	return PhaseOutside
}

// phaseColumn is a figure of a batch that the report sums up: one of the store's
// phases, a sum of them, or the batch's wall time.
type phaseColumn struct {
	name string
	// note says how it relates to the columns above it in the table.
	note string
	get  func(PhaseRecord) int64
}

// phaseColumns are the columns of the report, in its order. "wall" is the
// engine's Write as the gate times it; the rows from validate to other add up to it;
// the rows after them are sums and parts of those.
var phaseColumns = []phaseColumn{
	{"wall", "the engine's Write, as the gate times it", func(r PhaseRecord) int64 { return r.WallNs }},
	{"validate", "", func(r PhaseRecord) int64 { return r.Validate }},
	{"state", "state reads (cold ones included), under the lock", func(r PhaseRecord) int64 { return r.State }},
	{"invalidate", "", func(r PhaseRecord) int64 { return r.Invalidate }},
	{"record_commit", "the batch's own (synced) commit", func(r PhaseRecord) int64 { return r.RecordCommit }},
	{"ckpt_build", "decision, learning checkpoints, walks", func(r PhaseRecord) int64 { return r.CkptBuild }},
	{"ckpt_commit", "the second commit (unsynced)", func(r PhaseRecord) int64 { return r.CkptCommit }},
	{"other", "lock wait + keys + engine, not sampled", func(r PhaseRecord) int64 { return r.Other() }},
	{"= build+commit", "ckpt_build + ckpt_commit: the checkpoint build and the second commit", func(r PhaseRecord) int64 { return r.CkptBuild + r.CkptCommit }},
	{"= walk+commit", "the walks + the second commit: what a builder would move", func(r PhaseRecord) int64 { return r.Walk + r.CkptCommit }},
	{"  of which walk", "the walks alone (counter checkpoint.build_ns)", func(r PhaseRecord) int64 { return r.Walk }},
}

// PhaseSpread is the p50, p99, maximum and mean of one column over a class.
type PhaseSpread struct {
	P50, P99, Max int64
	Mean          float64
}

// PhaseTailRow is what one column contributes to the tail of a class: the batches
// whose wall time is at or above the class's 99th percentile.
type PhaseTailRow struct {
	// Share is the column's time in the tail over the tail's wall time: the share
	// of the time of the batches above the p99 that the column accounts for (as the
	// store gate attributes a tail).
	Share float64
	// ExcessShare is the part of the tail's excess over the other batches that the
	// column accounts for: (the column's mean in the tail less its mean outside the
	// tail) over (the same of the wall time). Over the rows that add up to the wall
	// time they add up to 1; NaN if the tail has no excess.
	ExcessShare float64
	// MedianShare is the median, over the tail batches, of the column's share of the
	// batch's wall time: a figure that one batch with a stall of seconds cannot move,
	// as it moves Share.
	MedianShare float64
	// Dominant counts the tail batches in which the column is the largest of the
	// eight rows that add up to the wall time (only those rows are counted).
	Dominant int
}

// PhaseClassReport is the report of one class of batches.
type PhaseClassReport struct {
	Class   string
	Batches int
	Records int64
	// Spread and Tail have one entry for each of [phaseColumns], in its order.
	Spread []PhaseSpread
	Tail   []PhaseTailRow
	// TailBatches is the number of batches at or above the p99 of the wall time,
	// and TailFrom the wall time that begins it.
	TailBatches int
	TailFrom    int64
	// Halves is the share of the tail time that the columns "= build+commit" and
	// "= walk+commit" account for when the class's batches are cut in two at their
	// middle in time and each half judged on its own, to show how steady the figure
	// is: first half, then second half. NaN where a half has no tail.
	Halves [2][2]float64
	// HalfTail is each half's number of tail batches.
	HalfTail [2]int
}

// PhaseReport is the report of a phase log.
type PhaseReport struct {
	Total int
	// TailQuantile is where the tail of each class begins: 0.99 unless asked.
	TailQuantile float64
	Classes      []PhaseClassReport
	// Counts is how many batches fell in each class.
	Counts map[string]int
	// Totals are the sums of the log's counters over all its batches.
	Totals PhaseRecord
}

// phaseColumnIndex is the index in phaseColumns of the column with that name.
func phaseColumnIndex(name string) int {
	for i, c := range phaseColumns {
		if c.name == name {
			return i
		}
	}
	panic("runner: no phase column " + name)
}

// AnalyzePhases classifies the batches of the log by the gate's windows and sums up
// each class that is in the log, outside first.
func AnalyzePhases(recs []PhaseRecord, trace *GateTrace) *PhaseReport {
	return AnalyzePhasesAt(recs, trace, 0.99)
}

// AnalyzePhasesAt is [AnalyzePhases] with the tail beginning at the q-quantile of the
// wall time instead of the 99th percentile (a lower q gives a tail of more batches,
// to see whether a figure of the p99 tail, which is few batches, holds).
func AnalyzePhasesAt(recs []PhaseRecord, trace *GateTrace, q float64) *PhaseReport {
	rep := &PhaseReport{Total: len(recs), Counts: map[string]int{}, TailQuantile: q}
	by := map[string][]PhaseRecord{}
	for _, r := range recs {
		c := ClassifyPhaseBatch(trace, r.StartNs, r.WallNs)
		rep.Counts[c]++
		by[c] = append(by[c], r)
		rep.Totals.Walk += r.Walk
		rep.Totals.CkptWritten += r.CkptWritten
		rep.Totals.StateLoads += r.StateLoads
		rep.Totals.WalkedRecords += r.WalkedRecords
		rep.Totals.Iterators += r.Iterators
		rep.Totals.Records += r.Records
	}
	for _, c := range []string{PhaseOutside, PhaseSettling, PhaseInside, PhaseUnpaced, PhaseExcluded} {
		if len(by[c]) > 0 {
			rep.Classes = append(rep.Classes, analyzeClass(c, by[c], q))
		}
	}
	return rep
}

// Class returns the report of a class, or nil if no batch fell in it.
func (r *PhaseReport) Class(name string) *PhaseClassReport {
	for i := range r.Classes {
		if r.Classes[i].Class == name {
			return &r.Classes[i]
		}
	}
	return nil
}

// quantileRank is the value at nearest rank q of the sorted values: the smallest
// value that at least a share q of them are at or below.
func quantileRank(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1]
}

// quantileRankOf is the q-quantile of the wall times of recs.
func quantileRankOf(recs []PhaseRecord, q float64) int64 {
	walls := make([]int64, len(recs))
	for i, r := range recs {
		walls[i] = r.WallNs
	}
	slices.Sort(walls)
	return quantileRank(walls, q)
}

func analyzeClass(class string, recs []PhaseRecord, q float64) PhaseClassReport {
	rep := PhaseClassReport{Class: class, Batches: len(recs)}
	for _, r := range recs {
		rep.Records += int64(r.Records)
	}
	rep.Spread = make([]PhaseSpread, len(phaseColumns))
	for i, c := range phaseColumns {
		vals := make([]int64, len(recs))
		var sum int64
		for j, r := range recs {
			vals[j] = c.get(r)
			sum += vals[j]
		}
		slices.Sort(vals)
		rep.Spread[i] = PhaseSpread{P50: quantileRank(vals, 0.5), P99: quantileRank(vals, 0.99), Max: vals[len(vals)-1], Mean: float64(sum) / float64(len(vals))}
	}
	rep.TailFrom = quantileRankOf(recs, q)
	rep.Tail, rep.TailBatches = tailRows(recs, rep.TailFrom)

	bc, wc := phaseColumnIndex("= build+commit"), phaseColumnIndex("= walk+commit")
	// Halves in time: the log is in order of the build's clock.
	mid := len(recs) / 2
	for h, half := range [][]PhaseRecord{recs[:mid], recs[mid:]} {
		rep.Halves[h] = [2]float64{math.NaN(), math.NaN()}
		if len(half) == 0 {
			continue
		}
		walls := make([]int64, len(half))
		for i, r := range half {
			walls[i] = r.WallNs
		}
		slices.Sort(walls)
		rows, n := tailRows(half, quantileRank(walls, q))
		rep.HalfTail[h] = n
		rep.Halves[h] = [2]float64{rows[bc].Share, rows[wc].Share}
	}
	return rep
}

// tailRows attributes the batches at or above from (a wall time) to the columns.
func tailRows(recs []PhaseRecord, from int64) ([]PhaseTailRow, int) {
	var tail, rest []PhaseRecord
	for _, r := range recs {
		if r.WallNs >= from {
			tail = append(tail, r)
		} else {
			rest = append(rest, r)
		}
	}
	rows := make([]PhaseTailRow, len(phaseColumns))
	mean := func(rs []PhaseRecord, c phaseColumn) float64 {
		if len(rs) == 0 {
			return 0
		}
		var s int64
		for _, r := range rs {
			s += c.get(r)
		}
		return float64(s) / float64(len(rs))
	}
	tailWall := mean(tail, phaseColumns[0]) * float64(len(tail))
	excessWall := mean(tail, phaseColumns[0]) - mean(rest, phaseColumns[0])
	for i, c := range phaseColumns {
		rows[i].Share = math.NaN()
		if tailWall > 0 {
			rows[i].Share = mean(tail, c) * float64(len(tail)) / tailWall
		}
		rows[i].ExcessShare = math.NaN()
		if len(rest) > 0 && excessWall > 0 {
			rows[i].ExcessShare = (mean(tail, c) - mean(rest, c)) / excessWall
		}
	}
	for i, c := range phaseColumns {
		ratios := make([]float64, 0, len(tail))
		for _, r := range tail {
			if w := r.WallNs; w > 0 {
				ratios = append(ratios, float64(c.get(r))/float64(w))
			}
		}
		rows[i].MedianShare = math.NaN()
		if len(ratios) > 0 {
			slices.Sort(ratios)
			n := len(ratios)
			rows[i].MedianShare = (ratios[(n-1)/2] + ratios[n/2]) / 2
		}
	}
	// The eight rows that add up to the wall time: validate to other.
	first, last := 1, phaseColumnIndex("other")
	for _, r := range tail {
		best, bestV := -1, int64(-1)
		for i := first; i <= last; i++ {
			if v := phaseColumns[i].get(r); v > bestV {
				best, bestV = i, v
			}
		}
		if best >= 0 {
			rows[best].Dominant++
		}
	}
	return rows, len(tail)
}

func ms(ns int64) string { return fmt.Sprintf("%.3f", float64(ns)/1e6) }

func pct(f float64) string {
	if math.IsNaN(f) {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*f)
}

// Format writes the report as text: per class a table of the p50, p99, maximum and
// mean of each column in milliseconds, then the same columns' share of the tail.
// Only classes with at least one batch are written; the tail is the batches at or
// above the class's p99 (or the quantile asked for).
func (r *PhaseReport) Format(w io.Writer, only string) {
	fmt.Fprintf(w, "batches: %d (", r.Total)
	var parts []string
	for _, c := range []string{PhaseOutside, PhaseSettling, PhaseInside, PhaseUnpaced, PhaseExcluded} {
		if n := r.Counts[c]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", c, n))
		}
	}
	fmt.Fprintf(w, "%s)\n", strings.Join(parts, ", "))
	fmt.Fprintf(w, "counters over the whole log: %d records, %d checkpoints written, %d state loads (an upper bound in a background build), %d records walked by builds, %d iterators opened by writes\n",
		r.Totals.Records, r.Totals.CkptWritten, r.Totals.StateLoads, r.Totals.WalkedRecords, r.Totals.Iterators)
	for _, c := range r.Classes {
		if only != "" && only != c.Class {
			continue
		}
		fmt.Fprintf(w, "\nclass %s: %d batches, %d records; tail = the %d batches with wall >= %s ms (the p%g)\n", c.Class, c.Batches, c.Records, c.TailBatches, ms(c.TailFrom), 100*r.TailQuantile)
		tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "phase\tp50 ms\tp99 ms\tmax ms\tmean ms\tshare of tail time\tshare of tail excess\tmedian batch share\ttail batches led\tnote")
		for i, col := range phaseColumns {
			sp, tr := c.Spread[i], c.Tail[i]
			led := ""
			if i >= 1 && i <= phaseColumnIndex("other") {
				led = fmt.Sprint(tr.Dominant)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.3f\t%s\t%s\t%s\t%s\t%s\n", col.name, ms(sp.P50), ms(sp.P99), ms(sp.Max), sp.Mean/1e6, pct(tr.Share), pct(tr.ExcessShare), pct(tr.MedianShare), led, col.note)
		}
		_ = tw.Flush()
		fmt.Fprintf(w, "steadiness (share of tail time, first half | second half of the class in time; tail batches %d | %d): build+commit %s | %s, walk+commit %s | %s\n",
			c.HalfTail[0], c.HalfTail[1], pct(c.Halves[0][0]), pct(c.Halves[1][0]), pct(c.Halves[0][1]), pct(c.Halves[1][1]))
	}
}
