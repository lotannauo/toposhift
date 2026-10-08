package runner

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// GateVerdict is how one candidate does on one of the gates that are not G1: the
// value it measured, the limit the rules put on it, and whether it is within.
type GateVerdict struct {
	Candidate, Gate string
	Value, Limit    string
	Pass            bool
	// Local says the value is a timing taken on this machine, which informs and does
	// not decide: a result is a timing on CI hardware.
	Local bool
	// NotComparable says the value is shown and not judged: the first batch after a
	// retention of a build that rested after it (see [BuildOptions]) does not carry
	// the catch-up of the compactions, so it is not comparable with one that did not.
	NotComparable bool
	// Label, if not empty, is printed in brackets after the verdict instead of "local
	// timing": "CI timing", or why a verdict is not judged.
	Label string
}

// The gates whose rows more than one place names.
const (
	gateG2         = "G2 retention and the slowness after it, longest"
	gateFirstBatch = "first batch after a retention, longest (a diagnostic: under G2 since rules 5)"
)

// passesG0 is whether the candidate answers every query as the reference engine
// does, before the build was compacted and after, and its counters repeat.
func passesG0(c *Candidate) bool {
	return len(c.Results.Mismatches) == 0 && len(c.Manifest.UncompactedWrong) == 0 && len(c.Results.Unstable) == 0
}

// Gates judges G0, G2, G3 and G4 of the rules for each candidate, and the share of
// the bytes written that went to checkpoints against ReopenCheckpointWrites:
//
//   - G0: every answer is the reference engine's.
//   - G2: the largest, over the retentions, of the retention's time plus the excess over
//     the build's median commit of each of the first PostRetentionBatches batches after
//     it ([stallOf]), within StallBudgetSeconds. The block bytes right after a retention,
//     within PostRetentionBytes of the settled value, are not measured by this runner.
//     This report judges the absolute budget only: it has one machine and no reference
//     set, so the relative fallback of the rules is the judgement of the timed builds
//     alone. A build that rested after each retention, or that did not record the
//     batches after one, is shown and not judged.
//   - G3: the 99th percentile of the commit of a batch, within CommitFactor of the best
//     candidate that passes G0. It is a timing of this machine.
//   - G4: the bytes of tables per record written, after compaction, within
//     BytesPerRecordFactor of the best candidate that passes G0.
//
// The longest first batch after a retention, the longest retention alone and the
// settling of a retention are rows that do not decide (NotComparable): the first batch
// is the first term of G2 since rules 5, and carries the ratio the former rule took.
func Gates(cs []*Candidate, r Rules) []GateVerdict {
	var out []GateVerdict
	add := func(c *Candidate, gate, value, limit string, pass, local bool) {
		out = append(out, GateVerdict{Candidate: c.Results.Candidate, Gate: gate, Value: value, Limit: limit, Pass: pass, Local: local})
	}
	// notJudged adds a row that is shown and does not decide.
	notJudged := func(c *Candidate, gate, value, limit string) {
		out = append(out, GateVerdict{Candidate: c.Results.Candidate, Gate: gate, Value: value, Limit: limit, Pass: true, Local: true, NotComparable: true})
	}
	var good []*Candidate
	for _, c := range cs {
		if passesG0(c) {
			good = append(good, c)
		}
	}
	best := func(f func(*Candidate) float64) float64 {
		b := -1.0
		for _, c := range good {
			if v := f(c); v > 0 && (b < 0 || v < b) {
				b = v
			}
		}
		return b
	}
	perRecord := func(c *Candidate) float64 { return measureOf(c.Manifest).perRecord }
	commit := func(c *Candidate) float64 { return float64(measureOf(c.Manifest).commit) }
	rested := func(c *Candidate) bool { return c.Manifest.Describe[RestKey] == "true" }
	after := func(c *Candidate) float64 {
		m := measureOf(c.Manifest)
		if !m.hasAfter || rested(c) { // a rested build sets no standard
			return 0
		}
		return float64(m.after)
	}
	for _, c := range cs {
		g0 := passesG0(c)
		m := measureOf(c.Manifest)
		add(c, "G0 answers", fmt.Sprintf("%d wrong, %d wrong before compaction, %d unstable", len(c.Results.Mismatches), len(c.Manifest.UncompactedWrong), len(c.Results.Unstable)), "none", g0, false)
		if m.hasLongest {
			switch {
			case rested(c):
				value, _, _ := g2Verdict(time.Duration(m.longest), r)
				notJudged(c, gateG2, value, "not comparable: the build rested after each retention")
			case m.hasStall:
				value, limit, pass := g2Verdict(time.Duration(m.stall), r)
				add(c, gateG2, value, limit, pass, true)
			default:
				value, _, _ := g2Verdict(time.Duration(m.longest), r)
				notJudged(c, gateG2, value, "not comparable: the build did not record the batches after a retention")
			}
		}
		if g0 {
			if b := best(perRecord); b > 0 {
				value, limit, pass := g4Verdict(perRecord(c), b, r)
				add(c, "G4 table bytes per record", value, limit, pass, false)
			}
			if b := best(commit); b > 0 {
				value, limit, pass := g3Verdict(commit(c), b, r)
				add(c, "G3 batch commit, 99th percentile", value, limit, pass, true)
			}
			if rested(c) && m.hasAfter {
				notJudged(c, gateFirstBatch, time.Duration(m.after).String(), "not comparable: the build rested after each retention")
			} else if b := best(after); b > 0 && after(c) > 0 {
				// What the former rule would have said, shown beside the number.
				value, limit, pass := g3Verdict(after(c), b, r)
				former := "ok"
				if !pass {
					former = "OVER"
				}
				notJudged(c, gateFirstBatch, value, limit+" [the former rule: "+former+"]")
			}
		}
		if m.hasLongest {
			value, _, _ := g2Verdict(time.Duration(m.longest), r)
			notJudged(c, "retention alone, longest", value, "a diagnostic")
		}
		if longest, hits, ok := settlingOf(c.Manifest.Timing); ok {
			value, _, _ := g2Verdict(longest, r)
			notJudged(c, "settling of a retention, longest", value, fmt.Sprintf("a diagnostic; deadline reached %d times", hits))
		}
		if m.hasCheckpoint {
			value, limit, pass := checkpointVerdict(m.checkpointShare, r)
			add(c, "checkpoint bytes written, share of bytes in", value, limit, pass, false)
		}
	}
	return out
}

// gateMeasure is what one manifest gives the gates: the numbers of its timings, its
// bytes and its checkpoints, read the same way for a local build and a timed one.
type gateMeasure struct {
	// commit is the 99th percentile of the commit of a batch, in nanoseconds (a bucket
	// bound; 0 for a build that timed no batch).
	commit int64
	// after is the longest of the first batches after a retention and longest the
	// longest retention, in nanoseconds; each is valid only where its has flag is set.
	after, longest       int64
	hasAfter, hasLongest bool
	// stall is G2's value, in nanoseconds ([stallOf]), valid only where hasStall says the
	// build recorded the batches after its retentions and retained; stallN is the number
	// of batches after a retention it counted.
	stall    int64
	hasStall bool
	stallN   int
	// perRecord is the bytes of tables per record written, after compaction.
	perRecord float64
	// checkpointShare is the share of the bytes in that went to checkpoints, valid only
	// where hasCheckpoint says the build wrote any.
	checkpointShare float64
	hasCheckpoint   bool
}

// measureOf reads the numbers the gates judge from a manifest.
func measureOf(m *Manifest) gateMeasure {
	g := gateMeasure{
		commit:    m.Timing.Writes.Quantile(0.99),
		perRecord: float64(m.StatsCompacted["live_table_bytes"]) / float64(max(m.Stream.Records, 1)),
	}
	if len(m.Timing.AfterRetention) > 0 {
		g.after, g.hasAfter = slices.Max(m.Timing.AfterRetention), true
	}
	if len(m.Timing.Retains) > 0 {
		g.longest, g.hasLongest = slices.Max(m.Timing.Retains), true
	}
	if n, err := strconv.Atoi(m.Describe[PostRetentionKey]); err == nil {
		g.stallN = n
		g.stall, g.hasStall = stallOf(m.Timing, n)
	}
	if written := m.Counters["checkpoint.bytes_written"]; written > 0 {
		g.checkpointShare, g.hasCheckpoint = float64(written)/float64(max(m.StatsBuilt["bytes_in"], 1)), true
	}
	return g
}

// stallOf is G2's value of a build (see the log of 2026-10-08): the largest, over its
// retentions, of the retention's time plus the excess over the build's median commit
// of each of the first n batches after it. ok is false when the build did not record
// the batches after its retentions (len(PostRetention) != len(Retains)) or retained
// nothing.
func stallOf(t Timing, n int) (stall int64, ok bool) {
	if len(t.Retains) == 0 || len(t.PostRetention) != len(t.Retains) {
		return 0, false
	}
	n = max(n, 0)
	m := t.Writes.Quantile(0.5)
	for i, retain := range t.Retains {
		s := retain
		for _, d := range t.PostRetention[i][:min(n, len(t.PostRetention[i]))] {
			s += max(0, d-m)
		}
		stall = max(stall, s)
	}
	return stall, true
}

// settlingOf is the longest wait for the database to be at rest among the retentions
// that said how they spent their time, and how many of them reached the deadline of
// that wait. ok is false when none did.
func settlingOf(t Timing) (longest time.Duration, deadlineHits int, ok bool) {
	for _, p := range t.RetainPhases {
		longest = max(longest, time.Duration(p.Settle))
		if p.DeadlineHit {
			deadlineHits++
		}
	}
	return longest, deadlineHits, len(t.RetainPhases) > 0
}

// g2Verdict is the value, the limit and the verdict of G2 for a build's value v, the
// largest retention plus the excess of the batches after it.
func g2Verdict(v time.Duration, r Rules) (value, limit string, pass bool) {
	return v.Round(time.Millisecond).String(), time.Duration(r.StallBudgetSeconds * float64(time.Second)).String(), v.Seconds() <= r.StallBudgetSeconds
}

// g3Verdict is the value, the limit and the verdict of G3 for a duration v, in
// nanoseconds, against the best, which is not zero.
func g3Verdict(v, best float64, r Rules) (value, limit string, pass bool) {
	return time.Duration(v).String(), fmt.Sprintf("%s (x%.1f the best, %s)", time.Duration(best*r.CommitFactor), r.CommitFactor, time.Duration(best)), v <= best*r.CommitFactor
}

// g4Verdict is the value, the limit and the verdict of G4 for the bytes per record v
// against the best, which is not zero.
func g4Verdict(v, best float64, r Rules) (value, limit string, pass bool) {
	return fmt.Sprintf("%.1f", v), fmt.Sprintf("%.1f (x%.1f the best, %.1f)", best*r.BytesPerRecordFactor, r.BytesPerRecordFactor, best), v <= best*r.BytesPerRecordFactor
}

// checkpointVerdict is the value, the limit and the verdict for the share of the bytes
// in that went to checkpoints.
func checkpointVerdict(share float64, r Rules) (value, limit string, pass bool) {
	return fmt.Sprintf("%.1f%%", 100*share), fmt.Sprintf("%.0f%% (the design is reopened above it)", 100*r.ReopenCheckpointWrites), share <= r.ReopenCheckpointWrites
}

// GatesOf is [Gates] for the stream of a plan. A plan with pins writes a projection
// (unless its stores are full), about a tenth of the records and the busiest prefixes:
// a retention over it stalls for a fraction of what one over the whole stream does, a
// batch commits faster, and the share of checkpoints is higher than a full store's.
// Only G0, which is about answers, is judged on it; the other gates are those of a
// full build.
func GatesOf(plan *Plan, cs []*Candidate, r Rules) []GateVerdict {
	vs := Gates(cs, r)
	if !plan.Spec.Projected() {
		return vs
	}
	return slices.DeleteFunc(vs, func(v GateVerdict) bool { return !strings.HasPrefix(v.Gate, "G0") })
}

// WriteGates prints the verdicts of [Gates].
func WriteGates(w io.Writer, vs []GateVerdict) {
	fmt.Fprintln(w, "\nthe other gates (G0 answers, G2 retention and the slowness after it, G3 commit, G4 bytes; the block bytes right after a retention are not measured here; timings are of this machine and inform, they are not results):")
	writeGateLines(w, vs)
}

// writeGateLines prints the rows of verdicts, the candidates in name order and each
// one's verdicts in the order given.
func writeGateLines(w io.Writer, vs []GateVerdict) {
	names := map[string]bool{}
	for _, v := range vs {
		names[v.Candidate] = true
	}
	order := sortedKeys(names)
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	for _, name := range order {
		for _, v := range vs {
			if v.Candidate != name {
				continue
			}
			verdict := "ok"
			switch {
			case v.NotComparable:
				verdict = "not judged"
			case !v.Pass:
				verdict = "OVER"
			}
			suffix := ""
			switch {
			case v.Label != "":
				suffix = " (" + v.Label + ")"
			case v.Local:
				suffix = " (local timing)"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\tlimit %s\t%s%s\n", name, v.Gate, v.Value, v.Limit, verdict, suffix)
		}
	}
	_ = tw.Flush()
}
