package runner

import (
	"fmt"
	"io"
	"slices"
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
}

// passesG0 is whether the candidate answers every query as the reference engine
// does, before the build was compacted and after, and its counters repeat.
func passesG0(c *Candidate) bool {
	return len(c.Results.Mismatches) == 0 && len(c.Manifest.UncompactedWrong) == 0 && len(c.Results.Unstable) == 0
}

// Gates judges G0, G2, G3 and G4 of the rules for each candidate, and the share of
// the bytes written that went to checkpoints against ReopenCheckpointWrites:
//
//   - G0: every answer is the reference engine's.
//   - G2: the longest retention is within StallBudgetSeconds, which is the time the
//     writer is held for (a retention holds the lock for its whole run). The block
//     bytes right after a retention, within PostRetentionBytes of the settled value,
//     are not measured by this runner.
//   - G3: the 99th percentile of the commit of a batch, and the longest of the first
//     batches after a retention, within CommitFactor of the best candidate that passes
//     G0. Both are timings of this machine.
//   - G4: the bytes of tables per record written, after compaction, within
//     BytesPerRecordFactor of the best candidate that passes G0.
func Gates(cs []*Candidate, r Rules) []GateVerdict {
	var out []GateVerdict
	add := func(c *Candidate, gate, value, limit string, pass, local bool) {
		out = append(out, GateVerdict{Candidate: c.Results.Candidate, Gate: gate, Value: value, Limit: limit, Pass: pass, Local: local})
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
	perRecord := func(c *Candidate) float64 {
		return float64(c.Manifest.StatsCompacted["live_table_bytes"]) / float64(max(c.Manifest.Stream.Records, 1))
	}
	commit := func(c *Candidate) float64 { return float64(c.Manifest.Timing.Writes.Quantile(0.99)) }
	rested := func(c *Candidate) bool { return c.Manifest.Describe[RestKey] == "true" }
	after := func(c *Candidate) float64 {
		if len(c.Manifest.Timing.AfterRetention) == 0 || rested(c) { // a rested build sets no standard
			return 0
		}
		return float64(slices.Max(c.Manifest.Timing.AfterRetention))
	}
	for _, c := range cs {
		g0 := passesG0(c)
		add(c, "G0 answers", fmt.Sprintf("%d wrong, %d wrong before compaction, %d unstable", len(c.Results.Mismatches), len(c.Manifest.UncompactedWrong), len(c.Results.Unstable)), "none", g0, false)
		if n := len(c.Manifest.Timing.Retains); n > 0 {
			longest := time.Duration(slices.Max(c.Manifest.Timing.Retains))
			add(c, "G2 longest retention", longest.Round(time.Millisecond).String(), time.Duration(r.StallBudgetSeconds*float64(time.Second)).String(), longest.Seconds() <= r.StallBudgetSeconds, true)
		}
		if g0 {
			if b := best(perRecord); b > 0 {
				add(c, "G4 table bytes per record", fmt.Sprintf("%.1f", perRecord(c)), fmt.Sprintf("%.1f (x%.1f the best, %.1f)", b*r.BytesPerRecordFactor, r.BytesPerRecordFactor, b), perRecord(c) <= b*r.BytesPerRecordFactor, false)
			}
			if b := best(commit); b > 0 {
				add(c, "G3 batch commit, 99th percentile", time.Duration(commit(c)).String(), fmt.Sprintf("%s (x%.1f the best, %s)", time.Duration(b*r.CommitFactor), r.CommitFactor, time.Duration(b)), commit(c) <= b*r.CommitFactor, true)
			}
			if rested(c) && len(c.Manifest.Timing.AfterRetention) > 0 {
				longest := time.Duration(slices.Max(c.Manifest.Timing.AfterRetention))
				out = append(out, GateVerdict{Candidate: c.Results.Candidate, Gate: "G3 first batch after a retention, longest", Value: longest.String(), Limit: "not comparable: the build rested after each retention", Pass: true, Local: true, NotComparable: true})
			} else if b := best(after); b > 0 && after(c) > 0 {
				add(c, "G3 first batch after a retention, longest", time.Duration(after(c)).String(), fmt.Sprintf("%s (x%.1f the best, %s)", time.Duration(b*r.CommitFactor), r.CommitFactor, time.Duration(b)), after(c) <= b*r.CommitFactor, true)
			}
		}
		if written := c.Manifest.Counters["checkpoint.bytes_written"]; written > 0 {
			share := float64(written) / float64(max(c.Manifest.StatsBuilt["bytes_in"], 1))
			add(c, "checkpoint bytes written, share of bytes in", fmt.Sprintf("%.1f%%", 100*share), fmt.Sprintf("%.0f%% (the design is reopened above it)", 100*r.ReopenCheckpointWrites), share <= r.ReopenCheckpointWrites, false)
		}
	}
	return out
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
	fmt.Fprintln(w, "\nthe other gates (G0 answers, G2 stall, G3 commit, G4 bytes; the block bytes right after a retention are not measured here; timings are of this machine and inform, they are not results):")
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
			local := ""
			if v.Local {
				local = " (local timing)"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\tlimit %s\t%s%s\n", name, v.Gate, v.Value, v.Limit, verdict, local)
		}
	}
	_ = tw.Flush()
}
