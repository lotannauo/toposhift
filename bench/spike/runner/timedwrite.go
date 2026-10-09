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

// timingGateNames are the names the text gives the gates, as [Gates] words them.
var timingGateNames = map[string]string{
	GateG0:         "G0 answers",
	GateG2:         "G2 retention and the slowness after it",
	GateG3Commit:   "G3 batch commit, 99th percentile",
	GateG4:         "G4 table bytes per record",
	GateCheckpoint: "checkpoint bytes written, share of bytes in",
}

// WriteTiming prints the report of [JudgeTiming]. It is a function of the report alone:
// no clock, no path, and every list in an order that does not depend on the order
// the artifacts were read in.
func WriteTiming(w io.Writer, rep TimingReport) {
	carried := ""
	if d, err := DefaultRules().Digest(); err == nil && d == rep.RulesDigest {
		carried = ", the rules this binary carries"
	}
	fmt.Fprintf(w, "timed builds of the bench workflow, judged by rules %.12s (version %d%s)\n", rep.RulesDigest, rep.RulesVersion, carried)
	r := rep.Rule
	fmt.Fprintf(w, "the rule: G2 the largest over the repetitions of the retention plus the slowness of the first %d batches after it, within %s (or, where no layout of the reference set fits, within x%.1f the best); G3 the median 99th-percentile commit over the repetitions, within x%.1f the best median of the reference set, not decided where %s varies by more than x%.1f; G4 the median, within x%.1f the best median; a gate passes only if it passes on both architectures; timings are compared within one CPU model, or pooled when every candidate compared has at least %d repetitions\n",
		r.PostRetentionBatches, time.Duration(r.StallBudgetSeconds*float64(time.Second)), r.CommitFactor, r.CommitFactor, timingBaseline, timingAAFloor, r.BytesPerRecordFactor, timingMinReps)
	runWord := "run"
	if len(rep.Runs) > 1 {
		runWord = "runs"
	}
	fmt.Fprintf(w, "revision %.12s, family %s, pins from the %d-day window, %d builds from %s %s\n", rep.Revision, rep.Family, rep.PinsWindow, len(rep.Builds), runWord, strings.Join(rep.Runs, ", "))
	ref := rep.RevisionRef
	if ref == "" {
		ref = "origin/main"
	}
	switch {
	case rep.RevisionError != "":
		fmt.Fprintf(w, "WARNING: the revision could not be checked against %s: %s\n", ref, rep.RevisionError)
	case rep.RevisionChecked:
		fmt.Fprintf(w, "revision %.12s is an ancestor of %s\n", rep.Revision, ref)
	case slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.Contains(p, "is not an ancestor of") }):
		fmt.Fprintf(w, "WARNING: the revision is not an ancestor of %s\n", ref)
	default:
		fmt.Fprintf(w, "WARNING: the revision was not checked against %s (give -git): these results are unverified\n", ref)
	}
	fmt.Fprintf(w, "builds: %s, %s, %s, %s, %s, %s; binaries %s (%s)\n",
		settingWords("sync", rep.Settings.Sync), settingWords("rest", rep.Settings.RestAfterRetention),
		settingWords("canonical", rep.Settings.CanonicalLayout), settingWords("metrics", rep.Settings.MetricsEvery),
		settleWords(rep.Settings), memoryLimitWords(rep.Settings.GoMemoryLimit), binaryWords(rep.Binaries), goVersions(rep.Binaries))
	var plans []string
	for _, p := range rep.Plans {
		if p.Found {
			plans = append(plans, fmt.Sprintf("%d days %.12s (%d records, rules %.12s)", p.Window, p.Digest, p.Records, p.RulesDigest))
		} else {
			plans = append(plans, fmt.Sprintf("%d days %.12s not among the inputs", p.Window, p.Digest))
		}
	}
	fmt.Fprintf(w, "plans: %s\n", strings.Join(plans, "; "))

	if len(rep.Problems) > 0 {
		fmt.Fprintln(w, "\nTHESE TIMINGS ARE NOT TO BE JUDGED TOGETHER:")
		for _, p := range rep.Problems {
			fmt.Fprintln(w, "  -", p)
		}
	}

	writeTimedBuilds(w, rep)
	fmt.Fprintf(w, "\n%s\n", referenceLine(rep))
	for _, f := range rep.Findings {
		fmt.Fprintf(w, "\nFINDING: %s\n", f)
	}

	if rep.Judged {
		fmt.Fprintln(w, "\nevery precondition holds: these are CI timings")
	} else {
		fmt.Fprintln(w, "\nNOT JUDGED:")
		for _, r := range rep.NotJudged {
			fmt.Fprintln(w, "  -", r)
		}
	}

	var windows []int
	for _, v := range rep.Verdicts {
		if !slices.Contains(windows, v.Window) {
			windows = append(windows, v.Window)
		}
	}
	for _, days := range windows {
		fmt.Fprintf(w, "\nthe gates at %d days (G0 from the counters runs on the same plan; G2 the largest longest retention over the repetitions; G3 and G4 the median over the repetitions against the best median of the reference set; both = the verdict, which needs both architectures; a line not judged shows what the value would give):\n", days)
		var vs []GateVerdict
		for _, v := range rep.Verdicts {
			if v.Window == days {
				vs = append(vs, gateVerdictOf(v))
			}
		}
		writeGateLines(w, vs)
		writeDiagnostics(w, rep, days)
	}
}

// writeDiagnostics prints the numbers of a window that are shown and do not decide.
func writeDiagnostics(w io.Writer, rep TimingReport, days int) {
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	n := 0
	for _, d := range rep.Diagnostics {
		if d.Window != days {
			continue
		}
		if n == 0 {
			fmt.Fprintf(tw, "\ndiagnostics at %d days (they do not decide; the first batch after a retention is judged under G2 since rules %d):\n", days, rep.RulesVersion)
		}
		n++
		where := d.Arch
		if d.Scope != "" {
			where += " " + d.Scope
		}
		what, stat := "", "largest"
		switch d.What {
		case "first batch":
			what, stat = "first batch after a retention, longest", "median"
		case "retention alone":
			what = "retention alone, longest"
		default:
			what = "settling of a retention, longest"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s (%s of %d)\t%s\t%s\n", d.Candidate, where, what, stat, d.Reps, time.Duration(d.Value), d.Text)
	}
	_ = tw.Flush()
}

// gateVerdictOf is the row of a verdict for the writer shared with [WriteGates].
func gateVerdictOf(v TimingVerdict) GateVerdict {
	name := timingGateNames[v.Gate]
	if v.Gate != GateG0 {
		name += ", " + v.Arch
	}
	if v.Scope != "" && v.Arch != ArchBoth {
		name += " " + v.Scope
	}
	if v.Arch != ArchBoth && v.Gate != GateG0 {
		stat := "median"
		if v.Gate == GateG2 || v.Gate == GateCheckpoint {
			stat = "largest"
		}
		name += fmt.Sprintf(" (%s of %d)", stat, v.Reps)
	}
	g := GateVerdict{Candidate: v.Candidate, Gate: name, Value: v.ValueText, Limit: v.LimitText, Pass: v.Pass, NotComparable: v.NotComparable}
	// A verdict that could not be made at all (G0 that no counters run reads, a gate with
	// no build on an architecture) is not judged and not over.
	forced := !v.Judged && !v.Pass && (v.Gate == GateG0 && v.ValueText == "not established" || v.Arch == ArchBoth && strings.Contains(v.ValueText, "no builds"))
	if forced {
		g.NotComparable = true
	}
	switch {
	case forced:
		g.Label = v.Reason // the verdict word says it is not judged; the bracket says why
	case v.NotComparable:
	case !v.Judged:
		g.Label = "not judged: " + v.Reason
	case v.Gate == GateG2 || v.Gate == GateG3Commit:
		g.Label = "CI timing"
	}
	return g
}

func settingWords(key, value string) string {
	switch key {
	case "sync":
		switch value {
		case "false":
			return "no sync of the log"
		case "true":
			return "a sync of every commit"
		}
		return fmt.Sprintf("sync %q", value)
	case "rest":
		switch value {
		case "false":
			return "no rest after a retention"
		case "true":
			return "a rest after each retention"
		case "":
			return "no record of a rest after a retention"
		}
		return fmt.Sprintf("rest %q", value)
	case "canonical":
		switch value {
		case "false":
			return "not rewritten into the canonical layout"
		case "true":
			return "rewritten into the canonical layout"
		}
		return fmt.Sprintf("canonical layout %q", value)
	}
	if value == "0s" {
		return "metrics not sampled"
	}
	return "metrics sampled every " + value
}

func settleWords(s TimingSettings) string {
	if s.SettleTombstones == "true" {
		if s.SettleDeadline == "" {
			return "settled each retention (no deadline recorded)"
		}
		return "settled each retention (deadline " + s.SettleDeadline + ")"
	}
	return "did not settle"
}

// memoryLimitWords says a recorded Go memory limit (bytes in decimal, or "none") in the
// largest whole unit it is a multiple of.
func memoryLimitWords(limit string) string {
	if limit == "none" {
		return "no Go memory limit"
	}
	n, err := strconv.ParseInt(limit, 10, 64)
	switch {
	case err != nil || n <= 0:
		return fmt.Sprintf("Go memory limit %q", limit)
	case n%(1<<30) == 0:
		return fmt.Sprintf("Go memory limit %d GiB", n>>30)
	case n%(1<<20) == 0:
		return fmt.Sprintf("Go memory limit %d MiB", n>>20)
	}
	return fmt.Sprintf("Go memory limit %d bytes", n)
}

func binaryWords(bs []TimingBinary) string {
	var parts []string
	for _, b := range bs {
		parts = append(parts, fmt.Sprintf("%s %.12s", b.Arch, b.Executable))
	}
	return list(parts)
}

func goVersions(bs []TimingBinary) string {
	seen := map[string]bool{}
	for _, b := range bs {
		seen[b.GoVersion] = true
	}
	return strings.Join(sortedKeys(seen), ", ")
}

// writeTimedBuilds lists how many builds there are of each candidate on each CPU model.
func writeTimedBuilds(w io.Writer, rep TimingReport) {
	fmt.Fprintln(w, "\nbuilds (window, candidate, architecture, CPU model, repetitions):")
	type key struct {
		window               int
		candidate, arch, cpu string
	}
	var order []key
	counts := map[key]int{}
	for _, b := range rep.Builds {
		k := key{b.Window, b.Candidate, b.Arch, b.CPUModel}
		if counts[k] == 0 {
			order = append(order, k)
		}
		counts[k]++
	}
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	for _, k := range order {
		fmt.Fprintf(tw, "  %d days\t%s\t%s\t%s\t%d\n", k.window, k.candidate, k.arch, k.cpu, counts[k])
	}
	_ = tw.Flush()
}

// referenceLine says what the best of G3 and G4 is taken over, and why the joiner is
// or is not in it.
func referenceLine(rep TimingReport) string {
	var sb strings.Builder
	sb.WriteString("the reference set (the best of G3 and G4 is taken over it): " + timingBaseline + ", " + timingChosenPolicy + " (the checkpoint policy G1 chose)")
	j := rep.Join
	var details []string
	for _, a := range j.Arches {
		switch {
		case a.Comparable:
			details = append(details, fmt.Sprintf("%s %s: %s against %s", a.Arch, a.Scope, time.Duration(a.JoinerP99), time.Duration(a.BaselineP99)))
		default:
			details = append(details, a.Arch+": not compared")
		}
	}
	detail := ""
	if len(details) > 0 {
		detail = " (" + strings.Join(details, "; ") + ")"
	}
	from := ""
	if len(j.Runs) > 0 {
		word := "run"
		if len(j.Runs) > 1 {
			word = "runs"
		}
		from = fmt.Sprintf(", from %s %s at revision %.12s", word, strings.Join(j.Runs, ", "), j.Revision)
	}
	switch {
	case j.Joins:
		var below []string
		for _, a := range j.Arches {
			if a.Comparable && a.Below {
				below = append(below, a.Arch)
			}
		}
		fmt.Fprintf(&sb, "; %s joins, at every window: its median 99th-percentile commit at %d days is more than 10%% below %s's on %s%s%s", timingJoiner, timingJoinWindow, timingBaseline, list(below), detail, from)
	case j.Decided && j.Reason == "":
		fmt.Fprintf(&sb, "; %s does not join: its median 99th-percentile commit at %d days is not more than 10%% below %s's on either architecture%s%s", timingJoiner, timingJoinWindow, timingBaseline, detail, from)
	case j.Decided:
		fmt.Fprintf(&sb, "; %s does not join: %s%s", timingJoiner, j.Reason, from)
	default:
		fmt.Fprintf(&sb, "; whether %s joins is not decided: %s%s", timingJoiner, j.Reason, from)
	}
	for _, v := range rep.Verdicts {
		if v.Gate == GateG0 && v.Judged && !v.Pass && slices.Contains(rep.Reference, v.Candidate) {
			fmt.Fprintf(&sb, "; at %d days %s fails G0 and is left out of the best", v.Window, v.Candidate)
		}
	}
	return sb.String()
}
