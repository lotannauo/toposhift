package rulejudge

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// Write prints the judgement: the verdict, or why the set is refused; what it rests
// on at the verdict's scope; what gives no result and why; every comparison; and
// every build. The numbers are CI timings, printed for the gate log.
func Write(w io.Writer, rep *Report) {
	fmt.Fprintf(w, "Rule R (layout L on RocksDB against layout L on Pebble), version 1\n")
	fmt.Fprintf(w, "revision %s (not checked to be a commit of main: the gate log names it): %d builds\n", orNone(rep.Revision), len(rep.Builds))
	for _, p := range rep.Plans {
		switch {
		case p.Multiple:
			fmt.Fprintf(w, "plan at %d days: several\n", p.Window)
		case p.Digest == "":
			fmt.Fprintf(w, "plan at %d days: no build recorded one\n", p.Window)
		case !p.Found:
			fmt.Fprintf(w, "plan at %d days: %.12s, not among the inputs\n", p.Window, p.Digest)
		default:
			fmt.Fprintf(w, "plan at %d days: %.12s, %d records, %d queries\n", p.Window, p.Digest, p.Records, p.Queries)
		}
	}
	if len(rep.Refused) > 0 {
		fmt.Fprintln(w, "\nREFUSED: the set is not one the rule judges, and there is no verdict:")
		for _, r := range rep.Refused {
			fmt.Fprintln(w, "  -", r)
		}
	} else {
		fmt.Fprintf(w, "\nVERDICT: %s\n", rep.Verdict)
		for _, why := range rep.Why {
			fmt.Fprintln(w, "  -", why)
		}
	}

	fmt.Fprintf(w, "\nAt %s, %d days (%s decides, %s guards the direction):\n", VerdictPolicy, VerdictWindow, DecidingArch, GuardArch)
	for _, c := range rep.Classes {
		fmt.Fprintf(w, "  %s: %s\n", c.Candidate, c.Verdict)
		for _, g := range c.Gates {
			fmt.Fprintf(w, "    %-4s %s; %s %s, %s %s\n", g.Gate, g.Outcome, DecidingArch, outcomeText(g.Deciding), GuardArch, outcomeText(g.Guard))
		}
	}

	if len(rep.NotResults) > 0 {
		fmt.Fprintln(w, "\nNOT RESULTS (each is void, or left nothing to take a value from):")
		for _, why := range rep.NotResults {
			fmt.Fprintln(w, "  -", why)
		}
	}

	fmt.Fprintf(w, "\nCOMPARISONS (RocksDB's median over Pebble's; better or worse by more than the larger of x%.2f and the noise floor):\n", margin)
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "days\tarch\tcandidate\tgate\tscope\treps\tvalue\tPebble\tratio\tfloor\toutcome")
	for _, c := range rep.Comparisons {
		if c.Scope == "" && c.Value == 0 {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t\t\t\t\t\t\t%s: %s\n", c.Window, c.Arch, c.Candidate, c.Gate, c.Outcome, c.Why)
			continue
		}
		outcome := string(c.Outcome)
		if c.Why != "" {
			outcome += ": " + c.Why
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\tx%.3f\tx%.3f\t%s\n", c.Window, c.Arch, c.Candidate, c.Gate, c.Scope, c.Reps, c.AgainstReps,
			FormatValue(c.Gate, c.Value), FormatValue(c.Gate, c.AgainstValue), c.Ratio, c.Floor, outcome)
	}
	_ = tw.Flush()

	fmt.Fprintln(w, "\nBUILDS:")
	tw = tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "days\tarch\tcandidate\trep\tCPU model\tG3\tG2\tG4\tRSS\tread")
	for _, b := range rep.Builds {
		if !b.Manifest {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\tno manifest\t\t\t%s\t\n", b.Window, b.Arch, b.Candidate, b.Rep, b.CPUModel, valueOr(b, GateRSS))
			continue
		}
		read := ""
		if b.Read {
			read = "read"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", b.Window, b.Arch, b.Candidate, b.Rep, b.CPUModel,
			valueOr(b, GateG3), valueOr(b, GateG2), valueOr(b, GateG4), valueOr(b, GateRSS), read)
	}
	_ = tw.Flush()
}

// FormatValue is a gate's value as the report prints it: G3 and G2 as the runner's
// gates print them (G2 to the millisecond), G4 to a tenth of a byte, the resident
// set in MiB.
func FormatValue(gate string, v float64) string {
	switch gate {
	case GateG3:
		return time.Duration(int64(v)).String()
	case GateG2:
		return time.Duration(int64(v)).Round(time.Millisecond).String()
	case GateG4:
		return fmt.Sprintf("%.1f", v)
	case GateRSS:
		return fmt.Sprintf("%.0f MiB", v/1024)
	}
	return fmt.Sprint(v)
}

func valueOr(b BuildRow, gate string) string {
	if v, ok := b.Values[gate]; ok {
		return FormatValue(gate, v)
	}
	return "-"
}

func outcomeText(c Comparison) string {
	if c.Outcome == NotDecided {
		return fmt.Sprintf("not decided (%s)", c.Why)
	}
	return fmt.Sprintf("%s (%s against %s, x%.3f, limit x%.3f)", c.Outcome, FormatValue(c.Gate, c.Value), FormatValue(c.Gate, c.AgainstValue), c.Ratio, c.Limit)
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
