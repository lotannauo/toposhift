package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lotannauo/toposhift/internal/query/decay"
)

// report is what a run says. Everything in it is a count, a bound or a name: no
// clock reading of the machine, so the same commit, capture and flags give the same
// Digest on any machine.
type report struct {
	Tool string `json:"tool"`
	// Commit and Command say what produced the counts: the build's commit when it was
	// stamped with one, and the arguments.
	Commit  string   `json:"commit"`
	Command []string `json:"command"`
	// What follows is what the Digest covers.
	Capture inputInfo    `json:"capture"`
	Window  window       `json:"window"`
	Policy  decay.Policy `json:"policy"`
	Lines   lineStats    `json:"lines"`
	Runs    []runResult  `json:"runs"`
	Digest  string       `json:"digest"`
}

// inputInfo names the input: its parts, the digest of the parts joined that the
// command was given and found, and their size.
type inputInfo struct {
	Parts  []string `json:"parts"`
	SHA256 string   `json:"sha256"`
	Bytes  int64    `json:"bytes"`
}

type window struct {
	From     string        `json:"from"`
	To       string        `json:"to"`
	Step     time.Duration `json:"step_ns"`
	Lookback time.Duration `json:"lookback_ns"`
}

// failures lists every self-check that failed, with its expiry.
func (r *report) failures() []string {
	var out []string
	for _, run := range r.Runs {
		for _, f := range run.Failures {
			out = append(out, fmt.Sprintf("expiry %s: %s", run.Expiry, f))
		}
	}
	return out
}

// seal sets the digest: the SHA-256 of the JSON of what it covers.
func (r *report) seal() error {
	b, err := json.Marshal(struct {
		Capture inputInfo    `json:"capture"`
		Window  window       `json:"window"`
		Policy  decay.Policy `json:"policy"`
		Lines   lineStats    `json:"lines"`
		Runs    []runResult  `json:"runs"`
	}{r.Capture, r.Window, r.Policy, r.Lines, r.Runs})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	r.Digest = hex.EncodeToString(sum[:])
	return nil
}

// buildCommit is the revision the binary was stamped with, and whether the tree was
// modified, or "unknown".
func buildCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	rev, modified := "", ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	switch {
	case rev == "":
		return "unknown"
	case modified == "true":
		return rev + " (modified)"
	}
	return rev
}

// writeText prints the report as tables.
func writeText(w io.Writer, r *report) error {
	var sb strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&sb, format, args...) }
	p("decaycheck: where the derived silence of a dependency shows, replayed through the product's receiver, sequencer and coalescer\n")
	p("local, deterministic counts: not timings, and not for publication\n\n")
	p("commit      %s\n", r.Commit)
	p("command     %s\n", strings.Join(r.Command, " "))
	p("capture     %d parts, %d bytes, sha256 %s\n", len(r.Capture.Parts), r.Capture.Bytes, r.Capture.SHA256)
	p("window      [%s, %s), a sample every %s, each read from %s before its instant\n", r.Window.From, r.Window.To, r.Window.Step, r.Window.Lookback)
	p("policy      version %d, extension fraction %g, half-life %g of the extension interval\n", r.Policy.Version, r.Policy.ExtendFraction, r.Policy.HalfLifeOfX)
	p("lines       %d read, %d sent, %d at or after the end of the window, %d with no time, %d out of order; first %s, newest %s\n\n",
		r.Lines.Lines, r.Lines.Sent, r.Lines.AtOrAfterTo, r.Lines.NoTime, r.Lines.OutOfOrder, r.Lines.First, r.Lines.Last)
	for _, run := range r.Runs {
		p("== expiry %s: X = %s, half-life %s\n", run.Expiry, run.X, run.HalfLife)
		p("store last seq %d; %d edges, %d sightings; %d samples from %s\n", run.LastSeq, run.Edges, run.Sightings, run.Samples, run.FirstSample)
		for _, c := range run.Counters {
			p("  %-48s %d\n", c.Name, c.N)
		}
		for _, t := range run.Tokens {
			p("\n-- read at the token %s\n", strings.ReplaceAll(t.Kind, "_", " "))
			tw := tabwriter.NewWriter(&sb, 0, 4, 2, ' ', tabwriter.AlignRight)
			_, _ = fmt.Fprintln(tw, "edge\tsightings\tmembers\tshown\ttrue>X\tunsound\tloose\tother\tmax bound\tmax true\tmax short\t")
			for _, e := range append(append([]edgeRow{}, t.Edges...), t.Totals) {
				_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t\n", e.Edge, e.Sightings, e.Members, e.Shown, e.TrueOverX,
					e.Unsound, e.Loose, e.NoRecency+e.NoTruth+e.OtherTTL, e.MaxBound, e.MaxTrue, e.MaxShort)
			}
			_ = tw.Flush()
			p("members by the bound in half-lives: 0: %d; (0,1): %d; [1,2): %d; [2,3): %d; [3,4): %d; 4 or more: %d. unknown edges: %d\n",
				t.SilenceBins[0], t.SilenceBins[1], t.SilenceBins[2], t.SilenceBins[3], t.SilenceBins[4], t.SilenceBins[5], t.UnknownEdges)
		}
		p("\n")
	}
	if f := r.failures(); len(f) > 0 {
		p("FAILED:\n")
		for _, s := range f {
			p("  %s\n", s)
		}
	} else {
		p("every sample agrees with the truth: no bound above the true silence, none short of it by more than X\n")
	}
	p("digest      %s\n", r.Digest)
	_, err := io.WriteString(w, sb.String())
	return err
}
