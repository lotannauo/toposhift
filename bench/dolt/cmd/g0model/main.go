// Command g0model regenerates the stream of a plan into the in-memory model of the Dolt
// baseline's tables and asks every question of the plan, as the first gate of the baseline
// does for a database: the model's answer must equal the reference engine's. It also
// prints what the model wrote and which tables each group of questions was answered from,
// which are counts (functions of the stream and the questions), not measurements of Dolt.
//
//	g0model -plan plan.json [-arm commit|rows|commit-time] [-expect-plan sha256]
//
// The arm commit is the registered design (a commit per batch, the state read as of a
// commit for an old token), rows the control with no commits. The arm commit-time is the
// diagnostic: the same tables read as if a commit's place in history were an event time. Its
// wrong answers are its result, so it prints them and does not fail on them; it is never a
// comparator.
//
// It prints no time. The model holds the whole stream in memory.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "g0model:", err)
		os.Exit(1)
	}
}

const usage = "usage: g0model -plan plan.json [-arm commit|rows|commit-time] [-expect-plan sha256]"

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("g0model", flag.ContinueOnError)
	fs.SetOutput(stderr)
	plan := fs.String("plan", "", "the plan.json to check")
	arm := fs.String("arm", "commit", "the design: commit, rows or commit-time (the diagnostic)")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; the run is refused if the file differs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *plan == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	b, err := os.ReadFile(*plan)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	got := hex.EncodeToString(sum[:])
	if *expect != "" && !strings.EqualFold(*expect, got) {
		return fmt.Errorf("%s has SHA-256 %s, not the expected %s", *plan, got, *expect)
	}
	p, err := runner.LoadPlan(*plan)
	if err != nil {
		return err
	}

	// The payload column's registered capacity, which every payload of the plan must fit.
	if need := doltmodel.WorkloadPayload(p.Spec.Workload); need > doltmodel.RegisteredPayloadCapacity {
		return fmt.Errorf("the plan's payloads reach %d bytes, over the registered capacity of %d", need, doltmodel.RegisteredPayloadCapacity)
	}
	capacity := doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)
	var (
		m          *doltmodel.Model
		e          engine.Engine
		diagnostic bool
	)
	switch *arm {
	case "commit":
		m = doltmodel.New(capacity)
		e = m
	case "rows":
		m = doltmodel.New(doltmodel.WithArm(doltmodel.ArmRows), capacity)
		e = m
	case "commit-time":
		d := doltmodel.NewCommitTime(capacity)
		m, e, diagnostic = d.Model, d, true
	default:
		return fmt.Errorf("unknown arm %q: %s", *arm, usage)
	}

	rep, err := g0.Run(ctx, p, e, func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) })
	if err != nil {
		return err
	}
	c := m.Counts()
	fmt.Fprintf(stdout, "plan sha256 %s\nstream digest %s\narm %s\n", got, rep.Stream.Digest, *arm)
	fmt.Fprintf(stdout, "stream: %d records, last seq %d, %d retentions\n", rep.Stream.Records, rep.Stream.LastSeq, len(rep.Stream.Retentions))
	label := "G0"
	if diagnostic {
		label = "DIAGNOSTIC"
	}
	for _, g := range rep.Groups {
		fmt.Fprintf(stdout, "%s %-40s %-10s %d of %d%s\n", label, g.Group, g.Age, g.Matched, g.Queries, pathSummary(g.Paths))
	}
	if diagnostic {
		fmt.Fprintf(stdout, "DIAGNOSTIC total: %d of %d answers wrong (the commit-time reading; not a comparator)\n", rep.Queries-rep.Matched, rep.Queries)
	} else {
		fmt.Fprintf(stdout, "G0 total: %d of %d answers equal the plan's\n", rep.Matched, rep.Queries)
	}
	fmt.Fprintf(stdout, "paths (reads by kind and table): %s\n", strings.TrimPrefix(pathSummary(rep.TotalPaths()), " "))
	fmt.Fprintf(stdout, "model writer counts (rows of the model, not Dolt): records %d, log rows %d, state replacements %d, log deletes %d, state deletes %d, commits %d\n",
		c.Records, c.LogRows, c.StateWrites, c.LogDeletes, c.StateDeletes, c.Commits)
	if head, ok := m.Head(); ok {
		fmt.Fprintf(stdout, "model head commit %s (ordinal %d, token %d; the model's id, not a Dolt hash)\n", head.ID, head.Ordinal, head.Token)
	}
	fmt.Fprintf(stdout, "payload capacity %d bytes (registered); records refused for it: %d\n", doltmodel.RegisteredPayloadCapacity, c.PayloadRefusals)
	if !diagnostic {
		printSettlement(stdout, rep)
	}
	if diagnostic {
		return nil
	}
	for _, mm := range rep.Mismatches {
		fmt.Fprintf(stdout, "MISMATCH %s: got %+v, plan %+v %s\n", mm.Query, mm.Got, mm.Want, mm.AskError)
	}
	// In the arm with commits every token of a plan names a commit, so no read lacks one.
	if noCommit := rep.TotalPaths(); *arm == "commit" {
		var n uint64
		for k, v := range noCommit {
			if strings.HasSuffix(k, "/no-commit") {
				n += v
			}
		}
		fmt.Fprintf(stdout, "reads whose token names no commit: %d\n", n)
		if n != 0 {
			return fmt.Errorf("%d reads of the plan had no commit to read in the arm with commits", n)
		}
	}
	if c.PayloadRefusals != 0 {
		return fmt.Errorf("%d records were refused for a payload over the registered capacity: the capability row must be empty on a registered input", c.PayloadRefusals)
	}
	if !rep.OK() {
		return fmt.Errorf("%d of %d answers differ from the plan's", rep.Queries-rep.Matched, rep.Queries)
	}
	return nil
}

// printSettlement prints, per age, how the rule settled the reads: the queries answered by
// the state as of a commit alone, those that needed the log for some key, and the keys
// settled by a row and answered from the log. They are counts of the questions and the
// stream.
func printSettlement(w io.Writer, rep *g0.Report) {
	type sum struct{ alone, needed, noCommit, keysState, keysLog, queries uint64 }
	byAge := map[string]*sum{}
	var ages []string
	for _, g := range rep.Groups {
		x := byAge[g.Age]
		if x == nil {
			x = &sum{}
			byAge[g.Age] = x
			ages = append(ages, g.Age)
		}
		for k, v := range g.Paths {
			switch {
			case strings.HasSuffix(k, "/as-of-alone"):
				x.alone += v
			case strings.HasSuffix(k, "/needed-log"):
				x.needed += v
			case strings.HasSuffix(k, "/no-commit"):
				x.noCommit += v
			case strings.HasSuffix(k, "/keys-state"):
				x.keysState += v
			case strings.HasSuffix(k, "/keys-log"):
				x.keysLog += v
			}
		}
		x.queries += uint64(g.Queries)
	}
	fmt.Fprintln(w, "settlement by age (neighbors, batch and alive reads: queries answered by AS OF alone, queries that needed the log, queries whose token names no commit, keys from a state row, keys from the log):")
	for _, age := range ages {
		x := byAge[age]
		if x.alone+x.needed+x.noCommit == 0 {
			continue
		}
		fmt.Fprintf(w, "  %-10s as-of-alone %d, needed-log %d, no-commit %d, keys-state %d, keys-log %d\n", age, x.alone, x.needed, x.noCommit, x.keysState, x.keysLog)
	}
}

// pathSummary is the path counts in key order, with a leading space, or nothing.
func pathSummary(paths map[string]uint64) string {
	if len(paths) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, k := range slices.Sorted(maps.Keys(paths)) {
		fmt.Fprintf(&sb, " %s=%d", k, paths[k])
	}
	return sb.String()
}
