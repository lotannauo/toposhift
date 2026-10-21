// Command duckspike is the driver of the DuckDB baseline. It has one subcommand so far:
//
//	duckspike g0 -plan plan.json [-expect-plan sha256] [-arms records,intervals]
//
// g0 regenerates the stream of a plan into the in-memory model of each arm's files and
// asks every question of the plan, as the first gate of the baseline does for DuckDB:
// the model's answer must equal the reference engine's. It also prints what the model
// holds at the end, which is a count of rows (a function of the stream), not a
// measurement of DuckDB.
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
	"os"
	"strings"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/duckdb/g0"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "duckspike:", err)
		os.Exit(1)
	}
}

const usage = "usage: duckspike g0 -plan plan.json [-expect-plan sha256] [-arms records,intervals]"

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	switch args[0] {
	case "g0":
		return runG0(ctx, args[1:], stdout, stderr)
	}
	return fmt.Errorf("no subcommand %q; %s", args[0], usage)
}

func parseArms(list string) ([]duckmodel.Arm, error) {
	var arms []duckmodel.Arm
	seen := map[duckmodel.Arm]bool{}
	for _, name := range strings.Split(list, ",") {
		var arm duckmodel.Arm
		switch strings.TrimSpace(name) {
		case "records":
			arm = duckmodel.ArmRecords
		case "intervals":
			arm = duckmodel.ArmIntervals
		default:
			return nil, fmt.Errorf("no arm %q: the arms are records and intervals", name)
		}
		if seen[arm] {
			return nil, fmt.Errorf("arm %s is listed twice", arm)
		}
		seen[arm] = true
		arms = append(arms, arm)
	}
	return arms, nil
}

func runG0(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike g0", flag.ContinueOnError)
	fs.SetOutput(stderr)
	plan := fs.String("plan", "", "the plan.json to check")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; the run is refused if the file differs")
	armList := fs.String("arms", "records,intervals", "the arms to check, comma separated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *plan == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	arms, err := parseArms(*armList)
	if err != nil {
		return err
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
	say := func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }
	fmt.Fprintf(stdout, "plan sha256 %s\n", got)
	failed := 0
	for _, arm := range arms {
		m := duckmodel.New(arm)
		rep, err := g0.Run(ctx, p, m, say)
		if err != nil {
			return fmt.Errorf("arm %s: %w", arm, err)
		}
		st, err := m.Stats()
		if err != nil {
			return fmt.Errorf("arm %s: %w", arm, err)
		}
		fmt.Fprintf(stdout, "arm %s\nstream digest %s\n", arm, rep.Stream.Digest)
		fmt.Fprintf(stdout, "stream: %d records, last seq %d, %d retentions\n", rep.Stream.Records, rep.Stream.LastSeq, len(rep.Stream.Retentions))
		for _, g := range rep.Groups {
			fmt.Fprintf(stdout, "G0 %-40s %-10s %d of %d\n", g.Group, g.Age, g.Matched, g.Queries)
		}
		fmt.Fprintf(stdout, "G0 total: %d of %d answers equal the plan's\n", rep.Matched, rep.Queries)
		fmt.Fprintf(stdout, "model rows at the end (rows of the model, not DuckDB): %d subjects, %d edges, %d records, %d intervals\n",
			st.Subjects, st.Edges, st.Records, st.Intervals)
		for _, mm := range rep.Mismatches {
			fmt.Fprintf(stdout, "MISMATCH %s: got %+v, plan %+v %s\n", mm.Query, mm.Got, mm.Want, mm.AskError)
		}
		if !rep.OK() {
			failed += rep.Queries - rep.Matched
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d answers differ from the plan's", failed)
	}
	return nil
}
