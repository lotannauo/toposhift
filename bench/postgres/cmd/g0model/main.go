// Command g0model regenerates the stream of a plan into the in-memory model of the
// Postgres interval tables and asks every question of the plan, as the first gate of
// the Postgres baseline does for a database: the model's answer must equal the
// reference engine's. It also prints what the model wrote to its interval tables,
// which is a count of rows (a function of the stream), not a measurement of Postgres.
//
//	g0model -plan plan.json [-expect-plan sha256]...
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

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "g0model:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("g0model", flag.ContinueOnError)
	fs.SetOutput(stderr)
	plan := fs.String("plan", "", "the plan.json to check")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; the run is refused if the file differs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *plan == "" || fs.NArg() != 0 {
		return fmt.Errorf("usage: g0model -plan plan.json [-expect-plan sha256]")
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
	m := pgmodel.New()
	rep, err := g0.Run(ctx, p, m, func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) })
	if err != nil {
		return err
	}
	tb, c := m.Tables(), m.Counts()
	fmt.Fprintf(stdout, "plan sha256 %s\nstream digest %s\n", got, rep.Stream.Digest)
	fmt.Fprintf(stdout, "stream: %d records, last seq %d, %d retentions\n", rep.Stream.Records, rep.Stream.LastSeq, len(rep.Stream.Retentions))
	for _, g := range rep.Groups {
		fmt.Fprintf(stdout, "G0 %-40s %-10s %d of %d\n", g.Group, g.Age, g.Matched, g.Queries)
	}
	fmt.Fprintf(stdout, "G0 total: %d of %d answers equal the plan's\n", rep.Matched, rep.Queries)
	fmt.Fprintf(stdout, "model interval rows at the end: %d edge, %d entity\n", len(tb.Edges), len(tb.Entities))
	fmt.Fprintf(stdout, "model writer counts (rows of the model, not Postgres): fast subjects %d, refolds %d, records folded %d, inserted %d, updated %d (%d heap-only, %d finalized), deleted %d\n",
		c.FastSubjects, c.Refolds, c.RecordsFolded, c.RowsInserted, c.RowsUpdated, c.RowsHeapOnly, c.RowsFinalized, c.RowsDeleted)
	for _, mm := range rep.Mismatches {
		fmt.Fprintf(stdout, "MISMATCH %s: got %+v, plan %+v %s\n", mm.Query, mm.Got, mm.Want, mm.AskError)
	}
	if !rep.OK() {
		return fmt.Errorf("%d of %d answers differ from the plan's", rep.Queries-rep.Matched, rep.Queries)
	}
	return nil
}
