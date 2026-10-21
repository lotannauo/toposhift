// Command duckspike is the driver of the DuckDB baseline.
//
//	duckspike g0        -plan plan.json [-expect-plan sha256] [-arms records,intervals]
//	duckspike questions -activity file -expect-digest sha256:hex -start time -duration d -out file [-batch n] [-hot n] [-median n] [-min-non-empty x]
//	duckspike build     (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256 -activity file) -out dir [-arms A1,A2,I,H] [-segment d]
//	duckspike adopt     -questions file -expect-questions sha256 -activity file -archive file -expect-archive sha256 -out dir
//	duckspike read      -dir dir (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256) [-arms A1,A2,I,H] [-out dir]
//	duckspike explain   -dir dir (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256) -arm A2 [-query n,...]
//	duckspike sizes     -dir dir [-verify-hashes]
//	duckspike env
//
// g0 regenerates the stream of a plan into the in-memory model of each arm's files and
// asks every question of the plan: the model's answer must equal the reference engine's.
// It needs no DuckDB.
//
// questions makes a question set from an activity file that no generator made (one hour of
// a trace, reduced to the records a store takes): the questions the runner would choose
// from the stream, with the reference engine's answer to each. The set takes the place of
// a plan wherever one is named, and the SHA-256 of its file names it.
//
// build writes the Parquet files of the arms from the plan's stream, with the retention
// applied once, and a manifest; from a question set it writes them from the very bytes of
// the activity file the questions were made from. adopt makes a directory of one arm, A1,
// of an archive that was made before: it is checked by its SHA-256 and to hold the rows of
// the activity file, and read as it is. read opens the arms through DuckDB and asks every
// question: no counter is kept for an arm unless all the answers are the reference
// engine's, and every question is asked twice and must cost the same. explain audits the
// counting of some questions against what DuckDB read from the files. sizes reports the
// bytes of a build; env, where the binary runs. The arm H is a diagnostic: A2's files for
// each hour of ingest, read through a glob.
//
// Counters are rows, row groups and bytes. The commands print no time, and refuse an -out
// inside a git work tree: what they write is local scratch, not a result.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

const usage = `usage:
  duckspike g0        -plan plan.json [-expect-plan sha256] [-arms records,intervals]
  duckspike questions -activity file -expect-digest sha256:hex -start time -duration d -out file [-batch n] [-hot n] [-median n] [-min-non-empty x]
  duckspike build     (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256 -activity file) -out dir [-arms A1,A2,I,H] [-segment d]
  duckspike adopt     -questions file -expect-questions sha256 -activity file -archive file -expect-archive sha256 -out dir
  duckspike read      -dir dir (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256) [-arms A1,A2,I,H] [-out dir]
  duckspike explain   -dir dir (-plan plan.json -expect-plan sha256 | -questions file -expect-questions sha256) -arm A2 [-query n,...]
  duckspike sizes     -dir dir [-verify-hashes]
  duckspike env`

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	switch args[0] {
	case "g0":
		return runG0(ctx, args[1:], stdout, stderr)
	case "questions":
		return runQuestions(ctx, args[1:], stdout, stderr)
	case "build":
		return runBuild(ctx, args[1:], stdout, stderr)
	case "adopt":
		return runAdopt(ctx, args[1:], stdout, stderr)
	case "read":
		return runRead(ctx, args[1:], stdout, stderr)
	case "explain":
		return runExplain(ctx, args[1:], stdout, stderr)
	case "sizes":
		return runSizes(ctx, args[1:], stdout, stderr)
	case "env":
		return runEnv(ctx, args[1:], stdout, stderr)
	}
	return fmt.Errorf("no subcommand %q; %s", args[0], usage)
}

// loadPlan reads a plan file, refusing it if its SHA-256 is not the expected one, and
// returns the plan and the digest. An empty expectation is allowed only where the caller
// says so.
func loadPlan(path, expect string, required bool) (*runner.Plan, string, error) {
	if path == "" {
		return nil, "", errors.New("no -plan")
	}
	if required && expect == "" {
		return nil, "", errors.New("-expect-plan is required: a counter is only of a plan that was named in advance")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(b)
	got := hex.EncodeToString(sum[:])
	if expect != "" && !strings.EqualFold(expect, got) {
		return nil, "", fmt.Errorf("%s has SHA-256 %s, not the expected %s", path, got, expect)
	}
	p, err := runner.LoadPlan(path)
	if err != nil {
		return nil, "", err
	}
	return p, got, nil
}

// refuseInWorkTree refuses a directory inside a git work tree: what these commands write
// is local scratch and never a result, so it does not belong where `git add` would find
// it. The directory need not exist; its nearest ancestor that does is looked at.
func refuseInWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	p := abs
	for {
		if _, err := os.Lstat(p); err == nil {
			break
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	if real, err := filepath.EvalSymlinks(p); err == nil {
		p = real
	}
	for {
		if _, err := os.Lstat(filepath.Join(p, ".git")); err == nil {
			return fmt.Errorf("%s is inside the git work tree %s: write scratch outside it (for example under ~/spikebench/duckdb)", dir, p)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return nil
		}
		p = parent
	}
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
	p, got, err := loadPlan(*plan, *expect, false)
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
