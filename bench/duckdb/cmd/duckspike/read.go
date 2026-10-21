package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/duckdb/g0"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func runRead(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike read", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "the directory of a build")
	plan := fs.String("plan", "", "the plan.json the build was made from")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; required")
	armList := fs.String("arms", "A1,A2,I", "the arms to read")
	out := fs.String("out", "", "a directory to write each arm's counters file in: outside any git work tree")
	hashes := fs.Bool("verify-hashes", true, "check each file's SHA-256 against the manifest before reading")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	arms, err := duckengine.ParseArms(*armList)
	if err != nil {
		return err
	}
	if *out != "" {
		if err := refuseInWorkTree(*out); err != nil {
			return err
		}
	}
	p, sum, err := loadPlan(*plan, *expect, true)
	if err != nil {
		return err
	}
	m, err := duckengine.ReadManifest(*dir)
	if err != nil {
		return err
	}
	if err := checkBuildIsOfPlan(m, p, sum); err != nil {
		return err
	}
	mb, err := os.ReadFile(filepath.Join(*dir, duckengine.ManifestName))
	if err != nil {
		return err
	}
	msum := sha256.Sum256(mb)
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
	}
	say := func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }
	fmt.Fprintf(stdout, "plan sha256 %s\n", sum)
	for _, arm := range arms {
		cf, err := readArm(ctx, *dir, arm, p, say, stdout, *hashes)
		if err != nil {
			return fmt.Errorf("arm %s: %w", arm, err)
		}
		cf.Plan = duckengine.PlanRef{SHA256: sum, StreamDigest: p.Stream.Digest, QueriesDigest: p.QueriesDigest, Queries: len(p.Queries)}
		cf.ManifestSHA256 = hex.EncodeToString(msum[:])
		if *out != "" {
			b, err := json.MarshalIndent(cf, "", "  ")
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(*out, string(arm)+".counters.json"), append(b, '\n'), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkBuildIsOfPlan refuses a build made from another plan or stream.
func checkBuildIsOfPlan(m *duckengine.Manifest, p *runner.Plan, sum string) error {
	switch {
	case m.Stream.Label != sum:
		return fmt.Errorf("the build was made from the plan with SHA-256 %s, not %s", m.Stream.Label, sum)
	case m.Stream.Digest != p.Stream.Digest:
		return fmt.Errorf("the build's stream digest is %s, the plan's is %s", m.Stream.Digest, p.Stream.Digest)
	case m.Stream.LastSeq != p.Stream.LastSeq || m.Stream.Records != p.Stream.Records:
		return fmt.Errorf("the build's stream is %d records to seq %d, the plan's %d to %d", m.Stream.Records, m.Stream.LastSeq, p.Stream.Records, p.Stream.LastSeq)
	}
	return nil
}

// readArm is the gate and the count for one arm: G0 first, and counters only if every
// answer is the plan's.
func readArm(ctx context.Context, dir string, arm duckengine.Arm, p *runner.Plan, say runner.Progress, stdout io.Writer, hashes bool) (*duckengine.CountersFile, error) {
	tmp, err := os.MkdirTemp("", "duckspike-read-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	db, err := duckengine.Open(ctx, duckengine.Config{Mode: duckengine.ModeRead, TempDir: tmp, Profile: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	r, err := duckengine.OpenReader(ctx, db, dir, arm, hashes)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	env, err := duckengine.Environment(ctx, db)
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(stdout, "arm %s\n", arm)
	rep, err := g0.Check(ctx, p, r, say)
	if err != nil {
		return nil, err
	}
	for _, g := range rep.Groups {
		fmt.Fprintf(stdout, "G0 %-40s %-10s %d of %d\n", g.Group, g.Age, g.Matched, g.Queries)
	}
	fmt.Fprintf(stdout, "G0 total: %d of %d answers equal the plan's\n", rep.Matched, rep.Queries)
	if !rep.OK() {
		for _, mm := range rep.Mismatches {
			fmt.Fprintf(stdout, "MISMATCH %s: got %+v, plan %+v %s\n", mm.Query, mm.Got, mm.Want, mm.AskError)
		}
		return nil, fmt.Errorf("%d answers differ from the plan's: no counter is kept", rep.Queries-rep.Matched)
	}
	if err := db.Check(ctx); err != nil {
		return nil, err
	}
	qs, err := duckengine.Collect(ctx, r, p)
	if err != nil {
		return nil, err
	}
	groups := duckengine.Summarize(qs)
	fmt.Fprintf(stdout, "work counters (every question asked twice, the same each time): rows scanned are the rows of the row groups admitted, from the footer; admitted row groups are those whose bounds and bloom filters the filters do not exclude; the profiler's scan cardinality and, as a diagnostic, the row groups admitted by bounds alone are beside them:\n")
	for _, g := range groups {
		bound := ""
		if g.Bound {
			bound = " (bound: an IN-list upper bound, in G0 and in no slope class)"
		}
		fmt.Fprintf(stdout, "  %-40s %-10s n %4d  rows scanned %12d  row groups admitted %7d of %7d  bytes admitted %13d  profiler cardinality %9d  (diagnostic, bounds alone: %7d)%s\n",
			g.Group, g.Age, g.Queries, g.RowsScanned, g.GroupsAdmitted, g.GroupsTotal, g.BytesAdmitted, g.ProfilerCardinality, g.GroupsBoundsOnly, bound)
	}
	return &duckengine.CountersFile{
		Format: duckengine.CountersFormat, Env: env, Arm: arm, Passes: 2, Groups: groups, Queries: qs,
	}, nil
}
