package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
)

func runBuild(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	plan := fs.String("plan", "", "the plan.json whose stream the files are built from")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; required")
	out := fs.String("out", "", "the directory to build in: empty or absent, and outside any git work tree")
	armList := fs.String("arms", "A1,A2,I", "the arms to build: A1 (the archive), A2 (record files, both orders), I (interval files; also builds A2's files, which it reads for what an interval cannot answer)")
	verify := fs.Bool("verify-model", true, "check the interval files row for row against what the model derives, which holds every kept record's metadata in memory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	arms, err := duckengine.ParseArms(*armList)
	if err != nil {
		return err
	}
	p, sum, err := loadPlan(*plan, *expect, true)
	if err != nil {
		return err
	}
	if err := refuseInWorkTree(*out); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Clean(*out)), 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(filepath.Clean(*out)), ".duckspike-tmp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	db, err := duckengine.Open(ctx, duckengine.Config{Mode: duckengine.ModeBuild, TempDir: tmp})
	if err != nil {
		return err
	}
	defer db.Close()
	fmt.Fprintf(stdout, "plan sha256 %s\n", sum)
	m, err := duckengine.Build(ctx, db, *out, duckengine.PlanSource(p, sum), arms, duckengine.BuildOptions{VerifyModel: *verify})
	if err != nil {
		return err
	}
	printManifest(stdout, m)
	return nil
}

func printManifest(w io.Writer, m *duckengine.Manifest) {
	fmt.Fprintf(w, "stream digest %s\n", m.Stream.Digest)
	fmt.Fprintf(w, "stream: %d records written, %d kept by the retention (%d moves of the horizon), last seq %d\n",
		m.Stream.Records, m.Stream.Kept, m.Stream.Retentions, m.Stream.LastSeq)
	fmt.Fprintf(w, "DuckDB %s, settings sha256 %s\n", m.DuckDB, m.SettingsSHA256)
	for _, f := range m.Files {
		fmt.Fprintf(w, "file %-24s rows %10d row groups %5d bytes %12d sha256 %s\n", f.Path, f.Rows, f.RowGroups, f.Bytes, f.SHA256)
	}
	if !slices.Equal(m.Arms, duckengine.Arms) {
		fmt.Fprintf(w, "arms built: %v\n", m.Arms)
	}
}
