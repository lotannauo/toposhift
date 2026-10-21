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
	in := addInputFlags(fs)
	activity := fs.String("activity", "", "with -questions: the activity file the questions were made from, whose very bytes are the build's activity file")
	out := fs.String("out", "", "the directory to build in: empty or absent, and outside any git work tree")
	armList := fs.String("arms", "A1,A2,I", "the arms to build: A1 (the archive), A2 (record files, both orders), I (interval files; also builds A2's files, which it reads for what an interval cannot answer), and, as a diagnostic, H (A2's files for each hour of ingest)")
	segment := fs.Duration("segment", duckengine.DefaultSegment, "the length of an hour of ingest for the arm H")
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
	input, err := in.load()
	if err != nil {
		return err
	}
	src, err := input.source(*activity)
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
	fmt.Fprintf(stdout, "%s sha256 %s\n", input.kind, input.sum)
	m, err := duckengine.Build(ctx, db, *out, src, arms, duckengine.BuildOptions{VerifyModel: *verify, Segment: *segment})
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
	var segFiles int
	var segBytes int64
	for _, f := range m.Files {
		if f.Role == duckengine.RoleHFwd || f.Role == duckengine.RoleHRev {
			segFiles++
			segBytes += f.Bytes
			continue
		}
		fmt.Fprintf(w, "file %-24s rows %10d row groups %5d bytes %12d sha256 %s\n", f.Path, f.Rows, f.RowGroups, f.Bytes, f.SHA256)
	}
	if len(m.Segments) > 0 {
		fmt.Fprintf(w, "hours of ingest of %d nanoseconds: %d segments, %d files, %d bytes (the files are in the manifest)\n", m.SegmentNanos, len(m.Segments), segFiles, segBytes)
	}
	for _, s := range m.Segments {
		fmt.Fprintf(w, "segment %6d hour %d seq %d to %d records %d\n", s.Index, s.StartNanos, s.FirstSeq, s.LastSeq, s.Records)
	}
	if !slices.Equal(m.Arms, duckengine.Arms) {
		fmt.Fprintf(w, "arms built: %v\n", m.Arms)
	}
}
