package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
)

func runAdopt(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike adopt", flag.ContinueOnError)
	fs.SetOutput(stderr)
	questions := fs.String("questions", "", "the question set made from the activity file the archive was made from")
	expectQuestions := fs.String("expect-questions", "", "the SHA-256 of the question set's file, as hex; required")
	activity := fs.String("activity", "", "the activity file the archive was made from, which the questions were made from")
	archive := fs.String("archive", "", "the archive file, as it was made")
	expectArchive := fs.String("expect-archive", "", "the SHA-256 the archive file has, as hex; required")
	out := fs.String("out", "", "the directory to make: empty or absent, and outside any git work tree")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *questions == "" || *activity == "" || *archive == "" || *out == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	if *expectArchive == "" {
		return fmt.Errorf("-expect-archive is required: an archive is only read as the file that was named in advance")
	}
	qs, sum, err := loadQuestions(*questions, *expectQuestions)
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
	db, err := duckengine.Open(ctx, duckengine.Config{Mode: duckengine.ModeRead, TempDir: tmp})
	if err != nil {
		return err
	}
	defer db.Close()
	fmt.Fprintf(stdout, "questions sha256 %s\n", sum)
	m, err := duckengine.Adopt(ctx, db, *out, duckengine.FileSource(*activity, qs, sum), duckengine.AdoptOptions{Archive: *archive, SHA256: *expectArchive})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "stream digest %s\n", m.Stream.Digest)
	fmt.Fprintf(stdout, "stream: %d records, last seq %d\n", m.Stream.Records, m.Stream.LastSeq)
	fmt.Fprintf(stdout, "DuckDB %s, settings sha256 %s\n", m.DuckDB, m.SettingsSHA256)
	fmt.Fprintf(stdout, "the archive holds the activity file's %d rows (content %s)\n", m.Adopted.Records, m.Adopted.Content)
	for _, f := range m.Files {
		fmt.Fprintf(stdout, "file %-24s rows %10d row groups %5d bytes %12d sha256 %s\n", f.Path, f.Rows, f.RowGroups, f.Bytes, f.SHA256)
	}
	return nil
}
