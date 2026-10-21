package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
)

func runSizes(_ context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike sizes", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "the directory of a build")
	hashes := fs.Bool("verify-hashes", false, "also check each file's SHA-256 against the manifest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	m, err := duckengine.ReadManifest(*dir)
	if err != nil {
		return err
	}
	if err := duckengine.VerifyFiles(*dir, m, *hashes); err != nil {
		return err
	}
	rep := duckengine.Sizes(m)
	fmt.Fprintf(stdout, "records held (the activity file's rows): %d\n", rep.Records)
	if m.Adopted == nil {
		fmt.Fprintf(stdout, "activity file: %d bytes (%.2f per record)\n", rep.Activity.Bytes, float64(rep.Activity.Bytes)/float64(rep.Records))
	} else {
		fmt.Fprintf(stdout, "adopted archive of SHA-256 %s: its rows are the activity file's, whose content is %s\n", m.Adopted.ArchiveSHA256, m.Adopted.Content)
	}
	for _, a := range rep.Arms {
		fmt.Fprintf(stdout, "arm %s: %d bytes (%.2f per record held)\n", a.Arm, a.Bytes, a.BytesPerRecord)
		if a.Arm == duckengine.ArmH {
			fmt.Fprintf(stdout, "  %d files of %d hours of ingest of %d nanoseconds (the files are in the manifest)\n", len(a.Files), len(m.Segments), m.SegmentNanos)
			continue
		}
		for _, f := range a.Files {
			fmt.Fprintf(stdout, "  %-24s %12d bytes, %d rows, %d row groups\n", f.Path, f.Bytes, f.Rows, f.RowGroups)
		}
	}
	if rep.Composite != nil {
		fmt.Fprintf(stdout, "arm I with the record files it reads: %d bytes (%.2f per record held)\n", rep.Composite.Bytes, rep.Composite.BytesPerRecord)
	}
	return nil
}
