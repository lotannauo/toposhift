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
	fmt.Fprintf(stdout, "activity file: %d bytes (%.2f per record)\n", rep.Activity.Bytes, float64(rep.Activity.Bytes)/float64(rep.Records))
	for _, a := range rep.Arms {
		fmt.Fprintf(stdout, "arm %s: %d bytes (%.2f per record held)\n", a.Arm, a.Bytes, a.BytesPerRecord)
		for _, f := range a.Files {
			fmt.Fprintf(stdout, "  %-24s %12d bytes, %d rows, %d row groups\n", f.Path, f.Bytes, f.Rows, f.RowGroups)
		}
	}
	if rep.Composite != nil {
		fmt.Fprintf(stdout, "arm I with the record files it reads: %d bytes (%.2f per record held)\n", rep.Composite.Bytes, rep.Composite.BytesPerRecord)
	}
	return nil
}
