package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
)

func runEnv(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike env", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	tmp, err := os.MkdirTemp("", "duckspike-env-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	db, err := duckengine.Open(ctx, duckengine.Config{Mode: duckengine.ModeRead, TempDir: tmp})
	if err != nil {
		return err
	}
	defer db.Close()
	e, err := duckengine.Environment(ctx, db)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(e)
}
