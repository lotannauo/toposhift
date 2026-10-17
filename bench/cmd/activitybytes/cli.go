package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// tsvColumns are the columns of the line on standard output, in order.
var tsvColumns = []string{
	"label", "records", "keys", "checkpoints", "checkpoints_entity", "checkpoints_edge",
	"live_table_bytes", "blob_live_bytes", "bytes_per_record", "bytes_per_sample",
	"tables_per_level", "data_digest", "checkpoint_setting", "input_digest",
}

// tsvRow is the result as the line of tab-separated values. A figure that does not
// exist for the measurement is "-".
func tsvRow(r *result) string {
	opt := func(v *float64) string {
		if v == nil {
			return "-"
		}
		return strconv.FormatFloat(*v, 'f', 4, 64)
	}
	levels := make([]string, len(r.TablesPerLevel))
	for i, n := range r.TablesPerLevel {
		levels[i] = strconv.FormatInt(n, 10)
	}
	digest := r.InputDigest
	if digest == "" {
		digest = "-"
	}
	f := []string{
		r.Label, i64(r.Records), i64(r.Keys), i64(r.Checkpoints), i64(r.CheckpointsEntity), i64(r.CheckpointsEdge),
		strconv.FormatUint(r.LiveTableBytes, 10), strconv.FormatUint(r.BlobLiveBytes, 10), opt(r.BytesPerRecord), opt(r.BytesPerSample),
		strings.Join(levels, ","), r.DataDigest, r.CheckpointSetting, digest,
	}
	return strings.Join(f, "\t")
}

func i64(n int64) string { return strconv.FormatInt(n, 10) }

// writeJSON writes the result to dir/<label>.json, which must not exist.
func writeJSON(dir string, r *result) (err error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, fileLabel(r.Label)+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	_, err = f.Write(append(b, '\n'))
	return err
}

// run is main with its streams: it returns the exit status, 1 if the measurement
// failed and 2 for bad usage.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("activitybytes", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := config{Progress: stderr}
	fs.StringVar(&cfg.In, "in", "", "the activity file to weigh (required unless -empty)")
	fs.BoolVar(&cfg.Empty, "empty", false, "weigh an empty store, opened as for a file (refused with -in, -expect-records and -samples)")
	fs.StringVar(&cfg.Out, "out", "", "the directory the store is made in and the result is written to, never inside a git work tree (required)")
	fs.StringVar(&cfg.Label, "label", "", "the name of the measurement, [A-Za-z0-9/._-]+ (required)")
	fs.StringVar(&cfg.Checkpoints, "checkpoints", checkpointsDefault, "the store's checkpoint policy: default or off")
	fs.IntVar(&cfg.Batch, "batch", DefaultBatch, "records per write")
	fs.Int64Var(&cfg.ExpectRecords, "expect-records", 0, "the records the file must hold, checked against its footer before any write and against the count written after (0: not checked)")
	fs.Int64Var(&cfg.Samples, "samples", 0, "the samples the records assert, for the bytes per sample (0: left out)")
	fs.BoolVar(&cfg.Keep, "keep", false, "keep the store after weighing")
	header := fs.Bool("header", false, "print the header line before the row")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: activitybytes -in FILE -out DIR -label LABEL [flags]  |  activitybytes -empty -out DIR -label LABEL")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return 2
	}
	if err := cfg.validate(); err != nil {
		_, _ = fmt.Fprintln(stderr, "activitybytes:", err)
		return 2
	}
	res, _, err := weigh(context.Background(), cfg)
	if err == nil {
		err = writeJSON(cfg.Out, res)
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "activitybytes:", err)
		return 1
	}
	if *header {
		_, _ = fmt.Fprintln(stdout, strings.Join(tsvColumns, "\t"))
	}
	_, _ = fmt.Fprintln(stdout, tsvRow(res))
	return 0
}
