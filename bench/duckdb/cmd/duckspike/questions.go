package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/filestream"
)

func runQuestions(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike questions", flag.ContinueOnError)
	fs.SetOutput(stderr)
	activity := fs.String("activity", "", "the activity file to make questions from")
	digest := fs.String("expect-digest", "", "the content digest the file must have (the footer's: sha256: and 64 hex digits); required")
	start := fs.String("start", "", "the start of the period the stream is about, as RFC 3339 UTC, on a whole second; required")
	duration := fs.Duration("duration", 0, "how long the period lasts: the ages are looked back from its end; required")
	out := fs.String("out", "", "the question set to write: outside any git work tree")
	batch := fs.Int("batch", filestream.DefaultBatchSize, "the records of a batch; the old snapshot token is a batch boundary")
	hot := fs.Int("hot", 10, "how many of the busiest prefixes of each class to ask about")
	median := fs.Int("median", 5, "how many prefixes around the middle of each class to ask about")
	minNonEmpty := fs.Float64("min-non-empty", filestream.DefaultMinNonEmpty, "the least share of each group of questions, at one age, with an answer that is not empty")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *activity == "" || *digest == "" || *start == "" || *duration <= 0 || *out == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	t, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		return fmt.Errorf("-start: %w", err)
	}
	if t.UTC().Format(time.RFC3339) != *start {
		return fmt.Errorf("-start %q is not UTC (as %s)", *start, t.UTC().Format(time.RFC3339))
	}
	if *batch < 1 || *hot < 1 || *median < 1 {
		return fmt.Errorf("-batch, -hot and -median must be at least 1")
	}
	if err := refuseInWorkTree(*out); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Clean(*out)), 0o755); err != nil {
		return err
	}
	say := func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }
	qs, err := filestream.Make(ctx, *activity, filestream.MakeOptions{
		Options: filestream.Options{Period: filestream.Period{Start: t.UTC(), Duration: *duration}, BatchSize: *batch, Digest: *digest},
		Hot:     *hot, Median: *median, MinNonEmpty: *minNonEmpty,
	}, say)
	if err != nil {
		return err
	}
	sum, err := qs.Save(*out)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "questions sha256 %s\n", sum)
	fmt.Fprintf(stdout, "input content digest %s, %d records\n", qs.Input.Digest, qs.Input.Records)
	fmt.Fprintf(stdout, "stream digest %s, last seq %d, old token %d\n", qs.Stream.Digest, qs.Stream.LastSeq, qs.Stream.OldToken)
	fmt.Fprintf(stdout, "%d questions over %d entities, queries digest %s\n", len(qs.Queries), qs.Shadow, qs.QueriesDigest)
	for _, g := range qs.Groups {
		fmt.Fprintf(stdout, "  %-40s %-10s %4d questions, %4d with an answer that is not empty\n", g.Group, g.Age, g.Queries, g.NonEmpty)
	}
	return nil
}
