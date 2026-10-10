// Command producercost measures what a longer producer name costs a store.
//
// It reads a capture of the Kubernetes objects receiver (the files run1.jsonl and
// run2.jsonl of a directory), translates it with the real translator, coalesces it with
// the real coalescer and writes it to the real store with no record, with producer names
// of 18 bytes (the baseline), and with each length of -lengths (default 32 and 64). Each store
// is flushed, compacted to the bottom level and left to rest before its tables are weighed. It prints one table and
// the cost lines:
//
//	cost_N = (live_N - live18) / (live18 - liveEmpty), for each N of -lengths
//
// where live is the bytes of the live tables, and the empty store (which holds only the
// store's own meta keys) is the fixed overhead taken off. The numbers are local and
// informational: they say what this capture costs, not what a deployment will.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

func main() {
	home, _ := os.UserHomeDir()
	dir := flag.String("capture", filepath.Join(home, "spikebench", "kwok-capture", "2026-10-12", "out"), "the directory of the capture: run1.jsonl, then run2.jsonl")
	tmp := flag.String("tmp", "", "the directory the stores are made in (default: the system's temporary directory); each is removed when it has been weighed")
	lengths := flag.String("lengths", "32,64", "the producer name lengths to measure besides the baseline of 18 bytes, each from 14 to 64 and none 18")
	timeout := flag.Duration("timeout", 10*time.Minute, "the longest the measurement may take")
	flag.Parse()
	ns, err := parseLengths(*lengths)
	if err == nil {
		err = report(os.Stdout, *dir, *tmp, ns, *timeout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "producercost:", err)
		os.Exit(1)
	}
}

func report(w io.Writer, dir, tmp string, lengths []int, timeout time.Duration) error {
	in, err := loadCapture(filepath.Join(dir, "run1.jsonl"), filepath.Join(dir, "run2.jsonl"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := run(ctx, tmp, in, lengths)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "N\tcapture log records\ttranslated\twritten (records)\tlive table bytes\tbytes written\tlive bytes per record\tmean payload bytes\t")
	for _, r := range res.Rows() {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t\n", r.Label, r.Captured, r.Translated, r.Records, r.LiveTableBytes, r.BytesWritten, r.perRecord(), r.meanPayload())
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, r := range res.Lengths {
		cost, err := res.CostOf(r)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "cost_a(%s) = (live_%s - live_18) / (live_18 - live_empty) = (%d - %d) / (%d - %d) = %.4f (%.2f%%)\n",
			r.Label, r.Label, r.LiveTableBytes, res.Baseline.LiveTableBytes, res.Baseline.LiveTableBytes, res.Empty.LiveTableBytes, cost, cost*100); err != nil {
			return err
		}
	}
	return nil
}
