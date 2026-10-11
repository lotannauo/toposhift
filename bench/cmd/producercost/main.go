// Command producercost measures what a longer producer name costs a store.
//
// It reads a capture of the Kubernetes receivers (the files of -files in a directory,
// run1.jsonl and run2.jsonl by default, each in the order of replay), translates it with
// the real translators, coalesces it with the real coalescer and writes it to the real
// store with no record, with producer names of 18 bytes (the baseline), and with each length
// of -lengths (default 32). Each store is flushed, compacted to the bottom level and
// left to rest before its tables are weighed. It prints one table and the cost lines:
//
//	cost_N = (live_N - live18) / (live18 - liveEmpty), for each N of -lengths
//
// where live is the bytes of the live tables, and the empty store (which holds only the
// store's own meta keys) is the fixed overhead taken off. The numbers are local and
// informational: they say what this capture costs, not what a deployment will.
//
// -translators says which translators the capture goes through. "objects", the default,
// is the translator of the objects receiver alone, with a fixed TTL and a fixed bound on
// the age of a coalesced run. "cluster" is the translator of the cluster receiver's entity
// events alone, and "both" is the shipped default: every record goes to the translator of
// its kind, each with a producer of exactly N bytes (k8sobjects/... and k8scluster/...),
// and both write to one coalescer, whose runs are bounded by their own extension interval,
// and one store. With the cluster translator the table has two more columns, the records
// each translator made, and a last line says what the cluster translator set aside.
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
	dir := flag.String("capture", filepath.Join(home, "spikebench", "kwok-capture", "2026-10-12", "out"), "the directory of the capture")
	files := flag.String("files", defaultFiles, "the names of the files of the capture to read, in order, each in the order of replay")
	translators := flag.String("translators", defaultTranslators, "the translators the capture goes through: objects, cluster or both")
	tmp := flag.String("tmp", "", "the directory the stores are made in (default: the system's temporary directory); each is removed when it has been weighed")
	lengths := flag.String("lengths", "32", "the producer name lengths to measure besides the baseline of 18 bytes, each from 14 to 32 (the longest the store writes) and none 18")
	timeout := flag.Duration("timeout", 10*time.Minute, "the longest the measurement may take")
	flag.Parse()
	ns, err := parseLengths(*lengths)
	var names []string
	if err == nil {
		names, err = parseFiles(*files)
	}
	var m mode
	if err == nil {
		m, err = parseTranslators(*translators)
	}
	if err == nil {
		err = report(os.Stdout, *dir, names, *tmp, m, ns, *timeout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "producercost:", err)
		os.Exit(1)
	}
}

func report(w io.Writer, dir string, files []string, tmp string, m mode, lengths []int, timeout time.Duration) error {
	in, err := loadDir(dir, files)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := run(ctx, tmp, in, lengths, m)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	header := "N\tcapture log records\ttranslated\twritten (records)\tlive table bytes\tbytes written\tlive bytes per record\tmean payload bytes\t"
	if m != objectsOnly {
		header += "translated by k8sobjects\ttranslated by k8scluster\t"
	}
	fmt.Fprintln(tw, header)
	for _, r := range res.Rows() {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t", r.Label, r.Captured, r.Translated, r.Records, r.LiveTableBytes, r.BytesWritten, r.perRecord(), r.meanPayload())
		if m != objectsOnly {
			fmt.Fprintf(tw, "%d\t%d\t", r.ByObjects, r.ByCluster)
		}
		fmt.Fprintln(tw)
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
	if m.usesCluster() {
		// The same at every length: the producer name changes no decision of the translator.
		if line := skippedLine(res.Baseline.ClusterSkipped); line != "" {
			if _, err := fmt.Fprintf(w, "k8scluster set aside, at every N: %s\n", line); err != nil {
				return err
			}
		}
	}
	return nil
}
