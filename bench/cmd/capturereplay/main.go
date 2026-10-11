// Command capturereplay replays a capture of a k3s cluster through both translators, the
// coalescer and both stores, and compares what the stores say exists with what the cluster
// itself listed.
//
// The capture is a directory made by the k3s capture script: out/run1.jsonl to run5.jsonl
// (OTLP JSON lines from a collector that was restarted between runs), and logs/ with the
// truth files (truth-pods.tsv and truth-nodes.tsv, a listing every 30 seconds), the
// workload log (when each run started and stopped, which pods were deleted while the
// collector was down) and sizes.txt. The command never writes under it.
//
// Each record of a run goes, in the order of the file, to the cluster translator if it is
// an entity event and to the objects translator if not; their records go through one
// coalescer, and what is left is written, a batch for each line, to a memory store and a
// Pebble store. This is done twice: in pass A one translator pair and one coalescer see
// all five runs (toposhift kept running while the collector restarted), and in pass B a
// new pair and a new coalescer start at each run boundary (toposhift restarted with the
// collector). Then every pod and node UID of the truth is asked about at every sample
// time. A pair within the margin of a UID's first or last presence is not judged.
//
// The report is printed, and written to result.json and report.txt in the -out directory,
// which is never inside a git work tree. Its numbers are local and informational: they
// say what this capture shows, and they are never published.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	home, _ := os.UserHomeDir()
	capDir := flag.String("capture", filepath.Join(home, "spikebench", "k3s-capture", "run1"), "the capture directory (out/run1.jsonl .. run5.jsonl, logs/)")
	out := flag.String("out", "", "the directory the report is written to (default: a new directory under ~/Downloads); never inside a git work tree")
	tmp := flag.String("tmp", "", "the directory the stores are made in (default: the system's temporary directory); each store is removed at the end")
	keep := flag.Bool("keep", false, "leave the stores' directories in place")
	margin := flag.Duration("margin", 60*time.Second, "pairs within this of a UID's first or last presence in the truth are not judged")
	timeout := flag.Duration("timeout", 2*time.Hour, "the longest the replay may take")
	flag.Parse()

	dir := *out
	if dir == "" {
		dir = filepath.Join(home, "Downloads", "toposhift_k3s_replay_"+time.Now().Format("2006-01-02_150405"))
	}
	failed, err := execute(config{Capture: *capDir, Tmp: *tmp, Keep: *keep, Margin: *margin, Progress: os.Stderr}, dir, os.Stdout, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "capturereplay:", err)
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

// execute replays the capture, prints the report to w and writes it to dir. It reports
// whether a self-check failed.
func execute(cfg config, dir string, w io.Writer, timeout time.Duration) (failed bool, err error) {
	if err := refuseWorkTree(dir); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rep, err := build(ctx, cfg)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	var text bytes.Buffer
	if err := writeText(io.MultiWriter(w, &text), rep); err != nil {
		return false, err
	}
	js, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return false, err
	}
	err = errors.Join(
		os.WriteFile(filepath.Join(dir, "result.json"), js, 0o644),
		os.WriteFile(filepath.Join(dir, "report.txt"), text.Bytes(), 0o644),
	)
	fmt.Fprintln(os.Stderr, "capturereplay: report written to", dir)
	return len(rep.Failures) > 0, err
}

// refuseWorkTree is an error if dir, or the nearest directory above it that exists, is
// inside a git work tree: the report is not written into a repository.
func refuseWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for d := abs; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			return fmt.Errorf("%s is inside the git work tree %s: the report is not written into a repository", dir, d)
		}
		if filepath.Dir(d) == d {
			return nil
		}
	}
}
