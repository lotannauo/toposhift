// Command decaycheck replays a capture of the service graph connector's metrics
// through the product's ingest path and counts where the derived silence of a
// dependency (package decay) shows, and whether it is ever more than the log
// proves.
//
//	decaycheck -capture DIR -capture-sha256 HEX -from T -to T -out DIR [-expiries 6h,1h,20m] [-step 1m] [-lookback 0]
//
// # The pipeline
//
// Each line of the capture, an OTLP JSON object as the collector's file exporter
// writes it, becomes a protobuf MetricsData (protojson, then proto.Marshal) and is
// posted, in process, to the receiver's handler (otlphttp.MetricsHandler), exactly
// as a collector's OTLP/HTTP exporter would send it. The handler's sink hands the
// request to a real sequencer, which translates it, coalesces it with ingest's own
// coalescer and writes it to a memory store; and, when the sequencer has taken the
// request, to a second service graph translator, built as the sequencer builds its
// own and given the same requests at the same instants. Its output is kept whole,
// before the coalescer thins it: those are the sightings, the instants a producer
// saw an edge, and they are the truth the derived silence is checked against. The
// two translators must agree on how many sightings they made.
//
// The sequencer's clock and its flush tick are the replay's, in event time, as the
// demo's driver moves them: the clock is the newest point time of the line being
// sent, and a tick is sent every thirty seconds of it. Nothing reads the wall
// clock, so the same capture and flags give the same counts on any machine. One
// pass over the capture drives one pipeline for each expiry, which is both the
// service graph TTL and the span count TTL, in lockstep.
//
// # The check
//
// Instants are sampled from -from, every -step, below -to. At each, every edge the
// translator wrote is read with the interval reader's evidence option over
// [instant - lookback, instant + 1ns), twice: at the store's last sequence number
// when the replay's clock reached the instant (what a reader at that time had seen,
// as_of_instant) and at the last one when the replay ended (full). Each edge that
// exists at the instant gets its recency from decay.Of and is judged against the
// truth, the time since the latest sighting at or before the instant that the token
// knows of: the bound must not exceed it, and must fall short of it by at most the
// extension interval X. Either failing exits with status 1, with the report still
// written. The counts say, for each edge, how often it existed at a sample (members),
// how often a bound above zero was derived (shown) and how often the true silence
// was above X (the silences that can show).
//
// The check decides nothing about a half-life: the policy is the product's, and the
// counts only say how the bound and its confidence are spread over what was read.
//
// # Inputs it refuses to work without
//
// The capture's SHA-256 (of the parts joined in name order) is required and checked
// before anything is replayed, and so are the window, and an output directory that is
// not inside a git work tree or inside the capture and holds no report yet. With any
// of them missing or wrong the command exits with status 2 without replaying a line
// or writing anything. The report is result.json and report.txt in -out, and is
// printed.
//
// Its counts are local and deterministic; they are not timings and are not
// published.
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
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/query/decay"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// The exit statuses: 1 for a self-check that failed or a replay that could not be
// made, 2 for a refusal, which happens before anything is replayed.
const (
	exitFailed  = 1
	exitRefused = 2
)

// refusal is an error that is a refusal to start.
type refusal struct{ msg string }

func (e *refusal) Error() string { return e.msg }

func refuse(format string, args ...any) error { return &refusal{fmt.Sprintf(format, args...)} }

// run is main with its streams.
func run(args []string, stdout, stderr io.Writer) int {
	failed, err := execute(args, stdout, stderr)
	var r *refusal
	switch {
	case errors.As(err, &r):
		_, _ = fmt.Fprintln(stderr, "decaycheck: refused:", r.msg)
		return exitRefused
	case err != nil:
		_, _ = fmt.Fprintln(stderr, "decaycheck:", err)
		return exitFailed
	case failed:
		return exitFailed
	}
	return 0
}

// options are the flags.
type options struct {
	cfg     config
	capture string
	sum     string
	out     string
	timeout time.Duration
}

// parseArgs reads the flags and refuses what is missing or unusable.
func parseArgs(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("decaycheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	capPath := fs.String("capture", "", "the capture: a file, or a directory of .jsonl parts replayed in name order (required)")
	sum := fs.String("capture-sha256", "", "the SHA-256 of the parts joined in name order, checked before anything is replayed (required)")
	fromS := fs.String("from", "", "the first instant sampled, RFC 3339 (required)")
	toS := fs.String("to", "", "the end of the window, RFC 3339: later lines are not sent and no instant at or after it is sampled (required)")
	step := fs.Duration("step", time.Minute, "how far apart the instants sampled are")
	lookback := fs.Duration("lookback", 0, "how long before an instant a read of it starts; zero reads the one instant")
	expiries := fs.String("expiries", "6h,1h,20m", "the expiries (the TTL of the service graph and span count producers) replayed, comma separated")
	out := fs.String("out", "", "the directory the report is written to, outside any git work tree and the capture (required)")
	timeout := fs.Duration("timeout", 12*time.Hour, "the longest the replay may take")
	if err := fs.Parse(args); err != nil {
		return options{}, refuse("%v", err)
	}
	if fs.NArg() != 0 {
		return options{}, refuse("unexpected arguments %q", fs.Args())
	}
	for _, f := range []struct{ name, v string }{
		{"capture", *capPath}, {"capture-sha256", *sum}, {"from", *fromS}, {"to", *toS}, {"out", *out},
	} {
		if f.v == "" {
			return options{}, refuse("-%s is required: a run is made only on a registered capture and window", f.name)
		}
	}
	o := options{capture: *capPath, sum: strings.ToLower(*sum), out: *out, timeout: *timeout}
	if len(o.sum) != 64 || strings.Trim(o.sum, "0123456789abcdef") != "" {
		return options{}, refuse("-capture-sha256 %q is not a SHA-256 in hexadecimal", *sum)
	}
	var err error
	if o.cfg.From, err = time.Parse(time.RFC3339Nano, *fromS); err != nil {
		return options{}, refuse("-from: %v", err)
	}
	if o.cfg.To, err = time.Parse(time.RFC3339Nano, *toS); err != nil {
		return options{}, refuse("-to: %v", err)
	}
	o.cfg.From, o.cfg.To = o.cfg.From.UTC(), o.cfg.To.UTC()
	switch {
	case !o.cfg.From.Before(o.cfg.To):
		return options{}, refuse("-from %s is not before -to %s", *fromS, *toS)
	case *step <= 0:
		return options{}, refuse("-step %s is not positive", *step)
	case *lookback < 0:
		return options{}, refuse("-lookback %s is negative", *lookback)
	case *timeout <= 0:
		return options{}, refuse("-timeout %s is not positive", *timeout)
	}
	o.cfg.Step, o.cfg.Lookback = *step, *lookback
	for _, s := range strings.Split(*expiries, ",") {
		e, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return options{}, refuse("-expiries: %v", err)
		}
		if e <= 0 || e > 366*24*time.Hour {
			return options{}, refuse("-expiries: %s is not above zero and at most a year", e)
		}
		for _, have := range o.cfg.Expiries {
			if have == e {
				return options{}, refuse("-expiries: %s is given twice", e)
			}
		}
		o.cfg.Expiries = append(o.cfg.Expiries, e)
	}
	o.cfg.Policy = decay.DefaultPolicy()
	if want := sequencer.CoalesceConfig().ExtendTTLFraction; o.cfg.Policy.ExtendFraction != want {
		return options{}, refuse("the decay policy's extension fraction %g is not the coalescer's %g: the bound would not mean what it says", o.cfg.Policy.ExtendFraction, want)
	}
	for _, e := range o.cfg.Expiries {
		if o.cfg.Policy.Extension(e) <= 0 {
			return options{}, refuse("-expiries: %s has no extension interval", e)
		}
	}
	return o, nil
}

// execute checks the inputs, replays the capture, prints the report and writes it
// to -out. It reports whether a self-check failed.
func execute(args []string, stdout, stderr io.Writer) (failed bool, err error) {
	o, err := parseArgs(args, stderr)
	if err != nil {
		return false, err
	}
	if err := refuseOutput(o.out, o.capture); err != nil {
		return false, err
	}
	parts, err := listParts(o.capture)
	if err != nil {
		return false, refuse("-capture: %v", err)
	}
	sum, size, err := digestParts(parts)
	if err != nil {
		return false, err
	}
	if sum != o.sum {
		return false, refuse("the capture's SHA-256 is %s, not the %s given: it is not the registered capture", sum, o.sum)
	}
	o.cfg.Parts = parts
	o.cfg.Progress = stderr
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	lines, runs, err := replay(ctx, o.cfg)
	if err != nil {
		return false, err
	}
	rep := &report{
		Tool: "decaycheck", Commit: buildCommit(), Command: append([]string{"decaycheck"}, args...),
		Capture: inputInfo{SHA256: sum, Bytes: size},
		Window: window{
			From: o.cfg.From.Format(time.RFC3339Nano), To: o.cfg.To.Format(time.RFC3339Nano),
			Step: o.cfg.Step, Lookback: o.cfg.Lookback,
		},
		Policy: o.cfg.Policy, Lines: lines, Runs: runs,
	}
	for _, p := range parts {
		rep.Capture.Parts = append(rep.Capture.Parts, filepath.Base(p))
	}
	if err := rep.seal(); err != nil {
		return false, err
	}
	var text bytes.Buffer
	if err := writeText(io.MultiWriter(stdout, &text), rep); err != nil {
		return false, err
	}
	js, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return false, err
	}
	err = errors.Join(
		writeNew(filepath.Join(o.out, resultFile), append(js, '\n')),
		writeNew(filepath.Join(o.out, reportFile), text.Bytes()),
	)
	if err != nil {
		return false, err
	}
	_, _ = fmt.Fprintln(stderr, "decaycheck: report written to", o.out)
	return len(rep.failures()) > 0, nil
}

// The files of a report.
const (
	resultFile = "result.json"
	reportFile = "report.txt"
)

// writeNew writes a file that must not exist.
func writeNew(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	_, err = f.Write(data)
	return err
}

// refuseOutput refuses an output directory inside a git work tree or inside the
// capture, and one that already holds a report: a report is never written over.
func refuseOutput(dir, capPath string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for d := abs; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			return refuse("-out %s is inside the git work tree %s: the report is not written into a repository", dir, d)
		}
		if filepath.Dir(d) == d {
			break
		}
	}
	if capAbs, err := filepath.Abs(capPath); err == nil {
		if rel, err := filepath.Rel(capAbs, abs); err == nil && (rel == "." || !strings.HasPrefix(rel, "..")) {
			return refuse("-out %s is inside the capture: the capture is read only", dir)
		}
		if rel, err := filepath.Rel(filepath.Dir(capAbs), abs); err == nil && rel == "." {
			return refuse("-out %s holds the capture file: the capture is read only", dir)
		}
	}
	for _, name := range []string{resultFile, reportFile} {
		if _, err := os.Lstat(filepath.Join(abs, name)); err == nil {
			return refuse("-out %s already holds %s: a report is never written over", dir, name)
		}
	}
	return nil
}
