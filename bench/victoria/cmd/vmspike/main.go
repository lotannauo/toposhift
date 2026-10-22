// Command vmspike is the driver of the metrics-store baseline.
//
//	vmspike convert   INPUT -out dir [-runs 2]
//	vmspike facts     INPUT [-coalesced file -extend-fraction 0.5] -out dir
//	vmspike questions -activity file -out dir
//	vmspike load      INPUT SERVER -out dir
//	vmspike merge     SERVER -out dir
//	vmspike sizes     SERVER -data dir -out dir
//	vmspike read      INPUT SERVER -out dir
//	vmspike env       [SERVER]
//
// INPUT is -plan plan.json -expect-plan sha256 (a synthetic plan) or -activity file
// [-ttl 15m] [-questions q.json -expect-questions sha256] (a recorded stream). SERVER is
// -server url [-flags scripts/flags.conf] [-script scripts/vm-server.sh] [-container name];
// the url defaults to $TOPOSHIFT_VM_URL.
//
// convert turns the input's every-sample stream into the samples a metrics store would
// hold and counts them (samples, markers, series, churn, the digest of the samples), twice:
// the counts are called deterministic only if the runs agree. facts checks, on the full
// input, that the coalesced stream the reference's answers come from and the every-sample
// stream written to the store describe the same existence. questions chooses the
// questions of an activity file, which has no plan, and records the reference's answers.
// load writes the samples into a server and checks the server's own counters against the
// converter's; merge forces the merged state; sizes reads the bytes; read asks the
// questions of the store and sorts the answers against the reference; env records where
// the run is made and checks the server's flags, version and image.
//
// Every command refuses an -out inside a git work tree: what they write is local
// scratch, not a result. The commands print no time. The bytes of a merged data
// directory are a figure only from CI hardware.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vminput"
	"github.com/lotannauo/toposhift/bench/victoria/vmrun"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "vmspike:", err)
		os.Exit(1)
	}
}

const usage = `usage:
  vmspike convert   INPUT -out dir [-runs 2]
  vmspike facts     INPUT [-coalesced file -extend-fraction 0.5] -out dir
  vmspike questions -activity file -out dir
  vmspike load      INPUT SERVER -out dir
  vmspike merge     SERVER -out dir
  vmspike sizes     SERVER -data dir -out dir
  vmspike read      INPUT SERVER -out dir
  vmspike env       [SERVER]
INPUT:  -plan plan.json -expect-plan sha256 | -activity file [-ttl 15m] [-questions q.json -expect-questions sha256]
SERVER: -server url [-flags scripts/flags.conf] [-script scripts/vm-server.sh] [-container name]`

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	rest := args[1:]
	switch args[0] {
	case "convert":
		return runConvert(ctx, rest, stdout, stderr)
	case "facts":
		return runFacts(ctx, rest, stdout, stderr)
	case "questions":
		return runQuestions(ctx, rest, stdout, stderr)
	case "load":
		return runLoad(ctx, rest, stdout, stderr)
	case "merge":
		return runMerge(ctx, rest, stdout, stderr)
	case "sizes":
		return runSizes(ctx, rest, stdout, stderr)
	case "read":
		return runRead(ctx, rest, stdout, stderr)
	case "env":
		return runEnv(ctx, rest, stdout, stderr)
	}
	return fmt.Errorf("no subcommand %q; %s", args[0], usage)
}

// inputFlags are the flags that name an input.
type inputFlags struct {
	plan, expectPlan           *string
	activity                   *string
	ttl                        *time.Duration
	questions, expectQuestions *string
}

func addInputFlags(fs *flag.FlagSet) *inputFlags {
	return &inputFlags{
		plan:            fs.String("plan", "", "the plan.json whose every-sample stream is the input"),
		expectPlan:      fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; required with -plan: the run is refused if the file differs"),
		activity:        fs.String("activity", "", "an activity file (Parquet) of every-sample records, such as the Alibaba placement file"),
		ttl:             fs.Duration("ttl", 0, "the TTL the activity file's records carry (default 15m, the Alibaba C300 stream's)"),
		questions:       fs.String("questions", "", "the questions file of the activity file (vmspike questions)"),
		expectQuestions: fs.String("expect-questions", "", "the SHA-256 of the questions file, as hex; required with -questions"),
	}
}

func (f *inputFlags) open() (*vmrun.Input, error) {
	switch {
	case *f.plan != "" && *f.activity != "":
		return nil, errors.New("give -plan or -activity, not both")
	case *f.plan != "":
		return vmrun.LoadPlanInput(*f.plan, *f.expectPlan)
	case *f.activity != "":
		return vmrun.ActivityInput(*f.activity, vmrun.ActivityOptions{TTL: *f.ttl, Questions: *f.questions, ExpectQuestions: *f.expectQuestions})
	}
	return nil, errors.New("give -plan or -activity")
}

// serverFlags are the flags that name the server and what it is checked against.
type serverFlags struct {
	url, flagsPath, script, container *string
}

func addServerFlags(fs *flag.FlagSet) *serverFlags {
	return &serverFlags{
		url:       fs.String("server", os.Getenv("TOPOSHIFT_VM_URL"), "the server's address on loopback, such as http://127.0.0.1:8428 (default $TOPOSHIFT_VM_URL)"),
		flagsPath: fs.String("flags", "scripts/flags.conf", "the registered server flags: the server is refused if its flags differ"),
		script:    fs.String("script", "scripts/vm-server.sh", "the server script, whose image line is the registered image"),
		container: fs.String("container", "", "the server's container: it is refused unless it was started from the registered image"),
	}
}

func (f *serverFlags) client() (*vmengine.Client, error) {
	if *f.url == "" {
		return nil, errors.New("no -server (and no $TOPOSHIFT_VM_URL)")
	}
	return vmengine.NewClient(*f.url)
}

func (f *serverFlags) env() vmrun.EnvOptions {
	return vmrun.EnvOptions{FlagsPath: *f.flagsPath, ScriptPath: *f.script, Container: *f.container}
}

// parse parses the flags, refuses stray arguments and a missing or unsafe -out.
func parse(fs *flag.FlagSet, args []string, stderr io.Writer) error {
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments %q; %s", fs.Args(), usage)
	}
	return nil
}

func outDir(out string) (string, error) {
	if out == "" {
		return "", errors.New("no -out: a directory outside any git work tree")
	}
	return out, vmrun.RefuseInWorkTree(out)
}

func runConvert(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike convert", flag.ContinueOnError)
	in := addInputFlags(fs)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory to write convert.json in: outside any git work tree")
	runs := fs.Int("runs", 2, "how many conversions must agree (at least 2)")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	input, err := in.open()
	if err != nil {
		return err
	}
	rep, err := vmrun.Convert(ctx, input, *runs, srv.env())
	if err != nil {
		return err
	}
	if err := vmrun.WriteJSON(dir, "convert.json", rep); err != nil {
		return err
	}
	c := rep.Counts
	fmt.Fprintf(stdout, "input %s, registry sha256 %s\n", rep.Input.Kind, rep.Input.RegistrySHA256)
	fmt.Fprintf(stdout, "every-sample stream digest %s (%d records, last seq %d)\n", rep.Every.Digest, rep.Every.Records, rep.Every.LastSeq)
	if rep.Input.CoalescedStreamDigest != "" {
		fmt.Fprintf(stdout, "coalesced stream digest   %s\n", rep.Input.CoalescedStreamDigest)
	}
	fmt.Fprintf(stdout, "%s after %d equal runs:\n", rep.Label, rep.Runs)
	fmt.Fprintf(stdout, "  samples %d (%d scraped, %d reasserted), markers %d, series %d (%d entity, %d edge)\n", c.Samples, c.Scraped, c.Reasserted, c.Markers, c.Series, c.EntitySeries, c.EdgeSeries)
	fmt.Fprintf(stdout, "  same-millisecond collapses %d (%d observes, %d deletes)\n", c.Collapses(), c.CollapsedObserves, c.CollapsedDeletes)
	fmt.Fprintf(stdout, "  hours with new series %d, months spanned %d\n", len(c.Churn), rep.Months)
	fmt.Fprintf(stdout, "  samples digest %s\n", rep.SamplesDigest)
	return nil
}

func runFacts(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike facts", flag.ContinueOnError)
	in := addInputFlags(fs)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory to write facts.json in: outside any git work tree")
	coalesced := fs.String("coalesced", "", "with -activity: the coalesced file the every-sample file is compared with")
	fraction := fs.Float64("extend-fraction", vminput.DefaultExtendFraction, "with -activity: the share of a run's TTL after which the coalescer that wrote -coalesced re-asserts it")
	strict := fs.Bool("strict-band", false, "also fail if the largest amount any end is earlier is not below the smallest band of any subject compared (it is reported either way)")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	input, err := in.open()
	if err != nil {
		return err
	}
	rep, err := vmrun.Facts(ctx, input, *coalesced, *fraction, *strict, srv.env())
	if err != nil {
		return err
	}
	if err := vmrun.WriteJSON(dir, "facts.json", rep); err != nil {
		return err
	}
	f := rep.Facts
	fmt.Fprintf(stdout, "input %s: the two streams describe the same existence (%s)\n", rep.Input.Kind, rep.Label)
	fmt.Fprintf(stdout, "  subjects %d, identical %d, ends earlier %d, largest amount earlier %s, smallest band %s (largest early end below the smallest band: %t)\n", f.Subjects, f.Identical, f.EndsEarlier, f.MaxEarly, f.SmallestBand, rep.WithinSmallestBand)
	if rep.Input.Kind == vmrun.KindPlan {
		fmt.Fprintf(stdout, "  projected %t (%d pinned entities), retentions %d\n", rep.Projected, rep.PinnedEntities, rep.Retentions)
	}
	return nil
}

func runQuestions(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike questions", flag.ContinueOnError)
	activity := fs.String("activity", "", "the activity file to choose the questions from")
	out := fs.String("out", "", "the directory to write questions.json in: outside any git work tree")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	if *activity == "" {
		return errors.New("no -activity")
	}
	say := func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }
	q, err := vminput.BuildQuestions(ctx, *activity, say)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "questions.json")
	if err := q.Save(path); err != nil {
		return err
	}
	sum, err := vmrun.FileSHA256(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "questions sha256 %s\n", sum)
	fmt.Fprintf(stdout, "file digest %s (%d records, last seq %d); questions digest %s\n", q.Digest, q.Records, q.LastSeq, q.QueriesDigest)
	for _, g := range q.Groups {
		fmt.Fprintf(stdout, "  %-40s %-10s %4d questions, %4d with an answer\n", g.Group, g.Age, g.Queries, g.NonEmpty)
	}
	return nil
}

func runLoad(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike load", flag.ContinueOnError)
	in := addInputFlags(fs)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory to write load.json in: outside any git work tree")
	size := fs.Int("request-samples", 0, "samples a remote-write request holds (default 10,000)")
	wait := fs.Duration("wait", 0, "how long to wait for the server's counters to arrive (default a minute)")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	input, err := in.open()
	if err != nil {
		return err
	}
	c, err := srv.client()
	if err != nil {
		return err
	}
	rep, err := vmrun.Load(ctx, input, c, vmrun.LoadOptions{RequestSamples: *size, Wait: *wait, Env: srv.env()})
	if rep != nil {
		if werr := vmrun.WriteJSON(dir, "load.json", rep); werr != nil && err == nil {
			err = werr
		}
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "loaded %d rows (%d samples, %d markers) in %d series, %d requests\n", rep.Samples, rep.Counts.Samples, rep.Counts.Markers, rep.Counts.Series, rep.Requests)
	fmt.Fprintf(stdout, "the server's counters equal the converter's: received %d, added %d, ignored %d, series created %d\n", rep.Server.Received, rep.Server.Added, rep.Server.Ignored, rep.Server.NewSeries)
	fmt.Fprintf(stdout, "samples digest %s, server %s %s\n", rep.SamplesDigest, rep.Env.Version, rep.Env.Image)
	return nil
}

func runMerge(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike merge", flag.ContinueOnError)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory load.json is in, and merge.json is written to")
	settle := fs.Duration("settle", 0, "how often the server's counters are read while waiting for it to be idle (default 1s)")
	stable := fs.Int("stable", 0, "how many equal reads in a row say it is idle (default 3)")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	var load vmrun.LoadReport
	if err := vmrun.ReadJSON(dir, "load.json", &load); err != nil {
		return err
	}
	c, err := srv.client()
	if err != nil {
		return err
	}
	rep, err := vmrun.Merge(ctx, c, &load, vmrun.MergeOptions{Settle: *settle, Stable: *stable, Env: srv.env()})
	if rep != nil {
		if werr := vmrun.WriteJSON(dir, "merge.json", rep); werr != nil && err == nil {
			err = werr
		}
	}
	if err != nil {
		return err
	}
	a := rep.After
	fmt.Fprintf(stdout, "merged: %d rows stored of %d written, %d series; parts %d small, %d big; idle\n", rep.Stored, rep.Rows, rep.Series, a.SmallParts, a.BigParts)
	return nil
}

func runSizes(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike sizes", flag.ContinueOnError)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory load.json and merge.json are in, and sizes.json is written to")
	data := fs.String("data", "", "the server's data directory, as the host sees it")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	if *data == "" {
		return errors.New("no -data")
	}
	var load vmrun.LoadReport
	var merge vmrun.MergeReport
	if err := vmrun.ReadJSON(dir, "load.json", &load); err != nil {
		return err
	}
	if err := vmrun.ReadJSON(dir, "merge.json", &merge); err != nil {
		return err
	}
	c, err := srv.client()
	if err != nil {
		return err
	}
	rep, err := vmrun.Sizes(ctx, c, &load, &merge, *data, vmrun.MergeOptions{Env: srv.env()})
	if err != nil {
		return err
	}
	if err := vmrun.WriteJSON(dir, "sizes.json", rep); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "data directory: %d bytes in %d files (the server counts %d bytes of parts); %.2f per row, %.2f per series\n", rep.Dir.Total, rep.Dir.Files, rep.DataBytes, rep.PerRow, rep.PerSeries)
	for _, name := range rep.Dir.Entries() {
		fmt.Fprintf(stdout, "  %-12s %d\n", name, rep.Dir.ByEntry[name])
	}
	fmt.Fprintf(stdout, "%s\n", rep.Note)
	return nil
}

func runRead(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike read", flag.ContinueOnError)
	in := addInputFlags(fs)
	srv := addServerFlags(fs)
	out := fs.String("out", "", "the directory to write read.json in: outside any git work tree")
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	dir, err := outDir(*out)
	if err != nil {
		return err
	}
	input, err := in.open()
	if err != nil {
		return err
	}
	c, err := srv.client()
	if err != nil {
		return err
	}
	env, err := vmrun.CollectEnv(ctx, c, srv.env())
	if err != nil {
		return err
	}
	rep, err := vmrun.Read(ctx, input, vmengine.New(c, input.Registry, input.LastSeq()), env)
	if rep != nil {
		if werr := vmrun.WriteJSON(dir, "read.json", rep); werr != nil && err == nil {
			err = werr
		}
		fmt.Fprint(stdout, rep.Gate.String())
		for _, f := range rep.NotCompared {
			fmt.Fprintf(stdout, "family %s: not compared at sample resolution\n", f)
		}
		for _, m := range rep.Gate.Mismatches {
			fmt.Fprintf(stdout, "MISMATCH %s: got %+v, reference %+v %s\n", m.Query, m.Got, m.Want, m.AskError)
		}
	}
	return err
}

func runEnv(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("vmspike env", flag.ContinueOnError)
	srv := addServerFlags(fs)
	if err := parse(fs, args, stderr); err != nil {
		return err
	}
	var c *vmengine.Client
	if *srv.url != "" {
		var err error
		if c, err = srv.client(); err != nil {
			return err
		}
	}
	env, err := vmrun.CollectEnv(ctx, c, srv.env())
	if werr := vmrun.EncodeJSON(stdout, env); werr != nil && err == nil {
		err = werr
	}
	return err
}
