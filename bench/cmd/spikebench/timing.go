package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// usageError is a mistake in the command line, which ends the program with status 2.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

// ancestryFunc says whether rev is an ancestor of ref in the repository checked out at
// checkout. It is the git check in production and a fake in the tests.
type ancestryFunc func(ctx context.Context, checkout, rev, ref string) runner.RevisionCheck

// stringList is a flag that may be given more than once.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// doTiming judges G2, G3 and G4 from the artifacts of the bench workflow.
func doTiming(ctx context.Context, args []string) error {
	err := runTiming(ctx, args, os.Stdout, os.Stderr, gitAncestry)
	var usage usageError
	if errors.As(err, &usage) {
		fmt.Fprintln(os.Stderr, "spikebench timing:", usage)
		os.Exit(2)
	}
	return err
}

// runTiming is [doTiming] with its streams and its git check given.
func runTiming(ctx context.Context, args []string, stdout, stderr io.Writer, git ancestryFunc) error {
	fs := flag.NewFlagSet("timing", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var in, counters, joinFrom stringList
	var checkout, ref, jsonOut string
	fs.Var(&in, "in", "a directory searched at any depth for the artifacts of the bench workflow and for plans; repeatable, and the arguments after the flags are more of them")
	fs.Var(&counters, "counters", "a directory searched for counters runs (candidate directories with results.json), which give G0; repeatable")
	fs.Var(&joinFrom, "join-from", "a directory of the artifacts of the first timed run, from which it is decided whether M/crdb1 joins the reference set (default: the -in builds); repeatable")
	fs.StringVar(&checkout, "git", "", "a checkout of this repository, in which to check that the builds' revision is an ancestor of -ref (without it git is not run and the result is unverified)")
	fs.StringVar(&ref, "ref", "origin/main", "the ref the builds' revision must be an ancestor of")
	fs.StringVar(&jsonOut, "json", "", "also write the report as JSON to this file, outside the repository")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return usageError{err.Error()}
	}
	roots := append(slicesClone(in), fs.Args()...)
	switch {
	case len(roots) == 0:
		return usageError{"give at least one directory of artifacts (-in DIR)"}
	case ref == "" || strings.HasPrefix(ref, "-"):
		return usageError{fmt.Sprintf("-ref %q: a ref is not empty and does not start with -", ref)}
	}
	if jsonOut != "" {
		if err := runner.CheckOutside(filepath.Dir(jsonOut)); err != nil {
			return err
		}
	}

	builds, plans, err := runner.LoadTimedBuilds(roots)
	if err != nil {
		return err
	}
	var runs []*runner.Candidate
	if len(counters) > 0 {
		if runs, err = runner.LoadCountersRuns(counters); err != nil {
			return err
		}
	}
	inputs := runner.TimedInputs{Builds: builds, Plans: plans, Counters: runs}
	if len(joinFrom) > 0 {
		if inputs.JoinFrom, inputs.JoinPlans, err = runner.LoadTimedBuilds(joinFrom); err != nil {
			return err
		}
		if inputs.JoinFrom == nil {
			inputs.JoinFrom = []runner.TimedBuild{}
		}
	}
	opts := runner.TimingOptions{Rules: runner.DefaultRules()}
	if checkout != "" {
		opts.Ref = ref
		opts.Check = func(rev string) runner.RevisionCheck { return git(ctx, checkout, rev, ref) }
	}
	rep := runner.JudgeTiming(inputs, opts)

	runner.WriteTiming(stdout, rep)
	if jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			return err
		}
		if err := writeFileAtomic(jsonOut, append(b, '\n')); err != nil {
			return err
		}
	}
	if n := len(rep.Problems); n > 0 {
		return fmt.Errorf("these timings cannot be judged together (%d reasons, listed above): %s", n, rep.Problems[0])
	}
	return nil
}

func slicesClone(l stringList) []string { return append([]string(nil), l...) }

// writeFileAtomic writes the file whole or not at all: to a temporary file in the same
// directory, which is renamed over it and removed if anything fails.
func writeFileAtomic(path string, b []byte) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Chmod(f.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// gitAncestry asks git, read-only and without the network, whether rev is an ancestor
// of ref in the checkout. It runs two commands and no other: rev-parse of the ref, and
// merge-base --is-ancestor.
func gitAncestry(ctx context.Context, checkout, rev, ref string) runner.RevisionCheck {
	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", checkout}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0")
		return cmd.Run()
	}
	if why := refCheckFromExit(ref, checkout, run("rev-parse", "--verify", "--quiet", ref+"^{commit}")); why != "" {
		return runner.RevisionCheck{Err: why}
	}
	return ancestryFromExit(rev, checkout, run("merge-base", "--is-ancestor", rev, ref))
}

// refCheckFromExit is why the exit of git rev-parse --verify says the ref is not a
// commit of the checkout, or "" for a ref that is one. Only an exit of git says the ref
// is not a commit; a git that could not be run says so.
func refCheckFromExit(ref, checkout string, err error) string {
	if err == nil {
		return ""
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() > 0 {
		return fmt.Sprintf("%s is not a commit in %s", ref, checkout)
	}
	return "git could not be run: " + err.Error()
}

// ancestryFromExit is what the exit of git merge-base --is-ancestor says: 0 an
// ancestor, 1 not, and anything else (128 for a commit the checkout does not have,
// after a shallow clone or before a fetch) that the check could not be made.
func ancestryFromExit(rev, checkout string, err error) runner.RevisionCheck {
	if err == nil {
		return runner.RevisionCheck{Ancestor: true}
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return runner.RevisionCheck{Err: "git could not be run: " + err.Error()}
	}
	switch code := exit.ExitCode(); code {
	case 1:
		return runner.RevisionCheck{}
	case 128:
		return runner.RevisionCheck{Err: fmt.Sprintf("revision %.12s is not in %s: fetch it", rev, checkout)}
	default:
		return runner.RevisionCheck{Err: fmt.Sprintf("git exited with status %d", code)}
	}
}
