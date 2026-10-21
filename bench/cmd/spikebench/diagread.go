package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func doDiagRead(args []string) error { return runDiagRead(args, os.Stdout, os.Stderr) }

// runDiagRead is [doDiagRead] with its streams given. It reads the builds of a CI diag run
// under the directory after the flags and prints each build's figures and, for each
// architecture, CPU model and configuration, their median, least and largest. It judges
// nothing. A mistake of the command line, a run that is not the one asked for, and a build
// that cannot be read end it with status 3 and print nothing (see runner.ReadDiag).
func runDiagRead(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("diagread", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o runner.DiagReadOptions
	var jsonOut string
	var expect stringList
	fs.StringVar(&o.Commit, "commit", "", "the commit every build must be of, as 40 hexadecimal digits (required)")
	fs.StringVar(&o.Run, "run", "", "the workflow run id (required)")
	fs.Var(&expect, "expect", "a configuration's table shape as 'name=shape' (repeatable): 'none' for a manifest without one, otherwise its pebble_shape text")
	fs.StringVar(&jsonOut, "json", "", "also write the report here as JSON")
	refuse := func(err error) error {
		return exitCodeError{runner.DiagStatusRefused, err.Error()}
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return refuse(err)
	}
	if fs.NArg() != 1 {
		return refuse(errors.New("diagread: give the one directory that holds the run's diag-build-* artifacts"))
	}
	o.Expect = map[string]string{}
	for _, e := range expect {
		name, shape, ok := strings.Cut(e, "=")
		switch {
		case !ok || name == "" || shape == "":
			return refuse(fmt.Errorf("diagread: -expect %q: give name=shape (shape %q for a manifest without one)", e, runner.DiagAbsentShape))
		case o.Expect[name] != "":
			return refuse(fmt.Errorf("diagread: -expect names %q twice", name))
		}
		o.Expect[name] = shape
	}
	rep, err := runner.ReadDiag(fs.Arg(0), o)
	if err != nil {
		return refuse(err)
	}
	if jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			return refuse(err)
		}
		if err := os.WriteFile(jsonOut, append(b, '\n'), 0o644); err != nil {
			return refuse(err)
		}
	}
	runner.WriteDiag(stdout, rep)
	return nil
}
