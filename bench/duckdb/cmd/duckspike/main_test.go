package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func tinyPlan(t *testing.T) string {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.MinNonEmpty = 0
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	return path
}

// duration matches a number with a unit of time, as a timing would print.
var duration = regexp.MustCompile(`\b\d+(\.\d+)?\s?(ns|µs|us|ms|s|m|h)\b|\b(elapsed|duration|seconds)\b`)

func TestG0ChecksAPlanFileOnEveryArm(t *testing.T) {
	path := tinyPlan(t)
	var out, errOut bytes.Buffer
	if err := run(context.Background(), []string{"g0", "-plan", path}, &out, &errOut); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	text := out.String()
	for _, want := range []string{"arm records", "arm intervals", "G0 total:", "plan sha256 "} {
		if !strings.Contains(text, want) {
			t.Errorf("no %q in:\n%s", want, text)
		}
	}
	if strings.Contains(text, "MISMATCH") || strings.Count(text, "G0 total:") != 2 {
		t.Fatalf("output:\n%s", text)
	}
	for _, s := range []string{text, errOut.String()} {
		if m := duration.FindString(s); m != "" {
			t.Errorf("the output has what looks like a time, %q:\n%s", m, s)
		}
	}

	var one bytes.Buffer
	if err := run(context.Background(), []string{"g0", "-plan", path, "-arms", "intervals"}, &one, &errOut); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(one.String(), "arm records") || !strings.Contains(one.String(), "arm intervals") {
		t.Fatalf("-arms intervals:\n%s", one.String())
	}
}

func TestG0RefusesWhatItShould(t *testing.T) {
	path := tinyPlan(t)
	var out, errOut bytes.Buffer
	runWith := func(args ...string) error {
		out.Reset()
		return run(context.Background(), args, &out, &errOut)
	}
	// A plan file other than the expected one is refused before anything is read.
	if err := runWith("g0", "-plan", path, "-expect-plan", strings.Repeat("0", 64)); err == nil || out.Len() != 0 {
		t.Fatalf("a plan with another SHA-256: %v, output %q", err, out.String())
	}
	for name, args := range map[string][]string{
		"no subcommand":      {},
		"another subcommand": {"read", "-plan", path},
		"no plan":            {"g0"},
		"a stray argument":   {"g0", "-plan", path, "extra"},
		"an unknown arm":     {"g0", "-plan", path, "-arms", "records,files"},
		"an arm twice":       {"g0", "-plan", path, "-arms", "records,records"},
		"a missing plan":     {"g0", "-plan", filepath.Join(t.TempDir(), "none.json")},
	} {
		if err := runWith(args...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A plan whose questions were changed is refused by LoadPlan.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Replace(b, []byte(`"Rank": 0`), []byte(`"Rank": 7`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runWith("g0", "-plan", path); err == nil {
		t.Fatal("an edited plan was accepted")
	}
}
