package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func savePlan(t *testing.T, cfg workload.Config, mod func(*runner.Spec)) string {
	t.Helper()
	spec := runner.DefaultSpec(cfg)
	spec.BatchSize = 64
	spec.MinNonEmpty = 0
	if mod != nil {
		mod(&spec)
	}
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

func TestRunChecksAPlanFileInEachArm(t *testing.T) {
	path := savePlan(t, workload.Tiny(), nil)
	for _, arm := range []string{"commit", "rows"} {
		var out, errOut bytes.Buffer
		if err := run(context.Background(), []string{"-plan", path, "-arm", arm}, &out, &errOut); err != nil {
			t.Fatalf("%s: %v\n%s", arm, err, out.String())
		}
		if !strings.Contains(out.String(), "G0 total:") || strings.Contains(out.String(), "MISMATCH") || strings.Contains(out.String(), "DIAGNOSTIC") {
			t.Fatalf("%s output:\n%s", arm, out.String())
		}
		if !strings.Contains(out.String(), "payload capacity 256 bytes (registered); records refused for it: 0") ||
			!strings.Contains(out.String(), "settlement by age") ||
			(arm == "commit") != strings.Contains(out.String(), "reads whose token names no commit: 0") ||
			!strings.Contains(out.String(), "arm "+arm+"\n") || !strings.Contains(out.String(), "paths (reads by kind and table):") {
			t.Fatalf("%s output:\n%s", arm, out.String())
		}
	}
}

// TestTheOutputPrintsNoTime: the command prints counts, never a duration or a rate.
func TestTheOutputPrintsNoTime(t *testing.T) {
	path := savePlan(t, workload.Tiny(), nil)
	var out, errOut bytes.Buffer
	if err := run(context.Background(), []string{"-plan", path}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	for _, re := range []string{`\d(ns|µs|us|ms|s)\b`, `(?i)seconds`, `(?i)duration`, `(?i)elapsed`, `/s\b`, `(?i)per second`} {
		// The instants of the old snapshot are nanoseconds since the epoch, named as such.
		text := regexp.MustCompile(`\d+ ns\b`).ReplaceAllString(out.String()+errOut.String(), "")
		if regexp.MustCompile(re).MatchString(text) {
			t.Errorf("the output matches %q:\n%s", re, text)
		}
	}
}

func TestTheDiagnosticArmPrintsItsWrongAnswersAndDoesNotFailOnThem(t *testing.T) {
	path := savePlan(t, workload.Small(), func(s *runner.Spec) {
		s.Workload.LateProbability, s.Workload.LateMeanDelay = 0.4, 5*time.Minute
	})
	var out, errOut bytes.Buffer
	if err := run(context.Background(), []string{"-plan", path, "-arm", "commit-time"}, &out, &errOut); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	m := regexp.MustCompile(`DIAGNOSTIC total: (\d+) of (\d+) answers wrong`).FindStringSubmatch(out.String())
	if m == nil || m[1] == "0" {
		t.Fatalf("output:\n%s", out.String())
	}
	if strings.Contains(out.String(), "G0 ") || strings.Contains(out.String(), "MISMATCH") {
		t.Fatalf("the diagnostic is reported as a gate:\n%s", out.String())
	}
}

func TestRunRefusesWhatItShould(t *testing.T) {
	path := savePlan(t, workload.Tiny(), nil)
	var out, errOut bytes.Buffer
	// A plan file other than the expected one is refused before anything is read.
	if err := run(context.Background(), []string{"-plan", path, "-expect-plan", strings.Repeat("0", 64)}, &out, &errOut); err == nil {
		t.Fatal("a plan with another SHA-256 was accepted")
	}
	if err := run(context.Background(), []string{}, &out, &errOut); err == nil {
		t.Fatal("no plan was accepted")
	}
	if err := run(context.Background(), []string{"-plan", path, "-arm", "wal"}, &out, &errOut); err == nil {
		t.Fatal("an unknown arm was accepted")
	}
	if err := run(context.Background(), []string{"-plan", path, "extra"}, &out, &errOut); err == nil {
		t.Fatal("an extra argument was accepted")
	}
	// A plan whose payloads do not fit the registered column is refused, whatever else holds.
	wide := savePlan(t, workload.Tiny(), func(s *runner.Spec) { s.Workload.PayloadPad = 300 })
	if err := run(context.Background(), []string{"-plan", wide}, &out, &errOut); err == nil || !strings.Contains(err.Error(), "registered capacity") {
		t.Fatalf("a plan with payloads over the registered capacity: %v", err)
	}
	// A plan whose questions were changed is refused by LoadPlan.
	b, _ := os.ReadFile(path)
	if err := os.WriteFile(path, bytes.Replace(b, []byte(`"Rank": 0`), []byte(`"Rank": 7`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-plan", path}, &out, &errOut); err == nil {
		t.Fatal("an edited plan was accepted")
	}
}
