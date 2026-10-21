package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func TestRunChecksAPlanFile(t *testing.T) {
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
	var out, errOut bytes.Buffer
	if err := run(context.Background(), []string{"-plan", path}, &out, &errOut); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "G0 total:") || strings.Contains(out.String(), "MISMATCH") {
		t.Fatalf("output:\n%s", out.String())
	}

	// A plan file other than the expected one is refused before anything is read.
	if err := run(context.Background(), []string{"-plan", path, "-expect-plan", strings.Repeat("0", 64)}, &out, &errOut); err == nil {
		t.Fatal("a plan with another SHA-256 was accepted")
	}
	if err := run(context.Background(), []string{}, &out, &errOut); err == nil {
		t.Fatal("no plan was accepted")
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
