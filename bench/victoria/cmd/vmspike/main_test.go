package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/actfixture"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmrun"
)

// The commands read scripts/flags.conf and scripts/vm-server.sh from where they are run,
// which is bench/victoria.
const (
	flagsPath  = "../../scripts/flags.conf"
	scriptPath = "../../scripts/vm-server.sh"
)

var (
	planOnce sync.Once
	planMade *runner.Plan
	planErr  error
)

func planFile(t *testing.T) (string, string) {
	t.Helper()
	planOnce.Do(func() { planMade, planErr = runner.MakePlan(context.Background(), fixture.Spec(), nil) })
	if planErr != nil {
		t.Fatal(planErr)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := planMade.Save(path); err != nil {
		t.Fatal(err)
	}
	sum, err := vmrun.FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, sum
}

func do(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errb bytes.Buffer
	err := run(context.Background(), args, &out, &errb)
	return out.String(), errb.String(), err
}

func TestUsageAndUnknownCommands(t *testing.T) {
	if _, _, err := do(t); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Errorf("no arguments: %v", err)
	}
	if _, _, err := do(t, "frobnicate"); err == nil || !strings.Contains(err.Error(), `no subcommand "frobnicate"`) {
		t.Errorf("unknown: %v", err)
	}
	if _, _, err := do(t, "convert", "-plan", "p.json", "stray"); err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Errorf("stray argument: %v", err)
	}
	for _, cmd := range []string{"convert", "facts", "questions", "load", "merge", "sizes", "read"} {
		if _, _, err := do(t, cmd); err == nil {
			t.Errorf("%s with no flags ran", cmd)
		}
	}
}

// Every command that writes refuses an -out inside a git work tree, before it does any
// work (here: before it reads the plan, which does not exist).
func TestNoCommandWritesInsideAGitWorkTree(t *testing.T) {
	inside := filepath.Join("..", "..", "scratch-that-must-not-exist")
	for _, cmd := range [][]string{
		{"convert", "-plan", "none.json", "-expect-plan", "x"},
		{"facts", "-plan", "none.json", "-expect-plan", "x"},
		{"questions", "-activity", "none.parquet"},
		{"load", "-plan", "none.json", "-expect-plan", "x", "-server", "http://127.0.0.1:1"},
		{"merge", "-server", "http://127.0.0.1:1"},
		{"sizes", "-server", "http://127.0.0.1:1", "-data", "d"},
		{"read", "-plan", "none.json", "-expect-plan", "x", "-server", "http://127.0.0.1:1"},
	} {
		_, _, err := do(t, append(cmd, "-out", inside)...)
		if err == nil || !strings.Contains(err.Error(), "inside") || !strings.Contains(err.Error(), "git work tree") {
			t.Errorf("%s: %v", cmd[0], err)
		}
	}
	if _, err := os.Stat(inside); err == nil {
		os.RemoveAll(inside)
		t.Error("a directory was made inside the work tree")
	}
	if _, _, err := do(t, "convert", "-plan", "none.json"); err == nil || !strings.Contains(err.Error(), "-out") {
		t.Errorf("no -out: %v", err)
	}
}

func TestAPlanThatIsNotTheOneNamedIsRefused(t *testing.T) {
	path, sum := planFile(t)
	out := t.TempDir()
	if _, _, err := do(t, "convert", "-plan", path, "-out", out, "-flags", flagsPath, "-script", scriptPath); err == nil || !strings.Contains(err.Error(), "named in advance") {
		t.Errorf("no -expect-plan: %v", err)
	}
	wrong := "0" + sum[1:]
	if sum[0] == '0' {
		wrong = "1" + sum[1:]
	}
	if _, _, err := do(t, "convert", "-plan", path, "-expect-plan", wrong, "-out", out, "-flags", flagsPath, "-script", scriptPath); err == nil || !strings.Contains(err.Error(), "not the expected") {
		t.Errorf("another digest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "convert.json")); err == nil {
		t.Error("an output was written for a refused plan")
	}
	if _, _, err := do(t, "convert", "-plan", path, "-activity", "x", "-out", out); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Errorf("both inputs: %v", err)
	}
	if _, _, err := do(t, "convert", "-out", out); err == nil || !strings.Contains(err.Error(), "-plan or -activity") {
		t.Errorf("no input: %v", err)
	}
}

func TestConvertAPlanWritesItsDeterministicCounts(t *testing.T) {
	path, sum := planFile(t)
	out := filepath.Join(t.TempDir(), "run")
	stdout, _, err := do(t, "convert", "-plan", path, "-expect-plan", sum, "-out", out, "-flags", flagsPath, "-script", scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	var rep vmrun.ConvertReport
	if err := vmrun.ReadJSON(out, "convert.json", &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Label != "local, deterministic" || rep.Runs != 2 || rep.Counts.Samples == 0 || rep.Months != 1 || rep.Input.PlanSHA256 != sum {
		t.Errorf("%+v", rep)
	}
	for _, want := range []string{"local, deterministic after 2 equal runs", "samples digest " + rep.SamplesDigest, "coalesced stream digest", "months spanned 1"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the summary lacks %q:\n%s", want, stdout)
		}
	}
	// No time is printed or recorded.
	b, _ := os.ReadFile(filepath.Join(out, "convert.json"))
	for _, bad := range []string{"Elapsed", "Duration", "WallTime", "Seconds"} {
		if strings.Contains(string(b), bad) || strings.Contains(stdout, bad) {
			t.Errorf("output mentions %s", bad)
		}
	}
	if _, _, err := do(t, "convert", "-plan", path, "-expect-plan", sum, "-out", out, "-runs", "1", "-flags", flagsPath, "-script", scriptPath); err == nil {
		t.Error("one run was accepted")
	}
}

func TestTheActivityCommands(t *testing.T) {
	dir := t.TempDir()
	rs := actfixture.Placement(actfixture.Default())
	every := actfixture.Write(t, dir, "s.parquet", rs)
	coalesced := actfixture.Write(t, dir, "c.parquet", actfixture.Coalesce(t, rs, actfixture.DefaultCoalescer()))
	out := filepath.Join(dir, "out")

	stdout, _, err := do(t, "questions", "-activity", every, "-out", out)
	if err != nil {
		t.Fatal(err)
	}
	sum := ""
	for _, line := range strings.Split(stdout, "\n") {
		if rest, ok := strings.CutPrefix(line, "questions sha256 "); ok {
			sum = rest
		}
	}
	if sum == "" || !strings.Contains(stdout, "node<- hot-records neighbors") {
		t.Fatalf("%s", stdout)
	}
	got, err := vmrun.FileSHA256(filepath.Join(out, "questions.json"))
	if err != nil || got != sum {
		t.Errorf("questions.json has sha256 %s, printed %s (%v)", got, sum, err)
	}

	// Convert and facts over the file; facts needs the coalesced counterpart.
	if _, _, err := do(t, "convert", "-activity", every, "-questions", filepath.Join(out, "questions.json"), "-expect-questions", sum, "-out", filepath.Join(dir, "c"), "-flags", flagsPath, "-script", scriptPath); err != nil {
		t.Fatal(err)
	}
	stdout, _, err = do(t, "facts", "-activity", every, "-coalesced", coalesced, "-out", filepath.Join(dir, "f"), "-flags", flagsPath, "-script", scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	var fr vmrun.FactsRun
	if err := vmrun.ReadJSON(filepath.Join(dir, "f"), "facts.json", &fr); err != nil || fr.Facts.EndsEarlier == 0 || fr.Coalesced != "c.parquet" {
		t.Errorf("%+v, %v", fr, err)
	}
	if !strings.Contains(stdout, "describe the same existence") || !strings.Contains(stdout, "smallest band") {
		t.Errorf("%s", stdout)
	}
	if _, _, err := do(t, "facts", "-activity", every, "-out", filepath.Join(dir, "f2"), "-flags", flagsPath, "-script", scriptPath); err == nil {
		t.Error("facts of an activity file without its coalesced file")
	}
	// The facts of a file that is not the coalescer's: refused, and nothing is written.
	if _, _, err := do(t, "facts", "-activity", every, "-coalesced", coalesced, "-extend-fraction", "0.01", "-out", filepath.Join(dir, "f3"), "-flags", flagsPath, "-script", scriptPath); err == nil {
		t.Error("a fraction the coalescer did not use was accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "f3", "facts.json")); err == nil {
		t.Error("facts.json was written for a failed check")
	}
}

func TestEnvPrintsWhereTheRunIsMade(t *testing.T) {
	stdout, _, err := do(t, "env", "-flags", flagsPath, "-script", scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	var env vmrun.Env
	if err := json.Unmarshal([]byte(stdout), &env); err != nil {
		t.Fatalf("%v\n%s", err, stdout)
	}
	if env.FlagsSHA256 == "" || !strings.Contains(env.Image, "@sha256:") || env.CPU == "" || env.FlagsChecked {
		t.Errorf("%+v", env)
	}
	if _, _, err := do(t, "env", "-flags", filepath.Join(t.TempDir(), "missing.conf")); err == nil {
		t.Error("a missing flags file")
	}
	// A server that is not on loopback is refused.
	if _, _, err := do(t, "env", "-flags", flagsPath, "-script", scriptPath, "-server", "http://example.com:8428"); err == nil {
		t.Error("a server that is not on loopback was accepted")
	}
}
