package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

func TestParseSizes(t *testing.T) {
	got, err := parseSizes("run1 10\nrun2 20\n\nrun5 50\n")
	if err != nil || got[1] != 10 || got[2] != 20 || got[5] != 50 || len(got) != 3 {
		t.Errorf("%v %v", got, err)
	}
	for _, bad := range []string{"run1\n", "x 1\n", "runx 1\n", "run1 x\n"} {
		if _, err := parseSizes(bad); err == nil {
			t.Errorf("parseSizes(%q): no error", bad)
		}
	}
}

func TestCheckSizesAndLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "run1.jsonl")
	if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wl := &workload{Runs: []runSpan{{N: 1, HasStop: true, Bytes: 3, Lines: 1}}}
	if bad := checkSizes([]string{p}, map[int]int64{1: 3}, wl); len(bad) != 0 {
		t.Errorf("matching sizes: %v", bad)
	}
	if bad := checkSizes([]string{p}, map[int]int64{1: 4}, wl); len(bad) != 1 || !strings.Contains(bad[0], "sizes.txt") {
		t.Errorf("sizes.txt says 4: %v", bad)
	}
	wl.Runs[0].Bytes = 9
	if bad := checkSizes([]string{p}, map[int]int64{1: 3}, wl); len(bad) != 1 || !strings.Contains(bad[0], "workload log") {
		t.Errorf("the log says 9: %v", bad)
	}
	if bad := checkSizes([]string{p}, map[int]int64{}, wl); len(bad) < 1 {
		t.Errorf("no size in sizes.txt: %v", bad)
	}
	if bad := checkLines([]*runFile{{N: 1, Lines: make([]lineSpan, 1)}}, wl); len(bad) != 0 {
		t.Errorf("lines: %v", bad)
	}
	if bad := checkLines([]*runFile{{N: 1, Lines: make([]lineSpan, 2)}}, wl); len(bad) != 1 {
		t.Errorf("lines: %v", bad)
	}
}

func TestReportIsNotWrittenInsideAWorkTree(t *testing.T) {
	root := t.TempDir()
	if err := refuseWorkTree(filepath.Join(root, "out")); err != nil {
		t.Errorf("a directory outside any repository is refused: %v", err)
	}
	// A work tree has .git: a directory, or a file for a linked worktree.
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := refuseWorkTree(filepath.Join(root, "sub", "out")); err == nil || !strings.Contains(err.Error(), "git work tree") {
		t.Errorf("a directory inside a work tree is not refused: %v", err)
	}
}

// A whole capture in miniature: five runs, the logs, and the truth, through build and
// the writers.
func TestExecuteOnAMiniatureCapture(t *testing.T) {
	dir := t.TempDir()
	created := t0.Add(-time.Hour)
	var sizes, stopped strings.Builder
	for n := 1; n <= 5; n++ {
		at := t0.Add(time.Duration(n) * time.Minute)
		path := runPath(dir, n)
		writeRun(t, path,
			[]capture.LogRecord{pullRecord(podObject(uidA, "web-a", "node1", created), at)},
			[]capture.LogRecord{podState(uidA, "web-a", "node1", created, at)},
		)
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sizes, "run%d %d\n", n, st.Size())
		fmt.Fprintf(&stopped, "2026-10-10T12:%02d:00Z collector run %d started, T0=%d\n2026-10-10T12:%02d:30Z collector run %d stopped: 2 lines, %d bytes\n", n*10, n, n, n*10, n, st.Size())
	}
	logs := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"sizes.txt": sizes.String(), "workload.txt": stopped.String()} {
		if err := os.WriteFile(filepath.Join(logs, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"truth-pods.tsv", "truth-nodes.tsv"} {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(logs, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(t.TempDir(), "report")
	var stdout bytes.Buffer
	failed, err := execute(config{Capture: dir, Tmp: t.TempDir(), Margin: 60 * time.Second}, out, &stdout, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if failed {
		t.Errorf("a self-check failed:\n%s", stdout.String())
	}
	text, err := os.ReadFile(filepath.Join(out, "report.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(text, stdout.Bytes()) {
		t.Error("report.txt is not what was printed")
	}
	for _, want := range []string{"BLOCKING", "PASS A", "PASS B", "the largest pull", "pass A against pass B", "cadences"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("the report lacks %q", want)
		}
	}
	js, err := os.ReadFile(filepath.Join(out, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(js, &back); err != nil {
		t.Fatalf("result.json: %v", err)
	}
	if len(back.Runs) != 5 || len(back.Passes) != 2 || back.Pull == nil || back.Truth.PodUIDs != 2 {
		t.Errorf("result.json: %d runs, %d passes, truth %+v", len(back.Runs), len(back.Passes), back.Truth)
	}

	// A capture that is not the one the logs describe is refused before anything is replayed.
	if err := os.WriteFile(runPath(dir, 3), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(config{Capture: dir, Tmp: t.TempDir(), Margin: time.Minute}, filepath.Join(t.TempDir(), "r2"), &stdout, time.Minute); err == nil || !strings.Contains(err.Error(), "run3.jsonl") {
		t.Errorf("a changed run file: %v", err)
	}
}
