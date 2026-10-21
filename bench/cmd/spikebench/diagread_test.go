package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// Nothing here is a measurement: the artifacts are synthetic, written by the test from the
// structs of the runner and the diag workflow's job.json as its collect script writes it.

const diagRun = "1000000001"

// writeDiagArtifact writes one build of a diag run (a synthetic one) and returns its
// directory. shape is the manifest's table shape, "" for none.
func writeDiagArtifact(t *testing.T, root, name string, rep int, shape string, mod func(*runner.Manifest, map[string]any)) string {
	t.Helper()
	var in, out runner.Histogram
	in.Add(30 * time.Millisecond)
	out.Add(2 * time.Millisecond)
	const day = int64(24 * time.Hour)
	m := runner.Manifest{
		Candidate: "Lroot/k64a2l5m", Layout: "L", PlanDigest: "p", Build: runner.BuildInfo{Revision: gateCommit},
		Describe: map[string]string{
			runner.RetentionModeKey: "background", runner.GoGCKey: "100", runner.SyncKey: "true",
			runner.PaceKey: strconv.Itoa(runner.StoreGatePaceRate), runner.PaceFromDayKey: "11.5", runner.RetainFlushDelsKey: "32768",
		},
		Gate: &runner.GateTrace{
			Mode: "background", PaceStartNs: 1,
			Windows: []runner.GateWindow{{StartNs: 1, RewriteEndNs: 40e9 + 1, SettleEndNs: 41e9, ReturnNs: 1}},
			Inside:  runner.GateBatches{Batches: 1, Records: 4000, Commit: in},
			Outside: runner.GateBatches{Batches: 1, Records: 10, Commit: out},
			WriteNs: 100e9, StreamEndNs: 100e9, KeepNs: 7 * day,
		},
	}
	if shape != "" {
		m.Describe[runner.ShapeKey] = shape
	}
	job := map[string]any{
		"os": "ubuntu-24.04-arm", "arch": "aarch64", "name": name, "role": "A", "rep": rep, "base_sha": gateCommit,
		"patched": "false", "cpu_model": "synthetic cpu", "run_id": diagRun, "run_attempt": "1",
		"binary_sha256": strings.Repeat("a", 64), "peak_rss_mib": 1024,
	}
	if mod != nil {
		mod(&m, job)
	}
	dir := filepath.Join(root, fmt.Sprintf("diag-build-%s-%s-%s-rep%d", diagRun, job["os"], job["name"], rep))
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	man, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	line := `{"elapsed_ms":1,"phase":"write","batches":1,"retentions":0,"stats":{},"go":{"heap_live":123456789}}` + "\n"
	for file, content := range map[string]string{
		runner.ManifestFile: string(man), runner.JobFile: string(jobJSON), runner.MetricsFile: line,
		filepath.Join("logs", "time.txt"): "synthetic\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// diagread prints the figures of a run's builds, judges nothing and ends with status 0; a
// mistake of the command line, a build of another commit and a damaged run end it with
// status 3 with nothing printed, and no -json file is left.
func TestDiagReadPrintsFiguresOrRefusesWithStatusThree(t *testing.T) {
	t.Parallel()

	const shape = "delete_only_compactions=off"
	root := t.TempDir()
	for rep := 1; rep <= 2; rep++ {
		writeDiagArtifact(t, root, "control", rep, "", nil)
		writeDiagArtifact(t, root, "p5shape", rep, shape, nil)
	}
	jsonPath := filepath.Join(t.TempDir(), "diag.json")
	good := []string{"-commit", gateCommit, "-run", diagRun, "-expect", "control=none", "-expect", "p5shape=" + shape, "-json", jsonPath, root}
	var out, errOut bytes.Buffer
	if err := runDiagRead(good, &out, &errOut); err != nil {
		t.Fatalf("a valid run: %v", err)
	}
	for _, want := range []string{
		"diag run " + diagRun, "name=p5shape", "Q1 30ms (outside maximum 2ms over 1 batches)", "R 40s (0 passes", "peak heap 123.457 MB",
		"peak_rss_mib 1024", "group arch=aarch64", "n=2",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, out.String())
		}
	}
	var rep runner.DiagReport
	raw, err := os.ReadFile(jsonPath)
	if err != nil || json.Unmarshal(raw, &rep) != nil || len(rep.Builds) != 4 || len(rep.Groups) != 2 {
		t.Errorf("the JSON: %d builds, %d groups, %v", len(rep.Builds), len(rep.Groups), err)
	}

	for _, c := range []struct {
		name string
		args []string
	}{
		{"no directory", []string{"-commit", gateCommit, "-run", diagRun}},
		{"two directories", append(append([]string{}, good[:len(good)-1]...), root, root)},
		{"no commit", []string{"-run", diagRun, root}},
		{"another commit", []string{"-commit", strings.Repeat("9", 40), "-run", diagRun, root}},
		{"no run", []string{"-commit", gateCommit, root}},
		{"an unknown flag", []string{"-nonsense"}},
		{"an -expect without a shape", []string{"-commit", gateCommit, "-run", diagRun, "-expect", "control", root}},
		{"an -expect with an empty shape", []string{"-commit", gateCommit, "-run", diagRun, "-expect", "control=", root}},
		{"an -expect twice", []string{"-commit", gateCommit, "-run", diagRun, "-expect", "control=none", "-expect", "control=none", root}},
		{"a shape that differs", []string{"-commit", gateCommit, "-run", diagRun, "-expect", "p5shape=none", root}},
		{"an empty directory", []string{"-commit", gateCommit, "-run", diagRun, t.TempDir()}},
		{"a directory that is not there", []string{"-commit", gateCommit, "-run", diagRun, filepath.Join(t.TempDir(), "absent")}},
	} {
		out.Reset()
		missing := filepath.Join(t.TempDir(), "never.json")
		err := runDiagRead(append([]string{"-json", missing}, c.args...), &out, &errOut)
		if exitCode(err) != 3 || out.Len() != 0 {
			t.Errorf("%s: status %d, printed %q", c.name, exitCode(err), out.String())
		}
		if _, err := os.Stat(missing); err == nil {
			t.Errorf("%s: a -json file was written", c.name)
		}
	}

	// A damaged run: one build of another commit, and one run read as another.
	bad := t.TempDir()
	writeDiagArtifact(t, bad, "control", 1, "", func(m *runner.Manifest, _ map[string]any) { m.Build.Revision = strings.Repeat("e", 40) })
	out.Reset()
	err = runDiagRead([]string{"-commit", gateCommit, "-run", diagRun, bad}, &out, &errOut)
	if exitCode(err) != 3 || out.Len() != 0 || err == nil || !strings.Contains(err.Error(), "manifest revision") {
		t.Errorf("a manifest of another revision: status %d, printed %q, %v", exitCode(err), out.String(), err)
	}
}
