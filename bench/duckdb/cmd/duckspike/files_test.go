package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/duckdb/filestream"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// anActivityFile is the activity file of a build of a tiny plan, which holds records that
// have been through retentions, with the period they are in.
func anActivityFile(t *testing.T) (path, digest, start string, duration time.Duration) {
	t.Helper()
	plan := planFile(t, 3)
	sum := shaOf(t, plan)
	build := filepath.Join(t.TempDir(), "plan-build")
	if r := exec("build", "-plan", plan, "-expect-plan", sum, "-out", build, "-arms", "A2"); r.err != nil {
		t.Fatalf("build: %v\n%s", r.err, r.out)
	}
	path = filepath.Join(build, "activity.parquet")
	d, err := filestream.FileDigest(path)
	if err != nil {
		t.Fatal(err)
	}
	w := workload.Tiny()
	return path, d, w.Start.UTC().Format(time.RFC3339), w.Duration
}

// TestQuestionsBuildAdoptReadExplainSizes runs the path of an Alibaba hour on a synthetic
// file: questions from the activity file, every arm built from it (the hourly one among
// them), the archive adopted by its SHA-256, and each read, audited and sized.
func TestQuestionsBuildAdoptReadExplainSizes(t *testing.T) {
	act, digest, start, dur := anActivityFile(t)
	root := t.TempDir()
	qfile := filepath.Join(root, "questions.json")

	r := exec("questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", dur.String(), "-out", qfile, "-batch", "64", "-min-non-empty", "0")
	if r.err != nil {
		t.Fatalf("questions: %v\n%s", r.err, r.errOut)
	}
	qsum := shaOf(t, qfile)
	for _, want := range []string{"questions sha256 " + qsum, "input content digest " + digest, "stream digest ", "questions over "} {
		if !strings.Contains(r.out, want) {
			t.Errorf("questions: no %q in\n%s", want, r.out)
		}
	}
	noDuration(t, "questions", r.out)
	// The same questions again have the same name.
	again := filepath.Join(root, "again.json")
	if r2 := exec("questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", dur.String(), "-out", again, "-batch", "64", "-min-non-empty", "0"); r2.err != nil || shaOf(t, again) != qsum {
		t.Errorf("questions twice: %v, SHA-256 %s then %s", r2.err, qsum, shaOf(t, again))
	}

	build := filepath.Join(root, "build")
	r = exec("build", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-out", build, "-arms", "A1,A2,I,H", "-segment", "3m")
	if r.err != nil {
		t.Fatalf("build: %v\n%s", r.err, r.out)
	}
	for _, want := range []string{"questions sha256 " + qsum, "file a1/archive.parquet", "segments, ", "segment      0 hour "} {
		if !strings.Contains(r.out, want) {
			t.Errorf("build: no %q in\n%s", want, r.out)
		}
	}
	if strings.Contains(r.out, "file h/") {
		t.Errorf("build lists every file of the hourly arm:\n%s", r.out)
	}
	noDuration(t, "build", r.out, r.errOut)
	if shaOf(t, filepath.Join(build, "activity.parquet")) != shaOf(t, act) {
		t.Error("the build's activity file is not the input's bytes")
	}

	// The archive of the build is adopted by its SHA-256 as if it were an old file.
	archive := filepath.Join(build, "a1", "archive.parquet")
	adopted := filepath.Join(root, "adopted")
	r = exec("adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-archive", archive, "-expect-archive", shaOf(t, archive), "-out", adopted)
	if r.err != nil {
		t.Fatalf("adopt: %v\n%s", r.err, r.out)
	}
	for _, want := range []string{"the archive holds the activity file's ", "file a1/archive.parquet"} {
		if !strings.Contains(r.out, want) {
			t.Errorf("adopt: no %q in\n%s", want, r.out)
		}
	}
	noDuration(t, "adopt", r.out, r.errOut)

	for _, c := range []struct {
		dir  string
		arms string
		n    int
	}{{build, "A1,A2,I,H", 4}, {adopted, "A1", 1}} {
		counters := filepath.Join(root, "counters-"+filepath.Base(c.dir))
		r = exec("read", "-dir", c.dir, "-questions", qfile, "-expect-questions", qsum, "-arms", c.arms, "-out", counters)
		if r.err != nil {
			t.Fatalf("read %s: %v\n%s", c.dir, r.err, r.out)
		}
		if strings.Count(r.out, "G0 total:") != c.n || strings.Contains(r.out, "MISMATCH") {
			t.Errorf("read %s:\n%s", c.dir, r.out)
		}
		noDuration(t, "read", r.out, r.errOut)
		var cf duckengine.CountersFile
		b, err := os.ReadFile(filepath.Join(counters, "A1.counters.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &cf); err != nil {
			t.Fatal(err)
		}
		if cf.Plan.Kind != "questions" || cf.Plan.SHA256 != qsum || cf.Plan.Queries != len(cf.Queries) || cf.Passes != 2 {
			t.Errorf("the counters file: %+v", cf.Plan)
		}
	}
	if r.err == nil && !strings.Contains(r.out, "G0 total:") {
		t.Error("no read")
	}
	// The hourly arm prints the files it opened.
	r = exec("read", "-dir", build, "-questions", qfile, "-expect-questions", qsum, "-arms", "H")
	if r.err != nil || !strings.Contains(r.out, "files opened ") || !strings.Contains(r.out, "with a row group admitted") {
		t.Errorf("read H: %v\n%s", r.err, r.out)
	}
	h := filepath.Join(root, "counters-build", "H.counters.json")
	var hc duckengine.CountersFile
	if b, err := os.ReadFile(h); err != nil || json.Unmarshal(b, &hc) != nil || len(hc.Groups) == 0 || hc.Groups[0].FilesOpened == 0 {
		t.Errorf("the hourly arm's counters: %v %+v", err, hc.Groups)
	}

	for _, arm := range []string{"A1", "A2", "I", "H"} {
		r = exec("explain", "-dir", build, "-questions", qfile, "-expect-questions", qsum, "-arm", arm, "-max", "6")
		if r.err != nil || !strings.Contains(r.out, "the count agrees") || strings.Contains(r.out, "DIFFERS") {
			t.Errorf("explain %s: %v\n%s", arm, r.err, r.out)
		}
		if arm == "H" && !strings.Contains(r.out, "files: ") {
			t.Errorf("explain H does not audit files:\n%s", r.out)
		}
		noDuration(t, "explain "+arm, r.out, r.errOut)
	}

	r = exec("sizes", "-dir", build, "-verify-hashes")
	if r.err != nil || !strings.Contains(r.out, "arm H:") || strings.Contains(r.out, "h/000000") {
		t.Errorf("sizes: %v\n%s", r.err, r.out)
	}
	noDuration(t, "sizes", r.out)
	r = exec("sizes", "-dir", adopted, "-verify-hashes")
	if r.err != nil || !strings.Contains(r.out, "adopted archive of SHA-256 "+shaOf(t, archive)) {
		t.Errorf("sizes of the adopted archive: %v\n%s", r.err, r.out)
	}
}

// lateRefusals are the refusals of a build that the replay of the stream decides.
var lateRefusals = map[string]bool{
	"a build from questions and another file": true,
}

func TestQuestionsAndAdoptRefusals(t *testing.T) {
	act, digest, start, dur := anActivityFile(t)
	root := t.TempDir()
	qfile := filepath.Join(root, "questions.json")
	if r := exec("questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", dur.String(), "-out", qfile, "-batch", "64", "-min-non-empty", "0"); r.err != nil {
		t.Fatal(r.err)
	}
	qsum := shaOf(t, qfile)
	plan := planFile(t, 3)
	psum := shaOf(t, plan)
	build := filepath.Join(root, "build")
	if r := exec("build", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-out", build, "-arms", "A1,A2"); r.err != nil {
		t.Fatal(r.err)
	}
	archive := filepath.Join(build, "a1", "archive.parquet")
	asum := shaOf(t, archive)
	wd, _ := os.Getwd()
	inTree := filepath.Join(wd, "scratch-that-must-not-be-made")
	zeros := strings.Repeat("0", 64)
	otherDigest := "sha256:" + zeros
	otherQ := filepath.Join(root, "other.json")
	if r := exec("questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", "21m", "-out", otherQ, "-batch", "64", "-min-non-empty", "0"); r.err != nil {
		t.Fatal(r.err)
	}
	otherSum := shaOf(t, otherQ)
	x := filepath.Join(root, "x")

	for name, args := range map[string][]string{
		"questions of a file with another digest":         {"questions", "-activity", act, "-expect-digest", otherDigest, "-start", start, "-duration", "20m", "-out", x},
		"questions with no expected digest":               {"questions", "-activity", act, "-start", start, "-duration", "20m", "-out", x},
		"questions with no period":                        {"questions", "-activity", act, "-expect-digest", digest, "-out", x},
		"questions with a start that is not UTC":          {"questions", "-activity", act, "-expect-digest", digest, "-start", "2026-01-01T01:00:00+01:00", "-duration", "20m", "-out", x},
		"questions with a start that does not parse":      {"questions", "-activity", act, "-expect-digest", digest, "-start", "yesterday", "-duration", "20m", "-out", x},
		"questions of a period the file is not in":        {"questions", "-activity", act, "-expect-digest", digest, "-start", "2026-01-01T00:10:00Z", "-duration", "20m", "-out", x},
		"questions in a git work tree":                    {"questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", "20m", "-out", inTree},
		"questions with a batch of nothing":               {"questions", "-activity", act, "-expect-digest", digest, "-start", start, "-duration", "20m", "-out", x, "-batch", "0"},
		"questions of a file that is not there":           {"questions", "-activity", filepath.Join(root, "none"), "-expect-digest", digest, "-start", start, "-duration", "20m", "-out", x},
		"a build from questions with no activity file":    {"build", "-questions", qfile, "-expect-questions", qsum, "-out", x},
		"a build from questions with no hash":             {"build", "-questions", qfile, "-activity", act, "-out", x},
		"a build from questions with another hash":        {"build", "-questions", qfile, "-expect-questions", zeros, "-activity", act, "-out", x},
		"a build from a plan with an activity file":       {"build", "-plan", plan, "-expect-plan", psum, "-activity", act, "-out", x},
		"a build from a plan and questions":               {"build", "-plan", plan, "-expect-plan", psum, "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-out", x},
		"a build from a plan and questions, no plan hash": {"build", "-plan", plan, "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-out", x},
		"a build with a plan's hash for questions":        {"build", "-questions", qfile, "-expect-plan", psum, "-activity", act, "-out", x},
		"a build with questions' hash for a plan":         {"build", "-plan", plan, "-expect-questions", qsum, "-out", x},
		"a build from questions and another file":         {"build", "-questions", qfile, "-expect-questions", qsum, "-activity", archive, "-out", x},
		"a build of an hour of nothing":                   {"build", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-out", x, "-arms", "H", "-segment", "-1m"},
		"a read of other questions":                       {"read", "-dir", build, "-questions", otherQ, "-expect-questions", otherSum, "-arms", "A2"},
		"a read of the hourly arm that was not built":     {"read", "-dir", build, "-questions", qfile, "-expect-questions", qsum, "-arms", "H"},
		"an adopt with no expected archive hash":          {"adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-archive", archive, "-out", x},
		"an adopt of another hash":                        {"adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-archive", archive, "-expect-archive", zeros, "-out", x},
		"an adopt in a git work tree":                     {"adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-archive", archive, "-expect-archive", asum, "-out", inTree},
		"an adopt with no questions' hash":                {"adopt", "-questions", qfile, "-activity", act, "-archive", archive, "-expect-archive", asum, "-out", x},
		"an adopt whose activity file is not one":         {"adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", archive, "-archive", archive, "-expect-archive", asum, "-out", x},
	} {
		r := exec(args...)
		if r.err == nil {
			t.Errorf("%s was accepted:\n%s", name, r.out)
		}
		noDuration(t, name, r.out)
		// A refusal that is decided before anything is built leaves nothing. One that the
		// replay of the stream decides (a file that is not the questions' stream) leaves
		// the directory it had begun, as every build does, and it is not reused: a build
		// refuses a directory that holds anything.
		if _, err := os.Stat(x); err == nil {
			if !lateRefusals[name] {
				t.Errorf("%s made %s", name, x)
			}
			os.RemoveAll(x)
		}
	}
	if _, err := os.Stat(inTree); err == nil {
		os.RemoveAll(inTree)
		t.Error("a directory was made inside the work tree")
	}
	// The archive's hash is asked for by the command, not left to the adoption.
	if r := exec("adopt", "-questions", qfile, "-expect-questions", qsum, "-activity", act, "-archive", archive, "-out", filepath.Join(root, "y")); r.err == nil || !strings.Contains(r.err.Error(), "-expect-archive is required") {
		t.Errorf("an adopt with no -expect-archive: %v", r.err)
	}
	// A question set whose bytes were changed is refused by its SHA-256 before it is read.
	b, err := os.ReadFile(qfile)
	if err != nil {
		t.Fatal(err)
	}
	edited := filepath.Join(root, "edited.json")
	if err := os.WriteFile(edited, bytes.Replace(b, []byte(`"Rank": 0`), []byte(`"Rank": 7`), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := exec("read", "-dir", build, "-questions", edited, "-expect-questions", qsum, "-arms", "A2"); r.err == nil || !strings.Contains(r.err.Error(), "SHA-256") {
		t.Errorf("an edited question set: %v", r.err)
	}
}
