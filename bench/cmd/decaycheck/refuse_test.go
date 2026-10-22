package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A run is made only on a registered capture and window: with any of the inputs
// missing or wrong the command exits with status 2, says why, and has read none of
// the capture and written nothing.
func TestRefusesWithoutItsRegisteredInputs(t *testing.T) {
	capture := fixtureFile(t)
	base := func(out string) map[string]string {
		return map[string]string{
			"-capture":        capture,
			"-capture-sha256": fixtureSHA256,
			"-from":           "2026-01-01T00:00:00Z",
			"-to":             "2026-01-01T00:19:00Z",
			"-out":            out,
		}
	}
	args := func(m map[string]string) []string {
		var a []string
		for k, v := range m {
			a = append(a, k, v)
		}
		return a
	}
	for _, missing := range []string{"-capture", "-capture-sha256", "-from", "-to", "-out"} {
		t.Run("missing "+missing, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "out")
			m := base(out)
			delete(m, missing)
			code, stdout, stderr := runCmd(t, args(m)...)
			if code != exitRefused || !strings.Contains(stderr, "refused: "+missing+" is required") {
				t.Errorf("exit %d, stderr %q", code, stderr)
			}
			if stdout != "" {
				t.Errorf("printed %q", stdout)
			}
			if _, err := os.Stat(out); err == nil {
				t.Errorf("%s was made", out)
			}
		})
	}
}

// The digest is checked before anything is replayed: a capture that does not have the
// registered digest is refused, even when it could not be replayed anyway, and its
// lines are never read.
func TestRefusesACaptureThatIsNotTheRegisteredOne(t *testing.T) {
	dir := t.TempDir()
	// Not metrics, not even JSON: a replay would fail on its first line, so the
	// refusal shows the digest was checked first.
	bad := writeFile(t, dir, "x.jsonl", "this is not a capture\n")
	out := filepath.Join(dir, "out")
	for name, sum := range map[string]string{
		"a digest of another capture": fixtureSHA256,
		"zeros":                       strings.Repeat("0", 64),
	} {
		t.Run(name, func(t *testing.T) {
			code, _, stderr := runCmd(t, fixtureArgs(t, out, "-capture", bad, "-capture-sha256", sum)...)
			if code != exitRefused || !strings.Contains(stderr, "is not the registered capture") {
				t.Errorf("exit %d, stderr %q", code, stderr)
			}
			if _, err := os.Stat(out); err == nil {
				t.Errorf("%s was made", out)
			}
		})
	}
	// The same file, given its own digest, is read, and fails there: status 1.
	sum := sha256.Sum256([]byte("this is not a capture\n"))
	code, _, stderr := runCmd(t, fixtureArgs(t, out, "-capture", bad, "-capture-sha256", hex.EncodeToString(sum[:]))...)
	if code != exitFailed || strings.Contains(stderr, "refused") {
		t.Errorf("exit %d, stderr %q: a registered capture that cannot be read fails, it is not refused", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(out, resultFile)); err == nil {
		t.Errorf("a report was written for a replay that failed")
	}
}

// The digest is of the parts joined in name order, as a frozen capture's is made.
func TestTheDigestIsOfThePartsJoinedInNameOrder(t *testing.T) {
	b, err := os.ReadFile(fixtureSource)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(b), "\n")
	dir := t.TempDir()
	cut := 7
	// Named so that name order is the capture's order and file-system order is not
	// guaranteed to be.
	writeFile(t, dir, "b.jsonl", strings.Join(lines[cut:], ""))
	writeFile(t, dir, "a.jsonl", strings.Join(lines[:cut], ""))
	writeFile(t, dir, "notes.txt", "not a part")
	out := filepath.Join(t.TempDir(), "out")
	code, _, stderr := runCmd(t, fixtureArgs(t, out, "-capture", dir)...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	rep := readReport(t, out)
	got, err := json.Marshal(rep.Runs)
	if err != nil {
		t.Fatal(err)
	}
	whole := t.TempDir()
	if code, _, stderr := runCmd(t, fixtureArgs(t, whole)...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want, err := json.Marshal(readReport(t, whole).Runs)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Capture.SHA256 != fixtureSHA256 || len(rep.Capture.Parts) != 2 || rep.Capture.Parts[0] != "a.jsonl" || string(got) != string(want) {
		t.Errorf("sha256 %s, parts %v: the same lines in two parts must give the fixture's table", rep.Capture.SHA256, rep.Capture.Parts)
	}
}

// The report is never written inside a git work tree or the capture, and never over
// another report.
func TestRefusesAnUnsafeOutput(t *testing.T) {
	t.Run("inside a git work tree", func(t *testing.T) {
		// A directory with a .git in it, as a work tree's root has (a file in a
		// linked work tree).
		tree := t.TempDir()
		writeFile(t, tree, ".git", "gitdir: elsewhere\n")
		dir := filepath.Join(tree, "reports", "today")
		code, _, stderr := runCmd(t, fixtureArgs(t, dir)...)
		if code != exitRefused || !strings.Contains(stderr, "git work tree") {
			t.Errorf("exit %d, stderr %q", code, stderr)
		}
		if _, err := os.Stat(dir); err == nil {
			t.Errorf("%s was made", dir)
		}
	})
	t.Run("inside the capture", func(t *testing.T) {
		cap := t.TempDir()
		b, _ := os.ReadFile(fixtureSource)
		writeFile(t, cap, "a.jsonl", string(b))
		code, _, stderr := runCmd(t, fixtureArgs(t, filepath.Join(cap, "out"), "-capture", cap)...)
		if code != exitRefused || !strings.Contains(stderr, "inside the capture") {
			t.Errorf("exit %d, stderr %q", code, stderr)
		}
		code, _, stderr = runCmd(t, fixtureArgs(t, cap, "-capture", cap)...)
		if code != exitRefused || !strings.Contains(stderr, "inside the capture") {
			t.Errorf("the capture's own directory: exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("a directory that holds the capture file", func(t *testing.T) {
		cap := t.TempDir()
		b, _ := os.ReadFile(fixtureSource)
		file := writeFile(t, cap, "a.jsonl", string(b))
		code, _, stderr := runCmd(t, fixtureArgs(t, cap, "-capture", file)...)
		if code != exitRefused || !strings.Contains(stderr, "holds the capture") {
			t.Errorf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("over a report", func(t *testing.T) {
		out := t.TempDir()
		if code, _, stderr := runCmd(t, fixtureArgs(t, out)...); code != 0 {
			t.Fatalf("exit %d: %s", code, stderr)
		}
		before, err := os.ReadFile(filepath.Join(out, resultFile))
		if err != nil {
			t.Fatal(err)
		}
		code, _, stderr := runCmd(t, fixtureArgs(t, out)...)
		if code != exitRefused || !strings.Contains(stderr, "never written over") {
			t.Errorf("exit %d, stderr %q", code, stderr)
		}
		after, _ := os.ReadFile(filepath.Join(out, resultFile))
		if string(before) != string(after) {
			t.Errorf("the report was changed")
		}
	})
}

// A window, a step or an expiry that cannot be used is refused before the capture is
// read.
func TestRefusesAnUnusableSetting(t *testing.T) {
	for name, extra := range map[string][]string{
		"a window that ends before it starts": {"-from", "2026-01-01T00:19:00Z", "-to", "2026-01-01T00:00:00Z"},
		"an empty window":                     {"-to", "2026-01-01T00:00:00Z"},
		"a time that is not RFC 3339":         {"-from", "yesterday"},
		"a step of zero":                      {"-step", "0s"},
		"a negative lookback":                 {"-lookback", "-1m"},
		"an expiry of zero":                   {"-expiries", "0s"},
		"an expiry past a year":               {"-expiries", "9000h"},
		"an expiry twice":                     {"-expiries", "1h,1h"},
		"an expiry with no extension":         {"-expiries", "1ns"},
		"an expiry that is not a duration":    {"-expiries", "six hours"},
		"a digest that is not hexadecimal":    {"-capture-sha256", strings.Repeat("g", 64)},
		"a digest of the wrong length":        {"-capture-sha256", "abc"},
		"an argument that is not a flag":      {"extra"},
		"an unknown flag":                     {"-quick"},
	} {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "out")
			code, _, stderr := runCmd(t, fixtureArgs(t, out, extra...)...)
			if code != exitRefused {
				t.Errorf("exit %d, stderr %q", code, stderr)
			}
			if _, err := os.Stat(out); err == nil {
				t.Errorf("%s was made", out)
			}
		})
	}
}

// A capture that is not metrics is an error, not a replay of nothing.
func TestAFileOfLogsFails(t *testing.T) {
	dir := t.TempDir()
	logs := `{"resourceLogs":[]}` + "\n"
	sum := sha256.Sum256([]byte(logs))
	file := writeFile(t, dir, "l.jsonl", logs)
	out := filepath.Join(dir, "out")
	code, _, stderr := runCmd(t, fixtureArgs(t, out, "-capture", file, "-capture-sha256", hex.EncodeToString(sum[:]))...)
	if code != exitFailed || !strings.Contains(stderr, "must hold metrics") {
		t.Errorf("exit %d, stderr %q", code, stderr)
	}
}

// A capture with no line before the end of the window has nothing to replay.
func TestAWindowBeforeTheCaptureFails(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	code, _, stderr := runCmd(t, fixtureArgs(t, out, "-from", "2025-12-31T00:00:00Z", "-to", "2025-12-31T01:00:00Z")...)
	if code != exitFailed || !strings.Contains(stderr, "no line of the capture is before the end of the window") {
		t.Errorf("exit %d, stderr %q", code, stderr)
	}
}
