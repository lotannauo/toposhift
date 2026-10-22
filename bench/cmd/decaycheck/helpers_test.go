package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// fixtureSHA256 is the digest of the fixture written as a capture file (see
// fixtureFile): a hand-made capture of
// twenty lines, one a minute from 2026-01-01T00:00:00Z but for one that arrives
// late. It holds four service graph series that are exported every minute, one that
// stops being incremented, one that is not exported for a while, and one whose only
// increase arrives after a later line; a series of calls to a database, which is
// set aside; a span count; and a gauge.
const fixtureSHA256 = "c767bc563f329630f8a989e44a45ba00dc0201646179c3912b0bb91f8dba84e3"

// fixtureSource is the fixture as it is tracked. It is not named *.jsonl, which
// the repository keeps out of bench/; every test that needs a capture writes it
// to a directory of its own as fixture.jsonl.
const fixtureSource = "testdata/fixture.ndjson.txt"

// fixtureFile writes the fixture to a new temporary directory as fixture.jsonl, and
// returns its path.
func fixtureFile(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(fixtureSource)
	if err != nil {
		t.Fatal(err)
	}
	return writeFile(t, t.TempDir(), "fixture.jsonl", string(b))
}

// runCmd runs the command with its own streams.
func runCmd(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

// fixtureArgs are the arguments of a run of the fixture that writes to out; extra
// flags come after and override the defaults (a later flag wins).
func fixtureArgs(t *testing.T, out string, extra ...string) []string {
	t.Helper()
	args := []string{
		"-capture", fixtureFile(t),
		"-capture-sha256", fixtureSHA256,
		"-from", "2026-01-01T00:00:00Z",
		"-to", "2026-01-01T00:19:00Z",
		"-expiries", "10m,4m",
		"-out", out,
	}
	return append(args, extra...)
}

// readReport reads the result.json a run wrote.
func readReport(t *testing.T, dir string) report {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, resultFile))
	if err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// writeFile writes a capture file for a test.
func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}
