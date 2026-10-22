package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// fixtureDigest is the digest of the report of the fixture run by fixtureArgs.
const fixtureDigest = "a31c0b867b7f8c142554575b980975efd98953a527eec84e2e041933eec8e368"

// The fixture through the pipeline gives the table pinned in testdata: the
// sightings the product's translator makes of it, what the coalescer writes, and for
// each edge at each sampled instant whether it exists and whether a bound above zero
// is derived, at both tokens and both expiries. A pipeline that drifted from the
// product's receiver, sequencer or translator would change a count.
func TestFixtureTable(t *testing.T) {
	out := t.TempDir()
	code, stdout, stderr := runCmd(t, fixtureArgs(t, out)...)
	if code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	rep := readReport(t, out)
	got, err := json.MarshalIndent(rep.Runs, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "fixture.runs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
		t.Errorf("the table differs from testdata/fixture.runs.json:\n%s", got)
	}
	if rep.Digest != fixtureDigest {
		t.Errorf("digest %s, want %s", rep.Digest, fixtureDigest)
	}
	if f := rep.failures(); len(f) > 0 {
		t.Errorf("self-checks failed: %v", f)
	}
	if rep.Lines.Lines != 20 || rep.Lines.Sent != 20 || rep.Lines.OutOfOrder != 1 {
		t.Errorf("lines %+v: want 20 read, 20 sent, 1 out of order", rep.Lines)
	}
}

// The counts the table pins, read by hand from the fixture, so that a golden made
// by the code alone is not the only witness. At an expiry of ten minutes the
// extension interval is five: an edge whose last sighting a producer made at m
// minutes has a record whose last instant is at most that, and its silence at t
// shows from m+5 on; the late sighting arrives after the instants it is before.
func TestFixtureHandCounts(t *testing.T) {
	out := t.TempDir()
	if code, _, stderr := runCmd(t, fixtureArgs(t, out)...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	rep := readReport(t, out)
	run := rep.Runs[0] // 10 minutes
	if run.X.Minutes() != 5 || run.HalfLife.Seconds() != 75 {
		t.Fatalf("X %s, half-life %s", run.X, run.HalfLife)
	}
	// Sightings: frontend -> cart is incremented by every line but the first, whose
	// point has no state yet (the translator is warming up): 18. frontend -> ads is
	// incremented by lines 1 to 7: 7. cart -> payment by lines 1 to 4 and 12: 5.
	// checkout -> email only by the late line: 1. The calls to a database make none.
	want := map[string]int{"frontend -> cart": 18, "frontend -> ads": 7, "cart -> payment": 5, "checkout -> email": 1}
	for _, tk := range run.Tokens {
		for _, e := range tk.Edges {
			if e.Sightings != want[e.Edge] {
				t.Errorf("%s: %s has %d sightings, want %d", tk.Kind, e.Edge, e.Sightings, want[e.Edge])
			}
		}
	}
	row := func(kind, edge string) edgeRow {
		for _, tk := range run.Tokens {
			if tk.Kind != kind {
				continue
			}
			for _, e := range tk.Edges {
				if e.Edge == edge {
					return e
				}
			}
		}
		t.Fatalf("no row %s %s", kind, edge)
		return edgeRow{}
	}
	// frontend -> ads: last sighting at minute 7, so it exists until minute 17 (its
	// record's expiry is ten minutes on) and is read at minutes 1 to 16, minute 1
	// included only at the full token; at minute 16 its true silence is nine minutes
	// and the bound four. The bound is the true silence less X, so the shortfall is
	// X exactly there.
	if a := row(tokenFull, "frontend -> ads"); a.Members != 16 || a.MaxTrue.Minutes() != 9 || a.MaxBound.Minutes() != 4 || a.MaxShort.Minutes() != 5 {
		t.Errorf("full, ads: %+v", a)
	}
	if a := row(tokenAsOfInstant, "frontend -> ads"); a.Members != 15 {
		t.Errorf("as of the instant, ads: %+v: the sighting of an instant arrives after it", a)
	}
	// checkout -> email: its one sighting is at 9.5 minutes and arrives after the
	// instant at minute 10 has been read, so only the full token has that instant.
	if a, b := row(tokenFull, "checkout -> email"), row(tokenAsOfInstant, "checkout -> email"); a.Members != 9 || b.Members != 8 {
		t.Errorf("email: %d members at the full token, %d as of the instant, want 9 and 8", a.Members, b.Members)
	}
	// frontend -> cart is sighted every minute: its silence never exceeds a minute,
	// below X, so it never shows.
	for _, kind := range []string{tokenFull, tokenAsOfInstant} {
		if a := row(kind, "frontend -> cart"); a.Shown != 0 || a.TrueOverX != 0 {
			t.Errorf("%s, cart: %+v shows a silence of a steady edge", kind, a)
		}
	}
	for _, c := range run.Counters {
		switch c.Name {
		case "svcgraph.skipped_connection_database":
			if c.N != 19 {
				t.Errorf("%s = %d, want 19: the database series is on every line", c.Name, c.N)
			}
		case "svcgraph.skipped_warm_up":
			if c.N != 4 {
				t.Errorf("%s = %d, want 4: the first point of four series", c.Name, c.N)
			}
		case "metrics.dropped_not_sum":
			if c.N != 1 {
				t.Errorf("%s = %d, want 1: the gauge", c.Name, c.N)
			}
		case "spancount.sightings":
			if c.N != 3 {
				t.Errorf("%s = %d, want 3", c.Name, c.N)
			}
		}
	}
}

// Two runs of the same capture and flags give the same digest and the same files,
// but for the command line, which names the output directory.
func TestRunsAreDeterministic(t *testing.T) {
	var digests, tables []string
	for range 2 {
		out := t.TempDir()
		if code, _, stderr := runCmd(t, fixtureArgs(t, out)...); code != 0 {
			t.Fatalf("exit %d: %s", code, stderr)
		}
		rep := readReport(t, out)
		b, err := json.Marshal(rep.Runs)
		if err != nil {
			t.Fatal(err)
		}
		digests, tables = append(digests, rep.Digest), append(tables, string(b))
	}
	if digests[0] != digests[1] || tables[0] != tables[1] {
		t.Errorf("two runs differ: %s and %s", digests[0], digests[1])
	}
}

// The window ends the replay: lines whose newest point is at or after the end are
// not sent, and no instant at or after it is sampled; the sampling starts at the
// first instant of the grid at or after the first line.
func TestWindowBoundsTheReplay(t *testing.T) {
	out := t.TempDir()
	code, _, stderr := runCmd(t, fixtureArgs(t, out, "-to", "2026-01-01T00:10:00Z", "-from", "2025-12-31T23:59:30Z", "-step", "1m")...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	rep := readReport(t, out)
	// Lines at minutes 0 to 9 are before the end; the minute 10 line and everything
	// after it, the late line (at 9.5 minutes, after minute 10's line) included only
	// if it is before the end: it is.
	if rep.Lines.Lines != 20 || rep.Lines.AtOrAfterTo != 9 || rep.Lines.Sent != 11 {
		t.Errorf("lines %+v: want 20 read, 9 at or after the end, 11 sent", rep.Lines)
	}
	for _, run := range rep.Runs {
		// The grid is 23:59:30, 00:00:30, ...: the first line is at 00:00:00, so the
		// first instant sampled is 00:00:30, and the last is 00:09:30.
		if run.FirstSample != "2026-01-01T00:00:30Z" || run.Samples != 10 {
			t.Errorf("expiry %s: first sample %s, %d samples; want 00:00:30 and 10", run.Expiry, run.FirstSample, run.Samples)
		}
	}
}

// A run writes a report that is also printed, and never over a report.
func TestReportIsWrittenOnceAndPrinted(t *testing.T) {
	out := filepath.Join(t.TempDir(), "new", "dir")
	code, stdout, stderr := runCmd(t, fixtureArgs(t, out)...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	text, err := os.ReadFile(filepath.Join(out, reportFile))
	if err != nil {
		t.Fatal(err)
	}
	if stdout != string(text) {
		t.Errorf("the printed report is not the written one")
	}
	for _, want := range []string{"local, deterministic", fixtureSHA256, "digest      " + fixtureDigest} {
		if !bytes.Contains(text, []byte(want)) {
			t.Errorf("report.txt lacks %q", want)
		}
	}
}

// Reading from before the instant, so that records of the window and not only the
// seeds are read, changes no membership and finds no fault.
func TestALookbackChangesNoMembership(t *testing.T) {
	var members [2][]int
	for i, lookback := range []string{"0s", "7m"} {
		out := t.TempDir()
		code, _, stderr := runCmd(t, fixtureArgs(t, out, "-lookback", lookback)...)
		if code != 0 {
			t.Fatalf("lookback %s: exit %d: %s", lookback, code, stderr)
		}
		for _, run := range readReport(t, out).Runs {
			for _, tk := range run.Tokens {
				members[i] = append(members[i], tk.Totals.Members, tk.Totals.Shown)
			}
		}
	}
	if len(members[0]) == 0 || !slices.Equal(members[0], members[1]) {
		t.Errorf("members and shown without a lookback %v, with one %v", members[0], members[1])
	}
}
