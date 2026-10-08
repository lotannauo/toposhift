package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// buildStore writes a few keys the way a layout-L store lays them out: a meta key
// under 0x00 and 37-byte data keys under layers, one of each kind.
func buildStore(t *testing.T) (dir string, keys [][]byte) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "db")
	db, err := pebble.Open(dir, &pebble.Options{Logger: quiet{}})
	if err != nil {
		t.Fatal(err)
	}
	data := func(layer, id, kind byte) []byte {
		k := make([]byte, 37)
		k[0], k[19], k[36] = layer, id, kind
		return k
	}
	keys = [][]byte{{0, 'm'}, data(1, 1, 0), data(1, 1, 1), data(2, 3, 2), data(4, 9, 0)}
	for i, k := range keys {
		if err := db.Set(k, []byte(fmt.Sprintf("value %d", i)), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, keys
}

func digestOf(t *testing.T, dir string, lo, hi []byte) pebblekv.Digest {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true, Logger: quiet{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	d, err := pebblekv.DigestRange(db, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func runOK(t *testing.T, args ...string) string {
	t.Helper()
	var out, errOut bytes.Buffer
	if code := run(args, &out, &errOut); code != 0 || errOut.Len() != 0 {
		t.Fatalf("run(%q) = %d, stderr %q; want 0 and nothing", args, code, errOut.String())
	}
	return out.String()
}

func TestPrintsTheDigestOfTheWholeDatabase(t *testing.T) {
	t.Parallel()
	dir, keys := buildStore(t)
	d := digestOf(t, dir, nil, nil)
	if d.Keys != int64(len(keys)) {
		t.Fatalf("test setup: digest counts %d keys, store has %d", d.Keys, len(keys))
	}
	want := fmt.Sprintf("%s keys %d records %d checkpoints %d baselines %d sha256 %x\n", dir, d.Keys, d.Records, d.Checkpoints, d.Baselines, d.SHA256)
	if got := runOK(t, dir); got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
	if d.Records != 2 || d.Checkpoints != 1 || d.Baselines != 1 {
		t.Errorf("counts = %d records, %d checkpoints, %d baselines; want 2, 1, 1", d.Records, d.Checkpoints, d.Baselines)
	}
}

func TestDataIgnoresTheMetaKeys(t *testing.T) {
	t.Parallel()
	dir, keys := buildStore(t)
	whole := digestOf(t, dir, nil, nil)
	data := digestOf(t, dir, pebblekv.DataLo, pebblekv.DataHi)
	if data.Keys != int64(len(keys)-1) || data.SHA256 == whole.SHA256 {
		t.Fatalf("test setup: the data digest has %d keys and %x, the whole %x; want one key fewer and another hash", data.Keys, data.SHA256, whole.SHA256)
	}
	want := fmt.Sprintf("%s data keys %d records %d checkpoints %d baselines %d sha256 %x\n", dir, data.Keys, data.Records, data.Checkpoints, data.Baselines, data.SHA256)
	if got := runOK(t, "-data", dir); got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestOneLinePerDatabaseInOrder(t *testing.T) {
	t.Parallel()
	a, _ := buildStore(t)
	b, _ := buildStore(t)
	lines := strings.Split(strings.TrimSuffix(runOK(t, a, b), "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], a+" ") || !strings.HasPrefix(lines[1], b+" ") {
		t.Errorf("output = %q, want a line for %s and then one for %s", lines, a, b)
	}
}

func TestTailsReportsThePrefixes(t *testing.T) {
	t.Parallel()
	dir, _ := buildStore(t)
	got := runOK(t, "-tails", dir)
	// Three data prefixes (layer, entity bytes): the first holds a record and a
	// checkpoint, and the others are one key each.
	if !strings.HasPrefix(got, dir+" prefixes 3 |") {
		t.Errorf("output = %q, want 3 prefixes", got)
	}
}

func TestAMissingDatabaseExitsWithOne(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	missing := filepath.Join(t.TempDir(), "nothing")
	for _, args := range [][]string{{missing}, {"-data", missing}, {"-tails", missing}} {
		out.Reset()
		errOut.Reset()
		if code := run(args, &out, &errOut); code != 1 {
			t.Errorf("run(%q) = %d, want 1", args, code)
		}
		if !strings.Contains(errOut.String(), missing) || out.Len() != 0 {
			t.Errorf("run(%q): stdout %q, stderr %q; want the directory named on stderr and nothing on stdout", args, out.String(), errOut.String())
		}
	}
}

func TestBadUsageExitsWithTwo(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"-data"}, {"-tails"}, {"-nope", "x"}, {"-data", "-tails", "x"}} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 2 {
			t.Errorf("run(%q) = %d, want 2", args, code)
		}
		if out.Len() != 0 || errOut.Len() == 0 {
			t.Errorf("run(%q): stdout %q, stderr %q; want usage on stderr only", args, out.String(), errOut.String())
		}
	}
}

func TestEachDatabaseIsOpenedReadOnlyAndLeftAsItWas(t *testing.T) {
	t.Parallel()
	dir, _ := buildStore(t)
	before := digestOf(t, dir, nil, nil)
	runOK(t, dir)
	runOK(t, "-data", dir)
	runOK(t, "-tails", dir)
	if after := digestOf(t, dir, nil, nil); after != before {
		t.Errorf("digest after running the command = %+v, before %+v", after, before)
	}
}

func TestAFailingDatabaseDoesNotStopTheOthers(t *testing.T) {
	t.Parallel()
	a, _ := buildStore(t)
	b, _ := buildStore(t)
	missing := filepath.Join(t.TempDir(), "nothing")
	for _, mode := range [][]string{nil, {"-data"}, {"-tails"}} {
		var out, errOut bytes.Buffer
		args := append(slices.Clone(mode), a, missing, b)
		if code := run(args, &out, &errOut); code != 1 {
			t.Errorf("run(%q) = %d, want 1", args, code)
		}
		lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], a+" ") || !strings.HasPrefix(lines[1], b+" ") {
			t.Errorf("run(%q): stdout %q, want a line for %s and one for %s", args, out.String(), a, b)
		}
		if got := errOut.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, missing) {
			t.Errorf("run(%q): stderr %q, want one line naming %s", args, got, missing)
		}
	}
}
