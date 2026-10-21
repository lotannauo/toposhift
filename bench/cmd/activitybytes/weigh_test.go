package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// frozenDataDigest is the digest of the data keys and values of the store the fixture
// makes under the default options, in the form dbhash -data prints. It is of keys and
// values, not of table bytes, so a new version of Pebble does not move it. It is
// never regenerated to make a test pass: a change of it is a change of what the
// store holds for these records. The fixture is written in batches of 37, as
// testConfig writes them; the keys and values do not depend on the batch, but the
// digest was taken with it.
const frozenDataDigest = "ace952da5560b27d2d39b4cf73269815416b6c0bba81c5bc405c91b73f83750f"

// testConfig is a measurement of the fixture with its own output directory.
func testConfig(tb testing.TB, f fixture, label string) config {
	tb.Helper()
	return config{
		In: f.path, Out: tb.TempDir(), Label: label, Checkpoints: checkpointsDefault, Batch: 37,
		ExpectRecords: int64(len(f.records)), Samples: f.samples,
	}
}

// load runs a measurement, failing the test on an error.
func load(t *testing.T, cfg config) (*result, diagnostics) {
	t.Helper()
	res, diag, err := weigh(context.Background(), cfg)
	if err != nil {
		t.Fatalf("weighing %s: %v", cfg.Label, err)
	}
	return res, diag
}

// The default measurement of the fixture is made once for the tests that only read
// its result: a load takes seconds.
var (
	baselineOnce sync.Once
	baselineRes  *result
	baselineDiag diagnostics
	baselineErr  error
)

// baseline is the default measurement of the fixture, with checkpoints on.
func baseline(t *testing.T) (fixture, *result, diagnostics) {
	t.Helper()
	f := newFixture(t)
	baselineOnce.Do(func() {
		baselineRes, baselineDiag, baselineErr = weigh(context.Background(), testConfig(t, f, "T/S"))
	})
	if baselineErr != nil {
		t.Fatalf("the default measurement of the fixture: %v", baselineErr)
	}
	return f, baselineRes, baselineDiag
}

// checkRecords is an error unless the records written are the file's, and the keys
// are not taken for them: an edge is stored under two prefixes.
func checkRecords(res *result, f fixture) error {
	if res.Records != int64(len(f.records)) {
		return fmt.Errorf("%d records written, the file holds %d", res.Records, len(f.records))
	}
	if res.Keys <= res.Records {
		return fmt.Errorf("%d keys for %d records: the fixture's edges are stored twice, so the keys must pass the records", res.Keys, res.Records)
	}
	if res.Input == "" || res.InputDigest == "" || len(res.InputSHA256) != 64 {
		return fmt.Errorf("the input is not named: %q %q %q", res.Input, res.InputDigest, res.InputSHA256)
	}
	return nil
}

// checkSettled is an error unless the store was compacted and canonicalized: every
// table is in the bottom level, and Canonicalize ingested its tables.
func checkSettled(res *result, diag diagnostics) error {
	last := len(res.TablesPerLevel) - 1
	for l, n := range res.TablesPerLevel {
		if l < last && n != 0 {
			return fmt.Errorf("%d tables in level %d, want all in the bottom level: %v", n, l, res.TablesPerLevel)
		}
	}
	if res.TablesPerLevel[last] == 0 || res.LiveTableBytes == 0 {
		return fmt.Errorf("no live tables: %v, %d bytes", res.TablesPerLevel, res.LiveTableBytes)
	}
	if diag.Ingests == 0 {
		return errors.New("Canonicalize did not ingest its tables")
	}
	return nil
}

// checkDefaultCheckpoints is an error unless the default policy under the boot key
// places checkpoints on edge prefixes and none on an entity prefix.
func checkDefaultCheckpoints(res *result) error {
	if res.CheckpointsEdge == 0 {
		return fmt.Errorf("no checkpoint on an edge prefix with more than 64 records: %d in all", res.Checkpoints)
	}
	if res.CheckpointsEntity != 0 {
		return fmt.Errorf("%d checkpoints on entity prefixes, which the boot key keeps free of them", res.CheckpointsEntity)
	}
	return nil
}

// checkNoCheckpoints is an error if the store holds a checkpoint.
func checkNoCheckpoints(res *result) error {
	if res.Checkpoints != 0 || res.CheckpointsEntity != 0 || res.CheckpointsEdge != 0 {
		return fmt.Errorf("%d checkpoints (%d on entity prefixes, %d on edge prefixes) with the policy off", res.Checkpoints, res.CheckpointsEntity, res.CheckpointsEdge)
	}
	return nil
}

func checkFrozenDigest(res *result) error {
	if res.DataDigest != frozenDataDigest {
		return fmt.Errorf("data digest %s, frozen %s", res.DataDigest, frozenDataDigest)
	}
	return nil
}

func TestLoadWritesTheFilesRecords(t *testing.T) {
	t.Parallel()

	f, res, diag := baseline(t)
	if err := checkRecords(res, f); err != nil {
		t.Error(err)
	}
	if err := checkSettled(res, diag); err != nil {
		t.Error(err)
	}
	if res.BlobLiveBytes != 0 {
		t.Errorf("%d blob bytes", res.BlobLiveBytes)
	}
	if res.BytesPerRecord == nil || *res.BytesPerRecord != float64(res.LiveTableBytes)/float64(res.Records) {
		t.Errorf("bytes per record %v, want live %d over records %d", res.BytesPerRecord, res.LiveTableBytes, res.Records)
	}
	if res.BytesPerSample == nil || *res.BytesPerSample != float64(res.LiveTableBytes)/float64(f.samples) {
		t.Errorf("bytes per sample %v, want live %d over samples %d", res.BytesPerSample, res.LiveTableBytes, f.samples)
	}
	if diag.Baselines != 0 {
		t.Errorf("%d baselines in a store nothing retained", diag.Baselines)
	}
	if res.InputDigest != "" && !strings.HasPrefix(res.InputDigest, "sha256:") {
		t.Errorf("input digest %q", res.InputDigest)
	}
}

func TestExpectedRecordsAreCheckedBeforeAnyWrite(t *testing.T) {
	f := newFixture(t)
	cfg := testConfig(t, f, "T/S")
	base := t.TempDir()
	cfg.Out = filepath.Join(base, "not", "made", "yet")
	cfg.ExpectRecords++
	if _, _, err := weigh(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "expected") {
		t.Fatalf("weigh with one record more expected than the footer holds = %v, want a refusal", err)
	}
	if _, err := os.Stat(filepath.Join(base, "not")); err == nil {
		t.Errorf("the refusal came after %s was made: nothing is made before the count is checked", cfg.Out)
	}
}

func TestTwoLoadsAgree(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	a, da := load(t, testConfig(t, f, "T/S"))
	b, _ := load(t, testConfig(t, f, "T/S-rep"))
	if err := checkSettled(a, da); err != nil {
		t.Error(err)
	}
	if a.DataDigest != b.DataDigest || a.LiveTableBytes != b.LiveTableBytes || a.Keys != b.Keys || a.Records != b.Records || a.Checkpoints != b.Checkpoints {
		t.Errorf("two loads of one file differ:\n%+v\n%+v", *a, *b)
	}
}

func TestDataDigestIsFrozen(t *testing.T) {
	t.Parallel()

	_, res, _ := baseline(t)
	if err := checkFrozenDigest(res); err != nil {
		t.Error(err)
	}
}

func TestCheckpointsLandOnEdgePrefixesOnly(t *testing.T) {
	t.Parallel()

	f, res, _ := baseline(t)
	if err := checkDefaultCheckpoints(res); err != nil {
		t.Error(err)
	}
	cfg := testConfig(t, f, "T/S0")
	cfg.Checkpoints = checkpointsOff
	off, _ := load(t, cfg)
	if err := checkNoCheckpoints(off); err != nil {
		t.Error(err)
	}
	if off.Keys != res.Keys-res.Checkpoints {
		t.Errorf("%d keys with the policy off and %d with it on, of which %d checkpoints: the checkpoints are the only difference", off.Keys, res.Keys, res.Checkpoints)
	}
}

// The blob guard is judged on the metrics the engine reports; a test sets them.
func TestBlobFilesFailTheMeasurementWithNoRow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	for name, set := range map[string]func(m *pebble.Metrics){
		"live blob files": func(m *pebble.Metrics) { m.BlobFiles.LiveCount = 1 },
		"live blob bytes": func(m *pebble.Metrics) { m.BlobFiles.LiveSize = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := testConfig(t, f, "T/S")
			cfg.judge = set
			if _, _, err := weigh(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "blob") {
				t.Fatalf("weigh = %v, want a refusal naming the blob files", err)
			}
			if left, _ := filepath.Glob(filepath.Join(cfg.Out, "*")); len(left) != 0 {
				t.Errorf("a failed measurement left %v", left)
			}
		})
	}
}

func TestEmptyStore(t *testing.T) {
	t.Parallel()

	cfg := config{Empty: true, Out: t.TempDir(), Label: "E", Checkpoints: checkpointsDefault, Batch: DefaultBatch}
	a, da := load(t, cfg)
	cfg.Label, cfg.Out = "E-rep", t.TempDir()
	b, _ := load(t, cfg)
	if a.Records != 0 || a.Keys != 0 || a.Checkpoints != 0 {
		t.Errorf("an empty store holds %d records, %d data keys and %d checkpoints", a.Records, a.Keys, a.Checkpoints)
	}
	if a.LiveTableBytes == 0 {
		t.Error("an empty store has no live table bytes: the fixed overhead was not measured")
	}
	if a.BytesPerRecord != nil || a.BytesPerSample != nil || a.Input != "" || a.InputDigest != "" || a.InputSHA256 != "" {
		t.Errorf("an empty store reports an input or a figure per record: %+v", *a)
	}
	if err := checkSettled(a, da); err != nil {
		t.Error(err)
	}
	if a.LiveTableBytes != b.LiveTableBytes || a.DataDigest != b.DataDigest {
		t.Errorf("two empty stores weigh %d and %d bytes", a.LiveTableBytes, b.LiveTableBytes)
	}
}

func TestEmptyIsRefusedBesideAnInput(t *testing.T) {
	f := newFixture(t)
	for name, change := range map[string]func(c *config){
		"-in":             func(c *config) { c.In = f.path },
		"-expect-records": func(c *config) { c.ExpectRecords = 5 },
		"-samples":        func(c *config) { c.Samples = 5 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config{Empty: true, Out: t.TempDir(), Label: "E", Checkpoints: checkpointsDefault, Batch: DefaultBatch}
			change(&cfg)
			if _, _, err := weigh(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "-empty") {
				t.Fatalf("weigh = %v, want a refusal naming -empty", err)
			}
		})
	}
}

func TestRefusesAnOutputDirectoryInAGitWorkTree(t *testing.T) {
	f := newFixture(t)
	tree := t.TempDir()
	if err := os.Mkdir(filepath.Join(tree, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, f, "T/S")
	cfg.Out = filepath.Join(tree, "results", "deeper")
	if _, _, err := weigh(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "git work tree") {
		t.Fatalf("weigh in a work tree = %v, want a refusal", err)
	}
	if _, err := os.Stat(filepath.Join(tree, "results")); err == nil {
		t.Error("the refusal came after the directory was made")
	}
}

func TestLabelAndFlagsAreChecked(t *testing.T) {
	f := newFixture(t)
	for name, change := range map[string]func(c *config){
		"no label":          func(c *config) { c.Label = "" },
		"a space":           func(c *config) { c.Label = "C60 S" },
		"a star":            func(c *config) { c.Label = "C60*" },
		"no output":         func(c *config) { c.Out = "" },
		"bad checkpoints":   func(c *config) { c.Checkpoints = "on" },
		"batch 0":           func(c *config) { c.Batch = 0 },
		"no input":          func(c *config) { c.In = "" },
		"negative expected": func(c *config) { c.ExpectRecords = -1 },
		"manifest":          func(c *config) { c.Label = "manifest" },
		"results":           func(c *config) { c.Label = "results" },
		"plan":              func(c *config) { c.Label = "plan" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t, f, "T/S")
			change(&cfg)
			if _, _, err := weigh(context.Background(), cfg); err == nil {
				t.Fatal("weigh accepted it")
			}
		})
	}
}

func TestAResultIsNeverOverwritten(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	out := t.TempDir()
	args := []string{"-in", f.path, "-out", out, "-label", "T/S"}
	var o, e bytes.Buffer
	if code := run(args, &o, &e); code != 0 {
		t.Fatalf("run = %d: %s", code, e.String())
	}
	if code := run(args, &o, &e); code != 1 || !strings.Contains(e.String(), "never overwritten") {
		t.Fatalf("a second run with the same label = %d: %s", code, e.String())
	}
}

func TestRunWritesTheRowAndTheJSON(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	args := []string{"-header", "-in", f.path, "-out", out, "-label", "T/S", "-expect-records", fmt.Sprint(len(f.records)), "-samples", fmt.Sprint(f.samples), "-keep"}
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("run = %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines on standard output, want a header and a row: %q", len(lines), stdout.String())
	}
	header, row := strings.Split(lines[0], "\t"), strings.Split(lines[1], "\t")
	if !slices.Equal(header, tsvColumns) || len(row) != len(header) || row[0] != "T/S" {
		t.Errorf("header %v, row %v", header, row)
	}

	b, err := os.ReadFile(filepath.Join(out, "T_S.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"label", "input", "input_digest", "input_sha256", "records", "keys", "checkpoints", "checkpoints_entity",
		"checkpoints_edge", "live_table_bytes", "blob_live_bytes", "bytes_per_record", "bytes_per_sample",
		"tables_per_level", "data_digest", "checkpoint_setting", "batch", "go_version", "vcs_revision", "vcs_modified",
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		t.Errorf("the JSON holds the keys %v, want %v", keys, want)
	}
	if got["batch"] != float64(37) && got["batch"] != float64(DefaultBatch) {
		t.Errorf("batch %v", got["batch"])
	}
	if _, err := os.Stat(filepath.Join(out, "T_S.store")); err != nil {
		t.Errorf("-keep did not keep the store: %v", err)
	}
	for _, name := range []string{"manifest.json", "results.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err == nil {
			t.Errorf("%s was written: check:no-results forbids that name", name)
		}
	}
}

func TestTheStoreIsRemovedUnlessKept(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	cfg := testConfig(t, f, "T/S")
	load(t, cfg)
	if _, err := os.Stat(filepath.Join(cfg.Out, "T_S.store")); err == nil {
		t.Error("the store was kept without -keep")
	}
}

func TestUsage(t *testing.T) {
	f := newFixture(t)
	var o, e bytes.Buffer
	for _, args := range [][]string{
		{},
		{"-in", f.path, "-label", "x"},
		{"-empty", "-in", f.path, "-out", t.TempDir(), "-label", "x"},
		{"-in", f.path, "-out", t.TempDir(), "-label", "x", "extra"},
		{"-bogus"},
	} {
		if code := run(args, &o, &e); code != 2 {
			t.Errorf("run(%v) = %d, want 2", args, code)
		}
	}
	if o.Len() != 0 {
		t.Errorf("usage errors printed a row: %q", o.String())
	}
}

func TestArchiveSQLUsesVariablesDuckDBAccepts(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "archive.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	for _, want := range []string{"getvariable('src')", "getvariable('dst')", "getvariable('tmp')"} {
		if !strings.Contains(sql, want) {
			t.Errorf("archive.sql does not read %s", want)
		}
	}
	for _, bad := range []string{"getvariable('in')", "getvariable('out')", "TO getvariable"} {
		if strings.Contains(sql, bad) {
			t.Errorf("archive.sql has %s, which DuckDB refuses (in is reserved; COPY TO takes a parenthesised expression)", bad)
		}
	}
}

// TestTheDefaultCompressionLeavesTheOutputAsItWas shows that naming the default, or
// naming nothing, writes the same row and the same result file, with the columns and
// the keys they had before -compression: the flag adds nothing to a measurement that
// does not use it.
func TestTheDefaultCompressionLeavesTheOutputAsItWas(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	n := fmt.Sprint(len(f.records))
	outs := make([]string, 2)
	rows := make([]string, 2)
	for i, extra := range [][]string{nil, {"-compression", "default"}} {
		out := t.TempDir()
		args := append([]string{"-header", "-in", f.path, "-out", out, "-label", "T/D", "-batch", "37", "-expect-records", n, "-samples", fmt.Sprint(f.samples)}, extra...)
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 0 {
			t.Fatalf("run(%v) = %d: %s", extra, code, stderr.String())
		}
		outs[i], rows[i] = out, stdout.String()
	}
	if rows[0] != rows[1] {
		t.Errorf("the row differs:\n%s\n%s", rows[0], rows[1])
	}
	header := strings.Split(strings.SplitN(rows[0], "\n", 2)[0], "\t")
	if !slices.Equal(header, []string{
		"label", "records", "keys", "checkpoints", "checkpoints_entity", "checkpoints_edge",
		"live_table_bytes", "blob_live_bytes", "bytes_per_record", "bytes_per_sample",
		"tables_per_level", "data_digest", "checkpoint_setting", "input_digest",
	}) {
		t.Errorf("the header is %v", header)
	}
	a, err := os.ReadFile(filepath.Join(outs[0], "T_D.json"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(outs[1], "T_D.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The input is the same file in both, so the two results are the same bytes.
	if !bytes.Equal(a, b) {
		t.Errorf("the result files differ:\n%s\n%s", a, b)
	}
	if bytes.Contains(a, []byte("compression")) {
		t.Errorf("the result of a default measurement names a compression:\n%s", a)
	}
	_, base, _ := baseline(t)
	if base.DataDigest != frozenDataDigest || base.Compression != "" {
		t.Errorf("the baseline holds data %s, compression %q", base.DataDigest, base.Compression)
	}
	if !strings.Contains(string(a), fmt.Sprintf(`"live_table_bytes": %d`, base.LiveTableBytes)) {
		t.Errorf("the default flag weighs other table bytes than the baseline's %d:\n%s", base.LiveTableBytes, a)
	}
}

// TestCompressionChangesTheTableBytesAndNothingElse shows that a store written under
// another block compression holds the same keys and values as the default, is named in
// the result, and, but for explicit Snappy (which is the default written out), weighs
// other table bytes.
func TestCompressionChangesTheTableBytesAndNothingElse(t *testing.T) {
	t.Parallel()

	f, base, _ := baseline(t)
	for _, name := range []string{"snappy", "zstd", "balanced"} {
		cfg := testConfig(t, f, "T/"+name)
		cfg.Compression = name
		res, diag := load(t, cfg)
		if err := checkSettled(res, diag); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if res.DataDigest != frozenDataDigest || res.Keys != base.Keys || res.Records != base.Records || res.Checkpoints != base.Checkpoints {
			t.Errorf("%s: the store holds other data than the default's", name)
		}
		if res.Compression != name {
			t.Errorf("%s: the result says %q", name, res.Compression)
		}
		if same := res.LiveTableBytes == base.LiveTableBytes; same != (name == "snappy") {
			t.Errorf("%s: %d table bytes, the default's %d", name, res.LiveTableBytes, base.LiveTableBytes)
		}
	}
}

func TestACompressionThatIsNotOneIsRefused(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"", "default", "snappy", "zstd", "balanced", "lz4", "zstd1", "ZSTD", "Snappy"} {
		cfg := config{Empty: true, Out: t.TempDir(), Label: "E", Checkpoints: checkpointsDefault, Batch: DefaultBatch, Compression: name}
		ok := slices.Contains([]string{"", "default", "snappy", "zstd", "balanced"}, name)
		if err := cfg.validate(); ok != (err == nil) {
			t.Errorf("-compression %q: %v", name, err)
		}
	}
	var o, e bytes.Buffer
	if code := run([]string{"-empty", "-out", t.TempDir(), "-label", "E", "-compression", "lz4"}, &o, &e); code != 2 || o.Len() != 0 {
		t.Errorf("run with -compression lz4 = %d, printed %q", code, o.String())
	}
}
