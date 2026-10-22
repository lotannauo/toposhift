package pgreport_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
)

func env() pgreport.Env {
	return pgreport.Env{
		Commit: "c0ffee", Modified: "false", GoVersion: "go1.27.3", GOOS: "linux", GOARCH: "arm64", BinarySHA256: "b1",
		SettingsSHA256: "s1", Image: "postgres:17.11-trixie@sha256:2d2b", ServerVersion: "PostgreSQL 17.11",
		Configure: "'--with-lz4'", BlockSize: 8192, WALBlockSize: 8192, DataChecksums: "off", ServerCPU: "cpu a",
	}
}

func input() pgreport.Input {
	return pgreport.Input{
		Kind: "plan", PlanSHA256: "p1", StreamDigest: "d1", QueriesDigest: "q1", Records: 1000, LastSeq: 5000,
	}
}

func sizes() pgreport.Sizes {
	return pgreport.NewSizes([]pgreport.Relation{
		{Name: "edges", Kind: "table", Main: 8192 * 10, FSM: 24576, VM: 8192},
		{Name: "edges_src_g", Kind: "index", Table: "edges", Main: 8192 * 12},
		{Name: "log", Kind: "table", Main: 8192 * 100, FSM: 24576, VM: 8192},
		{Name: "log_g_src", Kind: "index", Table: "log", Main: 8192 * 20},
		{Name: "log_base", Kind: "table", Main: 8192 * 4},
		{Name: "meta", Kind: "table", Main: 8192},
		{Name: "meta_pkey", Kind: "index", Table: "meta", Main: 8192},
		{Name: "pg_toast_1", Kind: "toast", Table: "log"},
	})
}

func load() pgreport.Load {
	return pgreport.Load{
		Version: pgreport.Version, Env: env(), Arm: "pg-bulk", Schema: "s_a", Input: input(),
		Retentions: 3, HorizonNS: 86400e9, StreamVacuums: 2,
		Rows:         pgreport.Rows{Log: 900, LogBase: 40, Edges: 300, Entities: 200},
		TablesDigest: "t1", TablesRows: 500,
		Phases: []pgreport.Phase{
			{Name: "stream", Contiguous: true, LogBytes: 19_000_000, Records: 54_000, FPIs: 5, RecordBytes: 18_800_000},
			{Name: "copy", Records: 3_000, FPIs: 1, RecordBytes: 14_500_000},
			{Name: "indexes", Contiguous: true, LogBytes: 1_180_000, Records: 280, FPIs: 510, RecordBytes: 1_176_000},
		},
		Sizes: sizes(),
	}
}

func read() pgreport.Read {
	run := func(shared int64) []pgreport.StatementRun {
		return []pgreport.StatementRun{{
			Statement: 0, Shape: "Index Scan on edges using edges_src_g",
			Counters: pgreport.Counters{SharedHit: 3, SharedRead: shared, RowsReturned: 2, IndexEntries: 4, HeapFetches: 0, RowsRemovedByFilter: 1},
		}}
	}
	res := func(name string, digest string, shared int64) pgreport.Result {
		runs := run(shared)
		return pgreport.Result{
			Name: name, Group: "g", Age: "now", Op: "neighbors", Digest: digest, Size: 2, Runs: runs, Total: runs[0].Counters,
			BytesRead: shared * 8192, BytesTouched: (shared + 3) * 8192,
		}
	}
	return pgreport.Read{
		Version: pgreport.Version, Env: env(), Arm: "pg-bulk", Schema: "s_a", Input: input(), Queries: 2, Matched: 2, Repeats: 2,
		Statements: []string{"SELECT dst, rel FROM edges WHERE src = $1"},
		Results:    []pgreport.Result{res("q one", "aa", 5), res("q two", "bb", 7)},
	}
}

func TestIdenticalRecordsAreEqualWhateverSchemaTheyWereMadeIn(t *testing.T) {
	a, b := load(), load()
	b.Schema = "s_b"
	r, err := pgreport.DiffLoads(a, b, pgreport.Options{})
	if err != nil || !r.Equal() || len(r.Skipped) != 0 || len(r.EnvDiffers) != 0 {
		t.Fatalf("loads: %+v, %v", r, err)
	}
	ra, rb := read(), read()
	rb.Schema = "s_b"
	rr, err := pgreport.DiffReads(ra, rb)
	if err != nil || !rr.Equal() {
		t.Fatalf("reads: %+v, %v", rr, err)
	}
}

// Each field of a read that two runs must agree on is compared: a comparison that left
// one out would let two loads that differ be called equal.
func TestEveryCountOfAReadIsComparedAndNamedByItsCategory(t *testing.T) {
	for _, c := range []struct {
		name string
		cat  string
		edit func(r *pgreport.Read)
	}{
		{"digest", pgreport.CatAnswer, func(r *pgreport.Read) { r.Results[1].Digest = "cc" }},
		{"size", pgreport.CatAnswer, func(r *pgreport.Read) { r.Results[0].Size = 3 }},
		{"naive", pgreport.CatAnswer, func(r *pgreport.Read) { r.Results[0].Naive = true }},
		{"rows fetched", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].RowsFetched = 9 }},
		{"shared hit", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.SharedHit++ }},
		{"shared read", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.SharedRead++ }},
		{"rows returned", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.RowsReturned++ }},
		{"index entries", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.IndexEntries++ }},
		{"heap fetches", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.HeapFetches++ }},
		{"removed by filter", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.RowsRemovedByFilter++ }},
		{"removed by recheck", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[0].Runs[0].Counters.RowsRemovedByRecheck++ }},
		{"total", pgreport.CatCounter, func(r *pgreport.Read) { r.Results[1].Total.SharedHit++ }},
		{"plan shape", pgreport.CatShape, func(r *pgreport.Read) { r.Results[0].Runs[0].Shape = "Seq Scan on edges" }},
		{"statement text", pgreport.CatShape, func(r *pgreport.Read) { r.Statements[0] += " ORDER BY 1" }},
		{"statement count", pgreport.CatShape, func(r *pgreport.Read) { r.Results[1].Runs = nil }},
		{"bytes read", pgreport.CatBytes, func(r *pgreport.Read) { r.Results[0].BytesRead += 8192 }},
		{"bytes touched", pgreport.CatBytes, func(r *pgreport.Read) { r.Results[0].BytesTouched += 8192 }},
	} {
		b := read()
		c.edit(&b)
		r, err := pgreport.DiffReads(read(), b)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		found := false
		for _, d := range r.Differences {
			found = found || d.Category == c.cat
		}
		if !found {
			t.Errorf("%s: no difference of category %s in %+v", c.name, c.cat, r.Differences)
		}
	}
}

func TestReadsOfOtherQuestionsOrWithWrongAnswersAreNotCompared(t *testing.T) {
	b := read()
	b.Results = b.Results[:1]
	if _, err := pgreport.DiffReads(read(), b); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("another number of results: %v", err)
	}
	b = read()
	b.Results[0].Name = "another question"
	if _, err := pgreport.DiffReads(read(), b); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("another question: %v", err)
	}
	b = read()
	b.Mismatches = []pgreport.Mismatch{{Query: "q one", WantDigest: "aa", GotDigest: "zz"}}
	if _, err := pgreport.DiffReads(read(), b); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("a record with a wrong answer: %v", err)
	}
}

func TestRecordsOfAnotherStreamServerOrSettingsAreNotComparable(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(l *pgreport.Load)
	}{
		{"settings", func(l *pgreport.Load) { l.Env.SettingsSHA256 = "other" }},
		{"image", func(l *pgreport.Load) { l.Env.Image = "postgres:17.12@sha256:ffff" }},
		{"server version", func(l *pgreport.Load) { l.Env.ServerVersion = "PostgreSQL 17.12" }},
		{"build options", func(l *pgreport.Load) { l.Env.Configure = "'--without-lz4'" }},
		{"block size", func(l *pgreport.Load) { l.Env.BlockSize = 16384 }},
		{"wal block size", func(l *pgreport.Load) { l.Env.WALBlockSize = 16384 }},
		{"checksums", func(l *pgreport.Load) { l.Env.DataChecksums = "on" }},
		{"plan", func(l *pgreport.Load) { l.Input.PlanSHA256 = "p2" }},
		{"stream", func(l *pgreport.Load) { l.Input.StreamDigest = "d2" }},
		{"queries", func(l *pgreport.Load) { l.Input.QueriesDigest = "q2" }},
		{"activity", func(l *pgreport.Load) { l.Input.ContentDigest = "sha256:x" }},
		{"records", func(l *pgreport.Load) { l.Input.Records++ }},
		{"arm", func(l *pgreport.Load) { l.Arm = "pg-bulk-b" }},
	} {
		b := load()
		c.edit(&b)
		if _, err := pgreport.DiffLoads(load(), b, pgreport.Options{}); !errors.Is(err, pgreport.ErrNotComparable) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	// What made a record, and on what processor, does not make two records of one
	// stream incomparable: it is what a run on a laptop and one on CI differ by.
	b := load()
	b.Env.Commit, b.Env.BinarySHA256, b.Env.ServerCPU = "other", "other", "cpu b"
	r, err := pgreport.DiffLoads(load(), b, pgreport.Options{})
	if err != nil || !r.Equal() || strings.Join(r.EnvDiffers, ",") != "commit,binary,server cpu" {
		t.Errorf("environment only: %+v, %v", r, err)
	}
}

func TestEveryCountOfALoadIsCompared(t *testing.T) {
	for _, c := range []struct {
		name string
		cat  string
		edit func(l *pgreport.Load)
	}{
		{"log rows", pgreport.CatRows, func(l *pgreport.Load) { l.Rows.Log++ }},
		{"baseline rows", pgreport.CatRows, func(l *pgreport.Load) { l.Rows.LogBase++ }},
		{"edge rows", pgreport.CatRows, func(l *pgreport.Load) { l.Rows.Edges++ }},
		{"entity rows", pgreport.CatRows, func(l *pgreport.Load) { l.Rows.Entities++ }},
		{"dictionary rows", pgreport.CatRows, func(l *pgreport.Load) { l.Rows.Dictionary++ }},
		{"retentions", pgreport.CatRows, func(l *pgreport.Load) { l.Retentions++ }},
		{"horizon", pgreport.CatRows, func(l *pgreport.Load) { l.HorizonNS++ }},
		{"stream vacuums", pgreport.CatRows, func(l *pgreport.Load) { l.StreamVacuums++ }},
		{"tables digest", pgreport.CatDigest, func(l *pgreport.Load) { l.TablesDigest = "t2" }},
		{"tables rows", pgreport.CatRows, func(l *pgreport.Load) { l.TablesRows++ }},
		{"main fork", pgreport.CatBytes, func(l *pgreport.Load) { l.Sizes.Relations[1].Main += 8192 }},
		{"free space map", pgreport.CatBytes, func(l *pgreport.Load) { l.Sizes.Relations[0].FSM += 8192 }},
		{"visibility map", pgreport.CatBytes, func(l *pgreport.Load) { l.Sizes.Relations[2].VM += 8192 }},
		{"a relation missing", pgreport.CatBytes, func(l *pgreport.Load) { l.Sizes.Relations = l.Sizes.Relations[1:] }},
		{"log records", pgreport.CatWAL, func(l *pgreport.Load) { l.Phases[0].Records++ }},
		{"full-page images", pgreport.CatWAL, func(l *pgreport.Load) { l.Phases[2].FPIs++ }},
		{"record bytes", pgreport.CatWAL, func(l *pgreport.Load) { l.Phases[1].RecordBytes++ }},
		{"a phase missing", pgreport.CatWAL, func(l *pgreport.Load) { l.Phases = l.Phases[:2] }},
	} {
		b := load()
		c.edit(&b)
		r, err := pgreport.DiffLoads(load(), b, pgreport.Options{})
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		found := false
		for _, d := range r.Differences {
			found = found || d.Category == c.cat
		}
		if !found {
			t.Errorf("%s: no difference of category %s in %+v", c.name, c.cat, r.Differences)
		}
	}
}

func TestTheLogIsComparedWithItsToleranceAndItsSlack(t *testing.T) {
	// A page header more in the log position is not a difference; a hundred bytes more
	// is, unless the tolerance allows it.
	b := load()
	b.Phases[0].LogBytes += 40
	if r, err := pgreport.DiffLoads(load(), b, pgreport.Options{}); err != nil || !r.Equal() {
		t.Errorf("a page header: %+v, %v", r, err)
	}
	b.Phases[0].LogBytes += 100
	if r, _ := pgreport.DiffLoads(load(), b, pgreport.Options{}); r.Equal() {
		t.Error("140 bytes of log position more were not a difference")
	}
	// 0.01 % of 19 MB is 1,900 bytes.
	b = load()
	b.Phases[0].RecordBytes += 1_500
	if r, _ := pgreport.DiffLoads(load(), b, pgreport.Options{}); r.Equal() {
		t.Error("1,500 record bytes more were not a difference with no tolerance")
	}
	if r, err := pgreport.DiffLoads(load(), b, pgreport.Options{WALTolerance: 0.0001}); err != nil || !r.Equal() {
		t.Errorf("within the tolerance: %+v, %v", r, err)
	}
	b.Phases[0].RecordBytes += 3_000
	if r, _ := pgreport.DiffLoads(load(), b, pgreport.Options{WALTolerance: 0.0001}); r.Equal() {
		t.Error("4,500 record bytes more were within a tolerance of 0.01 %")
	}
	// The tolerance is for the log only: nothing else is let through by it.
	b = load()
	b.Rows.Edges++
	b.TablesDigest = "t2"
	if r, _ := pgreport.DiffLoads(load(), b, pgreport.Options{WALTolerance: 1}); r.Equal() {
		t.Error("a tolerance for the log let a different table through")
	}
}

func TestAPhaseWithACheckpointInsideItIsNotComparedAndSaysSo(t *testing.T) {
	a, b := load(), load()
	b.Phases[2].CheckpointsInside = 1
	b.Phases[2].FPIs += 4000
	b.Phases[2].RecordBytes += 32_000_000
	r, err := pgreport.DiffLoads(a, b, pgreport.Options{})
	if err != nil || !r.Equal() {
		t.Fatalf("%+v, %v", r, err)
	}
	if len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0], "phase indexes") {
		t.Errorf("skipped %v", r.Skipped)
	}
	// The phases without one are still compared.
	b.Phases[0].Records++
	if r, _ := pgreport.DiffLoads(a, b, pgreport.Options{}); r.Equal() {
		t.Error("a checkpoint in one phase excused another")
	}
}

func TestAnExtraVacuumOnlyExcusesTheVacuumPhase(t *testing.T) {
	l := load()
	l.Phases = append(l.Phases, pgreport.Phase{Name: "vacuum", Contiguous: true, LogBytes: 6_900_000, Records: 3_800, FPIs: 1_800, RecordBytes: 6_800_000})
	b := l
	b.Phases = append([]pgreport.Phase(nil), l.Phases...)
	b.Phases[3].RecordBytes += 900_000
	b.Phases[3].LogBytes += 900_000
	if r, _ := pgreport.DiffLoads(l, b, pgreport.Options{}); r.Equal() {
		t.Fatal("a vacuum that wrote more was not a difference")
	}
	b.ExtraVacuums = 1
	r, err := pgreport.DiffLoads(l, b, pgreport.Options{})
	if err != nil || !r.Equal() || len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0], "more vacuums") {
		t.Errorf("a table that needed another vacuum: %+v, %v", r, err)
	}
	b.Phases[0].Records++
	if r, _ := pgreport.DiffLoads(l, b, pgreport.Options{}); r.Equal() {
		t.Error("an extra vacuum excused the stream")
	}
}

func TestSizesAreGroupedByWhatTheyServe(t *testing.T) {
	s := sizes()
	want := map[string]int64{
		pgreport.GroupIntervals: 8192*10 + 24576 + 8192 + 8192*12,
		pgreport.GroupLog:       8192*100 + 24576 + 8192 + 8192*20 + 8192*4,
		pgreport.GroupOther:     8192 * 2,
	}
	for g, n := range want {
		if s.ByGroup[g] != n {
			t.Errorf("group %s: %d, want %d", g, s.ByGroup[g], n)
		}
	}
	var total int64
	for _, n := range s.ByGroup {
		total += n
	}
	if total != s.Total {
		t.Errorf("groups add up to %d, total %d", total, s.Total)
	}
	// The id arm's dictionary, and a partition of the log, are named by their tables.
	for name, g := range map[string]string{"entity": pgreport.GroupDictionary, "log_p20260102": pgreport.GroupLog, "entities": pgreport.GroupIntervals, "meta": pgreport.GroupOther} {
		if got := (pgreport.Relation{Name: name, Kind: "table"}).Group(); got != g {
			t.Errorf("table %s: group %s, want %s", name, got, g)
		}
	}
	if got := (pgreport.Relation{Name: "entity_fp_key", Kind: "index", Table: "entity"}).Group(); got != pgreport.GroupDictionary {
		t.Errorf("an index of the dictionary: %s", got)
	}
}

func TestSizesReportsCompareAsLeftAndCompacted(t *testing.T) {
	mk := func() pgreport.SizesReport {
		c := sizes()
		return pgreport.SizesReport{Version: pgreport.Version, Env: env(), Arm: "pg-bulk", Records: 1000, AsLeft: sizes(), Compacted: &c}
	}
	if r, err := pgreport.DiffSizes(mk(), mk()); err != nil || !r.Equal() {
		t.Fatalf("%+v, %v", r, err)
	}
	b := mk()
	b.Compacted.Relations[0].Main += 8192
	if r, _ := pgreport.DiffSizes(mk(), b); r.Equal() || !strings.HasPrefix(r.Differences[0].Where, "compacted: ") {
		t.Errorf("compacted sizes: %+v", r)
	}
	b = mk()
	b.AsLeft.Relations[0].Main += 8192
	if r, _ := pgreport.DiffSizes(mk(), b); r.Equal() || !strings.HasPrefix(r.Differences[0].Where, "as left: ") {
		t.Errorf("as left sizes: %+v", r)
	}
	b = mk()
	b.Compacted = nil
	if r, _ := pgreport.DiffSizes(mk(), b); r.Equal() {
		t.Error("a record with no compacted sizes equals one that has them")
	}
	b = mk()
	b.Records++
	if _, err := pgreport.DiffSizes(mk(), b); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("another number of records: %v", err)
	}
}

func TestRecordsRoundTripAndAnUnknownFieldOrVersionIsRefused(t *testing.T) {
	dir := t.TempDir()
	if err := pgreport.Write(filepath.Join(dir, "sub"), "load.json", load()); err != nil {
		t.Fatal(err)
	}
	var got pgreport.Load
	path := filepath.Join(dir, "sub", "load.json")
	if err := pgreport.ReadFile(path, &got); err != nil {
		t.Fatal(err)
	}
	if r, err := pgreport.DiffLoads(load(), got, pgreport.Options{}); err != nil || !r.Equal() || got.Schema != "s_a" {
		t.Errorf("after a round trip: %+v, %v", r, err)
	}
	// The same record is the same bytes.
	if err := pgreport.Write(dir, "again.json", load()); err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(path)
	b, _ := os.ReadFile(filepath.Join(dir, "again.json"))
	if string(a) != string(b) {
		t.Error("one record made two files")
	}

	bad := filepath.Join(dir, "bad.json")
	for name, text := range map[string]string{
		"an unknown field": strings.Replace(string(a), `"Arm"`, `"Timing": 1, "Arm"`, 1),
		"another version":  strings.Replace(string(a), `"Version": 1`, `"Version": 2`, 1),
		"not json":         "Seq Scan",
	} {
		if err := os.WriteFile(bad, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		var l pgreport.Load
		if err := pgreport.ReadFile(bad, &l); !errors.Is(err, pgreport.ErrFormat) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := pgreport.ReadFile(path, &struct{}{}); err == nil {
		t.Error("a type that is not a record was read")
	}
	if err := pgreport.ReadFile(filepath.Join(dir, "missing.json"), &got); err == nil {
		t.Error("a missing file was read")
	}
}

func TestACountersSumAddsEveryField(t *testing.T) {
	a := pgreport.Counters{SharedHit: 1, SharedRead: 2, RowsReturned: 3, IndexEntries: 4, HeapFetches: 5, RowsRemovedByFilter: 6, RowsRemovedByRecheck: 7}
	want := pgreport.Counters{SharedHit: 2, SharedRead: 4, RowsReturned: 6, IndexEntries: 8, HeapFetches: 10, RowsRemovedByFilter: 12, RowsRemovedByRecheck: 14}
	if got := a.Add(a); got != want {
		t.Errorf("%+v, want %+v", got, want)
	}
}

func stream() pgreport.Stream {
	l := load()
	return pgreport.Stream{
		Version: pgreport.Version, Env: l.Env, Arm: "pg-stream", Schema: "s_a", Input: l.Input,
		Retentions: l.Retentions, HorizonNS: l.HorizonNS, Queries: 40, Matched: 40,
		Writer: pgreport.WriterCounts{
			Records: 1000, FastSubjects: 700, Refolds: 120, RecordsFolded: 480, RowsInserted: 300, RowsUpdated: 650, RowsHeapOnly: 500,
			RowsFinalized: 80, RowsDeleted: 100, Vacuums: 5, Analyzes: 7, PartitionsDropped: 1,
			VacuumRuns: map[string]int{"edges": 3, "log_p19700101": 2}, AnalyzeRuns: map[string]int{"edges": 4, "log_p19700101": 3},
		},
		Rows: l.Rows, TablesDigest: l.TablesDigest, TablesRows: l.TablesRows, Phases: l.Phases, Sizes: l.Sizes,
	}
}

func TestTwoStreamsOfOneStreamAreEqualAndTheirWritersCountsAreCompared(t *testing.T) {
	a, b := stream(), stream()
	b.Schema = "s_b"
	r, err := pgreport.DiffStreams(a, b, pgreport.Options{})
	if err != nil || !r.Equal() {
		t.Fatalf("identical streams: %v %+v", err, r.Differences)
	}
	for name, change := range map[string]func(s *pgreport.Stream){
		"fast subjects":    func(s *pgreport.Stream) { s.Writer.FastSubjects++ },
		"refolds":          func(s *pgreport.Stream) { s.Writer.Refolds++ },
		"records folded":   func(s *pgreport.Stream) { s.Writer.RecordsFolded++ },
		"inserted":         func(s *pgreport.Stream) { s.Writer.RowsInserted++ },
		"updated":          func(s *pgreport.Stream) { s.Writer.RowsUpdated++ },
		"heap-only":        func(s *pgreport.Stream) { s.Writer.RowsHeapOnly++ },
		"finalized":        func(s *pgreport.Stream) { s.Writer.RowsFinalized++ },
		"deleted":          func(s *pgreport.Stream) { s.Writer.RowsDeleted++ },
		"vacuums":          func(s *pgreport.Stream) { s.Writer.Vacuums++ },
		"partitions":       func(s *pgreport.Stream) { s.Writer.PartitionsDropped++ },
		"vacuums of edge":  func(s *pgreport.Stream) { s.Writer.VacuumRuns["edges"]++ },
		"analyzes":         func(s *pgreport.Stream) { s.Writer.Analyzes++ },
		"analyzes of edge": func(s *pgreport.Stream) { s.Writer.AnalyzeRuns["edges"]++ },
		"a table analyzed": func(s *pgreport.Stream) {
			s.Writer.AnalyzeRuns = map[string]int{"edges": 4, "log_p19700101": 3, "entities": 1}
		},
		"a table vacuumed": func(s *pgreport.Stream) {
			s.Writer.VacuumRuns = map[string]int{"edges": 3, "log_p19700101": 2, "entities": 1}
		},
		"answers matched": func(s *pgreport.Stream) { s.Matched-- },
		"autovacuum":      func(s *pgreport.Stream) { s.Autovacuum = true },
		"tables digest":   func(s *pgreport.Stream) { s.TablesDigest = "other" },
	} {
		b := stream()
		change(&b)
		if r, err := pgreport.DiffStreams(a, b, pgreport.Options{}); err != nil || r.Equal() {
			t.Errorf("a difference in %s was not found (%v)", name, err)
		}
	}
	// A stream of another arm, or on another server, is not comparable.
	other := stream()
	other.Arm = "pg-stream-naive"
	if _, err := pgreport.DiffStreams(a, other, pgreport.Options{}); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("two arms: %v", err)
	}
	other = stream()
	other.Env.SettingsSHA256 = "s2"
	if _, err := pgreport.DiffStreams(a, other, pgreport.Options{}); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("two settings: %v", err)
	}
}

func TestAStreamRecordRoundTripsAndRefusesUnknownFields(t *testing.T) {
	dir := t.TempDir()
	s := stream()
	if err := pgreport.Write(dir, "stream.json", &s); err != nil {
		t.Fatal(err)
	}
	var back pgreport.Stream
	if err := pgreport.ReadFile(filepath.Join(dir, "stream.json"), &back); err != nil {
		t.Fatal(err)
	}
	if r, err := pgreport.DiffStreams(s, back, pgreport.Options{}); err != nil || !r.Equal() {
		t.Fatalf("a record read back differs: %v %+v", err, r.Differences)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "stream.json"))
	bad := strings.Replace(string(raw), `"Autovacuum"`, `"Elapsed": 3, "Autovacuum"`, 1)
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := pgreport.ReadFile(filepath.Join(dir, "bad.json"), &back); !errors.Is(err, pgreport.ErrFormat) {
		t.Errorf("a record with a field this build does not know: %v", err)
	}
}
