package doltspike

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/g0"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func testEnv() Env {
	return Env{Commit: "c1", Modified: "false", GoVersion: "go1", GOOS: "linux", GOARCH: "arm64", BinarySHA256: "b1", Image: "i", DoltVersion: "2.4.2", DriverVersion: "v1.10.1", SchemaVersion: 1, PayloadCapacity: 256}
}

func testInput() Input {
	return Input{Kind: "plan", PlanSHA256: "p", StreamDigest: "s", QueriesDigest: "q", Records: 10, LastSeq: 10}
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func testLoad() LoadRecord {
	return LoadRecord{
		Kind: KindLoad, Format: RecordFormat, Env: testEnv(), Input: testInput(), Arm: "commit", Database: "d1",
		Counts: doltmodel.Counts{Records: 10, LogRows: 18, StateWrites: 9, Commits: 3}, Commits: 3, HeadHash: "h", HeadOrdinal: 3,
		Frontiers: []int64{1, 2, 3}, FrontiersSHA256: frontiersDigest([]int64{1, 2, 3}),
	}
}

func categories(ds []Difference) map[string]int {
	out := map[string]int{}
	for _, d := range ds {
		out[d.Category]++
	}
	return out
}

func TestTwoLoadsOfOneStreamAreEqualOrDifferInAResult(t *testing.T) {
	a, b := testLoad(), testLoad()
	b.Database = "d2" // the name of the database is not a result
	ds, err := Diff(marshal(t, a), marshal(t, b))
	if err != nil || len(ds) != 0 {
		t.Fatalf("%v, %v", ds, err)
	}
	for name, mutate := range map[string]func(*LoadRecord){
		"the head":       func(r *LoadRecord) { r.HeadHash = "h2" },
		"the counts":     func(r *LoadRecord) { r.Counts.StateWrites++ },
		"the commits":    func(r *LoadRecord) { r.Commits++ },
		"the frontiers":  func(r *LoadRecord) { r.FrontiersSHA256 = "x" },
		"the retentions": func(r *LoadRecord) { r.Retentions = 1 },
	} {
		c := testLoad()
		mutate(&c)
		ds, err := Diff(marshal(t, a), marshal(t, c))
		if err != nil || categories(ds)["result"] != 1 {
			t.Errorf("%s: %v, %v", name, ds, err)
		}
	}
	// The program that made a record is named and is not a result.
	c := testLoad()
	c.Env.Commit, c.Env.BinarySHA256 = "c2", "b2"
	ds, err = Diff(marshal(t, a), marshal(t, c))
	if err != nil || categories(ds)["binary"] != 1 || categories(ds)["result"] != 0 {
		t.Fatalf("%v, %v", ds, err)
	}
}

func TestRecordsOfAnotherStreamArmServerOrKindAreNotCompared(t *testing.T) {
	a := testLoad()
	for name, mutate := range map[string]func(*LoadRecord){
		"the stream":      func(r *LoadRecord) { r.Input.StreamDigest = "other" },
		"the questions":   func(r *LoadRecord) { r.Input.QueriesDigest = "other" },
		"the arm":         func(r *LoadRecord) { r.Arm = "rows" },
		"the image":       func(r *LoadRecord) { r.Env.Image = "other" },
		"the version":     func(r *LoadRecord) { r.Env.DoltVersion = "2.5.0" },
		"the driver":      func(r *LoadRecord) { r.Env.DriverVersion = "v2" },
		"the settings":    func(r *LoadRecord) { r.Env.ServerYAMLSHA256 = "other" },
		"the capacity":    func(r *LoadRecord) { r.Env.PayloadCapacity = 512 },
		"the schema":      func(r *LoadRecord) { r.Env.SchemaVersion = 2 },
		"the driver mode": func(r *LoadRecord) { r.Env.InterpolateParams = true },
	} {
		c := testLoad()
		mutate(&c)
		if _, err := Diff(marshal(t, a), marshal(t, c)); !errors.Is(err, ErrNotComparable) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A label is not part of what the stream is.
	c := testLoad()
	c.Input.Label = "run two"
	if ds, err := Diff(marshal(t, a), marshal(t, c)); err != nil || len(ds) != 0 {
		t.Errorf("a label: %v, %v", ds, err)
	}
	read := ReadRecord{Kind: KindRead, Format: RecordFormat}
	if _, err := Diff(marshal(t, a), marshal(t, read)); !errors.Is(err, ErrNotComparable) {
		t.Errorf("a load and a read: %v", err)
	}
	old := testLoad()
	old.Format = 0
	if _, err := Diff(marshal(t, old), marshal(t, old)); !errors.Is(err, ErrNotComparable) {
		t.Errorf("records of another format: %v", err)
	}
	if _, err := Diff([]byte("{"), []byte("{}")); err == nil {
		t.Error("a record that is not JSON")
	}
	if _, err := Diff(marshal(t, map[string]any{"Kind": "walk", "Format": RecordFormat}), marshal(t, map[string]any{"Kind": "walk", "Format": RecordFormat})); err == nil {
		t.Error("a kind of record that does not exist")
	}
}

func TestTwoReadsAreEqualOrDifferInAQuestion(t *testing.T) {
	mk := func() ReadRecord {
		return ReadRecord{
			Kind: KindRead, Format: RecordFormat, Env: testEnv(), Input: testInput(), Arm: "commit", HeadHash: "h", Queries: 2, Matched: 2,
			Results: []QueryResult{
				{Group: "g", Rank: 0, Age: "now", Op: "neighbors", Match: true, Outcome: "as-of-alone", KeysState: 3},
				{Group: "g", Rank: 1, Age: "1h", Op: "neighbors", Match: true, Outcome: "needed-log", KeysState: 1, KeysLog: 2},
			},
		}
	}
	a := mk()
	if ds, err := Diff(marshal(t, a), marshal(t, mk())); err != nil || len(ds) != 0 {
		t.Fatalf("%v, %v", ds, err)
	}
	for name, mutate := range map[string]func(*ReadRecord){
		"an outcome":  func(r *ReadRecord) { r.Results[1].Outcome = "as-of-alone" },
		"a key count": func(r *ReadRecord) { r.Results[0].KeysState++ },
		"a match":     func(r *ReadRecord) { r.Results[0].Match = false; r.Matched = 1 },
		"the work":    func(r *ReadRecord) { r.Results[0].Work = map[string]uint64{"neighbors/rows-state": 4} },
		"the head":    func(r *ReadRecord) { r.HeadHash = "x" },
		"a question":  func(r *ReadRecord) { r.Results = r.Results[:1] },
	} {
		b := mk()
		mutate(&b)
		ds, err := Diff(marshal(t, a), marshal(t, b))
		if err != nil || categories(ds)["result"] == 0 {
			t.Errorf("%s: %v, %v", name, ds, err)
		}
	}
	// Many differences are counted and the first few listed.
	big := mk()
	big.Results = nil
	other := mk()
	other.Results = nil
	for range 25 {
		big.Results = append(big.Results, QueryResult{Group: "g", Match: true})
		other.Results = append(other.Results, QueryResult{Group: "g", Match: true, KeysLog: 1})
	}
	big.Queries, other.Queries, big.Matched, other.Matched = 25, 25, 25, 25
	ds, err := Diff(marshal(t, big), marshal(t, other))
	if err != nil || len(ds) != 11 || !strings.Contains(ds[10].What, "15 more") {
		t.Fatalf("%d differences, %v; the last: %+v", len(ds), err, ds[len(ds)-1])
	}
}

func TestTwoSizesDifferInBytesAndBytesAloneAreNotAResult(t *testing.T) {
	mk := func() SizesRecord {
		return SizesRecord{
			Kind: KindSizes, Format: RecordFormat, Env: testEnv(), Input: testInput(), Arm: "commit", RecordsHeld: 10, Commits: 3, HeadHash: "h",
			AsLeft: Bytes{Total: 100, Journal: 100, Files: 4}, AfterGC: &Bytes{Total: 50, Tables: 50, Files: 5},
		}
	}
	a := mk()
	b := mk()
	b.AsLeft.Total, b.AsLeft.Journal = 101, 101
	ds, err := Diff(marshal(t, a), marshal(t, b))
	if err != nil || categories(ds)["bytes"] != 1 || categories(ds)["result"] != 0 {
		t.Fatalf("%v, %v", ds, err)
	}
	b = mk()
	b.AfterGC = nil
	if ds, _ := Diff(marshal(t, a), marshal(t, b)); categories(ds)["bytes"] != 1 {
		t.Fatalf("one collected and one not: %v", ds)
	}
	b = mk()
	b.RecordsHeld = 11
	if ds, _ := Diff(marshal(t, a), marshal(t, b)); categories(ds)["result"] != 1 {
		t.Fatalf("records held: %v", ds)
	}
}

func TestTwoExplainsDifferInAPlan(t *testing.T) {
	mk := func() ExplainRecord {
		return ExplainRecord{
			Kind: KindExplain, Format: RecordFormat, Env: testEnv(), Input: testInput(), Arm: "commit",
			Queries: []ExplainedQuery{{Group: "g", Age: "now", Op: "neighbors", Statements: []ExplainedStatement{{Kind: "rule", Plan: []string{"a"}, Range: true}}}},
		}
	}
	a := mk()
	if ds, err := Diff(marshal(t, a), marshal(t, mk())); err != nil || len(ds) != 0 {
		t.Fatalf("%v, %v", ds, err)
	}
	b := mk()
	b.Queries[0].Statements[0].Plan = []string{"b"}
	if ds, err := Diff(marshal(t, a), marshal(t, b)); err != nil || categories(ds)["result"] != 1 {
		t.Fatalf("%v, %v", ds, err)
	}
	b = mk()
	b.Queries = nil
	if ds, err := Diff(marshal(t, a), marshal(t, b)); err != nil || categories(ds)["result"] != 1 {
		t.Fatalf("%v, %v", ds, err)
	}
}

func TestTheDiffCommandSaysWhetherTheTwoAreRepeatable(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, v any) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, marshal(t, v), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a, same, other := testLoad(), testLoad(), testLoad()
	other.HeadHash = "h2"
	var out strings.Builder
	if err := runDiff([]string{write("a", a), write("same", same)}, &out, &out); err != nil || !strings.Contains(out.String(), "equal") {
		t.Fatalf("%v: %s", err, out.String())
	}
	out.Reset()
	err := runDiff([]string{write("a2", a), write("other", other)}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "not repeatable") || !strings.Contains(out.String(), "DIFFERENT (result) head commit hash") {
		t.Fatalf("%v: %s", err, out.String())
	}
	bytesOnly := SizesRecord{Kind: KindSizes, Format: RecordFormat, Env: testEnv(), Input: testInput(), Arm: "rows", AsLeft: Bytes{Total: 1}}
	bytesOnly2 := bytesOnly
	bytesOnly2.AsLeft.Total = 2
	out.Reset()
	err = runDiff([]string{write("s1", bytesOnly), write("s2", bytesOnly2)}, &out, &out)
	if err == nil || strings.Contains(err.Error(), "not repeatable") || !strings.Contains(out.String(), "DIFFERENT (bytes)") {
		t.Fatalf("%v: %s", err, out.String())
	}
	if err := runDiff([]string{"only-one"}, &out, &out); err == nil {
		t.Fatal("one file")
	}
	if err := runDiff([]string{filepath.Join(dir, "nope"), filepath.Join(dir, "nope")}, &out, &out); err == nil {
		t.Fatal("missing files")
	}
}

func TestMeasureDirSumsTheFilesByKind(t *testing.T) {
	root := t.TempDir()
	put := func(rel string, n int) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	put(".dolt/noms/"+strings.Repeat("v", 32), 1000)                // the journal
	put(".dolt/noms/journal.idx", 10)                               // its index
	put(".dolt/noms/"+strings.Repeat("0", 32), 200)                 // a table file
	put(".dolt/noms/oldgen/"+strings.Repeat("a", 31)+"b.darc", 300) // an archive
	put(".dolt/noms/manifest", 5)
	put(".dolt/noms/LOCK", 0)
	put(".dolt/config.json", 7)
	put(".dolt/stats/.dolt/noms/"+strings.Repeat("v", 32), 40) // the statistics' own journal
	put(".dolt/stats/.dolt/noms/manifest", 2)
	if err := os.Symlink(filepath.Join(root, ".dolt", "config.json"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	b, err := MeasureDir(root)
	if err != nil {
		t.Fatal(err)
	}
	want := Bytes{Total: 1000 + 10 + 200 + 300 + 5 + 7 + 40 + 2, Journal: 1010, Tables: 500, Statistics: 42, Other: 12, Files: 9}
	if b != want {
		t.Fatalf("%+v, want %+v", b, want)
	}
	if _, err := MeasureDir(filepath.Join(root, "missing")); err == nil {
		t.Fatal("a missing directory")
	}
	if _, err := MeasureDir(filepath.Join(root, ".dolt", "config.json")); err == nil {
		t.Fatal("a file")
	}
}

func TestTheResultsOfAGateAreSummedByGroupAndAge(t *testing.T) {
	rep := &g0.Report{Results: []g0.Result{
		{Group: "g1", Rank: 0, Age: "now", Op: runner.OpNeighbors, Match: true, Paths: map[string]uint64{"neighbors/as-of-alone": 1, "neighbors/keys-state": 4}},
		{Group: "g1", Rank: 1, Age: "now", Op: runner.OpNeighbors, Match: true, Paths: map[string]uint64{"neighbors/as-of-alone": 1, "neighbors/keys-state": 2}},
		{Group: "g1", Rank: 0, Age: "old-token", Op: runner.OpNeighbors, Match: true, Paths: map[string]uint64{"neighbors/needed-log": 1, "neighbors/keys-state": 1, "neighbors/keys-log": 3}, Work: map[string]uint64{"neighbors/rows-state": 5, "neighbors/rows-log": 9}},
		{Group: "g2", Rank: 0, Age: "old-token", Op: runner.OpAlive, Match: false, Paths: map[string]uint64{"alive/no-commit": 1}},
		{Group: "g3", Rank: 0, Age: "window-1h", Op: runner.OpWindow, Match: true, Paths: map[string]uint64{"window/log": 1}, Work: map[string]uint64{"window/rows-log": 7, "window/rows-prefix": 11}},
	}}
	results := resultsOf(rep)
	if got := results[0]; got.Outcome != "as-of-alone" || got.KeysState != 4 || got.Op != "neighbors" {
		t.Fatalf("%+v", got)
	}
	if got := results[3]; got.Outcome != "no-commit" || got.Match {
		t.Fatalf("%+v", got)
	}
	if got := results[4]; got.Outcome != "log" {
		t.Fatalf("%+v", got)
	}
	groups, ages := sums(results)
	if len(groups) != 4 {
		t.Fatalf("%d groups: %+v", len(groups), groups)
	}
	g := groups[0]
	if g.Group != "g1" || g.Age != "now" || g.Queries != 2 || g.AsOfAlone != 2 || g.KeysState != 6 || g.Wrong != 0 {
		t.Fatalf("%+v", g)
	}
	if g := groups[1]; g.NeededLog != 1 || g.KeysLog != 3 || g.WorkRowsState != 5 || g.WorkRowsLog != 9 {
		t.Fatalf("%+v", g)
	}
	if g := groups[2]; g.NoCommit != 1 || g.Wrong != 1 || g.Matched != 0 {
		t.Fatalf("%+v", g)
	}
	if g := groups[3]; g.WorkWindowRows != 7 || g.WorkWindowPrefix != 11 || g.AsOfAlone+g.NeededLog+g.NoCommit != 0 {
		t.Fatalf("%+v", g)
	}
	// Windows read the log in every arm: they are in no age's sums.
	if len(ages) != 2 || ages[0].Age != "now" || ages[1].Age != "old-token" || ages[1].Queries != 2 || ages[1].Wrong != 1 || ages[1].NeededLog != 1 || ages[1].NoCommit != 1 {
		t.Fatalf("%+v", ages)
	}
}

// TestTheModelCheckFindsAServerThatDiffers: the server's report of a plan, here a model's, is
// accepted when it is the model's for every question, and refused when one answer, one path or
// one count of keys differs, for each arm and the diagnostic.
func TestTheModelCheckFindsAServerThatDiffers(t *testing.T) {
	p := thePlans(t)["retained"]
	s := &stream{plan: p.plan, planSHA: p.sha}
	for _, tc := range []struct {
		name string
		arm  doltmodel.Arm
		diag bool
	}{{"commit", doltmodel.ArmCommit, false}, {"rows", doltmodel.ArmRows, false}, {"commit-time", doltmodel.ArmCommit, true}} {
		t.Run(tc.name, func(t *testing.T) {
			capacity := doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)
			var eng engineLike
			if tc.diag {
				eng = doltmodel.NewCommitTime(capacity)
			} else {
				eng = doltmodel.New(doltmodel.WithArm(tc.arm), capacity)
			}
			// The "server" is a second model: what it reports is what the check holds.
			info, err := runner.Drive(t.Context(), p.plan.Spec, modelSink{eng})
			if err != nil || info.Records == 0 {
				t.Fatal(err)
			}
			srv, err := g0.Check(t.Context(), p.plan, eng, nil, p.plan.Stream)
			if err != nil {
				t.Fatal(err)
			}
			check := func(srv *g0.Report) error {
				rec := &ReadRecord{Results: resultsOf(srv)}
				return checkAgainstModel(t.Context(), s, tc.arm, tc.diag, srv, rec, nil)
			}
			if err := check(srv); err != nil {
				t.Fatalf("a server that is the model: %v", err)
			}
			// Tamper with one question of the report, in each way.
			for name, mutate := range map[string]func(*g0.Result){
				"a match":        func(r *g0.Result) { r.Match = !r.Match },
				"a key count":    func(r *g0.Result) { r.Paths = bump(r.Paths, "/keys-log") },
				"an outcome":     func(r *g0.Result) { r.Paths = swapOutcome(r.Paths) },
				"a path removed": func(r *g0.Result) { r.Paths = nil },
			} {
				tampered := *srv
				tampered.Results = append([]g0.Result(nil), srv.Results...)
				i := len(tampered.Results) / 2
				for ; i < len(tampered.Results); i++ { // a question that has paths to tamper with
					if tampered.Results[i].Op != runner.OpWindow && len(tampered.Results[i].Paths) > 0 {
						break
					}
				}
				r := tampered.Results[i]
				r.Paths = clonePaths(r.Paths)
				mutate(&r)
				if name == "a key count" && tc.diag {
					continue // the diagnostic counts no keys
				}
				tampered.Results[i] = r
				if err := check(&tampered); err == nil {
					t.Errorf("a report with %s was accepted as the model's", name)
				}
			}
		})
	}
}

type engineLike interface {
	engine.Engine
	g0.PathReporter
}

func clonePaths(m map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// bump adds one to a path whose name ends in suffix, or adds the path.
func bump(m map[string]uint64, suffix string) map[string]uint64 {
	m = clonePaths(m)
	for k := range m {
		if strings.HasSuffix(k, suffix) {
			m[k]++
			return m
		}
	}
	m["neighbors"+suffix] = 1
	return m
}

func swapOutcome(m map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range m {
		switch {
		case strings.HasSuffix(k, "/as-of-alone"):
			k = strings.TrimSuffix(k, "/as-of-alone") + "/needed-log"
		case strings.HasSuffix(k, "/needed-log"):
			k = strings.TrimSuffix(k, "/needed-log") + "/as-of-alone"
		case strings.HasSuffix(k, "/commit-time"):
			k = strings.TrimSuffix(k, "/commit-time") + "/needed-log"
		}
		out[k] = v
	}
	return out
}
