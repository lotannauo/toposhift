package duckengine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// beforeTheStream moves the instant of the first question of an op, asked as of now, to a
// day before everything in the build, so that no interval has started by then. The answer
// is the record files' (arm A2 reads them alone), so that it is not the interval files'
// own.
func beforeTheStream(t *testing.T, dir string, plan *runner.Plan, op runner.Op) runner.Query {
	t.Helper()
	i := slices.IndexFunc(plan.Queries, func(q runner.Query) bool { return q.Op == op && q.Age == runner.AgeNow })
	if i < 0 {
		t.Fatalf("the plan has no %s question as of now", op)
	}
	q := plan.Queries[i]
	q.At = q.At.Add(-24 * time.Hour)
	ref, err := OpenReader(context.Background(), openRead(t, false), dir, ArmA2, false)
	mustf(t, err, "open")
	ans, err := runner.Ask(ref, q)
	mustf(t, err, "the reference answer")
	q.Expect, q.Size = ans.Digest, ans.Size
	return q
}

// TestAQuestionPlannedToNoScanIsCountedAsReadingNothing: DuckDB proves the interval files
// hold no row that holds before the stream began, plans the statement with no table scan,
// and the footers admit no row group either. The question is counted as a read of nothing,
// marked, and two passes of it are equal.
func TestAQuestionPlannedToNoScanIsCountedAsReadingNothing(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	r, err := OpenReader(context.Background(), openRead(t, true), dir, ArmI, false)
	mustf(t, err, "open")
	q := beforeTheStream(t, dir, plan, runner.OpAlive)
	qs := []runner.Query{q, plan.Queries[0]}
	got, err := CollectQueries(context.Background(), r, qs)
	if err != nil {
		t.Fatalf("a question planned to no scan: %v", err)
	}
	c := got[0]
	if !c.PlannedNoScan || len(c.Scans) != 1 || c.Statement != "alive.intervals" {
		t.Fatalf("%+v", c)
	}
	// The file is listed with nothing read: no row group, row or byte admitted, and no
	// cardinality, but its totals and its bounds-only count are kept.
	s := c.Scans[0]
	if s.Role != RoleIEnt || s.RowsScanned != 0 || s.ProfilerCardinality != 0 || s.Admitted != (Share{}) || s.Total.Groups == 0 || s.Total.Rows == 0 || s.Total.Bytes == 0 {
		t.Errorf("%+v", s)
	}
	// The question adds nothing to the work counters of its group, and its file's row
	// groups to the total.
	g := Summarize(got[:1])[0]
	if g.Queries != 1 || g.RowsScanned != 0 || g.GroupsAdmitted != 0 || g.BytesAdmitted != 0 || g.ProfilerCardinality != 0 ||
		g.GroupsTotal != s.Total.Groups || g.GroupsBoundsOnly != s.BoundsOnly.Groups || g.RowsBoundsOnly != s.BoundsOnly.Rows || g.BytesBoundsOnly != s.BoundsOnly.Bytes {
		t.Errorf("%+v", g)
	}
	// Another question of the set, which scans, is not marked.
	if got[1].PlannedNoScan || len(got[1].Scans) == 0 {
		t.Errorf("a question with a scan: %+v", got[1])
	}
	b, err := json.Marshal(got[0])
	mustf(t, err, "json")
	if !strings.Contains(string(b), `"PlannedNoScan":true`) {
		t.Errorf("the mark is not written: %s", b)
	}
	if b, _ := json.Marshal(got[1]); strings.Contains(string(b), "PlannedNoScan") {
		t.Errorf("a question with a scan has the mark: %s", b)
	}
	// What the statement did on each pass is the same, and the mark is part of that.
	again, err := CollectQueriesPass(context.Background(), r, qs)
	mustf(t, err, "pass")
	if !equalCounters(got[0], again[0]) {
		t.Errorf("two passes differ: %+v, then %+v", got[0], again[0])
	}
}

// TestAQuestionPlannedToNoScanThatTheFootersAdmitIsRefused: the tool counted a row group
// of the file, where DuckDB scanned none. That is a disagreement the counters do not hide.
// The same question is accepted when the footers admit nothing.
func TestAQuestionPlannedToNoScanThatTheFootersAdmitIsRefused(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	q := beforeTheStream(t, dir, plan, runner.OpAlive)
	r, err := OpenReader(context.Background(), openRead(t, true), dir, ArmI, false)
	mustf(t, err, "open")
	if _, err := CollectQueriesPass(context.Background(), r, []runner.Query{q}); err != nil {
		t.Fatalf("with the footers as they are: %v", err)
	}
	// The footer of the entity file with no bound on any column: every row group of it is
	// admitted by every filter.
	meta, err := r.fileMeta(context.Background(), RoleIEnt)
	mustf(t, err, "footer")
	blind := *meta
	blind.Groups = slices.Clone(meta.Groups)
	for i, g := range blind.Groups {
		cols := make(map[string]chunk, len(g.cols))
		for name, c := range g.cols {
			c.min, c.max, c.bloom = nil, nil, false
			cols[name] = c
		}
		blind.Groups[i].cols = cols
	}
	r.meta[RoleIEnt] = &blind
	_, err = CollectQueriesPass(context.Background(), r, []runner.Query{q})
	if err == nil || !strings.Contains(err.Error(), "the plan has no table scan but the footers admit 1 row groups of ient") {
		t.Errorf("a plan with no scan whose footers admit a row group: %v", err)
	}
	if _, err := CollectQueries(context.Background(), r, []runner.Query{q}); err == nil {
		t.Error("two passes of it were accepted")
	}
}

func TestCountScansPairsScansWithAdmissions(t *testing.T) {
	admit := func(role Role, groups int) Admission {
		return Admission{
			Role: role, Total: Share{Groups: 4, Rows: 40, Bytes: 400}, BoundsOnly: Share{Groups: groups, Rows: int64(groups) * 10, Bytes: int64(groups) * 100},
			Admitted: Share{Groups: groups, Rows: int64(groups) * 10, Bytes: int64(groups) * 100},
		}
	}
	scan := Node{Type: "TABLE_SCAN", Cardinality: 3}
	for name, c := range map[string]struct {
		scans   []Node
		adm     []Admission
		noScan  bool
		wantErr string
	}{
		"no scan, nothing admitted":                  {nil, []Admission{admit(RoleIEnt, 0)}, true, ""},
		"no scan, nothing admitted in either of two": {nil, []Admission{admit(RoleFwd, 0), admit(RoleRev, 0)}, true, ""},
		"no scan and no file":                        {nil, nil, false, ""},
		"no scan, a row group admitted":              {nil, []Admission{admit(RoleIEnt, 1)}, false, "the plan has no table scan but the footers admit 1 row groups of ient"},
		"no scan, a row group of the second file":    {nil, []Admission{admit(RoleFwd, 0), admit(RoleRev, 2)}, false, "admit 2 row groups of " + string(RoleRev)},
		"a scan, a file":                             {[]Node{scan}, []Admission{admit(RoleIEnt, 1)}, false, ""},
		"a scan of a file that admits nothing":       {[]Node{scan}, []Admission{admit(RoleIEnt, 0)}, false, ""},
		"two scans, a file":                          {[]Node{scan, scan}, []Admission{admit(RoleIEnt, 1)}, false, "the plan has 2 table scans and the statement was counted for 1 files"},
		"a scan, two files":                          {[]Node{scan}, []Admission{admit(RoleFwd, 1), admit(RoleRev, 1)}, false, "the plan has 1 table scans and the statement was counted for 2 files"},
	} {
		got, noScan, err := countScans("q", c.scans, c.adm)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) || got != nil || noScan {
				t.Errorf("%s: %v %v %v", name, got, noScan, err)
			}
			continue
		}
		// A file is listed once for each scan, or, with none, once with nothing scanned.
		want := len(c.scans)
		if c.noScan {
			want = len(c.adm)
		}
		if err != nil || noScan != c.noScan || len(got) != want || want == 0 && got != nil {
			t.Errorf("%s: %v %v %v", name, got, noScan, err)
			continue
		}
		for i, s := range got {
			cardinality := int64(3)
			if c.noScan {
				cardinality = 0
			}
			if s.Role != c.adm[i].Role || s.RowsScanned != c.adm[i].Admitted.Rows || s.ProfilerCardinality != cardinality || !s.Same(c.adm[i]) {
				t.Errorf("%s: scan %d is %+v", name, i, s)
			}
		}
	}
}

// TestNoQuestionThatScansCarriesTheMark: the counters of every arm of the plan, and of
// the hourly arm, have the mark on no question, and none of their JSON names it, so that
// a counters file of a read with no such question is the file it was before the mark.
func TestNoQuestionThatScansCarriesTheMark(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	check := func(t *testing.T, label string, qs []QueryCounters) {
		t.Helper()
		for _, q := range qs {
			if q.PlannedNoScan || len(q.Scans) == 0 {
				t.Errorf("%s: %s: %+v", label, q.Name, q)
			}
		}
		b, err := json.Marshal(struct {
			Groups  []GroupCounters
			Queries []QueryCounters
		}{Summarize(qs), qs})
		mustf(t, err, "json")
		if strings.Contains(string(b), "PlannedNoScan") {
			t.Errorf("%s: the mark is in the counters", label)
		}
	}
	for _, arm := range Arms {
		r, err := OpenReader(context.Background(), openRead(t, true), dir, arm, false)
		mustf(t, err, "open")
		qs, err := Collect(context.Background(), r, plan)
		mustf(t, err, "collect")
		check(t, string(arm), qs)
	}
	_, set, _ := sharedSet(t)
	fdir := filepath.Join(t.TempDir(), "build")
	_, err := buildFromFile(t, fdir, AllArms, hourlyOpts)
	mustf(t, err, "build")
	for _, arm := range AllArms {
		r, err := OpenReader(context.Background(), openRead(t, true), fdir, arm, false)
		mustf(t, err, "open")
		qs, err := CollectQueries(context.Background(), r, set.Queries)
		mustf(t, err, "collect")
		check(t, "file "+string(arm), qs)
	}
}
