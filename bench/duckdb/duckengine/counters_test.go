package duckengine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func TestCountersOfEveryArmAreRepeatableAndOrdered(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	for _, arm := range Arms {
		r, err := OpenReader(context.Background(), openRead(t, true), dir, arm, false)
		mustf(t, err, "open")
		qs, err := Collect(context.Background(), r, plan)
		if err != nil {
			t.Fatalf("%s: %v", arm, err)
		}
		if len(qs) != len(plan.Queries) {
			t.Fatalf("%s: %d counters for %d questions", arm, len(qs), len(plan.Queries))
		}
		for _, q := range qs {
			if len(q.Scans) == 0 || q.PlanShape == "" || q.ProfileDigest == "" {
				t.Fatalf("%s: %s has no scan or plan: %+v", arm, q.Name, q)
			}
			for _, s := range q.Scans {
				a := s.Admission
				if a.Admitted.Groups < 0 || a.Admitted.Groups > a.BoundsOnly.Groups || a.BoundsOnly.Groups > a.Total.Groups ||
					a.Admitted.Rows > a.BoundsOnly.Rows || a.BoundsOnly.Rows > a.Total.Rows ||
					a.Admitted.Bytes > a.BoundsOnly.Bytes || a.BoundsOnly.Bytes > a.Total.Bytes ||
					len(a.IDs) != a.Admitted.Groups || len(a.BoundsIDs) != a.BoundsOnly.Groups {
					t.Errorf("%s: %s: an admission that does not nest: %+v", arm, q.Name, a)
				}
				if s.RowsScanned != s.Admitted.Rows || s.RowsScanned <= 0 || s.ProfilerCardinality > s.RowsScanned {
					t.Errorf("%s: %s: scan rows %+v", arm, q.Name, s)
				}
				// A list is the only thing counted by an upper bound.
				if s.Bound != (q.Op == runner.OpBatch) {
					t.Errorf("%s: %s (%s): bound %v", arm, q.Name, q.Statement, s.Bound)
				}
			}
			isIntervals := strings.HasSuffix(q.Statement, ".intervals")
			if isIntervals != (arm == ArmI && q.Age != runner.AgeOldToken && q.Op != runner.OpWindow) {
				t.Errorf("%s: %s was answered by %s", arm, q.Name, q.Statement)
			}
			role := q.Scans[0].Role
			switch {
			case arm == ArmA1 && role != RoleArchive,
				arm == ArmA2 && role != RoleFwd && role != RoleRev,
				arm == ArmI && isIntervals && role != RoleIFwd && role != RoleIRev && role != RoleIEnt,
				arm == ArmI && !isIntervals && role != RoleFwd && role != RoleRev:
				t.Errorf("%s: %s scanned %s", arm, q.Name, role)
			}
		}
		sum := Summarize(qs)
		var n int
		for _, g := range sum {
			n += g.Queries
		}
		if n != len(qs) {
			t.Errorf("%s: the groups hold %d of %d questions", arm, n, len(qs))
		}
	}
}

func TestNoCounterIsKeptOfAWrongRead(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	r, err := OpenReader(context.Background(), openRead(t, true), dir, ArmA2, false)
	mustf(t, err, "open")
	wrong := *plan
	wrong.Queries = slices.Clone(plan.Queries)
	wrong.Queries[3].Expect = strings.Repeat("0", len(wrong.Queries[3].Expect))
	if _, err := CollectPass(context.Background(), r, &wrong); !errors.Is(err, ErrWrongAnswer) {
		t.Errorf("a plan whose answer differs: %v", err)
	}
	wrong.Queries = slices.Clone(plan.Queries)
	wrong.Queries[5].Size++
	if _, err := Collect(context.Background(), r, &wrong); !errors.Is(err, ErrWrongAnswer) {
		t.Errorf("a plan whose size differs: %v", err)
	}
	noProfile, err := OpenReader(context.Background(), openRead(t, false), dir, ArmA2, false)
	mustf(t, err, "open")
	if _, err := CollectPass(context.Background(), noProfile, plan); err == nil {
		t.Error("counting on a connection that does not profile")
	}
}

// TestTwoPassesThatCostDifferentlyAreRefused: every field the two passes must agree on is
// compared.
func TestTwoPassesThatCostDifferentlyAreRefused(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	r, err := OpenReader(context.Background(), openRead(t, true), dir, ArmA2, false)
	mustf(t, err, "open")
	base, err := CollectPass(context.Background(), r, plan)
	mustf(t, err, "pass")
	again, err := CollectPass(context.Background(), r, plan)
	mustf(t, err, "pass")
	for i := range base {
		if !equalCounters(base[i], again[i]) {
			t.Fatalf("two passes differ at %s", base[i].Name)
		}
	}
	for name, mutate := range map[string]func(*QueryCounters){
		"a plan shape":         func(q *QueryCounters) { q.PlanShape += ">" },
		"a profile digest":     func(q *QueryCounters) { q.ProfileDigest = "x" },
		"an answer":            func(q *QueryCounters) { q.Answer.Size++ },
		"a statement":          func(q *QueryCounters) { q.Statement = "x" },
		"rows scanned":         func(q *QueryCounters) { q.Scans[0].RowsScanned++ },
		"profiler cardinality": func(q *QueryCounters) { q.Scans[0].ProfilerCardinality++ },
		"row groups":           func(q *QueryCounters) { q.Scans[0].Admitted.Groups++ },
		"row groups by bounds": func(q *QueryCounters) { q.Scans[0].BoundsOnly.Groups++ },
		"bytes":                func(q *QueryCounters) { q.Scans[0].Admitted.Bytes++ },
		"a file":               func(q *QueryCounters) { q.Scans[0].Role = "x" },
		"a scan less":          func(q *QueryCounters) { q.Scans = nil },
		"a plan with no scan":  func(q *QueryCounters) { q.PlannedNoScan = true },
	} {
		q := base[0]
		q.Scans = slices.Clone(q.Scans)
		mutate(&q)
		if equalCounters(base[0], q) {
			t.Errorf("a difference in %s was not seen", name)
		}
	}
}

func TestSummarizeSumsByGroupAndAge(t *testing.T) {
	scan := func(rows int64, groups int, bound bool) ScanCounters {
		return ScanCounters{RowsScanned: int64(groups-1) * 10, ProfilerCardinality: 1, Admission: Admission{
			Total: Share{10, 100, 1000}, BoundsOnly: Share{Groups: groups, Rows: int64(groups) * 10, Bytes: int64(groups) * 100},
			Admitted: Share{Groups: groups - 1, Rows: int64(groups-1) * 10, Bytes: int64(groups-1) * 100}, Bound: bound,
		}}
	}
	got := Summarize([]QueryCounters{
		{Group: "g", Age: "now", Scans: []ScanCounters{scan(100, 3, false)}},
		{Group: "h", Age: "now", Scans: []ScanCounters{scan(50, 2, true)}},
		{Group: "g", Age: "now", Scans: []ScanCounters{scan(100, 4, false), scan(5, 2, false)}},
		{Group: "g", Age: "1h", Scans: []ScanCounters{scan(1, 2, false)}},
	})
	if len(got) != 3 || got[0].Group != "g" || got[1].Group != "h" || got[2].Age != "1h" {
		t.Fatalf("%+v", got)
	}
	g := got[0]
	if g.Queries != 2 || g.RowsScanned != 60 || g.ProfilerCardinality != 3 || g.GroupsTotal != 30 || g.GroupsBoundsOnly != 9 || g.GroupsAdmitted != 6 ||
		g.RowsBoundsOnly != 90 || g.BytesBoundsOnly != 900 || g.BytesAdmitted != 600 || g.Bound {
		t.Errorf("%+v", g)
	}
	if !got[1].Bound || got[0].Bound {
		t.Error("the bound flag is not carried by the group that has a list")
	}
}
