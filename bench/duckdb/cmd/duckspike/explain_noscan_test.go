package main

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// TestTheAuditHoldsAQuestionPlannedToNoScanToReadingNothing: a question of arm I that
// asks for an instant before the stream began admits no row group of the entity file, and
// DuckDB reads none of it. The audit prints it with a profiler cardinality of 0, whether
// the profile it is given has scans (the connection that audits profiles its log queries)
// or none (a statement planned with no table scan), and agrees. A question that reads a
// row group is printed as it always was.
func TestTheAuditHoldsAQuestionPlannedToNoScanToReadingNothing(t *testing.T) {
	plan := planFile(t, 1)
	sum := shaOf(t, plan)
	build := filepath.Join(t.TempDir(), "build")
	if r := exec("build", "-plan", plan, "-expect-plan", sum, "-out", build); r.err != nil {
		t.Fatalf("build: %v\n%s", r.err, r.out)
	}
	p, err := runner.LoadPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	db, err := duckengine.Open(context.Background(), duckengine.Config{Mode: duckengine.ModeObserve, TempDir: t.TempDir(), Profile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := duckengine.OpenReader(context.Background(), db, build, duckengine.ArmI, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// A question as of the newest token is answered from the interval files in arm I.
	i := slices.IndexFunc(p.Queries, func(q runner.Query) bool {
		if q.Op != runner.OpAlive || q.Age != runner.AgeNow {
			return false
		}
		_, err := runner.Ask(r, q)
		return err == nil && r.Trace().Statement == "alive.intervals"
	})
	if i < 0 {
		t.Fatal("no alive question answered from the interval files")
	}
	q := p.Queries[i]

	var scanned bytes.Buffer
	if d := auditScans(&scanned, r.Trace()); d != 0 || strings.Contains(scanned.String(), "planned with no table scan") || !strings.Contains(scanned.String(), "the count agrees") ||
		strings.Contains(scanned.String(), "row groups admitted 0 of") {
		t.Errorf("a question that reads a row group: %d\n%s", d, scanned.String())
	}

	q.At = q.At.Add(-24 * time.Hour)
	if _, err := runner.Ask(r, q); err != nil {
		t.Fatal(err)
	}
	tr := r.Trace()
	if len(tr.Admissions) != 1 || tr.Admissions[0].Admitted.Groups != 0 || tr.Admissions[0].Total.Groups == 0 {
		t.Fatalf("the question before the stream does not admit nothing of one file: %+v", tr.Admissions)
	}
	const line = "scan ient: row groups admitted 0 of 1; rows scanned 0, bytes admitted 0; profiler cardinality "
	for name, profile := range map[string]duckengine.Profile{"the profile as it is": tr.Profile, "a profile with no scan": {}} {
		tr := tr
		tr.Profile = profile
		var out bytes.Buffer
		if d := auditScans(&out, tr); d != 0 || strings.Contains(out.String(), "DIFFERS") || !strings.Contains(out.String(), "the count agrees") || !strings.Contains(out.String(), line) {
			t.Errorf("%s: %d\n%s", name, d, out.String())
		}
		if got := strings.Contains(out.String(), "(planned with no table scan)"); got != (len(profile.Scans()) == 0) {
			t.Errorf("%s: the suffix is %v:\n%s", name, got, out.String())
		}
		if len(profile.Scans()) == 0 && !strings.Contains(out.String(), line+"0;") {
			t.Errorf("%s: a cardinality that is not 0:\n%s", name, out.String())
		}
	}
	// A count of a row group that DuckDB did not read is still found.
	bad := tr
	bad.Profile = duckengine.Profile{}
	bad.Admissions = slices.Clone(tr.Admissions)
	bad.Admissions[0].IDs = []int{0}
	var out bytes.Buffer
	if d := auditScans(&out, bad); d != 1 || !strings.Contains(out.String(), "DIFFERS: counted [0]") {
		t.Errorf("a count of a row group nobody read: %d\n%s", d, out.String())
	}
}
