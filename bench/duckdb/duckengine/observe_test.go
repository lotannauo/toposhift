package duckengine

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// TestTheCountOfRowGroupsIsWhatDuckDBRead builds the files of an hour of a modest cluster
// with row groups of 2,048 rows, so that a read has many to skip, and audits the count
// against the file system's log on a connection that caches nothing: the row groups the
// count admits are the row groups whose pages DuckDB read, for every question but a batch,
// and for a batch they are a bound on them.
//
// It also shows that the bloom filters are part of the count: counted from the bounds of
// the footer alone, DuckDB would have been said to read row groups it did not.
func TestTheCountOfRowGroupsIsWhatDuckDBRead(t *testing.T) {
	ctx := context.Background()
	spec := runner.DefaultSpec(workload.Small())
	spec.MinNonEmpty = 0
	spec.Retentions = []runner.Retention{{At: 20 * time.Minute, Keep: 10 * time.Minute}}
	plan, err := runner.MakePlan(ctx, spec, nil)
	mustf(t, err, "plan")
	dir := filepath.Join(t.TempDir(), "build")
	bdb, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open build")
	man, err := Build(ctx, bdb, dir, PlanSource(plan, "small"), AllArms, BuildOptions{RowGroups: RowGroupRows{2048, 2048, 2048}, Segment: 10 * time.Minute})
	bdb.Close()
	mustf(t, err, "build")
	if f, _ := man.File(RoleFwd); f.RowGroups < 10 {
		t.Fatalf("the files have %d row groups: too few to skip any", f.RowGroups)
	}

	step := 3
	if testing.Short() {
		step = 11
	}
	if len(man.Segments) < 3 {
		t.Fatalf("%d segments: too few to tell a file from the glob", len(man.Segments))
	}
	for _, arm := range AllArms {
		db, err := Open(ctx, Config{Mode: ModeObserve, TempDir: t.TempDir(), Profile: true})
		mustf(t, err, "open observe")
		r, err := OpenReader(ctx, db, dir, arm, false)
		mustf(t, err, "reader")
		var pruned, bloomMatters, boundsWrong, audited, filesSkipped int
		for i := 0; i < len(plan.Queries); i += step {
			q := plan.Queries[i]
			if _, err := runner.Ask(r, q); err != nil {
				t.Fatalf("%s: %s: %v", arm, q.Name(), err)
			}
			tr := r.Trace()
			if len(tr.Observed) != len(tr.Admissions) || len(tr.Admissions) == 0 {
				t.Fatalf("%s: %s: %d scans, %d observed", arm, q.Name(), len(tr.Admissions), len(tr.Observed))
			}
			for j, a := range tr.Admissions {
				o := tr.Observed[j]
				audited++
				if arm == ArmH {
					// Every read of a glob opens the footer of every file; and the files whose
					// pages it reads are those with a row group admitted (at most, for a bound).
					if a.Files != len(man.Segments) || o.Files != a.Files {
						t.Errorf("%s: %s: the glob names %d files, the count says %d and DuckDB opened %d", arm, tr.Statement, len(man.Segments), a.Files, o.Files)
					}
					if a.Bound && o.FilesRead > a.FilesRead || !a.Bound && o.FilesRead != a.FilesRead {
						t.Errorf("%s: %s (%s): counted %d files read, DuckDB read the pages of %d", arm, tr.Statement, q.Name(), a.FilesRead, o.FilesRead)
					}
					if a.FilesRead < a.Files {
						filesSkipped++
					}
				}
				if a.Bound {
					if !subset(o.Groups, a.IDs) {
						t.Errorf("%s: %s: DuckDB read row groups %v, which the upper bound %v does not hold", arm, tr.Statement, o.Groups, a.IDs)
					}
					continue
				}
				if !slices.Equal(o.Groups, a.IDs) {
					t.Errorf("%s: %s (%s): counted row groups %v, DuckDB read %v", arm, tr.Statement, q.Name(), a.IDs, o.Groups)
				}
				if a.Admitted.Groups < a.Total.Groups {
					pruned++
				}
				if a.Admitted.Groups < a.BoundsOnly.Groups {
					bloomMatters++
				}
				if !slices.Equal(o.Groups, a.BoundsIDs) {
					boundsWrong++
				}
			}
		}
		if audited == 0 || pruned == 0 {
			t.Errorf("%s: %d scans audited, %d pruned: the audit saw nothing skipped", arm, audited, pruned)
		}
		if arm == ArmH && filesSkipped == 0 {
			t.Errorf("%s: no read skipped a whole file: the audit cannot tell files read from files opened", arm)
		}
		if bloomMatters == 0 || boundsWrong == 0 {
			t.Errorf("%s: the bloom filters changed the count of %d scans, and counting by bounds alone would have been wrong for %d: the audit cannot tell the two counts apart", arm, bloomMatters, boundsWrong)
		}
		db.Close()
	}
}

func subset(a, b []int) bool {
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}
