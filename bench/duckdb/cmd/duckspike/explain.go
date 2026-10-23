package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/duckdb/duckengine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func runExplain(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duckspike explain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "the directory of a build")
	in := addInputFlags(fs)
	armName := fs.String("arm", "", "the arm to audit: A1, A2, I or H")
	queries := fs.String("query", "", "the questions to audit, by position in the plan or question set, comma separated; by default the first of each shape of question, at most -max of them")
	max := fs.Int("max", 40, "how many shapes to audit by default")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *armName == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	arms, err := duckengine.ParseArms(*armName)
	if err != nil || len(arms) != 1 {
		return fmt.Errorf("-arm names one arm: A1, A2, I or H")
	}
	input, err := in.load()
	if err != nil {
		return err
	}
	m, err := duckengine.ReadManifest(*dir)
	if err != nil {
		return err
	}
	if err := checkBuildIsOf(m, input); err != nil {
		return err
	}
	picks, err := pickQueries(input.queries, *queries, *max)
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "duckspike-explain-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	db, err := duckengine.Open(ctx, duckengine.Config{Mode: duckengine.ModeObserve, TempDir: tmp, Profile: true})
	if err != nil {
		return err
	}
	defer db.Close()
	r, err := duckengine.OpenReader(ctx, db, *dir, arms[0], true)
	if err != nil {
		return err
	}
	defer r.Close()

	fmt.Fprintf(stdout, "%s sha256 %s\narm %s, a connection that caches nothing and logs the file system's reads\n", input.kind, input.sum, arms[0])
	differs := 0
	for _, i := range picks {
		q := input.queries[i]
		ans, err := runner.Ask(r, q)
		if err != nil {
			return fmt.Errorf("question %d (%s): %w", i, q.Name(), err)
		}
		if want := (runner.Answer{Digest: q.Expect, Size: q.Size}); ans != want {
			return fmt.Errorf("question %d (%s) answered %+v, the plan has %+v", i, q.Name(), ans, want)
		}
		tr := r.Trace()
		fmt.Fprintf(stdout, "\nquestion %d: %s\nstatement %s\n%s\nplan %s\n", i, q.Name(), tr.Statement, indent(tr.SQL), tr.Profile.Shape())
		differs += auditScans(stdout, tr)
	}
	if differs > 0 {
		return fmt.Errorf("%d scans were counted otherwise than DuckDB read them", differs)
	}
	return nil
}

// auditScans prints, for each file the count admitted row groups of, what the count says
// and what DuckDB read of it, and returns how many it counted otherwise than DuckDB read
// them. A trace with no table scan in its profile has no figure of the profiler's to
// print for a file: it is 0, and the line says so. The count of such a file admitted no
// row group, and DuckDB read none, so that the audit agrees. In observe mode the profile
// is that of the log query run after the statement, so the cardinality printed is a
// diagnostic only; the verdict compares the file-system log with the count.
func auditScans(stdout io.Writer, tr duckengine.Trace) (differs int) {
	scans := tr.Profile.Scans()
	for j, a := range tr.Admissions {
		o := tr.Observed[j]
		bound := ""
		if a.Bound {
			bound = " (bound: an IN-list upper bound, in G0 and in no slope class)"
		}
		var cardinality int64
		switch {
		case len(scans) == 0:
			bound += " (planned with no table scan)"
		case j < len(scans):
			cardinality = scans[j].Cardinality
		}
		fmt.Fprintf(stdout, "scan %s: row groups admitted %d of %d; rows scanned %d, bytes admitted %d; profiler cardinality %d; diagnostic, row groups admitted by bounds alone %d%s\n",
			a.Role, a.Admitted.Groups, a.Total.Groups, a.Admitted.Rows, a.Admitted.Bytes, cardinality, a.BoundsOnly.Groups, bound)
		fmt.Fprintf(stdout, "  read from the file: row groups %d (%v), bytes of pages %d, of bloom filters %d, other %d\n", len(o.Groups), o.Groups, o.DataBytes, o.BloomBytes, o.OtherBytes)
		verdict := "agrees"
		if a.Files > 0 {
			fmt.Fprintf(stdout, "  files: %d opened by the count, %d by DuckDB; pages read of %d by the count, %d by DuckDB\n", a.Files, o.Files, a.FilesRead, o.FilesRead)
		}
		switch {
		case a.Files > 0 && (o.Files != a.Files || !a.Bound && o.FilesRead != a.FilesRead || a.Bound && o.FilesRead > a.FilesRead):
			verdict, differs = "DIFFERS: files", differs+1
		case a.Bound && len(o.Groups) > a.Admitted.Groups:
			verdict, differs = "DIFFERS: more row groups read than the upper bound", differs+1
		case !a.Bound && !slices.Equal(o.Groups, a.IDs):
			verdict, differs = fmt.Sprintf("DIFFERS: counted %v", a.IDs), differs+1
		case a.Bound:
			verdict = "is within the upper bound"
		}
		fmt.Fprintf(stdout, "  the count %s\n", verdict)
	}
	return differs
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n  ")
}

// pickQueries is the questions named, or the first of each shape: operation, direction,
// age and layer.
func pickQueries(queries []runner.Query, list string, max int) ([]int, error) {
	if list != "" {
		var out []int
		for _, f := range strings.Split(list, ",") {
			i, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || i < 0 || i >= len(queries) {
				return nil, fmt.Errorf("-query %q: there are questions 0 to %d", f, len(queries)-1)
			}
			out = append(out, i)
		}
		return out, nil
	}
	seen := map[string]bool{}
	var out []int
	for i, q := range queries {
		k := fmt.Sprintf("%s/%s/%s/%s", q.Op, q.Dir, q.Age, q.Layer)
		if seen[k] || len(out) >= max {
			continue
		}
		seen[k] = true
		out = append(out, i)
	}
	return out, nil
}
