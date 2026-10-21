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
	plan := fs.String("plan", "", "the plan.json the build was made from")
	expect := fs.String("expect-plan", "", "the SHA-256 of the plan file, as hex; required")
	armName := fs.String("arm", "", "the arm to audit: A1, A2 or I")
	queries := fs.String("query", "", "the questions to audit, by position in the plan, comma separated; by default the first of each shape of question, at most -max of them")
	max := fs.Int("max", 40, "how many shapes to audit by default")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *armName == "" || fs.NArg() != 0 {
		return fmt.Errorf("%s", usage)
	}
	arms, err := duckengine.ParseArms(*armName)
	if err != nil || len(arms) != 1 {
		return fmt.Errorf("-arm names one arm: A1, A2 or I")
	}
	p, sum, err := loadPlan(*plan, *expect, true)
	if err != nil {
		return err
	}
	m, err := duckengine.ReadManifest(*dir)
	if err != nil {
		return err
	}
	if err := checkBuildIsOfPlan(m, p, sum); err != nil {
		return err
	}
	picks, err := pickQueries(p, *queries, *max)
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

	fmt.Fprintf(stdout, "plan sha256 %s\narm %s, a connection that caches nothing and logs the file system's reads\n", sum, arms[0])
	differs := 0
	for _, i := range picks {
		q := p.Queries[i]
		ans, err := runner.Ask(r, q)
		if err != nil {
			return fmt.Errorf("question %d (%s): %w", i, q.Name(), err)
		}
		if want := (runner.Answer{Digest: q.Expect, Size: q.Size}); ans != want {
			return fmt.Errorf("question %d (%s) answered %+v, the plan has %+v", i, q.Name(), ans, want)
		}
		tr := r.Trace()
		fmt.Fprintf(stdout, "\nquestion %d: %s\nstatement %s\n%s\nplan %s\n", i, q.Name(), tr.Statement, indent(tr.SQL), tr.Profile.Shape())
		scans := tr.Profile.Scans()
		for j, a := range tr.Admissions {
			o := tr.Observed[j]
			bound := ""
			if a.Bound {
				bound = " (bound: an IN-list upper bound, in G0 and in no slope class)"
			}
			fmt.Fprintf(stdout, "scan %s: row groups admitted %d of %d; rows scanned %d, bytes admitted %d; profiler cardinality %d; diagnostic, row groups admitted by bounds alone %d%s\n",
				a.Role, a.Admitted.Groups, a.Total.Groups, a.Admitted.Rows, a.Admitted.Bytes, scans[j].Cardinality, a.BoundsOnly.Groups, bound)
			fmt.Fprintf(stdout, "  read from the file: row groups %d (%v), bytes of pages %d, of bloom filters %d, other %d\n", len(o.Groups), o.Groups, o.DataBytes, o.BloomBytes, o.OtherBytes)
			verdict := "agrees"
			switch {
			case a.Bound && len(o.Groups) > a.Admitted.Groups:
				verdict, differs = "DIFFERS: more row groups read than the upper bound", differs+1
			case !a.Bound && !slices.Equal(o.Groups, a.IDs):
				verdict, differs = fmt.Sprintf("DIFFERS: counted %v", a.IDs), differs+1
			case a.Bound:
				verdict = "is within the upper bound"
			}
			fmt.Fprintf(stdout, "  the count %s\n", verdict)
		}
	}
	if differs > 0 {
		return fmt.Errorf("%d scans were counted otherwise than DuckDB read them", differs)
	}
	return nil
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n  ")
}

// pickQueries is the questions named, or the first of each shape: operation, direction,
// age and layer.
func pickQueries(p *runner.Plan, list string, max int) ([]int, error) {
	if list != "" {
		var out []int
		for _, f := range strings.Split(list, ",") {
			i, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || i < 0 || i >= len(p.Queries) {
				return nil, fmt.Errorf("-query %q: the plan has questions 0 to %d", f, len(p.Queries)-1)
			}
			out = append(out, i)
		}
		return out, nil
	}
	seen := map[string]bool{}
	var out []int
	for i, q := range p.Queries {
		k := fmt.Sprintf("%s/%s/%s/%s", q.Op, q.Dir, q.Age, q.Layer)
		if seen[k] || len(out) >= max {
			continue
		}
		seen[k] = true
		out = append(out, i)
	}
	return out, nil
}
