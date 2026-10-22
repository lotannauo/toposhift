package doltspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// A shape is how a plan may read one table of a statement: through which index, with how many
// leading columns fixed to a value, and whether a range bounds the next one.
type shape struct {
	variant string
	index   string
	bound   int
	ranged  bool
}

const (
	pkLog        = "layer,dir,a,peer,rel,producer,event_ns,seq"
	timeLog      = "layer,dir,a,event_ns,seq"
	variantTime  = "index on event time"
	variantByKey = "primary key"
)

// expected is how each kind of statement is assumed to read each table, which is what the
// model's derived counts of rows ([doltmodel.Model.Work]) rest on:
//
//   - rule: the state table by the subject's (layer, direction, fingerprint) prefix, three
//     columns, and the log by a lookup on the whole key of a state row (layer, direction,
//     fingerprint, peer, relation, producer), six, for the keys whose row is after the instant;
//   - log (no commit to read): the log by the subject's prefix, three columns, through the
//     primary key or the index on event time, which hold the same rows under it;
//   - alive-rule: the entity state by the fingerprint, one column, and the entity log by a
//     lookup on (fingerprint, producer), two;
//   - alive-log: the entity log by the fingerprint, one;
//   - window: the log by the subject's prefix, three columns, through the index on event time
//     with a range on the event time, or through the primary key, whose range for the prefix
//     has no bound on the event time that a seek can use. The planner chooses between the two
//     by its statistics and a hint does not bind it, so both are accepted and the plan names
//     which: the model counts the rows of each ("window/rows-log" and "window/rows-prefix").
func expected(kind, table string) []shape {
	switch kind + "/" + table {
	case "rule/state":
		return []shape{{"", "layer,dir,a,peer,rel,producer", 3, false}}
	case "rule/log":
		return []shape{{"", pkLog, 6, false}}
	case "log/log":
		return []shape{{variantByKey, pkLog, 3, false}, {variantTime, timeLog, 3, false}}
	case "alive-rule/ent_state":
		return []shape{{"", "fp,producer", 1, false}}
	case "alive-rule/ent_log":
		return []shape{{"", "fp,producer,event_ns,seq", 2, false}}
	case "alive-log/ent_log":
		return []shape{{"", "fp,producer,event_ns,seq", 1, false}}
	case "window/log":
		return []shape{{variantTime, timeLog, 3, true}, {variantByKey, pkLog, 3, false}}
	}
	return nil
}

// CheckAccesses holds a plan's table reads to the assumption of the statement's kind and
// returns what differs, one line each (a scan of a table with no index, a table the kind does
// not read, an index or a bound prefix that is not an assumed one), and, for a statement the
// assumption lets the planner read two ways, the way it chose.
func CheckAccesses(kind string, accesses []Access, scans bool) (problems []string, variant string) {
	if scans {
		problems = append(problems, "the plan scans a table with no index")
	}
	if len(accesses) == 0 {
		problems = append(problems, "the plan reads no table through an index")
	}
	for _, a := range accesses {
		shapes := expected(kind, a.Table)
		if shapes == nil {
			problems = append(problems, fmt.Sprintf("a %s statement reads %s, which it is not assumed to", kind, a.Table))
			continue
		}
		if a.Unbounded {
			problems = append(problems, fmt.Sprintf("%s is read whole", a.Table))
			continue
		}
		matched := false
		for _, sh := range shapes {
			if strings.Join(a.Index, ",") == sh.index && a.Bound == sh.bound && a.Ranged == sh.ranged {
				matched = true
				variant = sh.variant
			}
		}
		if !matched {
			problems = append(problems, fmt.Sprintf("%s is read through the index (%s) with %d leading columns fixed (range after them: %v), which is none of the assumed ways", a.Table, strings.Join(a.Index, ","), a.Bound, a.Ranged))
		}
	}
	return problems, variant
}

func runExplain(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("explain", stderr)
	var (
		c        common
		in       inputFlags
		arm      string
		loadPath string
		queries  string
		all      bool
	)
	c.register(fs, true)
	in.register(fs, false)
	fs.StringVar(&arm, "arm", "", "the arm: commit or rows")
	fs.StringVar(&loadPath, "load", "", "the load record (load-<arm>.json) of the database")
	fs.StringVar(&queries, "query", "", "the questions to explain, as 1-based positions in the plan separated by commas (default: every question)")
	fs.BoolVar(&all, "all-plans", false, "keep the text of the plan of every statement (default: the first of each group and age)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if loadPath == "" || fs.NArg() != 0 {
		return errors.New("usage: doltspike explain -dsn dsn -image image -db name -arm commit|rows -load load.json [-out dir] (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha) [-query n,...] [-all-plans]")
	}
	a, err := parseArm(arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(false, true); err != nil {
		return err
	}
	name := "explain-" + arm + ".json"
	if c.out != "" {
		if err := checkFree(c.out, name); err != nil {
			return err
		}
	}
	s, err := in.resolve(false)
	if err != nil {
		return err
	}
	pick, err := parsePositions(queries, len(s.questions().Queries))
	if err != nil {
		return err
	}
	env, err := serverEnv(ctx, c.dsn, c.image)
	if err != nil {
		return err
	}
	l, err := openLoaded(ctx, &c, loadPath, a, s)
	if err != nil {
		return err
	}
	defer func() { _ = l.e.Close() }()

	rec := &ExplainRecord{Kind: KindExplain, Format: RecordFormat, Env: env, Input: s.input(), Arm: arm, Database: c.db, LoadSHA256: l.sum}
	seen := map[[2]string]bool{}
	var problems []string
	statements := 0
	for i, q := range s.questions().Queries {
		if pick != nil && !pick[i] {
			continue
		}
		sts, err := l.e.ReadStatements(q)
		if err != nil {
			return err
		}
		eq := ExplainedQuery{Group: q.Group, Rank: q.Rank, Age: q.Age, Op: string(q.Op)}
		for _, st := range sts {
			lines, err := l.e.Explain(ctx, st)
			if err != nil {
				return fmt.Errorf("doltspike: question %d (%s #%d at %s): %w", i+1, q.Group, q.Rank, q.Age, err)
			}
			accesses, scans, err := ParsePlan(lines)
			if err != nil {
				return err
			}
			es := ExplainedStatement{Kind: st.Kind, Accesses: accesses}
			p, variant := CheckAccesses(st.Kind, accesses, scans)
			es.Variant = variant
			if len(p) > 0 {
				for _, x := range p {
					problems = append(problems, fmt.Sprintf("question %d (%s #%d at %s, %s): %s", i+1, q.Group, q.Rank, q.Age, st.Kind, x))
				}
			} else {
				es.Range = true
			}
			if k := [2]string{q.Group, q.Age}; all || !seen[k] || !es.Range {
				seen[k] = true
				es.Plan = lines
			}
			statements++
			eq.Statements = append(eq.Statements, es)
		}
		rec.Queries = append(rec.Queries, eq)
	}
	fmt.Fprintf(stdout, "arm %s, database %s: %d statements of %d questions explained\n", arm, c.db, statements, len(rec.Queries))
	byKind := map[string]int{}
	for _, eq := range rec.Queries {
		for _, es := range eq.Statements {
			if !es.Range {
				continue
			}
			k := es.Kind
			if es.Variant != "" {
				k += " (" + es.Variant + ")"
			}
			byKind[k]++
		}
	}
	for _, k := range slices.Sorted(maps.Keys(byKind)) {
		fmt.Fprintf(stdout, "  %-40s %d read as assumed\n", k, byKind[k])
	}
	if c.out != "" {
		if err := writeNew(c.out, name, rec); err != nil {
			return err
		}
	}
	if len(problems) > 0 {
		for _, p := range problems[:min(len(problems), 20)] {
			fmt.Fprintf(stdout, "NOT AS ASSUMED %s\n", p)
		}
		return fmt.Errorf("%d statements are not read as the derived counts assume", len(problems))
	}
	fmt.Fprintln(stdout, "every statement reads its tables through the assumed indexes and key prefixes, and none scans a table")
	return nil
}

// parsePositions reads "1,5,9" as a set of zero-based positions among n, or nil for none named.
func parsePositions(s string, n int) (map[int]bool, error) {
	if s == "" {
		return nil, nil
	}
	out := map[int]bool{}
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || v < 1 || v > n {
			return nil, fmt.Errorf("doltspike: -query %q: positions run from 1 to %d", f, n)
		}
		out[v-1] = true
	}
	return out, nil
}
