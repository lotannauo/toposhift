// Command evalblast scores the blast-radius queries against a labelled dataset.
//
//	go run ./cmd/evalblast -source itbench -data DIR -out DIR -stage truth [-scenarios 1,2,3]
//	go run ./cmd/evalblast -source itbench -data DIR -out DIR -stage map -table-a SHA256
//	go run ./cmd/evalblast -source itbench -data DIR -out DIR -stage pool
//	go run ./cmd/evalblast -source itbench -data DIR -out DIR -stage score -table-a SHA256 -table-b SHA256 -services SHA256
//
// DIR of -data holds the fetched files, one folder a scenario. The run has four
// stages:
//
//   - truth reads only the labels (ground_truth.yaml) and the alerts, and prints
//     table A: for each scenario the root of the chain, the seed rule, the class,
//     the window and the groups that make up the truth.
//   - map also reads the Kubernetes objects, and prints table B: those groups
//     mapped to services and pods, and the seed found. Nothing is loaded and
//     nothing is scored.
//   - pool reads every scenario's traces export that is present, and every probe
//     names file (DIR/probe/Scenario-N.services: one service.name a line, sorted,
//     without repeats, names only, kept beside the data and never in the repository),
//     and writes the union to services_pooled.txt with its sha256. The union only
//     grows as data is read. The rule for what to fetch uses the set as it stands when
//     the probe is read; the set is frozen by its hash after the covered trace files
//     have arrived and before any score. A service outside it is uninstrumented.
//   - score loads each scenario into a store of its own, runs the
//     product queries and the baselines on it, and judges the pooled strata by the
//     acceptance rule. The store is discarded after each scenario.
//
// There is no default stage. The truth and map stages print the sha256 of their
// table; map needs the sha256 of table A, and score needs both tables' and the
// pooled set's, recomputes all three, and refuses unless they match, so that no score
// is made before they are approved. Score refuses -scenarios: the dataset's rules
// are never judged on a subset. Every scenario stays in the dataset's count, also
// one whose files are missing.
//
// Every table and report names entities, times and labels of the dataset, and is
// written under -out, which must be outside the repository. The report prints the
// dataset revision, the loader version and the versions of the queries, and no
// timing: these are accuracy numbers.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
	"github.com/lotannauo/toposhift/bench/eval/factload"
	"github.com/lotannauo/toposhift/bench/eval/itbench"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("evalblast", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.source, "source", "", "the dataset: itbench")
	fs.StringVar(&o.data, "data", "", "the directory of the fetched dataset")
	fs.StringVar(&o.out, "out", "", "the directory the tables and the report are written to, outside the repository")
	fs.StringVar(&o.stage, "stage", "", "truth, map, pool or score (required)")
	fs.StringVar(&o.only, "scenarios", "", "truth and map only: a comma-separated list of scenario numbers; all if empty")
	fs.StringVar(&o.tableA, "table-a", "", "map and score: the sha256 of table A, as printed by the truth stage")
	fs.StringVar(&o.tableB, "table-b", "", "score: the sha256 of table B, as printed by the map stage")
	fs.StringVar(&o.services, "services", "", "score: the sha256 of the pooled service names, as printed by the pool stage")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := execute(context.Background(), o, stdout); err != nil {
		fmt.Fprintln(stderr, "evalblast:", err)
		var u usageError
		if errors.As(err, &u) {
			return 2
		}
		return 1
	}
	return 0
}

type options struct {
	source, data, out, stage, only, tableA, tableB, services string
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usage(format string, args ...any) error { return usageError{fmt.Sprintf(format, args...)} }

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

func sum(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])
}

func execute(ctx context.Context, o options, stdout io.Writer) error {
	switch {
	case o.source != "itbench":
		return usage("-source must be itbench")
	case o.data == "" || o.out == "":
		return usage("-data and -out are required")
	case o.stage != "truth" && o.stage != "map" && o.stage != "pool" && o.stage != "score":
		return usage("-stage is required: truth, map, pool or score")
	case o.stage == "pool" && o.only != "":
		return usage("-scenarios is for the truth and map stages")
	case o.stage == "score" && o.only != "":
		return usage("-scenarios is for the truth and map stages: the dataset's rule is never judged on a subset")
	case (o.stage == "map" || o.stage == "score") && !sha256Hex.MatchString(o.tableA):
		return usage("-stage %s needs -table-a, the sha256 of table A", o.stage)
	case o.stage == "score" && !sha256Hex.MatchString(o.tableB):
		return usage("-stage score needs -table-b, the sha256 of table B")
	case o.stage == "score" && !sha256Hex.MatchString(o.services):
		return usage("-stage score needs -services, the sha256 of the pooled service names")
	}
	numbers, err := parseNumbers(o.only)
	if err != nil {
		return usage("%v", err)
	}
	if err := outsideRepository(o.out); err != nil {
		return usage("%v", err)
	}

	if o.stage == "pool" {
		if err := os.MkdirAll(o.out, 0o755); err != nil {
			return err
		}
		pooled, err := itbench.PooledServices(o.data)
		if err != nil {
			return err
		}
		text := itbench.PooledText(pooled)
		return emit(stdout, filepath.Join(o.out, "services_pooled.txt"), text, "services", sum(text))
	}

	// Table A is made from the labels and the alerts only, and each later stage
	// recomputes it and refuses to go on unless it is the one that was approved.
	rows, err := itbench.TableA(o.data)
	if err != nil {
		return err
	}
	rows = selectRows(rows, numbers)
	textA := itbench.FormatTableA(rows)
	if o.stage != "truth" && sum(textA) != o.tableA {
		return usage("table A is not the one named by -table-a (sha256 %s)", sum(textA))
	}
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return err
	}
	if o.stage == "truth" {
		return emit(stdout, filepath.Join(o.out, "table_a.txt"), textA, "table A", sum(textA))
	}

	textB, err := itbench.TableBText(o.data, rows)
	if err != nil {
		return err
	}
	if o.stage == "map" {
		return emit(stdout, filepath.Join(o.out, "table_b.txt"), textB, "table B", sum(textB))
	}
	if sum(textB) != o.tableB {
		return usage("table B is not the one named by -table-b (sha256 %s)", sum(textB))
	}
	// The services that have a span anywhere in the trace data read, frozen by its
	// hash before any case is scored: a seed outside it is uninstrumented.
	pooled, err := itbench.PooledServices(o.data)
	if err != nil {
		return err
	}
	poolText := itbench.PooledText(pooled)
	if sum(poolText) != o.services {
		return usage("the pooled service names are not the set named by -services (sha256 %s)", sum(poolText))
	}
	return score(ctx, o.data, o.out, pooled, poolText, stdout)
}

func score(ctx context.Context, data, out string, pooled map[string]bool, poolText string, stdout io.Writer) error {
	scs, err := itbench.Scenarios(data)
	if err != nil {
		return err
	}

	var runs []blastscore.CaseRun
	var notes []string
	for _, dir := range scs {
		res, err := itbench.RunScenario(ctx, dir, pooled, blastscore.Config{})
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(dir), err)
		}
		if res.Unavailable != "" {
			notes = append(notes, fmt.Sprintf("%s: %s", res.Row.ID, res.Unavailable))
		}
		runs = append(runs, res.Run)
	}
	if len(runs) == 0 {
		return errors.New("the dataset has no scenario")
	}
	rep, err := blastscore.Evaluate(runs, blastscore.Meta{Dataset: itbench.Dataset, Loader: factload.Version})
	if err != nil {
		return err
	}
	js, err := rep.JSON()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "report.json"), append(js, '\n'), 0o644); err != nil {
		return err
	}

	var b strings.Builder
	b.WriteString(rep.Text())
	fmt.Fprintf(&b, "\nevaluation parameters: lifetime of an object in a single pull %s (a series of pulls: three intervals), service TTL %s, minute buckets; pod links from spans are address-associated\n",
		factload.ObjectTTL, factload.ServiceTTL)
	fmt.Fprintf(&b, "service names with a span in the trace data read: %d (sha256 %s)\n", len(pooled), sum(poolText))
	fmt.Fprintf(&b, "scenarios in the count: %d; not fully loaded: %d\n", len(runs), len(notes))
	for _, n := range notes {
		fmt.Fprintf(&b, "  not fully loaded: %s\n", n)
	}
	return emit(stdout, filepath.Join(out, "report.txt"), b.String(), "", "")
}

// emit writes text to a file and to the terminal, followed by the sha256 of the
// table if it is one.
func emit(stdout io.Writer, path, text, name, digest string) error {
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		return err
	}
	if _, err := io.WriteString(stdout, text); err != nil {
		return err
	}
	if name != "" {
		_, err := fmt.Fprintf(stdout, "\n%s sha256 %s\n", name, digest)
		return err
	}
	return nil
}

func parseNumbers(s string) ([]int, error) {
	if s == "" {
		return nil, nil
	}
	var out []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("-scenarios: %q is not a scenario number", f)
		}
		out = append(out, n)
	}
	return out, nil
}

func selectRows(rows []itbench.ARow, numbers []int) []itbench.ARow {
	if len(numbers) == 0 {
		return rows
	}
	return slices.DeleteFunc(slices.Clone(rows), func(r itbench.ARow) bool { return !slices.Contains(numbers, r.N) })
}

// outsideRepository refuses an output directory inside a Git checkout: what is
// written there names entities of the dataset and must not be committed.
func outsideRepository(out string) error {
	abs, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	for dir := abs; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return fmt.Errorf("-out %s is inside the repository at %s", out, dir)
		}
		if filepath.Dir(dir) == dir {
			return nil
		}
	}
}
