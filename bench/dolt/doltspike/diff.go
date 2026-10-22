package doltspike

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
)

// ErrNotComparable is wrapped by the error [Diff] returns for two records that are not of
// one kind, one stream, one arm and one server: a difference between them would say nothing
// about the design.
var ErrNotComparable = errors.New("doltspike: the records are not comparable")

// Difference is one way two comparable records differ, by category: "result" for what the
// design did (answers, counts, commits, plans), "bytes" for the size of the files, which
// the file layout may move even when nothing else does, and "binary" for the program that
// made the record.
type Difference struct {
	Category string
	What     string
}

// Diff compares two records of the same kind (read from JSON). It refuses, with
// [ErrNotComparable], two that are not of one kind, one stream (or question set), one arm,
// one database name's design and one server (image, settings, Dolt version, driver, schema);
// it lists what else differs.
func Diff(a, b []byte) ([]Difference, error) {
	var ka, kb struct {
		Kind   string
		Format int
	}
	if err := json.Unmarshal(a, &ka); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &kb); err != nil {
		return nil, err
	}
	if ka != kb || ka.Format != RecordFormat {
		return nil, fmt.Errorf("%w: kinds %+v and %+v", ErrNotComparable, ka, kb)
	}
	switch ka.Kind {
	case KindLoad:
		return diffAs[LoadRecord](a, b, diffLoad)
	case KindRead:
		return diffAs[ReadRecord](a, b, diffRead)
	case KindSizes:
		return diffAs[SizesRecord](a, b, diffSizes)
	case KindExplain:
		return diffAs[ExplainRecord](a, b, diffExplain)
	}
	return nil, fmt.Errorf("doltspike: no record kind %q", ka.Kind)
}

func diffAs[R any](a, b []byte, f func(x, y *R) ([]Difference, error)) ([]Difference, error) {
	var x, y R
	if err := json.Unmarshal(a, &x); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return nil, err
	}
	return f(&x, &y)
}

// comparableRecords refuses records of different streams, arms or servers.
func comparableRecords(ex, ey Env, inx, iny Input, ax, ay string) ([]Difference, error) {
	if !sameInput(inx, iny) {
		return nil, fmt.Errorf("%w: streams %+v and %+v", ErrNotComparable, inx, iny)
	}
	if ax != ay {
		return nil, fmt.Errorf("%w: arms %s and %s", ErrNotComparable, ax, ay)
	}
	bx, by := ex, ey
	bx.Commit, bx.Modified, bx.BinarySHA256, bx.GoVersion = "", "", "", ""
	by.Commit, by.Modified, by.BinarySHA256, by.GoVersion = "", "", "", ""
	if bx != by {
		return nil, fmt.Errorf("%w: servers or settings differ: %+v and %+v", ErrNotComparable, bx, by)
	}
	var out []Difference
	if ex.Commit != ey.Commit || ex.Modified != ey.Modified || ex.BinarySHA256 != ey.BinarySHA256 || ex.GoVersion != ey.GoVersion {
		out = append(out, Difference{"binary", fmt.Sprintf("the programs differ: commit %s (modified %s, sha256 %s, %s) and commit %s (modified %s, sha256 %s, %s)",
			ex.Commit, ex.Modified, ex.BinarySHA256, ex.GoVersion, ey.Commit, ey.Modified, ey.BinarySHA256, ey.GoVersion)})
	}
	return out, nil
}

func field(out *[]Difference, cat, name string, x, y any) {
	if !reflect.DeepEqual(x, y) {
		*out = append(*out, Difference{cat, fmt.Sprintf("%s: %v, then %v", name, x, y)})
	}
}

func diffLoad(x, y *LoadRecord) ([]Difference, error) {
	out, err := comparableRecords(x.Env, y.Env, x.Input, y.Input, x.Arm, y.Arm)
	if err != nil {
		return nil, err
	}
	field(&out, "result", "writer counts", x.Counts, y.Counts)
	field(&out, "result", "commits", x.Commits, y.Commits)
	field(&out, "result", "retentions", x.Retentions, y.Retentions)
	field(&out, "result", "head commit hash", x.HeadHash, y.HeadHash)
	field(&out, "result", "frontiers", x.FrontiersSHA256, y.FrontiersSHA256)
	field(&out, "result", "capability rows", x.Capabilities, y.Capabilities)
	return out, nil
}

func diffRead(x, y *ReadRecord) ([]Difference, error) {
	out, err := comparableRecords(x.Env, y.Env, x.Input, y.Input, x.Arm, y.Arm)
	if err != nil {
		return nil, err
	}
	field(&out, "result", "head commit hash", x.HeadHash, y.HeadHash)
	field(&out, "result", "questions matched", x.Matched, y.Matched)
	if len(x.Results) != len(y.Results) {
		out = append(out, Difference{"result", fmt.Sprintf("questions: %d, then %d", len(x.Results), len(y.Results))})
		return out, nil
	}
	n := 0
	for i := range x.Results {
		if !reflect.DeepEqual(x.Results[i], y.Results[i]) {
			if n < 10 {
				out = append(out, Difference{"result", fmt.Sprintf("question %d (%s #%d at %s): %+v, then %+v", i+1, x.Results[i].Group, x.Results[i].Rank, x.Results[i].Age, x.Results[i], y.Results[i])})
			}
			n++
		}
	}
	if n > 10 {
		out = append(out, Difference{"result", fmt.Sprintf("and %d more questions", n-10)})
	}
	return out, nil
}

func diffSizes(x, y *SizesRecord) ([]Difference, error) {
	out, err := comparableRecords(x.Env, y.Env, x.Input, y.Input, x.Arm, y.Arm)
	if err != nil {
		return nil, err
	}
	field(&out, "result", "records held", x.RecordsHeld, y.RecordsHeld)
	field(&out, "result", "commits", x.Commits, y.Commits)
	field(&out, "result", "head commit hash", x.HeadHash, y.HeadHash)
	field(&out, "bytes", "bytes as left", x.AsLeft, y.AsLeft)
	field(&out, "bytes", "bytes after garbage collection", x.AfterGC, y.AfterGC)
	return out, nil
}

func diffExplain(x, y *ExplainRecord) ([]Difference, error) {
	out, err := comparableRecords(x.Env, y.Env, x.Input, y.Input, x.Arm, y.Arm)
	if err != nil {
		return nil, err
	}
	if len(x.Queries) != len(y.Queries) {
		out = append(out, Difference{"result", fmt.Sprintf("questions explained: %d, then %d", len(x.Queries), len(y.Queries))})
		return out, nil
	}
	for i := range x.Queries {
		if !reflect.DeepEqual(x.Queries[i], y.Queries[i]) {
			out = append(out, Difference{"result", fmt.Sprintf("the plans of question %d (%s #%d at %s) differ", i+1, x.Queries[i].Group, x.Queries[i].Rank, x.Queries[i].Age)})
		}
	}
	return out, nil
}

func runDiff(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("diff", stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: doltspike diff a.json b.json")
	}
	a, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	b, err := os.ReadFile(fs.Arg(1))
	if err != nil {
		return err
	}
	diffs, err := Diff(a, b)
	if err != nil {
		return err
	}
	if len(diffs) == 0 {
		fmt.Fprintln(stdout, "the records are equal")
		return nil
	}
	results := 0
	for _, d := range diffs {
		fmt.Fprintf(stdout, "DIFFERENT (%s) %s\n", d.Category, d.What)
		if d.Category == "result" {
			results++
		}
	}
	if results > 0 {
		return fmt.Errorf("%d results differ: the two are not repeatable", results)
	}
	return fmt.Errorf("%d differences, none in a result", len(diffs))
}
