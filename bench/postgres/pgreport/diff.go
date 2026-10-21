package pgreport

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrNotComparable is wrapped by the error a Diff returns for two records that cannot be
// held to each other: they are of other streams, other questions, another arm, another
// server build, other settings or another block size. Their numbers would differ for
// reasons that say nothing about whether a run can be repeated.
var ErrNotComparable = errors.New("pgreport: the records are not of one stream on one server")

// The categories of a difference. Everything but the log is a function of the stream and
// the build, so any difference in it means two runs disagree; the log is counted by a
// server whose background processes and page headers move it a little (see
// [Options.WALTolerance]), and has a category of its own.
const (
	CatAnswer  = "answer"
	CatCounter = "counter"
	CatShape   = "shape"
	CatBytes   = "bytes"
	CatRows    = "rows"
	CatDigest  = "digest"
	CatWAL     = "wal"
)

// Difference is one thing two records say differently.
type Difference struct {
	Category string
	Where    string
	A, B     string
}

func (d Difference) String() string {
	return fmt.Sprintf("%s: %s: %s, then %s", d.Category, d.Where, d.A, d.B)
}

// Options says how much two records may differ in the log they report.
type Options struct {
	// WALTolerance is the largest relative difference of a phase's log counts that is
	// not a difference. Zero compares them exactly. The log of a load moves by a few
	// kilobytes between two runs of one stream (a page that a background writer held
	// when a COPY wanted it, the statistics ANALYZE draws at random, catalog pages in the
	// state the earlier loads left them): nothing in the stream or the settings.
	WALTolerance float64
}

// Comparison is what a comparison found.
type Comparison struct {
	Differences []Difference
	// Skipped are the comparisons not made, and why: a phase that had a checkpoint
	// inside it in either run puts full-page images in its log that the other run may
	// not have, so its log counts are not compared.
	Skipped []string
	// EnvDiffers names the fields of the environment that differ but that do not make
	// the runs incomparable: the binary and the processor.
	EnvDiffers []string
}

// Equal reports whether the runs agree.
func (r Comparison) Equal() bool { return len(r.Differences) == 0 }

func (r *Comparison) add(cat, where string, a, b any) {
	r.Differences = append(r.Differences, Difference{Category: cat, Where: where, A: fmt.Sprint(a), B: fmt.Sprint(b)})
}

func (r *Comparison) cmp(cat, where string, a, b any) {
	if a != b {
		r.add(cat, where, a, b)
	}
}

// checkEnv refuses records of other servers and settings, and lists what differs among
// the rest.
func checkEnv(a, b Env, r *Comparison) error {
	var bad []string
	for _, f := range []struct {
		name string
		a, b any
	}{
		{"settings", a.SettingsSHA256, b.SettingsSHA256},
		{"image", a.Image, b.Image},
		{"server version", a.ServerVersion, b.ServerVersion},
		{"server build options", a.Configure, b.Configure},
		{"block size", a.BlockSize, b.BlockSize},
		{"WAL block size", a.WALBlockSize, b.WALBlockSize},
		{"data checksums", a.DataChecksums, b.DataChecksums},
	} {
		if f.a != f.b {
			bad = append(bad, fmt.Sprintf("%s (%v, then %v)", f.name, f.a, f.b))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w: %s", ErrNotComparable, strings.Join(bad, "; "))
	}
	for _, f := range []struct {
		name string
		a, b string
	}{
		{"commit", a.Commit, b.Commit},
		{"modified", a.Modified, b.Modified},
		{"binary", a.BinarySHA256, b.BinarySHA256},
		{"go version", a.GoVersion, b.GoVersion},
		{"os", a.GOOS, b.GOOS},
		{"arch", a.GOARCH, b.GOARCH},
		{"server cpu", a.ServerCPU, b.ServerCPU},
	} {
		if f.a != f.b {
			r.EnvDiffers = append(r.EnvDiffers, f.name)
		}
	}
	return nil
}

func checkInput(a, b Input, arm1, arm2 string) error {
	var bad []string
	if arm1 != arm2 {
		bad = append(bad, fmt.Sprintf("arm (%s, then %s)", arm1, arm2))
	}
	for _, f := range []struct {
		name string
		a, b any
	}{
		{"kind", a.Kind, b.Kind},
		{"plan", a.PlanSHA256, b.PlanSHA256},
		{"activity content", a.ContentDigest, b.ContentDigest},
		{"questions", a.QuestionsSHA256, b.QuestionsSHA256},
		{"stream", a.StreamDigest, b.StreamDigest},
		{"queries", a.QueriesDigest, b.QueriesDigest},
		{"records", a.Records, b.Records},
		{"last seq", a.LastSeq, b.LastSeq},
	} {
		if f.a != f.b {
			bad = append(bad, fmt.Sprintf("%s (%v, then %v)", f.name, f.a, f.b))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w: %s", ErrNotComparable, strings.Join(bad, "; "))
	}
	return nil
}

// within reports whether two counts differ by no more than the tolerance of the larger.
func within(a, b int64, tol float64, slack int64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= slack || float64(d) <= tol*float64(max(a, b))
}

// DiffLoads compares two loads. The tables must be the same by digest and by row count,
// every relation the same size in every fork, and each phase's log the same within the
// tolerance. A GiST index is not the same twice: the server chooses at random among
// equally good pages as it builds one, so two builds of one table differ in size (by a few
// percent) and in the log they write, and the comparison says so, as it says everything
// else that differs; a btree is built the same every time.
func DiffLoads(a, b Load, o Options) (Comparison, error) {
	var r Comparison
	if err := checkEnv(a.Env, b.Env, &r); err != nil {
		return r, err
	}
	if err := checkInput(a.Input, b.Input, a.Arm, b.Arm); err != nil {
		return r, err
	}
	r.cmp(CatRows, "retentions", a.Retentions, b.Retentions)
	r.cmp(CatRows, "horizon", a.HorizonNS, b.HorizonNS)
	r.cmp(CatRows, "past end", a.PastEnd, b.PastEnd)
	r.cmp(CatRows, "stream vacuums", a.StreamVacuums, b.StreamVacuums)
	r.cmp(CatRows, "log rows", a.Rows.Log, b.Rows.Log)
	r.cmp(CatRows, "baseline rows", a.Rows.LogBase, b.Rows.LogBase)
	r.cmp(CatRows, "edge rows", a.Rows.Edges, b.Rows.Edges)
	r.cmp(CatRows, "entity rows", a.Rows.Entities, b.Rows.Entities)
	r.cmp(CatRows, "dictionary rows", a.Rows.Dictionary, b.Rows.Dictionary)
	r.cmp(CatDigest, "interval tables", a.TablesDigest, b.TablesDigest)
	r.cmp(CatRows, "interval table rows", a.TablesRows, b.TablesRows)
	diffSizes(a.Sizes, b.Sizes, "", &r)

	byName := func(ps []Phase) map[string]Phase {
		m := map[string]Phase{}
		for _, p := range ps {
			m[p.Name] = p
		}
		return m
	}
	pa, pb := byName(a.Phases), byName(b.Phases)
	var names []string
	for n := range pa {
		names = append(names, n)
	}
	for n := range pb {
		if _, ok := pa[n]; !ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	for _, n := range names {
		x, okx := pa[n]
		y, oky := pb[n]
		if !okx || !oky {
			r.add(CatWAL, "phase "+n, okx, oky)
			continue
		}
		if n == "vacuum" && a.ExtraVacuums != b.ExtraVacuums {
			r.Skipped = append(r.Skipped, fmt.Sprintf("phase vacuum: a table needed more vacuums in one run than in the other (%d extra, then %d)", a.ExtraVacuums, b.ExtraVacuums))
			continue
		}
		if x.CheckpointsInside != 0 || y.CheckpointsInside != 0 {
			r.Skipped = append(r.Skipped, fmt.Sprintf("phase %s: a checkpoint ran inside it (%d, then %d)", n, x.CheckpointsInside, y.CheckpointsInside))
			continue
		}
		for _, f := range []struct {
			name  string
			a, b  int64
			slack int64
		}{
			{"log records", x.Records, y.Records, 0},
			{"full-page images", x.FPIs, y.FPIs, 0},
			{"record bytes", x.RecordBytes, y.RecordBytes, 0},
			// A page header or two, which where the phase began in its page decides.
			{"log position", x.LogBytes, y.LogBytes, 64},
		} {
			if !within(f.a, f.b, o.WALTolerance, f.slack) {
				r.add(CatWAL, "phase "+n+" "+f.name, f.a, f.b)
			}
		}
	}
	return r, nil
}

func diffSizes(a, b Sizes, prefix string, r *Comparison) {
	rel := func(rs []Relation) map[string]Relation {
		m := map[string]Relation{}
		for _, x := range rs {
			m[x.Name] = x
		}
		return m
	}
	ra, rb := rel(a.Relations), rel(b.Relations)
	var names []string
	for n := range ra {
		names = append(names, n)
	}
	for n := range rb {
		if _, ok := ra[n]; !ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	for _, n := range names {
		x, okx := ra[n]
		y, oky := rb[n]
		where := prefix + "relation " + n
		switch {
		case !okx || !oky:
			r.add(CatBytes, where, okx, oky)
		default:
			r.cmp(CatBytes, where+" main fork", x.Main, y.Main)
			r.cmp(CatBytes, where+" free space map", x.FSM, y.FSM)
			r.cmp(CatBytes, where+" visibility map", x.VM, y.VM)
		}
	}
}

// DiffReads compares two records of the reads of one plan on two loads: the same
// questions, each answered the same, each statement planned the same and counting the
// same blocks, entries and rows.
func DiffReads(a, b Read) (Comparison, error) {
	var r Comparison
	if err := checkEnv(a.Env, b.Env, &r); err != nil {
		return r, err
	}
	if err := checkInput(a.Input, b.Input, a.Arm, b.Arm); err != nil {
		return r, err
	}
	if len(a.Results) != len(b.Results) {
		return r, fmt.Errorf("%w: %d results, then %d", ErrNotComparable, len(a.Results), len(b.Results))
	}
	if len(a.Mismatches) > 0 || len(b.Mismatches) > 0 {
		return r, fmt.Errorf("%w: a record with answers that differ from the reference's has no counters to compare", ErrNotComparable)
	}
	for i := range a.Results {
		x, y := a.Results[i], b.Results[i]
		if x.Name != y.Name || x.Op != y.Op || x.Group != y.Group || x.Age != y.Age {
			return r, fmt.Errorf("%w: result %d is %q, then %q", ErrNotComparable, i, x.Name, y.Name)
		}
		where := x.Name + " (" + x.Op + ")"
		r.cmp(CatAnswer, where+" digest", x.Digest, y.Digest)
		r.cmp(CatAnswer, where+" size", x.Size, y.Size)
		r.cmp(CatAnswer, where+" naive", x.Naive, y.Naive)
		r.cmp(CatCounter, where+" rows fetched", x.RowsFetched, y.RowsFetched)
		if len(x.Runs) != len(y.Runs) {
			r.add(CatShape, where+" statements", len(x.Runs), len(y.Runs))
			continue
		}
		for j := range x.Runs {
			sx, sy := x.Runs[j], y.Runs[j]
			w := fmt.Sprintf("%s statement %d", where, j)
			r.cmp(CatShape, w+" text", stmt(a, sx.Statement), stmt(b, sy.Statement))
			r.cmp(CatShape, w+" plan", sx.Shape, sy.Shape)
			r.cmp(CatCounter, w+" shared hit", sx.Counters.SharedHit, sy.Counters.SharedHit)
			r.cmp(CatCounter, w+" shared read", sx.Counters.SharedRead, sy.Counters.SharedRead)
			r.cmp(CatCounter, w+" rows returned", sx.Counters.RowsReturned, sy.Counters.RowsReturned)
			r.cmp(CatCounter, w+" index entries", sx.Counters.IndexEntries, sy.Counters.IndexEntries)
			r.cmp(CatCounter, w+" heap fetches", sx.Counters.HeapFetches, sy.Counters.HeapFetches)
			r.cmp(CatCounter, w+" rows removed by filter", sx.Counters.RowsRemovedByFilter, sy.Counters.RowsRemovedByFilter)
			r.cmp(CatCounter, w+" rows removed by recheck", sx.Counters.RowsRemovedByRecheck, sy.Counters.RowsRemovedByRecheck)
		}
		r.cmp(CatCounter, where+" total", x.Total, y.Total)
		r.cmp(CatBytes, where+" bytes read", x.BytesRead, y.BytesRead)
		r.cmp(CatBytes, where+" bytes touched", x.BytesTouched, y.BytesTouched)
	}
	return r, nil
}

func stmt(r Read, i int) string {
	if i < 0 || i >= len(r.Statements) {
		return fmt.Sprintf("<statement %d is not listed>", i)
	}
	return r.Statements[i]
}

// DiffSizes compares two records of sizes: every relation, in every fork, as left and, if
// both have it, compacted.
func DiffSizes(a, b SizesReport) (Comparison, error) {
	var r Comparison
	if err := checkEnv(a.Env, b.Env, &r); err != nil {
		return r, err
	}
	if a.Arm != b.Arm || a.Records != b.Records {
		return r, fmt.Errorf("%w: arm %s with %d records, then arm %s with %d", ErrNotComparable, a.Arm, a.Records, b.Arm, b.Records)
	}
	diffSizes(a.AsLeft, b.AsLeft, "as left: ", &r)
	switch {
	case (a.Compacted == nil) != (b.Compacted == nil):
		r.add(CatBytes, "compacted sizes", a.Compacted != nil, b.Compacted != nil)
	case a.Compacted != nil:
		diffSizes(*a.Compacted, *b.Compacted, "compacted: ", &r)
	}
	return r, nil
}
