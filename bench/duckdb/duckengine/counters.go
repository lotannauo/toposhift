package duckengine

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// ScanCounters is what one table scan of a read cost. RowsScanned is the work counter:
// the rows of the row groups the scan admitted, from the file's footer. The profiler's
// own figure for the scan, which is the rows that passed its filters, is recorded beside
// it as ProfilerCardinality; the profiler's count of the rows it was handed is the whole
// file's whatever row groups were skipped, and is not kept.
type ScanCounters struct {
	Role                Role
	RowsScanned         int64
	ProfilerCardinality int64
	Admission
}

// QueryCounters is what one question of a plan cost.
type QueryCounters struct {
	Name      string
	Group     string
	Age       string
	Op        runner.Op
	Statement string
	Answer    runner.Answer
	// PlanShape is the plan DuckDB ran, and ProfileDigest the SHA-256 of the profile
	// with everything that is not a count stripped.
	PlanShape     string
	ProfileDigest string
	Scans         []ScanCounters
}

// ErrNotRepeatable is wrapped by the error [Collect] returns when two passes over the
// questions do not cost the same.
var ErrNotRepeatable = errors.New("duckengine: two reads of one question cost differently")

// ErrWrongAnswer is wrapped by the error [Collect] returns for an answer that is not the
// plan's, so that no counter is kept of a read that is wrong.
var ErrWrongAnswer = errors.New("duckengine: an answer differs from the plan's")

// CollectPass asks every question of the plan once and returns what each cost. The
// reader's connection must profile. An answer that is not the plan's is an error: no
// counter is kept of a wrong read.
func CollectPass(ctx context.Context, r *Reader, plan *runner.Plan) ([]QueryCounters, error) {
	if !r.db.cfg.Profile {
		return nil, errors.New("duckengine: counting needs a connection that profiles")
	}
	out := make([]QueryCounters, 0, len(plan.Queries))
	for _, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ans, err := runner.Ask(r, q)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", q.Name(), err)
		}
		if want := (runner.Answer{Digest: q.Expect, Size: q.Size}); ans != want {
			return nil, fmt.Errorf("%w: %s answered %+v, the plan has %+v", ErrWrongAnswer, q.Name(), ans, want)
		}
		tr := r.Trace()
		qc := QueryCounters{
			Name: q.Name(), Group: q.Group, Age: q.Age, Op: q.Op, Statement: tr.Statement, Answer: ans,
			PlanShape: tr.Profile.Shape(), ProfileDigest: tr.Profile.Digest(),
		}
		scans := tr.Profile.Scans()
		if len(scans) != len(tr.Admissions) {
			return nil, fmt.Errorf("duckengine: %s: the plan has %d table scans and the statement was counted for %d files", q.Name(), len(scans), len(tr.Admissions))
		}
		for i, s := range scans {
			qc.Scans = append(qc.Scans, ScanCounters{Role: tr.Admissions[i].Role, RowsScanned: tr.Admissions[i].Admitted.Rows, ProfilerCardinality: s.Cardinality, Admission: tr.Admissions[i]})
		}
		out = append(out, qc)
	}
	return out, nil
}

// Collect asks every question twice and returns what each cost, refusing a pass whose
// answers are not the plan's and two passes that do not cost the same: the same plan, the
// same stripped profile, the same rows and the same row groups and bytes.
func Collect(ctx context.Context, r *Reader, plan *runner.Plan) ([]QueryCounters, error) {
	first, err := CollectPass(ctx, r, plan)
	if err != nil {
		return nil, err
	}
	second, err := CollectPass(ctx, r, plan)
	if err != nil {
		return nil, err
	}
	if len(first) != len(second) {
		return nil, fmt.Errorf("%w: %d questions, then %d", ErrNotRepeatable, len(first), len(second))
	}
	for i := range first {
		if !equalCounters(first[i], second[i]) {
			return nil, fmt.Errorf("%w: %s: %+v, then %+v", ErrNotRepeatable, first[i].Name, first[i], second[i])
		}
	}
	return first, nil
}

func equalCounters(a, b QueryCounters) bool {
	return a.Name == b.Name && a.Statement == b.Statement && a.Answer == b.Answer && a.PlanShape == b.PlanShape &&
		a.ProfileDigest == b.ProfileDigest && slices.EqualFunc(a.Scans, b.Scans, func(x, y ScanCounters) bool {
		return x.Role == y.Role && x.RowsScanned == y.RowsScanned && x.ProfilerCardinality == y.ProfilerCardinality && x.Same(y.Admission)
	})
}

// GroupCounters sums the counters of the questions of one group at one age.
type GroupCounters struct {
	Group   string
	Age     string
	Queries int
	// The work counters, summed over the group's questions and their scans: rows scanned
	// (the rows of the admitted row groups), the row groups admitted out of the files'
	// total, and the bytes of the columns the reads touch in them.
	RowsScanned                 int64
	GroupsAdmitted, GroupsTotal int
	BytesAdmitted               int64
	// ProfilerCardinality is the rows the profiler says the scans returned, beside them.
	ProfilerCardinality int64
	// The diagnostic: what counting by the footers' bounds alone would have said. It is
	// in no slope class.
	GroupsBoundsOnly                int
	RowsBoundsOnly, BytesBoundsOnly int64
	// Bound says the group is of batch reads, whose counts are an upper bound: it is in
	// G0 and in no slope class.
	Bound bool
}

// Summarize sums the counters by group and age, in the order they first appear.
func Summarize(qs []QueryCounters) []GroupCounters {
	var out []GroupCounters
	idx := map[[2]string]int{}
	for _, q := range qs {
		k := [2]string{q.Group, q.Age}
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, GroupCounters{Group: q.Group, Age: q.Age})
		}
		g := &out[i]
		g.Queries++
		for _, s := range q.Scans {
			g.RowsScanned += s.RowsScanned
			g.ProfilerCardinality += s.ProfilerCardinality
			g.GroupsTotal += s.Total.Groups
			g.GroupsAdmitted += s.Admitted.Groups
			g.BytesAdmitted += s.Admitted.Bytes
			g.GroupsBoundsOnly += s.BoundsOnly.Groups
			g.RowsBoundsOnly += s.BoundsOnly.Rows
			g.BytesBoundsOnly += s.BoundsOnly.Bytes
			g.Bound = g.Bound || s.Bound
		}
	}
	return out
}

// CountersFormat is the version of [CountersFile].
const CountersFormat = 1

// PlanRef names the plan a counters file is of.
type PlanRef struct {
	SHA256        string
	StreamDigest  string
	QueriesDigest string
	Queries       int
}

// CountersFile is what `duckspike read` writes for an arm: the environment, the plan, the
// build, and the counters of every question and group. It holds no time.
type CountersFile struct {
	Format         int
	Env            Env
	Plan           PlanRef
	Arm            Arm
	ManifestSHA256 string
	// Passes is how many times each question was asked; the counters of every pass were
	// equal.
	Passes  int
	Groups  []GroupCounters
	Queries []QueryCounters
}
