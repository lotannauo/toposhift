// Package vmgate is the first gate of the metrics-store baseline: an engine over a
// metrics store must give the reference engine's answer to every question of a plan that
// the store can be asked, at the resolution of its samples, before any counter of it is
// read.
//
// Each question is sorted, and the sorting is reported by family:
//
//   - refused: the store cannot express it, and the refusal names a row of the capability
//     table ([vmmodel.Capabilities]). It is never a loss, never timed, never in a ratio.
//   - boundary: an involved subject starts or ends within a millisecond of the instant, or
//     the plan's answer comes from a coalesced stream that ended it earlier than the
//     stream the store holds ([vmmodel.World.Classify]). Counted, never compared.
//   - reads a deleted fact: the store returned a subject a producer had deleted less than
//     its window before, because a range function over a window does not see a staleness
//     marker, and everything else in the answer is the reference's. Counted apart, never
//     compared, never hidden as a boundary ([vmmodel.World.ClassifyAnswer]).
//   - compared: the answer must equal the plan's, in digest and size. One that does not
//     makes the arm INVALID.
//
// A family where the questions that are neither refused nor compared exceed half of those
// the store can be asked is reported as "not compared at sample resolution", not as a
// pass ([Report.NotCompared]).
package vmgate

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/identity"
)

// MaxMismatches is how many wrong answers a report lists. The count is exact.
const MaxMismatches = 20

// Gate holds what the sorting needs.
type Gate struct {
	// Reg is the registered table of producers and windows.
	Reg *vmmodel.Registry
	// Ref is the reference's view of the subjects the plan reads, from the stream the
	// plan was made from. Store is the view of the stream the store was written from;
	// nil means the same as Ref.
	Ref, Store *vmmodel.World
	// Sem is the semantics of a window the answers are sorted under: the server's, as
	// verified.
	Sem vmmodel.Semantics
}

// Tally counts the questions of a family.
type Tally struct {
	Asked int
	// Refused are the questions the store cannot express (a row of the capability table).
	Refused int
	// Boundary, DeletedFact and Compared sort the rest; Equal are the compared answers
	// equal to the plan's.
	Boundary, DeletedFact, Compared, Equal int
}

func (t *Tally) add(o Tally) {
	t.Asked += o.Asked
	t.Refused += o.Refused
	t.Boundary += o.Boundary
	t.DeletedFact += o.DeletedFact
	t.Compared += o.Compared
	t.Equal += o.Equal
}

// Mismatch is a compared question the store answered differently from the reference.
type Mismatch struct {
	Query    string
	Got      runner.Answer
	Want     runner.Answer
	AskError string `json:",omitempty"`
}

// Report is the outcome of a run.
type Report struct {
	// Families are the families in the order they were first asked, and ByFamily their
	// tallies.
	Families []string
	ByFamily map[string]*Tally
	// Rows counts the refusals by row of the capability table.
	Rows map[string]int
	// Wrong counts the compared answers that differ; Mismatches lists the first
	// [MaxMismatches].
	Wrong      int
	Mismatches []Mismatch
}

// Family is the family of a group: the group's name up to its first space ("node<-",
// "pod->", "service<-L3", "node exists"), the read the plan's groups are made of.
func Family(group string) string {
	f, _, _ := strings.Cut(group, " ")
	if f == "node" && strings.HasPrefix(group, "node exists") {
		return "node exists"
	}
	return f
}

// Total adds the families.
func (r *Report) Total() Tally {
	var t Tally
	for _, f := range r.Families {
		t.add(*r.ByFamily[f])
	}
	return t
}

// OK reports whether no compared answer differs and every question is accounted for.
func (r *Report) OK() bool { return r.Wrong == 0 && r.Accounting() == nil }

// Accounting checks that every question is in exactly one of the five sorts and that the
// compared ones are equal or wrong.
func (r *Report) Accounting() error {
	for _, name := range r.Families {
		t := r.ByFamily[name]
		if t.Refused+t.Boundary+t.DeletedFact+t.Compared != t.Asked {
			return fmt.Errorf("vmgate: family %s: %d asked but %d refused, %d boundary, %d deleted and %d compared", name, t.Asked, t.Refused, t.Boundary, t.DeletedFact, t.Compared)
		}
	}
	tot := r.Total()
	if tot.Equal+r.Wrong != tot.Compared {
		return fmt.Errorf("vmgate: %d compared but %d equal and %d wrong", tot.Compared, tot.Equal, r.Wrong)
	}
	rows := 0
	for _, n := range r.Rows {
		rows += n
	}
	if rows != tot.Refused {
		return fmt.Errorf("vmgate: %d refused but %d in the capability rows", tot.Refused, rows)
	}
	return nil
}

// NotCompared lists the families where the questions that were neither refused nor
// compared (boundary and reads of a deleted fact) are more than half of those the store
// can be asked. Such a family is "not compared at sample resolution": it is reported so,
// and is not a pass.
func (r *Report) NotCompared() []string {
	var out []string
	for _, name := range r.Families {
		t := r.ByFamily[name]
		if askable := t.Asked - t.Refused; askable > 0 && 2*(t.Boundary+t.DeletedFact) > askable {
			out = append(out, name)
		}
	}
	return out
}

// String is the report as a table, one line a family.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-14s %7s %8s %9s %8s %9s %6s\n", "family", "asked", "refused", "boundary", "deleted", "compared", "equal")
	line := func(name string, t Tally) {
		fmt.Fprintf(&b, "%-14s %7d %8d %9d %8d %9d %6d\n", name, t.Asked, t.Refused, t.Boundary, t.DeletedFact, t.Compared, t.Equal)
	}
	for _, name := range r.Families {
		line(name, *r.ByFamily[name])
	}
	line("total", r.Total())
	rows := make([]string, 0, len(r.Rows))
	for id := range r.Rows {
		rows = append(rows, id)
	}
	slices.Sort(rows)
	for _, id := range rows {
		fmt.Fprintf(&b, "capability row %q: %d refused\n", id, r.Rows[id])
	}
	fmt.Fprintf(&b, "wrong: %d\n", r.Wrong)
	return b.String()
}

// recorder keeps what the engine returned for the last read, so that the subjects it
// stands for can be classified without asking again.
type recorder struct {
	engine.Engine
	neighbors [][]engine.Neighbor
	alive     bool
}

func (r *recorder) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	ns, err := r.Engine.Neighbors(fp, d, t, s)
	r.neighbors = [][]engine.Neighbor{ns}
	return ns, err
}

func (r *recorder) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	all, err := r.Engine.NeighborsBatch(fps, d, t, s)
	r.neighbors = all
	return all, err
}

func (r *recorder) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	ok, err := r.Engine.Alive(fp, t, s)
	r.alive = ok
	return ok, err
}

// returned is the subjects the last answer stands for.
func (r *recorder) returned(q runner.Query) []engine.Subject {
	switch q.Op {
	case runner.OpAlive:
		if r.alive {
			return []engine.Subject{engine.EntitySubject(q.Fps[0])}
		}
	case runner.OpNeighbors, runner.OpBatch:
		var out []engine.Subject
		for i, ns := range r.neighbors {
			out = append(out, vmmodel.NeighborSubjects(q.Fps[i], q.Dir, ns)...)
		}
		return out
	}
	return nil
}

// Run asks every question of the plan of the engine and sorts the answers. An error of
// the engine that is not a refusal naming a capability row is a compared answer that is
// wrong. Run itself fails only if a question cannot be classified.
func (g *Gate) Run(queries []runner.Query, e engine.Engine) (*Report, error) {
	store := g.Store
	if store == nil {
		store = g.Ref
	}
	rep := &Report{ByFamily: map[string]*Tally{}, Rows: map[string]int{}}
	for _, q := range queries {
		fam := Family(q.Group)
		t := rep.ByFamily[fam]
		if t == nil {
			t = &Tally{}
			rep.ByFamily[fam] = t
			rep.Families = append(rep.Families, fam)
		}
		t.Asked++
		rec := &recorder{Engine: e}
		got, err := runner.Ask(rec, q)
		var ne *vmquery.NotExpressibleError
		if errors.As(err, &ne) {
			t.Refused++
			rep.Rows[ne.Capability]++
			continue
		}
		want := runner.Answer{Digest: q.Expect, Size: q.Size}
		if err != nil {
			t.Compared++
			rep.wrong(Mismatch{Query: q.Name(), Want: want, AskError: err.Error()})
			continue
		}
		cls, err := g.Ref.ClassifyAnswer(g.Reg, store, g.Sem, q, rec.returned(q))
		if err != nil {
			return nil, fmt.Errorf("vmgate: %s: %w", q.Name(), err)
		}
		switch cls.Kind {
		case vmmodel.Boundary:
			t.Boundary++
		case vmmodel.ReadsDeletedFact:
			t.DeletedFact++
		default:
			t.Compared++
			if got == want {
				t.Equal++
			} else {
				rep.wrong(Mismatch{Query: q.Name(), Got: got, Want: want})
			}
		}
	}
	return rep, nil
}

func (r *Report) wrong(m Mismatch) {
	r.Wrong++
	if len(r.Mismatches) < MaxMismatches {
		r.Mismatches = append(r.Mismatches, m)
	}
}
