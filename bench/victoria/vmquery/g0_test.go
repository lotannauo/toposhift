package vmquery_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("25")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// built is a plan, and the model of a metrics store written the plan's every-sample
// stream, and the reference's view of the subjects the plan reads.
type built struct {
	plan   *runner.Plan
	reg    *vmmodel.Registry
	world  *vmmodel.World
	store  *vmmodel.Store
	counts vmmodel.Counts
	eng    *vmquery.ModelEngine
}

func build(t *testing.T, spec runner.Spec, sem vmmodel.Semantics) built {
	t.Helper()
	ctx := context.Background()
	reg, err := vmmodel.RegistryFor(spec.Workload)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runner.MakePlan(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The reference's intervals come from the coalesced stream the plan was made from.
	seen := map[identity.Fingerprint]bool{}
	var fps []identity.Fingerprint
	for _, q := range plan.Queries {
		for _, fp := range q.Fps {
			if !seen[fp] {
				seen[fp] = true
				fps = append(fps, fp)
			}
		}
	}
	world := vmmodel.NewWorld(fps...)
	if _, err := runner.Drive(ctx, spec, world); err != nil {
		t.Fatal(err)
	}

	// The store is written the every-sample stream.
	every, _ := vmmodel.EverySampleSpec(spec)
	store := vmmodel.NewStore()
	var conv *vmmodel.Converter
	conv, err = vmmodel.NewConverter(vmmodel.Options{Registry: reg, Emit: func(ss []vmmodel.Sample) error { return store.Sink(conv)(ss) }})
	if err != nil {
		t.Fatal(err)
	}
	info, err := runner.Drive(ctx, every, conv)
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Seal(); err != nil {
		t.Fatal(err)
	}
	return built{plan, reg, world, store, conv.Counts(), vmquery.NewModelEngine(reg, store, sem, info.LastSeq)}
}

type tally struct {
	// notModelled are the questions refused with vmquery.ErrNotModelled: counted apart, not a
	// capability row and not a loss.
	asked, refused, notModelled, boundary, compared, equal int
}

// judge asks every question of the plan of the model engine and sorts the answers as the
// comparison does: a question the store cannot express is a capability row; an instant
// near a change in the reference is a boundary and is not compared; every other answer
// must equal the plan's.
func (b built) judge(t *testing.T, eng engine.Engine, tolerateErrors bool) (groups map[string]*tally, rows map[string]int, wrong []string) {
	t.Helper()
	groups, rows = map[string]*tally{}, map[string]int{}
	for _, q := range b.plan.Queries {
		g := groups[q.Group]
		if g == nil {
			g = &tally{}
			groups[q.Group] = g
		}
		g.asked++
		got, err := runner.Ask(eng, q)
		var nm *vmquery.NotModelledError
		if errors.As(err, &nm) {
			g.notModelled++
			continue
		}
		var ne *vmquery.NotExpressibleError
		if errors.As(err, &ne) {
			g.refused++
			rows[ne.Capability]++
			continue
		}
		if err != nil {
			if !tolerateErrors {
				t.Fatalf("%s: %v", q.Name(), err)
			}
			g.compared++
			wrong = append(wrong, q.Name())
			continue
		}
		// Classify from the reference alone first; the store's answer can only add subjects.
		cls, err := b.world.Classify(b.reg, q, nil)
		if err != nil {
			t.Fatalf("%s: %v", q.Name(), err)
		}
		if !cls.Boundary {
			cls, err = b.world.Classify(b.reg, q, b.returned(t, q, eng))
			if err != nil {
				t.Fatalf("%s: %v", q.Name(), err)
			}
		}
		if cls.Boundary {
			g.boundary++
			continue
		}
		g.compared++
		if got.Digest == q.Expect && got.Size == q.Size {
			g.equal++
		} else {
			wrong = append(wrong, q.Name())
		}
	}
	return groups, rows, wrong
}

// returned is the subjects the store returned for a query.
func (b built) returned(t *testing.T, q runner.Query, eng engine.Engine) []engine.Subject {
	t.Helper()
	sc := engine.Scope{Layer: q.Layer, AsOf: q.AsOf}
	var out []engine.Subject
	switch q.Op {
	case runner.OpNeighbors:
		ns, err := eng.Neighbors(q.Fps[0], q.Dir, q.At, sc)
		if err != nil {
			return nil
		}
		out = vmmodel.NeighborSubjects(q.Fps[0], q.Dir, ns)
	case runner.OpBatch:
		all, err := eng.NeighborsBatch(q.Fps, q.Dir, q.At, sc)
		if err != nil {
			return nil
		}
		for i, ns := range all {
			out = append(out, vmmodel.NeighborSubjects(q.Fps[i], q.Dir, ns)...)
		}
	}
	return out
}

func sum(groups map[string]*tally) tally {
	var tot tally
	for _, g := range groups {
		tot.asked += g.asked
		tot.refused += g.refused
		tot.notModelled += g.notModelled
		tot.boundary += g.boundary
		tot.compared += g.compared
		tot.equal += g.equal
	}
	return tot
}

// TestG0OnTheModel is the first gate, with no server: for a plan made on the registered
// producers and intervals (scaled down), the model of the metrics store, written the
// every-sample stream and asked the rewritten queries, must give the plan's answer to every
// question it can be asked, except at the instants the reference changes near, which are
// counted and not compared.
//
// It rests on the two streams describing the same existence (vmmodel.CheckSameFacts),
// on the windows being the producers' TTLs (the converter refuses a record that says
// otherwise), and on the assumptions about the server listed in package vmmodel, which it
// tests no further than the model makes them.
func TestG0OnTheModel(t *testing.T) {
	cases := []struct {
		name string
		spec runner.Spec
		sem  vmmodel.Semantics
		slow bool
	}{
		{"two days", fixture.Spec(), vmmodel.Semantics{}, false},
		{"two days, a marker ends a window", fixture.Spec(), vmmodel.Semantics{StaleVisible: true}, true},
		{"sixty hours with a retention", fixture.RetainingSpec(), vmmodel.Semantics{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.slow && testing.Short() {
				t.Skip("short")
			}
			b := build(t, c.spec, c.sem)
			groups, rows, wrong := b.judge(t, b.eng, false)
			if len(wrong) > 0 {
				t.Fatalf("%d non-boundary answers differ from the plan's, first: %v", len(wrong), wrong[:min(5, len(wrong))])
			}
			tot := sum(groups)
			if tot.asked != len(b.plan.Queries) || tot.refused+tot.notModelled+tot.boundary+tot.compared != tot.asked || tot.equal != tot.compared {
				t.Errorf("accounting: %+v for %d queries", tot, len(b.plan.Queries))
			}

			// What was refused is exactly what the capability table says the store cannot be
			// asked: windows and reads as of an old token. The questions whose only holder
			// asserts once are not refused as a row: they are tallied apart as "not modelled
			// yet", the label in vmquery.NotModelledLabel, and are not a loss.
			var wantWindow, wantOld, wantNotModelled int
			for _, q := range b.plan.Queries {
				switch {
				case q.Op == runner.OpWindow:
					wantWindow++
				case q.AsOf != engine.Latest:
					wantOld++
				case strings.HasPrefix(q.Group, "service<- ") && q.Layer == catalog.L2:
					wantNotModelled++
				}
			}
			if rows[vmmodel.CapWindow] != wantWindow || rows[vmmodel.CapAsKnownAt] != wantOld || len(rows) != 2 {
				t.Errorf("capability rows %v, want window %d, as-known-at %d and no other", rows, wantWindow, wantOld)
			}
			if tot.notModelled != wantNotModelled || wantNotModelled == 0 {
				t.Errorf("%s: %d, want %d (not zero)", vmquery.NotModelledLabel, tot.notModelled, wantNotModelled)
			}

			// The comparison is not vacuous: every family that can be asked has answers that
			// were compared and were equal, and a good share of the questions were.
			for _, fam := range []string{"host<-", "node->L1", "service<-L3", "node exists", "pod->", "node<-"} {
				var asked, compared int
				for n, g := range groups {
					if strings.HasPrefix(n, fam+" ") {
						asked += g.asked
						compared += g.compared
					}
				}
				if compared == 0 {
					t.Errorf("family %s: nothing was compared (%d asked)", fam, asked)
				}
			}
			if tot.compared < 300 {
				t.Errorf("only %d of %d questions were compared", tot.compared, tot.asked)
			}
		})
	}
}

// tampered is the model engine with its rewriting damaged: it asks the store a different
// question from the one Rewrite wrote.
type tampered struct {
	*vmquery.ModelEngine
	b      built
	mutate func(*vmquery.Plan)
}

func (m tampered) ask(op runner.Op, fps []identity.Fingerprint, d engine.Direction, at time.Time, s engine.Scope) ([]vmquery.Result, error) {
	p, err := vmquery.Rewrite(m.b.reg, runner.Query{Group: "tampered", Op: op, Layer: s.Layer, Dir: d, Fps: fps, Age: "x", At: at, AsOf: s.AsOf})
	if err != nil {
		return nil, err
	}
	m.mutate(&p)
	return vmquery.Evaluate(m.b.store, vmmodel.Semantics{}, p.Expr, p.At)
}

func (m tampered) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	rs, err := m.ask(runner.OpNeighbors, []identity.Fingerprint{fp}, d, at, s)
	if err != nil {
		return nil, err
	}
	return vmquery.Neighbors(rs, fp, d)
}

func (m tampered) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, at time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	rs, err := m.ask(runner.OpBatch, fps, d, at, s)
	if err != nil {
		return nil, err
	}
	return vmquery.NeighborsBatch(rs, fps, d)
}

func (m tampered) Alive(fp identity.Fingerprint, at time.Time, s engine.Scope) (bool, error) {
	rs, err := m.ask(runner.OpAlive, []identity.Fingerprint{fp}, 0, at, s)
	if err != nil {
		return false, err
	}
	return vmquery.Alive(rs, fp)
}

// TestG0CatchesADamagedRewriting shows the gate can fail: each way of writing the queries
// wrong that matters (one window for every producer, a reverse read on the source end, a
// producer left out of the union) gives answers that differ from the plan's at instants
// that are not boundary instants. The same engine undamaged gives none. (A window that is
// too long for a producer differs from the plan only where a producer's refreshes stop
// and a plan's few reads rarely fall, so the exact window of each producer is pinned by
// the test of the rewriting itself.)
func TestG0CatchesADamagedRewriting(t *testing.T) {
	b := build(t, fixture.Spec(), vmmodel.Semantics{})
	damages := map[string]func(*vmquery.Plan){
		"the same window for every producer": func(p *vmquery.Plan) {
			for i := range p.Expr {
				p.Expr[i].Window = 240 * time.Second
			}
		},
		"a reverse read on the source": func(p *vmquery.Plan) {
			for i := range p.Expr {
				switch p.Expr[i].Label {
				case vmmodel.LabelDst:
					p.Expr[i].Label = vmmodel.LabelSrc
				case vmmodel.LabelSrc:
					p.Expr[i].Label = vmmodel.LabelDst
				}
			}
		},
		"a producer's term missing from the union": func(p *vmquery.Plan) { p.Expr = p.Expr[1:] },
		"the producer term dropped from every selector": func(p *vmquery.Plan) {
			for i := range p.Expr {
				p.Expr[i].Producer = "no-such-producer"
			}
		},
	}
	if _, _, wrong := b.judge(t, tampered{b.eng, b, func(*vmquery.Plan) {}}, false); len(wrong) != 0 {
		t.Fatalf("the undamaged engine differs from the plan on %v", wrong[:min(5, len(wrong))])
	}
	for name, mutate := range damages {
		_, _, wrong := b.judge(t, tampered{b.eng, b, mutate}, true)
		if len(wrong) == 0 {
			t.Errorf("%s: no answer differs from the plan's at an instant that is not a boundary", name)
		}
	}
}
