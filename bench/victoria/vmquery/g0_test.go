package vmquery_test

import (
	"context"
	"flag"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmgate"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("25")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// built is a plan, the model of a metrics store written the plan's every-sample stream,
// and the reference's two views of the subjects the plan reads: from the stream the plan
// was made from (the coalesced one) and from the one the store was written.
type built struct {
	plan   *runner.Plan
	reg    *vmmodel.Registry
	ref    *vmmodel.World
	held   *vmmodel.World
	store  *vmmodel.Store
	counts vmmodel.Counts
	last   uint64
}

func build(t *testing.T, spec runner.Spec) built {
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
	// The reference's intervals come from the coalesced stream the plan was made from.
	ref := vmmodel.NewWorld(fps...)
	if _, err := runner.Drive(ctx, spec, ref); err != nil {
		t.Fatal(err)
	}

	// The store is written the every-sample stream, which the second world folds too.
	every, pinned := vmmodel.EverySampleSpec(spec)
	held := vmmodel.NewWorld(fps...)
	store := vmmodel.NewStore()
	var conv *vmmodel.Converter
	conv, err = vmmodel.NewConverter(vmmodel.Options{
		Registry: reg, Until: spec.Workload.Start.Add(spec.Workload.Duration),
		Emit: func(ss []vmmodel.Sample) error { return store.Sink(conv)(ss) },
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := runner.Drive(ctx, every, vmmodel.Projection(pinned, conv, held))
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Seal(); err != nil {
		t.Fatal(err)
	}
	return built{plan, reg, ref, held, store, conv.Counts(), info.LastSeq}
}

func (b built) engine(sem vmmodel.Semantics) engine.Engine {
	return vmquery.NewModelEngine(b.reg, b.store, sem, b.last)
}

func (b built) gate(sem vmmodel.Semantics) *vmgate.Gate {
	return &vmgate.Gate{Reg: b.reg, Ref: b.ref, Store: b.held, Sem: sem}
}

// TestG0OnTheModel is the first gate, with no server: for a plan made on the registered
// producers and intervals (scaled down), the model of the metrics store, written the
// every-sample stream (the watch-mode producers re-exported at their scrape interval)
// and asked the rewritten queries, must give the plan's answer to every question it can
// be asked, except at the instants the reference changes at (counted as boundary) and
// where a fact a producer deleted is still read (counted apart).
//
// It rests on the two streams describing the same existence (vmmodel.CheckSameFacts),
// on the windows being the producers' TTLs (the converter refuses a record that says
// otherwise), and on the assumptions about the server listed in package vmmodel, which it
// tests no further than the model makes them; the server's own G0 (package vmengine)
// tests them.
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
			b := build(t, c.spec)
			rep, err := b.gate(c.sem).Run(b.plan.Queries, b.engine(c.sem))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("\n%s", rep)
			if rep.Wrong > 0 {
				t.Fatalf("%d non-boundary answers differ from the plan's, first: %+v", rep.Wrong, rep.Mismatches[:min(5, len(rep.Mismatches))])
			}
			if err := rep.Accounting(); err != nil {
				t.Error(err)
			}
			tot := rep.Total()
			if tot.Asked != len(b.plan.Queries) {
				t.Errorf("%d asked of %d", tot.Asked, len(b.plan.Queries))
			}

			// What was refused is exactly what the capability table says the store cannot be
			// asked: windows and reads as of an old token. Nothing else is refused.
			var wantWindow, wantOld int
			for _, q := range b.plan.Queries {
				switch {
				case q.Op == runner.OpWindow:
					wantWindow++
				case q.AsOf != engine.Latest:
					wantOld++
				}
			}
			if rep.Rows[vmmodel.CapWindow] != wantWindow || rep.Rows[vmmodel.CapAsKnownAt] != wantOld || len(rep.Rows) != 2 {
				t.Errorf("capability rows %v, want window %d, as-known-at %d and no other", rep.Rows, wantWindow, wantOld)
			}

			// The comparison is not vacuous: every family has answers that were compared and
			// were equal, and no family is one that could not be compared at sample
			// resolution.
			for _, fam := range []string{"host<-", "node->L1", "service<-L3", "node exists", "pod->", "node<-", "service<-"} {
				got := rep.ByFamily[fam]
				if got == nil || got.Compared == 0 {
					t.Errorf("family %s: nothing was compared (%+v)", fam, got)
				}
			}
			if bad := rep.NotCompared(); len(bad) > 0 {
				t.Errorf("not compared at sample resolution: %v", bad)
			}
			if tot.Compared < 300 {
				t.Errorf("only %d of %d questions were compared", tot.Compared, tot.Asked)
			}
			// With a marker that ends a window, no deleted fact is read.
			if c.sem.StaleVisible && tot.DeletedFact != 0 {
				t.Errorf("%d reads of a deleted fact though a marker ends a window", tot.DeletedFact)
			}
		})
	}
}

// damaged is a backend whose rewriting is damaged: it asks the store a different question
// from the one Rewrite wrote.
type damaged struct {
	vmquery.ModelBackend
	mutate func(*vmquery.Plan)
}

func (d damaged) Instant(ctx context.Context, p vmquery.Plan) ([]vmquery.Result, error) {
	p.Expr = slices.Clone(p.Expr)
	d.mutate(&p)
	return d.ModelBackend.Instant(ctx, p)
}

// TestG0CatchesADamagedRewriting shows the gate can fail: each way of writing the queries
// wrong that matters (one window for every producer, a reverse read on the source end, a
// producer left out of the union) gives answers that differ from the plan's at instants
// that are not boundary instants. The same engine undamaged gives none. (A window that is
// too long for a producer differs from the plan only where a producer's refreshes stop
// and a plan's few reads rarely fall, so the exact window of each producer is pinned by
// the test of the rewriting itself.)
func TestG0CatchesADamagedRewriting(t *testing.T) {
	b := build(t, fixture.Spec())
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
		"the watch's term missing from the union": func(p *vmquery.Plan) {
			p.Expr = slices.DeleteFunc(p.Expr, func(t vmquery.Term) bool { return t.Producer == "k8sobjects" })
		},
		"the producer term dropped from every selector": func(p *vmquery.Plan) {
			for i := range p.Expr {
				p.Expr[i].Producer = "no-such-producer"
			}
		},
	}
	run := func(mutate func(*vmquery.Plan)) *vmgate.Report {
		eng := vmquery.NewEngine(b.reg, damaged{vmquery.ModelBackend{Store: b.store}, mutate}, b.last)
		rep, err := b.gate(vmmodel.Semantics{}).Run(b.plan.Queries, eng)
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	if rep := run(func(*vmquery.Plan) {}); rep.Wrong != 0 {
		t.Fatalf("the undamaged engine differs from the plan on %+v", rep.Mismatches[:min(5, len(rep.Mismatches))])
	}
	for name, mutate := range damages {
		if rep := run(mutate); rep.Wrong == 0 {
			t.Errorf("%s: no answer differs from the plan's at an instant that is not a boundary", name)
		}
	}
}

// A marker that ends a window where the gate assumes one does not is a model the server
// does not follow: answers that omit what a producer's later sample still holds differ
// from the plan's, and the gate says so only where it compares. Conversely, a store that
// reads deleted facts where the gate is told markers end a window is refused: its extra
// subjects are not explained.
func TestG0DoesNotExplainWhatTheSemanticsDoNotAllow(t *testing.T) {
	b := build(t, fixture.Spec())
	rep, err := (&vmgate.Gate{Reg: b.reg, Ref: b.ref, Store: b.held, Sem: vmmodel.Semantics{StaleVisible: true}}).Run(b.plan.Queries, b.engine(vmmodel.Semantics{}))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total().DeletedFact != 0 {
		t.Errorf("deleted facts were explained under semantics that forbid them: %+v", rep.Total())
	}
	if rep.Wrong == 0 {
		t.Errorf("a store that reads deleted facts passed a gate told markers end a window")
	}
}
