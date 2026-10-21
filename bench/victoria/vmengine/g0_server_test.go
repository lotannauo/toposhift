package vmengine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmgate"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/identity"
)

// TestServerG0OnAPlan is the first gate on a live server: the every-sample stream of a
// plan (the producers in watch mode re-exported at their scrape interval) is converted,
// written to the server and made readable, the server's counters are checked against the
// converter's, and every question of the plan that a store can be asked is put to the
// server over HTTP.
//
//   - the server's answer must equal the model's for every question (both are the last
//     sample of each series in a window, over the same samples), which checks the
//     assumptions about windows and markers one query at a time;
//   - it must equal the plan's, except where the instant is a boundary or reads a
//     deleted fact (counted, never compared), and the questions the store cannot express
//     are refused as rows of the capability table.
//
// It needs a server with none of the baseline's series: start one on an empty data
// directory.
func TestServerG0OnAPlan(t *testing.T) {
	c := serverClient(t)
	ctx := context.Background()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if has, err := c.HasSeries(ctx, `{__name__=~"topo_.*"}`); err != nil || has {
		t.Fatalf("the server already holds the baseline's series (%v): start one on an empty data directory", err)
	}
	flags, _, err := vmengine.LoadFlags("../scripts/flags.conf")
	if err != nil {
		t.Fatal(err)
	}
	if err := vmengine.CheckFlags(ctx, c, flags); err != nil {
		t.Fatal(err)
	}

	spec := fixture.Spec()
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
	ref := vmmodel.NewWorld(fps...)
	if _, err := runner.Drive(ctx, spec, ref); err != nil {
		t.Fatal(err)
	}

	every, pinned := vmmodel.EverySampleSpec(spec)
	held := vmmodel.NewWorld(fps...)
	model := vmmodel.NewStore()
	var ld *vmengine.Loader
	var conv *vmmodel.Converter
	conv, err = vmmodel.NewConverter(vmmodel.Options{
		Registry: reg, Until: spec.Workload.Start.Add(spec.Workload.Duration),
		Emit: func(ss []vmmodel.Sample) error {
			if err := model.Sink(conv)(ss); err != nil {
				return err
			}
			return ld.Emit(ss)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	start, err := c.Settle(ctx, 500*time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	if ld, err = vmengine.NewLoader(ctx, c, conv, 0); err != nil {
		t.Fatal(err)
	}
	info, err := runner.Drive(ctx, every, vmmodel.Projection(pinned, conv, held))
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ld.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := model.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := ld.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	counts := conv.Counts()
	t.Logf("written: %d samples (%d scraped), %d markers, %d series, %d requests", counts.Samples, counts.Scraped, counts.Markers, counts.Series, ld.Requests)

	// Merged, as it is when measured, every row is stored.
	done, err := c.Compact(ctx, time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if d := done.Stored - start.Stored; d != counts.Samples+counts.Markers {
		t.Errorf("stored %d rows, wrote %d", d, counts.Samples+counts.Markers)
	}

	srv := vmengine.New(c, reg, info.LastSeq)
	mdl := vmquery.NewModelEngine(reg, model, vmmodel.Semantics{}, info.LastSeq)

	// The server answers as the model does, question by question.
	for _, q := range plan.Queries {
		got, gerr := runner.Ask(srv, q)
		want, werr := runner.Ask(mdl, q)
		var ge, we *vmquery.NotExpressibleError
		switch {
		case errors.As(gerr, &ge) || errors.As(werr, &we):
			if !errors.As(gerr, &ge) || !errors.As(werr, &we) || ge.Capability != we.Capability {
				t.Errorf("%s: refused by one and not the other: %v / %v", q.Name(), gerr, werr)
			}
		case gerr != nil || werr != nil:
			t.Errorf("%s: %v / %v", q.Name(), gerr, werr)
		case got != want:
			t.Errorf("%s: the server answered %+v, the model %+v", q.Name(), got, want)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	rep, err := (&vmgate.Gate{Reg: reg, Ref: ref, Store: held, Sem: vmmodel.Semantics{}}).Run(plan.Queries, srv)
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
	if tot := rep.Total(); tot.Asked != len(plan.Queries) || tot.Compared < 300 {
		t.Errorf("%+v for %d questions", tot, len(plan.Queries))
	}
	for _, fam := range []string{"host<-", "node->L1", "service<-L3", "node exists", "pod->", "node<-", "service<-"} {
		if got := rep.ByFamily[fam]; got == nil || got.Compared == 0 {
			t.Errorf("family %s: nothing was compared (%+v)", fam, got)
		}
	}
	if bad := rep.NotCompared(); len(bad) > 0 {
		t.Errorf("not compared at sample resolution: %v", bad)
	}
	if rep.Rows[vmmodel.CapWindow] == 0 || rep.Rows[vmmodel.CapAsKnownAt] == 0 || len(rep.Rows) != 2 {
		t.Errorf("capability rows %v", rep.Rows)
	}
}
