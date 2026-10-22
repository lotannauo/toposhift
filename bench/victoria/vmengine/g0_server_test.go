package vmengine_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/bench/victoria/vmrun"
)

// serverEnv is what the run is checked against: flags.conf, the registered image and, if
// TOPOSHIFT_VM_CONTAINER names the server's container, that it was started from it.
func serverEnv() vmrun.EnvOptions {
	return vmrun.EnvOptions{FlagsPath: "../scripts/flags.conf", ScriptPath: "../scripts/vm-server.sh", Container: os.Getenv("TOPOSHIFT_VM_CONTAINER")}
}

// TestServerG0OnAPlan is the first gate on a live server, made through the run the
// commands of vmspike make: the every-sample stream of a plan (the producers in watch
// mode re-exported at their scrape interval) is converted and written to a server that
// holds none of the baseline's series, after the server's flags, version and (when its
// container is named) image are checked; the server's counters are checked against the
// converter's; the data is merged; and every question of the plan that a store can be
// asked is put to the server over HTTP.
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
	spec := fixture.Spec()
	plan, err := runner.MakePlan(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	in, err := vmrun.PlanInput(plan, "test")
	if err != nil {
		t.Fatal(err)
	}

	load, err := vmrun.Load(ctx, in, c, vmrun.LoadOptions{Env: serverEnv()})
	if err != nil {
		t.Fatal(err)
	}
	counts := load.Counts
	t.Logf("written: %d samples (%d scraped), %d markers, %d series, %d requests", counts.Samples, counts.Scraped, counts.Markers, counts.Series, load.Requests)
	if !load.Env.FlagsChecked || load.Env.Version == "" || load.Env.ImageChecked != (os.Getenv("TOPOSHIFT_VM_CONTAINER") != "") {
		t.Errorf("the server was not checked: %+v", load.Env)
	}
	rows := counts.Samples + counts.Markers
	if load.Server.Received != rows || load.Server.Added != rows || load.Server.Ignored != 0 || load.Server.NewSeries != uint64(counts.Series) {
		t.Errorf("server counters %+v for %d rows in %d series", load.Server, rows, counts.Series)
	}

	// Merged, as it is when measured, every row is stored.
	merge, err := vmrun.Merge(ctx, c, load, vmrun.MergeOptions{Env: serverEnv()})
	if err != nil {
		t.Fatal(err)
	}
	if merge.Stored != rows || merge.After.PendingRows != 0 || merge.After.InMemoryParts != 0 || merge.After.Merging != 0 {
		t.Errorf("merge %+v", merge)
	}
	// The size of the data directory is read only where the test can see it.
	if data := os.Getenv("TOPOSHIFT_VM_DATA"); data != "" {
		sizes, err := vmrun.Sizes(ctx, c, load, merge, data, vmrun.MergeOptions{Env: serverEnv()})
		if err != nil {
			t.Fatal(err)
		}
		if sizes.Dir.Total == 0 || sizes.DataBytes == 0 || sizes.Dir.ByEntry["data"] == 0 {
			t.Errorf("sizes %+v", sizes)
		}
	}

	// The model of the store, written the same samples.
	model := vmmodel.NewStore()
	var conv *vmmodel.Converter
	conv, err = vmmodel.NewConverter(vmmodel.Options{
		Registry: in.Registry, Until: in.Until,
		Emit: func(ss []vmmodel.Sample) error { return model.Sink(conv)(ss) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := in.Every(ctx, conv); err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Seal(); err != nil {
		t.Fatal(err)
	}
	srv := vmengine.New(c, in.Registry, in.LastSeq())
	mdl := vmquery.NewModelEngine(in.Registry, model, vmmodel.Semantics{}, in.LastSeq())

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

	rep, err := vmrun.Read(ctx, in, srv, load.Env)
	if err != nil {
		t.Fatalf("%v\n%s\nfirst: %+v", err, rep.Gate, rep.Gate.Mismatches[:min(5, len(rep.Gate.Mismatches))])
	}
	t.Logf("\n%s", rep.Gate)
	if err := rep.Gate.Accounting(); err != nil {
		t.Error(err)
	}
	if tot := rep.Gate.Total(); tot.Asked != len(plan.Queries) || tot.Compared < 300 {
		t.Errorf("%+v for %d questions", tot, len(plan.Queries))
	}
	for _, fam := range []string{"host<-", "node->L1", "service<-L3", "node exists", "pod->", "node<-", "service<-"} {
		if got := rep.Gate.ByFamily[fam]; got == nil || got.Compared == 0 {
			t.Errorf("family %s: nothing was compared (%+v)", fam, got)
		}
	}
	if len(rep.NotCompared) > 0 {
		t.Errorf("not compared at sample resolution: %v", rep.NotCompared)
	}
	if rep.Gate.Rows[vmmodel.CapWindow] == 0 || rep.Gate.Rows[vmmodel.CapAsKnownAt] == 0 || len(rep.Gate.Rows) != 2 {
		t.Errorf("capability rows %v", rep.Gate.Rows)
	}
}

// TestServerLoadRefusesAServerThatHoldsSeries runs after the load above on the same
// server: a second load is refused, and writes nothing.
func TestServerLoadRefusesAServerThatHoldsSeries(t *testing.T) {
	c := serverClient(t)
	ctx := context.Background()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if has, err := c.HasSeries(ctx, `{__name__=~"topo_.*"}`); err != nil || !has {
		t.Skipf("the server holds none of the baseline's series (%v): this test follows TestServerG0OnAPlan", err)
	}
	before, err := c.Counters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runner.MakePlan(ctx, fixture.Spec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	in, err := vmrun.PlanInput(plan, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vmrun.Load(ctx, in, c, vmrun.LoadOptions{Env: serverEnv()}); !errors.Is(err, vmrun.ErrServerNotEmpty) {
		t.Fatalf("a load onto a server that holds series: %v", err)
	}
	after, err := c.Counters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Received != before.Received || after.Added != before.Added || after.NewSeries != before.NewSeries {
		t.Errorf("a refused load wrote: %+v then %+v", before, after)
	}
}
