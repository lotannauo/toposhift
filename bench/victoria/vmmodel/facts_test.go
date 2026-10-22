package vmmodel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The premise the whole comparison rests on: the coalesced stream the plans' expected
// answers come from, and the every-sample stream written to the store, describe the same
// existence, except that the coalescer's bounded extension may end an interval earlier.
func TestTheTwoStreamsDescribeTheSameExistence(t *testing.T) {
	specs := map[string]runner.Spec{"no retention": fixture.Spec(), "a retention": fixture.RetainingSpec()}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			reg, err := vmmodel.RegistryFor(spec.Workload)
			if err != nil {
				t.Fatal(err)
			}
			rep, err := vmmodel.CheckSameFacts(context.Background(), spec, reg)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("subjects %d, identical %d, ends earlier %d, at most %s", rep.Subjects, rep.Identical, rep.EndsEarlier, rep.MaxEarly)
			if rep.Subjects < 1000 || rep.Identical+rep.EndsEarlier != rep.Subjects {
				t.Errorf("report %+v", rep)
			}
			// The coalescer's bound is half a window, so no interval of a subject held by the
			// pod collector may end more than half an hour earlier, and none of a node more
			// than two minutes.
			if rep.MaxEarly >= 30*time.Minute {
				t.Errorf("an interval ends %s earlier", rep.MaxEarly)
			}
		})
	}
}

func TestEverySampleSpec(t *testing.T) {
	spec := fixture.Spec()
	if !spec.Workload.CoalesceRuns || spec.Workload.ExtendTTLFraction == 0 || spec.Workload.RunMaxAge == 0 {
		t.Fatalf("the fixture is not a coalescing spec: %+v", spec.Workload)
	}
	every, pinned := vmmodel.EverySampleSpec(spec)
	w := every.Workload
	if w.CoalesceRuns || w.ExtendEvery != 0 || w.ExtendTTLFraction != 0 || w.RunMaxAge != 0 {
		t.Errorf("the coalescer is not off: %+v", w)
	}
	if pinned != nil || every.Pins != nil {
		t.Error("a spec without pins has a projection")
	}
	// Nothing else changes: same seed, start, cluster, producers, retentions and batch.
	spec2, w2 := spec, spec.Workload
	w2.CoalesceRuns, w2.ExtendEvery, w2.ExtendTTLFraction, w2.RunMaxAge = false, 0, 0, 0
	spec2.Workload = w2
	d1, _ := every.Digest()
	d2, _ := spec2.Digest()
	if d1 != d2 {
		t.Error("EverySampleSpec changed something besides the coalescer")
	}
	if err := every.Validate(); err != nil {
		t.Errorf("the every-sample spec is not valid: %v", err)
	}
	// The input is not changed.
	if !spec.Workload.CoalesceRuns {
		t.Error("EverySampleSpec changed its argument")
	}

	// Pins become a projection: the pinned entities, and no pins in the spec.
	node, svc := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.Service, 2)
	spec.Pins = &runner.Pins{Classes: []runner.PinnedClass{{
		Class:   workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse},
		Records: []identity.Fingerprint{node}, Median: []identity.Fingerprint{svc},
	}}}
	spec.FullStore = true
	every, pinned = vmmodel.EverySampleSpec(spec)
	if every.Pins != nil || every.FullStore || len(pinned) != 2 {
		t.Errorf("pins %v, full store %v, pinned %v", every.Pins, every.FullStore, pinned)
	}
	if !vmmodel.Touches(pinned, engine.Record{Subject: engine.EdgeSubject(fixture.FP(catalog.K8sPod, 3), node, catalog.ScheduledOn)}) ||
		!vmmodel.Touches(pinned, engine.Record{Subject: engine.EntitySubject(svc)}) ||
		vmmodel.Touches(pinned, engine.Record{Subject: engine.EntitySubject(fixture.FP(catalog.K8sPod, 3))}) {
		t.Error("Touches: an end of an edge, or the entity, is a touch; nothing else is")
	}
}

// ---- the checker catches what it exists to catch ----

func rec(seq uint64, p lifecycle.Producer, at, through time.Duration, ttl time.Duration) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(fixture.FP(catalog.K8sPod, 1), fixture.FP(catalog.K8sNode, 2), catalog.ScheduledOn),
		Producer: p, EventTime: t0.Add(at), Seq: seq, Kind: lifecycle.Observe, TTL: ttl,
	}
	if through > 0 {
		r.Through = t0.Add(through)
	}
	return r
}

func facts(rs ...engine.Record) map[engine.Subject][]engine.Record {
	return map[engine.Subject][]engine.Record{rs[0].Subject: rs}
}

// Refreshes every 60 s for 5 minutes with a TTL of 4 minutes: the existence is [0, 9 min).
func everyRefresh() []engine.Record {
	var rs []engine.Record
	for i := 0; i <= 5; i++ {
		rs = append(rs, rec(uint64(i+1), "node-collector", time.Duration(i)*time.Minute, 0, 4*time.Minute))
	}
	return rs
}

func TestCompareFactsCatchesADifferentExistence(t *testing.T) {
	bound := func(engine.Subject) time.Duration { return 2 * time.Minute }
	every := everyRefresh()
	good := facts(rec(1, "node-collector", 0, 5*time.Minute, 4*time.Minute)) // [0, 9 min), as the refreshes say
	rep, err := vmmodel.CompareFacts(good, facts(every...), bound)
	if err != nil || rep.Identical != 1 || rep.EndsEarlier != 0 {
		t.Fatalf("equal existence: %+v, %v", rep, err)
	}

	cases := []struct {
		name string
		co   map[engine.Subject][]engine.Record
	}{
		{"an end shifted later", facts(rec(1, "node-collector", 0, 6*time.Minute, 4*time.Minute))},
		{"an end shifted earlier by the whole bound", facts(rec(1, "node-collector", 0, 3*time.Minute, 4*time.Minute))},
		{"a start shifted", facts(rec(1, "node-collector", time.Second, 5*time.Minute, 4*time.Minute))},
		{"an interval that is open", facts(func() engine.Record { r := rec(1, "node-collector", 0, 0, 0); return r }())},
		{"a subject missing", map[engine.Subject][]engine.Record{}},
		{"a subject only in the coalesced stream", map[engine.Subject][]engine.Record{
			good[every[0].Subject][0].Subject:                    good[every[0].Subject],
			engine.EntitySubject(fixture.FP(catalog.K8sNode, 7)): {{Layer: catalog.L2, Subject: engine.EntitySubject(fixture.FP(catalog.K8sNode, 7)), Producer: "node-collector", EventTime: t0, Seq: 9, Kind: lifecycle.Observe, TTL: time.Minute}},
		}},
	}
	for _, c := range cases {
		if _, err := vmmodel.CompareFacts(c.co, facts(every...), bound); !errors.Is(err, vmmodel.ErrFactsDiffer) {
			t.Errorf("%s: err = %v, want ErrFactsDiffer", c.name, err)
		}
	}

	// An interval more in one stream than in the other, whichever it is, with every
	// interval before it equal. The first interval is [0, 9 min) in both.
	later := rec(7, "node-collector", 30*time.Minute, 0, 4*time.Minute)
	twoIntervals := append(everyRefresh(), later)
	oneInterval := good
	if _, err := vmmodel.CompareFacts(oneInterval, facts(twoIntervals...), bound); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("an interval only in the every-sample stream: err = %v", err)
	}
	extra := facts(rec(1, "node-collector", 0, 5*time.Minute, 4*time.Minute), rec(2, "node-collector", 30*time.Minute, 0, 4*time.Minute))
	if _, err := vmmodel.CompareFacts(extra, facts(every...), bound); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("an interval only in the coalesced stream: err = %v", err)
	}
	if rep, err := vmmodel.CompareFacts(extra, facts(twoIntervals...), bound); err != nil || rep.Identical != 1 {
		t.Errorf("two intervals in both: %+v, %v", rep, err)
	}

	// An end earlier by less than the bound is what the coalescer may do, and is reported.
	early := facts(rec(1, "node-collector", 0, 4*time.Minute+30*time.Second, 4*time.Minute)) // ends 30 s early
	rep, err = vmmodel.CompareFacts(early, facts(every...), bound)
	if err != nil || rep.EndsEarlier != 1 || rep.MaxEarly != 30*time.Second || rep.Identical != 0 {
		t.Errorf("an end 30 s early: %+v, %v", rep, err)
	}
	// With no bound (the coalescer extends on every refresh) even that is a difference.
	if _, err := vmmodel.CompareFacts(early, facts(every...), func(engine.Subject) time.Duration { return 0 }); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("no bound: err = %v", err)
	}
}

// ---- the extension rule, the bound and the band ----

// The coalescer's extension interval may not exceed the window of a producer whose runs it
// extends: with the fixed interval it is refused, with the fraction (at most one of the
// window) it never can be, and a producer in watch mode has no run.
func TestCheckExtension(t *testing.T) {
	reg := registry(t) // node 240 s, traces 1,200 s, pods 3,600 s, kubelet 300 s; watch mode 240 s
	w := fixture.Registered(time.Hour)
	w.CoalesceRuns = true
	w.ExtendEvery = 240 * time.Second
	if err := vmmodel.CheckExtension(w, reg); err != nil {
		t.Errorf("an interval equal to the smallest window: %v", err)
	}
	w.ExtendEvery = 241 * time.Second
	if err := vmmodel.CheckExtension(w, reg); !errors.Is(err, vmmodel.ErrExtension) {
		t.Errorf("an interval longer than the node collector's window: %v", err)
	}
	w.ExtendEvery = 3 * time.Hour
	if err := vmmodel.CheckExtension(w, reg); !errors.Is(err, vmmodel.ErrExtension) {
		t.Errorf("a three-hour interval: %v", err)
	}
	w.ExtendEvery, w.ExtendTTLFraction = 0, 0.5
	if err := vmmodel.CheckExtension(w, reg); err != nil {
		t.Errorf("a fraction of the TTL: %v", err)
	}
	w.CoalesceRuns, w.ExtendEvery, w.ExtendTTLFraction = false, 3*time.Hour, 0
	if err := vmmodel.CheckExtension(w, reg); err != nil {
		t.Errorf("no coalescer: %v", err)
	}
	// A registry with only watch-mode producers has no run to extend.
	watch, err := vmmodel.NewRegistry(nil, map[catalog.EntityType][]vmmodel.Holder{catalog.Rack: {{Producer: "fabric", Window: time.Minute, Scrape: time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	w.CoalesceRuns, w.ExtendEvery = true, time.Hour
	if err := vmmodel.CheckExtension(w, watch); err != nil {
		t.Errorf("a watch-mode registry: %v", err)
	}
}

// The bound of a subject: the fixed interval or the fraction of the longest window of its
// refreshing holders, capped at the subject's band (the longest window of any holder and a
// millisecond), and nothing without a coalescer.
func TestExtensionBoundAndBand(t *testing.T) {
	reg := registry(t)
	node := engine.EntitySubject(fixture.FP(catalog.K8sNode, 1))
	podEdge := engine.EdgeSubject(fixture.FP(catalog.K8sPod, 1), fixture.FP(catalog.K8sNode, 2), catalog.ScheduledOn)
	unknown := engine.EntitySubject(fixture.FP(catalog.Process, 1))

	if got := reg.Band(node); got != 240*time.Second+vmmodel.Slack {
		t.Errorf("band of a node %s", got)
	}
	// scheduled_on is held by the watch (240 s), the pod collector (3,600 s) and the
	// kubelet (300 s): the band is the longest.
	if got := reg.Band(podEdge); got != 3600*time.Second+vmmodel.Slack {
		t.Errorf("band of a placement %s", got)
	}
	if got := reg.Band(engine.EntitySubject(fixture.FP(catalog.Host, 1))); got != 240*time.Second+vmmodel.Slack {
		t.Errorf("band of a host %s", got)
	}
	if reg.Band(unknown) != 0 {
		t.Error("a band for a subject the registry does not hold")
	}
	bound := func(co bool, every time.Duration, frac float64, s engine.Subject) time.Duration {
		return vmmodel.ExtensionBound(co, every, frac, reg, s)
	}
	if got := bound(true, 0, 0.5, node); got != 120*time.Second {
		t.Errorf("half of a node's window %s", got)
	}
	if got := bound(true, 0, 0.5, podEdge); got != 1800*time.Second {
		t.Errorf("half of the pod collector's window %s", got)
	}
	if got := bound(true, 90*time.Second, 0, node); got != 90*time.Second {
		t.Errorf("a fixed interval %s", got)
	}
	// A fixed interval past the band is capped at it: an end a whole band early is not slack.
	if got := bound(true, 3*time.Hour, 0, node); got != 240*time.Second+vmmodel.Slack {
		t.Errorf("a three-hour interval on a node %s", got)
	}
	if got := bound(false, time.Hour, 0.5, node); got != 0 {
		t.Errorf("no coalescer %s", got)
	}
	// A subject with no refreshing holder has no run to extend.
	if got := bound(true, 0, 0.5, unknown); got != 0 {
		t.Errorf("a subject nothing holds %s", got)
	}
	by := map[engine.Subject][]engine.Record{node: nil, podEdge: nil}
	if got := vmmodel.SmallestBand(reg, by); got != 240*time.Second+vmmodel.Slack {
		t.Errorf("smallest band %s", got)
	}
	if got := vmmodel.SmallestBand(reg, nil); got != 0 {
		t.Errorf("smallest band of nothing %s", got)
	}
}

// CheckSameFacts refuses a coalescer whose fixed interval is longer than a window before it
// generates anything, and a spec's report carries the smallest band.
func TestCheckSameFactsRefusesALongExtensionIntervalAndReportsTheBand(t *testing.T) {
	spec := fixture.Spec()
	spec.Workload.ExtendTTLFraction = 0
	spec.Workload.ExtendEvery = time.Hour
	reg, err := vmmodel.RegistryFor(spec.Workload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vmmodel.CheckSameFacts(context.Background(), spec, reg); !errors.Is(err, vmmodel.ErrExtension) {
		t.Errorf("an hour against a four-minute window: %v", err)
	}
	spec = fixture.Spec()
	reg, _ = vmmodel.RegistryFor(spec.Workload)
	rep, err := vmmodel.CheckSameFacts(context.Background(), spec, reg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.SmallestBand != 240*time.Second+vmmodel.Slack {
		t.Errorf("smallest band %s", rep.SmallestBand)
	}
}
