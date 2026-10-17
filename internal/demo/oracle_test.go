package demo

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle of the service layer: what the store must hold of the services, their
// calls, their instances and the bridge from an instance to its pod, and what the
// metrics path must have counted, written from the world and from the rules the
// translators document, and from nothing else. It imports no translator, no
// store and no tool package for a rule, and it shares no constant with the
// emitters: the numbers below are the documented ones.
//
// The rules it applies, with where each is documented:
//
//   - The service graph connector flushes a cumulative counter of each call once a
//     minute and re-exports a series for fifteen minutes after its last call
//     (package svcgraph, "A point is not a sighting").
//   - The translator has no state at its start, so a series that is alive at the
//     first flush is set aside as new during the warm-up, and is sighted at the next
//     flush, whose value is larger. A series that is new after the warm-up (two
//     flushes) is sighted at its first point. A later point is a sighting when its
//     value is larger, and not otherwise (package svcgraph, "The rule", cases 3 to
//     6, and the warm-up).
//   - A call to a database is set aside and takes no part in the state ("Not
//     written").
//   - A sighting is the two services and the depends_on edge, at the point's time,
//     with the expiry as TTL: the edge ends at the last sighting plus the expiry
//     (package svcgraph, "What is asserted").
//   - A span count point above zero is a sighting of the instance at the point's
//     time: the service, the instance, instance_of and part_of its pod, each with
//     the TTL, so each ends at the last sighting plus the TTL (package spancount).
//   - An edge is reported only while both its endpoints exist: the bridge ends where
//     its pod does (package interval, "The cascade").

// The documented numbers, written out here and not read from the emitters.
const (
	oFlushes  = 360
	oExpiry   = 15 * time.Minute
	oWindowTo = 6 * time.Hour
)

// oFlushAt and oSpanAt are the instants of the kth flush of the service graph
// connector and of the kth span count: a minute apart, 20 and 50 seconds past the
// anchor's minute.
func oFlushAt(k int) time.Time { return anchor.Add(20*time.Second + time.Duration(k)*time.Minute) }
func oSpanAt(k int) time.Time  { return anchor.Add(50*time.Second + time.Duration(k)*time.Minute) }

// run is an interval of existence: [start, end), with how it ends.
type run struct {
	start, end time.Time
	source     lifecycle.EndSource
}

func (r run) String() string {
	return fmt.Sprintf("%s..%s (%v)", r.start.Format(time.RFC3339), r.end.Format(time.RFC3339), r.source)
}

// expectation is what the oracle says the store holds and the metrics path counted.
type expectation struct {
	// services are by "namespace/name", calls by the client's and the server's,
	// instances by service.instance.id (which the world gives to one pod only), and
	// bridges by the instance's id.
	services  map[string][]run
	calls     map[[2]string][]run
	instances map[string][]run
	bridges   map[string][]run
	// serviceOf and podOf say which service an instance is of and which pod it runs
	// in.
	serviceOf map[string]string
	podOf     map[string]*Pod
	// sighted are the sightings of each call, by flush index.
	sighted map[[2]string][]int
	// counters are the non-zero counters of the metrics path.
	counters map[string]int64
	// warmUp is the number of series alive at the first flush.
	warmUp int
	// problems are what the oracle could not model: the world holds something the
	// rules above do not cover, and the expectation is not to be trusted.
	problems []string
}

func serviceKey(ns, name string) string { return ns + "/" + name }

// merge turns sighting instants into the runs they make: each sighting stands for
// the expiry after it, and runs that touch are one.
func merge(sightings []time.Time) []run {
	ts := slices.Clone(sightings)
	slices.SortFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
	var out []run
	for _, t := range ts {
		switch n := len(out); {
		case n > 0 && !t.After(out[n-1].end):
			out[n-1].end = maxTime(out[n-1].end, t.Add(oExpiry))
		default:
			out = append(out, run{start: t, end: t.Add(oExpiry), source: lifecycle.EndLivenessExpiry})
		}
	}
	return out
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// clip cuts the runs at the end of the window: a run that reaches it has no end
// source, as the reader reports.
func clip(runs []run) []run {
	to := anchor.Add(oWindowTo)
	var out []run
	for _, r := range runs {
		if !r.start.Before(to) {
			continue
		}
		if !r.end.Before(to) {
			r.end, r.source = to, lifecycle.EndUnknown
		}
		out = append(out, r)
	}
	return out
}

// podSends says whether the pod's instance sends a span count at s: its
// container runs, and its node is ready. A pod that was deleted sends nothing from
// the deletion on, and node-2's runtime is down from the instant it is not ready
// until it is ready again.
func podSends(w *World, p *Pod, s time.Time) bool {
	if s.Before(p.Started) || (!p.Deleted.IsZero() && !s.Before(p.Deleted)) {
		return false
	}
	for _, o := range w.Outages {
		if o.Node == p.Node && !s.Before(o.From) && s.Before(o.To) {
			return false
		}
	}
	return true
}

// expectServices is the oracle.
func expectServices(w *World) expectation {
	e := expectation{
		services: map[string][]run{}, calls: map[[2]string][]run{}, instances: map[string][]run{}, bridges: map[string][]run{},
		serviceOf: map[string]string{}, podOf: map[string]*Pod{}, sighted: map[[2]string][]int{},
		counters: map[string]int64{},
	}
	serviceTimes := map[string][]time.Time{}
	callTimes := map[[2]string][]time.Time{}

	// The service graph.
	for _, c := range w.Calls {
		client, server := serviceKey(c.ClientNamespace, c.Client), serviceKey(c.ServerNamespace, c.Server)
		key := [2]string{client, server}
		prevPresent, prevValue := false, 0
		for k := 0; k < oFlushes; k++ {
			f := oFlushAt(k)
			calls, last := 0, time.Time{}
			for m := c.From; !m.After(c.To) && !m.After(f); m = m.Add(time.Minute) {
				calls++
				last = m
			}
			if calls == 0 || f.Sub(last) > 15*time.Minute {
				prevPresent = false
				continue
			}
			value := calls * c.Rate
			e.counters["svcgraph.points"]++
			if c.Database {
				e.counters["svcgraph.skipped_connection_database"]++
				continue
			}
			switch {
			case !prevPresent && k == 0:
				e.counters["svcgraph.skipped_warm_up"]++
				e.warmUp++
			case !prevPresent && k == 1:
				// A series that is new at the second flush is still in the warm-up: the
				// world must not have one (checked beside the world).
				e.problems = append(e.problems, fmt.Sprintf("the call %s -> %s is new at the second flush, inside the warm-up", client, server))
			case !prevPresent, value > prevValue:
				e.counters["svcgraph.sightings"]++
				e.sighted[key] = append(e.sighted[key], k)
				callTimes[key] = append(callTimes[key], f)
				serviceTimes[client] = append(serviceTimes[client], f)
				serviceTimes[server] = append(serviceTimes[server], f)
			}
			prevPresent, prevValue = true, value
		}
	}

	// The span counts.
	instTimes := map[string][]time.Time{}
	for k := 0; k < oFlushes; k++ {
		s := oSpanAt(k)
		for _, p := range w.Pods {
			wl := w.Workloads[p.Workload]
			if wl.Service == "" || !podSends(w, p, s) {
				continue
			}
			id := fmt.Sprintf("%s.%s.%s", wl.Namespace, p.Name, wl.Name)
			e.counters["spancount.points"]++
			e.counters["spancount.sightings"]++
			instTimes[id] = append(instTimes[id], s)
			serviceTimes[serviceKey(wl.Namespace, wl.Service)] = append(serviceTimes[serviceKey(wl.Namespace, wl.Service)], s)
			e.serviceOf[id] = serviceKey(wl.Namespace, wl.Service)
			if prev, dup := e.podOf[id]; dup && prev != p {
				e.problems = append(e.problems, "two pods hold the instance id "+id)
			}
			e.podOf[id] = p
		}
	}
	e.counters["metrics.points"] = e.counters["svcgraph.points"] + e.counters["spancount.points"]

	for k, ts := range serviceTimes {
		e.services[k] = clip(merge(ts))
	}
	for k, ts := range callTimes {
		e.calls[k] = clip(merge(ts))
	}
	for id, ts := range instTimes {
		e.instances[id] = clip(merge(ts))
		// The bridge ends where its pod does, which is when the deletion was seen,
		// a second after the pod was deleted.
		var bridge []run
		for _, r := range merge(ts) {
			if p := e.podOf[id]; !p.Deleted.IsZero() {
				if gone := p.Deleted.Add(time.Second); gone.Before(r.end) {
					r.end, r.source = gone, lifecycle.EndCascade
				}
			}
			bridge = append(bridge, r)
		}
		e.bridges[id] = clip(bridge)
	}
	return e
}

// ---- reading the store ----

func serviceFP(t testing.TB, ns, name string) identity.Fingerprint {
	t.Helper()
	id, err := resolver.Resolve(catalog.Service, []identity.Attr{
		{Key: catalog.ServiceNamespace, Value: ns}, {Key: catalog.ServiceName, Value: name},
	})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func instanceFP(t testing.TB, ns, name, id string) identity.Fingerprint {
	t.Helper()
	r, err := resolver.Resolve(catalog.ServiceInstance, []identity.Attr{
		{Key: catalog.ServiceNamespace, Value: ns}, {Key: catalog.ServiceName, Value: name}, {Key: catalog.ServiceInstanceID, Value: id},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r.Fingerprint()
}

func splitKey(k string) (ns, name string) {
	ns, name, _ = strings.Cut(k, "/")
	return ns, name
}

func piecesOf(el interval.Element) []run {
	var out []run
	for _, p := range el.Pieces {
		out = append(out, run{start: p.Start, end: p.End, source: p.EndSource})
	}
	return out
}

// differ compares the runs of the store with the oracle's, exactly.
func differ(t *testing.T, what string, got, want []run) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s: the store holds %v, the oracle says %v", what, got, want)
	}
}

// edgeRuns are the runs of the edge of the relation between two entities, read with
// the cascade over the whole window.
func edgeRuns(t testing.TB, b *Built, from, to identity.Fingerprint, rel catalog.RelationType) []run {
	t.Helper()
	rd, win := reader(b)
	res, err := rd.Edges(context.Background(), from, store.Forward, win)
	if err != nil {
		t.Fatal(err)
	}
	var out []run
	n := 0
	for _, el := range res.Elements {
		if el.Subject.Relation == rel && el.Subject.B == to {
			out = append(out, piecesOf(el)...)
			n++
		}
	}
	if n > 1 {
		t.Fatalf("%d elements for one edge", n)
	}
	return out
}

func entityRuns(t testing.TB, b *Built, fp identity.Fingerprint) []run {
	t.Helper()
	rd, win := reader(b)
	res, err := rd.Entity(context.Background(), fp, win)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Elements) != 1 {
		t.Fatalf("%d elements for one entity", len(res.Elements))
	}
	return piecesOf(res.Elements[0])
}

// The store holds what the oracle says, exactly, of every service, every call,
// every instance and every bridge, interval by interval. The oracle is written
// first; nothing is read from the store before it is complete. A disagreement is
// reported and the oracle is not adjusted to the store.
func TestTheStoreHoldsWhatTheOracleSays(t *testing.T) {
	w := NewWorld(DefaultSeed)
	want := expectServices(w)
	if len(want.problems) > 0 {
		t.Fatalf("the oracle cannot model the world: %v", want.problems)
	}
	if len(want.services) != 10 || len(want.calls) != 13 {
		t.Fatalf("the oracle has %d services and %d calls, want 10 and 13", len(want.services), len(want.calls))
	}
	b := canonicalBuild(t)

	for key, runs := range want.services {
		ns, name := splitKey(key)
		differ(t, "service "+key, entityRuns(t, b, serviceFP(t, ns, name)), runs)
	}
	for key, runs := range want.calls {
		cns, cname := splitKey(key[0])
		sns, sname := splitKey(key[1])
		differ(t, "call "+key[0]+" -> "+key[1], edgeRuns(t, b, serviceFP(t, cns, cname), serviceFP(t, sns, sname), catalog.DependsOn), runs)
	}
	for id, runs := range want.instances {
		ns, name := splitKey(want.serviceOf[id])
		fp := instanceFP(t, ns, name, id)
		differ(t, "instance "+id, entityRuns(t, b, fp), runs)
		differ(t, "instance_of of "+id, edgeRuns(t, b, fp, serviceFP(t, ns, name), catalog.InstanceOf), runs)
		differ(t, "the bridge of "+id, edgeRuns(t, b, fp, fpOfPod(want.podOf[id].UID), catalog.PartOf), want.bridges[id])
	}
	if t.Failed() {
		return
	}

	// The hand-simulated instants, as a check on the oracle itself.
	at := func(d time.Duration) time.Time { return anchor.Add(d) }
	legacy := want.calls[[2]string{"shop/checkout", "payments/legacy-pay"}]
	if len(legacy) != 1 || !legacy[0].start.Equal(at(time.Minute+20*time.Second)) || !legacy[0].end.Equal(at(2*time.Hour+25*time.Minute+20*time.Second)) || legacy[0].source != lifecycle.EndLivenessExpiry {
		t.Errorf("checkout to legacy-pay: %v, want 00:01:20Z to 02:25:20Z by expiry", legacy)
	}
	if r := want.calls[[2]string{"shop/checkout", "payments/payments-api"}]; len(r) != 1 || !r[0].start.Equal(at(2*time.Hour+20*time.Second)) || r[0].source != lifecycle.EndUnknown {
		t.Errorf("checkout to payments-api: %v, want from 02:00:20Z to the end", r)
	}
}

// The metrics path counted exactly what the oracle predicts, every named counter,
// and nothing else: the warm-up and the database skips are the world's own numbers,
// no point was refused, dropped or set aside for any other reason, and every skip
// the build counted is explained.
func TestTheCountersAreWhatTheOracleSays(t *testing.T) {
	w := NewWorld(DefaultSeed)
	want := expectServices(w)
	if len(want.problems) > 0 {
		t.Fatalf("the oracle cannot model the world: %v", want.problems)
	}
	b := canonicalBuild(t)
	if got := want.counters["svcgraph.skipped_warm_up"]; got != 11 {
		t.Errorf("the oracle predicts %d warm-up skips, want 11", got)
	}
	if got := want.counters["svcgraph.skipped_connection_database"]; got != 720 {
		t.Errorf("the oracle predicts %d database skips, want 720", got)
	}
	for name, n := range want.counters {
		if got := b.Metrics[name]; got != n {
			t.Errorf("counter %s is %d, the oracle says %d", name, got, n)
		}
	}
	for name, got := range b.Metrics {
		if _, predicted := want.counters[name]; !predicted && got != 0 {
			t.Errorf("counter %s is %d, which the oracle did not predict", name, got)
		}
	}
	if u := b.UnexplainedSkips(); u != 0 {
		t.Errorf("%d skips unexplained", u)
	}
	skips := []Count{
		{"k8scluster/" + ClusterName, "entity type not in the catalog", b.NamespaceEvents},
		{"svcgraph/" + ClusterName, "connection type database", int(want.counters["svcgraph.skipped_connection_database"])},
		{"svcgraph/" + ClusterName, "series new during warm-up", want.warmUp},
	}
	if !slices.Equal(b.Skips, skips) {
		t.Errorf("skips %v, want %v", b.Skips, skips)
	}
	if len(b.Notes) != 0 {
		t.Errorf("notes %v, want none", b.Notes)
	}
}

// The services' own story, with its instants written out: where the calls begin and
// end, with the instants of each written out.
func TestTheStoryOfTheCalls(t *testing.T) {
	s := canonicalBuild(t).Story
	at := func(d time.Duration) time.Time { return anchor.Add(d) }
	if !s.Migrate.Equal(at(2*time.Hour)) || !s.LastLegacyCall.Equal(at(2*time.Hour+10*time.Minute)) ||
		!s.LegacyCallEnds.Equal(at(2*time.Hour+25*time.Minute+20*time.Second)) ||
		!s.FaultFrom.Equal(at(4*time.Hour)) || !s.FaultTo.Equal(at(4*time.Hour+20*time.Minute)) {
		t.Errorf("the story's instants %v %v %v %v %v", s.Migrate, s.LastLegacyCall, s.LegacyCallEnds, s.FaultFrom, s.FaultTo)
	}
	if len(s.Services) != 10 {
		t.Errorf("%d services in the story, want 10", len(s.Services))
	}
}

// Every producer of the records is one of the translators: the metrics path writes
// through the sequencer alone, and no derived record is written beside.
func TestOnlyTheTranslatorsWriteServices(t *testing.T) {
	b := canonicalBuild(t)
	want := []lifecycle.Producer{"k8scluster/" + ClusterName, "k8sobjects/" + ClusterName, "nodeagent/" + ClusterName, "spancount/" + ClusterName, "svcgraph/" + ClusterName}
	if !slices.Equal(b.Producers, want) {
		t.Errorf("the records are of %v, want %v", b.Producers, want)
	}
	if b.Records == 0 || b.Records != int(b.LastSeq) {
		t.Errorf("%d records and token %d: the demo writes through the sequencer alone", b.Records, b.LastSeq)
	}
}

// A service's pieces name the producers that kept it alive: both, because both
// translators assert it; a call, only the service graph's; an instance and its
// edges, only the span count's.
func TestWhoAssertsWhat(t *testing.T) {
	b := canonicalBuild(t)
	rd, win := reader(b)
	w := NewWorld(DefaultSeed)
	svc := serviceFP(t, "payments", "ledger")
	res, err := rd.Entity(context.Background(), svc, win)
	if err != nil {
		t.Fatal(err)
	}
	wantProd := []lifecycle.Producer{"spancount/" + ClusterName, "svcgraph/" + ClusterName}
	if got := res.Elements[0].Pieces[0].Producers; !slices.Equal(got, wantProd) {
		t.Errorf("ledger is asserted by %v, want %v", got, wantProd)
	}
	for _, p := range w.Pods {
		wl := w.Workloads[p.Workload]
		if wl.Name != "ledger" {
			continue
		}
		in, _ := w.Instance(p)
		fp := instanceFP(t, in.Namespace, in.Service, in.ID)
		res, err := rd.Entity(context.Background(), fp, win)
		if err != nil {
			t.Fatal(err)
		}
		if got := res.Elements[0].Pieces[0].Producers; !slices.Equal(got, []lifecycle.Producer{"spancount/" + ClusterName}) {
			t.Errorf("%s is asserted by %v, want the span count's", in.ID, got)
		}
		break
	}
}

// The hop of checkout in the minutes the demo asks service_context about, from the
// oracle's runs alone and with the instants written out: frontend calls it for the
// whole window; it calls cart for the whole window, legacy-pay until the silence of
// its last sighting ends the call, and payments-api from the first sighting of the
// new call. The window is as far as five minutes from every beginning and end the
// world has in it (a call, an instance, its bridge, a pod), so that no answer
// depends on which side of an event an end of the window falls; and the pods that are
// seed pods, the instances of frontend and the nodes they stand on are the numbers the
// answer's counts must be.
func TestTheHopOfCheckoutInTheWindowOfTheQuestion(t *testing.T) {
	w := NewWorld(DefaultSeed)
	want := expectServices(w)
	if len(want.problems) > 0 {
		t.Fatalf("the oracle cannot model the world: %v", want.problems)
	}
	at := func(d time.Duration) time.Time { return anchor.Add(d) }
	from, to := at(time.Hour+55*time.Minute), at(2*time.Hour+35*time.Minute)

	// A run, cut to the window: it ends there, with no source, if it goes on.
	within := func(r run) (run, bool) {
		if !r.end.After(from) || !r.start.Before(to) {
			return run{}, false
		}
		if r.start.Before(from) {
			r.start = from
		}
		if !r.end.Before(to) {
			r.end, r.source = to, lifecycle.EndUnknown
		}
		return r, true
	}
	callers, callees := map[string][]run{}, map[string][]run{}
	for key, runs := range want.calls {
		for _, r := range runs {
			if r, ok := within(r); ok {
				switch {
				case key[1] == "shop/checkout":
					callers[key[0]] = append(callers[key[0]], r)
				case key[0] == "shop/checkout":
					callees[key[1]] = append(callees[key[1]], r)
				}
			}
		}
	}
	legacyEnds := at(2*time.Hour + 25*time.Minute + 20*time.Second)
	wholeWindow := func() []run { return []run{{from, to, lifecycle.EndUnknown}} }
	wantCallers := map[string][]run{"shop/frontend": wholeWindow()}
	wantCallees := map[string][]run{
		"shop/cart":             wholeWindow(),
		"payments/legacy-pay":   {{from, legacyEnds, lifecycle.EndLivenessExpiry}},
		"payments/payments-api": {{at(2*time.Hour + 20*time.Second), to, lifecycle.EndUnknown}},
	}
	if !maps.EqualFunc(callers, wantCallers, slices.Equal) || !maps.EqualFunc(callees, wantCallees, slices.Equal) {
		t.Errorf("the callers of checkout are %v and its callees %v; want %v and %v", callers, callees, wantCallers, wantCallees)
	}

	// Every beginning and end the world has, and the distance of the window's ends.
	var events []time.Time
	for _, runs := range want.calls {
		for _, r := range runs {
			events = append(events, r.start, r.end)
		}
	}
	for _, byID := range []map[string][]run{want.instances, want.bridges} {
		for _, runs := range byID {
			for _, r := range runs {
				events = append(events, r.start, r.end)
			}
		}
	}
	for _, p := range w.Pods {
		events = append(events, p.Created, p.Scheduled, p.Started, p.Finished, p.Deleted)
		if !p.Deleted.IsZero() {
			events = append(events, p.Deleted.Add(time.Second))
		}
	}
	for _, end := range []time.Time{from, to} {
		for _, e := range events {
			if e.IsZero() {
				continue
			}
			if d := end.Sub(e).Abs(); d < 5*time.Minute {
				t.Errorf("the window's end %s is %s from an event at %s, under five minutes", end.Format(time.RFC3339), d, e.Format(time.RFC3339))
			}
		}
	}

	// The seed pods are the pods of checkout and the four services it shares a hop
	// with whose instance is part of them in the window; frontend has the three of its
	// rollout, the three it replaced having ended before the window; and they stand on
	// all four nodes, one short of where ubiquity is judged.
	members := map[string]bool{"shop/checkout": true, "shop/frontend": true, "shop/cart": true, "payments/legacy-pay": true, "payments/payments-api": true}
	seedPods, frontend, nodes := 0, 0, map[int]bool{}
	for id, runs := range want.bridges {
		svc := want.serviceOf[id]
		inWindow := false
		for _, r := range runs {
			if _, ok := within(r); ok {
				inWindow = true
			}
		}
		if inWindow && members[svc] {
			seedPods++
			nodes[want.podOf[id].Node] = true
			if svc == "shop/frontend" {
				frontend++
			}
		}
	}
	if seedPods != 13 || frontend != 3 || len(nodes) != 4 {
		t.Errorf("%d seed pods (want 13), %d of frontend (want 3), on %d nodes (want 4)", seedPods, frontend, len(nodes))
	}
}
