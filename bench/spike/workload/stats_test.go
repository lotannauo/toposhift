package workload_test

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func fp(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func classOf(rep workload.Report, layer catalog.Layer, owner catalog.EntityType, side workload.Side) (workload.ClassStats, bool) {
	for _, c := range rep.Classes {
		if c.Class == (workload.Class{Layer: layer, Owner: owner, Side: side}) {
			return c, true
		}
	}
	return workload.ClassStats{}, false
}

// A stream small enough to count by hand: every number in the report for the
// node's reverse prefix is known.
func TestAnalyzerCountsAStreamByHand(t *testing.T) {
	t.Parallel()

	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	p1 := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p1")
	p2 := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p2")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// On the third day of three, so they all fall in the last two.
	at := func(s int) time.Time { return start.Add(50*time.Hour + time.Duration(s)*time.Second) }
	edge := func(pod identity.Fingerprint, prod lifecycle.Producer, kind lifecycle.Kind, t, through int, ttl time.Duration) engine.Record {
		r := engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: prod,
			EventTime: at(t), Kind: kind, TTL: ttl, Payload: make([]byte, 10),
		}
		if through > 0 {
			r.Through = at(through)
		}
		return r
	}
	p3 := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p3")
	p4 := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p4")
	stream := []engine.Record{
		edge(p1, "a", lifecycle.Observe, 0, 0, 0),             // p1 placed
		edge(p3, "d", lifecycle.Observe, 1, 0, 0),             // p3 placed...
		edge(p4, "e", lifecycle.Observe, 2, 0, time.Minute),   // p4 asserted for a minute, never refreshed
		edge(p3, "d", lifecycle.Delete, 3, 0, 0),              // ...p3 gone
		edge(p2, "a", lifecycle.Observe, 10, 0, 0),            // p2 placed
		edge(p1, "a", lifecycle.Delete, 20, 0, 0),             // p1 gone
		edge(p2, "b", lifecycle.Observe, 10, 40, time.Minute), // an extension at the run's start: behind the newest
		edge(p1, "a", lifecycle.Observe, 5, 0, 0),             // late, older than p1's deletion: changes nothing
		edge(p2, "c", lifecycle.Observe, 20, 0, 0),            // a tie with the newest instant
	}
	a := workload.NewAnalyzer(start, 72*time.Hour)
	for _, r := range stream {
		a.Add(r)
	}
	rep := a.Report()

	if rep.Records != 9 || rep.Observes != 6 || rep.Extensions != 1 || rep.Deletes != 2 {
		t.Errorf("records %d (observes %d, extensions %d, deletes %d), want 9 (6, 1, 2)", rep.Records, rep.Observes, rep.Extensions, rep.Deletes)
	}
	if rep.PayloadBytes != 90 {
		t.Errorf("%d payload bytes, want 90", rep.PayloadBytes)
	}
	// Lateness is relative to the newest event time so far, and an extension is
	// not late (it is written at its run's start): only record 5, 15 s behind
	// the newest, is, of 9.
	if want := 1.0 / 9; rep.LateShare < want-1e-9 || rep.LateShare > want+1e-9 {
		t.Errorf("late share %v, want %v", rep.LateShare, want)
	}
	c, ok := classOf(rep, catalog.L2, catalog.K8sNode, workload.SideReverse)
	if !ok {
		t.Fatalf("no class for the node's reverse prefix in %v", rep.Classes)
	}
	if c.Prefixes != 1 || c.Observes != 6 || c.Extensions != 1 || c.Deletes != 2 {
		t.Errorf("node reverse: %+v", c)
	}
	// Behind: the extension (10 s, the newest is 20 s) and the late record (5 s).
	if c.Behind != 2 || c.Ties != 1 {
		t.Errorf("behind %d, ties %d, want 2 and 1", c.Behind, c.Ties)
	}
	if c.Peers.Max != 4 {
		t.Errorf("%d distinct peers, want 4", c.Peers.Max)
	}
	// At the end of the three days only p2 is live: producer a's watch-mode
	// assertion of it has no deadline. p3 was deleted and p4's minute ran out. p1 was deleted at 20 s, and
	// the observation at 5 s that arrives afterwards is older than the deletion
	// and must not revive it.
	if c.LiveDegree.Max != 1 {
		t.Errorf("live degree %d, want 1", c.LiveDegree.Max)
	}
	if h := c.History[2]; h.Max != 9 || c.HistoryActive[2] != 1 {
		t.Errorf("history over two days %+v over %d prefixes, want 9 records in 1", h, c.HistoryActive[2])
	}
}

// The text report names every class and says what the owner needs to judge the
// workload; the numbers behind it are checked above and below.
func TestAnalyzerReportOfARealisticStream(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.Duration = 2 * time.Hour
	cfg.FreshIdentities, cfg.MaxPodsPerNode = true, 9
	cfg.PodHeartbeatInterval, cfg.HeartbeatTTLFactor = 5*time.Minute, 4
	cfg.CoalesceRuns, cfg.ExtendTTLFraction = true, 0.5
	cfg.LateProbability = 0
	cfg.BacklogEvery, cfg.BacklogMeanDelay, cfg.BacklogSpan = 20*time.Minute, time.Minute, 4*time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a := workload.NewAnalyzer(cfg.Start, cfg.Duration)
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		a.Add(r)
	}
	rep := a.Report()
	if rep.Records == 0 || rep.Extensions == 0 || rep.Entities <= len(g.Entities())/2 {
		t.Fatalf("empty or implausible report: %+v", rep)
	}
	// The point of fresh identities: dead entities pile up behind the live ones.
	if rep.Entities-rep.EntitiesAlive <= rep.EntitiesAlive {
		t.Errorf("%d entities, %d alive: no churn left dead entities behind", rep.Entities, rep.EntitiesAlive)
	}
	node, ok := classOf(rep, catalog.L2, catalog.K8sNode, workload.SideReverse)
	if !ok {
		t.Fatal("no node reverse class")
	}
	if node.Peers.Max <= uint32(cfg.Pods)/uint32(cfg.Hosts) {
		t.Errorf("the busiest node saw %d distinct pods: fresh identities should give it more than its share of the slots", node.Peers.Max)
	}
	if rep.LateShare == 0 {
		t.Error("a backlog left no record late")
	}
	if len(rep.TopByRecords) == 0 || len(rep.TopByExtensions) == 0 || rep.TopByRecords[0].Records < rep.TopByRecords[len(rep.TopByRecords)-1].Records {
		t.Errorf("top lists are empty or unsorted: %+v", rep.TopByRecords)
	}
	var out bytes.Buffer
	rep.Write(&out)
	for _, want := range []string{"L2 k8s.node <-", "L1 k8s.node ->", "dead for each live", "busiest prefixes by extensions", "7d prefixes"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report does not mention %q:\n%s", want, out.String())
		}
	}
}

// The presets are configs the generator accepts, of the lengths they say, and
// the first day of the smallest one is the size the README says it is (about
// 2.6 million records a day, with the churn of fresh identities).
func TestPresetsAreValidAndTheSizeTheyClaim(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	for name, c := range map[string]struct {
		cfg  workload.Config
		days int
	}{"CI": {workload.CI(), 3}, "Week": {workload.Week(), 10}, "Month": {workload.Month(), 31}} {
		if _, err := workload.New(c.cfg); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if got := c.cfg.Duration; got != time.Duration(c.days)*24*time.Hour {
			t.Errorf("%s lasts %s, want %d days", name, got, c.days)
		}
	}
	cfg := workload.CI()
	cfg.Duration = 6 * time.Hour
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a := workload.NewAnalyzer(cfg.Start, cfg.Duration)
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		a.Add(r)
	}
	perDay := float64(a.Report().Records) * 4
	if perDay < 1.5e6 || perDay > 4e6 {
		t.Errorf("about %.1f million records a day: the presets no longer have the volume the README gives", perDay/1e6)
	}
}

// An entity is alive at the end if some producer's latest word on it is a live
// assertion: not a deletion, and not a TTL that ran out (a node whose heartbeats
// stopped is gone, whatever it said last).
func TestAnalyzerCountsEntitiesAliveAtTheEnd(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entity := func(name string, prod lifecycle.Producer, kind lifecycle.Kind, sec int, ttl time.Duration) engine.Record {
		return engine.Record{
			Layer: catalog.L2, Subject: engine.EntitySubject(fp(t, catalog.K8sPod, catalog.K8sPodUID, name)),
			Producer: prod, EventTime: start.Add(time.Duration(sec) * time.Second), Kind: kind, TTL: ttl,
		}
	}
	a := workload.NewAnalyzer(start, time.Hour)
	for _, r := range []engine.Record{
		entity("kept", "k", lifecycle.Observe, 0, 0),                 // asserted for good
		entity("deleted", "k", lifecycle.Observe, 0, 0),              // asserted...
		entity("deleted", "k", lifecycle.Delete, 5, 0),               // ...and withdrawn
		entity("expired", "n", lifecycle.Observe, 0, time.Minute),    // a heartbeat that stopped
		entity("held", "n", lifecycle.Observe, 3500, 10*time.Minute), // a heartbeat still in force at the end
		entity("two", "k", lifecycle.Observe, 0, 0),                  // held by one producer
		entity("two", "n", lifecycle.Observe, 0, time.Minute),        // and another whose TTL ran out
		entity("two", "k", lifecycle.Delete, 9, 0),                   // then the first lets go: only the dead second remains
	} {
		a.Add(r)
	}
	// An entity's own existence is not an edge: a pod that exists and is placed on
	// one node has one live peer, not two.
	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	a.Add(engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(fp(t, catalog.K8sPod, catalog.K8sPodUID, "kept"), node, catalog.ScheduledOn),
		Producer: "k", EventTime: start, Kind: lifecycle.Observe,
	})
	rep := a.Report()
	if rep.Entities != 5 || rep.EntitiesAlive != 2 {
		t.Errorf("%d entities, %d alive at the end; want 5 and 2 (kept, held)", rep.Entities, rep.EntitiesAlive)
	}
	pods, ok := classOf(rep, catalog.L2, catalog.K8sPod, workload.SideForward)
	if !ok || pods.LiveDegree.Max != 1 {
		t.Errorf("a placed pod has %d live peers, want 1 (%+v)", pods.LiveDegree.Max, pods)
	}
}

// Pick takes the busiest prefixes of a class by records and by extensions, and
// the ones in the middle, in an order that does not depend on map iteration.
func TestAnalyzerPicksPrefixesByHistory(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	node := func(i int) identity.Fingerprint {
		return fp(t, catalog.K8sNode, catalog.K8sNodeUID, string(rune('a'+i)))
	}
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	a := workload.NewAnalyzer(start, 24*time.Hour)
	seq := uint64(0)
	add := func(n int, records, extensions int) {
		for i := range records {
			r := engine.Record{
				Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node(n), catalog.ScheduledOn), Producer: "a",
				EventTime: start.Add(time.Duration(i) * time.Second), Kind: lifecycle.Observe, Seq: seq,
			}
			if i < extensions {
				r.Through = r.EventTime.Add(time.Second)
			}
			seq++
			a.Add(r)
		}
	}
	// Records per node: 9, 7, 5, 3, 1; extensions per node: 0, 7, 0, 3, 1.
	for n, rs := range []int{9, 7, 5, 3, 1} {
		add(n, rs, []int{0, 7, 0, 3, 1}[n])
	}
	c := workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse}
	p := a.Pick(c, 2, 3, false)
	counts := func(ps []workload.PrefixTop, f func(workload.PrefixTop) uint32) []uint32 {
		var out []uint32
		for _, x := range ps {
			out = append(out, f(x))
		}
		return out
	}
	rec := func(x workload.PrefixTop) uint32 { return x.Records }
	ext := func(x workload.PrefixTop) uint32 { return x.Extensions }
	if p.Prefixes != 5 {
		t.Errorf("prefixes = %d, want 5", p.Prefixes)
	}
	if got := counts(p.ByRecords, rec); !slices.Equal(got, []uint32{9, 7}) {
		t.Errorf("by records = %v, want [9 7]", got)
	}
	if got := counts(p.ByExtensions, ext); !slices.Equal(got, []uint32{7, 3}) {
		t.Errorf("by extensions = %v, want [7 3]", got)
	}
	if got := counts(p.Median, rec); !slices.Equal(got, []uint32{7, 5, 3}) {
		t.Errorf("median = %v, want [7 5 3]", got)
	}
	// Asking for more than there is returns what there is, and for none returns none.
	if q := a.Pick(c, 50, 50, false); len(q.ByRecords) != 5 || len(q.Median) != 5 || len(q.ByExtensions) != 3 {
		t.Errorf("greedy pick = %d, %d, %d", len(q.ByRecords), len(q.Median), len(q.ByExtensions))
	}
	if q := a.Pick(c, 0, 0, false); len(q.ByRecords)+len(q.Median)+len(q.ByExtensions) != 0 {
		t.Errorf("empty pick = %+v", q)
	}
	if q := a.Pick(workload.Class{Layer: catalog.L3, Owner: catalog.Service, Side: workload.SideForward}, 3, 3, false); q.Prefixes != 0 || len(q.Median) != 0 {
		t.Errorf("a class with no prefixes picked %+v", q)
	}
}

// Picking live prefixes leaves out the ones nothing refers to at the end of the
// stream: an edge that was deleted, an entity that was deleted.
func TestAnalyzerPicksOnlyWhatExistsAtTheEnd(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	pods := []identity.Fingerprint{
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "kept"),
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "deleted"),
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "expired"),
	}
	a := workload.NewAnalyzer(start, time.Hour)
	var seq uint64
	add := func(pod identity.Fingerprint, kind lifecycle.Kind, sec int, ttl time.Duration) {
		seq++
		base := engine.Record{Layer: catalog.L2, Producer: "p", EventTime: start.Add(time.Duration(sec) * time.Second), Seq: seq, Kind: kind, TTL: ttl}
		edge := base
		edge.Subject = engine.EdgeSubject(pod, node, catalog.ScheduledOn)
		a.Add(edge)
		ent := base
		ent.Subject = engine.EntitySubject(pod)
		a.Add(ent)
	}
	add(pods[0], lifecycle.Observe, 1, 0)
	add(pods[1], lifecycle.Observe, 2, 0)
	add(pods[1], lifecycle.Delete, 3, 0)
	add(pods[2], lifecycle.Observe, 4, time.Minute) // ran out long before the end
	for _, c := range []workload.Class{
		{Layer: catalog.L2, Owner: catalog.K8sPod, Side: workload.SideForward},
		{Layer: catalog.L2, Owner: catalog.K8sPod, Side: workload.SideExistence},
	} {
		if all := a.Pick(c, 10, 10, false); all.Prefixes != 3 {
			t.Fatalf("%s: %d prefixes, want 3", c, all.Prefixes)
		}
		live := a.Pick(c, 10, 10, true)
		if live.Prefixes != 1 || len(live.ByRecords) != 1 || live.ByRecords[0].Fingerprint != pods[0] || len(live.Median) != 1 {
			t.Errorf("%s: live picks %+v, want only the kept pod", c, live)
		}
	}
	// The node's reverse prefix has a live peer.
	if p := a.Pick(workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse}, 1, 1, true); p.Prefixes != 1 {
		t.Errorf("the node's reverse prefix is not live: %+v", p)
	}
}

// Prefixes with equal counts are ordered by fingerprint, so the choice does not
// depend on the order a map was walked in.
func TestAnalyzerPickBreaksTiesByFingerprint(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	a := workload.NewAnalyzer(start, time.Hour)
	var nodes []identity.Fingerprint
	for i := range 6 {
		n := fp(t, catalog.K8sNode, catalog.K8sNodeUID, string(rune('a'+i)))
		nodes = append(nodes, n)
		a.Add(engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pod, n, catalog.ScheduledOn), Producer: "p",
			EventTime: start, Seq: uint64(i + 1), Kind: lifecycle.Observe,
		})
	}
	slices.SortFunc(nodes, engine.CompareFingerprints)
	c := workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse}
	for range 5 {
		got := a.Pick(c, 3, 6, false)
		for i, p := range got.ByRecords {
			if p.Fingerprint != nodes[i] {
				t.Fatalf("hot prefix %d is %s, want %s", i, p.Fingerprint, nodes[i])
			}
		}
		for i, p := range got.Median {
			if p.Fingerprint != nodes[i] {
				t.Fatalf("median prefix %d is %s, want %s", i, p.Fingerprint, nodes[i])
			}
		}
	}
}

// What is live at the end is worked out again after more records are added: a
// pick made earlier does not stand for the stream as it is now.
func TestAnalyzerLiveSetFollowsTheStream(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	a := workload.NewAnalyzer(start, time.Hour)
	add := func(seq uint64, kind lifecycle.Kind) {
		a.Add(engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p",
			EventTime: start.Add(time.Duration(seq) * time.Second), Seq: seq, Kind: kind,
		})
	}
	c := workload.Class{Layer: catalog.L2, Owner: catalog.K8sPod, Side: workload.SideForward}
	add(1, lifecycle.Observe)
	if p := a.Pick(c, 5, 5, true); p.Prefixes != 1 {
		t.Fatalf("a placed pod is not live: %+v", p)
	}
	add(2, lifecycle.Delete)
	if p := a.Pick(c, 5, 5, true); p.Prefixes != 0 {
		t.Errorf("a pod that was deleted since is still live: %+v", p)
	}
}

// Prefixes are ranked by what a store holds in them after the final retention,
// not by the whole stream: the prefix with the most records before the horizon, all
// of which a retention folds into the baseline, is not the busiest after it.
func TestAnalyzerRanksByWhatARetentionLeaves(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	horizon := start.Add(30 * time.Minute)
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	old, fresh := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "old"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "fresh")
	build := func(from bool) workload.Picks {
		a := workload.NewAnalyzer(start, time.Hour)
		if from {
			a.SetRetainedFrom(horizon)
		}
		var seq uint64
		add := func(node identity.Fingerprint, minute, extensions int) {
			for i := range extensions + 1 {
				seq++
				r := engine.Record{
					Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p",
					EventTime: start.Add(time.Duration(minute) * time.Minute), Seq: seq, Kind: lifecycle.Observe, TTL: time.Minute,
				}
				if i > 0 {
					r.Through = r.EventTime.Add(time.Duration(i) * time.Second)
				}
				a.Add(r)
			}
		}
		add(old, 5, 40)   // an unbroken run from before the horizon: 41 records, none kept
		add(fresh, 40, 5) // a run since: 6 records, all kept
		return a.Pick(workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse}, 2, 2, false)
	}
	whole := build(false)
	if whole.ByRecords[0].Fingerprint != old || whole.ByExtensions[0].Fingerprint != old {
		t.Errorf("over the whole stream the unbroken run is busiest: %+v", whole.ByRecords)
	}
	kept := build(true)
	if kept.ByRecords[0].Fingerprint != fresh || kept.ByRecords[0].Kept != 6 || kept.ByRecords[1].Kept != 0 {
		t.Errorf("after the retention the new run is busiest, with 6 records kept and 0 for the old: %+v", kept.ByRecords)
	}
	if len(kept.ByExtensions) != 1 || kept.ByExtensions[0].Fingerprint != fresh || kept.ByExtensions[0].KeptExtensions != 5 {
		t.Errorf("by extensions held: %+v", kept.ByExtensions)
	}
}

// The retained history is reported for 2, 7, 14 and 30 days, 30 being the window the
// design plan's second layer is measured at, and only for a stream long enough to
// hold the window.
func TestHistoryIsReportedForThirtyDays(t *testing.T) {
	t.Parallel()

	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	edge := func(day int) engine.Record {
		return engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "a",
			EventTime: start.Add(time.Duration(day)*24*time.Hour + time.Hour), Kind: lifecycle.Observe, Payload: make([]byte, 10),
		}
	}
	history := func(days int) workload.ClassStats {
		a := workload.NewAnalyzer(start, time.Duration(days)*24*time.Hour)
		a.Add(edge(0))
		a.Add(edge(days - 1))
		c, ok := classOf(a.Report(), catalog.L2, catalog.K8sNode, workload.SideReverse)
		if !ok {
			t.Fatal("no class for the node's reverse prefix")
		}
		return c
	}
	// Thirty-one days: the record on the first day is outside the last thirty.
	if c := history(31); c.History[30].Max != 1 || c.HistoryActive[30] != 1 || c.History[14].Max != 1 {
		t.Errorf("31 days: 30-day history %+v over %d prefixes, 14-day %+v; want one record in each", c.History[30], c.HistoryActive[30], c.History[14])
	}
	// Thirty days exactly: both records are within it.
	if c := history(30); c.History[30].Max != 2 {
		t.Errorf("30 days: 30-day history %+v, want both records", c.History[30])
	}
	// Twenty-nine days is not long enough for the window.
	if c := history(29); c.History[30].Max != 0 || c.HistoryActive[30] != 0 || c.History[14].Max != 1 {
		t.Errorf("29 days: 30-day history %+v over %d prefixes, want none", c.History[30], c.HistoryActive[30])
	}
	var out strings.Builder
	a := workload.NewAnalyzer(start, 31*24*time.Hour)
	a.Add(edge(0))
	a.Report().Write(&out)
	if !strings.Contains(out.String(), "30d prefixes") {
		t.Errorf("the report has no column for 30 days:\n%s", out.String())
	}
}
