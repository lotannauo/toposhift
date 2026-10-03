package workload_test

import (
	"bytes"
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
