package workload_test

import (
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// A workload whose stream is in generation order (no lateness), so a test can
// replay it and know what was true at each record.
func orderly() workload.Config {
	c := workload.Tiny()
	c.Duration = 30 * time.Minute
	c.EventsPerSecond = 2
	c.LateProbability = 0
	c.ConfirmProbability = 0
	return c
}

func isPod(fp identity.Fingerprint) bool { return fp.Type() == catalog.K8sPod }

// Without FreshIdentities a deleted pod comes back under the same identity; with
// it a pod is created once, deleted at most once, and never mentioned again by
// the scheduler's producer afterwards. The first part is what the second is
// measured against: if reuse did not happen without the option, the test would
// pass for nothing.
func TestFreshIdentitiesNeverReuseAPod(t *testing.T) {
	t.Parallel()

	creations := func(cfg workload.Config) (perPod map[identity.Fingerprint]int, resurrected int) {
		perPod = map[identity.Fingerprint]int{}
		dead := map[identity.Fingerprint]bool{}
		for _, r := range generate(t, cfg) {
			if r.Producer != workload.ProducerK8sObjects || !isPod(r.Subject.A) {
				continue
			}
			if dead[r.Subject.A] {
				resurrected++
			}
			if r.Subject.Kind == engine.SubjectEntity {
				if r.Kind == lifecycle.Observe {
					perPod[r.Subject.A]++
				} else {
					dead[r.Subject.A] = true
				}
			}
		}
		return perPod, resurrected
	}

	reused, _ := creations(orderly())
	maxReused := 0
	for _, n := range reused {
		maxReused = max(maxReused, n)
	}
	if maxReused < 2 {
		t.Fatal("without FreshIdentities no pod was created twice: the test cannot tell the options apart")
	}

	fresh := orderly()
	fresh.FreshIdentities = true
	perPod, resurrected := creations(fresh)
	if resurrected != 0 {
		t.Errorf("%d records about pods after their deletion", resurrected)
	}
	for fp, n := range perPod {
		if n != 1 {
			t.Errorf("pod %s was created %d times", fp, n)
		}
	}
	if len(perPod) <= fresh.Pods {
		t.Errorf("%d pods ever existed with %d slots: churn made no new pod", len(perPod), fresh.Pods)
	}
}

// A reschedule creates a new pod, so over a couple of hours far more distinct
// pods are placed than there are slots, where reuse can never place more than
// the slots. That stream of new pods is what the option exists to produce (what
// it does to one node's prefix is checked on the report of a realistic stream).
func TestFreshIdentitiesPlaceFarMoreDistinctPodsThanSlots(t *testing.T) {
	t.Parallel()

	peers := func(cfg workload.Config) (distinct, observed int) {
		seen := map[identity.Fingerprint]bool{}
		for _, r := range generate(t, cfg) {
			if r.Subject.Kind == engine.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn &&
				r.Producer == workload.ProducerK8sObjects && r.Kind == lifecycle.Observe {
				seen[r.Subject.A] = true
				observed++
			}
		}
		return len(seen), observed
	}
	cfg := orderly()
	cfg.Duration = 2 * time.Hour
	reusedPeers, reusedPlacements := peers(cfg)
	cfg.FreshIdentities = true
	freshPeers, freshPlacements := peers(cfg)
	if reusedPeers > cfg.Pods {
		t.Fatalf("%d distinct pods with %d slots and reuse", reusedPeers, cfg.Pods)
	}
	if freshPeers < 3*cfg.Pods {
		t.Errorf("fresh identities gave %d distinct pods over %d placements, want far more than the %d slots (reuse gave %d over %d)",
			freshPeers, freshPlacements, cfg.Pods, reusedPeers, reusedPlacements)
	}
}

// Entities lists every entity that has existed, so a probe can ask about a pod
// that has long been gone as well as one that is there now.
func TestEntitiesListEveryEntityThatEverExisted(t *testing.T) {
	t.Parallel()

	cfg := orderly()
	cfg.FreshIdentities = true
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	before := len(g.Entities())
	all := g.All()
	listed := map[identity.Fingerprint]bool{}
	for _, fp := range g.Entities() {
		if listed[fp] {
			t.Fatalf("%s is listed twice", fp)
		}
		listed[fp] = true
	}
	if len(listed) <= before {
		t.Errorf("%d entities before the stream and %d after: new pods were not added", before, len(listed))
	}
	for _, r := range all {
		for _, fp := range []identity.Fingerprint{r.Subject.A, r.Subject.B} {
			if fp != (identity.Fingerprint{}) && !listed[fp] {
				t.Fatalf("record %+v is about %s, which Entities does not list", r, fp)
			}
		}
	}
}

// podsPerNode replays the scheduler's records and returns the most pods any node
// held at any moment.
func podsPerNode(t *testing.T, cfg workload.Config) (peak int) {
	t.Helper()
	at := map[identity.Fingerprint]identity.Fingerprint{} // pod -> node
	count := map[identity.Fingerprint]int{}
	for _, r := range generate(t, cfg) {
		if r.Producer != workload.ProducerK8sObjects || r.Subject.Kind != engine.SubjectEdge || r.Subject.Relation != catalog.ScheduledOn {
			continue
		}
		pod := r.Subject.A
		if old, ok := at[pod]; ok {
			count[old]--
			delete(at, pod)
		}
		if r.Kind == lifecycle.Observe {
			at[pod] = r.Subject.B
			count[r.Subject.B]++
			peak = max(peak, count[r.Subject.B])
		}
	}
	return peak
}

// MaxPodsPerNode holds at every moment of the stream, with or without fresh
// identities; without it the same skewed choice of node overloads one node.
func TestNodeCapacityIsRespected(t *testing.T) {
	t.Parallel()

	for _, fresh := range []bool{false, true} {
		cfg := orderly()
		cfg.Hosts, cfg.Pods, cfg.NodeSkew, cfg.EventsPerSecond = 6, 60, 1.6, 3
		cfg.FreshIdentities = fresh

		if peak := podsPerNode(t, cfg); peak <= 12 {
			t.Fatalf("fresh=%v: the uncapped peak is %d pods on a node: the cap cannot be told from skew", fresh, peak)
		}
		cfg.MaxPodsPerNode = 12
		if peak := podsPerNode(t, cfg); peak > 12 {
			t.Errorf("fresh=%v: %d pods on one node with a cap of 12", fresh, peak)
		}
	}
}

// The pod heartbeat re-asserts the placement of every live pod, and only of live
// pods, on its node, at each tick, with the TTL the interval implies.
func TestPodHeartbeatsRefreshEveryLivePod(t *testing.T) {
	t.Parallel()

	cfg := orderly()
	cfg.PodHeartbeatInterval = 2 * time.Minute
	cfg.HeartbeatTTLFactor = 4
	cfg.FreshIdentities = true
	live := map[identity.Fingerprint]identity.Fingerprint{} // pod -> node
	ticks := map[time.Time]int{}
	liveAt := map[time.Time]int{}
	for _, r := range generate(t, cfg) {
		switch {
		case r.Producer == workload.ProducerK8sObjects && r.Subject.Kind == engine.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn:
			if r.Kind == lifecycle.Observe {
				live[r.Subject.A] = r.Subject.B
			} else {
				delete(live, r.Subject.A)
			}
		case r.Producer == workload.ProducerPodHeartbeat:
			if r.Subject.Kind != engine.SubjectEdge || r.Subject.Relation != catalog.ScheduledOn || r.Kind != lifecycle.Observe {
				t.Fatalf("a pod heartbeat that is not a placement: %+v", r)
			}
			if want := 4 * cfg.PodHeartbeatInterval; r.TTL != want {
				t.Fatalf("TTL %s, want %s", r.TTL, want)
			}
			if node, ok := live[r.Subject.A]; !ok || node != r.Subject.B {
				t.Fatalf("heartbeat for pod %s on %s at %s, scheduler says %v", r.Subject.A, r.Subject.B, r.EventTime, node)
			}
			if _, seen := ticks[r.EventTime]; !seen {
				liveAt[r.EventTime] = len(live)
			}
			ticks[r.EventTime]++
		}
	}
	// A tick every interval after the start, until the end, exclusive.
	want := int(cfg.Duration/cfg.PodHeartbeatInterval) - 1
	if len(ticks) != want {
		t.Errorf("%d ticks, want %d", len(ticks), want)
	}
	for at, n := range ticks {
		if n != liveAt[at] {
			t.Errorf("at %s the heartbeat covered %d pods, %d were live", at, n, liveAt[at])
		}
	}
	none := orderly()
	for _, r := range generate(t, none) {
		if r.Producer == workload.ProducerPodHeartbeat {
			t.Fatal("a pod heartbeat without PodHeartbeatInterval")
		}
	}
}

// Under the backlog model each producer's records arrive in the order they
// happened, though the stream as a whole is out of order; under independent
// lateness a producer's own records are reordered. The second is what makes the
// first a check of something.
func TestBacklogKeepsEachProducersRecordsInOrder(t *testing.T) {
	t.Parallel()

	inversions := func(cfg workload.Config) (perProducer, overall int) {
		last := map[lifecycle.Producer]time.Time{}
		var prev time.Time
		for _, r := range generate(t, cfg) {
			if r.EventTime.Before(last[r.Producer]) {
				perProducer++
			}
			last[r.Producer] = r.EventTime
			if r.EventTime.Before(prev) {
				overall++
			}
			prev = r.EventTime
		}
		return perProducer, overall
	}
	cfg := orderly()
	cfg.Duration = 2 * time.Hour
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.4, 3*time.Minute

	if per, all := inversions(cfg); per != 0 || all != 0 {
		t.Fatalf("a stream with no lateness has %d inversions per producer and %d overall", per, all)
	}
	independent := cfg
	independent.LateProbability, independent.LateMeanDelay = 0.3, 3*time.Minute
	if per, _ := inversions(independent); per == 0 {
		t.Fatal("independent lateness reorders no producer's own records: the comparison proves nothing")
	}
	backlog := cfg
	backlog.BacklogEvery, backlog.BacklogMeanDelay, backlog.BacklogSpan = 10*time.Minute, 2*time.Minute, 4*time.Minute
	per, all := inversions(backlog)
	if per != 0 {
		t.Errorf("%d records arrived before an earlier record of their own producer", per)
	}
	if all == 0 {
		t.Error("the backlog delayed nothing: the stream is in order overall")
	}
}

// The workload with fresh identities, pod heartbeats, a node cap and backlog
// lateness, coalesced into runs, answers every question like the same workload
// uncoalesced: the property every run measurement stands on, for the shapes the
// new options make.
func TestRealisticStreamCoalescesLosslessly(t *testing.T) {
	t.Parallel()

	raw := workload.Tiny()
	raw.Duration = 25 * time.Minute
	raw.FreshIdentities, raw.MaxPodsPerNode = true, 9
	raw.PodHeartbeatInterval, raw.HeartbeatTTLFactor = time.Minute, 3
	raw.LateProbability = 0
	raw.BacklogEvery, raw.BacklogMeanDelay, raw.BacklogSpan = 6*time.Minute, time.Minute, 3*time.Minute
	raw.ConfirmProbability, raw.ConfirmTTL = 0.5, 3*time.Minute
	coalesced := raw
	coalesced.CoalesceRuns = true

	a, b := generate(t, raw), generate(t, coalesced)
	if len(b) > len(a) {
		t.Fatalf("coalescing produced %d records from %d", len(b), len(a))
	}
	extended := map[lifecycle.Producer]int{}
	for _, r := range b {
		if !r.Through.IsZero() {
			extended[r.Producer]++
		}
	}
	// ExtendEvery is zero, so every refresh is an extension, not a saving: what
	// matters is that the new producer's runs were extended and so were exercised
	// (the kubelet's single assertion per placement is never refreshed).
	if extended[workload.ProducerPodHeartbeat] == 0 {
		t.Fatalf("runs extended by producer: %v: the pod heartbeat never extended one", extended)
	}
	oa, ob := oracle.New(), oracle.New()
	if err := oa.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ob.Write(b); err != nil {
		t.Fatal(err)
	}
	g, _ := workload.New(raw)
	g.All()
	for _, fp := range g.Entities() {
		for off := time.Duration(0); off <= raw.Duration+10*time.Minute; off += 90 * time.Second {
			at := raw.Start.Add(off)
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				for _, layer := range allLayers {
					x, _ := oa.Neighbors(fp, dir, at, engine.Current(layer))
					y, _ := ob.Neighbors(fp, dir, at, engine.Current(layer))
					if !slices.Equal(x, y) {
						t.Fatalf("%s %s %s at +%s: raw %v, coalesced %v", fp, dir, layer, off, x, y)
					}
				}
			}
			x, _ := oa.Alive(fp, at, entityScope(fp))
			y, _ := ob.Alive(fp, at, entityScope(fp))
			if x != y {
				t.Fatalf("Alive(%s) at +%s: raw %v, coalesced %v", fp, off, x, y)
			}
		}
	}
}

// The same realistic config gives the same stream every time.
func TestRealisticStreamIsDeterministic(t *testing.T) {
	t.Parallel()

	cfg := orderly()
	cfg.FreshIdentities, cfg.MaxPodsPerNode = true, 9
	cfg.PodHeartbeatInterval, cfg.HeartbeatTTLFactor = time.Minute, 3
	cfg.LateProbability = 0
	cfg.BacklogEvery, cfg.BacklogMeanDelay, cfg.BacklogSpan = 6*time.Minute, time.Minute, 3*time.Minute
	cfg.CoalesceRuns = true
	if a, b := digest(generate(t, cfg)), digest(generate(t, cfg)); a != b {
		t.Error("two generators with one config produced different streams")
	}
	other := cfg
	other.Seed++
	if a, b := digest(generate(t, cfg)), digest(generate(t, other)); a == b {
		t.Error("a different seed gave the same stream")
	}
}

// ExtendTTLFraction is the design's rule (a run is re-asserted at most once per
// half its TTL) applied to each run's own TTL. Where every run has one TTL it is
// the same as ExtendEvery of that fraction, down to the byte; where TTLs differ,
// consecutive extensions of a run are at least that fraction of its TTL apart.
func TestExtensionBoundScalesWithEachRunsTTL(t *testing.T) {
	t.Parallel()

	base := orderly()
	base.CoalesceRuns = true
	base.HeartbeatInterval, base.RollupInterval, base.HeartbeatTTLFactor = time.Minute, time.Minute, 4
	byInterval, byFraction := base, base
	byInterval.ExtendEvery = 2 * time.Minute // half of the four-minute TTL every run has
	byFraction.ExtendTTLFraction = 0.5
	if a, b := digest(generate(t, byInterval)), digest(generate(t, byFraction)); a != b {
		t.Error("with one TTL for every run, a fraction of 0.5 differs from an interval of half of it")
	}

	mixed := byFraction
	mixed.FreshIdentities, mixed.PodHeartbeatInterval = true, 5*time.Minute // TTL 20 minutes
	mixed.Duration = time.Hour
	type key struct {
		subject  engine.Subject
		producer lifecycle.Producer
	}
	through := map[key]time.Time{}
	ttls := map[time.Duration]int{}
	for _, r := range generate(t, mixed) {
		if r.TTL == 0 || r.Through.IsZero() {
			continue
		}
		ttls[r.TTL]++
		k := key{r.Subject, r.Producer}
		if prev, ok := through[k]; ok {
			if gap, min := r.Through.Sub(prev), r.TTL/2; gap < min {
				t.Fatalf("%v extended %s after the last extension, TTL %s: less than half", k, gap, r.TTL)
			}
		}
		through[k] = r.Through
	}
	if len(ttls) < 2 {
		t.Fatalf("TTLs %v: the workload does not mix TTLs, so the test says nothing about scaling", ttls)
	}
}
