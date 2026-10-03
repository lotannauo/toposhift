package workload

import (
	"math"
	"testing"
	"time"
)

// A rate or a probability in a generator has to be tested as a rate: an earlier
// churn rate was silently capped at one event a second and nothing noticed until
// a test counted. The backlog model's two parameters are checked the same way,
// on the producer that writes often enough to start an episode as soon as it is
// due.
func TestBacklogEpisodesComeAtTheAskedRateAndDelay(t *testing.T) {
	t.Parallel()

	cfg := Tiny()
	cfg.Duration = 250 * time.Hour
	cfg.HeartbeatInterval, cfg.HeartbeatTTLFactor = time.Minute, 4
	cfg.EventsPerSecond, cfg.RollupInterval = 0, 0
	cfg.LateProbability, cfg.ConfirmProbability = 0, 0
	cfg.OutageProbability = 0
	cfg.BacklogEvery, cfg.BacklogMeanDelay, cfg.BacklogSpan = 20*time.Minute, 90*time.Second, 5*time.Minute
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g.All()
	b := g.backlogs[ProducerNodeCollector]
	if b == nil {
		t.Fatal("the node collector never had a backlog state")
	}

	// A renewal process with cycles of mean gap + span and gaps exponential: the
	// count over the run has the mean below and a standard deviation of about
	// sqrt(n) * gap / (gap + span).
	cycle := (cfg.BacklogEvery + cfg.BacklogSpan).Seconds()
	n := cfg.Duration.Seconds() / cycle
	sd := math.Sqrt(n) * cfg.BacklogEvery.Seconds() / cycle
	if got := float64(b.episodes); math.Abs(got-n) > 5*sd {
		t.Errorf("%v episodes in %s, want %.0f (sd %.1f)", got, cfg.Duration, n, sd)
	}
	// The mean of an exponential delay over k episodes has sd mean/sqrt(k).
	mean := b.delaySum.Seconds() / float64(b.episodes)
	want := cfg.BacklogMeanDelay.Seconds()
	if math.Abs(mean-want) > 5*want/math.Sqrt(float64(b.episodes)) {
		t.Errorf("mean delay %.1f s over %d episodes, want %.1f s", mean, b.episodes, want)
	}
}

// With fresh identities the coalescer forgets runs whose deadline has passed, so
// its memory follows the live runs and not the length of the simulation, and it
// still turns a run's refreshes into extensions.
func TestExpiredRunsAreForgottenWithFreshIdentities(t *testing.T) {
	t.Parallel()

	cfg := Tiny()
	cfg.Duration = 6 * time.Hour
	cfg.EventsPerSecond = 2
	cfg.CoalesceRuns, cfg.FreshIdentities = true, true
	cfg.ConfirmProbability, cfg.ConfirmTTL = 1, 3*time.Minute
	cfg.PodHeartbeatInterval, cfg.HeartbeatTTLFactor = 2*time.Minute, 3

	run := func(fresh bool) (kept, seen int) {
		c := cfg
		c.FreshIdentities = fresh
		g, err := New(c)
		if err != nil {
			t.Fatal(err)
		}
		keys := map[runKey]bool{}
		for {
			r, ok := g.Next()
			if !ok {
				break
			}
			if r.TTL > 0 {
				keys[runKey{r.Subject, r.Producer}] = true
			}
		}
		return len(g.runs), len(keys)
	}
	kept, seen := run(true)
	if seen < 2000 {
		t.Fatalf("only %d runs were ever started: the workload is too small to show the difference", seen)
	}
	if kept*8 > seen {
		t.Errorf("%d runs kept of %d seen: expired runs are not forgotten", kept, seen)
	}
	// And the control: without the option the map keeps every run.
	if keptAll, seenAll := run(false); keptAll < seenAll/2 {
		t.Errorf("without fresh identities %d runs kept of %d seen: nothing should be forgotten there", keptAll, seenAll)
	}
}

// A pod that moves must not land where it already is, even when that is the only
// node with room.
func TestADrawNeverReturnsTheNodeToAvoid(t *testing.T) {
	t.Parallel()

	cfg := Tiny()
	cfg.Hosts, cfg.Pods, cfg.MaxPodsPerNode = 3, 4, 2
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g.cl.nodeCount = []int{1, 2, 2} // only node 0 has room
	for range 300 {
		if got := g.drawNode(0); got == 0 {
			t.Fatal("drawNode(0) returned node 0")
		}
	}
}
