package storetest

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The statistical tests check that each rate or probability is drawn as
// specified. The seeds are fixed, so the tests are deterministic: a failure is a
// change in the generator, never bad luck. Each count has to be within four
// standard deviations of its expectation, and large enough that four standard
// deviations are under a fifth of the expectation, so a count that is off by a
// fifth cannot pass.

var statSeeds = []uint64{1, 2, 3}

// statConfig is a long, wide cluster with nothing happening: each test turns on
// what it measures.
func statConfig(seed uint64) Config {
	c := Tiny()
	c.Seed = seed
	c.Duration = 12 * time.Hour
	c.Racks, c.Hosts, c.Pods, c.Services = 4, 20, 60, 2
	c.ChurnPerMinute = 0
	c.HeartbeatInterval, c.TTLFactor = 0, 0
	c.ConfirmProbability, c.LateProbability = 0, 0
	return c
}

func newStatGenerator(t *testing.T, c Config) *Generator {
	t.Helper()
	g, err := NewGenerator(c)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// requireWithin fails unless observed is within four sigma of expected, and the
// test has power: four sigma is under a fifth of expected.
func requireWithin(t *testing.T, what string, observed, expected, sigma float64) {
	t.Helper()
	if 4*sigma >= 0.2*expected {
		t.Fatalf("%s: four standard deviations (%.1f) are not under a fifth of the expectation %.1f, the test has no power", what, 4*sigma, expected)
	}
	if math.Abs(observed-expected) > 4*sigma {
		t.Errorf("%s = %.0f, want %.1f within %.1f (four standard deviations)", what, observed, expected, 4*sigma)
	}
}

func requireBinomial(t *testing.T, what string, successes, trials int, p float64) {
	t.Helper()
	n := float64(trials)
	requireWithin(t, what, float64(successes), n*p, math.Sqrt(n*p*(1-p)))
}

// churnEvents counts the churn events of a generator: every event leaves at
// least one churn record, tagged with its number.
func churnEvents(g *Generator) int {
	seen := map[int]bool{}
	for _, it := range g.items {
		if it.origin == originChurn {
			seen[it.event] = true
		}
	}
	return len(seen)
}

func TestChurnIsAPoissonProcess(t *testing.T) {
	t.Parallel()
	const perMinute = 5
	for _, seed := range statSeeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			c := statConfig(seed)
			c.ChurnPerMinute = perMinute
			lambda := perMinute * c.Duration.Minutes()
			requireWithin(t, "churn events", float64(churnEvents(newStatGenerator(t, c))), lambda, math.Sqrt(lambda))
		})
	}

	t.Run("no churn", func(t *testing.T) {
		t.Parallel()
		if n := churnEvents(newStatGenerator(t, statConfig(1))); n != 0 {
			t.Errorf("%d churn events at rate 0", n)
		}
	})
}

// placements counts the pod placements k8sobjects asserts, and how many of them
// the kubelet asserts at the same instant.
func placements(recs []store.Record) (placed, confirmed, kubeletDeletes int) {
	type key struct {
		subject store.Subject
		at      time.Time
	}
	kubelet := map[key]bool{}
	for _, r := range recs {
		if r.Producer != ProducerKubelet {
			continue
		}
		if r.Kind == lifecycle.Observe {
			kubelet[key{r.Subject, r.EventTime}] = true
		} else {
			kubeletDeletes++
		}
	}
	for _, r := range recs {
		if r.Producer == ProducerK8sObjects && r.Kind == lifecycle.Observe && r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.ScheduledOn {
			placed++
			if kubelet[key{r.Subject, r.EventTime}] {
				confirmed++
			}
		}
	}
	return placed, confirmed, kubeletDeletes
}

func TestTheKubeletConfirmsPlacementsWithTheGivenProbability(t *testing.T) {
	t.Parallel()
	const p = 0.3
	for _, seed := range statSeeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			c := statConfig(seed)
			c.ChurnPerMinute = 10
			c.ConfirmProbability, c.ConfirmTTL = p, 2*time.Minute
			g := newStatGenerator(t, c)
			ends := g.confirmEnds
			placed, confirmed, deletes := placements(g.All())
			requireBinomial(t, "confirmed placements", confirmed, placed, p)
			// Of the confirmed placements that ended, the kubelet withdrew half and let
			// the rest expire.
			requireBinomial(t, "withdrawn confirmations", deletes, ends, 0.5)
		})
	}

	for _, tt := range []struct {
		p    float64
		want func(placed, confirmed int) bool
		desc string
	}{
		{0, func(placed, confirmed int) bool { return confirmed == 0 }, "nothing is confirmed"},
		{1, func(placed, confirmed int) bool { return placed > 0 && confirmed == placed }, "everything is confirmed"},
	} {
		t.Run(fmt.Sprint("probability ", tt.p), func(t *testing.T) {
			t.Parallel()
			c := statConfig(1)
			c.Duration, c.ChurnPerMinute = time.Hour, 10
			c.ConfirmProbability, c.ConfirmTTL = tt.p, time.Minute
			placed, confirmed, _ := placements(newStatGenerator(t, c).All())
			if !tt.want(placed, confirmed) {
				t.Errorf("with probability %v, %d of %d placements are confirmed; want that %s", tt.p, confirmed, placed, tt.desc)
			}
		})
	}
}

// churnLateness returns how many churn records there are, how many are late, and
// the delays of the late ones.
func churnLateness(g *Generator) (churn, late int, delays []time.Duration) {
	for _, it := range g.items {
		if it.origin != originChurn {
			if it.late {
				panic("a record that is not churn is late")
			}
			continue
		}
		churn++
		if it.late {
			late++
			delays = append(delays, it.arrival.Sub(it.rec.EventTime))
		} else if !it.arrival.Equal(it.rec.EventTime) {
			panic("a record that is not late arrives late")
		}
	}
	return churn, late, delays
}

func TestChurnRecordsAreLateWithTheGivenProbability(t *testing.T) {
	t.Parallel()
	const p, lateMax = 0.2, 5 * time.Second
	for _, seed := range statSeeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			c := statConfig(seed)
			c.ChurnPerMinute = 10
			c.ConfirmProbability, c.ConfirmTTL = 0.3, time.Minute // so the kubelet's records are churn records too
			c.LateProbability, c.LateMax = p, lateMax
			churn, late, delays := churnLateness(newStatGenerator(t, c))
			requireBinomial(t, "late churn records", late, churn, p)

			reached := map[time.Duration]int{}
			for _, d := range delays {
				if d < time.Second || d > lateMax || d%time.Second != 0 {
					t.Fatalf("a late record is delayed by %s, want whole seconds in [1s, %s]", d, lateMax)
				}
				reached[d]++
			}
			for d := time.Second; d <= lateMax; d += time.Second {
				if reached[d] == 0 {
					t.Errorf("no record was delayed by %s in %d late records: delays %v", d, late, reached)
				}
			}
		})
	}

	for _, tt := range []struct {
		p                  float64
		wantNone, wantAll  bool
		lateMax, wantDelay time.Duration
	}{
		{p: 0, wantNone: true, lateMax: time.Second},
		{p: 1, wantAll: true, lateMax: time.Second, wantDelay: time.Second},
		{p: 1, wantAll: true, lateMax: 90 * time.Second},
	} {
		t.Run(fmt.Sprint("probability ", tt.p, " up to ", tt.lateMax), func(t *testing.T) {
			t.Parallel()
			c := statConfig(1)
			c.Duration, c.ChurnPerMinute = time.Hour, 10
			c.ConfirmProbability, c.ConfirmTTL = 0.3, time.Minute
			c.LateProbability, c.LateMax = tt.p, tt.lateMax
			churn, late, delays := churnLateness(newStatGenerator(t, c))
			if churn == 0 || (tt.wantNone && late != 0) || (tt.wantAll && late != churn) {
				t.Errorf("%d of %d churn records are late at probability %v", late, churn, tt.p)
			}
			for _, d := range delays {
				if d < time.Second || d > tt.lateMax || (tt.wantDelay != 0 && d != tt.wantDelay) {
					t.Fatalf("delay %s with LateMax %s", d, tt.lateMax)
				}
			}
		})
	}
}

// bootChanges counts the host heartbeats after the first whose boot ID differs
// from the host's last, and the clone records.
func bootChanges(recs []store.Record) (beats, reboots, clones int) {
	last := map[store.Subject]string{}
	for _, r := range recs {
		if r.Subject.Kind != store.SubjectEntity || r.Subject.A.Type() != catalog.Host || r.Kind != lifecycle.Observe {
			continue
		}
		switch r.Producer {
		case ProducerNodeCollector:
			if prev, ok := last[r.Subject]; ok {
				beats++
				if prev != r.Boot {
					reboots++
				}
			}
			last[r.Subject] = r.Boot
		case ProducerClone:
			clones++
		}
	}
	return beats, reboots, clones
}

func rebootConfig(seed uint64) Config {
	c := statConfig(seed)
	c.Pods, c.Services = 1, 1
	c.HeartbeatInterval, c.TTLFactor = time.Minute, 3
	return c
}

func TestHostsRebootAndAreClonedWithTheGivenProbabilities(t *testing.T) {
	t.Parallel()
	const reboot, clone = 0.2, 0.5
	for _, seed := range statSeeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			c := rebootConfig(seed)
			c.RebootProbability, c.CloneProbability = reboot, clone
			beats, reboots, clones := bootChanges(newStatGenerator(t, c).All())
			requireBinomial(t, "reboots", reboots, beats, reboot)
			requireBinomial(t, "clones", clones, reboots, clone)
		})
	}

	for _, tt := range []struct {
		name                    string
		reboot, clone           float64
		wantReboots, wantClones bool
	}{
		{name: "nothing reboots"},
		{name: "every beat reboots", reboot: 1, wantReboots: true},
		{name: "reboots are never cloned", reboot: 0.5, wantReboots: true},
		{name: "every reboot is cloned", reboot: 0.5, clone: 1, wantReboots: true, wantClones: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := rebootConfig(1)
			c.Duration = time.Hour
			c.RebootProbability, c.CloneProbability = tt.reboot, tt.clone
			beats, reboots, clones := bootChanges(newStatGenerator(t, c).All())
			if beats == 0 {
				t.Fatal("no heartbeats")
			}
			if (reboots > 0) != tt.wantReboots || (clones > 0) != tt.wantClones {
				t.Errorf("%d reboots in %d beats and %d clones; want reboots %v, clones %v", reboots, beats, clones, tt.wantReboots, tt.wantClones)
			}
			if tt.reboot == 1 && reboots != beats {
				t.Errorf("%d reboots in %d beats at probability 1", reboots, beats)
			}
			if tt.clone == 1 && clones != reboots {
				t.Errorf("%d clones for %d reboots at probability 1", clones, reboots)
			}
		})
	}
}

// TestEveryFeatureIsReached makes sure the streams the other tests use are not
// quiet by accident: with every feature on, Tiny leaves at least one of each
// thing the model can do.
func TestEveryFeatureIsReached(t *testing.T) {
	t.Parallel()
	c := Tiny()
	c.Runs = true
	c.RebootProbability, c.CloneProbability = 0.2, 0.5
	c.LateProbability = 0.2
	c.ConfirmProbability = 0.5
	g := newStatGenerator(t, c)

	late := 0
	for _, it := range g.items {
		if it.late {
			late++
		}
	}
	recs := g.All()
	_, _, kubeletDeletes := placements(recs)
	_, reboots, clones := bootChanges(plainStream(t, c))

	extensions := 0
	type moment struct {
		pod store.Subject
		at  time.Time
	}
	deleted := map[moment]identity.Fingerprint{} // the node a pod was taken off, and when
	rescheduled := 0
	for _, r := range recs {
		if !r.Through.IsZero() {
			extensions++
		}
		if r.Producer != ProducerK8sObjects || r.Subject.Kind != store.SubjectEdge || r.Subject.Relation != catalog.ScheduledOn {
			continue
		}
		pod := store.EntitySubject(r.Subject.A)
		if r.Kind == lifecycle.Delete {
			deleted[moment{pod, r.EventTime}] = r.Subject.B
		} else if from, ok := deleted[moment{pod, r.EventTime}]; ok && from != r.Subject.B {
			rescheduled++
		}
	}

	for name, n := range map[string]int{
		"late churn records":            late,
		"kubelet withdrawals":           kubeletDeletes,
		"kubelet expiries":              g.confirmEnds - kubeletDeletes,
		"run extensions":                extensions,
		"reboots":                       reboots,
		"clone records":                 clones,
		"reschedules (delete, observe)": rescheduled,
	} {
		if n == 0 {
			t.Errorf("no %s in %d records", name, len(recs))
		}
	}
}

// plainStream is the stream of c without runs: reboots are easier to count in it.
func plainStream(t *testing.T, c Config) []store.Record {
	t.Helper()
	c.Runs = false
	return newStatGenerator(t, c).All()
}

// exponentialGrid is the arguments the exponential draw is checked at: the edges,
// every power of two and its neighbours, and an even spread.
func exponentialGrid() []uint64 {
	const top = uint64(1) << 53
	grid := []uint64{1, 2, 3, top - 1, top, top / 2, top/2 + 1, top/2 - 1}
	for j := range 53 {
		p := uint64(1) << j
		grid = append(grid, p, p+1, p-1+boolTo(p == 1))
	}
	for i := uint64(1); i <= 1000; i++ {
		grid = append(grid, i*(top/1000))
	}
	return grid
}

func boolTo(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// TestNegLogMatchesTheLogarithm checks the draw's -ln(k / 2^53) against
// math.Log. math.Log is only the yardstick here: the generator itself does not
// call it.
func TestNegLogMatchesTheLogarithm(t *testing.T) {
	t.Parallel()
	for _, k := range exponentialGrid() {
		if k == 0 || k > 1<<53 {
			continue
		}
		want := -math.Log(float64(k) / (1 << 53))
		got := negLog(k)
		if math.Abs(got-want) > 1e-13*want+1e-15 {
			t.Errorf("negLog(%d) = %.17g, want %.17g", k, got, want)
		}
	}
	// Near 1 the answer is tiny, and still has to be right in its leading digits.
	for _, k := range []uint64{1<<53 - 1, 1<<53 - 2, 1<<53 - 1000} {
		want := -math.Log(float64(k) / (1 << 53))
		if got := negLog(k); math.Abs(got-want) > 1e-9*want {
			t.Errorf("negLog(%d) = %.17g, want %.17g", k, got, want)
		}
	}
	if got := negLog(1 << 53); math.Abs(got) > 1e-15 {
		t.Errorf("negLog(2^53) = %g, want 0", got)
	}
}

// TestNegLogIsTheSameOnEveryArchitecture pins the bits of the draw at a few
// arguments. The function uses only +, -, * and / on float64 values, with every
// product converted so that it cannot be fused, so the bits are the same on
// every architecture; CI runs this on more than one.
func TestNegLogIsTheSameOnEveryArchitecture(t *testing.T) {
	t.Parallel()
	tests := map[uint64]uint64{
		1:         0x40425e4f7b2737fa,
		2:         0x404205966f2b4f12,
		1<<52 + 1: 0x3fe62e42fefa39ec,
		1 << 53:   0x0,
	}
	for k, want := range tests {
		if got := math.Float64bits(negLog(k)); got != want {
			t.Errorf("bits of negLog(%d) = %#x (%.17g), want %#x", k, got, negLog(k), want)
		}
	}
}

func TestTheExponentialDrawHasMeanOneAndVarianceOne(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	const n = 200000
	var sum, sumSquares float64
	for range n {
		x := exponential(rng)
		sum += x
		sumSquares += x * x
	}
	mean := sum / n
	variance := sumSquares/n - mean*mean
	// The standard error of the mean is 1/sqrt(n), and of the variance about 2/sqrt(n).
	if math.Abs(mean-1) > 4/math.Sqrt(n) || math.Abs(variance-1) > 8/math.Sqrt(n) {
		t.Errorf("mean %.4f and variance %.4f, want 1 and 1", mean, variance)
	}
}

// TestARescheduleMovesThePodToAnotherNode checks every churn event that takes a
// pod off a node and puts it on one at the same instant, which is how a
// reschedule is made: it never puts the pod back on the same node, unless the
// cluster has a single node.
func TestARescheduleMovesThePodToAnotherNode(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := Tiny()
		c.Seed = rapid.Uint64().Draw(t, "seed")
		c.Hosts = rapid.IntRange(1, 5).Draw(t, "hosts")
		c.Pods = rapid.IntRange(1, 6).Draw(t, "pods")
		c.Duration = 10 * time.Minute
		g, err := NewGenerator(c)
		if err != nil {
			t.Fatal(err)
		}
		type move struct{ from, to []identity.Fingerprint }
		moves := map[int]*move{}
		for _, it := range g.items {
			r := it.rec
			if it.origin != originChurn || r.Producer != ProducerK8sObjects || r.Subject.Relation != catalog.ScheduledOn {
				continue
			}
			m := moves[it.event]
			if m == nil {
				m = &move{}
				moves[it.event] = m
			}
			if r.Kind == lifecycle.Delete {
				m.from = append(m.from, r.Subject.B)
			} else {
				m.to = append(m.to, r.Subject.B)
			}
		}
		for event, m := range moves {
			if len(m.from) == 0 || len(m.to) == 0 {
				continue // a creation or a deletion
			}
			if len(m.from) != 1 || len(m.to) != 1 {
				t.Fatalf("event %d takes a pod off %d nodes and puts it on %d", event, len(m.from), len(m.to))
			}
			if c.Hosts > 1 && m.from[0] == m.to[0] {
				t.Fatalf("event %d moves a pod from a node to the same node", event)
			}
		}
	})
}
