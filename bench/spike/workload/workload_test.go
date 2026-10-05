package workload_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
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

func generate(t *testing.T, cfg workload.Config) []engine.Record {
	t.Helper()
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return g.All()
}

// digest is a stable hash of a stream, so determinism is a byte-for-byte claim.
func digest(rs []engine.Record) [32]byte {
	h := sha256.New()
	var buf [8]byte
	put := func(v uint64) { binary.BigEndian.PutUint64(buf[:], v); h.Write(buf[:]) }
	for _, r := range rs {
		put(uint64(r.Layer))
		put(uint64(r.Subject.Kind))
		h.Write([]byte(r.Subject.A.String()))
		h.Write([]byte(r.Subject.B.String()))
		h.Write([]byte(r.Subject.Relation))
		h.Write([]byte(r.Producer))
		put(uint64(r.EventTime.UnixNano()))
		put(r.Seq)
		put(uint64(r.Kind))
		put(uint64(r.TTL))
		put(uint64(r.Through.UnixNano()))
		h.Write(r.Payload)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func TestSameConfigGivesTheSameStream(t *testing.T) {
	t.Parallel()

	a, b := generate(t, workload.Tiny()), generate(t, workload.Tiny())
	if len(a) == 0 {
		t.Fatal("an empty stream")
	}
	if digest(a) != digest(b) {
		t.Error("two generators with one config produced different streams")
	}

	other := workload.Tiny()
	other.Seed = 2
	if digest(generate(t, other)) == digest(a) {
		t.Error("a different seed produced the same stream")
	}
}

func TestBatchesAreTheSameStreamAsAll(t *testing.T) {
	t.Parallel()

	g, _ := workload.New(workload.Tiny())
	var batched []engine.Record
	for n := 1; ; n = n%97 + 1 {
		b := g.Batch(n)
		if len(b) == 0 {
			break
		}
		batched = append(batched, b...)
	}
	if digest(batched) != digest(generate(t, workload.Tiny())) {
		t.Error("reading in batches changed the stream")
	}
}

func TestStreamIsWellFormed(t *testing.T) {
	t.Parallel()

	g, _ := workload.New(workload.Small())
	rs := g.All()
	layers := map[catalog.Layer]int{}
	kinds := map[lifecycle.Kind]int{}
	producers := map[lifecycle.Producer]int{}
	for i, r := range rs {
		if r.Seq != uint64(i+1) {
			t.Fatalf("record %d has seq %d: seqs must be 1, 2, 3...", i, r.Seq)
		}
		if err := r.Validate(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if r.EventTime.Before(g.Start()) || r.EventTime.After(g.End()) {
			t.Fatalf("record %d at %s is outside the simulated period", i, r.EventTime)
		}
		if r.EventTime.Nanosecond() != 0 {
			t.Fatalf("record %d at %s: event times have one-second resolution", i, r.EventTime)
		}
		if r.Kind == lifecycle.Delete && len(r.Payload) != 0 {
			t.Fatalf("record %d: a delete carries a payload", i)
		}
		if r.Kind == lifecycle.Observe && (len(r.Payload) < 16 || len(r.Payload) > 96) {
			t.Fatalf("record %d: payload of %d bytes is outside 16 to 96", i, len(r.Payload))
		}
		layers[r.Layer]++
		kinds[r.Kind]++
		producers[r.Producer]++
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if layers[l] == 0 {
			t.Errorf("no records in layer %s", l)
		}
	}
	if kinds[lifecycle.Observe] == 0 || kinds[lifecycle.Delete] == 0 {
		t.Errorf("kinds = %v: want both observes and deletes", kinds)
	}
	for _, p := range []lifecycle.Producer{workload.ProducerFabric, workload.ProducerK8sObjects, workload.ProducerNodeCollector, workload.ProducerTraces} {
		if producers[p] == 0 {
			t.Errorf("producer %s never spoke", p)
		}
	}
	t.Logf("%d records: layers %v, kinds %v, producers %v", len(rs), layers, kinds, producers)
}

func TestLateRecordsExistExactlyWhenAsked(t *testing.T) {
	t.Parallel()

	late := func(rs []engine.Record) int {
		n, maxSeen := 0, time.Time{}
		for _, r := range rs {
			if r.EventTime.Before(maxSeen) {
				n++
			}
			if r.EventTime.After(maxSeen) {
				maxSeen = r.EventTime
			}
		}
		return n
	}

	none := workload.Tiny()
	none.LateProbability = 0
	if n := late(generate(t, none)); n != 0 {
		t.Errorf("%d late records with LateProbability 0", n)
	}
	some := workload.Tiny()
	some.LateProbability = 0.3
	if n := late(generate(t, some)); n == 0 {
		t.Error("no late records with LateProbability 0.3")
	}
}

func TestSameInstantTiesAreCommon(t *testing.T) {
	t.Parallel()

	seen := map[engine.Subject]map[time.Time]int{}
	ties := 0
	for _, r := range generate(t, workload.Tiny()) {
		if seen[r.Subject] == nil {
			seen[r.Subject] = map[time.Time]int{}
		}
		if seen[r.Subject][r.EventTime]++; seen[r.Subject][r.EventTime] == 2 {
			ties++
		}
	}
	// A delete and a re-assertion of one edge in the same second: the lifecycle
	// rule is that the higher sequence number wins, and MVCC layouts overwrite.
	if ties == 0 {
		t.Error("no subject has two records at one instant")
	}
}

func TestPayloadsAreIncompressible(t *testing.T) {
	t.Parallel()

	// Distinct payloads only: a heartbeat repeats its description on purpose,
	// which is what run coalescing exploits, but each description is random.
	var raw bytes.Buffer
	seen := map[string]bool{}
	for _, r := range generate(t, workload.Small()) {
		if !seen[string(r.Payload)] {
			seen[string(r.Payload)] = true
			raw.Write(r.Payload)
		}
	}
	var packed bytes.Buffer
	zw := gzip.NewWriter(&packed)
	_, _ = zw.Write(raw.Bytes())
	_ = zw.Close()
	if ratio := float64(packed.Len()) / float64(raw.Len()); ratio < 0.9 {
		t.Errorf("payloads compress to %.0f%% of their size: they would flatter byte counts", ratio*100)
	}
}

func TestChurnIsSkewed(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	for _, r := range generate(t, workload.Small()) {
		if r.Subject.Relation == catalog.ScheduledOn {
			counts[r.Subject.A.String()]++
		}
	}
	var sorted []int
	for _, n := range counts {
		sorted = append(sorted, n)
	}
	slices.SortFunc(sorted, func(a, b int) int { return b - a })
	median := sorted[len(sorted)/2]
	if sorted[0] < 8*median {
		t.Errorf("hottest pod has %d placement records and the median %d: churn is not skewed", sorted[0], median)
	}
}

// TestHeartbeatsAreARunOnceCoalesced is the property the liveness measurements
// depend on: a node's refreshes repeat one description, so the lifecycle
// specification folds them into a handful of runs.
func TestHeartbeatsAreARunOnceCoalesced(t *testing.T) {
	t.Parallel()

	cfg := workload.Small()
	cfg.OutageProbability = 0
	g, _ := workload.New(cfg)
	var as []lifecycle.Assertion
	var subject engine.Subject
	for _, r := range g.All() {
		if r.Subject.Relation != catalog.RunsOn {
			continue
		}
		if subject == (engine.Subject{}) {
			subject = r.Subject
		}
		if r.Subject == subject {
			as = append(as, r.Assertion())
		}
	}
	got := lifecycle.Coalesce(as)
	if len(as) < 50 || len(got) != 1 {
		t.Errorf("%d heartbeats coalesced to %d, want 1", len(as), len(got))
	}
}

func TestOutagesExpireNodes(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.OutageProbability, cfg.OutageLength = 0.15, 8*time.Minute
	by := map[engine.Subject][]lifecycle.Assertion{}
	for _, r := range generate(t, cfg) {
		if r.Subject.Relation == catalog.RunsOn {
			by[r.Subject] = append(by[r.Subject], r.Assertion())
		}
	}
	expired := 0
	for _, as := range by {
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		for _, iv := range tl.Existence() {
			if iv.EndSource == lifecycle.EndLivenessExpiry && iv.End.Before(workload.Tiny().Start.Add(workload.Tiny().Duration)) {
				expired++
			}
		}
	}
	if expired == 0 {
		t.Error("no node expired mid-run: outages should leave gaps")
	}
}

func TestRejectsBadConfigs(t *testing.T) {
	t.Parallel()

	if _, err := workload.New(workload.Config{}); err == nil {
		t.Error("an empty config was accepted: nothing is defaulted")
	}
	for name, mod := range map[string]func(*workload.Config){
		"flat skew":        func(c *workload.Config) { c.PodSkew = 1 },
		"payload order":    func(c *workload.Config) { c.PayloadMin, c.PayloadMax = 50, 10 },
		"negative payload": func(c *workload.Config) { c.PayloadMin = -1 },
		"no duration":      func(c *workload.Config) { c.Duration = -time.Second },
		"no start":         func(c *workload.Config) { c.Start = time.Time{} },
		"before 1970":      func(c *workload.Config) { c.Start = time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC) },
		"runs past 2262": func(c *workload.Config) {
			c.Start = time.Date(2262, 4, 11, 0, 0, 0, 0, time.UTC)
			c.Duration = 24 * time.Hour
		},
		"start off a whole second":  func(c *workload.Config) { c.Start = c.Start.Add(time.Millisecond) },
		"fractional heartbeat":      func(c *workload.Config) { c.HeartbeatInterval = 1500 * time.Millisecond },
		"fractional rollup":         func(c *workload.Config) { c.RollupInterval = time.Second / 2 },
		"no heartbeat factor":       func(c *workload.Config) { c.HeartbeatTTLFactor = 0 },
		"late without a mean":       func(c *workload.Config) { c.LateProbability, c.LateMeanDelay = 0.1, 0 },
		"late probability over one": func(c *workload.Config) { c.LateProbability = 1.5 },
		"outage without a length":   func(c *workload.Config) { c.OutageProbability, c.OutageLength = 0.1, 0 },
		"no racks":                  func(c *workload.Config) { c.Racks = 0 },
		"no pods":                   func(c *workload.Config) { c.Pods = 0 },
		"negative dependencies":     func(c *workload.Config) { c.DependsPerService = -1 },
		"negative heartbeat":        func(c *workload.Config) { c.HeartbeatInterval = -time.Minute },
		"negative rollup":           func(c *workload.Config) { c.RollupInterval = -time.Minute },
		"negative rate":             func(c *workload.Config) { c.EventsPerSecond = -1 },
		"extend without coalescing": func(c *workload.Config) { c.ExtendEvery = time.Minute },
		"no first seq":              func(c *workload.Config) { c.FirstSeq = 0 },
		"first seq too high":        func(c *workload.Config) { c.FirstSeq = 1<<63 + 1 },
		"confirm without a TTL":     func(c *workload.Config) { c.ConfirmProbability, c.ConfirmTTL = 0.5, 0 },
		"confirm over one":          func(c *workload.Config) { c.ConfirmProbability, c.ConfirmTTL = 1.5, time.Minute },
		"fractional confirm TTL":    func(c *workload.Config) { c.ConfirmProbability, c.ConfirmTTL = 0.5, 1500*time.Millisecond },
		"negative capacity":         func(c *workload.Config) { c.MaxPodsPerNode = -1 },
		"capacity below the pods":   func(c *workload.Config) { c.MaxPodsPerNode = 1 }, // 8 hosts, 40 pods
		"fractional pod heartbeat":  func(c *workload.Config) { c.PodHeartbeatInterval = 1500 * time.Millisecond },
		"negative pod heartbeat":    func(c *workload.Config) { c.PodHeartbeatInterval = -time.Minute },
		"pod heartbeat no factor":   func(c *workload.Config) { c.PodHeartbeatInterval, c.HeartbeatTTLFactor = time.Minute, 0 },
		"backlog without a delay": func(c *workload.Config) {
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = time.Hour, 0, time.Minute
		},
		"backlog without a span": func(c *workload.Config) {
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = time.Hour, time.Minute, 0
		},
		"backlog fractional span": func(c *workload.Config) {
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = time.Hour, time.Minute, 1500*time.Millisecond
		},
		"backlog and independent lateness": func(c *workload.Config) {
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = time.Hour, time.Minute, time.Minute
			c.LateProbability, c.LateMeanDelay = 0.1, time.Minute
		},
		"negative backlog":            func(c *workload.Config) { c.BacklogEvery = -time.Hour },
		"fraction without coalescing": func(c *workload.Config) { c.ExtendTTLFraction = 0.5 },
		"fraction over one":           func(c *workload.Config) { c.CoalesceRuns, c.ExtendTTLFraction = true, 1.5 },
		"negative fraction":           func(c *workload.Config) { c.CoalesceRuns, c.ExtendTTLFraction = true, -0.5 },
		"fraction and an interval": func(c *workload.Config) {
			c.CoalesceRuns, c.ExtendTTLFraction, c.ExtendEvery = true, 0.5, time.Minute
		},
	} {
		c := workload.Tiny()
		mod(&c)
		if _, err := workload.New(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, err := workload.New(workload.Tiny()); err != nil {
			t.Fatalf("the preset is itself invalid: %v", err)
		}
	}
}

func TestEntitiesAreStable(t *testing.T) {
	t.Parallel()

	a, _ := workload.New(workload.Tiny())
	b, _ := workload.New(workload.Tiny())
	if !slices.Equal(a.Entities(), b.Entities()) || len(a.Entities()) == 0 {
		t.Error("the entity list is empty or differs between generators")
	}
}

// TestCoalescedStreamAnswersLikeTheRawOne is the property the run measurements
// stand on: the same workload, with its refreshes folded into runs by the
// ingest-side coalescer, is much smaller and gives the oracle the same answers.
func TestCoalescedStreamAnswersLikeTheRawOne(t *testing.T) {
	t.Parallel()

	raw := workload.Tiny()
	raw.LateProbability, raw.OutageProbability = 0.3, 0.1
	coalesced := raw
	coalesced.CoalesceRuns = true

	a, b := generate(t, raw), generate(t, coalesced)
	if len(b) > len(a) {
		t.Errorf("coalescing produced %d records from %d", len(b), len(a))
	}
	extended := 0
	for _, r := range b {
		if !r.Through.IsZero() {
			extended++
		}
	}
	if extended == 0 {
		t.Fatal("no record carries a Through: no run was extended")
	}

	oa, ob := oracle.New(), oracle.New()
	if err := oa.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ob.Write(b); err != nil {
		t.Fatal(err)
	}
	g, _ := workload.New(raw)
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

// A bound on the age of a run changes how existence is stored and not what exists:
// with every refresh recorded, the stream with the bound gives the oracle the same
// answers as the raw one, whatever lateness the records have.
func TestRunMaxAgeAnswersLikeTheRawStream(t *testing.T) {
	t.Parallel()

	raw := workload.Tiny()
	raw.LateProbability, raw.OutageProbability = 0.3, 0.1
	bounded := raw
	bounded.CoalesceRuns, bounded.RunMaxAge = true, 6*time.Minute
	unbounded := raw
	unbounded.CoalesceRuns = true

	a, b, c := generate(t, raw), generate(t, bounded), generate(t, unbounded)
	if len(b) <= len(c) || len(b) >= len(a) {
		t.Fatalf("%d records raw, %d coalesced, %d coalesced with the bound: the bound must write more than none and fewer than the raw stream", len(a), len(c), len(b))
	}
	oa, ob := oracle.New(), oracle.New()
	if err := oa.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ob.Write(b); err != nil {
		t.Fatal(err)
	}
	g, _ := workload.New(raw)
	for _, fp := range g.Entities() {
		for off := time.Duration(0); off <= raw.Duration+10*time.Minute; off += 90 * time.Second {
			at := raw.Start.Add(off)
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				for _, layer := range allLayers {
					x, _ := oa.Neighbors(fp, dir, at, engine.Current(layer))
					y, _ := ob.Neighbors(fp, dir, at, engine.Current(layer))
					if !slices.Equal(x, y) {
						t.Fatalf("%s %s %s at +%s: raw %v, bounded %v", fp, dir, layer, off, x, y)
					}
				}
			}
			x, _ := oa.Alive(fp, at, entityScope(fp))
			y, _ := ob.Alive(fp, at, entityScope(fp))
			if x != y {
				t.Fatalf("Alive(%s) at +%s: raw %v, bounded %v", fp, off, x, y)
			}
		}
	}
}

// TestBoundedExtensionOnlyEndsExistenceEarly checks the lossy write-rate knob:
// re-asserting a run less often writes far fewer records, and can only
// understate how long something existed, never overstate it.
func TestBoundedExtensionOnlyEndsExistenceEarly(t *testing.T) {
	t.Parallel()

	exact := workload.Small()
	exact.Duration, exact.CoalesceRuns = 20*time.Minute, true
	exact.LateProbability, exact.OutageProbability = 0.05, 0.01
	lossy := exact
	lossy.ExtendEvery = 2 * time.Minute // half the four-minute TTL

	a, b := generate(t, exact), generate(t, lossy)
	extensions := func(rs []engine.Record) (n int) {
		for _, r := range rs {
			if !r.Through.IsZero() {
				n++
			}
		}
		return n
	}
	if ea, eb := extensions(a), extensions(b); eb == 0 || eb > ea*6/10 {
		t.Errorf("bounded extension wrote %d run extensions against %d: it should write far fewer", eb, ea)
	}
	oa, ob := oracle.New(), oracle.New()
	if err := oa.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ob.Write(b); err != nil {
		t.Fatal(err)
	}
	g, _ := workload.New(exact)
	shorter := 0
	for _, fp := range g.Entities() {
		if fp.Type() != catalog.K8sNode { // the heartbeating entities
			continue
		}
		for off := time.Duration(0); off <= exact.Duration+10*time.Minute; off += 30 * time.Second {
			at := exact.Start.Add(off)
			x, _ := oa.Alive(fp, at, entityScope(fp))
			y, _ := ob.Alive(fp, at, entityScope(fp))
			if y && !x {
				t.Fatalf("%s at +%s: the bounded stream says alive, the exact one dead", fp, off)
			}
			if x && !y {
				shorter++
			}
		}
	}
	if shorter == 0 {
		t.Error("existence never ended early: the knob changed nothing observable")
	}
}

// TestNoChurnMeansOnlyRefreshes checks the disabled path: with no churn the
// stream is the initial list plus refreshes, and nothing is ever deleted.
func TestNoChurnMeansOnlyRefreshes(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.EventsPerSecond, cfg.LateProbability = 0, 0
	var placements, deletes, refreshes int
	for _, r := range generate(t, cfg) {
		switch {
		case r.Kind == lifecycle.Delete:
			deletes++
		case r.Subject.Relation == catalog.ScheduledOn:
			placements++
		case r.TTL > 0:
			refreshes++
		}
	}
	if deletes != 0 {
		t.Errorf("%d deletes with no churn", deletes)
	}
	if placements != cfg.Pods { // one placement per pod, asserted once at the start
		t.Errorf("%d placements for %d pods: only the initial list should place pods", placements, cfg.Pods)
	}
	if refreshes == 0 {
		t.Error("no refreshes: heartbeats and rollups should continue without churn")
	}
}

// TestNothingScheduledStillTerminates covers every schedule being off at once.
func TestNothingScheduledStillTerminates(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.EventsPerSecond, cfg.HeartbeatInterval, cfg.RollupInterval = 0, 0, 0
	rs := generate(t, cfg)
	if len(rs) == 0 {
		t.Fatal("not even the initial list was emitted")
	}
	for _, r := range rs {
		if !r.EventTime.Equal(cfg.Start) {
			t.Fatalf("record at %s: with nothing scheduled, everything happens at the start", r.EventTime)
		}
	}
}

// TestChurnRateIsHonored is the regression test for a rate silently capped at
// one churn event per second. Every churn event writes one or two placement
// records, so the count of placement records after the initial list is between
// the number of events and twice it.
func TestChurnRateIsHonored(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.EventsPerSecond, cfg.LateProbability = 4, 0
	cfg.Duration = 10 * time.Minute
	cfg.HeartbeatInterval, cfg.RollupInterval = 0, 0
	events := int(cfg.EventsPerSecond * cfg.Duration.Seconds()) // 2400

	n := 0
	for _, r := range generate(t, cfg) {
		if r.Subject.Relation == catalog.ScheduledOn && r.EventTime.After(cfg.Start) {
			n++
		}
	}
	if n < events*95/100 || n > events*2*105/100 {
		t.Errorf("%d placement records after the start for about %d churn events: want between %d and %d",
			n, events, events, events*2)
	}
}

// TestFlapsReassertAtTheSameInstant checks the specific pattern the MVCC
// layouts overwrite on: a delete and, immediately after it in arrival order, a
// re-assertion of the same edge at the same instant.
func TestFlapsReassertAtTheSameInstant(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.EventsPerSecond, cfg.LateProbability = 0.3, 0
	cfg.HeartbeatInterval, cfg.RollupInterval = 0, 0

	type del struct {
		at  time.Time
		seq uint64
	}
	last := map[engine.Subject]del{}
	flaps := 0
	for _, r := range generate(t, cfg) {
		switch r.Kind {
		case lifecycle.Delete:
			last[r.Subject] = del{r.EventTime, r.Seq}
		case lifecycle.Observe:
			if d, ok := last[r.Subject]; ok && d.at.Equal(r.EventTime) && d.seq+1 == r.Seq && r.EventTime.After(cfg.Start) {
				flaps++
			}
		}
	}
	if flaps == 0 {
		t.Error("no edge was deleted and re-asserted at one instant")
	}
}

var allLayers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

// entityScope reads the layer an entity of this type is stored in.
func entityScope(fp identity.Fingerprint) engine.Scope {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		panic("unknown entity type " + string(fp.Type()))
	}
	return engine.Current(e.Layer())
}

func TestSeqStartsWhereTheConfigSays(t *testing.T) {
	t.Parallel()

	for _, first := range []uint64{1, 1<<32 - 50, 1 << 63} {
		cfg := workload.Tiny()
		cfg.FirstSeq = first
		recs := generate(t, cfg)
		if len(recs) < 100 {
			t.Fatalf("only %d records", len(recs))
		}
		for i, r := range recs {
			if r.Seq != first+uint64(i) {
				t.Fatalf("FirstSeq %d: record %d has Seq %d, want %d", first, i, r.Seq, first+uint64(i))
			}
		}
	}
	// Starting just below 2^32 crosses it, which is the point.
	cfg := workload.Tiny()
	cfg.FirstSeq = 1<<32 - 50
	recs := generate(t, cfg)
	if recs[0].Seq >= 1<<32 || recs[len(recs)-1].Seq < 1<<32 {
		t.Errorf("the stream does not cross 2^32: %d to %d", recs[0].Seq, recs[len(recs)-1].Seq)
	}
}

func TestASecondProducerConfirmsPlacementAtTheAskedRate(t *testing.T) {
	t.Parallel()

	none := workload.Small()
	none.Duration = 20 * time.Minute
	for _, r := range generate(t, none) {
		if r.Producer == workload.ProducerKubelet {
			t.Fatal("a kubelet record appeared with ConfirmProbability 0")
		}
	}

	// The stream without the knob is the stream it always was: the knob draws
	// from the random stream only when it is on.
	first, second := generate(t, none), generate(t, none)
	if digest(first) != digest(second) {
		t.Error("the stream without a second producer is not deterministic")
	}

	for _, p := range []float64{0.2, 0.7} {
		cfg := none
		cfg.ConfirmProbability, cfg.ConfirmTTL = p, 5*time.Minute
		recs := generate(t, cfg)

		var scheduler, kubelet, kubeletDeletes, sameInstantDeletes int
		type at struct {
			s engine.Subject
			t int64
		}
		schedulerDeletes := map[at]bool{}
		for _, r := range recs {
			if r.Subject.Relation != catalog.ScheduledOn {
				continue
			}
			switch {
			case r.Producer == workload.ProducerK8sObjects && r.Kind == lifecycle.Observe:
				scheduler++
			case r.Producer == workload.ProducerK8sObjects:
				schedulerDeletes[at{r.Subject, r.EventTime.UnixNano()}] = true
			}
		}
		for _, r := range recs {
			if r.Subject.Relation != catalog.ScheduledOn || r.Producer != workload.ProducerKubelet {
				continue
			}
			if r.Kind == lifecycle.Observe {
				kubelet++
				if r.TTL != cfg.ConfirmTTL {
					t.Fatalf("a kubelet record has TTL %s, want %s", r.TTL, cfg.ConfirmTTL)
				}
				continue
			}
			kubeletDeletes++
			if schedulerDeletes[at{r.Subject, r.EventTime.UnixNano()}] {
				sameInstantDeletes++
			}
		}
		// One confirmation is attempted per scheduler placement, so the rate is
		// the probability, give or take sampling noise (several thousand draws).
		if scheduler < 3000 {
			t.Fatalf("only %d placements: the rate cannot be checked", scheduler)
		}
		if got := float64(kubelet) / float64(scheduler); got < p-0.03 || got > p+0.03 {
			t.Errorf("ConfirmProbability %.1f: %d kubelet records for %d placements (%.3f)", p, kubelet, scheduler, got)
		}
		// Withdrawals happen at the instant the scheduler withdraws, and not for
		// every confirmed placement: the rest are left to expire.
		if kubeletDeletes == 0 || kubeletDeletes != sameInstantDeletes {
			t.Errorf("%d kubelet deletes, %d of them at the instant of the scheduler's delete: want all, and some", kubeletDeletes, sameInstantDeletes)
		}
		if kubeletDeletes >= kubelet {
			t.Errorf("%d kubelet deletes for %d confirmations: some must be left to expire", kubeletDeletes, kubelet)
		}
		// Of the confirmed placements that end, the kubelet withdraws half. A
		// placement ends at a scheduler delete that is not part of a flap (a flap
		// deletes and re-observes at one instant).
		type event struct {
			at       int64
			producer lifecycle.Producer
			kind     lifecycle.Kind
		}
		bySubject := map[engine.Subject][]event{}
		for _, r := range recs {
			if r.Subject.Relation == catalog.ScheduledOn {
				bySubject[r.Subject] = append(bySubject[r.Subject], event{r.EventTime.UnixNano(), r.Producer, r.Kind})
			}
		}
		ended, withdrawn := 0, 0
		for _, evs := range bySubject {
			slices.SortStableFunc(evs, func(a, b event) int { return int(a.at - b.at) })
			active := false
			for i := 0; i < len(evs); {
				j := i
				for j < len(evs) && evs[j].at == evs[i].at {
					j++
				}
				group := evs[i:j]
				i = j
				var schedulerDelete, schedulerObserve, kubeletDelete, kubeletObserve bool
				for _, e := range group {
					switch {
					case e.producer == workload.ProducerK8sObjects && e.kind == lifecycle.Delete:
						schedulerDelete = true
					case e.producer == workload.ProducerK8sObjects:
						schedulerObserve = true
					case e.kind == lifecycle.Delete:
						kubeletDelete = true
					default:
						kubeletObserve = true
					}
				}
				if schedulerDelete && !schedulerObserve && active {
					ended++
					if kubeletDelete {
						withdrawn++
					}
				}
				if kubeletObserve {
					active = true
				} else if schedulerDelete && !schedulerObserve {
					active = false
				}
			}
		}
		if ended < 300 {
			t.Fatalf("only %d confirmed placements ended: the withdrawal rate cannot be checked", ended)
		}
		if got := float64(withdrawn) / float64(ended); got < 0.4 || got > 0.6 {
			t.Errorf("the kubelet withdrew %d of %d ended confirmations (%.2f), want about half", withdrawn, ended, got)
		}
	}
}

// storeStream plays a generator against a store that retains at h as soon as it
// has seen an event time at or after h, and refuses, drops, any later record
// older than h. It returns what the store kept. With tell, the generator is told
// about the horizon, as the conformance test tells it.
func storeStream(t *testing.T, cfg workload.Config, h time.Time, tell bool) []engine.Record {
	t.Helper()
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var kept []engine.Record
	retained := false
	for {
		r, ok := g.Next()
		if !ok {
			return kept
		}
		if retained && r.EventTime.Before(h) {
			continue // refused: before the horizon
		}
		kept = append(kept, r)
		if !retained && !r.EventTime.Before(h) {
			retained = true
			if tell {
				g.SetHorizon(h)
			}
		}
	}
}

func TestARunThatStartedBeforeTheHorizonIsContinuedNotExtended(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.CoalesceRuns, cfg.LateProbability = true, 0
	h := cfg.Start.Add(10 * time.Minute)

	extendedBefore := func(rs []engine.Record) (n int) {
		for _, r := range rs {
			if !r.Through.IsZero() && r.EventTime.Before(h) {
				n++
			}
		}
		return n
	}
	// Without the horizon, every heartbeat of a run that began at the start of
	// the stream re-asserts it at that start: the store would refuse all of them.
	generated := func() []engine.Record { // what the generator offers, before the store refuses any
		var all []engine.Record
		g, _ := workload.New(cfg)
		retained := false
		for {
			r, ok := g.Next()
			if !ok {
				return all
			}
			if retained {
				all = append(all, r)
			}
			if !r.EventTime.Before(h) {
				retained = true
			}
		}
	}()
	if extendedBefore(generated) == 0 {
		t.Fatal("without the horizon, no extension was offered at a start before it: the test shows nothing")
	}

	// Told the horizon, the generator never offers one.
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	told := false
	extended := 0
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		if told && !r.Through.IsZero() && r.EventTime.Before(h) {
			t.Fatalf("after SetHorizon(%s), an extension was offered at %s", h, r.EventTime)
		}
		if told && !r.Through.IsZero() {
			extended++
		}
		if !told && !r.EventTime.Before(h) {
			told = true
			g.SetHorizon(h)
		}
	}
	if extended == 0 {
		t.Error("after the horizon no run was extended at all: new runs are not being extended either")
	}
}

func TestRetentionDoesNotKillAHeartbeatingRun(t *testing.T) {
	t.Parallel()

	nodesAlive := func(rs []engine.Record, cfg workload.Config, at time.Time) map[identity.Fingerprint]bool {
		o := oracle.New()
		if err := o.Write(slices.Clone(rs)); err != nil {
			t.Fatal(err)
		}
		g, _ := workload.New(cfg)
		alive := map[identity.Fingerprint]bool{}
		for _, fp := range g.Entities() {
			if fp.Type() != catalog.K8sNode {
				continue
			}
			ok, err := o.Alive(fp, at, entityScope(fp))
			if err != nil {
				t.Fatal(err)
			}
			alive[fp] = ok
		}
		return alive
	}

	base := workload.Tiny()
	base.LateProbability = 0 // a late record that the store refuses would differ for a reason other than the runs
	h := base.Start.Add(10 * time.Minute)
	probes := []time.Time{h, h.Add(7 * time.Minute), base.Start.Add(base.Duration - time.Second)}

	truth := base // every heartbeat a record of its own: nothing coalesced, nothing retained
	whole, err := workload.New(truth)
	if err != nil {
		t.Fatal(err)
	}
	reference := whole.All()

	for _, extendEvery := range []time.Duration{0, 2 * time.Minute} {
		cfg := base
		cfg.CoalesceRuns, cfg.ExtendEvery = true, extendEvery
		for _, at := range probes {
			want := nodesAlive(reference, truth, at)
			told := nodesAlive(storeStream(t, cfg, h, true), cfg, at)
			untold := nodesAlive(storeStream(t, cfg, h, false), cfg, at)

			killed := 0
			for fp, alive := range want {
				switch {
				case told[fp] && !alive:
					t.Fatalf("ExtendEvery %s at %s: %s is alive after retention but not in the uncoalesced stream", extendEvery, at.Sub(base.Start), fp)
				case extendEvery == 0 && alive && !told[fp]:
					t.Fatalf("ExtendEvery 0 at %s: %s was alive and the retained, told stream says dead", at.Sub(base.Start), fp)
				}
				if alive && !untold[fp] {
					killed++
				}
			}
			// The stream that is not told the horizon loses every run that began
			// before it, so by the end of the stream they are dead.
			if at.After(h.Add(5*time.Minute)) && extendEvery == 0 && killed == 0 {
				t.Errorf("at %s no run was killed by retention without the horizon: the test shows nothing", at.Sub(base.Start))
			}
		}
	}
}
