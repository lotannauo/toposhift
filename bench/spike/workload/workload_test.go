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
				x, _ := oa.Neighbors(fp, dir, at)
				y, _ := ob.Neighbors(fp, dir, at)
				if !slices.Equal(x, y) {
					t.Fatalf("%s %s at +%s: raw %v, coalesced %v", fp, dir, off, x, y)
				}
			}
			x, _ := oa.Alive(fp, at)
			y, _ := ob.Alive(fp, at)
			if x != y {
				t.Fatalf("Alive(%s) at +%s: raw %v, coalesced %v", fp, off, x, y)
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
			x, _ := oa.Alive(fp, at)
			y, _ := ob.Alive(fp, at)
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
