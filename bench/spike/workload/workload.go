// Package workload generates a deterministic stream of assertions that
// imitates infrastructure churn, for the storage-engine spike.
//
// The same Config always yields the same stream, byte for byte. The stream is
// in arrival order, which is write order: Seq counts arrivals, and a late
// record, one whose event time is well before its arrival, simply appears after
// records that happened later. That is what makes late-event handling
// testable.
//
// The simulated cluster is a small model of Kubernetes infrastructure: racks
// hold hosts, each host runs one node, pods are scheduled onto nodes with a
// skew (a few nodes hold most pods), each pod has containers and a service
// instance, and services depend on one another. Producers differ in how they
// speak: a watch-mode producer asserts a fact once and deletes it explicitly; a
// heartbeating producer refreshes the same description on a schedule and can go
// quiet; a rollup producer refreshes service dependencies; and a second
// producer, the kubelet, can also assert a pod's placement, so two producers
// hold one edge and one can delete what the other still holds. Payloads are random
// bytes, so byte counts are not flattered by compression.
package workload

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Producers in the workload.
const (
	ProducerFabric        lifecycle.Producer = "fabric"         // declared topology, watch mode
	ProducerK8sObjects    lifecycle.Producer = "k8sobjects"     // Kubernetes watch, watch mode
	ProducerNodeCollector lifecycle.Producer = "node-collector" // heartbeats
	ProducerTraces        lifecycle.Producer = "traces"         // service dependency rollups
	ProducerKubelet       lifecycle.Producer = "kubelet"        // a second opinion on pod placement
	ProducerPodHeartbeat  lifecycle.Producer = "k8s-cluster"    // heartbeats of every pod's placement
)

// Config describes a workload. [New] validates it and fills in nothing, so a
// zero field means zero: start from [Tiny] or [Small] and change what you need.
type Config struct {
	Seed     uint64
	Start    time.Time
	Duration time.Duration

	// FirstSeq is the Seq of the first record; the rest count up from it. It is
	// at least 1 (0 means "nothing" as a snapshot token) and at most 1<<63. A
	// layout that stores Seq in too few bits is only found by a stream that
	// crosses the boundary, so a config can start just below one.
	FirstSeq uint64

	Racks, Hosts, Pods, Services int
	ContainersPerPod             int // each pod has between one and this many
	DependsPerService            int

	// EventsPerSecond is the rate of pod churn: reschedules, deletes and
	// re-creates. PodSkew (greater than 1) is the Zipf exponent for which pod
	// churns; NodeSkew for which node a pod lands on.
	EventsPerSecond float64
	PodSkew         float64
	NodeSkew        float64

	// HeartbeatInterval is how often a node's collector refreshes the node and
	// its binding to its host; zero makes them watch mode. Heartbeats carry a TTL of
	// HeartbeatTTLFactor times the interval. An outage silences one node for
	// OutageLength, starting with probability OutageProbability at each beat.
	HeartbeatInterval  time.Duration
	HeartbeatTTLFactor int
	OutageProbability  float64
	OutageLength       time.Duration

	// RollupInterval is how often service dependencies are refreshed.
	RollupInterval time.Duration

	// ConfirmProbability is the chance that a pod's placement edge is also
	// asserted by a second producer (the kubelet), at the same instant as the
	// scheduler's own assertion, with a TTL of ConfirmTTL. When the placement
	// ends, the kubelet withdraws its assertion half the time and otherwise lets it
	// expire. So an edge can be held up by one producer while the other has
	// deleted it, and two producers can assert and delete at one instant: the
	// cases that decide whether a layout keeps producers apart. Zero leaves the
	// stream as if there were no second producer.
	ConfirmProbability float64
	ConfirmTTL         time.Duration

	// CoalesceRuns models the ingest-side coalescer: a refresh that repeats a
	// producer's current run is not a new record but an extension of the run,
	// re-asserted at the run's first event time with a later Through. Refreshes
	// that arrive too late to matter are dropped. The stream then answers every
	// question exactly as the uncoalesced one does.
	CoalesceRuns bool

	// ExtendEvery, with CoalesceRuns, limits how often a run is re-asserted: a
	// refresh closer than this to the last extension is absorbed without a
	// record. The run then understates how long its producer was seen by less
	// than ExtendEvery, so existence can end early but never late, and only the
	// end of an interval moves. When a run is replaced by another (its
	// description changes, or a retention means it can no longer be extended),
	// the store is first given what its deadline lacks to reach the new run, so
	// an interval of existence has no gap the producer did not have, apart from
	// one before the horizon, which the store no longer keeps, and apart from
	// what a refresh that arrives more than one run late can do (see
	// LateProbability).
	// Zero extends on every refresh, which loses nothing.
	ExtendEvery time.Duration

	// ExtendTTLFraction is ExtendEvery as a share of each run's own TTL: with 0.5
	// a run is re-asserted at most once per half its TTL, which is the rule the
	// design gives a store (a run whose TTL is four minutes at most every two,
	// one whose TTL is twenty at most every ten). It needs CoalesceRuns, is in
	// [0, 1], and is exclusive with ExtendEvery. Zero is off.
	ExtendTTLFraction float64

	// RunMaxAge, with CoalesceRuns, bounds how long one run of refreshes can go on: a
	// refresh that comes when the run is that old, counted from its first event time,
	// starts a new run instead of extending the old one, at its own event time, with
	// the same description and TTL, and the old run is first carried to it if its
	// stored deadline falls short (so existence is continuous, as at a retention).
	// Nothing about the producer's facts changes: it still refreshed at the same
	// instants, and what the store holds is one more version of the entity or edge
	// for each RunMaxAge it was refreshed for. What it bounds is how far a layout
	// that keeps a run's refreshes at its first instant (an extension is a record
	// at the start of its run, and deletes every checkpoint after it) has to walk
	// back to the oldest live run of a prefix. It applies to every refreshed run
	// whatever its layer. Zero is no bound; otherwise at least a minute.
	RunMaxAge time.Duration

	// LateProbability is the chance a record arrives late, by an exponentially
	// distributed delay with mean LateMeanDelay. Each record is delayed on its own,
	// so one producer's refreshes arrive out of order; with CoalesceRuns and a
	// bounded extension, a refresh that arrives later than the TTL after the one
	// before it makes the coalescer see a gap that the producer did not have, and the
	// stream can show the subject absent for up to the extension interval; and a
	// refresh that arrives after more than one new run has begun is older than the run
	// before the current one, which does not cover it, so it passes through and
	// replaces the deadline of an earlier run: the subject can then be absent for as
	// long as that run would have lasted. The
	// backlog model (BacklogEvery), which keeps a producer's records in order, does
	// not, and is the one the presets use.
	LateProbability float64
	LateMeanDelay   time.Duration

	// BacklogEvery, with BacklogMeanDelay and BacklogSpan, is the other model of
	// lateness: a producer's pipeline backs up. On average every BacklogEvery
	// (of event time, from the end of the last one) a producer starts an episode
	// that lasts BacklogSpan, during which each of its records arrives
	// late by the episode's delay (exponentially distributed with mean
	// BacklogMeanDelay, drawn once per episode). A producer's records still arrive
	// in the order they happened: a backlog delays a span of them together and
	// then drains in order, which is what a queue does and what independent
	// per-record lateness (LateProbability) does not, because that reorders one
	// producer's own records. The two models are exclusive. Zero is off.
	BacklogEvery, BacklogMeanDelay, BacklogSpan time.Duration

	// FreshIdentities gives a pod, its service instance and its containers a new
	// identity every time the pod is created, as Kubernetes does (a pod is never
	// moved or restarted under its old UID), and makes a reschedule a deletion
	// followed by the creation of a new pod on the new node. Without it a pod
	// slot keeps one identity for ever, so a hot node only ever sees as many
	// distinct peers as there are pod slots, which is far from real churn, and
	// that is expected to favour a layout that keeps one key per peer. The entities that have
	// existed so far are all in [Generator.Entities]. Runs that have expired are
	// dropped from the coalescer so its memory stays bounded.
	FreshIdentities bool

	// MaxPodsPerNode caps the pods on one node (Kubernetes' default is 110): a
	// pod that would land on a full node goes to the next one with room. Zero
	// is no cap. With a Zipf-skewed choice of node the busiest node would
	// otherwise hold a fifth or more of all pods.
	MaxPodsPerNode int

	// PodHeartbeatInterval, if above zero, has the producer ProducerPodHeartbeat
	// (the cluster-level collector that refreshes every pod) re-assert each live
	// pod's placement every interval, with a TTL of HeartbeatTTLFactor times the
	// interval. Nothing withdraws it when the pod moves or goes: it expires. So
	// the busiest node's reverse prefix holds a live run for each of its pods,
	// each extended on every refresh, which is what a layout that stores an
	// extension as a record at the run's start pays for.
	PodHeartbeatInterval time.Duration

	// PayloadMin and PayloadMax bound the random payload of each record.
	PayloadMin, PayloadMax int

	// PayloadPad adds this many random bytes to the end of every payload, drawn from a
	// source of their own: a config with it draws the same numbers from the stream's
	// source as one without, so its stream has the same records with longer payloads,
	// and a change in a measurement is the payload's alone. Zero adds nothing, and is
	// left out of the config's JSON, so the digest of a spec without it is as it was.
	PayloadPad int `json:",omitempty"`
}

// Tiny is twenty minutes of a handful of entities, for tests.
func Tiny() Config {
	c := Small()
	c.Duration = 20 * time.Minute
	c.Racks, c.Hosts, c.Pods, c.Services = 3, 8, 40, 6
	c.EventsPerSecond = 0.5
	return c
}

// Small is an hour of a modest cluster: enough to exercise every path.
func Small() Config {
	return Config{
		Seed:     1,
		FirstSeq: 1,
		Start:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Duration: time.Hour,
		Racks:    10, Hosts: 100, Pods: 1000, Services: 40,
		ContainersPerPod:  3,
		DependsPerService: 3,
		EventsPerSecond:   5,
		PodSkew:           1.3,
		NodeSkew:          1.2,
		HeartbeatInterval: time.Minute, HeartbeatTTLFactor: 4,
		OutageProbability: 0.002, OutageLength: 10 * time.Minute,
		RollupInterval:  time.Minute,
		LateProbability: 0.05, LateMeanDelay: 2 * time.Minute,
		PayloadMin: 16, PayloadMax: 96,
	}
}

func (c Config) valid() error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("workload: "+format, args...)
	}
	switch {
	case c.Start.IsZero() || c.Start.Nanosecond() != 0:
		return bad("Start must be set, on a whole second: event times have one-second resolution")
	case c.Duration <= 0:
		return bad("Duration must be positive")
	case c.FirstSeq < 1 || c.FirstSeq > 1<<63:
		return bad("FirstSeq must be in [1, 1<<63]")
	case c.ConfirmProbability < 0 || c.ConfirmProbability > 1 || (c.ConfirmProbability > 0 && c.ConfirmTTL <= 0):
		return bad("ConfirmProbability must be in [0, 1], with a positive ConfirmTTL if above 0")
	case c.ConfirmTTL%time.Second != 0:
		return bad("ConfirmTTL must be whole seconds")
	case c.Start.Before(engine.MinEventTime) || c.Start.Add(c.Duration).After(engine.MaxEventTime):
		return bad("the period %s to %s is outside the representable event times (1970 to 2262)",
			c.Start.Format(time.RFC3339), c.Start.Add(c.Duration).Format(time.RFC3339))
	case c.Racks < 1 || c.Hosts < 1 || c.Pods < 1 || c.Services < 1 || c.ContainersPerPod < 1:
		return bad("Racks, Hosts, Pods, Services and ContainersPerPod must each be at least 1")
	case c.DependsPerService < 0:
		return bad("DependsPerService must not be negative")
	case c.EventsPerSecond < 0:
		return bad("EventsPerSecond must not be negative")
	case c.PodSkew <= 1 || c.NodeSkew <= 1:
		return bad("Zipf skews must exceed 1")
	case c.HeartbeatInterval < 0 || c.RollupInterval < 0:
		return bad("intervals must not be negative")
	case c.HeartbeatInterval%time.Second != 0 || c.RollupInterval%time.Second != 0 || c.Duration%time.Second != 0:
		return bad("Duration and the intervals must be whole seconds: event times have one-second resolution")
	case c.PodHeartbeatInterval < 0 || c.PodHeartbeatInterval%time.Second != 0:
		return bad("PodHeartbeatInterval must be whole seconds and not negative")
	case c.MaxPodsPerNode < 0 || (c.MaxPodsPerNode > 0 && c.Hosts*c.MaxPodsPerNode < c.Pods):
		return bad("MaxPodsPerNode must not be negative, and %d hosts of at most %d pods cannot hold %d pods", c.Hosts, c.MaxPodsPerNode, c.Pods)
	case c.BacklogEvery < 0 || c.BacklogMeanDelay < 0 || c.BacklogSpan < 0 ||
		(c.BacklogEvery > 0 && (c.BacklogMeanDelay <= 0 || c.BacklogSpan <= 0 || c.BacklogSpan%time.Second != 0)):
		return bad("BacklogEvery needs a positive BacklogMeanDelay and a BacklogSpan of whole seconds, and none may be negative")
	case c.BacklogEvery > 0 && c.LateProbability > 0:
		return bad("BacklogEvery and LateProbability are two models of lateness: use one")
	case (c.HeartbeatInterval > 0 || c.RollupInterval > 0 || c.PodHeartbeatInterval > 0) && c.HeartbeatTTLFactor < 1:
		return bad("HeartbeatTTLFactor must be at least 1 when anything refreshes")
	case c.OutageProbability < 0 || c.OutageProbability > 1 || (c.OutageProbability > 0 && c.OutageLength <= 0):
		return bad("OutageProbability must be in [0, 1], with a positive OutageLength if above 0")
	case c.LateProbability < 0 || c.LateProbability > 1 || (c.LateProbability > 0 && c.LateMeanDelay <= 0):
		return bad("LateProbability must be in [0, 1], with a positive LateMeanDelay if above 0")
	case c.PayloadMin < 0 || c.PayloadMax < c.PayloadMin:
		return bad("payload bounds are %d to %d", c.PayloadMin, c.PayloadMax)
	case c.PayloadPad < 0:
		return bad("PayloadPad must not be negative")
	case c.ExtendEvery < 0 || (c.ExtendEvery > 0 && !c.CoalesceRuns):
		return bad("ExtendEvery needs CoalesceRuns and must not be negative")
	case c.RunMaxAge < 0 || (c.RunMaxAge > 0 && (!c.CoalesceRuns || c.RunMaxAge < time.Minute)):
		return bad("RunMaxAge needs CoalesceRuns and is zero or at least a minute")
	case c.ExtendTTLFraction < 0 || c.ExtendTTLFraction > 1 || (c.ExtendTTLFraction > 0 && !c.CoalesceRuns):
		return bad("ExtendTTLFraction must be in [0, 1] and needs CoalesceRuns")
	case c.ExtendTTLFraction > 0 && c.ExtendEvery > 0:
		return bad("ExtendEvery and ExtendTTLFraction are two ways to bound extension: use one")
	}
	return nil
}

// Generator produces the stream. It is not safe for concurrent use.
type Generator struct {
	cfg Config
	rng *rand.Rand
	pad *rand.Rand // the padding of payloads (PayloadPad), apart from rng
	res *identity.Resolver

	podZipf, nodeZipf *rand.Zipf

	cl    cluster
	queue arrivalQueue
	seq   uint64
	tick  uint64 // tie-break for equal arrival times, so the stream is stable

	end                                  time.Time
	nextChurn, nextHeartbeat, nextRollup time.Time
	nextPodHeartbeat                     time.Time
	backlogs                             map[lifecycle.Producer]*backlog
	records                              uint64    // records returned so far, to pace pruning
	churnClock                           time.Time // continuous time behind nextChurn
	exhausted                            bool

	runs    map[runKey]*run
	ready   []engine.Record // what the coalescer wrote for the arrival last taken, not yet returned
	head    int             // the next of ready to return
	horizon time.Time       // the store's retention horizon, once told
}

// New builds a generator for a valid Config.
func New(cfg Config) (*Generator, error) {
	if err := cfg.valid(); err != nil {
		return nil, err
	}
	g := &Generator{
		cfg:  cfg,
		rng:  rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		res:  identity.NewResolver(catalog.Default()),
		end:  cfg.Start.Add(cfg.Duration),
		runs: make(map[runKey]*run),
		seq:  cfg.FirstSeq - 1,
	}
	if cfg.BacklogEvery > 0 {
		g.backlogs = make(map[lifecycle.Producer]*backlog)
	}
	if cfg.PayloadPad > 0 {
		g.pad = rand.New(rand.NewPCG(cfg.Seed^0x7061646469e67000, ^cfg.Seed))
	}
	g.podZipf = rand.NewZipf(g.rng, cfg.PodSkew, 1, uint64(cfg.Pods-1))
	g.nodeZipf = rand.NewZipf(g.rng, cfg.NodeSkew, 1, uint64(cfg.Hosts-1))

	g.buildCluster()
	g.emitInitial()

	g.churnClock = cfg.Start
	g.nextChurn = g.scheduleChurn()
	g.nextHeartbeat = cfg.Start.Add(cfg.HeartbeatInterval)
	g.nextRollup = cfg.Start.Add(cfg.RollupInterval)
	if cfg.HeartbeatInterval <= 0 {
		g.nextHeartbeat = g.end.Add(time.Hour)
	}
	if cfg.RollupInterval <= 0 {
		g.nextRollup = g.end.Add(time.Hour)
	}
	g.nextPodHeartbeat = g.end.Add(time.Hour)
	if cfg.PodHeartbeatInterval > 0 {
		g.nextPodHeartbeat = cfg.Start.Add(cfg.PodHeartbeatInterval)
	}
	if cfg.EventsPerSecond <= 0 {
		g.nextChurn = g.end.Add(time.Hour)
	}
	return g, nil
}

// Next returns the next record in arrival order, assigning its Seq, or false
// when the stream is over.
func (g *Generator) Next() (engine.Record, bool) {
	for g.head == len(g.ready) {
		g.ready, g.head = g.ready[:0], 0
		rec, ok := g.nextArrival()
		if !ok {
			return engine.Record{}, false
		}
		if g.cfg.CoalesceRuns {
			g.ready = g.coalesce(rec, g.ready)
		} else {
			g.ready = append(g.ready, rec)
		}
	}
	rec := g.ready[g.head]
	g.head++
	g.seq++
	rec.Seq = g.seq
	if g.records++; g.cfg.FreshIdentities && g.records%pruneEvery == 0 {
		g.pruneRuns()
	}
	return rec, true
}

// nextArrival pops the next record in arrival order, generating events as
// needed.
func (g *Generator) nextArrival() (engine.Record, bool) {
	for {
		if g.queue.Len() > 0 && (g.exhausted || !g.queue.min().arrival.After(g.frontier())) {
			return heap.Pop(&g.queue).(arrivalItem).rec, true
		}
		if g.exhausted {
			return engine.Record{}, false
		}
		g.advance()
	}
}

// Batch returns up to n records, fewer at the end of the stream.
func (g *Generator) Batch(n int) []engine.Record {
	out := make([]engine.Record, 0, n)
	for len(out) < n {
		r, ok := g.Next()
		if !ok {
			break
		}
		out = append(out, r)
	}
	return out
}

// All drains the stream.
func (g *Generator) All() []engine.Record {
	var out []engine.Record
	for {
		r, ok := g.Next()
		if !ok {
			return out
		}
		out = append(out, r)
	}
}

// Entities returns every entity fingerprint in the cluster, in a fixed order,
// for choosing what to query. With FreshIdentities it grows as pods are created
// and lists every pod, service instance and container that has existed so far,
// dead ones included; otherwise the list never changes.
func (g *Generator) Entities() []identity.Fingerprint {
	cl := &g.cl
	var out []identity.Fingerprint
	if g.cfg.FreshIdentities {
		// Every pod, instance and container that has existed, in the order they
		// first did, after the entities that never change.
		for _, group := range [][]identity.Fingerprint{cl.racks, cl.hosts, cl.nodes, cl.services, cl.minted} {
			out = append(out, group...)
		}
		return out
	}
	for _, group := range [][]identity.Fingerprint{cl.racks, cl.hosts, cl.nodes, cl.services, cl.pods, cl.instances} {
		out = append(out, group...)
	}
	for _, cs := range cl.containers {
		out = append(out, cs...)
	}
	return out
}

// SetHorizon tells the generator the store has retained everything before h, so
// it will refuse a record with an earlier event time. A run of refreshes that
// started before h is then continued by a new run rather than extended at its
// start (see the coalescer), preceded where the old run's deadline falls short by
// a record that carries it to the new run, which is never before h. It only moves
// forward, and it changes nothing unless CoalesceRuns is set. Call it after each
// retention, before drawing the next record.
func (g *Generator) SetHorizon(h time.Time) {
	if h.After(g.horizon) {
		g.horizon = h
	}
}

// Start is the start of the simulated period. Event times fall between Start
// and End; late records can still arrive after End.
func (g *Generator) Start() time.Time { return g.cfg.Start }

// End is the end of the simulated period.
func (g *Generator) End() time.Time { return g.end }

// frontier is the event time of the next thing to happen.
func (g *Generator) frontier() time.Time {
	at := g.nextChurn
	if g.nextHeartbeat.Before(at) {
		at = g.nextHeartbeat
	}
	if g.nextRollup.Before(at) {
		at = g.nextRollup
	}
	if g.nextPodHeartbeat.Before(at) {
		at = g.nextPodHeartbeat
	}
	return at
}

// advance performs the next event, which is at the frontier.
func (g *Generator) advance() {
	at := g.frontier()
	if !at.Before(g.end) {
		g.exhausted = true
		return
	}
	switch {
	case at.Equal(g.nextHeartbeat):
		g.heartbeat(at)
		g.nextHeartbeat = at.Add(g.cfg.HeartbeatInterval)
	case at.Equal(g.nextRollup):
		g.rollup(at)
		g.nextRollup = at.Add(g.cfg.RollupInterval)
	case at.Equal(g.nextPodHeartbeat):
		g.podHeartbeat(at)
		g.nextPodHeartbeat = at.Add(g.cfg.PodHeartbeatInterval)
	default:
		g.churn(at)
		g.nextChurn = g.scheduleChurn()
	}
}

type arrivalItem struct {
	arrival time.Time
	tick    uint64
	rec     engine.Record
}

type arrivalQueue []arrivalItem

func (q arrivalQueue) Len() int { return len(q) }
func (q arrivalQueue) Less(i, j int) bool {
	if c := q[i].arrival.Compare(q[j].arrival); c != 0 {
		return c < 0
	}
	return q[i].tick < q[j].tick
}
func (q arrivalQueue) Swap(i, j int)    { q[i], q[j] = q[j], q[i] }
func (q *arrivalQueue) Push(x any)      { *q = append(*q, x.(arrivalItem)) }
func (q *arrivalQueue) Pop() any        { old := *q; n := len(old); x := old[n-1]; *q = old[:n-1]; return x }
func (q arrivalQueue) min() arrivalItem { return q[0] }
